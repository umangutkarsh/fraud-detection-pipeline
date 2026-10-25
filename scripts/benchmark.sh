#!/usr/bin/env bash
# Benchmarks the local Docker Compose stack and prints CV-ready numbers.
#
#   ./scripts/benchmark.sh preflight          rebuild + start stack, check the fix is in
#   ./scripts/benchmark.sh ramp [rates...]    max sustained throughput (default 5000 10000 20000 40000)
#   ./scripts/benchmark.sh scale [rate]       throughput with 1, 3 and 6 replicas (default rate 20000)
#   ./scripts/benchmark.sh failover [rate]    kill a replica mid-load; recovery time + zero-loss check
#   ./scripts/benchmark.sh facts              machine, image size, scoring benchmark
#   ./scripts/benchmark.sh all                everything above, in order
#
# LOADGENS=3 ./scripts/benchmark.sh ramp 30000 45000   splits load over 3 parallel generators
#
# Every result line starts with "RESULT" and is also appended to benchmark-results.txt.
# Works with macOS's built-in bash 3.2; needs only docker, curl and awk.

set -uo pipefail

PROM="http://localhost:9090"
GROUP="fraud-processor"
TOPIC="transactions"
RUN_SECONDS="${RUN_SECONDS:-60}"
RESULTS_FILE="benchmark-results.txt"

cd "$(dirname "$0")/.." || exit 1

log()    { printf '\n==> %s\n' "$*"; }
result() { printf 'RESULT %s\n' "$*" | tee -a "$RESULTS_FILE"; }

# --- helpers -----------------------------------------------------------------

# prom_query 'PromQL' -> prints the scalar value of the first series, or "NaN".
prom_query() {
  local body
  body=$(curl -s "$PROM/api/v1/query" --data-urlencode "query=$1") || { echo NaN; return; }
  if printf '%s' "$body" | grep -q '"value":\['; then
    printf '%s' "$body" | sed -E 's/.*"value":\[[^,]*,"([^"]*)"\].*/\1/'
  else
    echo NaN
  fi
}

# Kafka's own view of the consumer group (authoritative lag and assignment).
group_describe() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --describe --group "$GROUP" 2>/dev/null
}

group_lag() {
  group_describe | awk -v t="$TOPIC" '$2==t && $6 ~ /^[0-9]+$/ {s+=$6} END {print s+0}'
}

# Distinct replicas holding partitions, identified by their HOST column ("/172.18.0.x").
# (kafka-go's consumer IDs contain spaces, so the column is found by its leading "/".)
group_members() {
  group_describe |
    awk -v t="$TOPIC" '$2==t { for (i=7; i<=NF; i++) if ($i ~ /^\//) { print $i; break } }' |
    sort -u | wc -l | tr -d ' '
}

# Wait until every message is consumed and committed; prints seconds waited.
wait_for_drain() {
  local start now lag
  start=$(date +%s)
  while :; do
    lag=$(group_lag)
    now=$(date +%s)
    if [ "$lag" -eq 0 ]; then echo $((now - start)); return 0; fi
    if [ $((now - start)) -ge 600 ]; then echo "timeout(lag=$lag)"; return 1; fi
    sleep 2
  done
}

wait_for_members() {
  local want=$1 start got
  start=$(date +%s)
  while :; do
    got=$(group_members)
    [ "$got" -ge "$want" ] && return 0
    if [ $(($(date +%s) - start)) -ge 120 ]; then
      echo "warning: only $got of $want consumers joined the group" >&2
      return 1
    fi
    sleep 3
  done
}

# run_load RATE SECONDS -> runs LOADGENS parallel generators (each RATE/LOADGENS),
# prints "total_sent combined_achieved_rate". One generator tops out around 15k tx/s
# on a laptop, so several are needed to find the pipeline's own limit.
LOADGENS="${LOADGENS:-1}"
run_load() {
  local rate=$1 secs=$2 per i dir
  per=$((rate / LOADGENS))
  dir=$(mktemp -d)
  for i in $(seq 1 "$LOADGENS"); do
    docker compose run --rm -T loadgen -rate "$per" -duration "${secs}s" >"$dir/$i.log" 2>&1 &
  done
  wait
  cat "$dir"/*.log | grep 'done:' |
    sed -E 's/.*sent=([0-9]+).*achieved_rate=([0-9]+).*/\1 \2/' |
    awk '{s+=$1; r+=$2} END {print s+0, r+0}'
  rm -rf "$dir"
}

latency_ms() { # $1 = quantile, $2 = window seconds
  prom_query "histogram_quantile($1, sum by (le) (increase(txn_processing_latency_seconds_bucket[${2}s])))" |
    awk '{ if ($1=="NaN") print "n/a"; else printf "%.1f", $1*1000 }'
}

# One measured run: offered rate -> effective throughput, drain time, p50/p99.
measure() {
  local rate=$1 label=$2 sent achieved drain p50 p99 flagged eff
  read -r sent achieved <<<"$(run_load "$rate" "$RUN_SECONDS")"
  p50=$(latency_ms 0.50 $((RUN_SECONDS + 5)))
  p99=$(latency_ms 0.99 $((RUN_SECONDS + 5)))
  drain=$(wait_for_drain)
  flagged=$(prom_query "sum(increase(txn_fraud_total[$((RUN_SECONDS + 20))s])) / sum(increase(txn_processed_total[$((RUN_SECONDS + 20))s])) * 100")
  eff=$(awk -v s="$sent" -v r="$RUN_SECONDS" -v d="${drain%%(*}" 'BEGIN { d=(d ~ /^[0-9]+$/)?d:0; printf "%.0f", s/(r+d) }')
  result "$label offered=${rate}/s loadgen_achieved=${achieved}/s sent=${sent} effective=${eff}/s drain_after_load=${drain}s p50=${p50}ms p99=${p99}ms flagged=$(awk -v f="$flagged" 'BEGIN{printf "%.1f", f}')%"
}

# --- tests -------------------------------------------------------------------

preflight() {
  log "Checking the throughput fix is in the source"
  if ! grep -q CommitInterval internal/pipeline/pipeline.go; then
    echo "The throughput patch is not applied (no CommitInterval in internal/pipeline/pipeline.go). Stop." >&2
    exit 1
  fi
  log "Rebuilding and starting the stack (3 replicas)"
  docker compose up -d --build --scale fraud-processor=3
  # loadgen is in the "tools" profile, so `up --build` skips it; build it explicitly
  # or `docker compose run loadgen` keeps using a stale image.
  log "Rebuilding the load generator image"
  docker compose --profile tools build loadgen
  log "Waiting for 3 consumers to join the group"
  wait_for_members 3
  log "Warm-up run (not recorded)"
  run_load 2000 15 >/dev/null
  wait_for_drain >/dev/null
  echo "Ready."
}

ramp() {
  local rates="${*:-5000 10000 20000 40000}" r
  log "Ramp test: ${RUN_SECONDS}s per step, rates: $rates"
  for r in $rates; do
    echo "  running ${r}/s ..."
    measure "$r" "ramp"
  done
  echo
  echo "drain_after_load of ~5s is the measuring floor (one Kafka status check), i.e. no backlog."
  echo "Max sustained throughput = the highest step where drain_after_load is a few seconds"
  echo "and effective is close to offered. If loadgen_achieved < offered, the laptop's load"
  echo "generator was the limit, not the pipeline."
}

scale() {
  local rate="${1:-20000}" n
  log "Scaling test at ${rate}/s with 1, 3 and 6 replicas"
  for n in 1 3 6; do
    docker compose up -d --no-recreate --scale fraud-processor="$n" >/dev/null 2>&1
    wait_for_members "$n"
    sleep 10
    echo "  running with $n replica(s) ..."
    measure "$rate" "scale replicas=$n"
  done
  docker compose up -d --no-recreate --scale fraud-processor=3 >/dev/null 2>&1
  wait_for_members 3
}

failover() {
  local rate="${1:-5000}" secs=120 tmp pid victim ip t_kill t_rebal rebal drain dups lag
  log "Failover test: ${rate}/s for ${secs}s, killing one replica after 40s"
  tmp=$(mktemp)
  docker compose run --rm -T loadgen -rate "$rate" -duration "${secs}s" >"$tmp" 2>&1 &
  pid=$!
  sleep 40

  victim=$(docker compose ps -q fraud-processor | head -n 1)
  ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$victim")
  echo "  killing replica $ip"
  t_kill=$(date +%s)
  docker kill "$victim" >/dev/null

  # Rebalance is complete when Kafka no longer assigns any partition to the dead replica.
  while group_describe | awk -v t="$TOPIC" -v h="/$ip" '$2==t { for (i=7; i<=NF; i++) if ($i==h) f=1 } END {exit !f}'; do
    sleep 2
    [ $(($(date +%s) - t_kill)) -ge 180 ] && break
  done
  t_rebal=$(date +%s)
  rebal=$((t_rebal - t_kill))
  echo "  partitions reassigned after ${rebal}s"

  wait "$pid"
  drain=$(wait_for_drain)
  lag=$(group_lag)
  dups=$(prom_query "sum(increase(txn_duplicate_total[$((secs + 60))s]))")
  result "failover offered=${rate}/s $(tail -n 1 "$tmp" | sed -E 's/.*(sent=[0-9]+).*/\1/') partitions_reassigned_after=${rebal}s drain_after_load=${drain}s final_group_lag=${lag} duplicates_dropped=$(awk -v d="$dups" 'BEGIN{printf "%.0f", d}')"
  echo "  final_group_lag=0 means every transaction was processed and committed (offsets"
  echo "  are only committed after processing, so nothing was skipped)."
  rm -f "$tmp"

  log "Restoring 3 replicas"
  docker compose up -d --no-recreate --scale fraud-processor=3 >/dev/null 2>&1
  wait_for_members 3
}

facts() {
  log "Machine and image facts"
  result "machine cpu=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m) docker_cpus=$(docker info --format '{{.NCPU}}') docker_mem=$(docker info --format '{{.MemTotal}}' | awk '{printf "%.1fGB", $1/1024/1024/1024}')"
  result "image $(docker image ls --format '{{.Repository}} {{.Size}}' | grep fraud-processor | head -n 1)"

  log "Scoring-path benchmark (note: flushes the local Redis)"
  local bench
  if command -v go >/dev/null 2>&1; then
    bench=$(REDIS_ADDR=localhost:6379 go test ./internal/pipeline -run '^$' -bench Scoring -benchtime 5s -cpu 1,8 2>&1)
  else
    bench=$(docker run --rm --network "$(basename "$PWD")_default" -v "$PWD":/src -w /src \
      -e REDIS_ADDR=redis:6379 golang:1.24-alpine \
      go test ./internal/pipeline -run '^$' -bench Scoring -benchtime 5s -cpu 1,8 2>&1)
  fi
  printf '%s\n' "$bench" | grep '^BenchmarkScoring' | while read -r line; do result "bench $line"; done
}

# --- main --------------------------------------------------------------------

cmd="${1:-}"; [ $# -gt 0 ] && shift
case "$cmd" in
  preflight) preflight ;;
  ramp)      ramp "$@" ;;
  scale)     scale "$@" ;;
  failover)  failover "$@" ;;
  facts)     facts ;;
  all)       preflight; ramp; scale; failover; facts
             echo; echo "All results:"; cat "$RESULTS_FILE" ;;
  *)         sed -n '2,14p' "$0"; exit 1 ;;
esac
