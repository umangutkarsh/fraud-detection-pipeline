# Real-time Transaction Ingestion & Risk Pipeline

A Go service that consumes transactions from Kafka, scores each one against
Redis-backed fraud rules in real time, and publishes flagged transactions to a
`quarantine` topic. It ships with Terraform for AWS (VPC, EKS, ECR, GitHub OIDC),
Kubernetes manifests, Prometheus/Grafana observability, and GitHub Actions CI/CD.

```
            ┌──────────────┐   transactions   ┌───────────────────────────────┐   quarantine   ┌─────────────┐
 loadgen ──►│    Kafka     │ ───────────────► │ fraud-processor (3–6 replicas)│ ─────────────► │ downstream  │
            │  6 partitions│                  │  fetch → worker pool → result │                │  review     │
            └──────────────┘                  └──────────────┬────────────────┘                └─────────────┘
                                                             │ velocity / sliding-window / idempotency
                                                             ▼
                                                        ┌─────────┐        /metrics  ┌────────────┐   ┌─────────┐
                                                        │  Redis  │   ◄──────────────│ Prometheus │──►│ Grafana │
                                                        └─────────┘                  └────────────┘   └─────────┘
```

## How it works

- **Ingestion:** a consumer-group reader fetches from Kafka into a bounded
  channel. Offsets are committed only after a message is fully processed
  (at-least-once delivery).
- **Scoring:** a worker pool applies the rules:
  - high value (> £10k)
  - velocity (two transactions from the same user within 2s, using Redis `SET … GET`)
  - cross-border sliding window (more than 5 large foreign transactions in 2 min, using a Redis sorted set)
- **Idempotency:** a Redis `SETNX` guard with a 24h TTL drops redelivered
  duplicates, so replicas can share state and survive restarts.
- **Output:** fraudulent results go to the `quarantine` topic, keyed by user.
- **Graceful shutdown:** on SIGTERM it stops fetching, drains the workers and
  the result channel, then closes Kafka and Redis.
- **Metrics:**
  - throughput
  - flagged count
  - duplicates
  - quarantine write failures
  - p50/p99 processing latency (histogram)
  - consumer lag
  - ingest-queue saturation

## Layout

```
cmd/fraud-processor   service entrypoint (+ /metrics, /healthz on :2112)
cmd/loadgen           synthetic transaction producer for load tests
internal/pipeline     consumer, workers, rules, result processor, shutdown
deploy/terraform      VPC, EKS, ECR, GitHub OIDC role, kube-prometheus-stack
deploy/k8s            Kafka (KRaft), Redis, fraud-processor, HPA, PDB, ServiceMonitor, dashboard
.github/workflows     CI (vet, test, bench, docker, terraform validate) and CD to EKS
```

## Run locally

Prerequisites: Docker Desktop and Go 1.24.

```bash
docker compose up -d --build                                    # Kafka, Redis, 3 processors, Prometheus, Grafana
docker compose run --rm loadgen -rate 5000 -duration 60s        # push load
open http://localhost:3000                                      # Grafana (admin/admin) → "Fraud Pipeline"
docker compose down
```

Unit tests and the scoring benchmark need a Redis:

```bash
docker compose up -d redis
REDIS_ADDR=localhost:6379 go test ./...
REDIS_ADDR=localhost:6379 go test ./internal/pipeline -run '^$' -bench Scoring -benchtime 5s -cpu 1,8,32
```

## Deploy to AWS EKS

Prerequisites:

- an AWS account and the AWS CLI logged in (`aws sts get-caller-identity`)
- Terraform ≥ 1.10
- kubectl

**Cost:** about $0.10/h for the EKS control plane, about $0.045/h for the NAT gateway, and 2× t3.large spot nodes (about $0.03/h each). That is roughly **$0.20–0.25/h**. Run `terraform destroy` when you're done.

```bash
# 1. Infrastructure (~15 min)
cd deploy/terraform
cp terraform.tfvars.example terraform.tfvars    # set grafana_admin_password
terraform init
terraform apply

# 2. kubectl access
$(terraform output -raw configure_kubectl)
kubectl get nodes
```

Then pick one way to deploy the app.

**A — through GitHub Actions (recommended).** In the repo, go to Settings → Secrets and variables → Actions → Variables. Add:

| Variable | Value |
|---|---|
| `AWS_ROLE_ARN` | `terraform output -raw github_actions_role_arn` |
| `AWS_REGION` | `eu-west-2` |
| `CLUSTER_NAME` | `fraud-pipeline` |
| `ECR_REPOSITORY` | `fraud-pipeline/fraud-processor` |

Push to `main` (or run the *Deploy to EKS* workflow). It builds the image, pushes it to ECR tagged with the commit SHA, applies `deploy/k8s`, and waits for the rollout.

**B — by hand from your laptop.**

```bash
REPO=$(terraform -chdir=deploy/terraform output -raw ecr_repository_url)
aws ecr get-login-password | docker login --username AWS --password-stdin ${REPO%%/*}
docker buildx build --platform linux/amd64 -t $REPO:v1 --push .
kubectl kustomize deploy/k8s | sed "s|image: fraud-processor:latest|image: $REPO:v1|" | kubectl apply -f -
```

### Load test on EKS and read the numbers

```bash
sed "s|IMAGE|$REPO:v1|" deploy/k8s/loadtest/loadgen-job.yaml | kubectl apply -f -
kubectl -n fraud logs -f job/loadgen
kubectl -n monitoring port-forward svc/monitoring-grafana 3000:80   # → Dashboards → Fraud Pipeline
```

Increase `-rate` until consumer lag starts climbing steadily. The highest rate with flat lag is the sustained throughput. Record that rate, the p99 latency, and the replica count the HPA scaled to.

### Tear down

```bash
kubectl delete -k deploy/k8s     # remove workloads first
cd deploy/terraform && terraform destroy
```

## Benchmark results

Scoring path only (idempotency check plus all rules against Redis, excluding the Kafka network hop), from `BenchmarkScoring`:

| Environment | Concurrency | Throughput | p50 | p99 |
|---|---|---|---|---|
| 2-vCPU Linux VM, Redis on same host | 1 | ~7.4k tx/s | 0.13 ms | 0.33 ms |
| 2-vCPU Linux VM, Redis on same host | 8 | ~26.7k tx/s | 0.22 ms | 1.5 ms |
| 2-vCPU Linux VM, Redis on same host | 32 | ~34.1k tx/s | 0.68 ms | 4.8 ms |
| EKS, 3 replicas, end-to-end via Kafka | — | *fill in from load test* | | |

## Design notes and production gaps

- **Kafka and Redis:** both run in-cluster on `emptyDir` to keep the demo cheap and simple. In production I'd use Amazon MSK and ElastiCache, or Strimzi with 3 brokers and PVCs on gp3 via the EBS CSI driver.
- **Autoscaling:** the HPA scales on CPU and is capped at 6, the number of topic partitions. Scaling on consumer lag through KEDA would track backlog more directly.
- **CI/CD access:** CI/CD authenticates with GitHub OIDC, so there are no long-lived AWS keys. The deploy role is limited to ECR push for one repository plus `eks:DescribeCluster`, and gets cluster access through an EKS access entry. For a shared cluster I'd scope that entry to the `fraud` namespace.
- **Terraform state:** state is local by default. Uncomment the S3 backend in `versions.tf` for team use.
