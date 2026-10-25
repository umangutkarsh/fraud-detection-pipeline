package pipeline

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"fraud-detection-pipeline/internal/domain"
)

// These tests need a real Redis. Set REDIS_ADDR (e.g. localhost:6379) to run
// them; CI starts a Redis service container for this.
func newTestPipeline(t testing.TB) *Pipeline {
	t.Helper()

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set; skipping Redis-backed test")
	}

	rdb := redis.NewClient(&redis.Options{Addr: addr, PoolSize: 64})
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not reachable at %s: %v", addr, err)
	}
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush redis: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })

	return &Pipeline{rdb: rdb, ctx: ctx}
}

func TestHighValueAloneIsNotFraud(t *testing.T) {
	p := newTestPipeline(t)

	res, err := p.evaluateFraud(context.Background(), domain.Transaction{
		ID: "t1", UserID: "u1", Amount: 15_000, Country: "GB", Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.IsFraudulent || res.RiskScore != 40 {
		t.Fatalf("want score 40 and not fraudulent, got %+v", res)
	}
}

func TestVelocityBreach(t *testing.T) {
	p := newTestPipeline(t)
	ctx := context.Background()
	now := time.Now()

	if _, err := p.evaluateFraud(ctx, domain.Transaction{ID: "a", UserID: "u2", Amount: 10, Timestamp: now}); err != nil {
		t.Fatal(err)
	}
	res, err := p.evaluateFraud(ctx, domain.Transaction{ID: "b", UserID: "u2", Amount: 10, Timestamp: now.Add(500 * time.Millisecond)})
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsFraudulent {
		t.Fatalf("second transaction within 2s should be flagged, got %+v", res)
	}
}

func TestCrossBorderWindow(t *testing.T) {
	p := newTestPipeline(t)
	ctx := context.Background()
	base := time.Now()

	var last domain.FraudResult
	for i := 0; i <= crossBorderMaxTransactions; i++ {
		res, err := p.evaluateFraud(ctx, domain.Transaction{
			ID:        fmt.Sprintf("x%d", i),
			UserID:    "u3",
			Amount:    12_000,
			Country:   "FR",
			Timestamp: base.Add(time.Duration(i) * 10 * time.Second), // outside velocity window
		})
		if err != nil {
			t.Fatal(err)
		}
		last = res
	}
	if last.RiskScore != 100 {
		t.Fatalf("6th cross-border txn should score 40+60, got %+v", last)
	}
}

func TestDuplicateDetection(t *testing.T) {
	p := newTestPipeline(t)
	ctx := context.Background()

	first, err := p.isDuplicate(ctx, "dup-1")
	if err != nil || first {
		t.Fatalf("first sighting should not be duplicate (dup=%v err=%v)", first, err)
	}
	second, err := p.isDuplicate(ctx, "dup-1")
	if err != nil || !second {
		t.Fatalf("second sighting should be duplicate (dup=%v err=%v)", second, err)
	}
}

// BenchmarkScoring measures the per-transaction work a worker does after
// reading from Kafka: idempotency check + all fraud rules against Redis.
//
//	REDIS_ADDR=localhost:6379 go test ./internal/pipeline -run '^$' -bench Scoring -cpu 1,8,32
func BenchmarkScoring(b *testing.B) {
	p := newTestPipeline(b)
	ctx := context.Background()

	var seq atomic.Int64
	var mu sync.Mutex
	latencies := make([]time.Duration, 0, b.N)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		local := make([]time.Duration, 0, 1024)
		for pb.Next() {
			n := seq.Add(1)
			txn := domain.Transaction{
				ID:        fmt.Sprintf("bench-%d", n),
				UserID:    fmt.Sprintf("user-%d", n%10_000),
				Amount:    float64(n % 20_000),
				Country:   []string{"GB", "FR"}[n%2],
				Timestamp: time.Now(),
			}
			start := time.Now()
			if _, err := p.isDuplicate(ctx, txn.ID); err != nil {
				b.Fatal(err)
			}
			if _, err := p.evaluateFraud(ctx, txn); err != nil {
				b.Fatal(err)
			}
			local = append(local, time.Since(start))
		}
		mu.Lock()
		latencies = append(latencies, local...)
		mu.Unlock()
	})
	b.StopTimer()

	if len(latencies) == 0 {
		return
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	pct := func(q float64) float64 {
		return float64(latencies[int(q*float64(len(latencies)-1))].Microseconds())
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "tx/s")
	b.ReportMetric(pct(0.50), "p50-µs")
	b.ReportMetric(pct(0.99), "p99-µs")
}
