package frauddetectionpipeline

// processor-v2/main.go
//
// This keeps the original Pipeline / worker-pool / channel shape, but:
//   - ingestChan is now fed by a Kafka consumer instead of an in-process loop
//   - the in-memory sync.Map velocity check is replaced by Redis (sorted-set
//     sliding window + SETNX idempotency guard), so state survives restarts
//     and is shared across every replica of this processor
//   - resultProcessor now writes flagged transactions to a Kafka
//     `quarantine` topic (for a Temporal saga to pick up) instead of just
//     printing them
//   - every stage is instrumented with Prometheus metrics
// package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

// ---------------------------------------------------------------------
// Domain types (unchanged shape from the original, JSON tags kept as-is)
// ---------------------------------------------------------------------

type Transaction struct {
	ID        string    `json:"id"`
	UserID    string    `json:"user_id"`
	Amount    float64   `json:"amount"`
	Currency  string    `json:"currency"`
	Location  string    `json:"location"`
	Country   string    `json:"country"` // used for the cross-border rule
	Timestamp time.Time `json:"timestamp"`
}

type FraudResult struct {
	Transaction  Transaction `json:"transaction"`
	IsFraudulent bool        `json:"is_fraudulent"`
	RiskScore    int         `json:"risk_score"`
	Reasons      []string    `json:"reasons"`
}

// ---------------------------------------------------------------------
// Rule thresholds
// ---------------------------------------------------------------------

const (
	highValueThreshold   = 10_000.0
	velocityWindow       = 2 * time.Second // matches the original single-tx velocity rule
	slidingWindowAML     = 2 * time.Minute // cross-border AML rolling window
	crossBorderMaxTxns   = 5
	crossBorderMinAmount = 10_000.0
	idempotencyTTL       = 24 * time.Hour
)

// ---------------------------------------------------------------------
// Prometheus metrics
// ---------------------------------------------------------------------

var (
	processedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "txn_processed_total", Help: "Total transactions processed by workers.",
	})
	fraudTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "txn_fraud_total", Help: "Total transactions flagged as fraudulent.",
	})
	duplicateTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "txn_duplicate_total", Help: "Total transactions dropped by the idempotency guard.",
	})
	quarantineWriteFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "txn_quarantine_write_failures_total", Help: "Failed writes to the quarantine topic.",
	})
	processingLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "txn_processing_latency_seconds",
		Help:    "Time from Kafka fetch to fraud decision.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 12),
	})
	consumerLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "txn_consumer_lag_estimate", Help: "Approximate Kafka consumer lag.",
	})
	ingestQueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "txn_ingest_queue_depth", Help: "Current buffered length of ingestChan.",
	})
)

// ---------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------

type Pipeline struct {
	// Kafka
	reader   *kafka.Reader
	writer   *kafka.Writer // quarantine producer
	commitMu sync.Mutex

	// Redis: backs both the idempotency guard and the sliding-window state
	rdb *redis.Client

	// Internal fan-out between the single Kafka fetch loop and the worker pool
	ingestChan chan kafka.Message
	resultChan chan FraudResult

	workerCount int
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
}

func NewPipeline(brokers []string, inTopic, outTopic, groupID, redisAddr string, workerCount, bufferSize int) *Pipeline {
	ctx, cancel := context.WithCancel(context.Background())

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       inTopic,
		GroupID:     groupID, // consumer-group parallelism across replicas
		StartOffset: kafka.LastOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
	})

	writer := &kafka.Writer{
		Addr:  kafka.TCP(brokers...),
		Topic: outTopic,
		// Hash balancer: quarantine events for the same account stay
		// ordered too, which matters once a Temporal saga is reacting to them.
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})

	return &Pipeline{
		reader:      reader,
		writer:      writer,
		rdb:         rdb,
		ingestChan:  make(chan kafka.Message, bufferSize),
		resultChan:  make(chan FraudResult, bufferSize),
		workerCount: workerCount,
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Start spawns the Kafka fetch loop, the worker pool, and the result sink.
func (p *Pipeline) Start() {
	go p.fetchLoop()

	for i := 1; i <= p.workerCount; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}
	go p.resultProcessor()
	go p.reportQueueDepth()
}

// fetchLoop is the single reader pulling from Kafka and fanning out into
// ingestChan for the worker pool to pick up -- this replaces the original
// direct-call Ingest(tx) from the mock traffic generator.
func (p *Pipeline) fetchLoop() {
	for {
		msg, err := p.reader.FetchMessage(p.ctx)
		if err != nil {
			if p.ctx.Err() != nil {
				close(p.ingestChan)
				return
			}
			log.Printf("[fetch] error: %v", err)
			continue
		}

		select {
		case p.ingestChan <- msg:
		case <-p.ctx.Done():
			close(p.ingestChan)
			return
		}

		if lag, err := p.reader.ReadLag(p.ctx); err == nil {
			consumerLag.Set(float64(lag))
		}
	}
}

func (p *Pipeline) reportQueueDepth() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			ingestQueueDepth.Set(float64(len(p.ingestChan)))
		case <-p.ctx.Done():
			return
		}
	}
}

// worker mirrors the original: pull off the channel, evaluate, push to
// resultChan. The evaluation itself is now Redis-backed and can fail
// (network hiccup), in which case the message is neither committed nor
// forwarded -- it will be redelivered and retried.
func (p *Pipeline) worker(workerID int) {
	defer p.wg.Done()
	log.Printf("[Worker %d] started", workerID)

	for msg := range p.ingestChan {
		start := time.Now()

		var tx Transaction
		if err := json.Unmarshal(msg.Value, &tx); err != nil {
			log.Printf("[Worker %d] bad payload, skipping: %v", workerID, err)
			p.commit(msg)
			continue
		}

		dup, err := p.isDuplicate(p.ctx, tx.ID)
		if err != nil {
			log.Printf("[Worker %d] idempotency check failed, will retry: %v", workerID, err)
			continue // do NOT commit -- Kafka redelivers
		}
		if dup {
			duplicateTotal.Inc()
			p.commit(msg)
			continue
		}

		result, err := p.evaluateFraud(p.ctx, tx)
		if err != nil {
			log.Printf("[Worker %d] rule evaluation failed, will retry: %v", workerID, err)
			continue
		}

		processedTotal.Inc()
		processingLatency.Observe(time.Since(start).Seconds())

		p.resultChan <- result
		p.commit(msg)
	}
	log.Printf("[Worker %d] stopped", workerID)
}

// commit is serialized because kafka-go's Reader.CommitMessages is not
// safe to call concurrently from multiple goroutines on the same reader.
func (p *Pipeline) commit(msg kafka.Message) {
	p.commitMu.Lock()
	defer p.commitMu.Unlock()
	if err := p.reader.CommitMessages(p.ctx, msg); err != nil {
		log.Printf("commit failed: %v", err)
	}
}

// isDuplicate replaces nothing from the original code (it had no dedupe
// concept) -- it's added here because Kafka redelivery on worker crash or
// rebalance is a real possibility and would otherwise double-flag/report
// the same transaction.
func (p *Pipeline) isDuplicate(ctx context.Context, txnID string) (bool, error) {
	ok, err := p.rdb.SetNX(ctx, "idem:"+txnID, 1, idempotencyTTL).Result()
	if err != nil {
		return false, err
	}
	return !ok, nil
}

// evaluateFraud is the direct Redis-backed replacement for the original
// in-memory sync.Map version: same two rules (high-value + velocity),
// plus the cross-border sliding-window AML rule from the architecture doc.
func (p *Pipeline) evaluateFraud(ctx context.Context, tx Transaction) (FraudResult, error) {
	reasons := make([]string, 0)
	score := 0

	// Rule 1: High-value single transaction (unchanged from the original).
	if tx.Amount > highValueThreshold {
		score += 40
		reasons = append(reasons, "High value transaction (>10k)")
	}

	// Rule 2: Velocity check -- was sync.Map LoadOrStore, now Redis GETSET
	// with a TTL, so it works the same way across every processor replica
	// instead of only within one process's memory.
	velocityKey := "velocity:" + tx.UserID
	lastSeenStr, err := p.rdb.SetArgs(ctx, velocityKey, tx.Timestamp.Format(time.RFC3339Nano), redis.SetArgs{
		TTL: velocityWindow * 5,
		Get: true,
	}).Result()
	if err != nil && err != redis.Nil {
		return FraudResult{}, fmt.Errorf("velocity check: %w", err)
	}
	p.rdb.Expire(ctx, velocityKey, velocityWindow*5) // keep key fresh; not load-bearing for correctness
	if err != redis.Nil {
		if lastSeen, parseErr := time.Parse(time.RFC3339Nano, lastSeenStr); parseErr == nil {
			if tx.Timestamp.Sub(lastSeen) < velocityWindow {
				score += 50
				reasons = append(reasons, "Velocity limit breached: multi-tx within 2s")
			}
		}
	}

	// Rule 3: Cross-border AML sliding window -- new rule from the
	// architecture doc, using a Redis sorted set per account.
	if tx.Country != "" && tx.Country != "GB" && tx.Amount >= crossBorderMinAmount {
		flagged, err := p.checkCrossBorderWindow(ctx, tx)
		if err != nil {
			return FraudResult{}, fmt.Errorf("sliding window check: %w", err)
		}
		if flagged {
			score += 60
			reasons = append(reasons, fmt.Sprintf(
				"Cross-border velocity: >%d txns over £%.0f within %s",
				crossBorderMaxTxns, crossBorderMinAmount, slidingWindowAML))
		}
	}

	result := FraudResult{
		Transaction:  tx,
		IsFraudulent: score >= 50,
		RiskScore:    score,
		Reasons:      reasons,
	}
	if result.IsFraudulent {
		fraudTotal.Inc()
	}
	return result, nil
}

func (p *Pipeline) checkCrossBorderWindow(ctx context.Context, tx Transaction) (bool, error) {
	key := "window:" + tx.UserID
	cutoff := tx.Timestamp.Add(-slidingWindowAML)

	pipe := p.rdb.TxPipeline()
	pipe.ZRemRangeByScore(ctx, key, "-inf", fmt.Sprintf("%d", cutoff.UnixMilli()))
	pipe.ZAdd(ctx, key, redis.Z{Score: float64(tx.Timestamp.UnixMilli()), Member: tx.ID})
	pipe.Expire(ctx, key, slidingWindowAML+time.Minute)
	card := pipe.ZCard(ctx, key)
	if _, err := pipe.Exec(ctx); err != nil {
		return false, err
	}
	return card.Val() > int64(crossBorderMaxTxns), nil
}

// resultProcessor is the sink: fraudulent results are published to the
// quarantine topic for a Temporal saga to pick up; everything is logged.
func (p *Pipeline) resultProcessor() {
	for res := range p.resultChan {
		if res.IsFraudulent {
			payload, _ := json.Marshal(res)
			err := p.writer.WriteMessages(p.ctx, kafka.Message{
				Key:   []byte(res.Transaction.UserID),
				Value: payload,
			})
			if err != nil {
				quarantineWriteFailures.Inc()
				log.Printf("failed to write quarantine message for tx %s: %v", res.Transaction.ID, err)
			}
			log.Printf("\n⚠️  [ALERT] FRAUD DETECTED\n   Tx: %s | User: %s | Amount: %.2f %s\n   Score: %d | Reasons: %v\n",
				res.Transaction.ID, res.Transaction.UserID, res.Transaction.Amount, res.Transaction.Currency,
				res.RiskScore, res.Reasons)
		} else {
			log.Printf("✅ [Approved] Tx: %s | User: %s | Score: %d",
				res.Transaction.ID, res.Transaction.UserID, res.RiskScore)
		}
	}
}

// Stop gracefully shuts everything down: stop reading, drain workers,
// drain the sink, close Kafka clients.
func (p *Pipeline) Stop() {
	p.cancel()
	p.wg.Wait()
	close(p.resultChan)
	_ = p.reader.Close()
	_ = p.writer.Close()
}

// ---------------------------------------------------------------------
// main
// ---------------------------------------------------------------------

func main() {
	brokers := flag.String("brokers", "localhost:9092", "kafka broker list (comma-separated)")
	inTopic := flag.String("in-topic", "transactions", "source topic")
	outTopic := flag.String("out-topic", "quarantine", "quarantine/DLQ topic")
	groupID := flag.String("group", "fraud-processor", "kafka consumer group id")
	redisAddr := flag.String("redis", "localhost:6379", "redis address")
	workers := flag.Int("workers", 8, "worker pool size")
	buffer := flag.Int("buffer", 1000, "channel buffer size")
	metricsAddr := flag.String("metrics-addr", ":2112", "prometheus /metrics listen address")
	flag.Parse()

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Printf("metrics listening on %s/metrics", *metricsAddr)
		log.Fatal(http.ListenAndServe(*metricsAddr, nil))
	}()

	pipeline := NewPipeline([]string{*brokers}, *inTopic, *outTopic, *groupID, *redisAddr, *workers, *buffer)
	pipeline.Start()
	log.Printf("Pipeline running: %d workers, consuming %q, quarantining to %q", *workers, *inTopic, *outTopic)

	// Graceful shutdown on SIGINT/SIGTERM instead of a fixed sleep.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	// log.Println("shutting down pipeline gracefully...")
	pipeline.Stop()
}