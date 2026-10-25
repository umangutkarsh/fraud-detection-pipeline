// Command loadgen publishes synthetic transactions to Kafka at a fixed rate so
// the fraud-processor can be load-tested locally or on EKS.
//
//	go run ./cmd/loadgen -brokers localhost:9092 -rate 2000 -duration 60s
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"fraud-detection-pipeline/internal/domain"
)

func main() {
	brokers := flag.String("brokers", "localhost:9092", "comma-separated Kafka broker addresses")
	topic := flag.String("topic", "transactions", "Kafka topic to publish to")
	rate := flag.Int("rate", 1000, "target transactions per second")
	duration := flag.Duration("duration", 60*time.Second, "how long to run (0 = until interrupted)")
	users := flag.Int("users", 10000, "number of distinct synthetic users")
	suspicious := flag.Float64("suspicious", 0.05, "fraction of high-value / cross-border transactions")
	batch := flag.Int("batch", 200, "messages per Kafka write")
	flag.Parse()

	writer := &kafka.Writer{
		Addr:         kafka.TCP(strings.Split(*brokers, ",")...),
		Topic:        *topic,
		Balancer:     &kafka.Hash{},
		BatchSize:    *batch,
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
	}
	defer writer.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	var sent atomic.Int64
	go reportProgress(ctx, &sent)

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	countries := []string{"GB", "GB", "GB", "FR", "DE", "US", "IN", "AE"}

	// Send in batches on a ticker so the achieved rate tracks -rate.
	ticksPerSecond := *rate / *batch
	if ticksPerSecond < 1 {
		ticksPerSecond = 1
	}
	perTick := *rate / ticksPerSecond
	ticker := time.NewTicker(time.Second / time.Duration(ticksPerSecond))
	defer ticker.Stop()

	started := time.Now()
	var seq int64

	for {
		select {
		case <-ctx.Done():
			elapsed := time.Since(started).Seconds()
			log.Printf("done: sent=%d elapsed=%.1fs achieved_rate=%.0f tx/s",
				sent.Load(), elapsed, float64(sent.Load())/elapsed)
			return
		case <-ticker.C:
		}

		messages := make([]kafka.Message, 0, perTick)
		for i := 0; i < perTick; i++ {
			seq++
			txn := domain.Transaction{
				ID:        fmt.Sprintf("txn-%d-%d", started.UnixNano(), seq),
				UserID:    fmt.Sprintf("user-%d", rng.Intn(*users)),
				Amount:    float64(rng.Intn(500)) + rng.Float64(),
				Currency:  "GBP",
				Location:  "online",
				Country:   "GB",
				Timestamp: time.Now().UTC(),
			}
			if rng.Float64() < *suspicious {
				txn.Amount = 10_000 + float64(rng.Intn(40_000))
				txn.Country = countries[rng.Intn(len(countries))]
			}

			payload, err := json.Marshal(txn)
			if err != nil {
				log.Fatalf("marshal transaction: %v", err)
			}
			messages = append(messages, kafka.Message{Key: []byte(txn.UserID), Value: payload})
		}

		if err := writer.WriteMessages(ctx, messages...); err != nil {
			if ctx.Err() != nil {
				continue
			}
			log.Printf("write failed: %v", err)
			continue
		}
		sent.Add(int64(len(messages)))
	}
}

func reportProgress(ctx context.Context, sent *atomic.Int64) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var last int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := sent.Load()
			log.Printf("sent=%d rate=%.0f tx/s", now, float64(now-last)/5)
			last = now
		}
	}
}
