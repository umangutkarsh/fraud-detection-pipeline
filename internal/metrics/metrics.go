package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	ProcessedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "txn_processed_total",
			Help: "Total transactions processed by workers.",
		},
	)

	FraudTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "txn_fraud_total",
			Help: "Total transactions flagged as fraudulent.",
		},
	)

	DuplicateTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "txn_duplicate_total",
			Help: "Total transactions dropped by the idempotency guard.",
		},
	)

	QuarantineWriteFailures = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "txn_quarantine_write_failures_total",
			Help: "Total failed writes to the Kafka quarantine topic.",
		},
	)

	ProcessingLatency = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "txn_processing_latency_seconds",
			Help:    "Time from Kafka fetch to fraud decision.",
			Buckets: prometheus.ExponentialBuckets(0.0005, 2, 12),
		},
	)

	ConsumerLag = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "txn_consumer_lag_estimate",
			Help: "Approximate Kafka consumer lag.",
		},
	)

	IngestQueueDepth = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "txn_ingest_queue_depth",
			Help: "Current buffered length of the ingestion channel.",
		},
	)
)