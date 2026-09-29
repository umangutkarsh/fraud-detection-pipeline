package main

import (
	"fraud-detection-pipeline/configs"
	"fraud-detection-pipeline/internal/pipeline"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	cfg := configs.Load()

	go startMetricsServer(cfg.MetricsAddr)

	fraudPipeline := pipeline.NewPipeline(
		cfg.Brokers,
		cfg.InputTopic,
		cfg.OutputTopic,
		cfg.ConsumerGroupID,
		cfg.RedisAddress,
		cfg.WorkerCount,
		cfg.BufferSize,
	)

	fraudPipeline.Start()

	log.Printf(
		"pipeline running: workers=%d, input_topic=%q, output_topic=%q",
		cfg.WorkerCount,
		cfg.InputTopic,
		cfg.OutputTopic,
	)

	waitForShutdownSignal()

	log.Println("shutting down pipeline gracefully...")

	if err := fraudPipeline.Stop(); err != nil {
		log.Printf("pipeline shutdown completed with errors: %v", err)
	}

	log.Println("pipeline stopped")
}

func startMetricsServer(address string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	server := &http.Server{
		Addr:    address,
		Handler: mux,
	}

	log.Printf("metrics listening on %s/metrics", address)

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("metrics server failed: %v", err)
	}
}

func waitForShutdownSignal() {
	signalChannel := make(chan os.Signal, 1)

	signal.Notify(
		signalChannel,
		os.Interrupt,
		syscall.SIGTERM,
	)

	<-signalChannel
}