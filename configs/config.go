package configs

import (
	"flag"
	"strings"
)

type Config struct {
	Brokers         []string
	InputTopic      string
	OutputTopic     string
	ConsumerGroupID string
	RedisAddress    string
	WorkerCount     int
	BufferSize      int
	MetricsAddr     string
}

func Load() Config {
	brokers := flag.String(
		"brokers",
		"localhost:9092",
		"comma-separated Kafka broker addresses",
	)

	inputTopic := flag.String(
		"in-topic",
		"transactions",
		"Kafka source topic",
	)

	outputTopic := flag.String(
		"out-topic",
		"quarantine",
		"Kafka quarantine topic",
	)

	groupID := flag.String(
		"group",
		"fraud-processor",
		"Kafka consumer group ID",
	)

	redisAddress := flag.String(
		"redis",
		"localhost:6379",
		"Redis server address",
	)

	workerCount := flag.Int(
		"workers",
		8,
		"number of fraud-processing workers",
	)

	bufferSize := flag.Int(
		"buffer",
		1000,
		"internal channel buffer size",
	)

	metricsAddress := flag.String(
		"metrics-addr",
		":2112",
		"Prometheus metrics server address",
	)

	flag.Parse()

	return Config{
		Brokers:         parseBrokers(*brokers),
		InputTopic:      *inputTopic,
		OutputTopic:     *outputTopic,
		ConsumerGroupID: *groupID,
		RedisAddress:    *redisAddress,
		WorkerCount:     *workerCount,
		BufferSize:      *bufferSize,
		MetricsAddr:     *metricsAddress,
	}
}

func parseBrokers(value string) []string {
	parts := strings.Split(value, ",")
	brokers := make([]string, 0, len(parts))

	for _, part := range parts {
		broker := strings.TrimSpace(part)

		if broker != "" {
			brokers = append(brokers, broker)
		}
	}

	return brokers
}