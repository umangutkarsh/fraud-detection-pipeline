package pipeline

import (
	"context"
	"fraud-detection-pipeline/internal/domain"
	"sync"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	pipelinemetrics "fraud-detection-pipeline/internal/metrics"
)

type Pipeline struct {
	reader *kafka.Reader
	writer *kafka.Writer
	rdb    *redis.Client

	ingestChan chan kafka.Message
	resultChan chan domain.FraudResult

	workerCount int

	commitMu sync.Mutex
	workerWG sync.WaitGroup
	resultWG sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc
}

func NewPipeline(
	brokers []string,
	inputTopic string,
	outputTopic string,
	groupID string,
	redisAddress string,
	workerCount int,
	bufferSize int,
) *Pipeline {
	ctx, cancel := context.WithCancel(context.Background())

	reader := kafka.NewReader(
		kafka.ReaderConfig{
			Brokers:     brokers,
			Topic:       inputTopic,
			GroupID:     groupID,
			StartOffset: kafka.LastOffset,
			MinBytes:    1,
			MaxBytes:    10e6,
		},
	)

	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        outputTopic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
	}

	redisClient := redis.NewClient(
		&redis.Options{
			Addr: redisAddress,
		},
	)

	p := &Pipeline{
		reader:      reader,
		writer:      writer,
		rdb:         redisClient,
		ingestChan:  make(chan kafka.Message, bufferSize),
		resultChan:  make(chan domain.FraudResult, bufferSize),
		workerCount: workerCount,
		ctx:         ctx,
		cancel:      cancel,
	}

	pipelinemetrics.RegisterIngestQueueDepth(
		func() float64 { return float64(len(p.ingestChan)) },
		func() float64 { return float64(cap(p.ingestChan)) },
	)

	return p
}

func (p *Pipeline) Start() {
	go p.fetchLoop()
	// go p.reportQueueDepth()

	p.resultWG.Add(1)
	go p.resultProcessor()

	for workerID := 1; workerID <= p.workerCount; workerID++ {
		p.workerWG.Add(1)
		go p.worker(workerID)
	}
}