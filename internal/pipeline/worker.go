package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"

	"fraud-detection-pipeline/internal/domain"

	pipelinemetrics "fraud-detection-pipeline/internal/metrics"
)

const idempotencyTTL = 24 * time.Hour

func (p *Pipeline) worker(workerID int) {
	defer p.workerWG.Done()

	log.Printf("[worker %d] started", workerID)
	defer log.Printf("[worker %d] stopped", workerID)

	for message := range p.ingestChan {
		p.processMessage(workerID, message)
	}
}

func (p *Pipeline) processMessage(
	workerID int,
	message kafka.Message,
) {
	startedAt := time.Now()

	var transaction domain.Transaction

	if err := json.Unmarshal(message.Value, &transaction); err != nil {
		log.Printf(
			"[worker %d] invalid transaction payload: %v",
			workerID,
			err,
		)

		// Poison messages are skipped rather than retried forever.
		p.commit(message)
		return
	}

	duplicate, err := p.isDuplicate(p.ctx, transaction.ID)
	if err != nil {
		log.Printf(
			"[worker %d] idempotency check failed: %v",
			workerID,
			err,
		)
		return
	}

	if duplicate {
		pipelinemetrics.DuplicateTotal.Inc()
		p.commit(message)
		return
	}

	result, err := p.evaluateFraud(p.ctx, transaction)
	if err != nil {
		log.Printf(
			"[worker %d] fraud evaluation failed: %v",
			workerID,
			err,
		)
		return
	}

	pipelinemetrics.ProcessedTotal.Inc()
	pipelinemetrics.ProcessingLatency.Observe(
		time.Since(startedAt).Seconds(),
	)

	select {
	case p.resultChan <- result:
		p.commit(message)

	case <-p.ctx.Done():
		return
	}
}

func (p *Pipeline) isDuplicate(
	ctx context.Context,
	transactionID string,
) (bool, error) {
	if transactionID == "" {
		return false, fmt.Errorf("transaction ID cannot be empty")
	}

	key := "idem:" + transactionID

	created, err := p.rdb.SetNX(
		ctx,
		key,
		1,
		idempotencyTTL,
	).Result()

	if err != nil {
		return false, fmt.Errorf("set idempotency key: %w", err)
	}

	return !created, nil
}