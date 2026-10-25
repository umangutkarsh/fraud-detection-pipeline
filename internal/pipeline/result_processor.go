package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"

	"fraud-detection-pipeline/internal/domain"
	pipelinemetrics "fraud-detection-pipeline/internal/metrics"
)

// maxQuarantineBatch caps how many flagged results go into one Kafka write.
const maxQuarantineBatch = 500

// resultProcessor drains resultChan and publishes flagged transactions to the
// quarantine topic. It batches whatever is already waiting in the channel into
// a single WriteMessages call, so one slow broker round trip is shared by many
// results instead of being paid per transaction.
func (p *Pipeline) resultProcessor() {
	defer p.resultWG.Done()

	batch := make([]domain.FraudResult, 0, maxQuarantineBatch)

	for result := range p.resultChan {
		if result.IsFraudulent {
			batch = append(batch, result)
		}

		// Greedily collect results that are already queued, without blocking.
	drain:
		for len(batch) < maxQuarantineBatch {
			select {
			case next, ok := <-p.resultChan:
				if !ok {
					break drain
				}
				if next.IsFraudulent {
					batch = append(batch, next)
				}
			default:
				break drain
			}
		}

		if len(batch) > 0 {
			p.publishToQuarantine(batch)
			batch = batch[:0]
		}
	}
}

func (p *Pipeline) publishToQuarantine(results []domain.FraudResult) {
	messages := make([]kafka.Message, 0, len(results))

	for _, result := range results {
		message, err := quarantineMessage(result)
		if err != nil {
			pipelinemetrics.QuarantineWriteFailures.Inc()
			log.Printf("failed to encode quarantine message for transaction %s: %v", result.Transaction.ID, err)
			continue
		}
		messages = append(messages, message)
	}

	if len(messages) == 0 {
		return
	}

	// Use a background context so results drained during shutdown are still
	// written after p.ctx has been cancelled.
	if err := p.writer.WriteMessages(context.Background(), messages...); err != nil {
		pipelinemetrics.QuarantineWriteFailures.Add(float64(len(messages)))
		log.Printf("failed to write %d quarantine messages: %v", len(messages), err)
		return
	}

	for _, result := range results {
		log.Printf(
			"[ALERT] fraud detected tx=%s user=%s amount=%.2f %s score=%d reasons=%v",
			result.Transaction.ID,
			result.Transaction.UserID,
			result.Transaction.Amount,
			result.Transaction.Currency,
			result.RiskScore,
			result.Reasons,
		)
	}
}

func quarantineMessage(result domain.FraudResult) (kafka.Message, error) {
	payload, err := json.Marshal(result)
	if err != nil {
		return kafka.Message{}, fmt.Errorf("marshal fraud result: %w", err)
	}

	return kafka.Message{
		Key:   []byte(result.Transaction.UserID),
		Value: payload,
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte("transaction.quarantined")},
			{Key: "transaction_id", Value: []byte(result.Transaction.ID)},
		},
	}, nil
}
