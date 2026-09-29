package pipeline

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"

	"fraud-detection-pipeline/internal/domain"
	pipelinemetrics "fraud-detection-pipeline/internal/metrics"
)

func (p *Pipeline) resultProcessor() {
	defer p.resultWG.Done()

	for result := range p.resultChan {
		if result.IsFraudulent {
			p.processFraudulentResult(result)
			continue
		}

		log.Printf(
			"✅ [Approved] Tx: %s | User: %s | Score: %d",
			result.Transaction.ID,
			result.Transaction.UserID,
			result.RiskScore,
		)
	}
}

func (p *Pipeline) processFraudulentResult(
	result domain.FraudResult,
) {
	if err := p.publishToQuarantine(result); err != nil {
		pipelinemetrics.QuarantineWriteFailures.Inc()

		log.Printf(
			"failed to write quarantine message for transaction %s: %v",
			result.Transaction.ID,
			err,
		)

		return
	}

	log.Printf(
		"\n⚠️ [ALERT] FRAUD DETECTED\n"+
			"Tx: %s | User: %s | Amount: %.2f %s\n"+
			"Score: %d | Reasons: %v\n",
		result.Transaction.ID,
		result.Transaction.UserID,
		result.Transaction.Amount,
		result.Transaction.Currency,
		result.RiskScore,
		result.Reasons,
	)
}

func (p *Pipeline) publishToQuarantine(
	result domain.FraudResult,
) error {
	payload, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal fraud result: %w", err)
	}

	message := kafka.Message{
		Key:   []byte(result.Transaction.UserID),
		Value: payload,
		Headers: []kafka.Header{
			{
				Key:   "event_type",
				Value: []byte("transaction.quarantined"),
			},
			{
				Key:   "transaction_id",
				Value: []byte(result.Transaction.ID),
			},
		},
	}

	if err := p.writer.WriteMessages(p.ctx, message); err != nil {
		return fmt.Errorf("write Kafka message: %w", err)
	}

	return nil
}