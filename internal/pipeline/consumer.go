package pipeline

import (
	"log"

	"github.com/segmentio/kafka-go"

	pipelinemetrics "fraud-detection-pipeline/internal/metrics"
)

func (p *Pipeline) fetchLoop() {
	defer close(p.ingestChan)

	for {
		message, err := p.reader.FetchMessage(p.ctx)
		if err != nil {
			if p.ctx.Err() != nil {
				return
			}

			log.Printf("[fetch] error: %v", err)
			continue
		}

		select {
		case p.ingestChan <- message:
		case <-p.ctx.Done():
			return
		}

		p.updateConsumerLag()
	}
}

func (p *Pipeline) updateConsumerLag() {
	lag, err := p.reader.ReadLag(p.ctx)
	if err != nil {
		return
	}

	pipelinemetrics.ConsumerLag.Set(float64(lag))
}

// func (p *Pipeline) reportQueueDepth() {
// 	ticker := time.NewTicker(2 * time.Second)
// 	defer ticker.Stop()

// 	for {
// 		select {
// 		case <-ticker.C:
// 			pipelinemetrics.IngestQueueDepth.Set(
// 				float64(len(p.ingestChan)),
// 			)

// 		case <-p.ctx.Done():
// 			return
// 		}
// 	}
// }

func (p *Pipeline) commit(message kafka.Message) {
	p.commitMu.Lock()
	defer p.commitMu.Unlock()

	if err := p.reader.CommitMessages(p.ctx, message); err != nil {
		log.Printf(
			"failed to commit Kafka message partition=%d offset=%d: %v",
			message.Partition,
			message.Offset,
			err,
		)
	}
}