package pipeline

import (
	"errors"
	"fmt"
)

func (p *Pipeline) Stop() error {
	// Stop fetching new Kafka messages.
	p.cancel()

	// Wait for workers to finish messages already received through
	// ingestChan. fetchLoop closes ingestChan when cancellation is noticed.
	p.workerWG.Wait()

	// No workers will write more results.
	close(p.resultChan)

	// Wait until resultProcessor drains resultChan.
	p.resultWG.Wait()

	var shutdownErrors []error

	if err := p.reader.Close(); err != nil {
		shutdownErrors = append(
			shutdownErrors,
			fmt.Errorf("close Kafka reader: %w", err),
		)
	}

	if err := p.writer.Close(); err != nil {
		shutdownErrors = append(
			shutdownErrors,
			fmt.Errorf("close Kafka writer: %w", err),
		)
	}

	if err := p.rdb.Close(); err != nil {
		shutdownErrors = append(
			shutdownErrors,
			fmt.Errorf("close Redis client: %w", err),
		)
	}

	return errors.Join(shutdownErrors...)
}