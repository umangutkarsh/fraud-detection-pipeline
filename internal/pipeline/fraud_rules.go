package pipeline

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"fraud-detection-pipeline/internal/domain"

	pipelinemetrics "fraud-detection-pipeline/internal/metrics"
)

const (
	highValueThreshold = 10_000.0

	velocityWindow = 2 * time.Second

	slidingWindowAML = 2 * time.Minute

	crossBorderMaxTransactions = 5
	crossBorderMinimumAmount   = 10_000.0
)

func (p *Pipeline) 	evaluateFraud(
	ctx context.Context,
	transaction domain.Transaction,
) (domain.FraudResult, error) {
	reasons := make([]string, 0)
	riskScore := 0

	if transaction.Amount > highValueThreshold {
		riskScore += 40
		reasons = append(
			reasons,
			"High value transaction (>10k)",
		)
	}

	velocityBreached, err := p.checkVelocity(ctx, transaction)
	if err != nil {
		return domain.FraudResult{}, err
	}

	if velocityBreached {
		riskScore += 50
		reasons = append(
			reasons,
			"Velocity limit breached: multiple transactions within 2 seconds",
		)
	}

	requiresCrossBorderCheck :=
		transaction.Country != "" &&
			transaction.Country != "GB" &&
			transaction.Amount >= crossBorderMinimumAmount

	if requiresCrossBorderCheck {
		windowBreached, err := p.checkCrossBorderWindow(
			ctx,
			transaction,
		)
		if err != nil {
			return domain.FraudResult{}, err
		}

		if windowBreached {
			riskScore += 60

			reasons = append(
				reasons,
				fmt.Sprintf(
					"Cross-border velocity: more than %d transactions over £%.0f within %s",
					crossBorderMaxTransactions,
					crossBorderMinimumAmount,
					slidingWindowAML,
				),
			)
		}
	}

	result := domain.FraudResult{
		Transaction:  transaction,
		IsFraudulent: riskScore >= 50,
		RiskScore:    riskScore,
		Reasons:      reasons,
	}

	if result.IsFraudulent {
		pipelinemetrics.FraudTotal.Inc()
	}

	return result, nil
}

func (p *Pipeline) checkVelocity(
	ctx context.Context,
	transaction domain.Transaction,
) (bool, error) {
	key := "velocity:" + transaction.UserID
	currentTimestamp := transaction.Timestamp.Format(time.RFC3339Nano)

	previousTimestamp, err := p.rdb.SetArgs(
		ctx,
		key,
		currentTimestamp,
		redis.SetArgs{
			TTL: velocityWindow * 5,
			Get: true,
		},
	).Result()

	if err == redis.Nil {
		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf("velocity Redis operation: %w", err)
	}

	lastSeen, err := time.Parse(
		time.RFC3339Nano,
		previousTimestamp,
	)
	if err != nil {
		return false, fmt.Errorf(
			"parse previous transaction timestamp: %w",
			err,
		)
	}

	timeDifference := transaction.Timestamp.Sub(lastSeen)

	return timeDifference >= 0 &&
		timeDifference < velocityWindow, nil
}

func (p *Pipeline) checkCrossBorderWindow(
	ctx context.Context,
	transaction domain.Transaction,
) (bool, error) {
	key := "window:" + transaction.UserID
	cutoff := transaction.Timestamp.Add(-slidingWindowAML)

	redisPipeline := p.rdb.TxPipeline()

	redisPipeline.ZRemRangeByScore(
		ctx,
		key,
		"-inf",
		fmt.Sprintf("%d", cutoff.UnixMilli()),
	)

	redisPipeline.ZAdd(
		ctx,
		key,
		redis.Z{
			Score:  float64(transaction.Timestamp.UnixMilli()),
			Member: transaction.ID,
		},
	)

	redisPipeline.Expire(
		ctx,
		key,
		slidingWindowAML+time.Minute,
	)

	transactionCount := redisPipeline.ZCard(ctx, key)

	if _, err := redisPipeline.Exec(ctx); err != nil {
		return false, fmt.Errorf(
			"execute cross-border Redis pipeline: %w",
			err,
		)
	}

	return transactionCount.Val() >
		int64(crossBorderMaxTransactions), nil
}