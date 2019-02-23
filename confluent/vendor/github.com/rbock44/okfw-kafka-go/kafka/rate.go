package kafka

import (
	"fmt"
	"time"
)

//RateReporter calculate the message rate and report it to the logger
type RateReporter struct {
	Name               string
	Counter            *int64
	Shutdown           *bool
	Logger             func(name string, rate float64)
	RatePeriod         time.Duration
	PerSecondMultipler float64
}

//NewRateReporter create a RateReporter
func NewRateReporter(name string, counter *int64, shutdown *bool, logger func(name string, rate float64), ratePeriodMs int) (*RateReporter, error) {
	if counter == nil {
		return nil, fmt.Errorf("counter should not be nil")
	}
	if shutdown == nil {
		return nil, fmt.Errorf("shutdown should not be nil")
	}

	if logger == nil {
		return nil, fmt.Errorf("logger should not be nil")
	}

	return &RateReporter{
		Name:               name,
		Counter:            counter,
		Shutdown:           shutdown,
		Logger:             logger,
		RatePeriod:         time.Duration(ratePeriodMs) * time.Millisecond,
		PerSecondMultipler: 1000 / float64(ratePeriodMs),
	}, nil
}

//Run run the rate reporter
func (r *RateReporter) Run() {
	lastCount := *r.Counter
	for range time.NewTicker(r.RatePeriod).C {
		currentCount := *r.Counter
		rate := r.calculateRatePerSecond(currentCount, lastCount)
		lastCount = currentCount
		r.Logger(r.Name, rate)
		if *r.Shutdown {
			break
		}
	}
}

func (r *RateReporter) calculateRatePerSecond(currentCount int64, lastCount int64) float64 {
	return float64(currentCount-lastCount) * r.PerSecondMultipler
}

//RateLimiter restricts the rate to max messages per second
type RateLimiter struct {
	LastIntervalCount int64
	StartTime         time.Time
	LimitPerSecond    int64
}

func newRateLimiter(limitPerSecond int) *RateLimiter {
	return &RateLimiter{
		StartTime:      time.Now(),
		LimitPerSecond: int64(limitPerSecond),
	}
}

// Check does
// - check the rate limit
// - return the time it needs to idle in case message count is reached
func (r *RateLimiter) Check(checkTime time.Time, currentCount int64) time.Duration {
	elapsedTime := checkTime.Sub(r.StartTime)
	if elapsedTime > time.Second {
		//reset as second is over
		r.LastIntervalCount = currentCount
		r.StartTime = checkTime
		return 0
	}
	messageDiff := currentCount - r.LastIntervalCount
	if messageDiff <= r.LimitPerSecond {
		if elapsedTime < time.Second {
			return 0
		}
	}

	remainingTime := time.Second - elapsedTime
	if remainingTime > 0 {
		//sleep rest of the second
		return remainingTime
	}
	//reset as we second is over
	r.LastIntervalCount = currentCount
	r.StartTime = checkTime

	return 0
}
