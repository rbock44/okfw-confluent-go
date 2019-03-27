package kafka

import (
	"time"
)

//SingleProducer combines a kafka producer with avro schema support
type SingleProducer struct {
	Topic        string
	producer     MessageProducer
	rateLimiter  *RateLimiter
	Shutdown     bool
	MessageCount int64
}

//NewSingleProducer creates a SingleProducer
func NewSingleProducer(topic string, clientID string) (*SingleProducer, error) {
	producer, err := fwFactory.NewProducer(topic, clientID)
	if err != nil {
		return nil, err
	}
	SingleProducer := SingleProducer{
		Topic:    topic,
		producer: producer,
		Shutdown: false,
	}

	return &SingleProducer, nil
}

//SetRateLimit limits the message send to limit per second
func (p *SingleProducer) SetRateLimit(limitPerSecond int) {
	p.rateLimiter = &RateLimiter{
		StartTime:      time.Now(),
		LimitPerSecond: int64(limitPerSecond),
	}
}

//SendKeyValue sends a key value pair
func (p *SingleProducer) SendKeyValue(key []byte, value []byte) error {
	err := p.producer.SendKeyValue(key, value)
	if err != nil {
		return err
	}
	p.MessageCount++
	if p.rateLimiter != nil {
		wait := p.rateLimiter.Check(time.Now(), p.MessageCount)
		if wait > 0 {
			time.Sleep(wait)
		}
	}

	return nil
}

//RunRateReporter starts the rate reporter should be run in a go routine
func (p *SingleProducer) RunRateReporter(intervalMs int) {
	prr, err := NewRateReporter(
		p.Topic,
		&p.MessageCount,
		&p.Shutdown,
		func(name string, rate float64) {
			logger.Infof("report rate [%s] [%4.2f]\n", p.Topic, rate)
		},
		intervalMs)
	if err == nil {
		prr.Run()
	}
}

//Close closes the underlying consumer implementation
func (p *SingleProducer) Close() {
	p.Shutdown = true
	p.producer.Close()
}
