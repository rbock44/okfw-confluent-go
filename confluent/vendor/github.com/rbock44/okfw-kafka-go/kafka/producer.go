package kafka

import (
	"bytes"
	"time"
)

//SingleProducer combines a kafka producer with avro schema support
type SingleProducer struct {
	Registry    Registry
	Producer    MessageProducer
	RateLimiter *RateLimiter
}

//NewSingleProducer creates a SingleProducer
func NewSingleProducer(topic string, clientID string, registry Registry) (*SingleProducer, error) {
	producer, err := fwFactory.NewProducer(topic, clientID)
	if err != nil {
		return nil, err
	}
	SingleProducer := SingleProducer{
		Registry: registry,
		Producer: producer,
	}

	return &SingleProducer, nil
}

//SetRateLimit limits the message send to limit per second
func (p *SingleProducer) SetRateLimit(limitPerSecond int) {
	p.RateLimiter = &RateLimiter{
		StartTime:      time.Now(),
		LimitPerSecond: int64(limitPerSecond),
	}
}

//SendKeyValue sends a key value pair
func (p *SingleProducer) SendKeyValue(keySchema MessageSchema, key interface{}, valueSchema MessageSchema, value interface{}) error {
	keyBuffer := &bytes.Buffer{}
	valueBuffer := &bytes.Buffer{}
	keySchema.WriteHeader(keyBuffer)
	valueSchema.WriteHeader(valueBuffer)
	keySchema.GetEncoder().Encode(key, keyBuffer)
	valueSchema.GetEncoder().Encode(value, valueBuffer)
	err := p.Producer.SendKeyValue(keyBuffer.Bytes(), valueBuffer.Bytes())
	if err != nil {
		return err
	}
	return nil
}

//GetRateCounter returns the message counter address to monitor e.g. with the rate limiter
func (p *SingleProducer) GetRateCounter() *int64 {
	return p.Producer.GetRateCounter()
}

//Close closes the underlying consumer implementation
func (p *SingleProducer) Close() {
	p.Producer.Close()
}
