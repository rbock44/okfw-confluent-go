package kafka

import (
	"bytes"
	"time"
)

//SimpleProducer combines a kafka producer with avro schema support
type SimpleProducer struct {
	Registry    Registry
	Producer    MessageProducer
	RateLimiter *RateLimiter
}

//NewSimpleProducer creates a SimpleProducer
func NewSimpleProducer(topic string, clientID string, registry Registry) (*SimpleProducer, error) {
	producer, err := fwFactory.NewProducer(topic, clientID)
	if err != nil {
		return nil, err
	}
	simpleProducer := SimpleProducer{
		Registry: registry,
		Producer: producer,
	}

	return &simpleProducer, nil
}

//SetRateLimit limits the message send to limit per second
func (p *SimpleProducer) SetRateLimit(limitPerSecond int) {
	p.RateLimiter = &RateLimiter{
		StartTime:      time.Now(),
		LimitPerSecond: int64(limitPerSecond),
	}
}

//SendKeyValue sends a key value pair
func (p *SimpleProducer) SendKeyValue(keySchema MessageSchema, key interface{}, valueSchema MessageSchema, value interface{}) error {
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

//GetCounter returns the message counter address to monitor e.g. with the rate limiter
func (p *SimpleProducer) GetCounter() *int64 {
	return p.Producer.GetCounter()
}
