package kafka

import (
	"bytes"
	"fmt"
	"time"
)

//SingleProducer combines a kafka producer with avro schema support
type SingleProducer struct {
	Topic        string
	Registry     Registry
	producer     MessageProducer
	rateLimiter  *RateLimiter
	Shutdown     bool
	MessageCount int64
}

//NewSingleProducer creates a SingleProducer
func NewSingleProducer(topic string, clientID string, registry Registry) (*SingleProducer, error) {
	producer, err := fwFactory.NewProducer(topic, clientID)
	if err != nil {
		return nil, err
	}
	SingleProducer := SingleProducer{
		Topic:    topic,
		Registry: registry,
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
func (p *SingleProducer) SendKeyValue(keySchema MessageSchema, key interface{}, valueSchema MessageSchema, value interface{}) error {
	keyBuffer := &bytes.Buffer{}
	valueBuffer := &bytes.Buffer{}
	keySchema.WriteHeader(keyBuffer)
	valueSchema.WriteHeader(valueBuffer)
	keySchema.GetEncoder().Encode(key, keyBuffer)
	valueSchema.GetEncoder().Encode(value, valueBuffer)
	err := p.producer.SendKeyValue(keyBuffer.Bytes(), valueBuffer.Bytes())
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
			fmt.Printf("report rate [%s] [%4.2f]\n", p.Topic, rate)
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
