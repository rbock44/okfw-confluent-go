package confluent

import (
	"github.com/rbock44/okfw-kafka-go/kafka"
)

//FrameworkFactory creates consumer and provider for the okfw-kafka-go
type FrameworkFactory struct{}

//NewConsumer creaes a new confluent consumer
func (p *FrameworkFactory) NewConsumer(topic string, clientID string) (kafka.MessageConsumer, error) {
	return newMessageConsumer(topic, clientID)
}

//NewProducer creates a new confluent provider
func (p *FrameworkFactory) NewProducer(topic string, clientID string) (kafka.MessageProducer, error) {
	return newMessageProducer(topic, clientID)
}

//NewRegistry creates a new registry
func (p *FrameworkFactory) NewRegistry() kafka.Registry {
	return kafka.NewKafkaRegistry(newSchemaResolver())
}

//NewFrameworkFactory creates the consumer and provider factory
func NewFrameworkFactory() *FrameworkFactory {
	return &FrameworkFactory{}
}
