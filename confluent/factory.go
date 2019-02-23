package confluent

import (
	"fmt"

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
func (p *FrameworkFactory) NewRegistry() (kafka.Registry, error) {
	_, err := getKafkaSchemaClient().Subjects()
	if err != nil {
		return nil, fmt.Errorf("cannot query subjects on kafka registry [%s]", err.Error())
	}
	return kafka.NewSchemaRegistry(), nil
}

//NewFrameworkFactory creates the consumer and provider factory
func NewFrameworkFactory() *FrameworkFactory {
	return &FrameworkFactory{}
}
