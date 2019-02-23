package confluent

//FrameworkFactory creates consumer and provider for the okfw-kafka-go
type FrameworkFactory struct{}

//NewConsumer creaes a new confluent consumer
func (p *FrameworkFactory) NewConsumer(topic string, clientID string) (*ConfluentConsumer, error) {
	return NewConfluentConsumer(topic, clientID)
}

//NewProducer creates a new confluent provider
func (p *FrameworkFactory) NewProducer(topic string, clientID string) (*ConfluentProducer, error) {
	return NewConfluentProducer(topic, clientID)
}

//NewFrameworkFactory creates the consumer and provider factory
func NewFrameworkFactory() *FrameworkFactory {
	return &FrameworkFactory{}
}
