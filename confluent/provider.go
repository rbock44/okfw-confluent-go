package confluent

type provider struct{}

func (p *provider) NewConsumer(topic string, clientID string) (*ConfluentConsumer, error) {
	return NewConfluentConsumer(topic, clientID)
}
func (p *provider) NewProducer(topic string, clientID string) (*ConfluentProducer, error) {
	return NewConfluentProducer(topic, clientID)
}
