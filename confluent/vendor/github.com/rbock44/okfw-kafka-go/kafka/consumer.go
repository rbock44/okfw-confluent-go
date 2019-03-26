package kafka

import (
	"bytes"
	"fmt"
	"time"
)

//MessageContext contains time stamp and received message key and value
type MessageContext struct {
	Timestamp time.Time
}

//MessageHandler handles the incoming message
type MessageHandler interface {
	Handle(context *MessageContext, key []byte, value []byte)
}

//Consumer consumes messages and passes each message to a handler
type Consumer struct {
	Topic      string
	Consumer   MessageConsumer
	Handler    MessageHandler
	Registry   Registry
	Shutdown   bool
	PollTimeMs int
}

//NewConsumer creates a consumer
func NewConsumer(topic string, clientID string, registry Registry, pollTimeMs int, handler MessageHandler) (*Consumer, error) {
	if handler == nil {
		return nil, fmt.Errorf("consumer message handler missing")
	}

	consumerImpl, err := fwFactory.NewConsumer(topic, clientID, handler)
	if err != nil {
		return nil, err
	}
	return &Consumer{
		Topic:      topic,
		Consumer:   consumerImpl,
		Registry:   registry,
		Shutdown:   false,
		PollTimeMs: pollTimeMs,
	}, nil
}

//ExtractSchema extracts the schema version and decodes the key and value
func (c *Consumer) ExtractSchema(context *MessageContext, key []byte, value []byte) (interface{}, interface{}, error) {
	keyBuffer := bytes.NewBuffer(key)
	valueBuffer := bytes.NewBuffer(value)

	if keyBuffer.Len() == 0 {
		//no message poll interval expired
		return nil, nil, nil
	}

	schemaID, err := readSchemaID(keyBuffer)
	if err != nil {
		return nil, nil, err
	}
	keySchema, err := c.Registry.GetSchemaByID(int(schemaID))
	if err != nil {
		return nil, nil, err
	}

	decoder := keySchema.GetDecoder()
	if decoder == nil {
		return nil, nil, fmt.Errorf("no key decoder")
	}

	decodedKey, err := decoder.Decode(keyBuffer)
	if err != nil {
		return nil, nil, err
	}

	id, err := readSchemaID(valueBuffer)
	if err != nil {
		return nil, nil, err
	}
	valueSchema, err := c.Registry.GetSchemaByID(int(id))
	if err != nil {
		return nil, nil, err
	}

	decoder = valueSchema.GetDecoder()
	if decoder == nil {
		return key, nil, fmt.Errorf("no value decoder")
	}
	decodedValue, err := decoder.Decode(valueBuffer)

	return decodedKey, decodedValue, err
}

//RunBacklogReporter runs the back log reporter should be run in a go routine
func (c *Consumer) RunBacklogReporter(intervalMs int) {
	br, err := NewBacklogReporter(
		c.Topic,
		c,
		func(name string, count int, err error) {
			logger.Infof("report backlog [%s] [%d]", name, count)
		},
		&c.Shutdown,
		intervalMs)
	if err == nil {
		br.Run()
	}
}

//RunRateReporter starts rate reporter should be run in a go routine
func (c *Consumer) RunRateReporter(intervalMs int) {
	br, err := NewRateReporter(
		c.Topic,
		c.Consumer.GetMessageCounter(),
		&c.Shutdown,
		func(name string, rate float64) {
			logger.Infof("report rate [%s] [%4.2f]\n", name, rate)
		},
		intervalMs)
	if err == nil {
		br.Run()
	}
}

//GetRateCounter returns the message counter address to monitor e.g. with the rate limiter
func (c *Consumer) GetRateCounter() *int64 {
	return c.Consumer.GetMessageCounter()
}

//GetBacklog returns the messages left in the topic
func (c *Consumer) GetBacklog() (int, error) {
	return c.Consumer.GetBacklog()
}

//Close closes the underlying consumer implementation
func (c *Consumer) Close() {
	if c.Consumer != nil {
		logger.Debugf("Consumer->Close\n")
		c.Consumer.Close()
	}
	//stop limiter and reporter
	c.Shutdown = true
}

//Process receive messages and dispatch to message handler and error handler until shutdown flag is true
func (c *Consumer) Process() {
	for {
		c.Consumer.Process(c.PollTimeMs)
		if c.Shutdown {
			return
		}
	}
}
