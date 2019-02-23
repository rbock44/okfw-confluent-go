package kafka

import (
	"bytes"
	"fmt"
)

//SingleConsumer only supports ReadMessage
type SingleConsumer struct {
	Topic    string
	Consumer MessageConsumer
	Registry Registry
	Shutdown bool
}

//BulkConsumer supports bulk reads with message handlere and error handler
type BulkConsumer struct {
	SingleConsumer *SingleConsumer
	MessageHandler func(key interface{}, value interface{}, err error)
	Registry       Registry
	PollTimeMs     int
	Shutdown       *bool
}

//NewSingleConsumer creates a SingleConsumer
func NewSingleConsumer(topic string, clientID string, registry Registry) (*SingleConsumer, error) {
	consumerImpl, err := fwFactory.NewConsumer(topic, clientID)
	if err != nil {
		return nil, err
	}
	sc := SingleConsumer{
		Topic:    topic,
		Consumer: consumerImpl,
		Registry: registry,
		Shutdown: false,
	}

	return &sc, nil
}

//NewBulkConsumer creates a new BulkConsumer
func NewBulkConsumer(
	topic string,
	clientID string,
	registry Registry,
	messageHandler func(key interface{}, value interface{}, err error),
	pollTimeMs int,
	shutdown *bool) (*BulkConsumer, error) {
	if messageHandler == nil {
		return nil, fmt.Errorf("messageHandler is nil")
	}
	SingleConsumer, err := NewSingleConsumer(topic, clientID, registry)
	if err != nil {
		return nil, err
	}
	bulkConsumer := BulkConsumer{
		MessageHandler: messageHandler,
		SingleConsumer: SingleConsumer,
		PollTimeMs:     pollTimeMs,
		Shutdown:       shutdown,
	}

	return &bulkConsumer, nil
}

//ReadMessage read a message and process it
func (c *SingleConsumer) ReadMessage(shutdownCheckInterfaceMs int) (interface{}, interface{}, error) {
	keyBuffer := &bytes.Buffer{}
	valueBuffer := &bytes.Buffer{}
	err := c.Consumer.ReadMessage(shutdownCheckInterfaceMs, keyBuffer, valueBuffer)
	if err != nil {
		return nil, nil, err
	}

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

	key, err := decoder.Decode(keyBuffer)
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
	value, err := decoder.Decode(valueBuffer)

	return key, value, err
}

//RunBacklogReporter runs the go routine for the backlog reporter
func (c *SingleConsumer) RunBacklogReporter(intervalMs int) {
	br, err := NewBacklogReporter(
		c.Topic,
		c,
		func(name string, count int, err error) {
			fmt.Printf("report backlog [%s] [%d]", name, count)
		},
		&c.Shutdown,
		intervalMs)
	if err == nil {
		br.Run()
	}
}

//RunRateReporter starts a go routine with the rate reporter
func (c *SingleConsumer) RunRateReporter(intervalMs int) {
	br, err := NewRateReporter(
		c.Topic,
		c.Consumer.GetRateCounter(),
		&c.Shutdown,
		func(name string, rate float64) {
			fmt.Printf("report rate [%s] [%4.2f]", name, rate)
		},
		intervalMs)
	if err == nil {
		br.Run()
	}
}

//GetRateCounter returns the message counter address to monitor e.g. with the rate limiter
func (c *SingleConsumer) GetRateCounter() *int64 {
	return c.Consumer.GetRateCounter()
}

//GetBacklog returns the messages left in the topic
func (c *SingleConsumer) GetBacklog() (int, error) {
	return c.GetBacklog()
}

//Close closes the underlying consumer implementation
func (c *SingleConsumer) Close() {
	c.Consumer.Close()
	//stop limiter and reporter
	c.Shutdown = true
}

//Process receive messages and dispatch to message handler and error handler until shutdown flag is true
func (c *BulkConsumer) Process() {
	for {
		key, value, err := c.SingleConsumer.ReadMessage(c.PollTimeMs)
		if err != nil {
			c.MessageHandler(key, value, err)
		} else {
			if key != nil {
				c.MessageHandler(key, value, err)
			}
		}

		if *c.Shutdown {
			return
		}
	}
}

//GetBacklog returns the messages left in the topic
func (c *BulkConsumer) GetBacklog() (int, error) {
	return c.SingleConsumer.GetBacklog()
}

//RunBacklogReporter runs the go routine for the backlog reporter
func (c *BulkConsumer) RunBacklogReporter(intervalMs int) {
	c.SingleConsumer.RunBacklogReporter(intervalMs)
}

//RunRateReporter runs the go routine for the rate reporter
func (c *BulkConsumer) RunRateReporter(intervalMs int) {
	c.SingleConsumer.RunRateReporter(intervalMs)
}

//Close closes the bulk consumer and makes sure the underlying simple consumer is closed
func (c *BulkConsumer) Close() {
	if c.SingleConsumer != nil {
		c.SingleConsumer.Close()
	}
}
