package confluent

import (
	"bytes"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	okfwkafka "github.com/rbock44/okfw-kafka-go/kafka"
)

//MessageConsumer high level consumer wrapper
type MessageConsumer struct {
	Topic          string
	ClientID       string
	Consumer       *kafka.Consumer
	FailedCount    int64
	IgnoredCount   int64
	DeliveredCount int64
	Handler        okfwkafka.MessageHandler
}

func newMessageConsumer(topic string, clientID string) (*MessageConsumer, error) {
	kc := MessageConsumer{Topic: topic, ClientID: clientID}

	var err error
	kc.Consumer, err = kafka.NewConsumer(
		&kafka.ConfigMap{
			"bootstrap.servers":  "localhost",
			"group.id":           "segmenter",
			"session.timeout.ms": 6000,
			"auto.offset.reset":  "earliest",
		})
	if err != nil {
		return nil, fmt.Errorf("cannot create kafka consumer error [%#v]", err)
	}

	err = kc.Consumer.Subscribe(kc.Topic, nil)
	if err != nil {
		return nil, fmt.Errorf("cannot subcribe to topic [%s] error [%#v]", kc.Topic, err)
	}

	return &kc, nil
}

//Process poll the consumer and call the message handler
func (kc *MessageConsumer) Process(timeoutMs int) error {
	ev := kc.Consumer.Poll(timeoutMs)
	switch e := ev.(type) {
	case *kafka.Message:
		kc.DeliveredCount++
		keyBuffer := &bytes.Buffer{}
		keyBuffer.Write(e.Key)
		valueBuffer := &bytes.Buffer{}
		valueBuffer.Write(e.Value)
		context := &okfwkafka.MessageContext{
			Timestamp: e.Timestamp,
		}
		kc.Handler.Handle(context, keyBuffer.Bytes(), valueBuffer.Bytes())
		return nil
	case kafka.Error:
		kc.FailedCount++
		return fmt.Errorf("consumer poll error [%#v]", e)
	case nil:
		//polling just indicated that there is no message
		return nil
	default:
		//other kafka message types are ignored
		kc.IgnoredCount++
		return nil
	}
}

//GetMessageCounter get the message counter
func (kc *MessageConsumer) GetMessageCounter() *int64 {
	return &kc.DeliveredCount
}

//Close close the consumer
func (kc *MessageConsumer) Close() {
	kc.Consumer.Close()
}
