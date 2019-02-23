package confluent

import (
	"fmt"
	"io"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

//MessageConsumer high level consumer wrapper
type MessageConsumer struct {
	Topic          string
	ClientID       string
	Consumer       *kafka.Consumer
	FailedCount    int64
	IgnoredCount   int64
	DeliveredCount int64
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

//ReadMessage read an event in case no event is available return nil
func (kc *MessageConsumer) ReadMessage(timeoutMs int, keyWriter io.Writer, valueWriter io.Writer) error {
	ev := kc.Consumer.Poll(timeoutMs)
	switch e := ev.(type) {
	case *kafka.Message:
		kc.DeliveredCount++
		keyWriter.Write(e.Key)
		valueWriter.Write(e.Value)
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
