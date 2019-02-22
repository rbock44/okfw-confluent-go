package confluent

import (
	"fmt"
	"io"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

//KafkaConsumer high level consumer wrapper
type KafkaConsumer struct {
	Topic          string
	ClientID       string
	Consumer       *kafka.Consumer
	FailedCount    int64
	IgnoredCount   int64
	DeliveredCount int64
}

//NewKafkaConsumer create a kafka consumer wrapper
func NewKafkaConsumer(topic string, clientID string) (*KafkaConsumer, error) {
	kc := KafkaConsumer{Topic: topic, ClientID: clientID}

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
func (kc *KafkaConsumer) ReadMessage(timeoutMs int, keyWriter io.Writer, valueWriter io.Writer) error {
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

//Close close the consumer
func (kc *KafkaConsumer) Close() {
	kc.Consumer.Close()
}
