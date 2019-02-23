package confluent

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

//MessageProducer holds the kafka producer and some message counters
type MessageProducer struct {
	SuccessCount int64
	FailedCount  int64
	MessageCount int64
	Topic        string
	ClientID     string
	Producer     *kafka.Producer
}

func newMessageProducer(topic string, clientID string) (*MessageProducer, error) {
	kp := &MessageProducer{
		Topic:    topic,
		ClientID: clientID,
	}

	var err error

	kp.Producer, err = kafka.NewProducer(
		&kafka.ConfigMap{
			"bootstrap.servers":                     "localhost",
			"acks":                                  "all",
			"compression.type":                      "lz4",
			"retries":                               10000000,
			"client.id":                             clientID,
			"max.in.flight.requests.per.connection": 5,
			"enable.idempotence":                    true,
		})
	if err != nil {
		return nil, fmt.Errorf("cannot create new producer error [%#v]", err)
	}

	// Delivery report handler for produced messages
	go func() {
		for e := range kp.Producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					kp.FailedCount++
				} else {
					kp.SuccessCount++
				}
				kp.MessageCount--
			}
		}
	}()

	return kp, nil
}

//Close the producer
func (kp *MessageProducer) Close() {
	kp.Producer.Close()
}

//GetMessageCounter returns the address to the message counter
func (kp *MessageProducer) GetMessageCounter() *int64 {
	return &kp.MessageCount
}

//SendKeyValue send message with key and value
func (kp *MessageProducer) SendKeyValue(key []byte, value []byte) error {
	err := kp.Producer.Produce(
		&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &kp.Topic,
				Partition: kafka.PartitionAny,
			},
			Key:   key,
			Value: value,
		},
		nil)
	if err == nil {
		kp.SuccessCount++
	}
	kp.MessageCount++

	return err

}

//WaitUntilSendComplete wait until all messages are sent
func (kp *MessageProducer) WaitUntilSendComplete() {
	for kp.MessageCount > 0 {
		time.Sleep(time.Second * 1)
	}
}
