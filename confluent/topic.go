package confluent

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

//CreateCompactTopic creates a topic that is used as state store
func CreateCompactTopic(topic string, numPartitions int, replicationFactor int) error {
	adminClient, err := kafka.NewAdminClient(
		&kafka.ConfigMap{
			"bootstrap.servers": "localhost",
		},
	)
	if err != nil {
		return fmt.Errorf("cannot create admin client from producer [%#v]", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result, err := adminClient.CreateTopics(
		ctx,
		[]kafka.TopicSpecification{
			kafka.TopicSpecification{
				Topic: topic,
				Config: map[string]string{
					"cleanup.policy": "compact",
				},
				ReplicationFactor: replicationFactor,
				NumPartitions:     numPartitions,
			},
		})
	if err != nil && result != nil {
		if result[0].Error.Code() == kafka.ErrTopicAlreadyExists {
			return nil
		}
	}

	return err
}
