package confluent

import (
	"fmt"

	schemaregistry "github.com/landoop/schema-registry"
	"github.com/rbock44/okfw-kafka-go/kafka"
)

var schemaClient *schemaregistry.Client

func getKafkaSchemaClient() *schemaregistry.Client {
	var err error
	if schemaClient == nil {
		schemaClient, err = schemaregistry.NewClient(schemaregistry.DefaultUrl)
		if err != nil {
			panic(fmt.Sprintf("schema registry client creation error [%v]\n", err))
		}
	}
	return schemaClient
}

type schemaClientType struct{}

func newSchemaResolver() kafka.SchemaResolver {
	return &schemaClientType{}
}

func (c *schemaClientType) GetSchemaBySubject(subject string, version int) (schemaID int, err error) {
	schema, err := getKafkaSchemaClient().GetSchemaBySubject(subject, version)
	if err != nil {
		return 0, err
	}
	return schema.ID, nil
}

func (c *schemaClientType) RegisterNewSchema(subject string, content string) (schemaID int, err error) {
	id, err := getKafkaSchemaClient().RegisterNewSchema(subject, content)
	if err != nil {
		return 0, err
	}
	return id, nil
}
