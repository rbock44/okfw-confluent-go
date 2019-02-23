package kafka

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"time"

	schemaregistry "github.com/landoop/schema-registry"
)

//AvroSchema handles schema lookup and codec
type AvroSchema struct {
	ID      uint32
	Version int
	Subject string
	Content string
	Decoder Decoder
	Encoder Encoder
}

//GetID get the schema ID
func (a AvroSchema) GetID() int {
	return int(a.ID)
}

//GetDecoder get the schema decoder
func (a AvroSchema) GetDecoder() Decoder {
	return a.Decoder
}

//GetEncoder get the schema encoder
func (a AvroSchema) GetEncoder() Encoder {
	return a.Encoder
}

//WriteHeader writes the encoding schema id with magic byte
func (a AvroSchema) WriteHeader(writer io.Writer) {
	header := make([]byte, 5)
	binary.BigEndian.PutUint32(header[1:5], uint32(a.ID))
	writer.Write(header)
}

//SchemaResolver looks up the schema in a remote registry
type SchemaResolver interface {
	GetSchemaBySubject(subject string, version int) (schemaID int, err error)
	RegisterNewSchema(subject string, content string) (id int, err error)
}

//KafkaRegistry contains all registered schema
type KafkaRegistry struct {
	SchemasByID   map[int]*AvroSchema
	SchemasByName map[string]*AvroSchema
	Resolver      SchemaResolver
}

//NewKafkaRegistry creates a kafka schema registry
func NewKafkaRegistry(resolver SchemaResolver) *KafkaRegistry {
	return &KafkaRegistry{
		Resolver:      resolver,
		SchemasByID:   map[int]*AvroSchema{},
		SchemasByName: map[string]*AvroSchema{},
	}
}

//Lookup lookup schema in kafka cluster
func (s KafkaRegistry) Lookup(subject string, version int) (*AvroSchema, error) {
	schemaID, err := s.Resolver.GetSchemaBySubject(subject, version)
	if err != nil {
		return nil, fmt.Errorf(fmt.Sprintf("schema registry lookup error [%#v]\n", err))
	}
	localSchema := &AvroSchema{
		ID:      uint32(schemaID),
		Subject: subject,
		Version: version,
	}
	s.SchemasByID[schemaID] = localSchema
	s.SchemasByName[subject] = localSchema

	return localSchema, nil
}

//GetSchemaByID gets the schema by id
func (s KafkaRegistry) GetSchemaByID(id int) (MessageSchema, error) {
	ms := s.SchemasByID[id]
	if ms == nil {
		return nil, fmt.Errorf("schema id not found [%d]", id)
	}
	return ms, nil
}

//GetSchemaByName gets the schema by name
func (s KafkaRegistry) GetSchemaByName(name string) (MessageSchema, error) {
	ms := s.SchemasByName[name]
	if ms == nil {
		return nil, fmt.Errorf("schema subject not found [%s]", name)
	}
	return ms, nil
}

//Register schema with registry
func (s KafkaRegistry) Register(subject string, version int, schemaPath string, decoder Decoder, encoder Encoder) (MessageSchema, error) {
	retryCount := 5
	retryTimeMs := 1000
	schemaContent, err := ioutil.ReadFile(schemaPath)
	if err != nil {
		return nil, fmt.Errorf("cannot read schema file [%s]", schemaPath)
	}
	var id int
	for i := 0; i <= retryCount; i++ {
		id, err = s.Resolver.RegisterNewSchema(subject, string(schemaContent))
		if err == nil {
			break
		}
		time.Sleep(time.Millisecond * time.Duration(retryTimeMs))
	}
	if err != nil {
		return nil, fmt.Errorf("cannot register avro schema [%#v]", err)
	}
	localSchema := &AvroSchema{
		ID:      uint32(id),
		Subject: subject,
		Version: version,
		Decoder: decoder,
		Encoder: encoder,
	}
	s.SchemasByID[id] = localSchema
	s.SchemasByName[subject] = localSchema
	return localSchema, nil
}

type schemaClientType struct{}

//NewKafkaSchemaResolver creates a schema resolver calling the Kafka Cluster
func NewKafkaSchemaResolver() SchemaResolver {
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
