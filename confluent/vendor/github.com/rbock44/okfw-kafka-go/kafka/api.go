package kafka

import (
	"io"

	"github.com/rbock44/okfw-log-go/logapi"
)

var fwFactory Provider
var logger logapi.Logger

//SetLogger sets the logger implementation
func SetLogger(logImpl logapi.Logger) {
	logger = logImpl
}

//SetFrameworkFactory sets the provider that generates the consumer and producers of the used kafka framework
func SetFrameworkFactory(provider Provider) {
	fwFactory = provider
}

//go:generate mockgen -source api.go -destination mock_api_test.go -package kafka MessageConsumer,MessageProducer,MessageSchema,Decoder,Encoder,Registry,Provider

//MessageSchema hold the schema of the message
type MessageSchema interface {
	GetID() int
	GetDecoder() Decoder
	GetEncoder() Encoder
	WriteHeader(writer io.Writer)
}

//Decoder decodes the binary stream to a value
type Decoder interface {
	Decode(reader io.Reader) (value interface{}, err error)
}

//Encoder encodes the value to a binary stream
type Encoder interface {
	Encode(value interface{}, writer io.Writer)
}

//Registry abstracts the schema registry
type Registry interface {
	Register(name string, version int, schemaFile string, decoder Decoder, encoder Encoder) (MessageSchema, error)
	GetSchemaByName(name string) (MessageSchema, error)
	GetSchemaByID(id int) (MessageSchema, error)
	DecodeMessage(context *MessageContext, key []byte, value []byte) (interface{}, interface{}, error)
}

//MessageConsumer interface to abstract message receiving and make writing tests simpler
type MessageConsumer interface {
	Process(pollTimeoutMs int) error
	GetMessageCounter() *int64
	GetBacklog() (backlog int, err error)
	Close()
}

//MessageProducer interface to abstract message sending and make writing tests simpler
type MessageProducer interface {
	SendKeyValue(key []byte, value []byte) error
	Close()
}

//BacklogRetriever retrieves the backlog of a consumer
type BacklogRetriever interface {
	GetBacklog() (backlog int, err error)
}

//DeliveredCounter delivers the address of the delivered messages (send or received)
type DeliveredCounter interface {
	GetDeliveredCounter() *int64
}

//Provider creates kafa consumer producer based on an implementation
type Provider interface {
	NewConsumer(topic string, clientID string, handler MessageHandler) (MessageConsumer, error)
	NewProducer(topic string, clientID string) (MessageProducer, error)
	NewSchemaResolver() (SchemaResolver, error)
}
