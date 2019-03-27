package confluent

import (
	"github.com/rbock44/okfw-logapi-go/logapi"
)

//Logger logger implementation
var logger logapi.Logger

//SetLogger sets the logger implementation
func SetLogger(logImpl logapi.Logger) {
	logger = logImpl
}
