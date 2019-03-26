package logapi

//Logger abstracts the logger
type Logger interface {
	IsLevelInfo() bool
	IsLevelDebug() bool
	Errorf(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Debugf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
}
