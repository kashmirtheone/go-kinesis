package kinesis

const (
	// LevelDebug LogLevel
	LevelDebug = "debug"
	// LevelInfo LogLevel
	LevelInfo = "info"
	// LevelError LogLevel
	LevelError = "error"
)

// loggerData map alias.
type loggerData map[string]interface{}

// Logger interface
type Logger interface {
	Log(level string, data map[string]interface{}, format string, args ...interface{})
}

type dumbLogger struct {
}

// dumbLogger dumbest logger ever.
func (l *dumbLogger) Log(_ string, _ map[string]interface{}, _ string, _ ...interface{}) {}
