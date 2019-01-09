package kinesis

const (
	// Debug LogLevel
	Debug = "debug"
	// Info LogLevel
	Info = "info"
	// Error LogLevel
	Error = "error"
)

// LoggerData map alias.
type LoggerData map[string]interface{}

// Logger interface
type Logger func(level string, msg string, data map[string]interface{})

// DumbLogger dumbest logger ever.
func DumbLogger(_ string, _ string, _ map[string]interface{}) {}
