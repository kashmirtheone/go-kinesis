package kinesis

import (
	"bytes"
	"fmt"
	"strings"
	"time"
)

const (
	// Debug LogLevel
	Debug = "debug"
	// Info LogLevel
	Info = "info"
	// Error LogLevel
	Error = "error"
)

var (
	log = dumbLogger
)

// loggerData map alias.
type loggerData map[string]interface{}

// Logger interface
type Logger func(level string, data map[string]interface{}, format string, args ...interface{})

// dumbLogger dumbest logger ever.
func dumbLogger(_ string, _ map[string]interface{}, _ string, _ ...interface{}) {}

// defaultLogger is the default logger.
func defaultLogger(level string, data map[string]interface{}, format string, args ...interface{}) {
	var now = time.Now().Format(time.RFC3339)
	var buffer bytes.Buffer

	buffer.WriteString(fmt.Sprintf("[%s] %-7s %s", now, fmt.Sprintf("[%s]", strings.ToUpper(level)), fmt.Sprintf(format, args...)))

	if len(data) == 0 {
		fmt.Println(buffer.String())
		return
	}

	buffer.WriteString(" [")

	dataStr := make([]string, 0, len(data))
	for key, value := range data {
		dataStr = append(dataStr, fmt.Sprintf("%v: %v", key, value))
	}

	buffer.WriteString(strings.Join(dataStr, ", "))
	buffer.WriteString("]")
	fmt.Println(buffer.String())
}
