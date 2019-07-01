package kinesis

import "time"

const (
	// StreamCheckedTriggered event
	StreamCheckedTriggered = "stream_checker_triggered"
	// ShardManagerTriggered event
	ShardManagerTriggered = "shard_manager_triggered"
	// ShardIteratorTriggered event
	ShardIteratorTriggered = "shard_iterator_triggered"
	// RecordProcessedSuccess event
	RecordProcessedSuccess = "record_processed_success"
	// RecordProcessedFail event
	RecordProcessedFail = "record_processed_fail"
)

// EventLog structure contains all information of what happened.
type EventLog struct {
	Event  string
	Elapse time.Duration
}

// EventLogger callback that is called every event.
type EventLogger interface {
	LogEvent(event EventLog)
}

type dumbEventLogger struct {
}

func (l *dumbEventLogger) LogEvent(_ EventLog) {}
