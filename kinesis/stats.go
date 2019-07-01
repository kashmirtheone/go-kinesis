package kinesis

import "time"

// ConsumerStats structure collects all stats.
type ConsumerStats struct {
	RecordsFailed  AverageStats
	RecordsSuccess AverageStats
}

// AverageStats holds average counters.
type AverageStats struct {
	Count       int
	MaxDuration time.Duration
	SumDuration time.Duration
}

// Add adds stat.
func (s *AverageStats) Add(duration time.Duration) {
	s.Count++
	s.SumDuration += duration
	if duration > s.MaxDuration {
		s.MaxDuration = duration
	}
}

func (s *ConsumerStats) statsHandler(event EventLog) {
	switch event.Event {
	case RecordProcessedSuccess:
		s.RecordsSuccess.Add(event.Elapse)
	case RecordProcessedFail:
		s.RecordsFailed.Add(event.Elapse)
	}
}
