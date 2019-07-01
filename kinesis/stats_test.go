package kinesis

import (
	"github.com/golang/mock/gomock"
	"testing"
	"time"

	. "github.com/onsi/gomega"
)

func TestAverageStats_Add(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	stats := AverageStats{}

	// Act
	stats.Add(time.Second)
	stats.Add(time.Second)

	// Assert
	Expect(stats.Count).To(Equal(2))
	Expect(stats.MaxDuration).To(Equal(time.Second))
	Expect(stats.SumDuration).To(Equal(time.Second * 2))
}

func TestStats_Handler(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	stats := ConsumerStats{}

	// Act
	stats.statsHandler(EventLog{Event: RecordProcessedSuccess, Elapse: time.Second})
	stats.statsHandler(EventLog{Event: RecordProcessedFail, Elapse: time.Minute})

	// Assert
	Expect(stats.RecordsSuccess.Count).To(Equal(1))
	Expect(stats.RecordsSuccess.MaxDuration).To(Equal(time.Second))
	Expect(stats.RecordsSuccess.SumDuration).To(Equal(time.Second))
	Expect(stats.RecordsFailed.Count).To(Equal(1))
	Expect(stats.RecordsFailed.MaxDuration).To(Equal(time.Minute))
	Expect(stats.RecordsFailed.SumDuration).To(Equal(time.Minute))
}
