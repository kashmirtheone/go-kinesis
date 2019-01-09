package kinesis

// Message is a kinesis message.
type Message struct {
	Data      []byte
	Partition string
}
