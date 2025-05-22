package taskiq

import "time"

// TaskMessage represents a task to be executed.
type TaskMessage struct {
	TaskID    string
	TaskName  string
	Args      [][]byte  // Serialized arguments
	Kwargs    map[string][]byte // Serialized keyword arguments
	Timestamp time.Time
	Headers   map[string]string
	// Internal fields for broker use
	BrokerMessageID interface{} // To store message ID from broker for Ack
}
