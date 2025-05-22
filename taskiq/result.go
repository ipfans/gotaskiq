package taskiq

import "time"

// ResultMessage represents the result of a task.
// The ResultMessage struct itself is always JSON encoded when stored by a result backend.
type ResultMessage struct {
	TaskID    string
	Status    string // e.g., "SUCCESS", "FAILURE"
	// Result contains the pre-encoded result data.
	// The encoding format is specified by ContentEncoding.
	Result    json.RawMessage
	// ContentEncoding specifies the encoding used for the Result field.
	// e.g., "application/json", "application/msgpack", "application/cbor"
	// This field is set by the worker when creating the result message.
	ContentEncoding string
	Timestamp time.Time
	Error     string // Error message if the task failed
}

// ResultBackend is the interface for storing and retrieving task results.
type ResultBackend interface {
	// SetResult sets the result for a given task ID.
	SetResult(taskID string, result *ResultMessage) error
	// GetResult retrieves the result for a given task ID.
	GetResult(taskID string) (*ResultMessage, error)
	// Close closes the result backend connection.
	Close() error
}
