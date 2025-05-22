package taskiq

import "time"

// ResultMessage represents the result of a task.
type ResultMessage struct {
	TaskID    string
	Status    string // e.g., "SUCCESS", "FAILURE"
	Result    []byte // Serialized result data
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
