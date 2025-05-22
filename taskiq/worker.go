package taskiq

import "context"

// Worker is the interface for the main worker logic.
type Worker interface {
	// Start starts the worker.
	// This function should block until the context is canceled or an error occurs.
	Start(ctx context.Context) error
	// Stop stops the worker gracefully.
	Stop() error
	// RegisterTask registers a task handler function.
	RegisterTask(taskName string, handlerFunc interface{}) error
	// AddMiddleware adds a middleware to the worker.
	AddMiddleware(m Middleware)
	// SetBroker sets the message broker for the worker.
	SetBroker(b Broker)
	// SetResultBackend sets the result backend for the worker.
	SetResultBackend(rb ResultBackend)
}

// Define Hook event types
const (
	EventWorkerStartup  = "WORKER_STARTUP"
	EventClientStartup  = "CLIENT_STARTUP" // Assuming client is the process that sends tasks
	EventWorkerShutdown = "WORKER_SHUTDOWN"
	EventClientShutdown = "CLIENT_SHUTDOWN" // Assuming client is the process that sends tasks
)

// HookFunc is a function type for worker lifecycle hooks.
type HookFunc func(ctx context.Context) error
