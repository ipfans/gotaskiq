package taskiq

import "context"

// Broker is the interface for message brokers.
type Broker interface {
	// DeclareQueue declares a queue.
	DeclareQueue(ctx context.Context, queueName string) error
	// PublishMessage publishes a message to a queue.
	PublishMessage(ctx context.Context, queueName string, message *TaskMessage) error
	// ConsumeMessages consumes messages from a queue.
	// It should call the provided handler function for each message.
	// This function should block until the context is canceled.
	ConsumeMessages(ctx context.Context, queueName string, handler func(ctx context.Context, message *TaskMessage) error) error
	// Close closes the broker connection.
	Close() error
	// Ack acknowledges a message.
	Ack(ctx context.Context, queueName string, message *TaskMessage) error
}
