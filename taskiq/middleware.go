package taskiq

import "context"

// MiddlewareContext holds context for middleware execution.
type MiddlewareContext struct {
	Task *TaskMessage
	// Potentially add more fields like worker instance, etc.
}

// Middleware is an interface for middleware components.
// Middlewares can inspect and modify tasks, or perform actions before/after task execution.
type Middleware interface {
	// BeforeProcessMessage is called before a message is processed by the worker.
	// It can modify the task message or context.
	// Returning an error will prevent the task from being processed.
	BeforeProcessMessage(ctx context.Context, mc *MiddlewareContext) error
	// AfterProcessMessage is called after a message has been processed by the worker.
	// It's called even if the task processing failed.
	// The err argument contains the error from task processing (if any).
	AfterProcessMessage(ctx context.Context, mc *MiddlewareContext, err error) error
	// BeforeSendResult is called before a result is sent to the result backend.
	// It can modify the result message.
	// Returning an error will prevent the result from being sent.
	BeforeSendResult(ctx context.Context, mc *MiddlewareContext, result *ResultMessage) error
	// AfterSendResult is called after a result has been sent to the result backend.
	// It's called even if sending the result failed.
	// The err argument contains the error from sending the result (if any).
	AfterSendResult(ctx context.Context, mc *MiddlewareContext, result *ResultMessage, err error) error
}
