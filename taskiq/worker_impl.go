package taskiq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"reflect"
	"sync"
	"time"
)

// DefaultWorker implements the Worker interface.
type DefaultWorker struct {
	broker         Broker
	resultBackend  ResultBackend
	taskHandlers   map[string]reflect.Value // Store reflect.Value of handler funcs
	middlewares    []Middleware
	hooks          map[string][]HookFunc
	logger         *log.Logger
	workerCtx      context.Context
	cancelWorker   context.CancelFunc
	queueName      string
	concurrency    int
	stopOnce       sync.Once // To ensure Stop logic runs once
	processWg      sync.WaitGroup // To wait for task processing goroutines
	taskSerializer TaskSerializer // Added for task (de)serialization
}

// WorkerOptions holds configuration for the DefaultWorker.
type WorkerOptions struct {
	QueueName      string
	Concurrency    int
	Broker         Broker
	ResultBackend  ResultBackend
	Logger         *log.Logger
	TaskSerializer TaskSerializer // Added for task (de)serialization
}

// NewWorker creates a new DefaultWorker.
func NewWorker(opts *WorkerOptions) Worker {
	if opts == nil {
		opts = &WorkerOptions{} // Should have sensible defaults or error out
	}
	if opts.QueueName == "" {
		// Default or error, for now, let's use a default.
		// In a real app, this should probably be a required field.
		opts.QueueName = "default_taskiq_queue"
		fmt.Println("Warning: QueueName not provided, using default:", opts.QueueName)
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = 1 // Default to at least one worker
	}
	if opts.Logger == nil {
		opts.Logger = log.New(os.Stdout, "taskiq-worker: ", log.LstdFlags|log.Lshortfile)
	}
	if opts.TaskSerializer == nil {
		opts.TaskSerializer = NewJSONTaskSerializer() // Default to JSON serializer
	}


	// Main worker context, distinct from the context passed to Start()
	// This internal context is cancelled by Stop()
	workerCtx, cancel := context.WithCancel(context.Background())

	return &DefaultWorker{
		broker:         opts.Broker,
		resultBackend:  opts.ResultBackend,
		taskHandlers:   make(map[string]reflect.Value),
		middlewares:    make([]Middleware, 0),
		hooks:          make(map[string][]HookFunc),
		logger:         opts.Logger,
		workerCtx:      workerCtx,
		cancelWorker:   cancel,
		queueName:      opts.QueueName,
		concurrency:    opts.Concurrency,
		taskSerializer: opts.TaskSerializer,
	}
}

// SetBroker sets the message broker for the worker.
func (w *DefaultWorker) SetBroker(b Broker) {
	w.broker = b
}

// SetResultBackend sets the result backend for the worker.
func (w *DefaultWorker) SetResultBackend(rb ResultBackend) {
	w.resultBackend = rb
}

// AddMiddleware adds a middleware to the worker.
func (w *DefaultWorker) AddMiddleware(m Middleware) {
	w.middlewares = append(w.middlewares, m)
}

// RegisterTask registers a task handler function.
func (w *DefaultWorker) RegisterTask(taskName string, handlerFunc interface{}) error {
	if taskName == "" {
		return fmt.Errorf("task name cannot be empty")
	}
	if handlerFunc == nil {
		return fmt.Errorf("handler function cannot be nil for task %s", taskName)
	}

	handlerVal := reflect.ValueOf(handlerFunc)
	if handlerVal.Kind() != reflect.Func {
		return fmt.Errorf("handler for task %s is not a function, got %T", taskName, handlerFunc)
	}

	// Basic validation (can be expanded)
	// E.g., check number of args, types, if first arg is context.Context etc.
	// For now, we just store it. The actual call site will handle type mismatches with reflection.

	w.taskHandlers[taskName] = handlerVal
	w.logger.Printf("Registered task: %s", taskName)
	return nil
}

// RegisterHook registers a lifecycle hook function.
func (w *DefaultWorker) RegisterHook(eventType string, hookFn HookFunc) error {
	switch eventType {
	case EventWorkerStartup, EventWorkerShutdown, EventClientStartup, EventClientShutdown:
		// Valid event types
	default:
		return fmt.Errorf("invalid hook event type: %s", eventType)
	}
	if hookFn == nil {
		return fmt.Errorf("hook function cannot be nil for event type %s", eventType)
	}

	w.hooks[eventType] = append(w.hooks[eventType], hookFn)
	w.logger.Printf("Registered hook for event: %s", eventType)
	return nil
}

func (w *DefaultWorker) triggerHooks(ctx context.Context, eventType string) {
	if hooks, ok := w.hooks[eventType]; ok {
		w.logger.Printf("Triggering hooks for event: %s", eventType)
		for i, hook := range hooks {
			if err := hook(ctx); err != nil {
				w.logger.Printf("Error running hook %d for event %s: %v", i, eventType, err)
				// Decide if hook error should stop worker or just be logged. For now, log and continue.
			}
		}
	}
}

// Start begins the worker's task processing loop.
func (w *DefaultWorker) Start(ctx context.Context) error {
	if w.broker == nil {
		return fmt.Errorf("broker is not set; cannot start worker")
	}

	// Link the provided context with the worker's internal context.
	// This allows the worker to be stopped by either the provided context
	// or by calling w.Stop().
	// The derived context `currentRunCtx` is what's passed to ConsumeMessages and hooks.
	currentRunCtx, currentRunCancel := context.WithCancel(w.workerCtx)
	defer currentRunCancel() // Ensure this cancel is called if Start exits early

	go func() {
		select {
		case <-ctx.Done(): // If the context passed to Start() is cancelled
			w.logger.Printf("External context cancelled. Initiating worker shutdown: %v", ctx.Err())
			w.Stop() // Trigger internal stop mechanism
		case <-w.workerCtx.Done(): // If w.Stop() was called (cancelling w.workerCtx)
			// This means stop was initiated internally, currentRunCtx will also be cancelled.
			w.logger.Printf("Worker's internal context cancelled. Propagating shutdown.")
		}
	}()
	
	w.logger.Printf("Worker starting. Queue: %s, Concurrency: %d", w.queueName, w.concurrency)
	w.triggerHooks(currentRunCtx, EventWorkerStartup)
	// Assuming CLIENT_STARTUP hooks are also triggered at worker startup for now
	w.triggerHooks(currentRunCtx, EventClientStartup)


	for i := 0; i < w.concurrency; i++ {
		w.processWg.Add(1)
		go func(workerID int) {
			defer w.processWg.Done()
			w.logger.Printf("Goroutine %d starting to consume messages from queue: %s", workerID, w.queueName)
			// ConsumeMessages should block until its context (currentRunCtx) is cancelled.
			err := w.broker.ConsumeMessages(currentRunCtx, w.queueName, w.messageHandler(workerID))
			if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
				// Log error unless it's due to context cancellation (expected on shutdown)
				w.logger.Printf("Error in ConsumeMessages for goroutine %d: %v. This goroutine will exit.", workerID, err)
				// Potentially signal a more general worker failure here if needed.
				// For now, individual consumer goroutine failure is logged, and it exits.
				// If all consumer goroutines exit due to such errors, the worker might become idle.
				// A more robust worker might try to restart consumer goroutines or have a health check.
			}
			w.logger.Printf("Goroutine %d finished consuming messages.", workerID)
		}(i)
	}

	w.logger.Printf("Worker started with %d consumer goroutines.", w.concurrency)
	
	// Wait for the worker's main context to be cancelled (e.g., by Stop() or parent context)
	// This makes Start() a blocking call.
	<-currentRunCtx.Done()
	w.logger.Println("Worker's run context cancelled. Waiting for processing goroutines to finish...")

	// Wait for all processing goroutines to complete.
	w.processWg.Wait()
	w.logger.Println("All processing goroutines have finished.")

	w.logger.Println("Worker shutting down...")
	w.triggerHooks(context.Background(), EventWorkerShutdown) // Use fresh context for shutdown hooks
	w.triggerHooks(context.Background(), EventClientShutdown)
	w.logger.Println("Worker stopped.")

	// If the cancellation came from w.workerCtx (internal Stop()), then ctx.Err() might be nil.
	// If it came from the parent ctx, then ctx.Err() will be set.
	// w.workerCtx.Err() will be context.Canceled if Stop() was called.
	if w.workerCtx.Err() == context.Canceled && ctx.Err() == nil {
		return nil // Graceful shutdown via Stop()
	}
	return ctx.Err() // Return error from the parent context if it was cancelled
}

// Stop gracefully stops the worker.
func (w *DefaultWorker) Stop() error {
	w.stopOnce.Do(func() {
		w.logger.Println("Stopping worker...")
		if w.cancelWorker != nil {
			w.cancelWorker() // This cancels w.workerCtx
		}
	})
	return nil
}

// messageHandler is the callback given to Broker.ConsumeMessages.
func (w *DefaultWorker) messageHandler(workerID int) func(ctx context.Context, msg *TaskMessage) error {
	return func(ctx context.Context, taskMsg *TaskMessage) error {
		w.logger.Printf("[Worker %d] Received message: TaskID=%s, TaskName=%s", workerID, taskMsg.TaskID, taskMsg.TaskName)

		handlerVal, ok := w.taskHandlers[taskMsg.TaskName]
		if !ok {
			w.logger.Printf("[Worker %d] No handler registered for task: %s. TaskID=%s", workerID, taskMsg.TaskName, taskMsg.TaskID)
			// Decide how to handle: Nack with no requeue? For now, Ack to remove from queue.
			// This behavior depends on broker capabilities and desired error handling.
			// If we Ack, it's gone. If we Nack (and broker supports it), it might go to DLQ or be dropped.
			// Let's assume for now we Ack to prevent reprocessing an unhandlable task.
			// However, this means the task is lost if it was important.
			// A robust system would have a strategy for unknown tasks (e.g., send to error queue).
			if ackErr := w.broker.Ack(ctx, w.queueName, taskMsg); ackErr != nil {
				w.logger.Printf("[Worker %d] Error Acking unhandled task %s (ID: %s): %v", workerID, taskMsg.TaskName, taskMsg.TaskID, ackErr)
			}
			return fmt.Errorf("no handler for task %s", taskMsg.TaskName) // Return error to consumer loop
		}

		// Middleware: BeforeProcessMessage
		mCtx := &MiddlewareContext{Task: taskMsg, Context: ctx}
		for _, mw := range w.middlewares {
			if err := mw.BeforeProcessMessage(mCtx); err != nil {
				w.logger.Printf("[Worker %d] Middleware BeforeProcessMessage error for TaskID=%s: %v. Aborting task.", workerID, taskMsg.TaskID, err)
				// If middleware fails, Ack the message to prevent reprocessing, as it's likely a persistent issue.
				if ackErr := w.broker.Ack(ctx, w.queueName, taskMsg); ackErr != nil {
					w.logger.Printf("[Worker %d] Error Acking task %s (ID: %s) after BeforeProcessMessage middleware error: %v", workerID, taskMsg.TaskName, taskMsg.TaskID, ackErr)
				}
				return err // Return error to consumer loop
			}
		}
		ctx = mCtx.Context // Update context if middleware changed it

		// Execute task handler
		// Argument deserialization and result serialization will be more complex
		// For now, assume handler expects ([]byte, map[string][]byte) or similar if using current TaskMessage directly.
		// Or, it expects actual types and we need to deserialize from TaskMessage.Args/Kwargs.
		// Let's use the TaskSerializer for this.
		
		var callArgs []reflect.Value
		var err error

		// First argument is always context.Context if handler expects it.
		handlerType := handlerVal.Type()
		if handlerType.NumIn() > 0 && handlerType.In(0).Implements(reflect.TypeOf((*context.Context)(nil)).Elem()) {
			callArgs = append(callArgs, reflect.ValueOf(ctx))
		}
		
		// Deserialize remaining arguments
		// This is a simplified example. Actual deserialization will depend on handler signature
		// and how arguments are packed by the client.
		// Using TaskSerializer to convert TaskMessage.Args/Kwargs to handler's expected types.
		deserializedArgs, err := w.taskSerializer.DeserializeArgs(taskMsg, handlerVal)
		if err != nil {
			w.logger.Printf("[Worker %d] Error deserializing args for TaskID=%s: %v", workerID, taskMsg.TaskID, err)
			// Handle error: Nack or Ack? For now, Ack and report failure.
			w.processTaskError(ctx, taskMsg, err, "deserialization_failure")
			return err
		}
		callArgs = append(callArgs, deserializedArgs...)


		// Call the handler
		w.logger.Printf("[Worker %d] Executing task: TaskID=%s, Handler: %s", workerID, taskMsg.TaskID, taskMsg.TaskName)
		returnValues := handlerVal.Call(callArgs)

		// Process results and errors
		var taskErr error
		var resultData []byte

		// Last return value is conventionally an error.
		if len(returnValues) > 0 {
			lastVal := returnValues[len(returnValues)-1]
			if errVal, ok := lastVal.Interface().(error); ok && errVal != nil {
				taskErr = errVal
			}
		}

		if taskErr != nil {
			w.logger.Printf("[Worker %d] Task execution failed: TaskID=%s, Error: %v", workerID, taskMsg.TaskID, taskErr)
			w.processTaskError(ctx, taskMsg, taskErr, StatusFailure) // Handles result backend
		} else {
			// If no error, the second to last (or only) value is the result.
			// This is a simplification; robust result handling needs more sophisticated signature analysis.
			if len(returnValues) > 0 {
				// If error was the last arg, and it was nil, then result is before it or the only one.
				var actualResultVal reflect.Value
				if len(returnValues) > 1 && handlerType.Out(handlerType.NumOut()-1).Implements(reflect.TypeOf((*error)(nil)).Elem()){
					actualResultVal = returnValues[len(returnValues)-2]
				} else if len(returnValues) == 1 && !handlerType.Out(0).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
					actualResultVal = returnValues[0]
				}

				if actualResultVal.IsValid() {
					var errSerialize error
					resultData, errSerialize = w.taskSerializer.SerializeResult(actualResultVal.Interface())
					if errSerialize != nil {
						w.logger.Printf("[Worker %d] Failed to serialize result for TaskID=%s: %v", workerID, taskMsg.TaskID, errSerialize)
						// Treat serialization error as a task failure
						w.processTaskError(ctx, taskMsg, errSerialize, StatusFailure)
						// Still call AfterProcessMessage, then Ack.
						for _, mw := range w.middlewares {
							mw.AfterProcessMessage(mCtx, errSerialize) // Pass serialization error
						}
						if ackErr := w.broker.Ack(ctx, w.queueName, taskMsg); ackErr != nil {
							w.logger.Printf("[Worker %d] Error Acking task %s (ID: %s) after result serialization error: %v", workerID, taskMsg.TaskName, taskMsg.TaskID, ackErr)
						}
						return errSerialize
					}
				}
			}
			w.logger.Printf("[Worker %d] Task execution successful: TaskID=%s", workerID, taskMsg.TaskID)
			w.processTaskSuccess(ctx, taskMsg, resultData) // Handles result backend
		}

		// Middleware: AfterProcessMessage
		for _, mw := range w.middlewares {
			mw.AfterProcessMessage(mCtx, taskErr) // Pass task execution error
		}
		
		// Ack the message after successful processing or after failure has been handled (e.g., result stored)
		if ackErr := w.broker.Ack(ctx, w.queueName, taskMsg); ackErr != nil {
			w.logger.Printf("[Worker %d] Error Acking task %s (ID: %s): %v", workerID, taskMsg.TaskName, taskMsg.TaskID, ackErr)
			// This is a problem: if Ack fails, message might be redelivered even if processed.
			// Broker specific error handling or retry for Ack might be needed.
			return ackErr // Return error to consumer loop, might cause redelivery if not handled by broker.ConsumeMessages
		}

		w.logger.Printf("[Worker %d] Successfully processed and Acked TaskID=%s", workerID, taskMsg.TaskID)
		return nil // Success
	}
}

func (w *DefaultWorker) processTaskSuccess(ctx context.Context, taskMsg *TaskMessage, resultData []byte) {
	if w.resultBackend == nil {
		return
	}
	resMsg := &ResultMessage{
		TaskID:    taskMsg.TaskID,
		Status:    StatusSuccess,
		Result:    resultData,
		Timestamp: time.Now().UTC(),
	}
	w.sendResult(ctx, taskMsg, resMsg)
}

func (w *DefaultWorker) processTaskError(ctx context.Context, taskMsg *TaskMessage, err error, status string) {
	if w.resultBackend == nil {
		return
	}
	resMsg := &ResultMessage{
		TaskID:    taskMsg.TaskID,
		Status:    status, // e.g. StatusFailure or a more specific error status
		Error:     err.Error(),
		Timestamp: time.Now().UTC(),
	}
	w.sendResult(ctx, taskMsg, resMsg)
}

func (w *DefaultWorker) sendResult(ctx context.Context, taskMsg *TaskMessage, resMsg *ResultMessage) {
	mCtx := &MiddlewareContext{Task: taskMsg, Context: ctx}

	// Middleware: BeforeSendResult
	for _, mw := range w.middlewares {
		if err := mw.BeforeSendResult(mCtx, resMsg); err != nil {
			w.logger.Printf("Middleware BeforeSendResult error for TaskID=%s: %v. Result not sent.", taskMsg.TaskID, err)
			// If BeforeSendResult fails, we might still call AfterSendResult with the error.
			for _, mwAfter := range w.middlewares { // Ensure all AfterSendResult are called
				mwAfter.AfterSendResult(mCtx, resMsg, err)
			}
			return
		}
	}

	var sendErr error
	if w.resultBackend != nil {
		sendErr = w.resultBackend.SetResult(taskMsg.TaskID, resMsg)
		if sendErr != nil {
			w.logger.Printf("Failed to set result for TaskID=%s: %v", taskMsg.TaskID, sendErr)
		}
	}

	// Middleware: AfterSendResult
	for _, mw := range w.middlewares {
		mw.AfterSendResult(mCtx, resMsg, sendErr)
	}
}


// TaskSerializer interface (should be in its own file, e.g., taskiq/serializer.go)
// For now, defining here to make worker_impl.go runnable.
type TaskSerializer interface {
	SerializeArgs(args ...interface{}) ([][]byte, map[string][]byte, error) // Placeholder for actual serialization
	DeserializeArgs(taskMsg *TaskMessage, handlerFunc reflect.Value) ([]reflect.Value, error)
	SerializeResult(result interface{}) ([]byte, error)
	DeserializeResult(data []byte, resultType reflect.Type) (interface{}, error) // Placeholder
}

// JSONTaskSerializer implements TaskSerializer using JSON.
// This is a very basic placeholder. Real implementation needs to handle types properly.
type JSONTaskSerializer struct{}

func NewJSONTaskSerializer() *JSONTaskSerializer {
	return &JSONTaskSerializer{}
}

func (s *JSONTaskSerializer) SerializeArgs(args ...interface{}) ([][]byte, map[string][]byte, error) {
	// This is a simplified conceptual placeholder.
	// Actual serialization would involve iterating through args, serializing each.
	// Kwargs would also be serialized. This doesn't match TaskMessage structure directly yet.
	// For this worker, client side is assumed to prepare TaskMessage.Args and TaskMessage.Kwargs.
	return nil, nil, fmt.Errorf("JSONTaskSerializer.SerializeArgs not fully implemented for client-side use")
}

func (s *JSONTaskSerializer) DeserializeArgs(taskMsg *TaskMessage, handlerFuncVal reflect.Value) ([]reflect.Value, error) {
    handlerType := handlerFuncVal.Type()
    expectedArgs := make([]reflect.Value, 0)
    
    currentMessageArgIndex := 0
    numHandlerArgs := handlerType.NumIn()

    for i := 0; i < numHandlerArgs; i++ {
        argType := handlerType.In(i)
        if argType.Implements(reflect.TypeOf((*context.Context)(nil)).Elem()) {
            // Context is handled separately, not from taskMsg.Args
            continue
        }

        if currentMessageArgIndex >= len(taskMsg.Args) {
            // Not enough arguments provided in TaskMessage for handler parameters
            // Check if remaining handler args are variadic or have defaults (not supported here)
             if handlerType.IsVariadic() && i == numHandlerArgs-1 {
                // If the last handler arg is variadic, it can be empty.
                // Create an empty slice of the variadic element type.
                sliceType := reflect.SliceOf(argType.Elem())
                emptyVariadicSlice := reflect.MakeSlice(sliceType, 0, 0)
                expectedArgs = append(expectedArgs, emptyVariadicSlice)
                continue
            }
            return nil, fmt.Errorf("not enough arguments for handler %s: expected %d, got %d from message",
                runtimeFuncName(handlerFuncVal), numHandlerArgs-i, len(taskMsg.Args)-currentMessageArgIndex)
        }

        argBytes := taskMsg.Args[currentMessageArgIndex]
        currentMessageArgIndex++

        // Create a new pointer to the argument type (e.g., *MyStruct)
        argPtr := reflect.New(argType)
        if err := json.Unmarshal(argBytes, argPtr.Interface()); err != nil {
            return nil, fmt.Errorf("error unmarshaling arg %d for handler %s: %w. Data: %s", i, runtimeFuncName(handlerFuncVal), err, string(argBytes))
        }
        // Dereference the pointer to get the actual value (e.g., MyStruct)
        expectedArgs = append(expectedArgs, argPtr.Elem())
    }
    
    // TODO: Handle Kwargs if the handler expects them (e.g., by mapping to struct fields or a map[string]interface{} argument)
    // For now, Kwargs are ignored in this deserializer.

    return expectedArgs, nil
}


func (s *JSONTaskSerializer) SerializeResult(result interface{}) ([]byte, error) {
	if result == nil {
		return nil, nil // No result to serialize
	}
	// Check if result is already []byte to prevent double marshaling
	if bytes, ok := result.([]byte); ok {
		// Heuristic: if it's already bytes and looks like JSON, pass through.
		// A more robust check might be needed, or rely on handler to return non-[]byte for auto-serialization.
		if (len(bytes) > 0 && (bytes[0] == '{' || bytes[0] == '[')) || len(bytes) == 0 {
			return bytes, nil
		}
	}
	return json.Marshal(result)
}

func (s *JSONTaskSerializer) DeserializeResult(data []byte, resultType reflect.Type) (interface{}, error) {
	if data == nil {
		return nil, nil
	}
	val := reflect.New(resultType).Interface()
	err := json.Unmarshal(data, val)
	if err != nil {
		return nil, err
	}
	return reflect.ValueOf(val).Elem().Interface(), nil
}

// Helper to get function name for logging (optional)
import "runtime"
func runtimeFuncName(f reflect.Value) string {
    return runtime.FuncForPC(f.Pointer()).Name()
}

// Status constants (should be in a central place like taskiq/status.go or taskiq/task.go)
const (
	StatusSuccess = "SUCCESS"
	StatusFailure = "FAILURE"
	// Add other statuses like RETRY, PENDING etc.
)
