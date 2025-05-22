package taskiq

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"runtime" // Added for runtimeFuncName

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes" // For setting span status
	oteltrace "go.opentelemetry.io/otel/trace"
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
	// logger         *log.Logger // Replaced by global taskiq.Logger
	workerCtx      context.Context
	cancelWorker   context.CancelFunc
	queueName      string
	concurrency    int
	stopOnce       sync.Once // To ensure Stop logic runs once
	processWg      sync.WaitGroup // To wait for task processing goroutines
	taskSerializer TaskSerializer // Serializer for task args/results
	encoder        Encoder        // Encoder for task args/results payload
}

// WorkerOptions holds configuration for the DefaultWorker.
type WorkerOptions struct {
	QueueName      string
	Concurrency    int
	Broker         Broker
	ResultBackend  ResultBackend
	// Logger         *log.Logger // Replaced by global taskiq.Logger
	TaskSerializer TaskSerializer // Serializer for task args/results
	Encoder        Encoder        // Encoder for task args/results payload
}

// NewWorker creates a new DefaultWorker.
func NewWorker(opts *WorkerOptions) Worker {
	// Initialize OpenTelemetry Tracer Provider
	// In a real application, you might want to make this more configurable
	// or allow a TracerProvider to be passed in.
	// We will call InitTracerProvider here. If it fails, we'll log it.
	// The Tracer variable in taskiq/tracing.go will be used.
	if _, err := InitTracerProvider(); err != nil {
		Logger.Error().Err(err).Msg("Failed to initialize OpenTelemetry TracerProvider. Tracing will be a NoOp.")
		// Optionally, explicitly set to NoOp if InitTracerProvider doesn't handle this
		// InitNoOpTracerProvider() // Call this if InitTracerProvider doesn't set a NoOp on error
	}


	if opts == nil {
		opts = &WorkerOptions{} // Should have sensible defaults or error out
	}
	if opts.QueueName == "" {
		// Default or error, for now, let's use a default.
		// In a real app, this should probably be a required field.
		opts.QueueName = "default_taskiq_queue"
		Logger.Warn().Str("queueName", opts.QueueName).Msg("QueueName not provided, using default")
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = 1 // Default to at least one worker
	}
	// Logger is now global, initialized in logger.go
	if opts.TaskSerializer == nil {
		opts.TaskSerializer = NewDefaultTaskSerializer() // Default to DefaultTaskSerializer
	}
	if opts.Encoder == nil {
		opts.Encoder = &JSONEncoder{} // Default to JSONEncoder
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
		// logger:         opts.Logger, // Replaced by global taskiq.Logger
		workerCtx:      workerCtx,
		cancelWorker:   cancel,
		queueName:      opts.QueueName,
		concurrency:    opts.Concurrency,
		taskSerializer: opts.TaskSerializer,
		encoder:        opts.Encoder,
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
	Logger.Info().Str("taskName", taskName).Msg("Registered task")
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
	Logger.Info().Str("eventType", eventType).Msg("Registered hook for event")
	return nil
}

func (w *DefaultWorker) triggerHooks(ctx context.Context, eventType string) {
	if hooks, ok := w.hooks[eventType]; ok {
		Logger.Debug().Str("eventType", eventType).Msg("Triggering hooks for event")
		for i, hook := range hooks {
			if err := hook(ctx); err != nil {
				Logger.Error().Err(err).Int("hookIndex", i).Str("eventType", eventType).Msg("Error running hook")
				// Decide if hook error should stop worker or just be logged. For now, log and continue.
			}
		}
	}
}

// Start begins the worker's task processing loop.
func (w *DefaultWorker) Start(ctx context.Context) error {
	if w.broker == nil {
		Logger.Error().Msg("Broker is not set; cannot start worker")
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
			Logger.Info().Err(ctx.Err()).Msg("External context cancelled. Initiating worker shutdown.")
			w.Stop() // Trigger internal stop mechanism
		case <-w.workerCtx.Done(): // If w.Stop() was called (cancelling w.workerCtx)
			// This means stop was initiated internally, currentRunCtx will also be cancelled.
			Logger.Info().Msg("Worker's internal context cancelled. Propagating shutdown.")
		}
	}()
	
	Logger.Info().Str("queueName", w.queueName).Int("concurrency", w.concurrency).Msg("Worker starting")
	w.triggerHooks(currentRunCtx, EventWorkerStartup)
	// Assuming CLIENT_STARTUP hooks are also triggered at worker startup for now
	w.triggerHooks(currentRunCtx, EventClientStartup)


	for i := 0; i < w.concurrency; i++ {
		w.processWg.Add(1)
		go func(workerID int) {
			defer w.processWg.Done()
			Logger.Info().Int("workerID", workerID).Str("queueName", w.queueName).Msg("Goroutine starting to consume messages")
			// ConsumeMessages should block until its context (currentRunCtx) is cancelled.
			err := w.broker.ConsumeMessages(currentRunCtx, w.queueName, w.messageHandler(workerID))
			if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
				// Log error unless it's due to context cancellation (expected on shutdown)
				Logger.Error().Err(err).Int("workerID", workerID).Msg("Error in ConsumeMessages. This goroutine will exit.")
				// Potentially signal a more general worker failure here if needed.
				// For now, individual consumer goroutine failure is logged, and it exits.
				// If all consumer goroutines exit due to such errors, the worker might become idle.
				// A more robust worker might try to restart consumer goroutines or have a health check.
			}
			Logger.Info().Int("workerID", workerID).Msg("Goroutine finished consuming messages.")
		}(i)
	}

	Logger.Info().Int("concurrency", w.concurrency).Msg("Worker started with consumer goroutines.")
	
	// Wait for the worker's main context to be cancelled (e.g., by Stop() or parent context)
	// This makes Start() a blocking call.
	<-currentRunCtx.Done()
	Logger.Info().Msg("Worker's run context cancelled. Waiting for processing goroutines to finish...")

	// Wait for all processing goroutines to complete.
	w.processWg.Wait()
	Logger.Info().Msg("All processing goroutines have finished.")

	Logger.Info().Msg("Worker shutting down...")
	w.triggerHooks(context.Background(), EventWorkerShutdown) // Use fresh context for shutdown hooks
	w.triggerHooks(context.Background(), EventClientShutdown)
	Logger.Info().Msg("Worker stopped.")

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
		Logger.Info().Msg("Stopping worker...")
		if w.cancelWorker != nil {
			w.cancelWorker() // This cancels w.workerCtx
		}
	})
	return nil
}

// messageHandler is the callback given to Broker.ConsumeMessages.
func (w *DefaultWorker) messageHandler(workerID int) func(ctx context.Context, msg *TaskMessage) error {
	return func(ctx context.Context, taskMsg *TaskMessage) error {
		// Start a parent span for the entire task handling process
		spanCtx, parentSpan := NewSpan(ctx, "Worker.MessageHandler",
			oteltrace.WithAttributes(
				attribute.String("task.id", taskMsg.TaskID),
				attribute.String("task.name", taskMsg.TaskName),
				attribute.Int("worker.id", workerID),
			),
			oteltrace.WithSpanKind(oteltrace.SpanKindConsumer), // Mark as a consumer span
		)
		defer parentSpan.End()

		Logger.Info().
			Int("workerID", workerID).
			Str("taskID", taskMsg.TaskID).
			Str("taskName", taskMsg.TaskName).
			Str("traceID", parentSpan.SpanContext().TraceID().String()).
			Str("spanID", parentSpan.SpanContext().SpanID().String()).
			Msg("Received message")

		handlerVal, ok := w.taskHandlers[taskMsg.TaskName]
		if !ok {
			Logger.Warn().
				Int("workerID", workerID).
				Str("taskID", taskMsg.TaskID).
				Str("taskName", taskMsg.TaskName).
				Msg("No handler registered for task. Acking to remove from queue.")
		parentSpan.SetAttributes(attribute.Bool("task.handler_found", false))
		parentSpan.SetStatus(codes.Error, "No handler registered for task")
			// Decide how to handle: Nack with no requeue? For now, Ack to remove from queue.
			// This behavior depends on broker capabilities and desired error handling.
			// If we Ack, it's gone. If we Nack (and broker supports it), it might go to DLQ or be dropped.
			// Let's assume for now we Ack to prevent reprocessing an unhandlable task.
			// However, this means the task is lost if it was important.
			// A robust system would have a strategy for unknown tasks (e.g., send to error queue).
		if ackErr := w.broker.Ack(spanCtx, w.queueName, taskMsg); ackErr != nil { // Use spanCtx
				Logger.Error().Err(ackErr).Int("workerID", workerID).Str("taskID", taskMsg.TaskID).Str("taskName", taskMsg.TaskName).Msg("Error Acking unhandled task")
			parentSpan.RecordError(ackErr, oteltrace.WithAttributes(attribute.String("event", "ack_unhandled_task_error")))
			}
			return fmt.Errorf("no handler for task %s", taskMsg.TaskName) // Return error to consumer loop
		}
	parentSpan.SetAttributes(attribute.Bool("task.handler_found", true))

		// Middleware: BeforeProcessMessage
		mCtx := &MiddlewareContext{Task: taskMsg}
		for _, mw := range w.middlewares {
		// Pass spanCtx to middleware methods
		if err := mw.BeforeProcessMessage(spanCtx, mCtx); err != nil {
				Logger.Error().Err(err).Int("workerID", workerID).Str("taskID", taskMsg.TaskID).Msg("Middleware BeforeProcessMessage error. Aborting task.")
			parentSpan.RecordError(err, oteltrace.WithAttributes(attribute.String("event", "middleware_before_process_error")))
			parentSpan.SetStatus(codes.Error, "Middleware BeforeProcessMessage error")
				// If middleware fails, Ack the message to prevent reprocessing, as it's likely a persistent issue.
			if ackErr := w.broker.Ack(spanCtx, w.queueName, taskMsg); ackErr != nil { // Use spanCtx
					Logger.Error().Err(ackErr).Int("workerID", workerID).Str("taskID", taskMsg.TaskID).Str("taskName", taskMsg.TaskName).Msg("Error Acking task after BeforeProcessMessage middleware error")
				parentSpan.RecordError(ackErr, oteltrace.WithAttributes(attribute.String("event", "ack_middleware_error")))
				}
				return err // Return error to consumer loop
			}
		}
		// ctx = mCtx.Context // Update context if middleware changed it // This line is removed as Context is part of middleware method signature

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
		// The TaskMessage.ContentEncoding should ideally inform which encoder to use.
		// For now, we use the worker's configured encoder.
		// A more advanced setup might involve a map of encoders accessible by content type.
		var currentEncoder Encoder = w.encoder
		if taskMsg.ContentEncoding != "" && taskMsg.ContentEncoding != w.encoder.ContentType() {
			// This is a mismatch. Log a warning. For now, we'll proceed with worker's default.
			// Future: Could try to find a matching encoder or fail.
			Logger.Warn().
				Str("taskID", taskMsg.TaskID).
				Str("taskContentEncoding", taskMsg.ContentEncoding).
				Str("workerEncoder", w.encoder.ContentType()).
				Msg("Task ContentEncoding mismatch with worker's encoder. Attempting with worker's default.")
		}


		_, deserializeSpan := NewSpan(spanCtx, "Worker.DeserializeArgs", oteltrace.WithAttributes(attribute.String("encoder.contentType", currentEncoder.ContentType())))
		deserializedArgs, err := w.taskSerializer.DeserializeArgs(currentEncoder, taskMsg, handlerVal)
		if err != nil {
			Logger.Error().Err(err).Int("workerID", workerID).Str("taskID", taskMsg.TaskID).Str("encoder", currentEncoder.ContentType()).Msg("Error deserializing args")
			deserializeSpan.RecordError(err, oteltrace.WithAttributes(attribute.String("encoder", currentEncoder.ContentType())))
			deserializeSpan.SetStatus(codes.Error, "Deserialization error")
			deserializeSpan.End()
			parentSpan.RecordError(err, oteltrace.WithAttributes(attribute.String("event", "deserialization_error")))
			parentSpan.SetStatus(codes.Error, "Deserialization error")
			// Handle error: Nack or Ack? For now, Ack and report failure.
			w.processTaskError(spanCtx, taskMsg, err, "deserialization_failure") // Use spanCtx
			return err
		}
		deserializeSpan.End()
		callArgs = append(callArgs, deserializedArgs...)


		// Call the handler
		// Ensure the context passed to the handler is the one with the active span (spanCtx)
		// if the handler expects a context.Context as its first argument.
		// The reflection logic for callArgs already handles prepending context if necessary.
		// We need to make sure `spanCtx` is the context used there.
		
		// Update callArgs to use spanCtx if context is the first argument
		if handlerType.NumIn() > 0 && handlerType.In(0).Implements(reflect.TypeOf((*context.Context)(nil)).Elem()) {
			// The first element of deserializedArgs might be a placeholder context or nil,
			// or callArgs was built without the context initially.
			// Let's rebuild callArgs ensuring spanCtx is the first if expected.
			
			// If callArgs already contains a context at index 0 from previous logic, replace it.
			// Otherwise, prepend. This depends on how callArgs was constructed initially.
			// The existing logic:
			//   if handlerType.NumIn() > 0 && handlerType.In(0).Implements(reflect.TypeOf((*context.Context)(nil)).Elem()) {
			// 	    callArgs = append(callArgs, reflect.ValueOf(ctx)) // Original ctx, not spanCtx
			//   }
			//   callArgs = append(callArgs, deserializedArgs...)
			// So, if context was added, it's at callArgs[0]. We need to replace it.
			if len(callArgs) > 0 && callArgs[0].Type().Implements(reflect.TypeOf((*context.Context)(nil)).Elem()) {
				callArgs[0] = reflect.ValueOf(spanCtx) // Replace original ctx with spanCtx
			} else {
				// This case should ideally not happen if the above logic for adding ctx is correct.
				// However, to be safe, if context is expected but not the first in callArgs,
				// it implies deserializedArgs are the only things in callArgs.
				// We would prepend spanCtx.
				// For now, assuming the existing logic correctly places context at callArgs[0] if expected.
			}
		}


		spanCtxCall, handlerSpan := NewSpan(spanCtx, "Worker.ExecuteTask", oteltrace.WithAttributes(attribute.String("handler.name", runtimeFuncName(handlerVal))))
		Logger.Debug().Int("workerID", workerID).Str("taskID", taskMsg.TaskID).Str("taskName", taskMsg.TaskName).Msg("Executing task")
		
		// Ensure the context within callArgs for the handler is spanCtxCall
		if len(callArgs) > 0 && callArgs[0].Type().Implements(reflect.TypeOf((*context.Context)(nil)).Elem()) {
			callArgs[0] = reflect.ValueOf(spanCtxCall) 
		}

		returnValues := handlerVal.Call(callArgs)
		handlerSpan.End()

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
			Logger.Error().Err(taskErr).Int("workerID", workerID).Str("taskID", taskMsg.TaskID).Msg("Task execution failed")
			parentSpan.RecordError(taskErr, oteltrace.WithAttributes(attribute.String("event", "task_execution_error")))
			parentSpan.SetStatus(codes.Error, "Task execution failed")
			w.processTaskError(spanCtxCall, taskMsg, taskErr, StatusFailure) // Handles result backend, use spanCtxCall
		} else {
			parentSpan.SetStatus(codes.OK, "") // Explicitly set OK status
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
					_, serializeResultSpan := NewSpan(spanCtxCall, "Worker.SerializeResult")
					var errSerialize error
					// Use the worker's configured encoder to serialize the result.
					resultDataRaw, errSerialize := w.taskSerializer.SerializeResult(w.encoder, actualResultVal.Interface())
					if errSerialize != nil {
						Logger.Error().Err(errSerialize).Int("workerID", workerID).Str("taskID", taskMsg.TaskID).Str("encoder", w.encoder.ContentType()).Msg("Failed to serialize result")
						serializeResultSpan.RecordError(errSerialize, oteltrace.WithAttributes(attribute.String("encoder", w.encoder.ContentType())))
						serializeResultSpan.SetStatus(codes.Error, "Result serialization error")
						serializeResultSpan.End()
						parentSpan.RecordError(errSerialize, oteltrace.WithAttributes(attribute.String("event", "result_serialization_error")))
						parentSpan.SetStatus(codes.Error, "Result serialization error")
						// Treat serialization error as a task failure
						w.processTaskError(spanCtxCall, taskMsg, errSerialize, StatusFailure) // Use spanCtxCall
						// Still call AfterProcessMessage, then Ack.
						// Use spanCtx for AfterProcessMessage as it's outside the direct handler call context.
						for _, mw := range w.middlewares {
							mw.AfterProcessMessage(spanCtx, mCtx, errSerialize) // Pass serialization error
						}
						if ackErr := w.broker.Ack(spanCtx, w.queueName, taskMsg); ackErr != nil { // Use spanCtx
							Logger.Error().Err(ackErr).Int("workerID", workerID).Str("taskID", taskMsg.TaskID).Str("taskName", taskMsg.TaskName).Msg("Error Acking task after result serialization error")
							parentSpan.RecordError(ackErr, oteltrace.WithAttributes(attribute.String("event", "ack_serialization_error")))
						}
						return errSerialize
					}
					serializeResultSpan.End()
				}
			}
			Logger.Info().Int("workerID", workerID).Str("taskID", taskMsg.TaskID).Msg("Task execution successful")
			w.processTaskSuccess(spanCtxCall, taskMsg, resultData) // Handles result backend, use spanCtxCall
		}

		// Middleware: AfterProcessMessage
		// Use spanCtx as this is outside the handler's direct execution context (spanCtxCall)
		for _, mw := range w.middlewares {
			mw.AfterProcessMessage(spanCtx, mCtx, taskErr) // Pass task execution error
		}
		
		// Ack the message after successful processing or after failure has been handled (e.g., result stored)
		if ackErr := w.broker.Ack(spanCtx, w.queueName, taskMsg); ackErr != nil { // Use spanCtx
			Logger.Error().Err(ackErr).Int("workerID", workerID).Str("taskID", taskMsg.TaskID).Str("taskName", taskMsg.TaskName).Msg("Error Acking task")
			parentSpan.RecordError(ackErr, oteltrace.WithAttributes(attribute.String("event", "ack_error")))
			// This is a problem: if Ack fails, message might be redelivered even if processed.
			// Broker specific error handling or retry for Ack might be needed.
			return ackErr // Return error to consumer loop, might cause redelivery if not handled by broker.ConsumeMessages
		}

		Logger.Info().Int("workerID", workerID).Str("taskID", taskMsg.TaskID).Msg("Successfully processed and Acked Task")
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
		Result:    resultDataRaw, // This is json.RawMessage from SerializeResult
		ContentEncoding: w.encoder.ContentType(), // Set the content encoding
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
		// Result field is not set for errors in this basic model, but ContentEncoding could still be set.
		ContentEncoding: w.encoder.ContentType(),
		Timestamp: time.Now().UTC(),
	}
	w.sendResult(ctx, taskMsg, resMsg)
}

func (w *DefaultWorker) sendResult(ctx context.Context, taskMsg *TaskMessage, resMsg *ResultMessage) {
	mCtx := &MiddlewareContext{Task: taskMsg}

	// The ctx passed to sendResult is the one from the handler's execution (spanCtxCall if handler took context, or spanCtx if not)
	// We'll start a new span for sendResult itself, as a child of the context it received.
	spanCtxSendResult, sendResultSpan := NewSpan(ctx, "Worker.SendResult", oteltrace.WithAttributes(attribute.String("task.id", taskMsg.TaskID)))
	defer sendResultSpan.End()

	// Middleware: BeforeSendResult
	for _, mw := range w.middlewares {
		if err := mw.BeforeSendResult(spanCtxSendResult, mCtx, resMsg); err != nil { // Use spanCtxSendResult
			Logger.Error().Err(err).Str("taskID", taskMsg.TaskID).Msg("Middleware BeforeSendResult error. Result not sent.")
			sendResultSpan.RecordError(err, oteltrace.WithAttributes(attribute.String("event", "middleware_before_send_error")))
			sendResultSpan.SetStatus(codes.Error, "Middleware BeforeSendResult error")
			// If BeforeSendResult fails, we might still call AfterSendResult with the error.
			for _, mwAfter := range w.middlewares { // Ensure all AfterSendResult are called
				mwAfter.AfterSendResult(spanCtxSendResult, mCtx, resMsg, err) // Use spanCtxSendResult
			}
			return
		}
	}

	var sendErr error
	if w.resultBackend != nil {
		_, setBackendSpan := NewSpan(spanCtxSendResult, "ResultBackend.SetResult")
		sendErr = w.resultBackend.SetResult(taskMsg.TaskID, resMsg)
		if sendErr != nil {
			Logger.Error().Err(sendErr).Str("taskID", taskMsg.TaskID).Msg("Failed to set result")
			setBackendSpan.RecordError(sendErr)
			setBackendSpan.SetStatus(codes.Error, "SetResult failed")
			sendResultSpan.RecordError(sendErr, oteltrace.WithAttributes(attribute.String("event", "set_result_backend_error"))) // Also record on parent
			sendResultSpan.SetStatus(codes.Error, "SetResult failed")
		}
		setBackendSpan.End()
	}


	// Middleware: AfterSendResult
	for _, mw := range w.middlewares {
		mw.AfterSendResult(spanCtxSendResult, mCtx, resMsg, sendErr) // Use spanCtxSendResult
	}
}


// Status constants (should be in a central place like taskiq/status.go or taskiq/task.go)
// runtimeFuncName is already defined in serializer.go, but also kept here for now
// as it was used by the old JSONTaskSerializer. It can be removed if no longer directly used here.
// func runtimeFuncName(f reflect.Value) string {
//     return runtime.FuncForPC(f.Pointer()).Name()
// }
const (
	StatusSuccess = "SUCCESS"
	StatusFailure = "FAILURE"
	// Add other statuses like RETRY, PENDING etc.
)
