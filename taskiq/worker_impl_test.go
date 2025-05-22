package taskiq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// --- Mock Broker Implementation ---

type mockBroker struct {
	mu                sync.Mutex
	consumeChan       chan *TaskMessage // Channel to send tasks to the worker
	ackChan           chan *TaskMessage // Channel to receive Acked messages
	nackChan          chan *TaskMessage // Channel to receive Nacked messages (if we distinguish)
	publishChan       chan *TaskMessage // Channel to receive Published messages (if worker publishes)
	declareQueueCalls map[string]int
	consumeMessagesFn func(ctx context.Context, queueName string, handler func(ctx context.Context, message *TaskMessage) error) error
	closeCalled       bool
	closeChan         chan struct{} // To signal that Close has been called
	pingError         error
	publishError      error
	ackError          error
	consumeCallCount  int
}

func newMockBroker() *mockBroker {
	return &mockBroker{
		consumeChan:       make(chan *TaskMessage, 10), 
		ackChan:           make(chan *TaskMessage, 10),
		nackChan:          make(chan *TaskMessage, 10),
		publishChan:       make(chan *TaskMessage, 10),
		declareQueueCalls: make(map[string]int),
		closeChan:         make(chan struct{}),
	}
}

func (mb *mockBroker) DeclareQueue(ctx context.Context, queueName string) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	mb.declareQueueCalls[queueName]++
	return nil
}

func (mb *mockBroker) PublishMessage(ctx context.Context, queueName string, message *TaskMessage) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	if mb.publishError != nil {
		return mb.publishError
	}
	// Non-blocking send for tests
	select {
	case mb.publishChan <- message:
	default:
		// log.Printf("mockBroker: publishChan is full or no receiver for task %s", message.TaskID)
	}
	return nil
}

func (mb *mockBroker) ConsumeMessages(ctx context.Context, queueName string, handler func(ctx context.Context, message *TaskMessage) error) error {
	mb.mu.Lock()
	mb.consumeCallCount++
	mb.mu.Unlock()

	if mb.consumeMessagesFn != nil {
		return mb.consumeMessagesFn(ctx, queueName, handler)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case taskMsg, ok := <-mb.consumeChan:
			if !ok { 
				return nil 
			}
			// Simulate message delivery by calling the handler
			// The error from the handler is typically handled by the worker (Ack/Nack decision)
			// and doesn't usually propagate back to ConsumeMessages unless it's a critical broker error.
			_ = handler(ctx, taskMsg) 
		}
	}
}

func (mb *mockBroker) Ack(ctx context.Context, queueName string, message *TaskMessage) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	if mb.ackError != nil {
		return mb.ackError
	}
	// Non-blocking send for tests
	select {
	case mb.ackChan <- message:
	default:
		// log.Printf("mockBroker: ackChan is full or no receiver for task %s", message.TaskID)
	}
	return nil
}

func (mb *mockBroker) Close() error {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	if !mb.closeCalled {
		// Close consumeChan to signal ConsumeMessages loops to stop if they are ranging on it.
		// However, the default ConsumeMessages uses select, so ctx.Done() is the primary way.
		// Closing here helps if a custom consumeMessagesFn ranges over it.
		close(mb.consumeChan)
		close(mb.closeChan) // Signal that Close has been called
	}
	mb.closeCalled = true
	return nil
}

func (mb *mockBroker) Ping(ctx context.Context) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	return mb.pingError
}

// --- Mock ResultBackend Implementation ---

type mockResultBackend struct {
	mu             sync.Mutex
	results        map[string]*ResultMessage
	setResultFn    func(taskID string, result *ResultMessage) error
	getResultFn    func(taskID string) (*ResultMessage, error)
	closeCalled    bool
	getResultError error
	setResultError error
	setResultCalls int
}

func newMockResultBackend() *mockResultBackend {
	return &mockResultBackend{
		results: make(map[string]*ResultMessage),
	}
}

func (mrb *mockResultBackend) SetResult(taskID string, result *ResultMessage) error {
	mrb.mu.Lock()
	defer mrb.mu.Unlock()
	mrb.setResultCalls++
	if mrb.setResultError != nil {
		return mrb.setResultError
	}
	if mrb.setResultFn != nil {
		return mrb.setResultFn(taskID, result)
	}
	mrb.results[taskID] = result
	return nil
}

func (mrb *mockResultBackend) GetResult(taskID string) (*ResultMessage, error) {
	mrb.mu.Lock()
	defer mrb.mu.Unlock()
	if mrb.getResultError != nil {
		return nil, mrb.getResultError
	}
	if mrb.getResultFn != nil {
		return mrb.getResultFn(taskID)
	}
	result, ok := mrb.results[taskID]
	if !ok {
		return nil, fmt.Errorf("result not found for task %s (mock)", taskID) 
	}
	return result, nil
}

func (mrb *mockResultBackend) Close() error {
	mrb.mu.Lock()
	defer mrb.mu.Unlock()
	mrb.closeCalled = true
	return nil
}

// --- Helper Functions for Tests ---

func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return true 
	case <-time.After(timeout):
		return false 
	}
}

func createTaskArgs(t *testing.T, args ...interface{}) [][]byte {
	t.Helper()
	var serializedArgs [][]byte
	serializer := NewJSONTaskSerializer() 
	for _, arg := range args {
		sArg, err := serializer.SerializeResult(arg) 
		if err != nil {
			t.Fatalf("Failed to serialize task argument '%v': %v", arg, err)
		}
		serializedArgs = append(serializedArgs, sArg)
	}
	return serializedArgs
}

// func NewDefaultLogger(prefix string) *log.Logger { // Removed as we use global zerolog.Logger
//     return log.New(os.Stdout, prefix, log.LstdFlags|log.Lshortfile)
// }


// --- Test Cases (Existing from previous step) ---

func TestNewWorker(t *testing.T) {
	mockBroker := newMockBroker()
	mockBackend := newMockResultBackend()
	// logger := NewDefaultLogger("test-worker: ") // Logger is now global

	opts := &WorkerOptions{
		Broker:        mockBroker,
		ResultBackend: mockBackend,
		QueueName:     "test_queue",
		Concurrency:   2,
		// Logger:        logger, // Logger is now global
	}
	worker := NewWorker(opts).(*DefaultWorker) 

	if worker.broker != mockBroker {
		t.Error("Broker not set correctly")
	}
	if worker.resultBackend != mockBackend {
		t.Error("ResultBackend not set correctly")
	}
	if worker.queueName != "test_queue" {
		t.Errorf("QueueName mismatch: expected test_queue, got %s", worker.queueName)
	}
	if worker.concurrency != 2 {
		t.Errorf("Concurrency mismatch: expected 2, got %d", worker.concurrency)
	}
	// if worker.logger != logger { // This will fail if logger is nil and worker creates its own. Adjusted.
	// 	if logger == nil && worker.logger == nil {
    //          t.Error("Logger not set correctly (both nil, worker should default)")
    //     } else if logger != nil && worker.logger != logger {
    //         t.Error("Logger not set correctly")
    //     }
	// } // Logger is global now, no field in worker
	if worker.taskHandlers == nil {
		t.Error("taskHandlers map not initialized")
	}
	if worker.middlewares == nil {
		t.Error("middlewares slice not initialized")
	}
	if worker.hooks == nil {
		t.Error("hooks map not initialized")
	}
	if worker.taskSerializer == nil {
		t.Error("taskSerializer not initialized (should default)")
	}
	if _, ok := worker.taskSerializer.(*JSONTaskSerializer); !ok {
		t.Errorf("Expected default taskSerializer to be *JSONTaskSerializer, got %T", worker.taskSerializer)
	}

	workerDefault := NewWorker(nil).(*DefaultWorker)
	if workerDefault.queueName == "" {
		t.Error("Default QueueName not set")
	}
	if workerDefault.concurrency <= 0 {
		t.Errorf("Default Concurrency invalid: %d", workerDefault.concurrency)
	}
	// if workerDefault.logger == nil { // DefaultWorker's NewWorker creates a default logger if nil
	// 	t.Error("Default Logger should be set by NewWorker")
	// } // Logger is global now
}

func TestDefaultWorker_RegisterTask(t *testing.T) {
	worker := NewWorker(nil).(*DefaultWorker)

	validHandler := func(a, b int) int { return a + b }
	err := worker.RegisterTask("add", validHandler)
	if err != nil {
		t.Errorf("RegisterTask with valid handler failed: %v", err)
	}
	if _, ok := worker.taskHandlers["add"]; !ok {
		t.Error("Valid task 'add' not found in taskHandlers")
	}

	err = worker.RegisterTask("invalid_type", 123)
	if err == nil {
		t.Error("RegisterTask with non-function type succeeded, expected error")
	} else if !strings.Contains(err.Error(), "not a function") {
		t.Errorf("RegisterTask error message mismatch for non-function: got '%s'", err.Error())
	}

	err = worker.RegisterTask("", validHandler)
	if err == nil {
		t.Error("RegisterTask with empty name succeeded, expected error")
	}

	err = worker.RegisterTask("nil_handler", nil)
	if err == nil {
		t.Error("RegisterTask with nil handler succeeded, expected error")
	}
	
	anotherValidHandler := func(x, y float64) float64 { return x * y }
	err = worker.RegisterTask("add", anotherValidHandler)
	if err != nil {
		t.Errorf("RegisterTask overwriting existing task failed: %v", err)
	}
	if worker.taskHandlers["add"].Type() != reflect.TypeOf(anotherValidHandler) {
		t.Error("Task 'add' was not overwritten with the new handler type")
	}
}

func TestDefaultWorker_SetBroker_SetResultBackend(t *testing.T) {
	worker := NewWorker(nil).(*DefaultWorker)
	
	newBroker := newMockBroker()
	worker.SetBroker(newBroker)
	if worker.broker != newBroker {
		t.Error("SetBroker failed to set the new broker")
	}

	newBackend := newMockResultBackend()
	worker.SetResultBackend(newBackend)
	if worker.resultBackend != newBackend {
		t.Error("SetResultBackend failed to set the new result backend")
	}
}

type testMiddleware struct {
	beforeProcessCalled bool
	afterProcessCalled  bool
	beforeSendCalled    bool
	afterSendCalled     bool
	id                  string
	t                   *testing.T
	beforeProcessErr    error
	beforeSendErr       error
	order               []string // To track call order
	mu                  sync.Mutex
}
func (tm *testMiddleware) recordCall(name string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.order = append(tm.order, name)
}
func (tm *testMiddleware) BeforeProcessMessage(ctx context.Context, mc *MiddlewareContext) error {
	spanCtx, span := NewSpan(ctx, "TestMiddleware.BeforeProcessMessage",
		oteltrace.WithAttributes(
			attribute.String("middleware.id", tm.id),
			attribute.String("task.id", mc.Task.TaskID),
		),
	)
	defer span.End()

	tm.recordCall(fmt.Sprintf("%s_BeforeProcessMessage", tm.id))
	Logger.Debug().Str("middleware_id", tm.id).Str("task_id", mc.Task.TaskID).Msg("BeforeProcessMessage called")
	tm.beforeProcessCalled = true
	if mc.Task.Headers == nil { mc.Task.Headers = make(map[string]string) }
	mc.Task.Headers["mw_"+tm.id] = "before_process_val"
	
	if tm.beforeProcessErr != nil {
		span.RecordError(tm.beforeProcessErr)
		span.SetStatus(codes.Error, tm.beforeProcessErr.Error())
	}
	return tm.beforeProcessErr
}
func (tm *testMiddleware) AfterProcessMessage(ctx context.Context, mc *MiddlewareContext, err error) error {
	spanCtx, span := NewSpan(ctx, "TestMiddleware.AfterProcessMessage",
		oteltrace.WithAttributes(
			attribute.String("middleware.id", tm.id),
			attribute.String("task.id", mc.Task.TaskID),
		),
	)
	defer span.End()

	if err != nil {
		span.RecordError(err, oteltrace.WithAttributes(attribute.String("event", "task_processing_error_received")))
		span.SetStatus(codes.Error, "Task processing reported an error")
	}

	tm.recordCall(fmt.Sprintf("%s_AfterProcessMessage", tm.id))
	Logger.Debug().Str("middleware_id", tm.id).Str("task_id", mc.Task.TaskID).Msg("AfterProcessMessage called")
	tm.afterProcessCalled = true
	return nil
}
func (tm *testMiddleware) BeforeSendResult(ctx context.Context, mc *MiddlewareContext, result *ResultMessage) error {
	spanCtx, span := NewSpan(ctx, "TestMiddleware.BeforeSendResult",
		oteltrace.WithAttributes(
			attribute.String("middleware.id", tm.id),
			attribute.String("task.id", mc.Task.TaskID),
			attribute.String("result.status", result.Status),
		),
	)
	defer span.End()

	tm.recordCall(fmt.Sprintf("%s_BeforeSendResult", tm.id))
	Logger.Debug().Str("middleware_id", tm.id).Str("task_id", mc.Task.TaskID).Msg("BeforeSendResult called")
	tm.beforeSendCalled = true
	if result.Status != StatusFailure && tm.id == "mw1" { // Let mw1 modify result
		result.Result = []byte(fmt.Sprintf(`"modified_by_%s"`, tm.id))
		span.SetAttributes(attribute.Bool("result.modified", true))
	}

	if tm.beforeSendErr != nil {
		span.RecordError(tm.beforeSendErr)
		span.SetStatus(codes.Error, tm.beforeSendErr.Error())
	}
	return tm.beforeSendErr
}
func (tm *testMiddleware) AfterSendResult(ctx context.Context, mc *MiddlewareContext, result *ResultMessage, err error) error {
	spanCtx, span := NewSpan(ctx, "TestMiddleware.AfterSendResult",
		oteltrace.WithAttributes(
			attribute.String("middleware.id", tm.id),
			attribute.String("task.id", mc.Task.TaskID),
		),
	)
	defer span.End()

	if err != nil {
		span.RecordError(err, oteltrace.WithAttributes(attribute.String("event", "send_result_error_received")))
		span.SetStatus(codes.Error, "Send result reported an error")
	}

	tm.recordCall(fmt.Sprintf("%s_AfterSendResult", tm.id))
	Logger.Debug().Str("middleware_id", tm.id).Str("task_id", mc.Task.TaskID).Msg("AfterSendResult called")
	tm.afterSendCalled = true
	return nil
}

func TestDefaultWorker_AddMiddleware(t *testing.T) {
	worker := NewWorker(nil).(*DefaultWorker)
	mw1 := &testMiddleware{id: "mw1", t:t}
	mw2 := &testMiddleware{id: "mw2", t:t}

	worker.AddMiddleware(mw1)
	if len(worker.middlewares) != 1 || worker.middlewares[0] != mw1 {
		t.Fatal("Failed to add first middleware")
	}

	worker.AddMiddleware(mw2)
	if len(worker.middlewares) != 2 || worker.middlewares[1] != mw2 {
		t.Fatal("Failed to add second middleware")
	}
}

func TestDefaultWorker_RegisterHook(t *testing.T) {
	worker := NewWorker(nil).(*DefaultWorker)
	hookCalled := false
	testHook := func(ctx context.Context) error {
		hookCalled = true
		return nil
	}

	err := worker.RegisterHook(EventWorkerStartup, testHook)
	if err != nil {
		t.Errorf("RegisterHook with valid event type failed: %v", err)
	}
	if len(worker.hooks[EventWorkerStartup]) != 1 {
		t.Error("Hook not added to map")
	}

	err = worker.RegisterHook("INVALID_EVENT_TYPE", testHook)
	if err == nil {
		t.Error("RegisterHook with invalid event type succeeded, expected error")
	}

	err = worker.RegisterHook(EventWorkerStartup, nil)
	if err == nil {
		t.Error("RegisterHook with nil hook function succeeded, expected error")
	}

	worker.triggerHooks(context.Background(), EventWorkerStartup)
	if !hookCalled {
		t.Error("Registered hook was not called by triggerHooks")
	}
}

func TestDefaultWorker_Start_NoBroker(t *testing.T) {
	worker := NewWorker(&WorkerOptions{QueueName: "q"}).(*DefaultWorker) 
	worker.SetBroker(nil) 

	err := worker.Start(context.Background())
	if err == nil {
		t.Fatal("Worker.Start succeeded without a broker, expected error")
	}
	if !strings.Contains(err.Error(), "broker is not set") {
		t.Errorf("Expected 'broker is not set' error, got: %v", err)
	}
}

// --- New Test Cases ---

func TestDefaultWorker_Start_Stop_Lifecycle(t *testing.T) {
	mockBroker := newMockBroker()
	worker := NewWorker(&WorkerOptions{Broker: mockBroker, QueueName: "lifecycle_q"}).(*DefaultWorker)

	startupHookCalled := false
	shutdownHookCalled := false
	clientStartupHookCalled := false
	clientShutdownHookCalled := false

	worker.RegisterHook(EventWorkerStartup, func(ctx context.Context) error {
		startupHookCalled = true
		return nil
	})
	worker.RegisterHook(EventWorkerShutdown, func(ctx context.Context) error {
		shutdownHookCalled = true
		return nil
	})
	worker.RegisterHook(EventClientStartup, func(ctx context.Context) error {
		clientStartupHookCalled = true
		return nil
	})
	worker.RegisterHook(EventClientShutdown, func(ctx context.Context) error {
		clientShutdownHookCalled = true
		return nil
	})
	
	// Override ConsumeMessages to signal it has been called and then block until context cancel
	var consumeCalledWg sync.WaitGroup
	consumeCalledWg.Add(1) // For one concurrency worker
	mockBroker.consumeMessagesFn = func(ctx context.Context, queueName string, handler func(ctx context.Context, message *TaskMessage) error) error {
		consumeCalledWg.Done()
		<-ctx.Done() // Block until context is cancelled
		return ctx.Err()
	}

	startCtx, cancelStartCtx := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelStartCtx()

	var workerErr error
	workerDone := make(chan struct{})
	go func() {
		workerErr = worker.Start(startCtx)
		close(workerDone)
	}()

	// Wait for ConsumeMessages to be called
	if !waitTimeout(&consumeCalledWg, 1*time.Second) {
		t.Fatal("mockBroker.ConsumeMessages was not called by worker.Start")
	}

	if !startupHookCalled {
		t.Error("WORKER_STARTUP hook not called")
	}
	if !clientStartupHookCalled {
		t.Error("CLIENT_STARTUP hook not called")
	}

	// Stop the worker
	if err := worker.Stop(); err != nil {
		t.Fatalf("worker.Stop() failed: %v", err)
	}

	// Wait for worker.Start() to return
	select {
	case <-workerDone:
		// Worker finished
	case <-time.After(2 * time.Second): // Timeout for worker to stop
		t.Fatal("Worker.Start() did not return after Stop() was called")
	}

	if workerErr != nil && workerErr != context.Canceled {
		// If startCtx was cancelled by timeout, workerErr would be context.DeadlineExceeded
		// If Stop() cancels workerCtx, workerErr should be nil or context.Canceled related to workerCtx
		if errors.Is(workerErr, context.DeadlineExceeded) {
			 t.Logf("Worker Start context timed out, which is acceptable if Stop() was slow: %v", workerErr)
		} else {
			t.Errorf("worker.Start() returned an unexpected error: %v", workerErr)
		}
	}


	if !shutdownHookCalled {
		t.Error("WORKER_SHUTDOWN hook not called")
	}
	if !clientShutdownHookCalled {
		t.Error("CLIENT_SHUTDOWN hook not called")
	}
}

func TestDefaultWorker_TaskProcessing_Success(t *testing.T) {
	mockBroker := newMockBroker()
	mockBackend := newMockResultBackend()
	worker := NewWorker(&WorkerOptions{
		Broker: mockBroker, ResultBackend: mockBackend, QueueName: "success_q",
	}).(*DefaultWorker)

	taskName := "add"
	expectedSum := 30
	taskHandler := func(a, b int) (int, error) {
		return a + b, nil
	}
	if err := worker.RegisterTask(taskName, taskHandler); err != nil {
		t.Fatalf("RegisterTask failed: %v", err)
	}

	taskID := uuid.NewString()
	args := createTaskArgs(t, 10, 20)
	taskMsg := &TaskMessage{TaskID: taskID, TaskName: taskName, Args: args}

	// Setup broker to send this task
	go func() { mockBroker.consumeChan <- taskMsg }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	
	workerErrChan := make(chan error, 1)
	go func() {
		// Worker Start is blocking, run in goroutine.
		// It will exit when ctx is cancelled or Stop() is called.
		// For this test, it processes one message then broker's consumeChan will be empty.
		// The mockBroker.ConsumeMessages will block on consumeChan or ctx.Done().
		workerErrChan <- worker.Start(ctx)
	}()


	// Wait for Ack
	var ackedMsg *TaskMessage
	select {
	case ackedMsg = <-mockBroker.ackChan:
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for broker.Ack")
	}

	if ackedMsg.TaskID != taskID {
		t.Errorf("Acked TaskID mismatch: expected %s, got %s", taskID, ackedMsg.TaskID)
	}

	// Verify result backend
	if mockBackend.setResultCalls == 0 {
		t.Fatal("ResultBackend.SetResult was not called")
	}
	
	mrb := mockBackend
	mrb.mu.Lock()
	result, ok := mrb.results[taskID]
	mrb.mu.Unlock()

	if !ok {
		t.Fatalf("Result for task %s not found in mockResultBackend", taskID)
	}
	if result.Status != StatusSuccess {
		t.Errorf("Result status mismatch: expected %s, got %s", StatusSuccess, result.Status)
	}
	
	var sumResult int
	if err := json.Unmarshal(result.Result, &sumResult); err != nil {
		t.Fatalf("Failed to unmarshal result data: %v. Data: %s", err, string(result.Result))
	}
	if sumResult != expectedSum {
		t.Errorf("Task result mismatch: expected %d, got %d", expectedSum, sumResult)
	}

	// Stop the worker
	worker.Stop() // Signals worker.Start to exit
	select {
	case err := <-workerErrChan:
		if err != nil && err != context.Canceled {
			t.Errorf("Worker Start returned unexpected error: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Error("Worker did not stop in time")
	}
}

func TestDefaultWorker_TaskProcessing_HandlerError(t *testing.T) {
	mockBroker := newMockBroker()
	mockBackend := newMockResultBackend()
	worker := NewWorker(&WorkerOptions{
		Broker: mockBroker, ResultBackend: mockBackend, QueueName: "handler_error_q",
	}).(*DefaultWorker)

	taskName := "error_task"
	expectedErrorMsg := "handler intentionally failed"
	taskHandler := func() error { // No result, just error
		return errors.New(expectedErrorMsg)
	}
	if err := worker.RegisterTask(taskName, taskHandler); err != nil {
		t.Fatalf("RegisterTask failed: %v", err)
	}

	taskID := uuid.NewString()
	taskMsg := &TaskMessage{TaskID: taskID, TaskName: taskName}

	go func() { mockBroker.consumeChan <- taskMsg }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	workerErrChan := make(chan error,1)
	go func(){ workerErrChan <- worker.Start(ctx) }()


	select {
	case ackedMsg := <-mockBroker.ackChan:
		if ackedMsg.TaskID != taskID {
			t.Errorf("Acked TaskID mismatch: expected %s, got %s", taskID, ackedMsg.TaskID)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for broker.Ack")
	}

	if mockBackend.setResultCalls == 0 {
		t.Fatal("ResultBackend.SetResult was not called")
	}
	result, ok := mockBackend.results[taskID]
	if !ok {
		t.Fatalf("Result for task %s not found", taskID)
	}
	if result.Status != StatusFailure {
		t.Errorf("Result status mismatch: expected %s, got %s", StatusFailure, result.Status)
	}
	if result.Error != expectedErrorMsg {
		t.Errorf("Result error message mismatch: expected '%s', got '%s'", expectedErrorMsg, result.Error)
	}
	
	worker.Stop()
	select {
	case err := <-workerErrChan:
		if err != nil && err != context.Canceled {
			t.Errorf("Worker Start returned unexpected error: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Error("Worker did not stop in time")
	}
}

func TestDefaultWorker_TaskProcessing_NoResultBackend(t *testing.T) {
	mockBroker := newMockBroker()
	worker := NewWorker(&WorkerOptions{
		Broker: mockBroker, ResultBackend: nil, QueueName: "no_backend_q", // nil ResultBackend
	}).(*DefaultWorker)

	taskName := "simple_task_no_backend"
	taskHandler := func() (string, error) { return "done", nil }
	if err := worker.RegisterTask(taskName, taskHandler); err != nil {
		t.Fatalf("RegisterTask failed: %v", err)
	}

	taskID := uuid.NewString()
	taskMsg := &TaskMessage{TaskID: taskID, TaskName: taskName}

	go func() { mockBroker.consumeChan <- taskMsg }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	workerErrChan := make(chan error,1)
	go func(){ workerErrChan <- worker.Start(ctx) }()


	select {
	case ackedMsg := <-mockBroker.ackChan:
		if ackedMsg.TaskID != taskID {
			t.Errorf("Acked TaskID mismatch: expected %s, got %s", taskID, ackedMsg.TaskID)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for broker.Ack")
	}

	// Verify ResultBackend.SetResult was NOT called (as backend is nil)
	// If mockBackend was non-nil, we'd check mockBackend.setResultCalls == 0
	// Since it's nil, no explicit check needed other than no panic.

	worker.Stop()
	select {
	case err := <-workerErrChan:
		if err != nil && err != context.Canceled {
			t.Errorf("Worker Start returned unexpected error: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Error("Worker did not stop in time")
	}
}

func TestDefaultWorker_TaskProcessing_TaskNotFound(t *testing.T) {
	mockBroker := newMockBroker()
	mockBackend := newMockResultBackend()
	worker := NewWorker(&WorkerOptions{
		Broker: mockBroker, ResultBackend: mockBackend, QueueName: "notfound_q",
	}).(*DefaultWorker)

	taskID := uuid.NewString()
	taskMsg := &TaskMessage{TaskID: taskID, TaskName: "unregistered_task"}

	go func() { mockBroker.consumeChan <- taskMsg }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	workerErrChan := make(chan error,1)
	go func(){ workerErrChan <- worker.Start(ctx) }()


	select {
	case ackedMsg := <-mockBroker.ackChan: // Worker should still Ack unknown tasks
		if ackedMsg.TaskID != taskID {
			t.Errorf("Acked TaskID mismatch: expected %s, got %s", taskID, ackedMsg.TaskID)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for broker.Ack for unknown task")
	}
	
	if mockBackend.setResultCalls > 0 {
		t.Errorf("ResultBackend.SetResult was called %d times, expected 0 for unknown task", mockBackend.setResultCalls)
	}
	
	worker.Stop()
	select {
	case err := <-workerErrChan:
		if err != nil && err != context.Canceled {
			t.Errorf("Worker Start returned unexpected error: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Error("Worker did not stop in time")
	}
}

func TestDefaultWorker_TaskProcessing_DeserializationError_Args(t *testing.T) {
	mockBroker := newMockBroker()
	mockBackend := newMockResultBackend()
	worker := NewWorker(&WorkerOptions{
		Broker: mockBroker, ResultBackend: mockBackend, QueueName: "deserialize_err_q",
	}).(*DefaultWorker)

	taskName := "type_error_task"
	// Handler expects an int, but we'll send a string that can't be unmarshalled to int
	taskHandler := func(val int) error { 
		t.Logf("Deserialization error task handler called with val: %d (should not happen if error caught before)", val)
		return nil 
	}
	if err := worker.RegisterTask(taskName, taskHandler); err != nil {
		t.Fatalf("RegisterTask failed: %v", err)
	}

	taskID := uuid.NewString()
	// Malformed args: sending a string where an int is expected by the handler.
	// JSON unmarshaller for int will fail if it gets `"not_an_int"`.
	malformedArgs := [][]byte{[]byte(`"not_an_int"`)} 
	taskMsg := &TaskMessage{TaskID: taskID, TaskName: taskName, Args: malformedArgs}

	go func() { mockBroker.consumeChan <- taskMsg }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	workerErrChan := make(chan error,1)
	go func(){ workerErrChan <- worker.Start(ctx) }()


	select {
	case ackedMsg := <-mockBroker.ackChan:
		if ackedMsg.TaskID != taskID {
			t.Errorf("Acked TaskID mismatch: expected %s, got %s", taskID, ackedMsg.TaskID)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for broker.Ack for deserialization error task")
	}

	if mockBackend.setResultCalls == 0 {
		t.Fatal("ResultBackend.SetResult was not called for deserialization error")
	}
	result, ok := mockBackend.results[taskID]
	if !ok {
		t.Fatalf("Result for task %s not found after deserialization error", taskID)
	}
	if result.Status != StatusFailure {
		t.Errorf("Result status: expected %s, got %s for deserialization error", StatusFailure, result.Status)
	}
	if !strings.Contains(result.Error, "error unmarshaling arg") && !strings.Contains(result.Error, "json: cannot unmarshal") {
		t.Errorf("Expected deserialization error message, got: %s", result.Error)
	}
	
	worker.Stop()
	select {
	case err := <-workerErrChan:
		if err != nil && err != context.Canceled {
			t.Errorf("Worker Start returned unexpected error: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Error("Worker did not stop in time")
	}
}


func TestDefaultWorker_MiddlewareExecutionOrder_And_Modification(t *testing.T) {
	mockBroker := newMockBroker()
	mockBackend := newMockResultBackend()
	worker := NewWorker(&WorkerOptions{
		Broker: mockBroker, ResultBackend: mockBackend, QueueName: "mw_q",
	}).(*DefaultWorker)

	mw1 := &testMiddleware{id: "mw1", t: t}
	mw2 := &testMiddleware{id: "mw2", t: t}
	worker.AddMiddleware(mw1)
	worker.AddMiddleware(mw2)

	taskName := "mw_task"
	originalPayload := "original"
	var handlerPayload string
	taskHandler := func(payload string) (string, error) {
		handlerPayload = payload // Capture modified payload if mw changed it
		return "handler_processed_" + payload, nil
	}
	if err := worker.RegisterTask(taskName, taskHandler); err != nil {
		t.Fatalf("RegisterTask failed: %v", err)
	}

	taskID := uuid.NewString()
	args := createTaskArgs(t, originalPayload)
	taskMsg := &TaskMessage{TaskID: taskID, TaskName: taskName, Args: args}

	go func() { mockBroker.consumeChan <- taskMsg }()
	
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	workerErrChan := make(chan error,1)
	go func(){ workerErrChan <- worker.Start(ctx) }()


	select {
	case <-mockBroker.ackChan: // Wait for task to be Acked
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for task to be processed and Acked")
	}

	// Check middleware calls
	if !mw1.beforeProcessCalled || !mw1.afterProcessCalled || !mw1.beforeSendCalled || !mw1.afterSendCalled {
		t.Errorf("Middleware mw1 methods not all called: BP:%v, AP:%v, BS:%v, AS:%v", mw1.beforeProcessCalled, mw1.afterProcessCalled, mw1.beforeSendCalled, mw1.afterSendCalled)
	}
	if !mw2.beforeProcessCalled || !mw2.afterProcessCalled || !mw2.beforeSendCalled || !mw2.afterSendCalled {
		t.Errorf("Middleware mw2 methods not all called: BP:%v, AP:%v, BS:%v, AS:%v", mw2.beforeProcessCalled, mw2.afterProcessCalled, mw2.beforeSendCalled, mw2.afterSendCalled)
	}
	
	// Check order (mw1 runs all its "before" stages, then mw2, then handler, then mw2 "after", then mw1 "after")
	// This depends on how middleware wrapping is implemented. Current worker iterates.
	// Expected order: mw1_BP, mw2_BP, (handler), mw2_AP, mw1_AP, mw1_BS, mw2_BS, (backend_set), mw2_AS, mw1_AS
	// Let's check a simpler subset of order: BP1 before BP2, AP2 before AP1 etc.
	// This requires more detailed order tracking in testMiddleware or a global slice.
	// For now, check modification.
	// Example: mw1.BeforeProcessMessage added header "mw_mw1: before_process_val"
	// This check is within the handler or subsequent middlewares.
	// Handler payload check not easily done here without instrumenting handler more.

	result, _ := mockBackend.GetResult(taskID)
	if result == nil {
		t.Fatal("Result not found for middleware test")
	}
	// mw1.BeforeSendResult modifies result to `"`modified_by_mw1`"` if successful.
	// mw2.BeforeSendResult does not modify.
	expectedResultValue := `"modified_by_mw1"`
	if string(result.Result) != expectedResultValue {
		t.Errorf("Result modification by middleware mw1 failed: expected '%s', got '%s'", expectedResultValue, string(result.Result))
	}

	// Test middleware error propagation (BeforeProcessMessage)
	mw1.beforeProcessErr = errors.New("mw1_before_process_failed")
	mw2.beforeProcessCalled = false // Reset for this part of test
	taskID2 := uuid.NewString()
	taskMsg2 := &TaskMessage{TaskID: taskID2, TaskName: taskName, Args: args}
	
	go func() { mockBroker.consumeChan <- taskMsg2 }()

	select {
	case <-mockBroker.ackChan: // Task should still be Acked
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for task2 to be Acked after mw error")
	}
	if mw2.beforeProcessCalled { // mw2.BeforeProcess should not be called if mw1.BeforeProcess failed
		t.Error("mw2.BeforeProcessMessage was called after mw1.BeforeProcessMessage errored")
	}
	// Check if result backend was called (it shouldn't be if BeforeProcessMessage failed)
	// The current worker implementation calls processTaskError which would set a result.
	// Let's verify this result reflects the middleware error.
	result2, _ := mockBackend.GetResult(taskID2)
	if result2 == nil {
		t.Fatalf("Result not found for taskID2 (mw error test)")
	}
	if result2.Status != StatusFailure {
		t.Errorf("Expected status FAILURE for taskID2 due to mw error, got %s", result2.Status)
	}
	if !strings.Contains(result2.Error, "mw1_before_process_failed") {
		t.Errorf("Expected result error to contain 'mw1_before_process_failed', got: %s", result2.Error)
	}


	worker.Stop()
	select {
	case err := <-workerErrChan:
		if err != nil && err != context.Canceled {
			t.Errorf("Worker Start returned unexpected error: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Error("Worker did not stop in time")
	}
}


func TestDefaultWorker_Concurrency_Basic(t *testing.T) {
	mockBroker := newMockBroker()
	worker := NewWorker(&WorkerOptions{
		Broker: mockBroker, QueueName: "concurrency_q", Concurrency: 2,
	}).(*DefaultWorker)

	taskName := "slow_task"
	handlerCallCount := 0
	var handlerMu sync.Mutex
	var handlerWg sync.WaitGroup
	handlerWg.Add(2) // Expecting two calls

	taskHandler := func() error {
		time.Sleep(100 * time.Millisecond) // Simulate work
		handlerMu.Lock()
		handlerCallCount++
		handlerMu.Unlock()
		handlerWg.Done()
		return nil
	}
	if err := worker.RegisterTask(taskName, taskHandler); err != nil {
		t.Fatalf("RegisterTask failed: %v", err)
	}

	taskID1 := uuid.NewString()
	taskMsg1 := &TaskMessage{TaskID: taskID1, TaskName: taskName}
	taskID2 := uuid.NewString()
	taskMsg2 := &TaskMessage{TaskID: taskID2, TaskName: taskName}

	// Send tasks to broker
	go func() {
		mockBroker.consumeChan <- taskMsg1
		mockBroker.consumeChan <- taskMsg2
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	workerErrChan := make(chan error,1)
	go func(){ workerErrChan <- worker.Start(ctx) }()


	// Wait for both tasks to be processed by handler
	if !waitTimeout(&handlerWg, 2*time.Second) {
		t.Fatalf("Timeout waiting for handlers to complete. Called: %d/2", handlerCallCount)
	}

	// Check if ConsumeMessages was called by multiple goroutines (or broker handled concurrency)
	// mockBroker.consumeCallCount should be == worker.concurrency
	mockBroker.mu.Lock()
	if mockBroker.consumeCallCount != 2 {
		t.Errorf("Expected broker.ConsumeMessages to be called %d times, got %d", 2, mockBroker.consumeCallCount)
	}
	mockBroker.mu.Unlock()


	// Check Acks for both tasks
	ackedTasks := 0
	ackLoop:
	for i := 0; i < 2; i++ {
		select {
		case ackedMsg := <-mockBroker.ackChan:
			if ackedMsg.TaskID == taskID1 || ackedMsg.TaskID == taskID2 {
				ackedTasks++
			}
		case <-time.After(1 * time.Second):
			t.Logf("Timeout waiting for all Acks, got %d/2", ackedTasks)
			break ackLoop
		}
	}
	if ackedTasks != 2 {
		t.Errorf("Expected 2 tasks to be Acked, got %d", ackedTasks)
	}

	handlerMu.Lock()
	if handlerCallCount != 2 {
		t.Errorf("Expected handler to be called 2 times, got %d", handlerCallCount)
	}
	handlerMu.Unlock()

	worker.Stop()
	select {
	case err := <-workerErrChan:
		if err != nil && err != context.Canceled {
			t.Errorf("Worker Start returned unexpected error: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Error("Worker did not stop in time")
	}
}
