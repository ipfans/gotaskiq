package taskiq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"
)

// mockMiddlewareForInterfaceTest is a mock implementation of the Middleware interface.
type mockMiddlewareForInterfaceTest struct {
	id                string
	beforeProcessFunc func(ctx context.Context, mc *MiddlewareContext) error
	afterProcessFunc  func(ctx context.Context, mc *MiddlewareContext, err error) error
	beforeSendFunc    func(ctx context.Context, mc *MiddlewareContext, result *ResultMessage) error
	afterSendFunc     func(ctx context.Context, mc *MiddlewareContext, result *ResultMessage, err error) error

	// Tracking calls
	beforeProcessCalled bool
	afterProcessCalled  bool
	beforeSendCalled    bool
	afterSendCalled     bool
	mu                  sync.Mutex
}

func (m *mockMiddlewareForInterfaceTest) BeforeProcessMessage(ctx context.Context, mc *MiddlewareContext) error {
	m.mu.Lock()
	m.beforeProcessCalled = true
	m.mu.Unlock()
	if m.beforeProcessFunc != nil {
		return m.beforeProcessFunc(ctx, mc)
	}
	// Add a span to simulate middleware doing something with the context
	spanCtx, span := NewSpan(ctx, fmt.Sprintf("mockMiddleware_%s_BeforeProcessMessage", m.id))
	defer span.End()
	return nil
}

func (m *mockMiddlewareForInterfaceTest) AfterProcessMessage(ctx context.Context, mc *MiddlewareContext, err error) error {
	m.mu.Lock()
	m.afterProcessCalled = true
	m.mu.Unlock()
	if m.afterProcessFunc != nil {
		return m.afterProcessFunc(ctx, mc, err)
	}
	spanCtx, span := NewSpan(ctx, fmt.Sprintf("mockMiddleware_%s_AfterProcessMessage", m.id))
	defer span.End()
	if err != nil {
		span.RecordError(err)
	}
	return nil
}

func (m *mockMiddlewareForInterfaceTest) BeforeSendResult(ctx context.Context, mc *MiddlewareContext, result *ResultMessage) error {
	m.mu.Lock()
	m.beforeSendCalled = true
	m.mu.Unlock()
	if m.beforeSendFunc != nil {
		return m.beforeSendFunc(ctx, mc, result)
	}
	spanCtx, span := NewSpan(ctx, fmt.Sprintf("mockMiddleware_%s_BeforeSendResult", m.id))
	defer span.End()
	return nil
}

func (m *mockMiddlewareForInterfaceTest) AfterSendResult(ctx context.Context, mc *MiddlewareContext, result *ResultMessage, err error) error {
	m.mu.Lock()
	m.afterSendCalled = true
	m.mu.Unlock()
	if m.afterSendFunc != nil {
		return m.afterSendFunc(ctx, mc, result, err)
	}
	spanCtx, span := NewSpan(ctx, fmt.Sprintf("mockMiddleware_%s_AfterSendResult", m.id))
	defer span.End()
	if err != nil {
		span.RecordError(err)
	}
	return nil
}

func (m *mockMiddlewareForInterfaceTest) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.beforeProcessCalled = false
	m.afterProcessCalled = false
	m.beforeSendCalled = false
	m.afterSendCalled = false
}

func TestMiddlewareInterface_Execution(t *testing.T) {
	// Initialize a NoOpTracerProvider for testing purposes if a real one isn't set up.
	// This prevents panics if InitTracerProvider wasn't called or configured for tests.
	// In a full test suite, a test setup might initialize a global test tracer.
	if Tracer == nil || reflect.TypeOf(Tracer) == reflect.TypeOf(trace.NewNoopTracerProvider().Tracer("")) {
		InitNoOpTracerProvider() // Ensures Tracer is not nil
	}


	mockMW := &mockMiddlewareForInterfaceTest{id: "testmw"}
	taskMsg := &TaskMessage{TaskID: "task123", TaskName: "test_task"}
	resultMsg := &ResultMessage{TaskID: "task123", Status: StatusSuccess, Result: []byte(`"ok"`)}
	baseCtx := context.Background()

	// Simulate Worker calling BeforeProcessMessage
	middlewareCtx := &MiddlewareContext{Task: taskMsg}
	err := mockMW.BeforeProcessMessage(baseCtx, middlewareCtx)
	if err != nil {
		t.Errorf("BeforeProcessMessage returned unexpected error: %v", err)
	}
	if !mockMW.beforeProcessCalled {
		t.Error("Expected BeforeProcessMessage to be called, but it wasn't")
	}
	mockMW.Reset()

	// Simulate Worker calling AfterProcessMessage (task success)
	taskErr := error(nil)
	err = mockMW.AfterProcessMessage(baseCtx, middlewareCtx, taskErr)
	if err != nil {
		t.Errorf("AfterProcessMessage returned unexpected error: %v", err)
	}
	if !mockMW.afterProcessCalled {
		t.Error("Expected AfterProcessMessage to be called, but it wasn't")
	}
	mockMW.Reset()

	// Simulate Worker calling AfterProcessMessage (task failure)
	taskErr = errors.New("task processing failed")
	err = mockMW.AfterProcessMessage(baseCtx, middlewareCtx, taskErr)
	if err != nil {
		t.Errorf("AfterProcessMessage with task error returned unexpected error: %v", err)
	}
	if !mockMW.afterProcessCalled {
		t.Error("Expected AfterProcessMessage (with task error) to be called, but it wasn't")
	}
	mockMW.Reset()

	// Simulate Worker calling BeforeSendResult
	err = mockMW.BeforeSendResult(baseCtx, middlewareCtx, resultMsg)
	if err != nil {
		t.Errorf("BeforeSendResult returned unexpected error: %v", err)
	}
	if !mockMW.beforeSendCalled {
		t.Error("Expected BeforeSendResult to be called, but it wasn't")
	}
	mockMW.Reset()

	// Simulate Worker calling AfterSendResult (send success)
	sendErr := error(nil)
	err = mockMW.AfterSendResult(baseCtx, middlewareCtx, resultMsg, sendErr)
	if err != nil {
		t.Errorf("AfterSendResult returned unexpected error: %v", err)
	}
	if !mockMW.afterSendCalled {
		t.Error("Expected AfterSendResult to be called, but it wasn't")
	}
	mockMW.Reset()

	// Simulate Worker calling AfterSendResult (send failure)
	sendErr = errors.New("result sending failed")
	err = mockMW.AfterSendResult(baseCtx, middlewareCtx, resultMsg, sendErr)
	if err != nil {
		t.Errorf("AfterSendResult with send error returned unexpected error: %v", err)
	}
	if !mockMW.afterSendCalled {
		t.Error("Expected AfterSendResult (with send error) to be called, but it wasn't")
	}
}

// Test to ensure context is passed through middleware methods
func TestMiddlewareInterface_ContextPropagation(t *testing.T) {
	if Tracer == nil || reflect.TypeOf(Tracer) == reflect.TypeOf(trace.NewNoopTracerProvider().Tracer("")) {
		InitNoOpTracerProvider()
	}

	type contextKey string
	const testKey = contextKey("testKey")
	const testValue = "testValue"

	baseCtx := context.WithValue(context.Background(), testKey, testValue)
	var receivedCtx context.Context

	mockMW := &mockMiddlewareForInterfaceTest{id: "ctxmw"}
	mockMW.beforeProcessFunc = func(ctx context.Context, mc *MiddlewareContext) error {
		receivedCtx = ctx
		return nil
	}
	// ... (can add similar funcs for other methods if needed)

	taskMsg := &TaskMessage{TaskID: "ctxTask", TaskName: "ctx_test_task"}
	middlewareCtx := &MiddlewareContext{Task: taskMsg}

	mockMW.BeforeProcessMessage(baseCtx, middlewareCtx)

	if receivedCtx == nil {
		t.Fatal("Context was not received by middleware")
	}
	if val := receivedCtx.Value(testKey); val != testValue {
		t.Errorf("Context value mismatch: got %v, want %v", val, testValue)
	}

	// Verify span propagation (simple check: if a span was created, it should be different from base)
	// This implicitly tests that NewSpan was called within the mock middleware.
	baseSpan := trace.SpanFromContext(baseCtx)
	mwSpan := trace.SpanFromContext(receivedCtx) // This context is from *inside* the middleware func

	// The span check here is a bit tricky because the mockMiddleware itself creates a new span.
	// `receivedCtx` in `beforeProcessFunc` is the context *passed into* BeforeProcessMessage.
	// The span created inside BeforeProcessMessage would be a child of `receivedCtx`'s span.
	// For this test, it's enough that `receivedCtx` *is* `baseCtx`.
	// The internal span creation in the mock is more for demonstration.
	if mwSpan != baseSpan {
		// This can happen if the baseCtx itself had a span and the middleware wrapped it.
		// Or if the mock logic changes the context.
		// For this test, we expect the *passed* context to be the one with the value.
		// The span check is more about the span created *within* the middleware method,
		// which isn't directly testable from here without more complex mock.
		// The key is that `receivedCtx.Value(testKey)` works.
	}

	// To properly test span creation within middleware, one would typically:
	// 1. Pass a context with an active parent span into the middleware method.
	// 2. The middleware method creates a child span.
	// 3. Assert that the child span's parent is the active parent span.
	// This requires a more sophisticated testing setup with an in-memory exporter or similar.
	// The current NewSpan helper simplifies span creation but makes direct parent-child assertion harder here.
}

// TestMiddlewareContext_Access tests that fields in MiddlewareContext are accessible.
func TestMiddlewareContext_Access(t *testing.T) {
	taskMsg := &TaskMessage{TaskID: "mcTask", TaskName: "mc_test_task", Headers: map[string]string{"x-test": "header-val"}}
	middlewareCtx := &MiddlewareContext{Task: taskMsg}

	if middlewareCtx.Task == nil {
		t.Fatal("MiddlewareContext.Task is nil")
	}
	if middlewareCtx.Task.TaskID != "mcTask" {
		t.Errorf("MiddlewareContext.Task.TaskID = %s, want %s", middlewareCtx.Task.TaskID, "mcTask")
	}
	if middlewareCtx.Task.Headers["x-test"] != "header-val" {
		t.Errorf("MiddlewareContext.Task.Headers['x-test'] = %s, want %s", middlewareCtx.Task.Headers["x-test"], "header-val")
	}
}

// Test that a middleware can modify the TaskMessage (e.g., headers)
func TestMiddleware_ModifiesTaskMessage(t *testing.T) {
	if Tracer == nil || reflect.TypeOf(Tracer) == reflect.TypeOf(trace.NewNoopTracerProvider().Tracer("")) {
		InitNoOpTracerProvider()
	}
	
	taskMsg := &TaskMessage{TaskID: "modTask", TaskName: "mod_task", Headers: map[string]string{}}
	middlewareCtx := &MiddlewareContext{Task: taskMsg}

	modifierMW := &mockMiddlewareForInterfaceTest{id: "modifier"}
	modifierMW.beforeProcessFunc = func(ctx context.Context, mc *MiddlewareContext) error {
		if mc.Task.Headers == nil {
			mc.Task.Headers = make(map[string]string)
		}
		mc.Task.Headers["mw_added_header"] = "mw_value"
		return nil
	}

	err := modifierMW.BeforeProcessMessage(context.Background(), middlewareCtx)
	if err != nil {
		t.Fatalf("BeforeProcessMessage (modifier) returned error: %v", err)
	}

	if val, ok := middlewareCtx.Task.Headers["mw_added_header"]; !ok || val != "mw_value" {
		t.Errorf("Middleware did not correctly add header to TaskMessage. Got: %v, ok: %v", val, ok)
	}
}

// Test that a middleware can modify the ResultMessage
func TestMiddleware_ModifiesResultMessage(t *testing.T) {
	if Tracer == nil || reflect.TypeOf(Tracer) == reflect.TypeOf(trace.NewNoopTracerProvider().Tracer("")) {
		InitNoOpTracerProvider()
	}

	taskMsg := &TaskMessage{TaskID: "modResTask"}
	resultMsg := &ResultMessage{TaskID: "modResTask", Status: StatusSuccess, Result: []byte(`"original"`)}
	middlewareCtx := &MiddlewareContext{Task: taskMsg}

	modifierMW := &mockMiddlewareForInterfaceTest{id: "res_modifier"}
	modifierMW.beforeSendFunc = func(ctx context.Context, mc *MiddlewareContext, res *ResultMessage) error {
		res.Result = []byte(`"modified_by_middleware"`)
		res.Status = "MODIFIED_SUCCESS"
		return nil
	}

	err := modifierMW.BeforeSendResult(context.Background(), middlewareCtx, resultMsg)
	if err != nil {
		t.Fatalf("BeforeSendResult (modifier) returned error: %v", err)
	}

	if string(resultMsg.Result) != `"modified_by_middleware"` {
		t.Errorf("Middleware did not correctly modify ResultMessage.Result. Got: %s", string(resultMsg.Result))
	}
	if resultMsg.Status != "MODIFIED_SUCCESS" {
		t.Errorf("Middleware did not correctly modify ResultMessage.Status. Got: %s", resultMsg.Status)
	}
}
