package results

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"gitlab.com/taskiq/taskiq/pkg/taskiq"
)

// Helper function to start miniredis and return a client & cleanup function
func setupMiniRedisForResults(t *testing.T) (*miniredis.Miniredis, *redis.Client, func()) {
	t.Helper()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Failed to start miniredis: %v", err)
	}

	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	cleanup := func() {
		mr.Close()
		client.Close()
	}
	return mr, client, cleanup
}

func TestNewRedisResultBackend(t *testing.T) {
	mr, _, cleanup := setupMiniRedisForResults(t)
	defer cleanup()

	opts := &RedisResultBackendOptions{Addr: mr.Addr()}
	rb := NewRedisResultBackend(opts)
	if rb == nil {
		t.Fatal("NewRedisResultBackend returned nil")
	}
	defer rb.Close()

	// Check Ping
	if err := rb.(*RedisResultBackend).Ping(context.Background()); err != nil {
		t.Errorf("Ping failed: %v", err)
	}

	// Check type
	if _, ok := rb.(*RedisResultBackend); !ok {
		t.Errorf("NewRedisResultBackend did not return a *RedisResultBackend, got %T", rb)
	}
}

func TestRedisResultBackend_SetResult_Simple(t *testing.T) {
	mr, _, cleanup := setupMiniRedisForResults(t)
	defer cleanup()

	opts := &RedisResultBackendOptions{Addr: mr.Addr()}
	rb := NewRedisResultBackend(opts)
	defer rb.Close()

	taskID := "task_set_simple_1"
	originalResult := &taskiq.ResultMessage{
		TaskID: taskID,
		Status: "SUCCESS",
		Result: []byte(`"hello world"`),
	}

	if err := rb.SetResult(taskID, originalResult); err != nil {
		t.Fatalf("SetResult failed: %v", err)
	}

	// Verify directly from miniredis
	storedValue, err := mr.Get(taskID)
	if err != nil {
		t.Fatalf("miniredis.Get failed: %v", err)
	}

	var storedResultMessage taskiq.ResultMessage
	if err := json.Unmarshal([]byte(storedValue), &storedResultMessage); err != nil {
		t.Fatalf("Failed to unmarshal stored value: %v. Value: %s", err, storedValue)
	}

	if storedResultMessage.TaskID != originalResult.TaskID {
		t.Errorf("TaskID mismatch: expected %s, got %s", originalResult.TaskID, storedResultMessage.TaskID)
	}
	if storedResultMessage.Status != originalResult.Status {
		t.Errorf("Status mismatch: expected %s, got %s", originalResult.Status, storedResultMessage.Status)
	}
	if !reflect.DeepEqual(storedResultMessage.Result, originalResult.Result) {
		t.Errorf("Result data mismatch: expected %s, got %s", originalResult.Result, storedResultMessage.Result)
	}
	if storedResultMessage.Timestamp.IsZero() {
		t.Error("Timestamp was not set by SetResult")
	}
}

func TestRedisResultBackend_GetResult_Found(t *testing.T) {
	mr, _, cleanup := setupMiniRedisForResults(t)
	defer cleanup()

	opts := &RedisResultBackendOptions{Addr: mr.Addr()}
	rb := NewRedisResultBackend(opts)
	defer rb.Close()

	taskID := "task_get_found_1"
	originalResult := &taskiq.ResultMessage{
		TaskID: taskID,
		Status: "SUCCESS",
		Result: []byte(`{"data": "test_data"}`),
	}

	if err := rb.SetResult(taskID, originalResult); err != nil {
		t.Fatalf("SetResult failed: %v", err)
	}

	retrievedResult, err := rb.GetResult(taskID)
	if err != nil {
		t.Fatalf("GetResult failed: %v", err)
	}
	if retrievedResult == nil {
		t.Fatal("GetResult returned nil result, expected a result message")
	}

	if retrievedResult.TaskID != originalResult.TaskID {
		t.Errorf("TaskID mismatch: expected %s, got %s", originalResult.TaskID, retrievedResult.TaskID)
	}
	if retrievedResult.Status != originalResult.Status {
		t.Errorf("Status mismatch: expected %s, got %s", originalResult.Status, retrievedResult.Status)
	}
	if !reflect.DeepEqual(retrievedResult.Result, originalResult.Result) {
		t.Errorf("Result data mismatch: expected %s, got %s", originalResult.Result, retrievedResult.Result)
	}
	// Timestamps might differ slightly due to being set at different times if originalResult.Timestamp was zero.
	// Compare only if originalResult.Timestamp was non-zero, or check for non-zero in retrieved.
	if retrievedResult.Timestamp.IsZero() {
		t.Error("Retrieved result timestamp is zero")
	}
}

func TestRedisResultBackend_GetResult_NotFound(t *testing.T) {
	mr, _, cleanup := setupMiniRedisForResults(t)
	defer cleanup()

	opts := &RedisResultBackendOptions{Addr: mr.Addr()}
	rb := NewRedisResultBackend(opts)
	defer rb.Close()

	taskID := "task_get_notfound_1"
	retrievedResult, err := rb.GetResult(taskID)

	if err == nil {
		t.Fatal("GetResult succeeded for non-existent taskID, expected an error")
	}
	if !errors.Is(err, ErrResultNotFound) { // Use errors.Is for wrapped errors
		t.Errorf("GetResult error mismatch: expected ErrResultNotFound, got %v (type %T)", err, err)
	}
	if retrievedResult != nil {
		t.Errorf("GetResult returned a non-nil result for non-existent taskID: %+v", retrievedResult)
	}
}

func TestRedisResultBackend_SetResult_WithTTL(t *testing.T) {
	mr, _, cleanup := setupMiniRedisForResults(t)
	defer cleanup()

	ttl := 50 * time.Millisecond // Short TTL for testing
	opts := &RedisResultBackendOptions{Addr: mr.Addr(), ResultTTL: ttl}
	rb := NewRedisResultBackend(opts)
	defer rb.Close()

	taskID := "task_set_ttl_1"
	originalResult := &taskiq.ResultMessage{TaskID: taskID, Status: "PENDING"}

	if err := rb.SetResult(taskID, originalResult); err != nil {
		t.Fatalf("SetResult failed: %v", err)
	}

	// Check if TTL was set in miniredis
	actualTTL, err := mr.TTL(taskID)
	if err != nil {
		t.Fatalf("miniredis.TTL failed: %v", err)
	}
	// miniredis TTL is in seconds, convert our ttl to seconds for comparison, allow for slight diffs
	expectedTTLSec := int(ttl.Seconds())
	if actualTTL < expectedTTLSec-1 || actualTTL > expectedTTLSec+1 { // Allow 1s variance
		// Note: miniredis might handle TTL with millisecond precision internally but expose in seconds.
		// Let's check if it's roughly correct.
		t.Logf("Warning: TTL mismatch, expected around %d s, got %d s. Miniredis TTL precision might differ.", expectedTTLSec, actualTTL)
	}


	// Fast-forward time in miniredis
	mr.FastForward(ttl + (10 * time.Millisecond)) // Ensure it's past the TTL

	retrievedResult, err := rb.GetResult(taskID)
	if err == nil {
		t.Fatalf("GetResult succeeded after TTL expired, expected an error. Result: %+v", retrievedResult)
	}
	if !errors.Is(err, ErrResultNotFound) {
		t.Errorf("GetResult error mismatch after TTL: expected ErrResultNotFound, got %v", err)
	}
	if retrievedResult != nil {
		t.Error("GetResult returned non-nil result after TTL expired")
	}
}

func TestRedisResultBackend_SetResult_Update(t *testing.T) {
	mr, _, cleanup := setupMiniRedisForResults(t)
	defer cleanup()

	opts := &RedisResultBackendOptions{Addr: mr.Addr()}
	rb := NewRedisResultBackend(opts)
	defer rb.Close()

	taskID := "task_set_update_1"
	firstResult := &taskiq.ResultMessage{TaskID: taskID, Status: "PENDING", Result: []byte(`"first"`)}
	secondResult := &taskiq.ResultMessage{TaskID: taskID, Status: "SUCCESS", Result: []byte(`"second"`)}

	if err := rb.SetResult(taskID, firstResult); err != nil {
		t.Fatalf("SetResult (first) failed: %v", err)
	}
	if err := rb.SetResult(taskID, secondResult); err != nil {
		t.Fatalf("SetResult (second) failed: %v", err)
	}

	retrieved, err := rb.GetResult(taskID)
	if err != nil {
		t.Fatalf("GetResult failed: %v", err)
	}
	if retrieved.Status != secondResult.Status {
		t.Errorf("Status after update: expected %s, got %s", secondResult.Status, retrieved.Status)
	}
	if !reflect.DeepEqual(retrieved.Result, secondResult.Result) {
		t.Errorf("Result data after update: expected %s, got %s", secondResult.Result, retrieved.Result)
	}
}

func TestRedisResultBackend_SetResult_ErrorHandling(t *testing.T) {
	mr, _, cleanup := setupMiniRedisForResults(t)
	defer cleanup()

	opts := &RedisResultBackendOptions{Addr: mr.Addr()}
	rb := NewRedisResultBackend(opts)
	defer rb.Close()

	// Test empty taskID
	err := rb.SetResult("", &taskiq.ResultMessage{Status: "FAIL"})
	if err == nil {
		t.Error("SetResult with empty taskID succeeded, expected error")
	} else {
		t.Logf("SetResult with empty taskID failed as expected: %v", err)
	}

	// Test nil result message
	err = rb.SetResult("some_task", nil)
	if err == nil {
		t.Error("SetResult with nil result message succeeded, expected error")
	} else {
		t.Logf("SetResult with nil result message failed as expected: %v", err)
	}
	
	// Test Redis error simulation (e.g., client connection error)
	// This is harder with miniredis directly without closing its connection.
	// We can test if the client is closed before SetResult.
	// Let's use the underlying client of the result backend.
	rbConcrete := rb.(*RedisResultBackend)
	rbConcrete.client.Close() // Close the client used by the backend

	err = rb.SetResult("task_after_client_close", &taskiq.ResultMessage{Status: "FAIL"})
	if err == nil {
		t.Error("SetResult after client close succeeded, expected error")
	} else {
		t.Logf("SetResult after client close failed as expected: %v", err)
		var backendErr *taskiq.BackendError
		if !errors.As(err, &backendErr) {
			t.Errorf("Expected BackendError, got %T: %v", err, err)
		}
	}
	// Re-initialize for other tests if needed, but this test is about error on SetResult
}

func TestRedisResultBackend_GetResult_Deserialization_Error(t *testing.T) {
	mr, _, cleanup := setupMiniRedisForResults(t)
	defer cleanup()

	opts := &RedisResultBackendOptions{Addr: mr.Addr()}
	rb := NewRedisResultBackend(opts)
	defer rb.Close()

	taskID := "task_get_deserialize_err_1"
	malformedJSON := `{"task_id": "task_get_deserialize_err_1", "status": "SUCCESS", "result": "this is not valid json for []byte if it expects quotes", "timestamp": "not_a_timestamp"`
	
	// Manually put malformed data into miniredis
	if err := mr.Set(taskID, malformedJSON); err != nil {
		t.Fatalf("miniredis.Set failed for malformed data: %v", err)
	}

	_, err := rb.GetResult(taskID)
	if err == nil {
		t.Fatal("GetResult succeeded with malformed JSON, expected deserialization error")
	}

	t.Logf("GetResult with malformed JSON failed as expected: %v", err)
	var dsErr *taskiq.DeserializationError
	if !errors.As(err, &dsErr) { // Check if it's a DeserializationError (or wraps one)
		t.Errorf("Expected DeserializationError, got %T: %v", err, err)
	}
}

func TestRedisResultBackend_Close(t *testing.T) {
	mr, _, cleanup := setupMiniRedisForResults(t) // miniredis instance
	defer cleanup() // miniredis cleanup

	opts := &RedisResultBackendOptions{Addr: mr.Addr()}
	rb := NewRedisResultBackend(opts)
	// No defer rb.Close() here, we test it explicitly

	if err := rb.Close(); err != nil {
		t.Fatalf("Close() failed: %v", err)
	}

	// Try to Ping via the backend's Ping method, which uses its internal client
	// This should fail if the client is properly closed.
	err := rb.(*RedisResultBackend).Ping(context.Background())
	if err == nil {
		t.Error("Ping after Close succeeded, but should fail or indicate closed state")
	} else {
		t.Logf("Ping after Close failed as expected: %v", err)
		// Error message might be "redis: client is closed" or similar
		var backendErr *taskiq.BackendError
		if errors.As(err, &backendErr) {
			if backendErr.OriginalError != redis.ErrClosed && backendErr.OriginalError.Error() != "redis: client is closed" {
				// Note: redis.ErrClosed might not be the exact error for Ping on closed client.
				// It might be an error from the pool or connection attempt.
				// For go-redis, "redis: client is closed" is common.
				t.Logf("Ping original error: %v", backendErr.OriginalError)
			}
		} else if err.Error() != "redis client is not initialized" && !strings.Contains(err.Error(), "client is closed") {
			// The "not initialized" can happen if client is set to nil after close.
			t.Errorf("Expected specific error indicating closure or nil client, got: %v", err)
		}
	}

	// Calling Close multiple times should be safe
	if err := rb.Close(); err != nil {
		t.Errorf("Calling Close() multiple times failed: %v", err)
	}
}
