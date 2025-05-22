package brokers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"gitlab.com/taskiq/taskiq/pkg/taskiq"
	// "github.com/pebbe/zmq4" // Not directly used in tests, but by the broker
)

// Helper to generate unique IPC addresses for ZMQ sockets
func uniqueIPCAddr(t *testing.T, prefix string) string {
	t.Helper()
	// Using a temp directory for IPC sockets
	// Ensure the directory exists or ZMQ might fail to bind/connect
	// For testing, /tmp/ is usually fine. For more robust, use os.TempDir()
	tmpDir := os.TempDir()
	ipcDir := filepath.Join(tmpDir, "taskiq_zmq_tests")
	if err := os.MkdirAll(ipcDir, 0700); err != nil {
		t.Fatalf("Failed to create temp IPC directory %s: %v", ipcDir, err)
	}

	randSuffix := uuid.NewString()[:8]
	// Example: ipc:///tmp/taskiq_zmq_tests/test_prefix_abc12345.ipc
	return fmt.Sprintf("ipc://%s/%s_%s.ipc", ipcDir, prefix, randSuffix)
}

// Helper to wait for a sync.WaitGroup with a channel (useful for select with timeout)
func waitGroupDone(wg *sync.WaitGroup) <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		wg.Wait()
		close(ch)
	}()
	return ch
}

func TestNewZMQBroker(t *testing.T) {
	opts := &ZMQBrokerOptions{BaseAddress: "ipc:///tmp/taskiq_test_new_"}
	broker, err := NewZMQBroker(opts)
	if err != nil {
		t.Fatalf("NewZMQBroker failed: %v", err)
	}
	if broker == nil {
		t.Fatal("NewZMQBroker returned nil")
	}

	// Check if it's the correct type
	zmb, ok := broker.(*ZMQBroker)
	if !ok {
		t.Fatalf("NewZMQBroker did not return a *ZMQBroker, got %T", broker)
	}
	if zmb.context == nil {
		t.Error("ZMQBroker context is nil after NewZMQBroker")
	}

	// Check Ping
	if err := broker.Ping(context.Background()); err != nil {
		t.Errorf("Ping failed: %v", err)
	}

	if err := broker.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Ping after close should fail
	if err := broker.Ping(context.Background()); err == nil {
		t.Error("Ping after Close succeeded, but should fail")
	}
}

func TestZMQBroker_DeclareQueue(t *testing.T) {
	opts := &ZMQBrokerOptions{BaseAddress: "ipc:///tmp/taskiq_test_declare_"}
	broker, _ := NewZMQBroker(opts)
	defer broker.Close()

	queueName := "test_declare_queue"
	err := broker.DeclareQueue(context.Background(), queueName)
	if err != nil {
		t.Errorf("DeclareQueue failed: %v (expected no-op to succeed)", err)
	}
	// No specific state to check as it's a no-op in current ZMQ PUSH/PULL broker
}

func TestZMQBroker_PublishMessage_NoConsumer(t *testing.T) {
	addr := uniqueIPCAddr(t, "test_publish_noconsumer")
	opts := &ZMQBrokerOptions{BaseAddress: addr[:len(addr)-len(filepath.Base(addr))]} // Pass base path
	broker, _ := NewZMQBroker(opts)
	defer broker.Close()

	queueName := filepath.Base(addr) // Use the random part as queue name for uniqueness
	ctx := context.Background()

	taskMsg := &taskiq.TaskMessage{
		TaskID:   "task_no_consumer",
		TaskName: "test_task",
	}

	// ZMQ PUSH sockets can send even if no PULL socket is connected (up to HWM).
	// This test mainly ensures it doesn't crash or block indefinitely.
	err := broker.PublishMessage(ctx, queueName, taskMsg)
	if err != nil {
		// Depending on ZMQ version and config, this might error if HWM is 0 or very small.
		// For default HWM, it should typically succeed by queueing.
		t.Fatalf("PublishMessage with no consumer failed: %v", err)
	}
	// No easy way to verify message is queued by ZMQ internally without a consumer.
	// Test passes if no crash/error.
	t.Log("PublishMessage with no consumer completed without error.")
}

func TestZMQBroker_PublishConsume_Simple(t *testing.T) {
	addr := uniqueIPCAddr(t, "test_pubcons_simple")
	opts := &ZMQBrokerOptions{BaseAddress: addr[:len(addr)-len(filepath.Base(addr))]}
	broker, _ := NewZMQBroker(opts)
	defer broker.Close()

	queueName := filepath.Base(addr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	taskMsg := &taskiq.TaskMessage{
		TaskID:   "task_simple_1",
		TaskName: "test_simple_task",
		Args:     [][]byte{[]byte(`"hello ZMQ"`)},
	}

	var wg sync.WaitGroup
	wg.Add(1)
	var receivedMsg *taskiq.TaskMessage
	handlerFunc := func(c context.Context, msg *taskiq.TaskMessage) error {
		receivedMsg = msg
		wg.Done()
		return nil // Implicitly Acks (no-op for ZMQ)
	}

	// Start consumer
	consumeErrCh := make(chan error, 1)
	go func() {
		err := broker.ConsumeMessages(ctx, queueName, handlerFunc)
		if err != nil && err != context.Canceled {
			consumeErrCh <- fmt.Errorf("ConsumeMessages error: %w", err)
		}
		close(consumeErrCh)
	}()

	// Wait a tiny bit for consumer to bind
	time.Sleep(100 * time.Millisecond)

	// Publish message
	if err := broker.PublishMessage(ctx, queueName, taskMsg); err != nil {
		t.Fatalf("PublishMessage failed: %v", err)
	}

	// Wait for handler or timeout
	select {
	case <-time.After(3 * time.Second): // ZMQ Recv has 1s timeout, so 3s overall should be enough
		t.Fatal("Timeout waiting for message handler")
	case <-waitGroupDone(&wg):
		// Handler finished
	}

	cancel() // Stop consumer

	if err := <-consumeErrCh; err != nil {
		t.Fatalf("Error from ConsumeMessages goroutine: %v", err)
	}

	if receivedMsg == nil {
		t.Fatal("Handler was not called")
	}
	if receivedMsg.TaskID != taskMsg.TaskID {
		t.Errorf("TaskID mismatch: expected %s, got %s", taskMsg.TaskID, receivedMsg.TaskID)
	}
	if !reflect.DeepEqual(receivedMsg.Args, taskMsg.Args) {
		t.Errorf("Args mismatch: expected %v, got %v", taskMsg.Args, receivedMsg.Args)
	}
}

func TestZMQBroker_PublishConsume_Multiple(t *testing.T) {
	addr := uniqueIPCAddr(t, "test_pubcons_multi")
	opts := &ZMQBrokerOptions{BaseAddress: addr[:len(addr)-len(filepath.Base(addr))]}
	broker, _ := NewZMQBroker(opts)
	defer broker.Close()

	queueName := filepath.Base(addr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	numMessages := 5
	publishedTasks := make(map[string]*taskiq.TaskMessage)
	for i := 0; i < numMessages; i++ {
		taskMsg := &taskiq.TaskMessage{
			TaskID:   fmt.Sprintf("task_multi_%d", i),
			TaskName: "test_multi_task",
			Args:     [][]byte{[]byte(fmt.Sprintf(`"message %d"`, i))},
		}
		publishedTasks[taskMsg.TaskID] = taskMsg
	}

	var wg sync.WaitGroup
	wg.Add(numMessages)
	receivedTasks := make(map[string]*taskiq.TaskMessage)
	var mu sync.Mutex

	handlerFunc := func(c context.Context, msg *taskiq.TaskMessage) error {
		mu.Lock()
		receivedTasks[msg.TaskID] = msg
		mu.Unlock()
		wg.Done()
		return nil
	}

	go func() {
		broker.ConsumeMessages(ctx, queueName, handlerFunc)
	}()
	time.Sleep(100 * time.Millisecond) // Give consumer time to start

	for _, taskMsg := range publishedTasks {
		if err := broker.PublishMessage(ctx, queueName, taskMsg); err != nil {
			t.Errorf("PublishMessage for task %s failed: %v", taskMsg.TaskID, err)
		}
	}

	select {
	case <-time.After(5 * time.Second): // Allow more time for multiple messages
		t.Fatalf("Timeout waiting for all messages. Received %d/%d", len(receivedTasks), numMessages)
	case <-waitGroupDone(&wg):
		// All messages processed
	}
	cancel()

	mu.Lock()
	if len(receivedTasks) != numMessages {
		t.Errorf("Expected %d messages, received %d", numMessages, len(receivedTasks))
	}
	for taskID, publishedTask := range publishedTasks {
		receivedTask, ok := receivedTasks[taskID]
		if !ok {
			t.Errorf("Task %s was not received", taskID)
			continue
		}
		if !reflect.DeepEqual(receivedTask.Args, publishedTask.Args) {
			t.Errorf("Args mismatch for task %s: expected %v, got %v", taskID, publishedTask.Args, receivedTask.Args)
		}
	}
	mu.Unlock()
}

func TestZMQBroker_ConsumeMessages_ContextCancellation(t *testing.T) {
	addr := uniqueIPCAddr(t, "test_consumecancel")
	opts := &ZMQBrokerOptions{BaseAddress: addr[:len(addr)-len(filepath.Base(addr))]}
	broker, _ := NewZMQBroker(opts)
	defer broker.Close() // Close broker at the end of the test

	queueName := filepath.Base(addr)
	ctx, cancel := context.WithCancel(context.Background())

	handlerCalled := false
	handlerFunc := func(c context.Context, msg *taskiq.TaskMessage) error {
		handlerCalled = true
		return nil
	}

	consumeDone := make(chan struct{})
	go func() {
		err := broker.ConsumeMessages(ctx, queueName, handlerFunc)
		if err != nil && err != context.Canceled {
			t.Errorf("ConsumeMessages returned an unexpected error: %v", err)
		}
		close(consumeDone)
	}()

	time.Sleep(100 * time.Millisecond) // Ensure consumer goroutine starts and binds
	cancel()                           // Trigger cancellation

	select {
	case <-time.After(2 * time.Second): // ZMQ Recv has 1s timeout, should exit quickly
		t.Fatal("Timeout waiting for ConsumeMessages to stop after context cancellation")
	case <-consumeDone:
		// Success
	}

	if handlerCalled {
		t.Error("Handler was called, but no messages were published")
	}
}

func TestZMQBroker_Ack(t *testing.T) {
	opts := &ZMQBrokerOptions{BaseAddress: "ipc:///tmp/taskiq_test_ack_"}
	broker, _ := NewZMQBroker(opts)
	defer broker.Close()

	// Ack is a no-op for ZMQ PUSH/PULL, so it should just return nil
	err := broker.Ack(context.Background(), "any_queue", &taskiq.TaskMessage{TaskID: "any_task"})
	if err != nil {
		t.Errorf("Ack failed: %v (expected no-op to succeed)", err)
	}
}

func TestZMQBroker_Close(t *testing.T) {
	addr := uniqueIPCAddr(t, "test_close")
	opts := &ZMQBrokerOptions{BaseAddress: addr[:len(addr)-len(filepath.Base(addr))]}
	broker, _ := NewZMQBroker(opts)

	queueName := filepath.Base(addr)
	ctx, cancel := context.WithCancel(context.Background()) // This context is for the consumer

	consumeDone := make(chan struct{})
	go func() {
		// This ConsumeMessages call should terminate when broker.Close() is called,
		// because broker.Close() cancels the broker's internal context.
		err := broker.ConsumeMessages(ctx, queueName, func(c context.Context, m *taskiq.TaskMessage) error { return nil })
		if err != nil && err != context.Canceled && err.Error() != "broker is shutting down: context canceled" {
			// Note: Error message check is brittle. Better to check specific error type if available.
			// For ZMQ, the Recv might also return an error if the context is terminated.
			if zmqErr, ok := err.(interface{ ZMQError() int }); ok { // Check if it's a ZMQ error
				// EINTR or ETERM are expected if socket is closed during Recv
				if zmqErr.ZMQError() != 4 && zmqErr.ZMQError() != 156384765 // EINTR and ETERM
					t.Errorf("ConsumeMessages after Close returned unexpected ZMQ error: %v (code: %d)", err, zmqErr.ZMQError())
			} else {
				t.Errorf("ConsumeMessages after Close returned unexpected error: %v", err)
			}
		}
		close(consumeDone)
	}()

	time.Sleep(100 * time.Millisecond) // Allow consumer to start

	if err := broker.Close(); err != nil {
		t.Fatalf("broker.Close() failed: %v", err)
	}
	cancel() // Cancel consumer context just in case, though broker.Close should handle it.

	select {
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for consumer to stop after broker.Close()")
	case <-consumeDone:
		// Consumer stopped as expected
	}

	// Try publishing after Close, should fail as ZMQ context is terminated
	err := broker.PublishMessage(context.Background(), queueName, &taskiq.TaskMessage{TaskID: "task_after_close"})
	if err == nil {
		t.Error("PublishMessage after Close succeeded, but should fail")
	} else {
		// Expected error: "context terminated" or similar from zmq4, or "broker is shutting down"
		t.Logf("PublishMessage after Close failed as expected: %v", err)
	}
}


func TestZMQBroker_Concurrency_DifferentQueues(t *testing.T) {
	numConsumers := 3
	numMessagesPerQueue := 2
	baseAddrPrefix := "ipc:///tmp/taskiq_test_concurr_"

	broker, err := NewZMQBroker(&ZMQBrokerOptions{BaseAddress: baseAddrPrefix})
	if err != nil {
		t.Fatalf("NewZMQBroker failed: %v", err)
	}
	defer broker.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var totalMessagesSent int
	var totalMessagesReceived int
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i := 0; i < numConsumers; i++ {
		queueName := fmt.Sprintf("concurrent_queue_%d_%s", i, uuid.NewString()[:4])
		// Use unique IPC path for each "queue" by ensuring BaseAddress + queueName is unique
		// The current ZMQBroker uses `ipc://{BaseAddress}{queueName}.ipc`
		// So, queueName itself must be unique enough. BaseAddress is fixed.

		wg.Add(numMessagesPerQueue) // Expect N messages for this consumer

		go func(qn string, consumerID int) {
			handlerFunc := func(c context.Context, msg *taskiq.TaskMessage) error {
				mu.Lock()
				totalMessagesReceived++
				t.Logf("Consumer %d on queue %s received task %s", consumerID, qn, msg.TaskID)
				mu.Unlock()
				wg.Done()
				return nil
			}
			// ConsumeMessages will block until context is cancelled or an error occurs
			err := broker.ConsumeMessages(ctx, qn, handlerFunc)
			if err != nil && err != context.Canceled && err.Error() != "broker is shutting down: context canceled" {
				mu.Lock()
				t.Errorf("Consumer %d on queue %s error: %v", consumerID, qn, err)
				mu.Unlock()
			}
		}(queueName, i)

		time.Sleep(100 * time.Millisecond) // Stagger consumer starts slightly

		for j := 0; j < numMessagesPerQueue; j++ {
			taskMsg := &taskiq.TaskMessage{
				TaskID:   fmt.Sprintf("task_q%d_m%d", i, j),
				TaskName: "concurrent_task",
			}
			if err := broker.PublishMessage(ctx, queueName, taskMsg); err != nil {
				t.Errorf("Publish to queue %s failed for task %s: %v", queueName, taskMsg.TaskID, err)
			} else {
				totalMessagesSent++
			}
		}
	}

	select {
	case <-time.After(8 * time.Second): // Generous timeout
		mu.Lock()
		t.Fatalf("Timeout waiting for all concurrent messages. Sent: %d, Received: %d", totalMessagesSent, totalMessagesReceived)
		mu.Unlock()
	case <-waitGroupDone(&wg):
		// All expected messages processed
	}
	cancel() // Signal consumers to stop

	mu.Lock()
	if totalMessagesReceived != totalMessagesSent {
		t.Errorf("Mismatch in sent and received messages. Sent: %d, Received: %d", totalMessagesSent, totalMessagesReceived)
	}
	if totalMessagesSent == 0 && numConsumers > 0 && numMessagesPerQueue > 0 {
		t.Error("No messages were sent, test might be flawed.")
	}
	t.Logf("Concurrency test finished. Total Sent: %d, Total Received: %d", totalMessagesSent, totalMessagesReceived)
	mu.Unlock()
}

// Note: Testing multiple consumers on the *same* PULL socket (same queueName for same broker instance)
// is tricky with ZMQ PULL sockets, as they distribute messages round-robin to connected PULL instances.
// The ZMQBroker current implementation returns an error if ConsumeMessages is called multiple times
// for the same queueName on the same broker instance, because it tries to bind the PULL socket again.
// To test true round-robin, one would need multiple ZMQBroker instances or a broker that manages
// a pool of workers behind a single PULL socket (e.g., using a ZMQ_DEALER to distribute internally).
// The Concurrency_DifferentQueues test above tests multiple independent consumers on different queues,
// which is a valid concurrency scenario for this broker.

// Helper for JSON serialization in task messages (if needed by tests, though broker should handle it)
func mustMarshal(t *testing.T, v interface{}) []byte {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("Failed to marshal %v: %v", v, err)
	}
	return data
}
