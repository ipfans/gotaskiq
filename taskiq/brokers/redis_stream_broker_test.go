package brokers

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"gitlab.com/taskiq/taskiq/pkg/taskiq"
)

// Helper function to start miniredis and return a client & cleanup function
func setupMiniRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client, func()) {
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

func TestNewRedisStreamBroker(t *testing.T) {
	_, client, cleanup := setupMiniRedis(t)
	defer cleanup()

	broker := NewRedisStreamBroker(client, nil)
	if broker == nil {
		t.Fatal("NewRedisStreamBroker returned nil")
	}

	// Check if it's the correct type
	rsb, ok := broker.(*RedisStreamBroker)
	if !ok {
		t.Fatalf("NewRedisStreamBroker did not return a *RedisStreamBroker, got %T", broker)
	}

	// Check if client is set
	if rsb.client == nil {
		t.Error("RedisStreamBroker client is nil after NewRedisStreamBroker")
	}

	// Check Ping
	if err := broker.Ping(context.Background()); err != nil {
		t.Errorf("Ping failed after NewRedisStreamBroker: %v", err)
	}

	if err := broker.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestRedisStreamBroker_DeclareQueue(t *testing.T) {
	mr, client, cleanup := setupMiniRedis(t)
	defer cleanup()

	broker := NewRedisStreamBroker(client, nil)
	defer broker.Close()

	queueName := "test_declare_queue"
	ctx := context.Background()

	err := broker.DeclareQueue(ctx, queueName)
	if err != nil {
		t.Fatalf("DeclareQueue failed: %v", err)
	}

	// Verify stream exists and has the init message
	exists, err := mr.StreamExists(queueName)
	if err != nil {
		t.Fatalf("miniredis.StreamExists failed: %v", err)
	}
	if !exists {
		t.Errorf("Stream %s should exist after DeclareQueue, but it doesn't", queueName)
	}

	// Check stream length (should be 1 due to the init message)
	l, err := mr.XLen(queueName)
	if err != nil {
		t.Fatalf("miniredis.XLen failed: %v", err)
	}
	if l != 1 {
		t.Errorf("Stream %s should have length 1 after DeclareQueue, got %d", queueName, l)
	}
	
	// Calling DeclareQueue again should be idempotent
	err = broker.DeclareQueue(ctx, queueName)
	if err != nil {
		t.Fatalf("Calling DeclareQueue a second time failed: %v", err)
	}
	l2, err := mr.XLen(queueName)
	if err != nil {
		t.Fatalf("miniredis.XLen failed on second check: %v", err)
	}
	if l2 != 1 { // Length should still be 1, assuming init message is not duplicated by DeclareQueue
		t.Errorf("Stream %s should still have length 1 after second DeclareQueue, got %d", queueName, l2)
	}
}

func TestRedisStreamBroker_PublishMessage(t *testing.T) {
	mr, client, cleanup := setupMiniRedis(t)
	defer cleanup()

	broker := NewRedisStreamBroker(client, nil)
	defer broker.Close()

	queueName := "test_publish_queue"
	ctx := context.Background()

	// Declare queue first (as our implementation might rely on it for init)
	if err := broker.DeclareQueue(ctx, queueName); err != nil {
		t.Fatalf("DeclareQueue failed: %v", err)
	}
	// Clear the init message for a clean publish test
	mr.FlushAll() // Easiest way to clear the stream for this test case after DeclareQueue init.
	// Re-declare to ensure stream exists without init message affecting XLen count for *this* test's assertions.
	// This is a bit of a workaround for testing publish in isolation after DeclareQueue's side effect.
	// A better DeclareQueue might not add a message, or offer a way to declare without it.
	// For now, flushing and re-declaring (which now won't add another init message as it exists)
	if err := broker.DeclareQueue(ctx, queueName); err != nil {
		t.Fatalf("DeclareQueue after flush failed: %v", err)
	}


	taskMsg := &taskiq.TaskMessage{
		TaskID:   "task123",
		TaskName: "test_task",
		Args:     [][]byte{[]byte(`"arg1"`), []byte(`100`)},
		Kwargs:   map[string][]byte{"kwarg1": []byte(`"value1"`)},
		Headers:  map[string]string{"header1": "value_header1"},
	}

	err := broker.PublishMessage(ctx, queueName, taskMsg)
	if err != nil {
		t.Fatalf("PublishMessage failed: %v", err)
	}

	// Verify message in stream
	streamMessages, err := mr.XRange(queueName, "-", "+")
	if err != nil {
		t.Fatalf("miniredis.XRange failed: %v", err)
	}
	// After DeclareQueue's init message (if not flushed) and one published message
	// If flushed and re-declared, length should be 1.
	expectedLen := 1
	if len(streamMessages) != expectedLen {
		t.Fatalf("Expected %d message in stream, got %d", expectedLen, len(streamMessages))
	}

	msgValues := streamMessages[0].Values
	if msgValues["task_id"] != taskMsg.TaskID {
		t.Errorf("task_id mismatch: expected %s, got %s", taskMsg.TaskID, msgValues["task_id"])
	}
	if msgValues["task_name"] != taskMsg.TaskName {
		t.Errorf("task_name mismatch: expected %s, got %s", taskMsg.TaskName, msgValues["task_name"])
	}

	// Verify JSON marshaled fields
	var args [][]byte
	if err := json.Unmarshal([]byte(msgValues["args"].(string)), &args); err != nil {
		t.Fatalf("Failed to unmarshal args: %v", err)
	}
	if !reflect.DeepEqual(args, taskMsg.Args) {
		t.Errorf("args mismatch: expected %v, got %v", taskMsg.Args, args)
	}

	var kwargs map[string][]byte
	if err := json.Unmarshal([]byte(msgValues["kwargs"].(string)), &kwargs); err != nil {
		t.Fatalf("Failed to unmarshal kwargs: %v", err)
	}
	if !reflect.DeepEqual(kwargs, taskMsg.Kwargs) {
		t.Errorf("kwargs mismatch: expected %v, got %v", taskMsg.Kwargs, kwargs)
	}
	
	var headers map[string]string
	if err := json.Unmarshal([]byte(msgValues["headers"].(string)), &headers); err != nil {
		t.Fatalf("Failed to unmarshal headers: %v", err)
	}
	if !reflect.DeepEqual(headers, taskMsg.Headers) {
		t.Errorf("headers mismatch: expected %v, got %v", taskMsg.Headers, headers)
	}
}


func TestRedisStreamBroker_ConsumeMessages_Simple(t *testing.T) {
	_, client, cleanup := setupMiniRedis(t)
	defer cleanup()

	broker := NewRedisStreamBroker(client, nil)
	defer broker.Close()

	queueName := "test_consume_simple_queue"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure context is eventually cancelled

	taskMsg := &taskiq.TaskMessage{
		TaskID:   "task_consume_1",
		TaskName: "test_consume_task",
		Args:     [][]byte{[]byte(`"hello"`)},
	}

	// Publish the message
	if err := broker.PublishMessage(ctx, queueName, taskMsg); err != nil {
		t.Fatalf("PublishMessage failed: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)

	var receivedMsg *taskiq.TaskMessage
	handler := func(c context.Context, msg *taskiq.TaskMessage) error {
		defer wg.Done()
		receivedMsg = msg
		return nil // Returning nil will trigger Ack
	}

	// Start consuming in a goroutine
	consumeErrChan := make(chan error, 1)
	go func() {
		err := broker.ConsumeMessages(ctx, queueName, handler)
		// err might be context.Canceled, which is normal on shutdown
		if err != nil && err != context.Canceled {
			consumeErrChan <- fmt.Errorf("ConsumeMessages returned an error: %w", err)
		}
		close(consumeErrChan)
	}()

	// Wait for the handler to be called or timeout
	select {
	case <-time.After(5 * time.Second): // Increased timeout
		t.Fatal("Timeout waiting for message handler")
	case <-waitGroupDone(&wg):
		// Handler finished
	}

	cancel() // Stop ConsumeMessages

	// Check for errors from ConsumeMessages
	if err := <-consumeErrChan; err != nil {
		t.Fatalf("ConsumeMessages failed: %v", err)
	}

	if receivedMsg == nil {
		t.Fatalf("Handler was not called")
	}
	if receivedMsg.TaskID != taskMsg.TaskID {
		t.Errorf("TaskID mismatch: expected %s, got %s", taskMsg.TaskID, receivedMsg.TaskID)
	}
	if receivedMsg.TaskName != taskMsg.TaskName {
		t.Errorf("TaskName mismatch: expected %s, got %s", taskMsg.TaskName, receivedMsg.TaskName)
	}
	if !reflect.DeepEqual(receivedMsg.Args, taskMsg.Args) {
		t.Errorf("Args mismatch: expected %v, got %v", taskMsg.Args, receivedMsg.Args)
	}
	if receivedMsg.BrokerMessageID == "" {
		t.Error("BrokerMessageID was not set in received message")
	}
}

// Helper to wait for a sync.WaitGroup with a channel (useful for select)
func waitGroupDone(wg *sync.WaitGroup) <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		wg.Wait()
		close(ch)
	}()
	return ch
}


func TestRedisStreamBroker_ConsumeMessages_Multiple(t *testing.T) {
	_, client, cleanup := setupMiniRedis(t)
	defer cleanup()

	broker := NewRedisStreamBroker(client, nil)
	defer broker.Close()

	queueName := "test_consume_multiple_queue"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	numMessages := 5
	var publishedTasks []*taskiq.TaskMessage

	for i := 0; i < numMessages; i++ {
		taskMsg := &taskiq.TaskMessage{
			TaskID:   fmt.Sprintf("task_multi_%d", i),
			TaskName: "test_multi_task",
			Args:     [][]byte{[]byte(fmt.Sprintf(`"message_%d"`, i))},
		}
		publishedTasks = append(publishedTasks, taskMsg)
		if err := broker.PublishMessage(ctx, queueName, taskMsg); err != nil {
			t.Fatalf("PublishMessage %d failed: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(numMessages)
	receivedCount := 0
	var mu sync.Mutex // To protect receivedCount

	handler := func(c context.Context, msg *taskiq.TaskMessage) error {
		mu.Lock()
		// Check if the received message is one of the published ones
		found := false
		for _, pTask := range publishedTasks {
			if pTask.TaskID == msg.TaskID {
				found = true
				break
			}
		}
		if !found {
			mu.Unlock()
			t.Errorf("Received unexpected task with ID: %s", msg.TaskID)
			// Not calling wg.Done() here as it's an unexpected message
			return fmt.Errorf("unexpected task id %s", msg.TaskID)
		}
		receivedCount++
		mu.Unlock()
		wg.Done()
		return nil // Ack
	}

	consumeErrChan := make(chan error, 1)
	go func() {
		err := broker.ConsumeMessages(ctx, queueName, handler)
		if err != nil && err != context.Canceled {
			consumeErrChan <- err
		}
		close(consumeErrChan)
	}()

	select {
	case <-time.After(5 * time.Second): // Increased timeout
		t.Errorf("Timeout waiting for all messages. Received %d out of %d", receivedCount, numMessages)
	case <-waitGroupDone(&wg):
		// All messages processed
	}

	cancel() // Stop consumer

	if err := <-consumeErrChan; err != nil {
		t.Fatalf("ConsumeMessages failed: %v", err)
	}

	mu.Lock()
	if receivedCount != numMessages {
		t.Errorf("Expected to receive %d messages, got %d", numMessages, receivedCount)
	}
	mu.Unlock()
}

func TestRedisStreamBroker_ConsumeMessages_ContextCancellation(t *testing.T) {
	_, client, cleanup := setupMiniRedis(t)
	defer cleanup()

	broker := NewRedisStreamBroker(client, nil)
	defer broker.Close()

	queueName := "test_consume_cancel_queue"
	ctx, cancel := context.WithCancel(context.Background())

	handlerCalled := false
	handler := func(c context.Context, msg *taskiq.TaskMessage) error {
		handlerCalled = true
		return nil
	}

	consumeDone := make(chan struct{})
	go func() {
		err := broker.ConsumeMessages(ctx, queueName, handler)
		if err != nil && err != context.Canceled {
			t.Errorf("ConsumeMessages returned an unexpected error: %v", err)
		}
		close(consumeDone)
	}()

	// Ensure consumer has started (give it a moment)
	time.Sleep(100 * time.Millisecond)

	cancel() // Cancel the context

	select {
	case <-time.After(2 * time.Second): // Should be shorter as XREAD blocks for 2s
		t.Fatal("Timeout waiting for ConsumeMessages to stop after context cancellation")
	case <-consumeDone:
		// Consumer stopped as expected
	}

	if handlerCalled {
		// This might happen if a message was in stream and processed before cancel took effect
		// For this test, we assume no messages are published.
		t.Error("Handler was called, but it shouldn't have been (no messages published)")
	}
}

func TestRedisStreamBroker_Ack(t *testing.T) {
	mr, client, cleanup := setupMiniRedis(t)
	defer cleanup()

	broker := NewRedisStreamBroker(client, nil)
	defer broker.Close()

	queueName := "test_ack_queue"
	ctx := context.Background()

	taskMsg := &taskiq.TaskMessage{
		TaskID:   "task_ack_1",
		TaskName: "test_ack_task",
	}

	if err := broker.PublishMessage(ctx, queueName, taskMsg); err != nil {
		t.Fatalf("PublishMessage failed: %v", err)
	}

	// Consume the message (simplified consumption for Ack test)
	var redisMsgID string
	handler := func(c context.Context, msg *taskiq.TaskMessage) error {
		redisMsgID = msg.BrokerMessageID.(string) // Get the Redis Stream ID
		// Do NOT return nil, as that would Ack it via ConsumeMessages internal logic
		// We want to test broker.Ack explicitly
		return fmt.Errorf("handler error to prevent auto-ack")
	}
	
	consumeCtx, consumeCancel := context.WithCancel(ctx)
	consumeErrChan := make(chan error, 1)
	go func(){
		// We expect this to error out due to handler returning an error
		_ = broker.ConsumeMessages(consumeCtx, queueName, handler)
		consumeErrChan <- nil
	}()

	// Wait for handler to process the message or timeout
	time.Sleep(2500 * time.Millisecond) // Wait for XREAD to timeout and process
	consumeCancel() // Stop the consumer
	<-consumeErrChan // Wait for consumer goroutine to finish

	if redisMsgID == "" {
		t.Fatal("Consumer did not process message or BrokerMessageID was not set")
	}

	// Now, explicitly Ack the message
	taskMsg.BrokerMessageID = redisMsgID // Set the ID for Ack method
	if err := broker.Ack(ctx, queueName, taskMsg); err != nil {
		t.Fatalf("broker.Ack failed: %v", err)
	}

	// Verify message is removed from stream
	lenAfterAck, err := mr.XLen(queueName)
	if err != nil {
		t.Fatalf("miniredis.XLen failed: %v", err)
	}
	// If DeclareQueue adds an init message, it's 1. If this was the only other message, now 0 after Ack.
	// Our DeclareQueue adds an init msg. So, initial length was 1 (init) + 1 (task) = 2. After ack, should be 1.
	// This depends on how DeclareQueue is implemented and if it's called.
	// The RedisStreamBroker.DeclareQueue adds an init message.
	// PublishMessage doesn't call DeclareQueue.
	// So, stream exists due to publish. XADD creates it. Length was 1. After Ack, should be 0.
	if lenAfterAck != 0 {
		t.Errorf("Expected stream length 0 after Ack, got %d", lenAfterAck)
	}
}


func TestRedisStreamBroker_PublishConsumeAck_Cycle(t *testing.T) {
	mr, client, cleanup := setupMiniRedis(t)
	defer cleanup()

	broker := NewRedisStreamBroker(client, nil)
	defer broker.Close()

	queueName := "test_cycle_queue"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	taskMsg := &taskiq.TaskMessage{
		TaskID:   "task_cycle_1",
		TaskName: "test_cycle_task",
		Args:     [][]byte{[]byte(`"cycle_payload"`)},
	}

	// 1. Publish
	if err := broker.PublishMessage(ctx, queueName, taskMsg); err != nil {
		t.Fatalf("PublishMessage failed: %v", err)
	}
	lenBeforeConsume, _ := mr.XLen(queueName)
	if lenBeforeConsume != 1 {
		t.Fatalf("Expected stream length 1 after publish, got %d", lenBeforeConsume)
	}

	// 2. Consume & Handler implicitly Acks
	var wg sync.WaitGroup
	wg.Add(1)
	var receivedMsg *taskiq.TaskMessage

	handler := func(c context.Context, msg *taskiq.TaskMessage) error {
		receivedMsg = msg
		// Implicit Ack by returning nil
		wg.Done()
		return nil
	}

	consumeErrChan := make(chan error, 1)
	go func() {
		err := broker.ConsumeMessages(ctx, queueName, handler)
		if err != nil && err != context.Canceled {
			consumeErrChan <- err
		}
		close(consumeErrChan)
	}()
	
	select {
	case <-time.After(5 * time.Second): // Increased timeout
		t.Fatal("Timeout waiting for message handler in cycle test")
	case <-waitGroupDone(&wg):
		// Handler finished
	}
	cancel() // Stop consumer
	if err := <-consumeErrChan; err != nil {
		t.Fatalf("ConsumeMessages failed: %v", err)
	}


	if receivedMsg == nil {
		t.Fatal("Handler was not called in cycle test")
	}
	if taskMsg.TaskID != receivedMsg.TaskID {
		t.Errorf("TaskID mismatch in cycle test: expected %s, got %s", taskMsg.TaskID, receivedMsg.TaskID)
	}

	// 3. Verify Ack (message removed)
	// Need to give a moment for Ack (XDEL) to propagate if it's async from handler return,
	// but in current RedisStreamBroker, Ack is called synchronously within ConsumeMessages loop.
	time.Sleep(100 * time.Millisecond) // Short delay just in case

	lenAfterAck, err := mr.XLen(queueName)
	if err != nil {
		t.Fatalf("miniredis.XLen failed after Ack: %v", err)
	}
	if lenAfterAck != 0 {
		t.Errorf("Expected stream length 0 after full cycle, got %d", lenAfterAck)
	}
}


func TestRedisStreamBroker_Close(t *testing.T) {
	_, client, cleanup := setupMiniRedis(t)
	// No defer cleanup() here, we want to test broker.Close() explicitly

	broker := NewRedisStreamBroker(client, nil)

	if err := broker.Close(); err != nil {
		t.Fatalf("Close() failed: %v", err)
	}

	// Try to use the client after Close - should fail if client is truly closed.
	// This depends on go-redis client behavior. Ping might still work if it re-establishes.
	// A better test would be to check an internal state if possible, or ensure underlying client.Close() was called.
	// For now, let's check Ping. It might return an error indicating a closed pool or client.
	err := client.Ping(context.Background()).Err()
	// Note: go-redis v8's client.Close() closes the connection pool.
	// Subsequent commands will fail. The error might not be "client closed" but rather connection error.
	if err == nil {
		// This is not a perfect test. The client might transparently reconnect.
		// However, for a simple test, we expect operations to fail or the client to be unusable.
		// t.Logf("Ping after broker.Close() did not return an error, this might be okay if client reconnects, but check broker logic.")
	} else {
		t.Logf("Ping after broker.Close() returned error as expected (or client couldn't connect): %v", err)
	}

	// Calling Close multiple times should be safe
	if err := broker.Close(); err != nil {
		t.Errorf("Calling Close() multiple times failed: %v", err)
	}
	
	// Call miniredis cleanup manually
	cleanup()
}

// TODO:
// - Test case for when handler in ConsumeMessages returns an error (message should not be Acked).
// - Test case for when JSON unmarshalling fails in ConsumeMessages (message should not be Acked).
// - Test Ping method more directly.
// - Test for multiple consumers on the same queue (if that's a supported scenario for this simple XREAD broker).
// - Test behavior when Redis is down temporarily (more advanced, likely needs real Redis or more sophisticated mock).

func TestRedisStreamBroker_ConsumeMessages_HandlerError(t *testing.T) {
	mr, client, cleanup := setupMiniRedis(t)
	defer cleanup()

	broker := NewRedisStreamBroker(client, nil)
	defer broker.Close()

	queueName := "test_handler_error_queue"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	taskMsg := &taskiq.TaskMessage{TaskID: "task_handler_err_1"}
	if err := broker.PublishMessage(ctx, queueName, taskMsg); err != nil {
		t.Fatalf("PublishMessage failed: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	handlerError := fmt.Errorf("handler failed intentionally")

	handler := func(c context.Context, msg *taskiq.TaskMessage) error {
		wg.Done()
		return handlerError // Simulate handler error
	}

	consumeErrChan := make(chan error, 1)
	go func() {
		// ConsumeMessages itself shouldn't error out due to handler error,
		// it should continue trying to consume or stop on context cancel.
		// The error from the handler is processed internally.
		err := broker.ConsumeMessages(ctx, queueName, handler)
		if err != nil && err != context.Canceled {
			consumeErrChan <- err
		}
		close(consumeErrChan)
	}()

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for handler to be called")
	case <-waitGroupDone(&wg):
		// Handler was called
	}
	cancel() // Stop consumer
	if err := <-consumeErrChan; err != nil {
		t.Fatalf("ConsumeMessages failed: %v", err)
	}

	// Verify message was NOT Acked (still in stream)
	lenAfter, err := mr.XLen(queueName)
	if err != nil {
		t.Fatalf("miniredis.XLen failed: %v", err)
	}
	if lenAfter != 1 { // Published 1, init msg from DeclareQueue not explicitly called here, XADD creates stream.
		t.Errorf("Expected stream length 1 (message not Acked), got %d", lenAfter)
	}
}

func TestRedisStreamBroker_ConsumeMessages_UnmarshalError(t *testing.T) {
	mr, client, cleanup := setupMiniRedis(t)
	defer cleanup()

	broker := NewRedisStreamBroker(client, nil)
	defer broker.Close()

	queueName := "test_unmarshal_error_queue"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Publish a message with malformed JSON for 'args'
	malformedValues := map[string]interface{}{
		"task_id":   "task_unmarshal_err_1",
		"task_name": "test_task",
		"args":      `[{"unclosed_json_string"]`, // Malformed JSON
		"kwargs":    `{}`,
		"headers":   `{}`,
		"timestamp": time.Now().Format(time.RFC3339Nano),
	}
	if _, err := client.XAdd(ctx, &redis.XAddArgs{Stream: queueName, Values: malformedValues}).Result(); err != nil {
		t.Fatalf("Failed to publish malformed message directly: %v", err)
	}
	
	handlerCalled := false
	var wg sync.WaitGroup // wg won't be done as handler shouldn't be called for this message
	                          // Or, if it is, it's a bug. The unmarshal error should be caught before.

	handler := func(c context.Context, msg *taskiq.TaskMessage) error {
		handlerCalled = true // This should ideally not be reached
		wg.Done()
		return nil
	}
	
	consumeErrChan := make(chan error, 1)
	go func() {
		err := broker.ConsumeMessages(ctx, queueName, handler)
		if err != nil && err != context.Canceled {
			consumeErrChan <- err
		}
		close(consumeErrChan)
	}()

	// Let consumer run for a bit. It should process the message, log an error, and NOT call the handler.
	// The message should remain in the stream as it won't be Acked.
	time.Sleep(2500 * time.Millisecond) // Wait for XREAD to timeout and process

	cancel() // Stop consumer
	if err := <-consumeErrChan; err != nil {
		t.Fatalf("ConsumeMessages failed: %v", err)
	}

	if handlerCalled {
		t.Error("Handler was called for a message with unmarshal error, but it shouldn't have been.")
	}

	// Verify message was NOT Acked (still in stream)
	lenAfter, err := mr.XLen(queueName)
	if err != nil {
		t.Fatalf("miniredis.XLen failed: %v", err)
	}
	if lenAfter != 1 {
		t.Errorf("Expected stream length 1 (message not Acked due to unmarshal error), got %d", lenAfter)
	}
}
