package brokers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/streadway/amqp"
	"gitlab.com/taskiq/taskiq/pkg/taskiq"
)

const (
	defaultRabbitMQURL = "amqp://guest:guest@localhost:5672/"
	envRabbitMQURL     = "RABBITMQ_TEST_URL"
	envCI              = "CI" // Standard env var for CI systems
)

// getTestRabbitMQURL returns the RabbitMQ URL from environment or default.
func getTestRabbitMQURL() string {
	if url := os.Getenv(envRabbitMQURL); url != "" {
		return url
	}
	return defaultRabbitMQURL
}

// checkRabbitMQAvailability skips the test if RabbitMQ is not available and not in CI.
func checkRabbitMQAvailability(t *testing.T) {
	t.Helper()
	url := getTestRabbitMQURL()
	conn, err := amqp.Dial(url)
	if err != nil {
		if os.Getenv(envCI) != "" { // In CI, always try to run, expect RabbitMQ to be available
			t.Fatalf("RabbitMQ connection failed in CI environment at %s: %v", url, err)
		} else {
			t.Skipf("Skipping RabbitMQ test: RabbitMQ not available at %s: %v", url, err)
		}
	}
	conn.Close() // Close the test connection immediately
}

// uniqueQueueName generates a unique queue name for testing.
func uniqueQueueName(prefix string) string {
	return fmt.Sprintf("%s_%s_%s", prefix, time.Now().Format("20060102150405"), uuid.NewString()[:8])
}

// setupRabbitMQTest provides a connection and channel for a test.
// It also returns a cleanup function to close them and optionally delete the queue.
func setupRabbitMQTestBroker(t *testing.T) (broker taskiq.Broker, userChannel *amqp.Channel, cleanup func()) {
	t.Helper()
	checkRabbitMQAvailability(t)
	url := getTestRabbitMQURL()

	var err error
	b, err := NewRabbitMQBroker(url)
	if err != nil {
		t.Fatalf("NewRabbitMQBroker failed: %v", err)
	}
	broker = b

	// For verification purposes, create a separate connection and channel
	connVerify, err := amqp.Dial(url)
	if err != nil {
		broker.Close()
		t.Fatalf("Failed to connect verification client to RabbitMQ: %v", err)
	}
	chVerify, err := connVerify.Channel()
	if err != nil {
		broker.Close()
		connVerify.Close()
		t.Fatalf("Failed to open verification channel: %v", err)
	}
	userChannel = chVerify

	cleanup = func() {
		broker.Close() // Closes the broker's connection and channel
		if userChannel != nil {
			userChannel.Close()
		}
		if connVerify != nil {
			connVerify.Close()
		}
	}
	return
}

// Helper to delete a queue using a provided channel
func deleteQueue(t *testing.T, ch *amqp.Channel, queueName string) {
	t.Helper()
	if ch == nil {
		t.Logf("Warning: Cannot delete queue %s, provided channel is nil.", queueName)
		return
	}
	// Args: queue, ifUnused, ifEmpty, noWait
	_, err := ch.QueueDelete(queueName, false, false, false)
	if err != nil {
		// Log as warning, as tests might run in parallel or queue might already be gone
		t.Logf("Warning: Failed to delete queue %s: %v", queueName, err)
	}
}

// Helper to wait for a sync.WaitGroup with a channel (useful for select with timeout)
func waitGroupDone(wg *sync.WaitGroup) <-chan struct{} {
	chDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(chDone)
	}()
	return chDone
}

func TestNewRabbitMQBroker(t *testing.T) {
	checkRabbitMQAvailability(t)
	url := getTestRabbitMQURL()

	broker, err := NewRabbitMQBroker(url)
	if err != nil {
		t.Fatalf("NewRabbitMQBroker failed with URL %s: %v", url, err)
	}
	if broker == nil {
		t.Fatal("NewRabbitMQBroker returned nil")
	}
	defer broker.Close()

	if err := broker.Ping(context.Background()); err != nil {
		t.Errorf("Ping failed: %v", err)
	}
	if _, ok := broker.(*RabbitMQBroker); !ok {
		t.Errorf("NewRabbitMQBroker did not return a *RabbitMQBroker, got %T", broker)
	}
	if err := broker.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}
	pingErr := broker.Ping(context.Background())
	if pingErr == nil {
		t.Error("Ping after Close succeeded, but should fail")
	}
}

func TestRabbitMQBroker_DeclareQueue(t *testing.T) {
	broker, userCh, cleanup := setupRabbitMQTestBroker(t)
	defer cleanup()

	queueName := uniqueQueueName("test_declare")
	ctx := context.Background()

	if err := broker.DeclareQueue(ctx, queueName); err != nil {
		t.Fatalf("broker.DeclareQueue failed for %s: %v", queueName, err)
	}
	defer deleteQueue(t, userCh, queueName)

	_, err := userCh.QueueDeclarePassive(queueName, true, false, false, false, nil)
	if err != nil {
		t.Errorf("QueueDeclarePassive check failed for queue %s: %v", queueName, err)
	}
}

func TestRabbitMQBroker_PublishMessage_NoConsumer(t *testing.T) {
	broker, userCh, cleanup := setupRabbitMQTestBroker(t)
	defer cleanup()

	queueName := uniqueQueueName("test_publish_noconsumer")
	ctx := context.Background()

	if err := broker.DeclareQueue(ctx, queueName); err != nil {
		t.Fatalf("DeclareQueue failed: %v", err)
	}
	defer deleteQueue(t, userCh, queueName)

	taskMsg := &taskiq.TaskMessage{TaskID: "task_noconsumer_1", TaskName: "test_task_noconsumer"}
	if err := broker.PublishMessage(ctx, queueName, taskMsg); err != nil {
		t.Fatalf("PublishMessage failed: %v", err)
	}

	delivery, ok, errGet := userCh.Get(queueName, true /*autoAck*/)
	if errGet != nil {
		t.Fatalf("userCh.Get failed: %v", errGet)
	}
	if !ok {
		t.Fatal("No message received from queue, expected one.")
	}
	var receivedTask taskiq.TaskMessage
	if err := json.Unmarshal(delivery.Body, &receivedTask); err != nil {
		t.Fatalf("Failed to unmarshal: %v. Body: %s", err, string(delivery.Body))
	}
	if receivedTask.TaskID != taskMsg.TaskID {
		t.Errorf("TaskID mismatch: expected %s, got %s", taskMsg.TaskID, receivedTask.TaskID)
	}
}

func TestRabbitMQBroker_PublishConsumeAck_Simple(t *testing.T) {
	broker, userCh, cleanup := setupRabbitMQTestBroker(t)
	defer cleanup()

	queueName := uniqueQueueName("test_pubconsack_simple")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := broker.DeclareQueue(ctx, queueName); err != nil {
		t.Fatalf("DeclareQueue failed: %v", err)
	}
	defer deleteQueue(t, userCh, queueName)

	taskMsg := &taskiq.TaskMessage{TaskID: "task_simple_ack_1", Args: [][]byte{[]byte(`"payload"`)}}
	if err := broker.PublishMessage(ctx, queueName, taskMsg); err != nil {
		t.Fatalf("PublishMessage failed: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	var receivedMsg *taskiq.TaskMessage
	handlerFunc := func(c context.Context, msg *taskiq.TaskMessage) error {
		receivedMsg = msg
		wg.Done()
		return nil // Implicitly Acks
	}

	consumeErrCh := make(chan error, 1)
	go func() {
		err := broker.ConsumeMessages(ctx, queueName, handlerFunc)
		if err != nil && !shouldIgnoreConsumeError(err) {
			consumeErrCh <- err
		}
		close(consumeErrCh)
	}()

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message handler")
	case <-waitGroupDone(&wg):
	}
	cancel()
	if err := <-consumeErrCh; err != nil {
		t.Fatalf("ConsumeMessages error: %v", err)
	}

	if receivedMsg == nil {
		t.Fatal("Handler was not called")
	}
	if receivedMsg.TaskID != taskMsg.TaskID {
		t.Errorf("TaskID mismatch: expected %s, got %s", taskMsg.TaskID, receivedMsg.TaskID)
	}

	time.Sleep(100 * time.Millisecond)
	_, ok, _ := userCh.Get(queueName, true)
	if ok {
		t.Error("Message still in queue after Ack.")
	}
}

func TestRabbitMQBroker_PublishConsumeNack_HandlerError(t *testing.T) {
	broker, userCh, cleanup := setupRabbitMQTestBroker(t)
	defer cleanup()

	queueName := uniqueQueueName("test_pubconsnack_err")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := broker.DeclareQueue(ctx, queueName); err != nil {
		t.Fatalf("DeclareQueue failed: %v", err)
	}
	defer deleteQueue(t, userCh, queueName)

	taskMsg := &taskiq.TaskMessage{TaskID: "task_nack_err_1"}
	if err := broker.PublishMessage(ctx, queueName, taskMsg); err != nil {
		t.Fatalf("PublishMessage failed: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	handlerFunc := func(c context.Context, msg *taskiq.TaskMessage) error {
		wg.Done()
		return fmt.Errorf("handler failed intentionally")
	}

	consumeErrCh := make(chan error, 1)
	go func() {
		err := broker.ConsumeMessages(ctx, queueName, handlerFunc)
		if err != nil && !shouldIgnoreConsumeError(err) {
			consumeErrCh <- err
		}
		close(consumeErrCh)
	}()

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for handler")
	case <-waitGroupDone(&wg):
	}
	cancel()
	if err := <-consumeErrCh; err != nil {
		t.Fatalf("ConsumeMessages error: %v", err)
	}

	time.Sleep(200 * time.Millisecond)
	_, ok, _ := userCh.Get(queueName, true)
	if ok {
		t.Error("Message still in queue after Nack (requeue=false).")
	}
}

func TestRabbitMQBroker_PublishConsume_Multiple(t *testing.T) {
	broker, userCh, cleanup := setupRabbitMQTestBroker(t)
	defer cleanup()

	queueName := uniqueQueueName("test_pubconsmulti")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := broker.DeclareQueue(ctx, queueName); err != nil {
		t.Fatalf("DeclareQueue failed: %v", err)
	}
	defer deleteQueue(t, userCh, queueName)

	numMessages := 5
	for i := 0; i < numMessages; i++ {
		taskMsg := &taskiq.TaskMessage{TaskID: fmt.Sprintf("task_multi_%d", i)}
		if err := broker.PublishMessage(ctx, queueName, taskMsg); err != nil {
			t.Fatalf("Publish for task %d failed: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(numMessages)
	receivedCount := 0
	var mu sync.Mutex
	handlerFunc := func(c context.Context, msg *taskiq.TaskMessage) error {
		mu.Lock()
		receivedCount++
		mu.Unlock()
		wg.Done()
		return nil
	}

	consumeErrCh := make(chan error, 1)
	go func() {
		err := broker.ConsumeMessages(ctx, queueName, handlerFunc)
		if err != nil && !shouldIgnoreConsumeError(err) {
			consumeErrCh <- err
		}
		close(consumeErrCh)
	}()

	select {
	case <-time.After(8 * time.Second):
		t.Fatalf("Timeout. Received %d/%d", receivedCount, numMessages)
	case <-waitGroupDone(&wg):
	}
	cancel()
	if err := <-consumeErrCh; err != nil {
		t.Fatalf("ConsumeMessages error: %v", err)
	}
	if receivedCount != numMessages {
		t.Errorf("Expected %d messages, received %d", numMessages, receivedCount)
	}
}

func TestRabbitMQBroker_ConsumeMessages_ContextCancellation(t *testing.T) {
	broker, userCh, cleanup := setupRabbitMQTestBroker(t)
	defer cleanup()

	queueName := uniqueQueueName("test_consumecancel")
	ctx, cancelManually := context.WithCancel(context.Background())
	// No defer cancelManually as we do it explicitly

	if err := broker.DeclareQueue(context.Background(), queueName); err != nil {
		t.Fatalf("DeclareQueue failed: %v", err)
	}
	defer deleteQueue(t, userCh, queueName)

	handlerCalled := false
	consumeDone := make(chan struct{})
	go func() {
		err := broker.ConsumeMessages(ctx, queueName, func(c context.Context, m *taskiq.TaskMessage) error { handlerCalled = true; return nil })
		if err != nil && !shouldIgnoreConsumeError(err) {
			t.Errorf("ConsumeMessages unexpected error: %v", err)
		}
		close(consumeDone)
	}()

	time.Sleep(200 * time.Millisecond)
	cancelManually()

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for ConsumeMessages to stop")
	case <-consumeDone:
	}
	if handlerCalled {
		t.Error("Handler called, but no messages were published.")
	}
}

func TestRabbitMQBroker_Ack_Explicit_Refined(t *testing.T) {
	broker, userCh, cleanup := setupRabbitMQTestBroker(t)
	defer cleanup()

	queueName := uniqueQueueName("test_ack_explicit_refined")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := broker.DeclareQueue(ctx, queueName); err != nil {
		t.Fatalf("DeclareQueue failed: %v", err)
	}
	defer deleteQueue(t, userCh, queueName)

	taskMsg := &taskiq.TaskMessage{TaskID: "task_explicit_ack_refined_1"}
	if err := broker.PublishMessage(ctx, queueName, taskMsg); err != nil {
		t.Fatalf("PublishMessage failed: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	var messageToAck *taskiq.TaskMessage
	ackSignal := make(chan *taskiq.TaskMessage, 1)

	handlerFunc := func(c context.Context, msg *taskiq.TaskMessage) error {
		messageToAck = msg // Capture the message with BrokerMessageID (DeliveryTag)
		ackSignal <- messageToAck
		wg.Done()
		return fmt.Errorf("intentional error to prevent implicit ack") // Prevent implicit Ack
	}

	consumeErrCh := make(chan error, 1)
	go func() {
		err := broker.ConsumeMessages(ctx, queueName, handlerFunc)
		if err != nil && !shouldIgnoreConsumeError(err) { // Ignore normal shutdown errors
			consumeErrCh <- err
		}
		close(consumeErrCh)
	}()

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for handler to capture message")
	case msgToAck := <-ackSignal:
		if msgToAck == nil || msgToAck.BrokerMessageID == nil {
			t.Fatal("Handler did not provide a message with BrokerMessageID to Ack")
		}
		// Now explicitly Ack the message using the broker's Ack method
		if err := broker.Ack(context.Background(), queueName, msgToAck); err != nil {
			t.Fatalf("broker.Ack failed: %v", err)
		}
	}
	
	// Wait for handler to complete its execution (wg.Done will be called)
	handlerWgDone := make(chan struct{})
	go func(){
		wg.Wait()
		close(handlerWgDone)
	}()
	select {
	case <-time.After(1 * time.Second):
		t.Log("Handler WaitGroup did not complete quickly after ackSignal, might be okay.")
	case <-handlerWgDone:
	}

	cancel() // Stop consumer
	if err := <-consumeErrCh; err != nil {
		t.Fatalf("ConsumeMessages error: %v", err)
	}

	time.Sleep(100 * time.Millisecond) // Give time for Ack to process on server
	_, ok, _ := userCh.Get(queueName, true)
	if ok {
		t.Error("Message still in queue after explicit Ack.")
	}
}


func TestRabbitMQBroker_Close(t *testing.T) {
	broker, userCh, cleanupBroker := setupRabbitMQTestBroker(t) // userCh not strictly needed here but setup is convenient
	defer cleanupBroker() // This ensures broker's resources are closed if test fails early.

	queueName := uniqueQueueName("test_close_consumer")
	ctx, cancel := context.WithCancel(context.Background())

	if err := broker.DeclareQueue(context.Background(), queueName); err != nil {
		t.Fatalf("DeclareQueue failed: %v", err)
	}
	defer deleteQueue(t, userCh, queueName) // Ensure queue cleanup

	consumeDone := make(chan struct{})
	go func() {
		err := broker.ConsumeMessages(ctx, queueName, func(c context.Context, m *taskiq.TaskMessage) error { return nil })
		if err != nil && !shouldIgnoreConsumeError(err) {
			t.Errorf("ConsumeMessages unexpected error: %v", err)
		}
		close(consumeDone)
	}()

	time.Sleep(200 * time.Millisecond) // Allow consumer to start

	if err := broker.Close(); err != nil {
		t.Fatalf("broker.Close() failed: %v", err)
	}
	cancel() // Cancel consumer's context as well, good practice

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for consumer to stop after broker.Close()")
	case <-consumeDone:
	}

	errPublish := broker.PublishMessage(context.Background(), queueName, &taskiq.TaskMessage{TaskID: "after_close"})
	if errPublish == nil {
		t.Error("PublishMessage after Close succeeded.")
	}
}

func TestRabbitMQBroker_ConsumeMessages_UnmarshalError(t *testing.T) {
	broker, userCh, cleanup := setupRabbitMQTestBroker(t)
	defer cleanup()

	queueName := uniqueQueueName("test_unmarshal_err")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := broker.DeclareQueue(ctx, queueName); err != nil {
		t.Fatalf("DeclareQueue failed: %v", err)
	}
	defer deleteQueue(t, userCh, queueName)

	malformedBody := []byte(`{"task_id": "unmarshal_task", "args": "not_an_array"`) // Invalid JSON for args
	err := userCh.Publish("", queueName, false, false, amqp.Publishing{ContentType: "application/json", Body: malformedBody})
	if err != nil {
		t.Fatalf("Failed to publish malformed message: %v", err)
	}

	handlerCalled := false
	consumeErrCh := make(chan error, 1)
	go func() {
		errConsume := broker.ConsumeMessages(ctx, queueName, func(c context.Context, m *taskiq.TaskMessage) error { handlerCalled = true; return nil })
		if errConsume != nil && !shouldIgnoreConsumeError(errConsume) {
			consumeErrCh <- errConsume
		}
		close(consumeErrCh)
	}()

	time.Sleep(1 * time.Second) // Allow time for message pickup and processing attempt
	cancel()
	if err := <-consumeErrCh; err != nil {
		t.Fatalf("ConsumeMessages error: %v", err)
	}

	if handlerCalled {
		t.Error("Handler called for malformed message.")
	}
	_, ok, _ := userCh.Get(queueName, true)
	if ok {
		t.Error("Malformed message still in queue after Nack (requeue=false).")
	}
}

// shouldIgnoreConsumeError checks if the error from ConsumeMessages is expected during shutdown.
func shouldIgnoreConsumeError(err error) bool {
	if err == context.Canceled {
		return true
	}
	if err != nil {
		// Check for common AMQP errors on shutdown
		errMsg := err.Error()
		if strings.Contains(errMsg, "channel/connection is not open") ||
			strings.Contains(errMsg, "channel closed") ||
			strings.Contains(errMsg, "connection closed") {
			return true
		}
		// Check for specific AMQP error codes
		if amqpErr, ok := err.(*amqp.Error); ok {
			return amqpErr.Code == amqp.ErrClosed || // 504
				   amqpErr.Code == amqp.ChannelClosed || // Other channel closed variants
				   amqpErr.Code == amqp.ConnectionForced // 320, if connection forced close
		}
	}
	return false
}
