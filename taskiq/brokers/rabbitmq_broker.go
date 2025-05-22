package brokers

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/streadway/amqp"
	"gitlab.com/taskiq/taskiq/pkg/taskiq"
)

// RabbitMQBroker is a broker that uses RabbitMQ.
type RabbitMQBroker struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	mu      sync.Mutex // For thread-safe operations on the channel, especially for publishing

	// For managing consumer goroutines and graceful shutdown
	consumerWg   sync.WaitGroup
	consumerStop chan struct{} // Used to signal consumers to stop
}

// NewRabbitMQBroker creates a new RabbitMQBroker.
// url is the RabbitMQ connection string, e.g., "amqp://guest:guest@localhost:5672/"
func NewRabbitMQBroker(url string) (taskiq.Broker, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close() // Clean up connection if channel creation fails
		return nil, fmt.Errorf("failed to open a channel: %w", err)
	}

	// Optional: Handle connection and channel closures
	// go func() {
	// 	fmt.Printf("RabbitMQ connection error: %s\n", <-conn.NotifyClose(make(chan *amqp.Error)))
	// 	// Implement reconnection logic or notify application
	// }()
	// go func() {
	// 	fmt.Printf("RabbitMQ channel error: %s\n", <-ch.NotifyClose(make(chan *amqp.Error)))
	// 	// Implement channel re-creation logic or notify application
	// }()

	return &RabbitMQBroker{
		conn:         conn,
		channel:      ch,
		consumerStop: make(chan struct{}),
	}, nil
}

// DeclareQueue declares a durable queue in RabbitMQ.
func (b *RabbitMQBroker) DeclareQueue(ctx context.Context, queueName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	_, err := b.channel.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue %s: %w", queueName, err)
	}
	return nil
}

// PublishMessage publishes a message to a RabbitMQ queue.
func (b *RabbitMQBroker) PublishMessage(ctx context.Context, queueName string, message *taskiq.TaskMessage) error {
	if message.TaskID == "" {
		message.TaskID = uuid.NewString()
	}
	if message.Timestamp.IsZero() {
		message.Timestamp = time.Now().UTC()
	}
	message.BrokerMessageID = nil // DeliveryTag is only relevant for received messages

	msgBytes, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal TaskMessage: %w", err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// Check if channel or connection is closed
	if b.channel == nil || b.conn == nil || b.conn.IsClosed() {
		return fmt.Errorf("RabbitMQ connection or channel is not available")
	}
	
	err = b.channel.Publish(
		"",        // exchange (default)
		queueName, // routing key (queue name)
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent, // Mark message as persistent
			Body:         msgBytes,
			MessageId:    message.TaskID, // Use TaskID as AMQP MessageId
			Timestamp:    message.Timestamp,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish message to queue %s: %w", queueName, err)
	}
	return nil
}

// ConsumeMessages consumes messages from a RabbitMQ queue.
func (b *RabbitMQBroker) ConsumeMessages(
	ctx context.Context, // Per-consumer context
	queueName string,
	handler func(ctx context.Context, message *taskiq.TaskMessage) error,
) error {
	// Lock to safely access b.channel for consumption setup (QueueDeclare, Qos, Consume)
	b.mu.Lock() 
	if b.channel == nil || b.conn == nil || b.conn.IsClosed() {
		b.mu.Unlock()
		return fmt.Errorf("RabbitMQ connection or channel is not available for consuming")
	}

	// Ensure the queue is declared. This is idempotent.
	// Done outside the consumer goroutine, but on the shared channel.
	_, err := b.channel.QueueDeclare(
		queueName, true, false, false, false, nil,
	)
	if err != nil {
		b.mu.Unlock()
		return fmt.Errorf("failed to declare queue %s for consumer: %w", queueName, err)
	}
	
	// Set Qos for the shared channel. This will apply to all consumers on this channel.
	// This is a potential issue if different consumers need different QoS.
	// For simplicity now, we set it once. If ConsumeMessages is called multiple times,
	// it will be reset. This should ideally be set once when channel is created or configured globally.
	// For now, setting it here means the last caller of ConsumeMessages dictates QoS.
	err = b.channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global (per-consumer on this channel)
	)
	if err != nil {
		b.mu.Unlock()
		return fmt.Errorf("failed to set QoS for channel: %w", err)
	}

	// consumerTag can be unique for each consumer if needed for management
	consumerTag := fmt.Sprintf("consumer-%s-%s", queueName, uuid.NewString()[:8])

	msgs, err := b.channel.Consume(
		queueName,   // queue
		consumerTag, // consumer tag
		false,       // auto-ack (false for manual ack)
		false,       // exclusive
		false,       // no-local (not relevant for default exchange)
		false,       // no-wait
		nil,         // args
	)
	if err != nil {
		b.mu.Unlock()
		return fmt.Errorf("failed to register a consumer for queue %s: %w", queueName, err)
	}
	b.mu.Unlock() // Unlock after consumption setup is done

	b.consumerWg.Add(1)
	go func() {
		defer b.consumerWg.Done()
		// Note: We don't close b.channel here as it's shared.
		// It will be closed by RabbitMQBroker.Close()

		fmt.Printf("Consumer %s started for queue %s. Waiting for messages...\n", consumerTag, queueName)
		for {
			select {
			case <-ctx.Done(): // Context for this specific consumer is cancelled
				fmt.Printf("Consumer for queue %s stopping due to context cancellation: %v\n", queueName, ctx.Err())
				return
			case <-b.consumerStop: // Broker is shutting down
				fmt.Printf("Consumer for queue %s stopping due to broker shutdown.\n", queueName)
				return
			case delivery, ok := <-msgs:
				if !ok {
					fmt.Printf("Delivery channel closed for consumer on queue %s. Goroutine exiting.\n", queueName)
					// This can happen if the channel or connection closes.
					return
				}

				var taskMsg taskiq.TaskMessage
				if err := json.Unmarshal(delivery.Body, &taskMsg); err != nil {
					fmt.Printf("Error unmarshaling RabbitMQ message on queue %s: %v. Message body: %s. Nacking.\n", queueName, err, string(delivery.Body))
					errNack := delivery.Nack(false, false) // Nack(multiple, requeue) - don't requeue malformed message
					if errNack != nil {
						fmt.Printf("Error Nacking message on queue %s: %v\n", queueName, errNack)
					}
					continue
				}

				taskMsg.BrokerMessageID = delivery.DeliveryTag // Store DeliveryTag for Ack/Nack

				handlerCtx, handlerCancel := context.WithTimeout(ctx, 30*time.Second) // Example: give handler 30s
				err := handler(handlerCtx, &taskMsg)
				handlerCancel()

				if err != nil {
					fmt.Printf("Error processing message (TaskID: %s, AMQP MsgID: %s) from RabbitMQ queue %s: %v. Nacking.\n", taskMsg.TaskID, delivery.MessageId, queueName, err)
					// Nack the message, decide whether to requeue based on error type or policy
					// For now, don't requeue on handler error to avoid poison pills.
					errNack := delivery.Nack(false, false) // Nack(multiple, requeue)
					if errNack != nil {
						fmt.Printf("Error Nacking message on queue %s (DeliveryTag: %d): %v\n", queueName, delivery.DeliveryTag, errNack)
					}
				} else {
					// Message processed successfully, Ack it using the main broker Ack method.
					// This ensures that Ack logic (if it becomes more complex) is centralized.
					// The `queueName` is passed as per interface, though not strictly needed for amqp.Ack with deliveryTag.
					ackErr := b.Ack(context.Background(), queueName, &taskMsg) // Use fresh context for Ack
					if ackErr != nil {
						// This is tricky. If Ack fails, the message might be redelivered.
						// Log critical error. May need more robust handling (e.g. panic, or try to Nack?)
						fmt.Printf("CRITICAL: Error Acknowledging message on queue %s (DeliveryTag: %d, TaskID: %s): %v\n",
							queueName, delivery.DeliveryTag, taskMsg.TaskID, ackErr)
					}
				}
			}
		}
	}()

	return nil // Return nil to indicate the consumer loop has started
}

// Ack acknowledges a message using its DeliveryTag.
// The queueName parameter is part of the interface but not used by amqp.Channel.Ack.
func (b *RabbitMQBroker) Ack(ctx context.Context, queueName string, message *taskiq.TaskMessage) error {
	if message.BrokerMessageID == nil {
		return fmt.Errorf("cannot ack message: BrokerMessageID is nil. TaskID: %s", message.TaskID)
	}

	deliveryTag, ok := message.BrokerMessageID.(uint64)
	if !ok {
		return fmt.Errorf("cannot ack message: BrokerMessageID is not uint64 (DeliveryTag). TaskID: %s, Type: %T", message.TaskID, message.BrokerMessageID)
	}

	// Ack must be performed on the *same channel the message was received on*.
	// This is a major issue with the current design of having Ack on the main broker
	// while ConsumeMessages uses a dedicated channel.
	//
	// Option 1: Ack/Nack must be methods on the `amqp.Delivery` object itself (delivery.Ack, delivery.Nack).
	// This is what `streadway/amqp` provides and is the standard way.
	// This means the handler in ConsumeMessages should call delivery.Ack() or delivery.Nack() directly.
	// The Broker.Ack() method would then either be a no-op, or the interface needs rethinking for RabbitMQ.
	//
	// Option 2: The Broker.Ack method needs access to the specific channel the message came from.
	// This is complex to manage.
	//
	// Let's go with the implication of `streadway/amqp`: Ack/Nack are tied to the delivery.
	// The `Broker.Ack` method, as defined, is problematic for RabbitMQ if consumers use dedicated channels.
	//
	// For now, to satisfy the interface and make it "work" for a single-channel consumer (which we moved away from for Consume):
	// If we were using b.channel for consuming, this would be:
	// return b.channel.Ack(deliveryTag, false) // false for single message ack
	//
	// Given that ConsumeMessages now creates a dedicated channel, the b.channel.Ack here is WRONG.
	// The Ack must happen on `consChannel` inside `ConsumeMessages`.
	//
	// I will adjust ConsumeMessages to call delivery.Ack directly.
	// The Broker.Ack method will then be problematic.
	// Let's assume for the purpose of this task that Broker.Ack is called by the worker
	// AFTER the handler, and it should somehow work. This implies that `BrokerMessageID`
	// must contain enough info, or the channel must be the shared one.
	//
	// Reverting ConsumeMessages to use b.channel for now to make b.Ack work as "intended" by the interface,
	// but this has performance/reliability implications.
	// The alternative is that the worker calls delivery.Ack() if the message object exposes it.
	//
	// Let's stick to the interface: b.Ack is called.
	// This means ConsumeMessages must use b.channel, and b.mu must protect it.
	// This will be a bottleneck. The alternative is to document that RabbitMQBroker.Ack might not be used
	// if handler performs its own ack through delivery object.
	//
	// For now, I will make `b.Ack` use `b.channel.Ack` and `ConsumeMessages` will also use `b.channel`.
	// This simplifies `Ack` but makes `ConsumeMessages` less robust for concurrent consumers on different queues.
	// Given the task prompt: "Ack method: Use channel.Ack", it implies the main channel.
	// I will revert ConsumeMessages to use the shared channel and protect it.

	// This lock is crucial if b.channel is shared by multiple goroutines (e.g. multiple consumers or publishers)
	b.mu.Lock()
	defer b.mu.Unlock()
	
	if b.channel == nil || b.conn == nil || b.conn.IsClosed() {
		return fmt.Errorf("RabbitMQ connection or channel is not available for Ack")
	}

	err := b.channel.Ack(deliveryTag, false) // false for Ack of single message
	if err != nil {
		return fmt.Errorf("failed to Ack message (DeliveryTag: %d): %w", deliveryTag, err)
	}
	return nil
}

// Close closes the RabbitMQ channel and connection.
func (b *RabbitMQBroker) Close() error {
	fmt.Println("Closing RabbitMQBroker...")
	close(b.consumerStop) // Signal all consumers to stop

	b.mu.Lock() // Ensure no operations are ongoing
	defer b.mu.Unlock()

	// Wait for consumer goroutines to finish, with a timeout
	waitTimeout := 5 * time.Second
	done := make(chan struct{})
	go func() {
		b.consumerWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		fmt.Println("All RabbitMQ consumers stopped.")
	case <-time.After(waitTimeout):
		fmt.Println("Timed out waiting for RabbitMQ consumers to stop.")
	}

	var lastErr error
	if b.channel != nil {
		fmt.Println("Closing RabbitMQ channel.")
		if err := b.channel.Close(); err != nil {
			lastErr = fmt.Errorf("failed to close RabbitMQ channel: %w", err)
			fmt.Printf("Error closing RabbitMQ channel: %v\n", err)
		}
		b.channel = nil
	}
	if b.conn != nil {
		fmt.Println("Closing RabbitMQ connection.")
		if err := b.conn.Close(); err != nil {
			if lastErr != nil {
				lastErr = fmt.Errorf("%v; failed to close RabbitMQ connection: %w", lastErr, err)
			} else {
				lastErr = fmt.Errorf("failed to close RabbitMQ connection: %w", err)
			}
			fmt.Printf("Error closing RabbitMQ connection: %v\n", err)
		}
		b.conn = nil
	}
	fmt.Println("RabbitMQBroker closed.")
	return lastErr
}

// Ping checks the connection to RabbitMQ.
// A simple way is to try to open a new channel.
func (b *RabbitMQBroker) Ping(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.conn == nil || b.conn.IsClosed() {
		return fmt.Errorf("no active RabbitMQ connection")
	}

	// Try to open a new channel and immediately close it.
	ch, err := b.conn.Channel()
	if err != nil {
		return fmt.Errorf("ping failed to open test channel: %w", err)
	}
	ch.Close()
	return nil
}

// Ensure RabbitMQBroker implements Broker interface
var _ taskiq.Broker = (*RabbitMQBroker)(nil)
