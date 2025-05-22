package brokers

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pebbe/zmq4"
	"gitlab.com/taskiq/taskiq/pkg/taskiq"
)

const (
	defaultZMQBaseAddr = "ipc:///tmp/taskiq_zmq_"
)

// ZMQBroker is a broker that uses ZeroMQ.
// It uses PUSH sockets for publishing and PULL sockets for consuming.
type ZMQBroker struct {
	baseAddr string
	context  *zmq4.Context

	// Sockets need to be managed carefully.
	// Publishers (PUSH) are typically created on demand per queue.
	// Consumers (PULL) are created and kept open by ConsumeMessages.
	// We need a way to close them.
	pullSockets map[string]*zmq4.Socket // queueName -> PULL socket
	pushSockets map[string]*zmq4.Socket // queueName -> PUSH socket
	mu          sync.Mutex              // To protect access to socket maps

	// Global context for the broker instance, and a cancel func to signal shutdown
	brokerCtx    context.Context
	brokerCancel context.CancelFunc
}

// ZMQBrokerOptions holds options for ZMQBroker.
type ZMQBrokerOptions struct {
	BaseAddress string // e.g., "tcp://localhost" or "ipc:///tmp/taskiq_"
}

// NewZMQBroker creates a new ZMQBroker.
func NewZMQBroker(opts *ZMQBrokerOptions) (taskiq.Broker, error) {
	zmqContext, err := zmq4.NewContext()
	if err != nil {
		return nil, fmt.Errorf("failed to create ZeroMQ context: %w", err)
	}

	addr := defaultZMQBaseAddr
	if opts != nil && opts.BaseAddress != "" {
		addr = opts.BaseAddress
		if addr[len(addr)-1] != '/' && addr[len(addr)-1] != '_' { // ensure trailing separator if needed by convention
			// This depends on how BaseAddress is used. If it's a prefix for IPC, it might need a separator.
			// For TCP, it's more complex (host vs host:port).
			// Assuming IPC style prefix for now.
			addr += "_" 
		}
	}
	
	brokerGlobalCtx, brokerGlobalCancel := context.WithCancel(context.Background())

	return &ZMQBroker{
		baseAddr:     addr,
		context:      zmqContext,
		pullSockets:  make(map[string]*zmq4.Socket),
		pushSockets:  make(map[string]*zmq4.Socket),
		brokerCtx:    brokerGlobalCtx,
		brokerCancel: brokerGlobalCancel,
	}, nil
}

func (b *ZMQBroker) getQueueAddr(queueName string) string {
	// Example: ipc:///tmp/taskiq_zmq_myqueue.ipc or tcp://localhost:5555 (if queueName maps to port)
	// For IPC, ensuring unique filenames is important.
	return fmt.Sprintf("%s%s.ipc", b.baseAddr, queueName)
}

// DeclareQueue prepares a queue for use. For ZMQ PULL sockets, this means binding it.
// We don't do it here, as the PULL socket is typically bound by the consumer.
// For PUSH/PULL, the "declaration" is more about knowing the address.
// This method can be a no-op or ensure the ZMQ context is valid.
func (b *ZMQBroker) DeclareQueue(ctx context.Context, queueName string) error {
	// With ZMQ, PUSH sockets connect to PULL sockets. PULL sockets bind.
	// "Declaring" a queue could mean ensuring the PULL side is ready,
	// but that's part of ConsumeMessages.
	// For now, this can be a no-op.
	// We could check if the broker context is active.
	select {
	case <-b.brokerCtx.Done():
		return fmt.Errorf("broker is shutting down: %w", b.brokerCtx.Err())
	default:
		// broker is active
	}
	return nil
}

// PublishMessage publishes a message to a ZMQ PUSH socket.
func (b *ZMQBroker) PublishMessage(ctx context.Context, queueName string, message *taskiq.TaskMessage) error {
	select {
	case <-b.brokerCtx.Done():
		return fmt.Errorf("broker is shutting down: %w", b.brokerCtx.Err())
	case <-ctx.Done():
		return fmt.Errorf("publish context cancelled: %w", ctx.Err())
	default:
	}

	b.mu.Lock()
	pushSocket, exists := b.pushSockets[queueName]
	if !exists {
		var err error
		pushSocket, err = b.context.NewSocket(zmq4.PUSH)
		if err != nil {
			b.mu.Unlock()
			return fmt.Errorf("failed to create ZMQ PUSH socket for queue %s: %w", queueName, err)
		}
		// For PUSH sockets, they connect to the PULL socket's bind address.
		// It's important that the PULL socket (consumer) is already bound or will be bound.
		// ZMQ allows connect before bind, messages will be queued or dropped based on HWM.
		addr := b.getQueueAddr(queueName)
		err = pushSocket.Connect(addr)
		if err != nil {
			b.mu.Unlock()
			pushSocket.Close() // clean up created socket
			return fmt.Errorf("failed to connect ZMQ PUSH socket to %s: %w", addr, err)
		}
		b.pushSockets[queueName] = pushSocket
	}
	b.mu.Unlock()

	if message.TaskID == "" {
		message.TaskID = uuid.NewString()
	}
	if message.Timestamp.IsZero() {
		message.Timestamp = time.Now().UTC()
	}
	// BrokerMessageID is not used by ZMQ PUSH/PULL for sending.
	message.BrokerMessageID = nil 

	msgBytes, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal TaskMessage: %w", err)
	}

	// Send with DONTWAIT to prevent blocking indefinitely if HWM is reached.
	// However, the taskiq interface implies PublishMessage can block or return error.
	// Let's use blocking send for now, but this is a point of consideration.
	// ZMQ_DONTWAIT could be used with error handling for EAGAIN.
	_, err = pushSocket.SendBytes(msgBytes, 0) // 0 for blocking send
	if err != nil {
		// If send fails, we might want to close and remove the socket from the map
		// so it can be re-established on next publish.
		b.mu.Lock()
		delete(b.pushSockets, queueName)
		b.mu.Unlock()
		pushSocket.Close()
		return fmt.Errorf("failed to send message on ZMQ PUSH socket for queue %s: %w", queueName, err)
	}

	return nil
}

// ConsumeMessages consumes messages from a ZMQ PULL socket.
func (b *ZMQBroker) ConsumeMessages(
	ctx context.Context, // This is the per-consumer context
	queueName string,
	handler func(ctx context.Context, message *taskiq.TaskMessage) error,
) error {
	select {
	case <-b.brokerCtx.Done(): // Check if the broker itself is shutting down
		return fmt.Errorf("broker is shutting down: %w", b.brokerCtx.Err())
	default:
	}
	
	b.mu.Lock()
	if _, exists := b.pullSockets[queueName]; exists {
		b.mu.Unlock()
		return fmt.Errorf("consumer already exists for queue %s", queueName)
	}

	pullSocket, err := b.context.NewSocket(zmq4.PULL)
	if err != nil {
		b.mu.Unlock()
		return fmt.Errorf("failed to create ZMQ PULL socket for queue %s: %w", queueName, err)
	}

	addr := b.getQueueAddr(queueName)
	err = pullSocket.Bind(addr)
	if err != nil {
		b.mu.Unlock()
		pullSocket.Close()
		return fmt.Errorf("failed to bind ZMQ PULL socket to %s: %w", addr, err)
	}
	b.pullSockets[queueName] = pullSocket
	b.mu.Unlock()

	// Deregister and close socket when the consumer goroutine exits.
	defer func() {
		b.mu.Lock()
		delete(b.pullSockets, queueName)
		b.mu.Unlock()
		pullSocket.Close()
		fmt.Printf("Closed ZMQ PULL socket for queue %s\n", queueName)
	}()

	fmt.Printf("ZMQ PULL socket bound to %s, waiting for messages...\n", addr)

	go func() {
		// Combine broker context and consumer context
		// If broker shuts down, all consumers should stop.
		// If consumer's specific context is cancelled, it should stop.
		consumerCtx, consumerCancel := context.WithCancel(ctx)
		defer consumerCancel() // Ensure resources tied to consumerCtx are released

		go func() {
			select {
			case <-b.brokerCtx.Done(): // If broker context is cancelled
				consumerCancel() // Cancel the consumer's context
			case <-consumerCtx.Done(): // If consumer context is cancelled (e.g. by caller)
				// already handled by the main loop's select
			}
		}()

		for {
			select {
			case <-consumerCtx.Done(): // handles both broker shutdown and specific consumer cancellation
				fmt.Printf("Consumer for queue %s stopping: %v\n", queueName, consumerCtx.Err())
				return
			default:
				// Use a short timeout for Recv to make the loop responsive to context cancellation.
				// ZMQ sockets can be polled, or Recv can have a timeout.
				// Using SetRcvtimeo for this.
				pullSocket.SetRcvtimeo(1 * time.Second)
				msgBytes, err := pullSocket.RecvBytes(0) // 0 for blocking (respecting Rcvtimeo)
				
				if err != nil {
					if zmqErr, ok := err.(zmq4.Errno); ok && zmqErr == zmq4.EAGAIN {
						// Timeout, good place to check context again
						continue
					}
					// Real error
					fmt.Printf("Error receiving from ZMQ PULL socket for queue %s: %v. Stopping consumer.\n", queueName, err)
					return // Exit goroutine on receive error
				}

				if len(msgBytes) == 0 { // Should not happen with RecvBytes unless an empty message was sent
					continue
				}

				var taskMsg taskiq.TaskMessage
				if err := json.Unmarshal(msgBytes, &taskMsg); err != nil {
					fmt.Printf("Error unmarshaling ZMQ message on queue %s: %v. Message: %s\n", queueName, err, string(msgBytes))
					// Decide how to handle: skip, DLQ? For now, skip.
					continue
				}
				
				// For ZMQ PUSH/PULL, the message is gone from the "queue" once Recv'd.
				// BrokerMessageID is not applicable here for Ack, but can be set if useful for logging/tracing.
				taskMsg.BrokerMessageID = nil // Or some identifier if available and meaningful

				err = handler(consumerCtx, &taskMsg) // Pass the cancellable consumerCtx
				if err != nil {
					fmt.Printf("Error processing message (TaskID: %s) from ZMQ queue %s: %v\n", taskMsg.TaskID, queueName, err)
					// No Nack/requeue mechanism in basic PUSH/PULL. Message is already consumed.
					// Error handling strategy (retry, DLQ) would be up to the application/handler.
				} else {
					// Message processed successfully. Ack is a no-op.
					// The Ack method will be called by the worker, but it won't do anything ZMQ-specific.
					// No need to explicitly call b.Ack here, the worker will do it.
				}
			}
		}
	}()

	return nil // Return nil to indicate the consumer loop has started
}

// Ack for ZMQ PUSH/PULL is a no-op, as messages are removed from the queue upon successful receive.
func (b *ZMQBroker) Ack(ctx context.Context, queueName string, message *taskiq.TaskMessage) error {
	// In PUSH/PULL, message is "acked" once it's received by PULL socket.
	// No further action required by the broker.
	return nil
}

// Close closes all managed ZMQ sockets and the ZMQ context.
func (b *ZMQBroker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	fmt.Println("Closing ZMQBroker...")
	
	// Signal all derived contexts (like those in ConsumeMessages) to cancel.
	b.brokerCancel()


	for queueName, socket := range b.pushSockets {
		fmt.Printf("Closing PUSH socket for queue %s\n", queueName)
		socket.Close()
	}
	b.pushSockets = make(map[string]*zmq4.Socket) // Clear map

	for queueName, socket := range b.pullSockets {
		fmt.Printf("Closing PULL socket for queue %s\n", queueName)
		// Note: PULL sockets are also closed by their consumer goroutine's defer.
		// Closing here ensures cleanup if ConsumeMessages wasn't called or exited abruptly.
		// ZMQ socket Close is idempotent.
		socket.Close()
	}
	b.pullSockets = make(map[string]*zmq4.Socket) // Clear map

	if b.context != nil {
		err := b.context.Term()
		if err != nil {
			return fmt.Errorf("failed to terminate ZMQ context: %w", err)
		}
		b.context = nil // Mark as terminated
		fmt.Println("ZMQ context terminated.")
	}
	return nil
}

// Ping can check if the ZMQ context is valid or if a test message can be sent/received.
// For simplicity, we'll just check if the context exists.
func (b *ZMQBroker) Ping(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.context == nil {
		return fmt.Errorf("zmq broker is closed or not initialized")
	}
	// Could also try to create a temporary socket.
	return nil
}

// Ensure ZMQBroker implements Broker interface
var _ taskiq.Broker = (*ZMQBroker)(nil)
