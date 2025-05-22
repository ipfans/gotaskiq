package brokers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"gitlab.com/taskiq/taskiq/pkg/taskiq" // Assuming taskiq.TaskMessage is here
)

// RedisStreamBroker is a broker that uses Redis Streams.
type RedisStreamBroker struct {
	client *redis.Client
	opts   *RedisStreamBrokerOptions
}

// RedisStreamBrokerOptions holds options for RedisStreamBroker.
type RedisStreamBrokerOptions struct {
	// Options like default MaxLen for streams, etc. can be added here.
	// For now, keeping it simple.
}

// NewRedisStreamBroker creates a new RedisStreamBroker.
func NewRedisStreamBroker(client *redis.Client, opts *RedisStreamBrokerOptions) taskiq.Broker {
	if opts == nil {
		opts = &RedisStreamBrokerOptions{}
	}
	return &RedisStreamBroker{
		client: client,
		opts:   opts,
	}
}

// DeclareQueue declares a queue (stream) in Redis.
func (b *RedisStreamBroker) DeclareQueue(ctx context.Context, queueName string) error {
	_, err := b.client.XInfoStream(ctx, queueName).Result()
	if err == redis.Nil {
		args := &redis.XAddArgs{
			Stream: queueName,
			ID:     "*",
			Values: map[string]interface{}{"_taskiq_init_stream_": "1"}, // Ensure stream exists
		}
		_, err = b.client.XAdd(ctx, args).Result()
		if err != nil {
			return fmt.Errorf("failed to create stream %s with initial message: %w", queueName, err)
		}
		// Optionally trim the stream if only one init message is desired.
		// b.client.XTrim(ctx, queueName, 1).Result() // Keeps the stream small
	} else if err != nil {
		return fmt.Errorf("failed to check stream %s: %w", queueName, err)
	}
	return nil
}

// PublishMessage publishes a message to a Redis Stream.
func (b *RedisStreamBroker) PublishMessage(ctx context.Context, queueName string, message *taskiq.TaskMessage) error {
	if message.TaskID == "" {
		message.TaskID = uuid.NewString()
	}
	if message.Timestamp.IsZero() {
		message.Timestamp = time.Now().UTC()
	}

	argsJSON, err := json.Marshal(message.Args)
	if err != nil {
		return fmt.Errorf("failed to marshal message.Args: %w", err)
	}

	kwargsJSON, err := json.Marshal(message.Kwargs)
	if err != nil {
		return fmt.Errorf("failed to marshal message.Kwargs: %w", err)
	}

	headersJSON, err := json.Marshal(message.Headers)
	if err != nil {
		return fmt.Errorf("failed to marshal message.Headers: %w", err)
	}

	values := map[string]interface{}{
		"task_id":    message.TaskID,
		"task_name":  message.TaskName,
		"args":       string(argsJSON),
		"kwargs":     string(kwargsJSON),
		"timestamp":  message.Timestamp.Format(time.RFC3339Nano),
		"headers":    string(headersJSON),
	}

	xaddArgs := &redis.XAddArgs{
		Stream: queueName,
		ID:     "*", // Auto-generate ID by Redis
		Values: values,
	}

	_, err = b.client.XAdd(ctx, xaddArgs).Result()
	if err != nil {
		return fmt.Errorf("failed to publish message to stream %s: %w", queueName, err)
	}
	return nil
}

// ConsumeMessages consumes messages from a Redis Stream.
func (b *RedisStreamBroker) ConsumeMessages(
	ctx context.Context,
	queueName string,
	handler func(ctx context.Context, message *taskiq.TaskMessage) error,
) error {
	lastID := "0-0" // Start reading from the beginning of the stream for a new consumer.

	go func() {
		for {
			select {
			case <-ctx.Done():
				// Context cancelled, stop consuming
				return
			default:
				readArgs := &redis.XReadArgs{
					Streams: []string{queueName, lastID},
					Block:   2 * time.Second, // Block for 2 seconds, then re-check context
					Count:   1,               // Process one message at a time
				}
				streams, err := b.client.XRead(ctx, readArgs).Result()

				if err != nil {
					if err == redis.Nil || err == context.DeadlineExceeded || err == context.Canceled {
						continue // Timeout or context done, loop to check ctx.Done()
					}
					fmt.Printf("Error reading from stream %s: %v\n", queueName, err)
					time.Sleep(1 * time.Second) // Backoff on other errors
					continue
				}

				for _, stream := range streams {
					for _, redisMsg := range stream.Messages {
						taskMsg := &taskiq.TaskMessage{}

						if idVal, ok := redisMsg.Values["task_id"].(string); ok {
							taskMsg.TaskID = idVal
						}
						if nameVal, ok := redisMsg.Values["task_name"].(string); ok {
							taskMsg.TaskName = nameVal
						}
						if tsVal, ok := redisMsg.Values["timestamp"].(string); ok {
							taskMsg.Timestamp, _ = time.Parse(time.RFC3339Nano, tsVal)
						}

						if argsVal, ok := redisMsg.Values["args"].(string); ok {
							if err := json.Unmarshal([]byte(argsVal), &taskMsg.Args); err != nil {
								fmt.Printf("Error unmarshaling args for message %s: %v\n", redisMsg.ID, err)
								// Decide how to handle: skip, Nack, DLQ? For now, skip.
								lastID = redisMsg.ID
								continue
							}
						}
						if kwargsVal, ok := redisMsg.Values["kwargs"].(string); ok {
							if err := json.Unmarshal([]byte(kwargsVal), &taskMsg.Kwargs); err != nil {
								fmt.Printf("Error unmarshaling kwargs for message %s: %v\n", redisMsg.ID, err)
								lastID = redisMsg.ID
								continue
							}
						}
						if headersVal, ok := redisMsg.Values["headers"].(string); ok {
							if err := json.Unmarshal([]byte(headersVal), &taskMsg.Headers); err != nil {
								fmt.Printf("Error unmarshaling headers for message %s: %v\n", redisMsg.ID, err)
								lastID = redisMsg.ID
								continue
							}
						}
						
						taskMsg.BrokerMessageID = redisMsg.ID // Store Redis Stream message ID

						err := handler(ctx, taskMsg)
						if err != nil {
							fmt.Printf("Error processing message %s (TaskID: %s): %v\n", redisMsg.ID, taskMsg.TaskID, err)
							// Message processing failed. It won't be Acked.
							// Future: implement Nack or dead-letter queue.
						} else {
							// Message processed successfully, Ack it.
							ackErr := b.Ack(ctx, taskMsg)
							if ackErr != nil {
								fmt.Printf("Error acking message %s (TaskID: %s): %v\n", redisMsg.ID, taskMsg.TaskID, ackErr)
							}
						}
						lastID = redisMsg.ID // Update lastID to read next messages
					}
				}
			}
		}
	}()
	return nil // Return nil to indicate the consumer loop has started
}

// Ack acknowledges a message using XDEL (deletes it from the stream).
// It expects BrokerMessageID to be populated in the TaskMessage, which should be the Redis Stream ID.
func (b *RedisStreamBroker) Ack(ctx context.Context, queueName string, message *taskiq.TaskMessage) error {
	if message.BrokerMessageID == nil {
		return fmt.Errorf("cannot ack message: BrokerMessageID is nil. TaskID: %s", message.TaskID)
	}

	redisMsgID, ok := message.BrokerMessageID.(string)
	if !ok {
		return fmt.Errorf("cannot ack message: BrokerMessageID is not a string (TaskID: %s, Type: %T)", message.TaskID, message.BrokerMessageID)
	}

	if queueName == "" {
		return fmt.Errorf("cannot ack message: queueName is empty. TaskID: %s, RedisMsgID: %s", message.TaskID, redisMsgID)
	}

	_, err := b.client.XDel(ctx, queueName, redisMsgID).Result()
	if err != nil {
		return fmt.Errorf("failed to ack (XDEL) message %s from stream %s: %w", redisMsgID, queueName, err)
	}
	return nil
}

// Close closes the Redis client connection.
func (b *RedisStreamBroker) Close() error {
	return b.client.Close()
}

// Ping checks the connection to Redis.
func (b *RedisStreamBroker) Ping(ctx context.Context) error {
	_, err := b.client.Ping(ctx).Result()
	if err != nil {
		return fmt.Errorf("failed to ping redis: %w", err)
	}
	return err
}

// Ensure RedisStreamBroker implements Broker interface
var _ taskiq.Broker = (*RedisStreamBroker)(nil)
