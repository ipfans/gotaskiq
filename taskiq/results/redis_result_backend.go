package results

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/go-redis/redis/v8"
	"gitlab.com/taskiq/taskiq/pkg/taskiq"
)

var (
	// ErrResultNotFound can be returned by GetResult if a task result is not found.
	// Consumers can check for this error specifically.
	ErrResultNotFound = errors.New("task result not found")
)

// RedisResultBackend implements the taskiq.ResultBackend interface using Redis.
type RedisResultBackend struct {
	client *redis.Client
	opts   *RedisResultBackendOptions
}

// RedisResultBackendOptions holds configuration options for the RedisResultBackend.
type RedisResultBackendOptions struct {
	// Redis connection options.
	Addr     string
	Password string
	DB       int

	// ResultTTL is the time-to-live for results stored in Redis.
	// A zero value means results do not expire.
	ResultTTL time.Duration
}

// NewRedisResultBackend creates a new RedisResultBackend.
func NewRedisResultBackend(opts *RedisResultBackendOptions) taskiq.ResultBackend {
	if opts == nil {
		// Provide default options if nil, though Addr is usually required.
		opts = &RedisResultBackendOptions{
			Addr: "localhost:6379", // Default Redis address
			DB:   0,
		}
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     opts.Addr,
		Password: opts.Password,
		DB:       opts.DB,
	})

	return &RedisResultBackend{
		client: rdb,
		opts:   opts,
	}
}

// SetResult stores the result of a task in Redis.
// The ResultMessage is serialized to JSON before storing.
func (r *RedisResultBackend) SetResult(taskID string, result *taskiq.ResultMessage) error {
	if result == nil {
		return errors.New("result message cannot be nil")
	}
	if taskID == "" {
		return errors.New("taskID cannot be empty")
	}

	// Ensure timestamp is set if not already
	if result.Timestamp.IsZero() {
		result.Timestamp = time.Now().UTC()
	}

	serializedResult, err := json.Marshal(result)
	if err != nil {
		return &taskiq.SerializationError{Message: "failed to serialize result message to JSON", OriginalError: err}
	}

	ctx := context.Background() // Or use a context passed down if available/appropriate for library design
	err = r.client.Set(ctx, taskID, serializedResult, r.opts.ResultTTL).Err()
	if err != nil {
		return &taskiq.BackendError{Message: "failed to set result in Redis", OriginalError: err}
	}

	return nil
}

// GetResult retrieves the result of a task from Redis.
// It returns ErrResultNotFound if the taskID is not found in Redis.
func (r *RedisResultBackend) GetResult(taskID string) (*taskiq.ResultMessage, error) {
	if taskID == "" {
		return nil, errors.New("taskID cannot be empty")
	}

	ctx := context.Background() // Or use a context passed down
	serializedResult, err := r.client.Get(ctx, taskID).Bytes()

	if err != nil {
		if err == redis.Nil {
			return nil, ErrResultNotFound // Specific error for not found
		}
		return nil, &taskiq.BackendError{Message: "failed to get result from Redis", OriginalError: err}
	}

	var resultMessage taskiq.ResultMessage
	if err := json.Unmarshal(serializedResult, &resultMessage); err != nil {
		return nil, &taskiq.DeserializationError{Message: "failed to deserialize result message from JSON", OriginalError: err}
	}

	return &resultMessage, nil
}

// Close closes the connection to the Redis server.
func (r *RedisResultBackend) Close() error {
	if r.client != nil {
		err := r.client.Close()
		if err != nil {
			return &taskiq.BackendError{Message: "failed to close Redis client connection", OriginalError: err}
		}
	}
	return nil
}

// Ping checks the connection to the Redis server.
func (r *RedisResultBackend) Ping(ctx context.Context) error {
	if r.client == nil {
		return errors.New("redis client is not initialized")
	}
	_, err := r.client.Ping(ctx).Result()
	if err != nil {
		return &taskiq.BackendError{Message: "failed to ping Redis server", OriginalError: err}
	}
	return nil
}

// Ensure RedisResultBackend implements ResultBackend interface
var _ taskiq.ResultBackend = (*RedisResultBackend)(nil)

// Custom error types (could be defined in a common errors package for taskiq)
// For now, defining them here for completeness, assuming they might be in taskiq/errors.go or similar.
// If not, they should be moved to where taskiq.SerializationError etc. are defined.
// For the purpose of this task, I'll assume they are available from taskiq package.
// If taskiq.SerializationError, taskiq.BackendError, taskiq.DeserializationError
// are not defined in the main taskiq package, this will need adjustment.
// Let's assume they are defined like this for now:

/*
package taskiq

// SerializationError indicates an error during message serialization.
type SerializationError struct {
	Message       string
	OriginalError error
}

func (e *SerializationError) Error() string {
	if e.OriginalError != nil {
		return e.Message + ": " + e.OriginalError.Error()
	}
	return e.Message
}

// DeserializationError indicates an error during message deserialization.
type DeserializationError struct {
	Message       string
	OriginalError error
}

func (e *DeserializationError) Error() string {
	if e.OriginalError != nil {
		return e.Message + ": " + e.OriginalError.Error()
	}
	return e.Message
}

// BackendError indicates an error interacting with a backend system (broker, result backend).
type BackendError struct {
	Message       string
	OriginalError error
}

func (e *BackendError) Error() string {
	if e.OriginalError != nil {
		return e.Message + ": " + e.OriginalError.Error()
	}
	return e.Message
}
*/
