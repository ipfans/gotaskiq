package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"

	// Adjust the import path according to your actual project structure
	"encoding/json" // For client-side result decoding in the example
	"errors"        // For example error checking

	"taskiq"                       // Main taskiq package
	redisbroker "taskiq/brokers/redis" // Redis broker
	redisresults "taskiq/results/redis" // Redis result backend
)

// Example Task Function
// Note: Logging within tasks will now use zerolog if configured in main.
// Task handlers now accept context.Context as their first argument.
func add(ctx context.Context, a, b int) (int, error) {
	// taskiq.Logger can be used here if needed, or rely on worker's logging for task events.
	// Example: taskiq.Logger.Info().Ctx(ctx).Int("a", a).Int("b", b).Msg("Executing add task")
	result := a + b
	// Legacy log for comparison during transition. In a real app, remove this and use zerolog.
	log.Printf("Task 'add' (legacy log): %d + %d = %d", a, b, result)
	return result, nil
}

func greet(ctx context.Context, name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("name cannot be empty")
	}
	result := fmt.Sprintf("Hello, %s!", name)
	log.Printf("Task 'greet' (legacy log): %s => %s", name, result) // Legacy log
	// Simulate some work
	time.Sleep(1 * time.Second)
	return result, nil
}

const (
	redisAddr     = "localhost:6379"
	taskQueueName = "my_simple_tasks"
	taskGreetName = "greet_user"
	taskAddName   = "add_numbers"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 1. Initialize Broker
	redisClientBroker := redis.NewClient(&redis.Options{Addr: redisAddr})
	// Use the correct package name for NewRedisStreamBroker
	broker := redisbroker.NewRedisStreamBroker(redisClientBroker, nil) // Use default options

	// 2. Initialize Result Backend
	// Create a new Redis client for the result backend or use the same one if appropriate.
	redisClientResult := redis.NewClient(&redis.Options{Addr: redisAddr})
	resultBackendOpts := &redisresults.RedisResultBackendOptions{
		Client:    redisClientResult, // Pass the client
		ResultTTL: 1 * time.Hour,     // Results expire in 1 hour
	}
	resultBackend := redisresults.NewRedisResultBackend(resultBackendOpts)
	// defer resultBackend.Close() // This will close the client if it's managed internally by the backend.

	// 3. Create Worker
	// Logging: zerolog is used by default. Configure via TASKIQ_LOG_LEVEL (e.g., "debug", "warn").
	//          Logs will be JSON formatted and include timestamps, level, and message.
	// Tracing: OpenTelemetry is integrated. Traces are printed to stdout by default if enabled.
	//          This helps in debugging and understanding the flow of tasks and middleware.
	// Encoders: Configure a specific encoder for task arguments and results.
	//           This example uses MsgpackEncoder. JSONEncoder and CBOR2Encoder are also available.
	selectedEncoder := &taskiq.MsgpackEncoder{}
	log.Printf("Using %s for task argument and result encoding.", selectedEncoder.ContentType())

	workerOpts := &taskiq.WorkerOptions{
		Broker:         broker,
		ResultBackend:  resultBackend,
		QueueName:      taskQueueName,
		Concurrency:    3,                                 // Process 3 tasks concurrently
		TaskSerializer: taskiq.NewDefaultTaskSerializer(), // Use the new default serializer
		Encoder:        selectedEncoder,                   // Set the chosen encoder
	}
	worker := taskiq.NewWorker(workerOpts)

	// 4. Register Tasks
	if err := worker.RegisterTask(taskGreetName, greet); err != nil {
		log.Fatalf("Failed to register task '%s': %v", taskGreetName, err)
	}
	if err := worker.RegisterTask(taskAddName, add); err != nil {
		log.Fatalf("Failed to register task '%s': %v", taskAddName, err)
	}

	// 5. Register Lifecycle Hook (optional)
	worker.RegisterHook(taskiq.EventWorkerStartup, func(ctx context.Context) error {
		log.Println("Worker is starting up!")
		return nil
	})
	worker.RegisterHook(taskiq.EventWorkerShutdown, func(ctx context.Context) error {
		log.Println("Worker is shutting down!")
		// Clean up broker resources specifically for this worker if necessary
		// Note: worker.Stop() will handle broker.Close() if it's set up that way,
		// but if the broker instance is shared or needs specific cleanup for this worker's consumption,
		// it could be done here. For RedisStreamBroker, ConsumeMessages handles its own cleanup.
		// The main broker connection (redisClientBroker) is closed via defer after Stop.
		return nil
	})

	// 6. Start the Worker in a new goroutine
	log.Println("Starting worker...")
	go func() {
		if err := worker.Start(ctx); err != nil {
			log.Printf("Worker execution failed: %v", err)
			// If worker.Start exits, it might indicate a critical error.
			// We might want to signal the main goroutine to shut down.
			// For this example, we just log it.
			// In a real app, you might `cancel()` the main context here.
		}
		log.Println("Worker has stopped consuming.")
	}()

	// --- Simulate Client Publishing a Task ---
	// In a real application, this would be in a separate client process.
	log.Println("Simulating client publishing tasks...")
	time.Sleep(2 * time.Second) // Give worker a moment to start up

	// Client needs its own broker instance (or a shared one, carefully managed)
	// For simplicity, creating a new one for the client part.
	clientRedis := redis.NewClient(&redis.Options{Addr: redisAddr})
	clientBroker := brokers.NewRedisStreamBroker(clientRedis, nil)
	defer clientBroker.Close()
	defer clientRedis.Close()

	// Use the same encoder as the worker for preparing task arguments.
	// This client part simulates sending a task.
	clientEncoder := selectedEncoder // In a real app, client and worker might configure this independently.

	// Publish "greet" task
	greetTaskID := uuid.NewString()
	// Arguments for greet(ctx context.Context, name string)
	// Context is not sent in the message, it's added by the worker.
	greetArgsList := []interface{}{"TaskIQ Fan"} 
	
	// Encode arguments using the chosen encoder
	// The DefaultTaskSerializer.SerializeArgs is more conceptual for a client;
	// here we directly use the encoder for the list of arguments.
	encodedGreetArgsData, err := clientEncoder.Encode(greetArgsList)
	if err != nil {
		log.Fatalf("Failed to encode greet task args with %s: %v", clientEncoder.ContentType(), err)
	}

	greetMessage := &taskiq.TaskMessage{
		TaskID:          greetTaskID,
		TaskName:        taskGreetName,
		Args:            json.RawMessage(encodedGreetArgsData), // Args are now a single json.RawMessage containing the encoded blob
		ContentEncoding: clientEncoder.ContentType(),         // Specify the encoding used for Args
		Timestamp:       time.Now().UTC(),
		Headers:         map[string]string{"example_header": "example_value"},
	}
	if err := clientBroker.PublishMessage(ctx, taskQueueName, greetMessage); err != nil {
		log.Fatalf("Failed to publish greet task: %v", err)
	}
	log.Printf("Published task '%s' with ID: %s using %s encoding", taskGreetName, greetTaskID, clientEncoder.ContentType())

	// Publish "add" task
	addTaskID := uuid.NewString()
	// Arguments for add(ctx context.Context, a, b int)
	addArgsList := []interface{}{10, 25} 
	encodedAddArgsData, err := clientEncoder.Encode(addArgsList)
	if err != nil {
		log.Fatalf("Failed to encode add task args with %s: %v", clientEncoder.ContentType(), err)
	}
	addMessage := &taskiq.TaskMessage{
		TaskID:          addTaskID,
		TaskName:        taskAddName,
		Args:            json.RawMessage(encodedAddArgsData),
		ContentEncoding: clientEncoder.ContentType(),
		Timestamp:       time.Now().UTC(),
	}
	if err := clientBroker.PublishMessage(ctx, taskQueueName, addMessage); err != nil {
		log.Fatalf("Failed to publish add task: %v", err)
	}
	log.Printf("Published task '%s' with ID: %s using %s encoding", taskAddName, addTaskID, clientEncoder.ContentType())


	// Retrieve and print results
	// The result itself will also be encoded using the worker's configured encoder.
	retrieveResult(ctx, resultBackend, greetTaskID, "greet result", clientEncoder)
	retrieveResult(ctx, resultBackend, addTaskID, "add result", clientEncoder)
	
	// --- Wait for shutdown signal ---
	log.Println("Worker is running. Press Ctrl+C to stop.")
	select {
	case <-sigChan:
		log.Println("Received shutdown signal.")
	case <-ctx.Done(): // If context was cancelled for other reasons
		log.Println("Main context cancelled.")
	}

	log.Println("Initiating graceful shutdown of the worker...")
	// Create a new context for shutdown procedure if main ctx is already done, or use it.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := worker.Stop(); err != nil { // This signals worker.Start to exit
		log.Printf("Error during worker stop: %v", err)
	}
	
	// Wait for worker.Start to finish its cleanup (which it should if Stop was effective)
	// This is implicitly handled by worker.Start returning.
	// The defer cancel() for the main ctx also plays a role if Stop() didn't complete quickly.

	log.Println("Closing main broker and result backend connections...")
	// Close resources
	if err := broker.Close(); err != nil { // Close main broker used by worker
		log.Printf("Error closing worker broker: %v", err)
	}
	// Check if redisClientBroker is the same as the one used by the broker, if not, close it too.
	// In this example, redisClientBroker is passed to NewRedisStreamBroker, which doesn't close it internally.
	if err := redisClientBroker.Close(); err != nil {
		log.Printf("Error closing worker Redis client for broker: %v", err)
	}
	// Result backend client (redisClientResult) is closed by resultBackend.Close() if it was created by it,
	// or needs to be closed manually if it was passed in and not managed.
	// In this setup, NewRedisResultBackend takes options that include the client, so it should manage it.
	// However, the defer resultBackend.Close() handles it.
	if err := resultBackend.Close(); err != nil {
		log.Printf("Error closing result backend: %v", err)
	}


	log.Println("Example finished.")
}

func retrieveResult(ctx context.Context, rb taskiq.ResultBackend, taskID string, description string, clientEncoder taskiq.Encoder) {
	log.Printf("Attempting to retrieve %s for TaskID %s...", description, taskID)
	for i := 0; i < 10; i++ { // Retry a few times
		select {
		case <-ctx.Done():
			log.Printf("Context cancelled while retrieving %s.", description)
			return
		default:
		}

		res, err := rb.GetResult(taskID)
		if err != nil {
			// Check if the error is specifically ErrResultNotFound from the redis results package
			if errors.Is(err, redisresults.ErrResultNotFound) { // Updated to use errors.Is and correct package
				// log.Printf("%s not found yet for TaskID %s. Retrying...", description, taskID)
				time.Sleep(500 * time.Millisecond)
				continue
			}
			log.Printf("Error getting %s for TaskID %s: %v", description, taskID, err)
			return
		}

		if res.Status == taskiq.StatusSuccess {
			// Result is encoded using the worker's encoder. We need to decode it.
			// Ensure the ResultMessage.ContentEncoding matches our expected clientEncoder.
			if res.ContentEncoding != clientEncoder.ContentType() {
				log.Printf("WARNING: Result content encoding '%s' does not match client's expected '%s' for TaskID %s",
					res.ContentEncoding, clientEncoder.ContentType(), taskID)
				// Attempt to decode with clientEncoder anyway, or handle more gracefully.
			}

			if description == "greet result" {
				var greetResult string
				if err := clientEncoder.Decode(res.Result, &greetResult); err == nil {
					log.Printf("SUCCESS: %s for TaskID %s: %s (decoded with %s)", description, taskID, greetResult, clientEncoder.ContentType())
				} else {
					log.Printf("SUCCESS: %s for TaskID %s (raw): %s (decode error with %s: %v)", description, taskID, string(res.Result), clientEncoder.ContentType(), err)
				}
			} else if description == "add result" {
				var addResult int
				if err := clientEncoder.Decode(res.Result, &addResult); err == nil {
					log.Printf("SUCCESS: %s for TaskID %s: %d (decoded with %s)", description, taskID, addResult, clientEncoder.ContentType())
				} else {
					log.Printf("SUCCESS: %s for TaskID %s (raw): %s (decode error with %s: %v)", description, taskID, string(res.Result), clientEncoder.ContentType(), err)
				}
			} else {
				log.Printf("SUCCESS: %s for TaskID %s: %s (ContentEncoding: %s)", description, taskID, string(res.Result), res.ContentEncoding)
			}
		} else {
			log.Printf("FAILURE: %s for TaskID %s: %s (Error: %s, ContentEncoding: %s)", description, taskID, string(res.Result), res.Error, res.ContentEncoding)
		}
		return
	}
	log.Printf("Failed to retrieve %s for TaskID %s after multiple attempts.", description, taskID)
}
