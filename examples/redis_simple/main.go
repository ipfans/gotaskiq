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
	"gitlab.com/taskiq/taskiq/pkg/taskiq"
	"gitlab.com/taskiq/taskiq/pkg/taskiq/brokers"
	"gitlab.com/taskiq/taskiq/pkg/taskiq/results"
)

// Example Task Function
func add(a, b int) (int, error) {
	result := a + b
	log.Printf("Task 'add': %d + %d = %d", a, b, result)
	return result, nil
}

func greet(name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("name cannot be empty")
	}
	result := fmt.Sprintf("Hello, %s!", name)
	log.Printf("Task 'greet': %s => %s", name, result)
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
	broker := brokers.NewRedisStreamBroker(redisClientBroker, nil) // Use default options

	// 2. Initialize Result Backend
	redisClientResult := redis.NewClient(&redis.Options{Addr: redisAddr})
	resultBackendOpts := &results.RedisResultBackendOptions{
		Addr:      redisAddr,
		ResultTTL: 1 * time.Hour, // Results expire in 1 hour
	}
	resultBackend := results.NewRedisResultBackend(resultBackendOpts)
	defer resultBackend.Close()

	// 3. Create Worker
	workerOpts := &taskiq.WorkerOptions{
		Broker:        broker,
		ResultBackend: resultBackend,
		QueueName:     taskQueueName,
		Concurrency:   3, // Process 3 tasks concurrently
		// Logger:        log.New(os.Stdout, "example-worker: ", log.LstdFlags), // Custom logger
		TaskSerializer: taskiq.NewJSONTaskSerializer(), // Use the one from taskiq package
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

	// Use the same serializer instance or a compatible one
	serializer := taskiq.NewJSONTaskSerializer()

	// Publish "greet" task
	greetTaskID := uuid.NewString()
	greetArgs := []interface{}{"TaskIQ Fan"} // Arguments for greet(name string)
	
	// Serialize arguments using a helper or directly if simple
	var serializedGreetArgs [][]byte
	for _, arg := range greetArgs {
		sArg, err := serializer.SerializeResult(arg) // Using SerializeResult as a generic JSON serializer here
		if err != nil {
			log.Fatalf("Failed to serialize greet task arg: %v", err)
		}
		serializedGreetArgs = append(serializedGreetArgs, sArg)
	}

	greetMessage := &taskiq.TaskMessage{
		TaskID:   greetTaskID,
		TaskName: taskGreetName,
		Args:     serializedGreetArgs,
		// Kwargs, Headers, etc. can be set if needed
	}
	if err := clientBroker.PublishMessage(ctx, taskQueueName, greetMessage); err != nil {
		log.Fatalf("Failed to publish greet task: %v", err)
	}
	log.Printf("Published task '%s' with ID: %s", taskGreetName, greetTaskID)

	// Publish "add" task
	addTaskID := uuid.NewString()
	addArgs := []interface{}{10, 25} // Arguments for add(a, b int)
	var serializedAddArgs [][]byte
	for _, arg := range addArgs {
		sArg, err := serializer.SerializeResult(arg)
		if err != nil {
			log.Fatalf("Failed to serialize add task arg: %v", err)
		}
		serializedAddArgs = append(serializedAddArgs, sArg)
	}
	addMessage := &taskiq.TaskMessage{
		TaskID:   addTaskID,
		TaskName: taskAddName,
		Args:     serializedAddArgs,
	}
	if err := clientBroker.PublishMessage(ctx, taskQueueName, addMessage); err != nil {
		log.Fatalf("Failed to publish add task: %v", err)
	}
	log.Printf("Published task '%s' with ID: %s", taskAddName, addTaskID)


	// Retrieve and print results
	retrieveResult(ctx, resultBackend, greetTaskID, "greet result")
	retrieveResult(ctx, resultBackend, addTaskID, "add result")
	
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
	if err := redisClientBroker.Close(); err != nil {
		log.Printf("Error closing worker Redis client for broker: %v", err)
	}
	// resultBackend is closed by defer
	// clientBroker and clientRedis are closed by defer

	log.Println("Example finished.")
}

func retrieveResult(ctx context.Context, rb taskiq.ResultBackend, taskID string, description string) {
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
			if err == results.ErrResultNotFound {
				// log.Printf("%s not found yet for TaskID %s. Retrying...", description, taskID)
				time.Sleep(500 * time.Millisecond)
				continue
			}
			log.Printf("Error getting %s for TaskID %s: %v", description, taskID, err)
			return
		}

		if res.Status == taskiq.StatusSuccess {
			// Assuming result is string for greet, int for add.
			// This part needs to know the expected type or use a generic way to display.
			// For simplicity, we'll try to print raw result or a specific interpretation.
			if description == "greet result" {
				var greetResult string
				// Using TaskSerializer's DeserializeResult (if it were more complete)
				// For now, simple json.Unmarshal
				if err := json.Unmarshal(res.Result, &greetResult); err == nil {
					log.Printf("SUCCESS: %s for TaskID %s: %s", description, taskID, greetResult)
				} else {
					log.Printf("SUCCESS: %s for TaskID %s (raw): %s (unmarshal error: %v)", description, taskID, string(res.Result), err)
				}
			} else if description == "add result" {
				var addResult int
				if err := json.Unmarshal(res.Result, &addResult); err == nil {
					log.Printf("SUCCESS: %s for TaskID %s: %d", description, taskID, addResult)
				} else {
					log.Printf("SUCCESS: %s for TaskID %s (raw): %s (unmarshal error: %v)", description, taskID, string(res.Result), err)
				}
			} else {
				log.Printf("SUCCESS: %s for TaskID %s: %s", description, taskID, string(res.Result))
			}
		} else {
			log.Printf("FAILURE: %s for TaskID %s: %s (Error: %s)", description, taskID, string(res.Result), res.Error)
		}
		return
	}
	log.Printf("Failed to retrieve %s for TaskID %s after multiple attempts.", description, taskID)
}
