package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"

	"sync"  // For sync.WaitGroup if used for result checking
	"errors" // For errors.Is

	"taskiq" // Main taskiq package
	redisbroker "taskiq/brokers/redis" // Redis broker
	redisresults "taskiq/results/redis" // Redis result backend
)

const (
	redisAddr     = "localhost:6379"
	taskQueueName = "my_simple_tasks" // Must match the queue the worker is listening to
	taskGreetName = "greet_user"      // Must match registered task name in worker
	taskAddName   = "add_numbers"     // Must match registered task name in worker
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) // Overall timeout for client operations
	defer cancel()

	log.Println("Starting TaskIQ client example...")

	// 1. Initialize Broker
	redisClientBroker := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer redisClientBroker.Close()
	// Use correct package name for NewRedisStreamBroker
	broker := redisbroker.NewRedisStreamBroker(redisClientBroker, nil)
	defer broker.Close() // Ensure broker resources are cleaned up

	// Optional: Initialize Result Backend if client wants to check results
	redisClientResult := redis.NewClient(&redis.Options{Addr: redisAddr})
	// defer redisClientResult.Close() // This client will be closed by the result backend if provided
	resultBackendOpts := &redisresults.RedisResultBackendOptions{
		Client:    redisClientResult, // Pass the client
		ResultTTL: 1 * time.Hour,     // Matches worker's result TTL for consistency
	}
	resultBackend := redisresults.NewRedisResultBackend(resultBackendOpts)
	defer resultBackend.Close()

	// Choose an encoder. This must match the encoder configured on the worker.
	// The main example (`examples/redis_simple/main.go`) was updated to use MsgpackEncoder.
	clientEncoder := &taskiq.MsgpackEncoder{}
	log.Printf("Client using %s for task argument and result encoding.", clientEncoder.ContentType())

	// --- Publish 'greet' Tasks ---
	taskIDsToWatch := make(map[string]string) // To store taskID -> description

	for i := 0; i < 2; i++ { // Reduced number of tasks for brevity
		name := fmt.Sprintf("ClientUser-%d", i+1)
		taskID := uuid.NewString()
		taskIDsToWatch[taskID] = fmt.Sprintf("greet task for %s", name)

		// Arguments for greet(ctx context.Context, name string)
		greetArgsList := []interface{}{name} 
		
		// Encode arguments using the chosen encoder.
		// The TaskMessage.Args field expects a json.RawMessage which contains the *actual* encoded bytes.
		encodedGreetArgsData, err := clientEncoder.Encode(greetArgsList)
		if err != nil {
			log.Fatalf("Failed to encode greet task args for %s with %s: %v", name, clientEncoder.ContentType(), err)
		}

		greetMessage := &taskiq.TaskMessage{
			TaskID:          taskID,
			TaskName:        taskGreetName,
			Args:            json.RawMessage(encodedGreetArgsData),
			ContentEncoding: clientEncoder.ContentType(),
			Timestamp:       time.Now().UTC(),
			Headers:         map[string]string{"published_by": "redis_simple_client_updated"},
		}

		log.Printf("Publishing task '%s' for '%s' with ID: %s (Encoding: %s)", taskGreetName, name, taskID, clientEncoder.ContentType())
		if err := broker.PublishMessage(ctx, taskQueueName, greetMessage); err != nil {
			log.Printf("Failed to publish greet task for %s: %v. Continuing...", name, err)
			delete(taskIDsToWatch, taskID) // Remove from watch list if publish failed
		}
	}

	// --- Publish 'add' Tasks ---
	addOperations := [][]int{{50, 60}, {70, -10}} // Reduced number of tasks
	for _, op := range addOperations {
		a, b := op[0], op[1]
		taskID := uuid.NewString()
		taskIDsToWatch[taskID] = fmt.Sprintf("add task for %d + %d", a, b)

		addArgsList := []interface{}{a, b}
		encodedAddArgsData, err := clientEncoder.Encode(addArgsList)
		if err != nil {
			log.Fatalf("Failed to encode add task args for %d+%d with %s: %v", a, b, clientEncoder.ContentType(), err)
		}
		addMessage := &taskiq.TaskMessage{
			TaskID:          taskID,
			TaskName:        taskAddName,
			Args:            json.RawMessage(encodedAddArgsData),
			ContentEncoding: clientEncoder.ContentType(),
			Timestamp:       time.Now().UTC(),
			Headers:         map[string]string{"published_by": "redis_simple_client_updated"},
		}

		log.Printf("Publishing task '%s' for %d+%d with ID: %s (Encoding: %s)", taskAddName, a, b, taskID, clientEncoder.ContentType())
		if err := broker.PublishMessage(ctx, taskQueueName, addMessage); err != nil {
			log.Printf("Failed to publish add task for %d+%d: %v. Continuing...", a,b, err)
			delete(taskIDsToWatch, taskID)
		}
	}

	log.Printf("All tasks published. Total tasks to watch for results: %d", len(taskIDsToWatch))

	// --- Optionally, check for results ---
	if len(taskIDsToWatch) > 0 {
		log.Println("Checking for task results (will wait up to 15 seconds)...")
		resultsCtx, resultsCancel := context.WithTimeout(ctx, 15*time.Second)
		defer resultsCancel()

		var wg sync.WaitGroup 
		for taskID, description := range taskIDsToWatch {
			wg.Add(1)
			// Launching as goroutine to (naively) check all "concurrently"
			go func(tid, desc string) {
				defer wg.Done()
				retrieveResult(resultsCtx, resultBackend, tid, desc, clientEncoder)
			}(taskID, description)
		}
		wg.Wait() // Wait for all launched goroutines to complete their polling attempt.
	}
	
	log.Println("Client example finished.")
}

// retrieveResult is similar to the one in the worker example, adapted for client use
func retrieveResult(ctx context.Context, rb taskiq.ResultBackend, taskID string, description string, currentEncoder taskiq.Encoder) {
	log.Printf("Attempting to retrieve result for: %s (TaskID %s)", description, taskID)
	for {
		select {
		case <-ctx.Done():
			log.Printf("Context cancelled while retrieving result for: %s (TaskID %s). Error: %v", description, taskID, ctx.Err())
			return
		default:
		}

		res, err := rb.GetResult(taskID)
		if err != nil {
			if errors.Is(err, redisresults.ErrResultNotFound) { // Use errors.Is for checking specific error types
				// log.Printf("Result not found yet for: %s (TaskID %s). Retrying in 500ms...", description, taskID)
				time.Sleep(500 * time.Millisecond)
				continue // Retry
			}
			log.Printf("Error getting result for: %s (TaskID %s): %v", description, taskID, err)
			return // Stop on other errors
		}

		if res.Status == taskiq.StatusSuccess {
			var resultValue interface{}
			// Result is encoded with worker's encoder, which should match clientEncoder here.
			if res.ContentEncoding != currentEncoder.ContentType() {
				log.Printf("WARNING: Result ContentEncoding (%s) does not match client's encoder (%s) for TaskID %s",
					res.ContentEncoding, currentEncoder.ContentType(), taskID)
				// Fallback or error
				resultValue = fmt.Sprintf("Raw Encoded Result (mismatched encoding %s): %s", res.ContentEncoding, string(res.Result))
			} else {
				// Attempt to unmarshal based on description hints or keep raw
				if contains(description, "greet") {
					var s string
					if err := currentEncoder.Decode(res.Result, &s); err == nil {
						resultValue = s
					} else {
						resultValue = fmt.Sprintf("Failed to decode greet result with %s: %v (Raw: %s)", currentEncoder.ContentType(), err, string(res.Result))
					}
				} else if contains(description, "add") {
					var i int
					if err := currentEncoder.Decode(res.Result, &i); err == nil {
						resultValue = i
					} else {
						resultValue = fmt.Sprintf("Failed to decode add result with %s: %v (Raw: %s)", currentEncoder.ContentType(), err, string(res.Result))
					}
				} else {
					resultValue = string(res.Result) // default to raw string if no hint
				}
			}
			log.Printf("SUCCESS: Result for %s (TaskID %s): %v (Encoder: %s)", description, taskID, resultValue, res.ContentEncoding)
		} else {
			log.Printf("FAILURE: Result for %s (TaskID %s): Status=%s, Error=%s, RawResult=%s, Encoding=%s",
				description, taskID, res.Status, res.Error, string(res.Result), res.ContentEncoding)
		}
		return // Result found (success or failure), stop polling
	}
}

// Simple helper function as strings.Contains is not available.
func contains(s, substr string) bool {
    for i := 0; i <= len(s)-len(substr); i++ {
        if s[i:i+len(substr)] == substr {
            return true
        }
    }
    return false
}
