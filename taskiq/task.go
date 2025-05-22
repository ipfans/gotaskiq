package taskiq

import "time"

// TaskMessage represents a task to be executed.
// The TaskMessage struct itself is always JSON encoded when sent over a broker.
type TaskMessage struct {
	TaskID    string
	TaskName  string
	// Args contains the pre-encoded arguments for the task.
	// The encoding format is specified by the ContentEncoding field of the chosen Encoder.
	Args      json.RawMessage
	// Kwargs contains the pre-encoded keyword arguments for the task.
	// The encoding format is specified by the ContentEncoding field of the chosen Encoder.
	// Note: While map[string]json.RawMessage is used here, typical handlers might expect map[string]interface{}.
	// The deserialization logic in the worker will need to handle this.
	// For simplicity in the TaskMessage structure, we keep it as json.RawMessage.
	// The TaskSerializer will decode this further into actual types.
	Kwargs    map[string]json.RawMessage
	// ContentEncoding specifies the encoding used for Args and Kwargs.
	// e.g., "application/json", "application/msgpack", "application/cbor"
	// This field is set by the client when creating the task message.
	// The worker uses this to select the correct decoder.
	ContentEncoding string
	Timestamp time.Time
	Headers   map[string]string
	// Internal fields for broker use
	BrokerMessageID interface{} // To store message ID from broker for Ack
}
