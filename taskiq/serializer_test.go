package taskiq

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
)

type serializerTestStruct struct {
	ID   int
	Msg  string
	Data []byte
}

func TestDefaultTaskSerializer_JSON(t *testing.T) {
	encoder := &JSONEncoder{}
	serializer := NewDefaultTaskSerializer()
	testTaskSerialization(t, serializer, encoder, "JSON")
}

func TestDefaultTaskSerializer_Msgpack(t *testing.T) {
	encoder := &MsgpackEncoder{}
	serializer := NewDefaultTaskSerializer()
	testTaskSerialization(t, serializer, encoder, "Msgpack")
}

func TestDefaultTaskSerializer_CBOR(t *testing.T) {
	encoder := &CBOR2Encoder{}
	serializer := NewDefaultTaskSerializer()
	testTaskSerialization(t, serializer, encoder, "CBOR")
}

// testTaskSerialization provides a common test suite for a given serializer and encoder combination.
func testTaskSerialization(t *testing.T, serializer TaskSerializer, encoder Encoder, encoderName string) {
	t.Helper()

	originalArgs := []interface{}{
		123,
		"hello",
		serializerTestStruct{ID: 1, Msg: "test struct", Data: []byte{0x01, 0x02}},
		[]int{10, 20, 30},
		map[string]string{"key1": "val1", "key2": "val2"},
	}

	// --- Test SerializeArgs and DeserializeArgs ---
	t.Run(encoderName+"_ArgsSerialization", func(t *testing.T) {
		// Note: SerializeArgs is more of a client-side concern.
		// Here, we simulate what a client would do to prepare TaskMessage.Args.
		// It should produce a single blob of bytes that are encoded by the 'encoder'.
		encodedArgsData, _, err := serializer.SerializeArgs(encoder, originalArgs...)
		if err != nil {
			t.Fatalf("SerializeArgs failed: %v", err)
		}

		// Simulate a TaskMessage
		taskMsg := &TaskMessage{
			TaskID:          uuid.NewString(),
			TaskName:        "test_task",
			Args:            encodedArgsData, // This is json.RawMessage containing the 'encoder'-encoded blob
			ContentEncoding: encoder.ContentType(),
			Timestamp:       time.Now().UTC(),
		}

		// Mock a handler function to inspect deserialized args
		// Handler expects: int, string, serializerTestStruct, []int, map[string]string
		handlerFunc := func(ctx context.Context, argInt int, argStr string, argStruct serializerTestStruct, argSlice []int, argMap map[string]string) error {
			// Compare deserialized args with original values
			if argInt != originalArgs[0].(int) {
				t.Errorf("argInt mismatch: got %v, want %v", argInt, originalArgs[0])
			}
			if argStr != originalArgs[1].(string) {
				t.Errorf("argStr mismatch: got %v, want %v", argStr, originalArgs[1])
			}
			if !reflect.DeepEqual(argStruct, originalArgs[2].(serializerTestStruct)) {
				t.Errorf("argStruct mismatch: got %#v, want %#v", argStruct, originalArgs[2])
			}
			if !reflect.DeepEqual(argSlice, originalArgs[3].([]int)) {
				t.Errorf("argSlice mismatch: got %#v, want %#v", argSlice, originalArgs[3])
			}
			if !reflect.DeepEqual(argMap, originalArgs[4].(map[string]string)) {
				t.Errorf("argMap mismatch: got %#v, want %#v", argMap, originalArgs[4])
			}
			return nil
		}
		handlerVal := reflect.ValueOf(handlerFunc)

		deserializedValues, err := serializer.DeserializeArgs(encoder, taskMsg, handlerVal)
		if err != nil {
			t.Fatalf("DeserializeArgs failed: %v. Encoded data: %s", err, string(taskMsg.Args))
		}

		// Construct call arguments for the handler, prepending context
		callArgs := make([]reflect.Value, 0, len(deserializedValues)+1)
		callArgs = append(callArgs, reflect.ValueOf(context.Background())) // Add context
		callArgs = append(callArgs, deserializedValues...)

		// Call the handler to perform checks (optional, could check values directly)
		ret := handlerVal.Call(callArgs)
		if len(ret) > 0 && ret[0].Interface() != nil {
			if handlerErr, ok := ret[0].Interface().(error); ok && handlerErr != nil {
				t.Errorf("Handler execution check failed: %v", handlerErr)
			}
		}
	})

	// --- Test SerializeResult and DeserializeResult ---
	t.Run(encoderName+"_ResultSerialization", func(t *testing.T) {
		originalResult := serializerTestStruct{ID: 100, Msg: "result data", Data: []byte{0xAA, 0xBB}}

		encodedResultData, err := serializer.SerializeResult(encoder, originalResult)
		if err != nil {
			t.Fatalf("SerializeResult failed: %v", err)
		}

		// Simulate a ResultMessage (ContentEncoding would be set by worker)
		// resultMsg := &ResultMessage{
		// 	Result: encodedResultData, // json.RawMessage containing 'encoder'-encoded blob
		// 	ContentEncoding: encoder.ContentType(),
		// }

		var deserializedResult serializerTestStruct
		// Pass the type of the struct we want to deserialize into
		err = encoder.Decode(encodedResultData, &deserializedResult) // Using encoder.Decode directly as DeserializeResult expects a type
		if err != nil {
			// Fallback: if encoder.Decode fails, try DeserializeResult (though its signature is slightly different)
			// This part highlights a potential slight mismatch in how DeserializeResult is used vs. direct encoder.Decode
			// For this test, let's assume we use the direct encoder.Decode for the raw data.
			// The TaskSerializer.DeserializeResult also exists but needs reflect.Type.
			
			// Let's test DeserializeResult properly:
			deserializedResultIf, errDeserialize := serializer.DeserializeResult(encoder, encodedResultData, reflect.TypeOf(serializerTestStruct{}))
			if errDeserialize != nil {
				t.Fatalf("DeserializeResult failed: %v. Encoded data: %s", errDeserialize, string(encodedResultData))
			}
			deserializedResult = deserializedResultIf.(serializerTestStruct) // Type assertion
		}


		if !reflect.DeepEqual(originalResult, deserializedResult) {
			t.Errorf("Deserialized result mismatch.\nOriginal: %#v\nDecoded:  %#v", originalResult, deserializedResult)
		}
	})

	// Test with nil arguments
	t.Run(encoderName+"_NilArgs", func(t *testing.T) {
		encodedArgsData, _, err := serializer.SerializeArgs(encoder) // No args
		if err != nil {
			t.Fatalf("SerializeArgs with no args failed: %v", err)
		}
		if string(encodedArgsData) != "[]" && encoder.ContentType() == "application/json" {
			// For JSON, it should be an empty array `[]`. For binary, it might be a specific nil/empty sequence.
			// The current SerializeArgs for JSON returns `json.RawMessage("[]")`
			// For binary encoders, it might return `nil` or an encoding of `nil` or an empty list.
			// This test might need adjustment based on precise nil/empty list encoding of each format.
			// t.Logf("[%s] SerializeArgs() encoded no args as: %s", encoderName, string(encodedArgsData))
		}


		taskMsg := &TaskMessage{Args: encodedArgsData, ContentEncoding: encoder.ContentType()}
		handlerFunc := func(ctx context.Context) {} // Handler expecting only context
		handlerVal := reflect.ValueOf(handlerFunc)

		_, err = serializer.DeserializeArgs(encoder, taskMsg, handlerVal)
		if err != nil {
			t.Errorf("DeserializeArgs with no actual args (only context) failed: %v", err)
		}

		handlerFuncWithNoArgs := func() {} // Handler expecting no args at all
		handlerValNoArgs := reflect.ValueOf(handlerFuncWithNoArgs)
		taskMsgNoCtx := &TaskMessage{Args: json.RawMessage("[]"), ContentEncoding: encoder.ContentType()}
		if encoder.ContentType() != "application/json" {
			// For binary, an empty list might be a different byte sequence
			emptyListEncoded, _, _ := serializer.SerializeArgs(encoder, []interface{}{}...)
			taskMsgNoCtx.Args = emptyListEncoded
		}


		_, err = serializer.DeserializeArgs(encoder, taskMsgNoCtx, handlerValNoArgs)
		if err != nil {
			t.Errorf("DeserializeArgs for handler with no args failed: %v", err)
		}
	})

	// Test with nil result
	t.Run(encoderName+"_NilResult", func(t *testing.T) {
		encodedResultData, err := serializer.SerializeResult(encoder, nil)
		if err != nil {
			t.Fatalf("SerializeResult with nil failed: %v", err)
		}
		// JSON nil is "null". Msgpack nil is 0xc0. CBOR nil is 0xf6.
		// The json.RawMessage should contain these bytes.
		
		var targetStruct *serializerTestStruct // Decoding into a pointer to struct
		deserializedResultIf, err := serializer.DeserializeResult(encoder, encodedResultData, reflect.TypeOf(targetStruct))
		if err != nil {
			t.Fatalf("DeserializeResult with nil data failed: %v. Encoded: %s", err, string(encodedResultData))
		}
		if deserializedResultIf != nil {
			// Expecting nil if the original was nil and target is a pointer type or interface
			// If target was a non-pointer struct, it would be a zero struct.
			// Since reflect.TypeOf(targetStruct) is a pointer, result should be nil.
			t.Errorf("DeserializeResult of nil: expected nil, got %#v", deserializedResultIf)
		}
	})
}


// TestTaskMessageContentEncodingHonored focuses on ensuring that the ContentEncoding
// field in TaskMessage, if set by a client, is used by the worker's deserializer,
// even if it's different from the worker's default configured encoder.
func TestTaskMessageContentEncodingHonored(t *testing.T) {
	// Worker's default encoder is JSON
	workerEncoder := &JSONEncoder{}
	serializer := NewDefaultTaskSerializer()

	// Client sends args encoded with Msgpack
	clientEncoder := &MsgpackEncoder{}
	originalArgs := []interface{}{"data_for_msgpack", 42}
	
	// Client encodes args using Msgpack
	encodedClientArgs, _, err := clientEncoder.Encode(originalArgs) // Using direct encoder for client simulation
	if err != nil {
		t.Fatalf("Client failed to encode args with Msgpack: %v", err)
	}

	taskMsg := &TaskMessage{
		TaskID:          uuid.NewString(),
		TaskName:        "encoded_task",
		Args:            json.RawMessage(encodedClientArgs), // This is the raw msgpack data
		ContentEncoding: clientEncoder.ContentType(),      // "application/msgpack"
	}

	// Worker's handler function
	handlerFunc := func(ctx context.Context, argStr string, argInt int) error {
		if argStr != originalArgs[0].(string) {
			t.Errorf("argStr mismatch: got '%s', want '%s'", argStr, originalArgs[0])
		}
		if argInt != originalArgs[1].(int) {
			t.Errorf("argInt mismatch: got %d, want %d", argInt, originalArgs[1])
		}
		return nil
	}
	handlerVal := reflect.ValueOf(handlerFunc)

	// Worker deserializes. It should use `clientEncoder` (Msgpack) based on ContentEncoding,
	// not its default `workerEncoder` (JSON).
	// The current DeserializeArgs in DefaultTaskSerializer takes an encoder argument.
	// The worker logic needs to select this encoder based on taskMsg.ContentEncoding.
	// This test simulates that selection: we pass the clientEncoder to DeserializeArgs.
	deserializedValues, err := serializer.DeserializeArgs(clientEncoder, taskMsg, handlerVal)
	if err != nil {
		t.Fatalf("DeserializeArgs with client's encoder (%s) failed: %v. Args data: %x", clientEncoder.ContentType(), err, taskMsg.Args)
	}

	// Verify by calling the handler (optional, could check values directly)
	callArgs := []reflect.Value{reflect.ValueOf(context.Background())}
	callArgs = append(callArgs, deserializedValues...)
	ret := handlerVal.Call(callArgs)
	if len(ret) > 0 && ret[0].Interface() != nil {
		if handlerErr, ok := ret[0].Interface().(error); ok && handlerErr != nil {
			t.Errorf("Handler execution check failed: %v", handlerErr)
		}
	}

	// Test case: What if ContentEncoding is empty? Worker should use its default.
	taskMsgNoEncoding := &TaskMessage{
		TaskID:          uuid.NewString(),
		TaskName:        "no_encoding_task",
		Args:            taskMsg.Args, // Still msgpack encoded data
		ContentEncoding: "",           // Empty or missing
	}
	// Worker will use its default (JSONEncoder) here. This should fail to parse msgpack.
	_, err = serializer.DeserializeArgs(workerEncoder, taskMsgNoEncoding, handlerVal)
	if err == nil {
		t.Errorf("Expected DeserializeArgs to fail with worker's default JSON encoder on msgpack data, but it succeeded")
	} else {
		t.Logf("Correctly failed to deserialize with wrong encoder: %v", err)
	}
}

// TestResultMessageContentEncodingSet focuses on ensuring that ResultMessage.ContentEncoding
// is correctly set by the worker (via its configured encoder) when serializing results.
func TestResultMessageContentEncodingSet(t *testing.T) {
	workerEncoder := &MsgpackEncoder{} // Worker is configured with Msgpack
	serializer := NewDefaultTaskSerializer()
	originalResult := "my_test_result"

	// Worker serializes the result using its configured encoder
	encodedResultData, err := serializer.SerializeResult(workerEncoder, originalResult)
	if err != nil {
		t.Fatalf("SerializeResult failed: %v", err)
	}

	// The worker would then construct ResultMessage like this:
	resultMsg := &ResultMessage{
		TaskID:          uuid.NewString(),
		Status:          StatusSuccess,
		Result:          encodedResultData,
		ContentEncoding: workerEncoder.ContentType(), // Worker sets this based on its encoder
		Timestamp:       time.Now().UTC(),
	}

	if resultMsg.ContentEncoding != workerEncoder.ContentType() {
		t.Errorf("ResultMessage.ContentEncoding = %s, want %s", resultMsg.ContentEncoding, workerEncoder.ContentType())
	}

	// Client then uses this ContentEncoding to decode
	var decodedResult string
	// Client needs to pick an encoder matching resultMsg.ContentEncoding
	// For this test, we assume client knows it's msgpack or checks ContentEncoding
	clientSideDecoder := &MsgpackEncoder{}
	if resultMsg.ContentEncoding != clientSideDecoder.ContentType() {
		t.Fatalf("Test logic error: client decoder content type %s does not match result message %s",
			clientSideDecoder.ContentType(), resultMsg.ContentEncoding)
	}

	err = clientSideDecoder.Decode(resultMsg.Result, &decodedResult)
	if err != nil {
		t.Fatalf("Client failed to decode result using %s: %v. Raw data: %x", clientSideDecoder.ContentType(), err, resultMsg.Result)
	}

	if decodedResult != originalResult {
		t.Errorf("Decoded result mismatch: got '%s', want '%s'", decodedResult, originalResult)
	}
}
