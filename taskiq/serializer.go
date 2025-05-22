package taskiq

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
)

// TaskSerializer defines the interface for serializing and deserializing
// task arguments and results, using a specific Encoder for the payload.
type TaskSerializer interface {
	// SerializeArgs prepares task arguments for sending.
	// It takes the arguments and an encoder, and returns the encoded
	// args and kwargs as json.RawMessage.
	// For simplicity, this example will only handle positional arguments.
	// Kwargs handling would require a map[string]interface{} and similar encoding.
	SerializeArgs(encoder Encoder, args ...interface{}) (argsEncoded json.RawMessage, kwargsEncoded map[string]json.RawMessage, err error)

	// DeserializeArgs converts task arguments from TaskMessage into actual types for the handler.
	// It uses the provided encoder to decode TaskMessage.Args.
	DeserializeArgs(encoder Encoder, taskMsg *TaskMessage, handlerFunc reflect.Value) ([]reflect.Value, error)

	// SerializeResult prepares the task result for sending.
	// It takes the result and an encoder, and returns the encoded result as json.RawMessage.
	SerializeResult(encoder Encoder, result interface{}) (json.RawMessage, error)

	// DeserializeResult converts result data from ResultMessage into the expected type.
	// It uses the provided encoder to decode ResultMessage.Result.
	DeserializeResult(encoder Encoder, data json.RawMessage, resultType reflect.Type) (interface{}, error)
}

// DefaultTaskSerializer implements TaskSerializer.
// It uses the provided Encoder for args, kwargs, and results.
type DefaultTaskSerializer struct{}

func NewDefaultTaskSerializer() *DefaultTaskSerializer {
	return &DefaultTaskSerializer{}
}

// SerializeArgs encodes positional arguments using the provided encoder.
// Kwargs are not implemented in this basic serializer for brevity.
func (s *DefaultTaskSerializer) SerializeArgs(encoder Encoder, args ...interface{}) (json.RawMessage, map[string]json.RawMessage, error) {
	if len(args) == 0 {
		return json.RawMessage("[]"), nil, nil // Empty JSON array for no args
	}

	// For simplicity, we'll encode the entire args slice as a single blob
	// if the encoder is not JSON. If it's JSON, each arg is marshalled individually
	// to fit into a json.RawMessage array structure for the TaskMessage.
	// This is a simplification; a more robust solution might wrap multiple args.

	// This simplified version assumes args is a list of arguments to be passed to the task.
	// We will encode this list.
	encodedArgs, err := encoder.Encode(args)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to encode args with %s: %w", encoder.ContentType(), err)
	}
	// The TaskMessage.Args expects a json.RawMessage.
	// If the encoder produced JSON, it's fine. If it produced binary (msgpack, cbor),
	// it needs to be base64 encoded or similar to be valid inside a JSON string,
	// or the TaskMessage.Args field would need to be `[]byte` and handled carefully.
	// Given TaskMessage.Args is json.RawMessage, we assume it's a JSON-compatible structure.
	// For this implementation, we'll wrap the encoded blob in a JSON string if it's not already JSON.
	// A better approach for binary would be to have TaskMessage.Args be `[]byte` and handle base64 outside.
	// However, sticking to json.RawMessage means it must be valid JSON.
	// So, we'll marshal the raw encoded bytes as a JSON string.
	
	// Let's assume the client will send an array of already-encoded arguments.
	// For this SerializeArgs, we are creating that array.
	// This part is more for the client-side, which is not fully implemented here.
	// For now, let's return the encoded args directly, assuming the client will package it correctly.
	// This means `encodedArgs` should be what's directly put into `TaskMessage.Args`.
	// If `encoder` is JSONEncoder, `encodedArgs` is a JSON array of args.
	// If `encoder` is Msgpack/CBOR, `encodedArgs` is a binary blob.
	// This needs to be reconciled with TaskMessage.Args being json.RawMessage.
	// The simplest is to have SerializeArgs on the client side produce a JSON array of individually encoded args.
	// This serializer is for the worker side, so DeserializeArgs is more critical here.

	// This method is more conceptual for a client.
	// For now, let's assume it encodes the whole args list into a single blob.
	return json.RawMessage(encodedArgs), nil, nil
}

// DeserializeArgs decodes arguments from TaskMessage.Args using the provided encoder.
func (s *DefaultTaskSerializer) DeserializeArgs(encoder Encoder, taskMsg *TaskMessage, handlerFuncVal reflect.Value) ([]reflect.Value, error) {
	handlerType := handlerFuncVal.Type()
	expectedArgs := make([]reflect.Value, 0)

	if len(taskMsg.Args) == 0 || string(taskMsg.Args) == "null" { // No args passed
		// Still check if handler expects context
		if handlerType.NumIn() > 0 && handlerType.In(0).Implements(reflect.TypeOf((*context.Context)(nil)).Elem()) {
			// Context is filled by worker, not from taskMsg.Args
		} else if handlerType.NumIn() > 0 && !(handlerType.IsVariadic() && handlerType.NumIn() == 1) {
			 // Handler expects args, but none were provided (and not just a variadic context)
			return nil, fmt.Errorf("handler %s expects arguments, but none were provided in task message", runtimeFuncName(handlerFuncVal))
		}
		return expectedArgs, nil // Return empty slice if no args expected or only context
	}
	
	// taskMsg.Args is json.RawMessage. This should be an array of arguments for the task.
	// These arguments themselves are encoded by the 'encoder'.
	// So, first, unmarshal taskMsg.Args into a slice of []byte (if not JSON encoder) or []json.RawMessage (if JSON encoder)
	
	var decodedArgs []interface{}
	// The content of taskMsg.Args is a single blob produced by the encoder
	err := encoder.Decode(taskMsg.Args, &decodedArgs)
	if err != nil {
		return nil, fmt.Errorf("failed to decode args blob using %s: %w. Data: %s", encoder.ContentType(), err, string(taskMsg.Args))
	}

	numHandlerArgs := handlerType.NumIn()
	handlerArgIndex := 0
	
	// Skip context if present
	if numHandlerArgs > 0 && handlerType.In(0).Implements(reflect.TypeOf((*context.Context)(nil)).Elem()) {
		handlerArgIndex++
	}

	if len(decodedArgs) < (numHandlerArgs - handlerArgIndex) && !handlerType.IsVariadic() {
		return nil, fmt.Errorf("not enough arguments for handler %s: expected %d, got %d from message after decoding",
			runtimeFuncName(handlerFuncVal), numHandlerArgs-handlerArgIndex, len(decodedArgs))
	}


	for i := 0; i < len(decodedArgs); i++ {
		if handlerArgIndex >= numHandlerArgs {
			if handlerType.IsVariadic() { // All remaining decodedArgs go to the variadic part
				break 
			}
			// Too many args from message for non-variadic handler
			return nil, fmt.Errorf("too many arguments provided for non-variadic handler %s: expected %d, got %d",
			runtimeFuncName(handlerFuncVal), numHandlerArgs-handlerArgIndex, len(decodedArgs))
		}
		
		argType := handlerType.In(handlerArgIndex)
		
		// Create a new pointer to the argument type (e.g., *MyStruct)
		argVal := reflect.New(argType)

		// Need to re-serialize the decoded interface{} to JSON, then unmarshal to target type,
		// as direct conversion from interface{} to reflect.Value of specific type is tricky.
		// This is inefficient but simpler for handling arbitrary decoded types.
		// A better way would be type assertions or more complex reflection.
		tempBytes, tempErr := json.Marshal(decodedArgs[i])
		if tempErr != nil {
			return nil, fmt.Errorf("error re-marshaling decoded arg %d for handler %s: %w", i, runtimeFuncName(handlerFuncVal), tempErr)
		}

		if err := json.Unmarshal(tempBytes, argVal.Interface()); err != nil {
			return nil, fmt.Errorf("error unmarshaling re-serialized arg %d for handler %s: %w. Data: %s", i, runtimeFuncName(handlerFuncVal), err, string(tempBytes))
		}
		expectedArgs = append(expectedArgs, argVal.Elem())
		handlerArgIndex++
	}
	
    // Handle variadic arguments
    if handlerType.IsVariadic() && handlerArgIndex < numHandlerArgs {
        variadicArgType := handlerType.In(handlerArgIndex).Elem() // Type of individual variadic elements
        sliceType := reflect.SliceOf(variadicArgType)
        variadicSlice := reflect.MakeSlice(sliceType, 0, len(decodedArgs)-handlerArgIndex+1)

        for i := handlerArgIndex -1 ; i < len(decodedArgs); i++ {
            argVal := reflect.New(variadicArgType)
            tempBytes, tempErr := json.Marshal(decodedArgs[i])
            if tempErr != nil {
                return nil, fmt.Errorf("error re-marshaling decoded variadic arg for handler %s: %w", runtimeFuncName(handlerFuncVal), tempErr)
            }
            if err := json.Unmarshal(tempBytes, argVal.Interface()); err != nil {
                return nil, fmt.Errorf("error unmarshaling re-serialized variadic arg for handler %s: %w. Data: %s", runtimeFuncName(handlerFuncVal), err, string(tempBytes))
            }
            variadicSlice = reflect.Append(variadicSlice, argVal.Elem())
        }
        expectedArgs = append(expectedArgs, variadicSlice)
    } else if handlerArgIndex < numHandlerArgs && !handlerType.IsVariadic() {
		 // Not enough args for non-variadic part
		 return nil, fmt.Errorf("not enough arguments for handler %s: expected %d, processed %d before variadic check",
		 runtimeFuncName(handlerFuncVal), numHandlerArgs, handlerArgIndex)
	}


	// TODO: Handle Kwargs if the handler expects them
	return expectedArgs, nil
}

// SerializeResult encodes the result using the provided encoder.
func (s *DefaultTaskSerializer) SerializeResult(encoder Encoder, result interface{}) (json.RawMessage, error) {
	if result == nil {
		return json.RawMessage("null"), nil
	}
	encodedResult, err := encoder.Encode(result)
	if err != nil {
		return nil, fmt.Errorf("failed to encode result with %s: %w", encoder.ContentType(), err)
	}
	// As with Args, this must be valid JSON to fit in json.RawMessage.
	// If the result is binary, it should be wrapped (e.g., as a base64 JSON string).
	// For now, assume it's directly usable or the encoder produces JSON.
	return json.RawMessage(encodedResult), nil
}

// DeserializeResult decodes result data using the provided encoder.
func (s *DefaultTaskSerializer) DeserializeResult(encoder Encoder, data json.RawMessage, resultType reflect.Type) (interface{}, error) {
	if data == nil || string(data) == "null" {
		return nil, nil
	}
	
	// Create a new pointer to the result type.
	valPtr := reflect.New(resultType)
	
	// data is json.RawMessage, which is []byte.
	// We need to decode this using the provided encoder.
	if err := encoder.Decode(data, valPtr.Interface()); err != nil {
		return nil, fmt.Errorf("error decoding result with %s: %w. Data: %s", encoder.ContentType(), err, string(data))
	}
	return valPtr.Elem().Interface(), nil
}

// Helper to get function name for logging (optional)
// import "runtime" // Already imported at the top
func runtimeFuncName(f reflect.Value) string {
	return runtime.FuncForPC(f.Pointer()).Name()
}
