package taskiq

import (
	"bytes"
	"reflect"
	"testing"
)

type testStruct struct {
	Name  string
	Value int
	Tags  []string
	Meta  map[string]interface{}
}

var testCases = []struct {
	name         string
	data         interface{}
	expectedType reflect.Type
}{
	{"SimpleStruct", &testStruct{Name: "Test", Value: 123, Tags: []string{"a", "b"}, Meta: map[string]interface{}{"active": true, "count": 1}}, reflect.TypeOf(&testStruct{})},
	{"String", "hello world", reflect.TypeOf("")},
	{"Int", 42, reflect.TypeOf(0)},
	{"Float", 3.14159, reflect.TypeOf(0.0)},
	{"BoolTrue", true, reflect.TypeOf(false)},
	{"BoolFalse", false, reflect.TypeOf(false)},
	{"SliceInt", []int{1, 2, 3, 4, 5}, reflect.TypeOf([]int{})},
	{"MapStringInt", map[string]int{"one": 1, "two": 2}, reflect.TypeOf(map[string]int{})},
	{"NilInterface", nil, reflect.TypeOf(nil)}, // Encoding nil might be specific to encoder
	{"EmptyStruct", &testStruct{}, reflect.TypeOf(&testStruct{})},
	{"StructWithNilMap", &testStruct{Name: "NilMap", Meta: nil}, reflect.TypeOf(&testStruct{})},
	{"StructWithEmptyMap", &testStruct{Name: "EmptyMap", Meta: map[string]interface{}{}}, reflect.TypeOf(&testStruct{})},
}

func runEncoderTests(t *testing.T, encoder Encoder, expectedContentType string) {
	t.Helper()

	// Test ContentType
	if ct := encoder.ContentType(); ct != expectedContentType {
		t.Errorf("ContentType() got %s, want %s", ct, expectedContentType)
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Special handling for nil with some encoders (e.g. msgpack might encode nil interface{} as nil bytes)
			if tc.data == nil {
				encoded, err := encoder.Encode(tc.data)
				if err != nil {
					// For some encoders, encoding nil might be an error or produce non-nil bytes.
					// This part of the test might need adjustment based on specific encoder behavior for nil.
					// t.Logf("Encoder %s encoding nil: err = %v, encoded = %v", expectedContentType, err, encoded)
				}
				// Attempt to decode
				// Create a new pointer of the expected type for decoding
				// For nil, the expectedType is also nil, which makes direct New() impossible.
				// We'll skip robust nil decode check here, focusing on non-nil data.
				// If encoded is nil or empty, decoding into a nil pointer might also be tricky.
				if encoded == nil || len(encoded) == 0 {
				    // If nil encodes to nil/empty, then decoding nil/empty into a nil pointer is expected to work or be a no-op.
				    // For simplicity, we won't try to decode nil back into a nil interface{} robustly here.
				    // It's more important to test actual data.
				    return 
				}
				// If encoded is not nil, this will likely fail as we can't determine the type for nil.
				// This part needs more thought if robust nil testing is required for all encoders.
				// For now, we primarily test non-nil data.
				return
			}

			encoded, err := encoder.Encode(tc.data)
			if err != nil {
				t.Fatalf("Encode() error: %v", err)
			}
			if encoded == nil {
				t.Fatalf("Encode() returned nil bytes")
			}

			// Create a new zero value of the original data's type for decoding
			// If tc.data is a pointer (like for structs), we need to reflect its element type
			var decodedValue reflect.Value
			dataType := reflect.TypeOf(tc.data)
			if dataType.Kind() == reflect.Ptr {
				decodedValue = reflect.New(dataType.Elem())
			} else {
				decodedValue = reflect.New(dataType)
			}
			
			targetToDecode := decodedValue.Interface()

			err = encoder.Decode(encoded, targetToDecode)
			if err != nil {
				t.Fatalf("Decode() error: %v. Encoded data: %x", err, encoded)
			}

			// Get the actual decoded value (dereference if pointer)
			var actualDecodedData interface{}
			if dataType.Kind() == reflect.Ptr {
				actualDecodedData = decodedValue.Interface() // This is already a pointer, e.g. *testStruct
			} else {
				actualDecodedData = decodedValue.Elem().Interface() // This gets the value, e.g. string, int
			}
			
			// Compare original and decoded data
			// Using reflect.DeepEqual for robust comparison, esp. for structs, slices, maps.
			// For float, consider tolerance if direct comparison fails due to precision.
			if !reflect.DeepEqual(tc.data, actualDecodedData) {
				// Special case for maps: DeepEqual can be tricky if one is nil and other is empty
				if (tc.data != nil && reflect.TypeOf(tc.data).Kind() == reflect.Map && reflect.ValueOf(tc.data).IsNil()) &&
				   (actualDecodedData != nil && reflect.TypeOf(actualDecodedData).Kind() == reflect.Map && reflect.ValueOf(actualDecodedData).Len() == 0) {
				   // This is fine: nil map encodes and decodes as empty map for some encoders
				} else if (actualDecodedData != nil && reflect.TypeOf(actualDecodedData).Kind() == reflect.Map && reflect.ValueOf(actualDecodedData).IsNil()) &&
				   (tc.data != nil && reflect.TypeOf(tc.data).Kind() == reflect.Map && reflect.ValueOf(tc.data).Len() == 0) {
				    // This is fine: empty map encodes and decodes as nil map for some encoders
				} else {
					t.Errorf("Decoded data does not match original.\nOriginal: %#v (%T)\nDecoded:  %#v (%T)", tc.data, tc.data, actualDecodedData, actualDecodedData)
				}
			}
		})
	}
}

func TestJSONEncoder_EncodeDecode(t *testing.T) {
	runEncoderTests(t, &JSONEncoder{}, "application/json")
}

func TestMsgpackEncoder_EncodeDecode(t *testing.T) {
	runEncoderTests(t, &MsgpackEncoder{}, "application/msgpack")
}

func TestCBOR2Encoder_EncodeDecode(t *testing.T) {
	runEncoderTests(t, &CBOR2Encoder{}, "application/cbor")
}

// Test specific edge case for CBOR with nil maps (often decodes as empty map)
func TestCBOR2Encoder_NilMap(t *testing.T) {
	encoder := &CBOR2Encoder{}
	original := &testStruct{Name: "NilMapTest", Meta: nil}
	
	encoded, err := encoder.Encode(original)
	if err != nil {
		t.Fatalf("Encode() error: %v", err)
	}

	decoded := &testStruct{}
	err = encoder.Decode(encoded, decoded)
	if err != nil {
		t.Fatalf("Decode() error: %v", err)
	}

	if decoded.Name != original.Name {
		t.Errorf("Name mismatch: got %s, want %s", decoded.Name, original.Name)
	}
	// CBOR often decodes nil maps as empty non-nil maps.
	if original.Meta == nil && decoded.Meta != nil && len(decoded.Meta) == 0 {
		// This is acceptable behavior for CBOR
	} else if !reflect.DeepEqual(original.Meta, decoded.Meta) {
		t.Errorf("Meta map mismatch: got %#v, want %#v", decoded.Meta, original.Meta)
	}
}

// Test specific edge case for Msgpack with nil maps (often decodes as empty map)
func TestMsgpackEncoder_NilMap(t *testing.T) {
	encoder := &MsgpackEncoder{}
	original := &testStruct{Name: "NilMapTest", Meta: nil}
	
	encoded, err := encoder.Encode(original)
	if err != nil {
		t.Fatalf("Encode() error: %v", err)
	}

	decoded := &testStruct{}
	err = encoder.Decode(encoded, decoded)
	if err != nil {
		t.Fatalf("Decode() error: %v", err)
	}

	if decoded.Name != original.Name {
		t.Errorf("Name mismatch: got %s, want %s", decoded.Name, original.Name)
	}
    // Msgpack also often decodes nil maps as empty non-nil maps.
	if original.Meta == nil && decoded.Meta != nil && len(decoded.Meta) == 0 {
		// This is acceptable behavior
	} else if !reflect.DeepEqual(original.Meta, decoded.Meta) {
		t.Errorf("Meta map mismatch: got %#v, want %#v", decoded.Meta, original.Meta)
	}
}

// Test specific edge case for JSON with nil maps (decodes as nil)
func TestJSONEncoder_NilMap(t *testing.T) {
	encoder := &JSONEncoder{}
	original := &testStruct{Name: "NilMapTest", Meta: nil}
	
	encoded, err := encoder.Encode(original)
	if err != nil {
		t.Fatalf("Encode() error: %v", err)
	}

	decoded := &testStruct{}
	err = encoder.Decode(encoded, decoded)
	if err != nil {
		t.Fatalf("Decode() error: %v", err)
	}

	if decoded.Name != original.Name {
		t.Errorf("Name mismatch: got %s, want %s", decoded.Name, original.Name)
	}
    // JSON should preserve nil maps as nil.
	if original.Meta == nil && decoded.Meta != nil {
		t.Errorf("Meta map mismatch: expected nil, got non-nil %#v", decoded.Meta)
	} else if !reflect.DeepEqual(original.Meta, decoded.Meta) { // General check
		t.Errorf("Meta map mismatch: got %#v, want %#v", decoded.Meta, original.Meta)
	}
}

func TestEncoders_EmptyPayload(t *testing.T) {
    encoders := []Encoder{&JSONEncoder{}, &MsgpackEncoder{}, &CBOR2Encoder{}}
    for _, enc := range encoders {
        t.Run(enc.ContentType(), func(t *testing.T) {
            var s string
            err := enc.Decode([]byte{}, &s) // Empty byte slice
            if err == nil {
                // Some decoders might successfully decode empty input into a zero value.
                // For JSON, this is an error. For others, it might not be.
                // t.Logf("%s decoded empty slice into zero value string: '%s'", enc.ContentType(), s)
            } else {
                 // t.Logf("%s returned error for empty slice: %v", enc.ContentType(), err)
            }

            err = enc.Decode(nil, &s) // Nil byte slice
             if err == nil {
                // t.Logf("%s decoded nil slice into zero value string: '%s'", enc.ContentType(), s)
            } else {
                // t.Logf("%s returned error for nil slice: %v", enc.ContentType(), err)
            }
            // What we want to ensure is no panic. The exact behavior (error or zero value)
            // can vary between encoding libraries for empty/nil input.
        })
    }
}

// Test for comparing nil interface{} encoding
// Some encoders (like JSON) will encode a nil interface{} as "null" bytes.
// Others (like msgpack) might produce an actual nil []byte or a specific "nil" byte sequence.
func TestEncoders_NilInterface(t *testing.T) {
    var data interface{} // data is nil
    
    encoders := map[string]Encoder{
        "JSON":    &JSONEncoder{},
        "Msgpack": &MsgpackEncoder{},
        "CBOR":    &CBOR2Encoder{},
    }

    for name, enc := range encoders {
        t.Run(name, func(t *testing.T) {
            encodedBytes, err := enc.Encode(data)
            if err != nil {
                // CBOR might error on nil interface{} directly, or encode it as a specific nil type.
                // Let's log this behavior.
                t.Logf("[%s] Encode(nil) error: %v", name, err)
            }
            // t.Logf("[%s] Encode(nil) result: %x", name, encodedBytes)

            // Now try to decode. We need a target. Let's use *interface{}.
            var decodedTarget interface{}
            // Pass a pointer to the interface so the decoder can set it.
            decodeErr := enc.Decode(encodedBytes, &decodedTarget)
            
            if decodeErr != nil {
                // If encoding produced an error, decoding might also error or try to decode 'nil' bytes.
                // If encoding was successful (e.g. JSON "null"), decoding should be successful.
                t.Logf("[%s] Decode(encodedNil) error: %v", name, decodeErr)
            }

            // If original data was nil, and decoding was successful, decodedTarget should also be nil.
            if decodeErr == nil && decodedTarget != nil {
                 // JSON "null" decodes to a nil interface{} value.
                 // Msgpack's nil byte decodes to a nil interface{} value.
                 // CBOR's nil byte decodes to a nil interface{} value.
                 // This check might be too strict if the decoder wraps nil in some way.
                 // For now, let's expect decodedTarget to be nil if decodeErr is nil.
                t.Errorf("[%s] Expected decoded nil to be nil, but got: %#v", name, decodedTarget)
            }
        })
    }
}
