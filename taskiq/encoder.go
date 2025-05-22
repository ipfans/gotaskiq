package taskiq

import (
	"encoding/json"

	"github.com/fxamacker/cbor/v2"
	"github.com/vmihailenco/msgpack/v5"
)

// Encoder defines the interface for encoding and decoding task arguments and results.
type Encoder interface {
	// Encode takes an interface and returns its byte representation.
	Encode(v interface{}) ([]byte, error)
	// Decode takes a byte slice and unmarshals it into the provided interface pointer.
	Decode(data []byte, v interface{}) error
	// ContentType returns the content type string for this encoder, e.g., "application/json".
	ContentType() string
}

// JSONEncoder implements the Encoder interface using encoding/json.
type JSONEncoder struct{}

// Encode marshals the given interface into a JSON byte slice.
func (e *JSONEncoder) Encode(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// Decode unmarshals the given JSON byte slice into the provided interface pointer.
func (e *JSONEncoder) Decode(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

// ContentType returns "application/json".
func (e *JSONEncoder) ContentType() string {
	return "application/json"
}

// MsgpackEncoder implements the Encoder interface using github.com/vmihailenco/msgpack/v5.
type MsgpackEncoder struct{}

// Encode marshals the given interface into a MessagePack byte slice.
func (e *MsgpackEncoder) Encode(v interface{}) ([]byte, error) {
	return msgpack.Marshal(v)
}

// Decode unmarshals the given MessagePack byte slice into the provided interface pointer.
func (e *MsgpackEncoder) Decode(data []byte, v interface{}) error {
	return msgpack.Unmarshal(data, v)
}

// ContentType returns "application/msgpack".
func (e *MsgpackEncoder) ContentType() string {
	return "application/msgpack"
}

// CBOR2Encoder implements the Encoder interface using github.com/fxamacker/cbor/v2.
type CBOR2Encoder struct{}

// Encode marshals the given interface into a CBOR byte slice.
func (e *CBOR2Encoder) Encode(v interface{}) ([]byte, error) {
	return cbor.Marshal(v)
}

// Decode unmarshals the given CBOR byte slice into the provided interface pointer.
func (e *CBOR2Encoder) Decode(data []byte, v interface{}) error {
	return cbor.Unmarshal(data, v)
}

// ContentType returns "application/cbor".
func (e *CBOR2Encoder) ContentType() string {
	return "application/cbor"
}
