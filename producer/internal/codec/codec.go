package codec

import (
	"fmt"

	eventv1 "vinnipuh1982/producer/generated/proto/event/v1"

	"google.golang.org/protobuf/proto"
)

// ISerializer defines the interface for event serialization.
type ISerializer interface {
	Serialize(e *eventv1.Event) ([]byte, error)
	Deserialize(b []byte) (*eventv1.Event, error)
}

// ProtobufSerializer implements ISerializer using protobuf.
type ProtobufSerializer struct{}

// New creates a new protobuf serializer.
func New() ISerializer {
	return &ProtobufSerializer{}
}

// Serialize serializes Event to protobuf bytes.
func (s *ProtobufSerializer) Serialize(e *eventv1.Event) ([]byte, error) {
	if e == nil {
		return nil, fmt.Errorf("nil event")
	}
	return proto.Marshal(e)
}

// Deserialize deserializes protobuf bytes to Event.
func (s *ProtobufSerializer) Deserialize(b []byte) (*eventv1.Event, error) {
	if len(b) == 0 {
		return nil, fmt.Errorf("empty payload")
	}
	var ev eventv1.Event
	if err := proto.Unmarshal(b, &ev); err != nil {
		return nil, err
	}
	return &ev, nil
}
