package bus

import (
	"context"
	"testing"
	"time"

	eventv1 "vinnipuh1982/producer/generated/proto/event/v1"
	"vinnipuh1982/producer/internal/kafka"
	mocks "vinnipuh1982/producer/internal/mocks"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type testSerializer struct{ payload []byte }

func (s *testSerializer) Serialize(_ *eventv1.Event) ([]byte, error)   { return s.payload, nil }
func (s *testSerializer) Deserialize(_ []byte) (*eventv1.Event, error) { return &eventv1.Event{}, nil }

func ensureInit(fp kafka.IProducerKafka) {
	ResetForTest()
	Init(fp, &testSerializer{payload: []byte("x")}, 2, 200*time.Millisecond, 64)
}

func TestBus_SendMessage_NilReply_Error(t *testing.T) {
	mp := &mocks.IProducerKafka{}
	ensureInit(mp)
	err := Get().SendMessage(context.Background(), kafka.TopicConfig{Topic: "t", Partition: -1}, Event{Type: "a", Data: map[string]string{"k": "v"}})
	assert.Error(t, err)
}

func TestBus_BatchBySize(t *testing.T) {
	mp := &mocks.IProducerKafka{}
	ensureInit(mp)
	sent := make(chan struct{}, 1)
	mp.On("ProduceMessagesAsync", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		msgs := args.Get(1).([]kafka.AsyncMessage)
		for _, m := range msgs {
			m.Callback(0, 1, nil)
		}
		sent <- struct{}{}
	}).Return()

	r1 := make(chan Reply, 1)
	r2 := make(chan Reply, 1)
	assert.NoError(t, Get().SendMessage(context.Background(), kafka.TopicConfig{Topic: "t", Partition: -1}, Event{Type: "a", Data: map[string]string{"1": "1"}, Reply: r1}))
	assert.NoError(t, Get().SendMessage(context.Background(), kafka.TopicConfig{Topic: "t", Partition: -1}, Event{Type: "b", Data: map[string]string{"2": "2"}, Reply: r2}))

	select {
	case <-sent:
	case <-time.After(1 * time.Second):
		t.Fatal("batch not sent")
	}
	select {
	case <-r1:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("no r1 reply")
	}
	select {
	case <-r2:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("no r2 reply")
	}
}

func TestBus_FlushByInterval(t *testing.T) {
	mp := &mocks.IProducerKafka{}
	ensureInit(mp)
	mp.On("ProduceMessagesAsync", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		msgs := args.Get(1).([]kafka.AsyncMessage)
		for _, m := range msgs {
			m.Callback(0, 1, nil)
		}
	}).Return()

	r := make(chan Reply, 1)
	assert.NoError(t, Get().SendMessage(context.Background(), kafka.TopicConfig{Topic: "t", Partition: -1}, Event{Type: "c", Data: map[string]string{"3": "3"}, Reply: r}))
	select {
	case <-r:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for interval flush")
	}
}
