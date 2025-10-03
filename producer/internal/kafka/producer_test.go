package kafka

import (
	"testing"
)

type testAsyncMsg struct {
	key    string
	val    []byte
	gotErr error
}

func (m *testAsyncMsg) GetKey() string                                    { return m.key }
func (m *testAsyncMsg) GetValue() []byte                                  { return m.val }
func (m *testAsyncMsg) Callback(partition int32, offset int64, err error) { m.gotErr = err }

func TestProduceMessagesAsync_ProducerNotEnabled(t *testing.T) {
	p := &producer{}
	m1 := &testAsyncMsg{key: "k1", val: []byte("v1")}
	m2 := &testAsyncMsg{key: "k2", val: []byte("v2")}

	p.ProduceMessagesAsync(TopicConfig{Topic: "t", Partition: -1}, []AsyncMessage{m1, m2})

	if m1.gotErr == nil || m2.gotErr == nil {
		t.Fatal("expected error callbacks when async producer not enabled")
	}
}

func TestProduceMessageAsync_ProducerNotEnabled(t *testing.T) {
	p := &producer{}
	m := &struct{ testAsyncMsg }{testAsyncMsg{key: "k", val: []byte("v")}}
	p.ProduceMessageAsync(TopicConfig{Topic: "t", Partition: -1}, m)
	if m.gotErr == nil {
		t.Fatal("expected error callback when async producer not enabled")
	}
}
