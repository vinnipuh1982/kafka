package bus

import (
	"context"
	"fmt"
	"sync"
	"time"

	eventv1 "vinnipuh1982/producer/generated/proto/event/v1"
	"vinnipuh1982/producer/internal/codec"
	"vinnipuh1982/producer/internal/kafka"

	"github.com/google/uuid"
)

type Reply struct {
	Partition int32
	Offset    int64
	Err       error
}

// Event is an incoming message request with a reply channel.
type Event struct {
	Type  string
	Data  map[string]string
	Reply chan Reply
}

// MessageSender defines the interface to send a single message.
type MessageSender interface {
	SendMessage(ctx context.Context, topic kafka.TopicConfig, e Event) error
}

var (
	manager     *batchManager
	managerOnce sync.Once
)

// Init configures the singleton batcher. It must be called once at startup.
func Init(prod kafka.IProducerKafka, serializer codec.ISerializer, maxBatchSize int, flushInterval time.Duration, inputBuffer int) MessageSender {
	managerOnce.Do(func() {
		if maxBatchSize <= 0 {
			maxBatchSize = 100
		}
		if flushInterval <= 0 {
			flushInterval = 200 * time.Millisecond
		}
		if inputBuffer <= 0 {
			inputBuffer = 1024
		}
		manager = &batchManager{
			producer:      prod,
			serializer:    serializer,
			maxBatchSize:  maxBatchSize,
			flushInterval: flushInterval,
			in:            make(chan eventWithTopic, inputBuffer),
		}
		go manager.loop()
	})
	return manager
}

// Get returns the initialized singleton. Must call Init first.
func Get() MessageSender { return manager }

type batchManager struct {
	producer      kafka.IProducerKafka
	serializer    codec.ISerializer
	maxBatchSize  int
	flushInterval time.Duration
	in            chan eventWithTopic
}

// ResetForTest resets the singleton for tests.
func ResetForTest() {
	manager = nil
	managerOnce = sync.Once{}
}

func (b *batchManager) SendMessage(ctx context.Context, topic kafka.TopicConfig, e Event) error {
	if e.Reply == nil {
		return fmt.Errorf("reply channel is required")
	}
	ew := eventWithTopic{Event: e, Topic: topic}
	select {
	case b.in <- ew:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *batchManager) loop() {
	ticker := time.NewTicker(b.flushInterval)
	defer ticker.Stop()

	batch := make([]eventWithTopic, 0, b.maxBatchSize)

	for {
		select {
		case ewt, ok := <-b.in:
			if !ok {
				return
			}
			batch = append(batch, ewt)
			if len(batch) >= b.maxBatchSize {
				msgsByTopic := b.flush(batch)
				for topic, msgs := range msgsByTopic {
					if len(msgs) > 0 {
						b.producer.ProduceMessagesAsync(topic, msgs)
					}
				}
				batch = batch[:0]
			}
		case <-ticker.C:
			msgsByTopic := b.flush(batch)
			for topic, msgs := range msgsByTopic {
				if len(msgs) > 0 {
					b.producer.ProduceMessagesAsync(topic, msgs)
				}
			}
			batch = batch[:0]
		}
	}
}

// eventWithTopic is a helper to pair Event with its topic during enqueue.
type eventWithTopic struct {
	Event Event
	Topic kafka.TopicConfig
}

// flush converts a batch of Events into topic-grouped AsyncMessages.
func (b *batchManager) flush(batch []eventWithTopic) map[kafka.TopicConfig][]kafka.AsyncMessage {
	result := make(map[kafka.TopicConfig][]kafka.AsyncMessage)
	if len(batch) == 0 {
		return result
	}
	for i := range batch {
		ewt := batch[i]
		ev := ewt.Event
		topic := ewt.Topic
		pb := &eventv1.Event{EventId: uuid.NewString(), Type: ev.Type, Data: ev.Data}
		payload, err := b.serializer.Serialize(pb)
		if err != nil {
			select {
			case ev.Reply <- Reply{Err: err}:
			default:
			}
			continue
		}

		result[topic] = append(result[topic], &asyncMsg{key: pb.EventId, value: payload, reply: ev.Reply})
	}
	return result
}

// asyncMsg adapts our Event to kafka.AsyncMessage and forwards callbacks to the per-event reply channel.
type asyncMsg struct {
	key   string
	value []byte
	reply chan Reply
}

func (m *asyncMsg) GetKey() string   { return m.key }
func (m *asyncMsg) GetValue() []byte { return m.value }
func (m *asyncMsg) Callback(partition int32, offset int64, err error) {
	// forward to caller; non-blocking to avoid deadlocks if caller already left
	select {
	case m.reply <- Reply{Partition: partition, Offset: offset, Err: err}:
	default:
	}
}
