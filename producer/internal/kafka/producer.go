package kafka

import (
	"fmt"
	"time"

	"github.com/IBM/sarama"
)

// Message represents a Kafka message with key and value.
type Message struct {
	Key   string
	Value []byte
}

// TopicConfig holds topic configuration for message production.
type TopicConfig struct {
	Topic     string
	Partition int32 // -1 for automatic partitioning
}

// AsyncCallback defines callback handler for async message production.
type AsyncCallback interface {
	Callback(partition int32, offset int64, err error)
}

type IProducerKafka interface {
	ProduceMessage(topicConfig TopicConfig, message Message) error
	ProduceMessages(topicConfig TopicConfig, messages []Message) error
	ProduceMessageAsync(topicConfig TopicConfig, message AsyncMessage)
	ProduceMessagesAsync(topicConfig TopicConfig, messages []AsyncMessage)
	Close() error
}

type ProducerConfig struct {
	Brokers []string
	// Optional configurations
	RetryMax     int
	RetryBackoff time.Duration
	RequiredAcks sarama.RequiredAcks
	Compression  sarama.CompressionCodec
	Async        bool // Enable async producer
}

// AsyncMessage represents an async-send unit with payload and callback.
type AsyncMessage interface {
	GetKey() string
	GetValue() []byte
	AsyncCallback
}

type producer struct {
	syncProducer  sarama.SyncProducer
	asyncProducer sarama.AsyncProducer
	config        ProducerConfig
}

func New(config ProducerConfig) (IProducerKafka, error) {
	if len(config.Brokers) == 0 {
		return nil, fmt.Errorf("at least one broker must be specified")
	}

	if config.RetryMax == 0 {
		config.RetryMax = 3
	}
	if config.RetryBackoff == 0 {
		config.RetryBackoff = 100 * time.Millisecond
	}
	if config.RequiredAcks == 0 {
		config.RequiredAcks = sarama.WaitForLocal
	}
	if config.Compression == 0 {
		config.Compression = sarama.CompressionSnappy
	}

	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true
	saramaConfig.Producer.Retry.Max = config.RetryMax
	saramaConfig.Producer.Retry.Backoff = config.RetryBackoff
	saramaConfig.Producer.RequiredAcks = config.RequiredAcks
	saramaConfig.Producer.Compression = config.Compression

	kp := &producer{
		config: config,
	}

	if config.Async {
		asyncProducer, err := sarama.NewAsyncProducer(config.Brokers, saramaConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create async Kafka producer: %w", err)
		}

		kp.asyncProducer = asyncProducer

		go kp.handleAsyncCallbacks()
	} else {
		syncProducer, err := sarama.NewSyncProducer(config.Brokers, saramaConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create sync Kafka producer: %w", err)
		}

		kp.syncProducer = syncProducer
	}

	return kp, nil
}

// ProduceMessage sends a single message to Kafka.
func (p *producer) ProduceMessage(topicConfig TopicConfig, message Message) error {
	producerMessage := &sarama.ProducerMessage{
		Topic:     topicConfig.Topic,
		Partition: topicConfig.Partition,
		Key:       sarama.StringEncoder(message.Key),
		Value:     sarama.ByteEncoder(message.Value),
		Timestamp: time.Now(),
	}

	partition, offset, err := p.syncProducer.SendMessage(producerMessage)
	if err != nil {
		return fmt.Errorf("failed to send message to topic %s: %w", topicConfig.Topic, err)
	}

	// Log successful send (optional)
	fmt.Printf("Message sent to topic %s, partition %d, offset %d\n", topicConfig.Topic, partition, offset)
	return nil
}

// ProduceMessages sends multiple messages to Kafka in batch.
func (p *producer) ProduceMessages(topicConfig TopicConfig, messages []Message) error {
	if len(messages) == 0 {
		return fmt.Errorf("no messages to send")
	}

	producerMessages := make([]*sarama.ProducerMessage, len(messages))
	for i, msg := range messages {
		producerMessages[i] = &sarama.ProducerMessage{
			Topic:     topicConfig.Topic,
			Partition: topicConfig.Partition,
			Key:       sarama.StringEncoder(msg.Key),
			Value:     sarama.ByteEncoder(msg.Value),
			Timestamp: time.Now(),
		}
	}

	err := p.syncProducer.SendMessages(producerMessages)
	if err != nil {
		return fmt.Errorf("failed to send batch messages to topic %s: %w", topicConfig.Topic, err)
	}

	fmt.Printf("Successfully sent %d messages to topic %s\n", len(messages), topicConfig.Topic)
	return nil
}

// ProduceMessageAsync sends a single message to Kafka asynchronously.
func (p *producer) ProduceMessageAsync(topicConfig TopicConfig, message AsyncMessage) {
	if p.asyncProducer == nil {
		if message != nil {
			safeInvokeCallback(message, 0, 0, fmt.Errorf("async producer not enabled"))
		}
		return
	}

	if message == nil {
		return
	}

	producerMessage := &sarama.ProducerMessage{
		Topic:     topicConfig.Topic,
		Partition: topicConfig.Partition,
		Key:       sarama.StringEncoder(message.GetKey()),
		Value:     sarama.ByteEncoder(message.GetValue()),
		Timestamp: time.Now(),
		Metadata:  message,
	}

	select {
	case p.asyncProducer.Input() <- producerMessage:
		// queued
	default:
		safeInvokeCallback(message, 0, 0, fmt.Errorf("producer input channel is full"))
	}
}

func (p *producer) ProduceMessagesAsync(topicConfig TopicConfig, messages []AsyncMessage) {
	if p.asyncProducer == nil {
		for _, m := range messages {
			if m != nil {
				safeInvokeCallback(m, 0, 0, fmt.Errorf("async producer not enabled"))
			}
		}
		return
	}

	if len(messages) == 0 {
		return
	}

	for _, m := range messages {
		if m == nil {
			continue
		}
		pm := &sarama.ProducerMessage{
			Topic:     topicConfig.Topic,
			Partition: topicConfig.Partition,
			Key:       sarama.StringEncoder(m.GetKey()),
			Value:     sarama.ByteEncoder(m.GetValue()),
			Timestamp: time.Now(),
			Metadata:  m,
		}

		select {
		case p.asyncProducer.Input() <- pm:
		default:
			safeInvokeCallback(m, 0, 0, fmt.Errorf("producer input channel is full"))
		}
	}
}

// handleAsyncCallbacks processes success and error callbacks for async messages.
func (p *producer) handleAsyncCallbacks() {
	for {
		select {
		case success, ok := <-p.asyncProducer.Successes():
			if !ok {
				return
			}
			if success != nil {
				if handler, ok := success.Metadata.(AsyncCallback); ok && handler != nil {
					safeInvokeCallback(handler, success.Partition, success.Offset, nil)
				}
			}
		case errItem, ok := <-p.asyncProducer.Errors():
			if !ok {
				return
			}
			if errItem != nil && errItem.Msg != nil {
				if handler, ok := errItem.Msg.Metadata.(AsyncCallback); ok && handler != nil {
					safeInvokeCallback(handler, 0, 0, errItem.Err)
				}
			}
		}
	}
}

func safeInvokeCallback(cb AsyncCallback, partition int32, offset int64, err error) {
	defer func() {
		_ = recover()
	}()
	if cb != nil {
		cb.Callback(partition, offset, err)
	}
}

func (p *producer) Close() error {
	var err error
	if p.syncProducer != nil {
		err = p.syncProducer.Close()
	}
	if p.asyncProducer != nil {
		if closeErr := p.asyncProducer.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}
