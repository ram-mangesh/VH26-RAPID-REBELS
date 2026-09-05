package kafka

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string) (*Producer, error) {
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireOne,
		Async:        false,
		BatchSize:    1,
		BatchTimeout: 10 * time.Millisecond,
		WriteTimeout: 5 * time.Second,
		ReadTimeout:  5 * time.Second,
		MaxAttempts:  3,
	}
	return &Producer{writer: w}, nil
}

func (p *Producer) Publish(ctx context.Context, topic, key string, value []byte) error {
	err := p.writer.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: value,
		Time:  time.Now(),
	})
	if err != nil {
		log.Printf("Kafka publish error: %v", err)
	}
	return err
}

func (p *Producer) PublishAsync(topic, key string, value []byte) {
	msg := kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: value,
		Time:  time.Now(),
	}
	err := p.writer.WriteMessages(context.Background(), msg)
	if err != nil {
		log.Printf("Kafka async publish error: %v", err)
	}
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

// EnsureTopics creates required Kafka topics if they don't exist.
func EnsureTopics(brokers []string, topics []string) error {
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("connect to kafka: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("get controller: %w", err)
	}

	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return fmt.Errorf("connect to controller: %w", err)
	}
	defer controllerConn.Close()

	specs := make([]kafka.TopicConfig, 0, len(topics))
	for _, t := range topics {
		specs = append(specs, kafka.TopicConfig{
			Topic:             t,
			NumPartitions:     3,
			ReplicationFactor: 1,
		})
	}

	err = controllerConn.CreateTopics(specs...)
	if err != nil && err.Error() != "Topic already exists." {
		return fmt.Errorf("create topics: %w", err)
	}
	return nil
}


