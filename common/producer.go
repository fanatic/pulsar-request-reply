package common

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/pulsar-client-go/pulsar"
)

type Producer struct {
	producer pulsar.Producer
}

func NewProducer(ctx context.Context, client pulsar.Client, topic string) (*Producer, error) {
	// Use the client to instantiate a producer
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}
	return &Producer{producer}, nil
}

func (p *Producer) Close() {
	p.producer.Flush()
	p.producer.Close()
}

func (p *Producer) Produce(ctx context.Context, payload []byte, properties map[string]string) {
	asyncMsg := pulsar.ProducerMessage{
		Payload:    payload,
		Properties: properties,
	}

	p.producer.SendAsync(ctx, &asyncMsg, func(msgID pulsar.MessageID, msg *pulsar.ProducerMessage, err error) {
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("the %s successfully published with the message ID %v\n", string(msg.Payload), msgID)
	})
}
