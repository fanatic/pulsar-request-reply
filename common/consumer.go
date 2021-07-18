package common

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/pulsar-client-go/pulsar"
)

type Consumer struct {
	consumer pulsar.Consumer
}

func NewConsumer(ctx context.Context, client pulsar.Client, topic, subscriptionName string) (*Consumer, error) {
	log.Printf("creating consumer...")

	// Use the client to instantiate a consumer
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subscriptionName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}
	return &Consumer{consumer}, nil
}

func (c *Consumer) Close() {
	c.consumer.Close()
}

func (c *Consumer) Consumer() pulsar.Consumer {
	return c.consumer
}

func (c *Consumer) Consume(ctx context.Context) {
	// infinite loop to receive messages
	go func() {
		for {
			msg, err := c.consumer.Receive(ctx)
			if err != nil {
				log.Fatal(err)
			} else {
				fmt.Printf("Consumed message : %v %v %v\n", msg.ProducerName(), msg.ID(), string(msg.Payload()))
			}

			c.consumer.Ack(msg)
		}
	}()

}
