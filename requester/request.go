package requester

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/google/uuid"
)

func Request(ctx context.Context, client pulsar.Client, service string, payload []byte) ([]byte, error) {
	requestID := uuid.NewString()

	r, err := New(ctx, client, service, requestID)
	if err != nil {
		log.Fatalf("Could not instantiate requester: %v", err)
	}
	defer r.Close()

	// Produce request (asynchronously)
	r.producer.Produce(ctx, payload, map[string]string{"replyTo": "reply-" + requestID})

	// Consume response
	msg, err := r.consumer.Consumer().Receive(ctx)
	if err != nil {
		log.Fatal(err)
	}
	r.consumer.Consumer().Ack(msg)

	return msg.Payload(), nil
}

func AsyncRequest(ctx context.Context, client pulsar.Client, service string, payload []byte, reply chan []byte) error {
	requestID := uuid.NewString()

	r, err := New(ctx, client, service, requestID)
	if err != nil {
		return fmt.Errorf("Could not instantiate requester: %w", err)
	}

	// Produce request (asynchronously)
	r.producer.Produce(ctx, payload, map[string]string{"replyTo": "reply-" + requestID})

	// Consume response
	go func() {
		msg, err := r.consumer.Consumer().Receive(ctx)
		if err != nil {
			log.Fatal(err)
		}
		r.consumer.Consumer().Ack(msg)

		reply <- msg.Payload()

		r.Close()
	}()

	return nil
}
