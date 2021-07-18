package requester

import (
	"context"
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
