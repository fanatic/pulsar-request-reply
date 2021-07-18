package responder

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/fanatic/pulsar-request-reply/common"
)

func HandleResponses(ctx context.Context, client pulsar.Client, service string, handler func(body []byte) ([]byte, error)) {
	r, err := New(ctx, client, service, "shared")
	if err != nil {
		log.Fatalf("Could not instantiate requester: %v", err)
	}
	defer r.Close()

	for {
		// Consume request
		msg, err := r.consumer.Consumer().Receive(ctx)
		if err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Consumed request : %v %v %v\n", msg.ProducerName(), msg.ID(), string(msg.Payload()))
		}

		reply, err := handler(msg.Payload())
		if err != nil {
			r.consumer.Consumer().Nack(msg)
		}

		r.consumer.Consumer().Ack(msg)

		// Produce response

		p, err := common.NewProducer(ctx, client, service+"."+msg.Properties()["replyTo"])
		if err != nil {
			log.Fatal("Could not instantiate Pulsar producer: %v", err)
		}

		p.Produce(ctx, reply, nil)
		p.Close()
	}
}
