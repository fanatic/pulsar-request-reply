package responder

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/fanatic/pulsar-request-reply/common"
)

type Responder struct {
	producer *common.Producer
	consumer *common.Consumer
}

func New(ctx context.Context, client pulsar.Client, topic, subscriptionName string) (*Responder, error) {
	log.Printf("creating responder...")

	consumer, err := common.NewConsumer(ctx, client, topic, subscriptionName)
	if err != nil {
		return nil, fmt.Errorf("Could not instantiate Pulsar consumer: %v", err)
	}

	return &Responder{nil, consumer}, nil
}

func (r *Responder) Close() {
	r.consumer.Close()
}
