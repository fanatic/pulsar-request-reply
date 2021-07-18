package requester

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/fanatic/pulsar-request-reply/common"
)

type Requester struct {
	producer *common.Producer
	consumer *common.Consumer
}

func New(ctx context.Context, client pulsar.Client, topic, requestID string) (*Requester, error) {
	log.Printf("creating requester...")

	producer, err := common.NewProducer(ctx, client, topic)
	if err != nil {
		return nil, fmt.Errorf("Could not instantiate Pulsar producer: %v", err)
	}

	consumer, err := common.NewConsumer(ctx, client, topic+".reply-"+requestID, "reply-"+requestID)
	if err != nil {
		return nil, fmt.Errorf("Could not instantiate Pulsar consumer: %v", err)
	}

	return &Requester{producer, consumer}, nil
}

func (r *Requester) Close() {
	r.producer.Close()
	r.consumer.Close()
}
