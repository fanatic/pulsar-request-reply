package common

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/pulsar-client-go/pulsar"
)

type Reader struct {
	reader pulsar.Reader
}

func NewReader(ctx context.Context, client pulsar.Client, topic string) (*Reader, error) {
	log.Printf("creating reader...")

	// Use the client to instantiate a reader
	reader, err := client.CreateReader(pulsar.ReaderOptions{
		Topic:          topic,
		StartMessageID: pulsar.EarliestMessageID(),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create reader: %w", err)
	}

	return &Reader{reader}, nil
}

func (r *Reader) Close() {
	r.reader.Close()
}

func (r *Reader) Read(ctx context.Context) {
	// infinite loop to receive messages
	go func() {
		for {
			log.Print("Reader waiting for next message\n")
			msg, err := r.reader.Next(ctx)
			if err != nil {
				log.Fatal(err)
			} else {
				fmt.Printf("Read message : %v %v %v\n", msg.ProducerName(), msg.ID(), string(msg.Payload()))
			}
		}
	}()
}
