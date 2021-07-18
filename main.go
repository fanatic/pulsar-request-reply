package main

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/pulsar-client-go/pulsar"
	lib "github.com/fanatic/pulsar-request-reply/requester"
	"github.com/fanatic/pulsar-request-reply/responder"
	"github.com/fanatic/pulsar-request-reply/timing"
)

func main() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})
	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}

	defer client.Close()

	ctx := context.Background()

	// Setup Response Handler (server-side)
	go responder.HandleResponses(ctx, client, "echo-service", func(body []byte) ([]byte, error) {
		defer timing.New("server").Start().Stop()

		return []byte(fmt.Sprintf("Received: %s", body)), nil
	})

	// Make our test requests (client-side)
	for i := 10; i >= 0; i-- {
		hello(ctx, client, fmt.Sprintf("hello delay:%d", i))
	}

	timing.Results()

}

func hello(ctx context.Context, client pulsar.Client, payload string) {
	defer timing.New("rtt").Start().Stop()

	reply, err := lib.Request(ctx, client, "echo-service", []byte(payload))
	if err != nil {
		log.Fatalf("Request failed: %v", err)
	}
	log.Printf("Received reply: %s", reply)
}
