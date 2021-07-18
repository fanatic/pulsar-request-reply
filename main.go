package main

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/pulsar-client-go/pulsar"
	lib "github.com/fanatic/pulsar-request-reply/requester"
	"github.com/fanatic/pulsar-request-reply/responder"
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
		return []byte(fmt.Sprintf("Received: %s", body)), nil
	})

	// Make our test requests (client-side)
	for i := 10; i >= 0; i-- {
		reply, err := lib.Request(ctx, client, "echo-service", []byte(fmt.Sprintf("hello delay:%d", i)))
		if err != nil {
			log.Fatalf("Request failed: %v", err)
		}
		log.Printf("Received reply: %s", reply)
	}

}
