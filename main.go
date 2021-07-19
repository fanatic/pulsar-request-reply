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
	go responder.HandleResponses(ctx, client, "echo-service", helloHandler)

	// Make our test requests (client-side)
	for i := 10; i >= 0; i-- {
		hello(ctx, client, "hello")
	}

	asyncHello(ctx, client, "hello")

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

func helloHandler(payload []byte) ([]byte, error) {
	defer timing.New("rtt").Start().Stop()

	return []byte(fmt.Sprintf("Received: %s", payload)), nil
}

func asyncHello(ctx context.Context, client pulsar.Client, payload string) {
	defer timing.New("async-rtt").Start().Stop()

	reply := make(chan []byte)
	n := 100

	for i := 0; i < n; i++ {
		err := lib.AsyncRequest(ctx, client, "echo-service", []byte(payload), reply)
		if err != nil {
			log.Fatalf("Request failed: %v", err)
		}
	}

	for i := 0; i < n; i++ {
		log.Printf("Received reply: %s", <-reply)
	}
}
