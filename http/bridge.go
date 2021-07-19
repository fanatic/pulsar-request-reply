package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/fanatic/pulsar-request-reply/requester"
	"github.com/fanatic/pulsar-request-reply/responder"
)

type APIGatewayProxyRequest struct {
	Resource              string              `json:"resource"`
	Path                  string              `json:"path"`
	HTTPMethod            string              `json:"httpMethod"`
	Headers               map[string][]string `json:"headers"`
	QueryStringParameters map[string][]string `json:"queryStringParameters"`
	Body                  string              `json:"body"`
	IsBase64Encoded       bool                `json:"isBase64Encoded,omitempty"`
}

// APIGatewayProxyResponse configures the response to be returned by API Gateway for the request
type APIGatewayProxyResponse struct {
	StatusCode      int                 `json:"statusCode"`
	Headers         map[string][]string `json:"headers"`
	Body            string              `json:"body"`
	IsBase64Encoded bool                `json:"isBase64Encoded,omitempty"`
}

func bridge() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})
	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}

	defer client.Close()

	go responder.HandleResponses(context.Background(), client, "welcome-service", bridgeHandler)

	http.HandleFunc("/welcome-service", func(w http.ResponseWriter, r *http.Request) {
		buf := new(bytes.Buffer)
		encoder := base64.NewEncoder(base64.StdEncoding, buf)
		if _, err := io.Copy(encoder, r.Body); err != nil {
			fmt.Printf("Error0: %v\n", err)
			return
		}

		firstPartOfPath := strings.Split(r.URL.Path, "/")[1]
		restOfPath := strings.Join(strings.Split(r.URL.Path, "/")[2:], "/")

		payload, _ := json.Marshal(APIGatewayProxyRequest{
			Resource:              firstPartOfPath,
			Path:                  restOfPath,
			HTTPMethod:            r.Method,
			Headers:               r.Header,
			QueryStringParameters: r.URL.Query(),
			Body:                  buf.String(),
			IsBase64Encoded:       true,
		})
		reply, err := requester.Request(r.Context(), client, "welcome-service", payload)
		if err != nil {
			log.Fatalf("Request failed: %v", err)
		}

		fmt.Printf("Reply: %v\n", reply)
		var responsePayload APIGatewayProxyResponse
		if err := json.Unmarshal(reply, &responsePayload); err != nil {
			log.Fatalf("Response failed: %v", err)
		}
		for k, v := range responsePayload.Headers {
			w.Header()[k] = v
		}
		w.WriteHeader(responsePayload.StatusCode)

		body := base64.NewDecoder(base64.StdEncoding, bytes.NewBufferString(responsePayload.Body))
		if _, err := io.Copy(w, body); err != nil {
			fmt.Printf("Error10: %v\n", err)
			return
		}
	})

	http.ListenAndServe(":8000", nil)
}

func bridgeHandler(payload []byte) ([]byte, error) {
	var bridgeRequest APIGatewayProxyRequest
	if err := json.Unmarshal(payload, &bridgeRequest); err != nil {
		return nil, err
	}

	if bridgeRequest.Path == "/welcome-service" {
		return nil, nil
	}

	body := base64.NewDecoder(base64.StdEncoding, bytes.NewBufferString(bridgeRequest.Body))

	req, err := http.NewRequestWithContext(
		context.Background(),
		bridgeRequest.HTTPMethod,
		"http://localhost:8090"+bridgeRequest.Path,
		body,
	)
	if err != nil {
		fmt.Printf("Error1: %v\n", err)
		return nil, err
	}
	req.Header = bridgeRequest.Headers
	for k, v := range bridgeRequest.QueryStringParameters {
		for _, vv := range v {
			req.URL.Query().Add(k, vv)
		}
	}
	fmt.Printf("Making request %s\n", req.URL.String())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Printf("Error2: %v\n", err)
		return nil, err
	}
	defer resp.Body.Close()

	buf := new(bytes.Buffer)
	encoder := base64.NewEncoder(base64.StdEncoding, buf)
	if n, err := io.Copy(encoder, resp.Body); err != nil {
		fmt.Printf("Error3: %v\n", err)
		return nil, err
	} else {
		fmt.Printf("Copied %d bytes\n", n)
	}
	encoder.Close()

	responsePayload, _ := json.Marshal(APIGatewayProxyResponse{
		StatusCode:      resp.StatusCode,
		Headers:         resp.Header,
		Body:            buf.String(),
		IsBase64Encoded: true,
	})

	return responsePayload, nil
}
