package broker

import (
	"fmt"

	"golang.org/x/net/context"
)

// Broker implements grpc broker.proto server.
// It commands the LogStore to create topics and manages tickets for clients
// that want to read from the broker. Tickets are forwarded to the log store
// for validation
type Broker struct {
}

// NewBroker creates a new broker that controls the log store
func NewBroker() *Broker {
	return &Broker{}
}

// CreateTopic implements helloworld.GreeterServer
func (b *Broker) CreateTopic(ctx context.Context, r *CreateTopicRequest) (*CreateTopicResponse, error) {
	fmt.Println("Creating topic!" + r.Topic)
	// Return that the topic is created
	return &CreateTopicResponse{r.Topic, int32(200), "Success"}, nil
}

// AuthorizeFetch a
func (b *Broker) AuthorizeFetch(ctx context.Context, authRequest *AuthFetchRequest) (*AuthFetchResponse, error) {
	fmt.Println("Creating topic!" + authRequest.Topic)

	// Client requests to read from a topic.
	// Find where the shards for the topic are hosted and return a list of
	// those hosts together with a token that validates the client for read
	// access on those data nodes.

	//  Generate access token for the clients request.
	token := int32(123)

	// Get list of shards that the client should read together with the address
	// of the host that are running those shards
	var readShards []*AddressTopicShard
	readShards = append(readShards, &AddressTopicShard{authRequest.Topic, "shard_01", "localhost:9999"})

	return &AuthFetchResponse{token, readShards}, nil
}
