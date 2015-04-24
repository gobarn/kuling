package broker

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"

	"github.com/boltdb/bolt"
	"github.com/fredrikbackstrom/kuling/kuling"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var (
	consumerPositionBucket = "consumer_position"
)

// Broker implements grpc broker.proto server.
// It commands the LogStore to create topics and manages tickets for clients
// that want to read from the broker. Tickets are forwarded to the log store
// for validation
type Broker struct {
	// The path for broker data
	path string
	// This should be discovered by seeds when done for real
	logStoreAddress string
	// Bolt DB used as broker storage
	brokerStore *bolt.DB
}

// NewBroker creates a new broker that controls the log store
func NewBroker(path, logStoreAddress string) *Broker {
	// Guard
	if len(path) == 0 {
		log.Fatalln("No path supplied")
	}
	if len(logStoreAddress) == 0 {
		log.Fatalln("No Log Store Address supplied")
	}

	// Load Broker boltdb from the path specified
	brokerStore, err := bolt.Open(path+"broker.data", 0600, nil)
	if err != nil {
		log.Fatalln("broker: unable to open bolt storage", err)
		return nil
	}

	return &Broker{path, logStoreAddress, brokerStore}
}

// CreateTopic implements helloworld.GreeterServer
func (b *Broker) CreateTopic(ctx context.Context, r *CreateTopicRequest) (*CreateTopicResponse, error) {
	// Call the log store client to create topic on behalf of the client
	// requesting it
	fmt.Println("Creating topic!", r.Topic)

	err := withClient(b.logStoreAddress, func(c kuling.CommandServerClient) error {
		_, err := c.CreateTopic(ctx, &kuling.CreateTopicRequest{r.Topic})
		return err
	})

	if err != nil {
		// Faled to execute function
		return nil, err
	}

	// Return that the topic is created
	return &CreateTopicResponse{r.Topic, int32(200), "Success"}, nil
}

// CommitPosition is a request from a consumer to commit it's position for a
// topic.
//
// If the group is not sent it means that the consumer is not part of a group
// and thus is iterating over the topic by itsefl.
//
func (b *Broker) CommitPosition(ctx context.Context, r *CommitPositionRequest) (*CommitPositionResponse, error) {
	// Handle non group consumer
	// Update position in bolt for the consumer
	err := b.brokerStore.Update(func(tx *bolt.Tx) error {
		consumerPositionBucket, err := tx.CreateBucket([]byte(consumerPositionBucket))
		if err != nil {
			return err
		}

		// Write key
		var keyBuff bytes.Buffer
		keyBuff.WriteString(r.Consumer)
		keyBuff.WriteString(r.Topic)
		keyBuff.WriteString(r.Shard)

		// Put the position of the consumer in the bucket
		var seqBuff bytes.Buffer
		err = binary.Write(&seqBuff, binary.BigEndian, r.SequenceID)
		if err != nil {
			return err
		}

		return consumerPositionBucket.Put(keyBuff.Bytes(), seqBuff.Bytes())
	})
	if err != nil {
		fmt.Println("broker: Could not update consumers position")
		return nil, errors.New("Could not commit consumers position")
	}

	return nil, nil
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

// Helper function that connects to the broker. Takes a function
// that will carry out function on the broker client. Handles opening
// and closing of the connection.
func withClient(dialAddress string, fn func(client kuling.CommandServerClient) error) error {
	// Dial a connection against the broker address
	log.Println("command-client: Calling broker on", dialAddress)
	conn, err := grpc.Dial(dialAddress)

	if err != nil {
		log.Fatalf("command-client: Failed to dial logstore: %v", err)
		return err
	}

	defer conn.Close()

	// Create client from
	client := kuling.NewCommandServerClient(conn)

	// Execcute function with client
	return fn(client)
}
