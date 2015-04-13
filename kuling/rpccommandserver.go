package kuling

import (
	"fmt"
	"log"
	"net"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// RPCCommandServer  implements RPC commands that may be executed towards
// a running LogServer instance.
type RPCCommandServer struct {
	logStore LogStore
}

// NewRPCCommandServer  creates a new broker that controls the log store
func NewRPCCommandServer(ls LogStore) *RPCCommandServer {
	return &RPCCommandServer{ls}
}

// CreateTopic implements helloworld.GreeterServer
func (s *RPCCommandServer) CreateTopic(ctx context.Context, r *CreateTopicRequest) (*CreateTopicResponse, error) {
	// Create topic in log store
	err := s.logStore.CreateTopic(r.Topic)

	if err != nil {
		return nil, err
	}

	fmt.Println("Creating topic!" + r.Topic)
	// Return that the topic is created
	return &CreateTopicResponse{int32(200), r.Topic}, nil
}

// Publish publishes a message
func (s *RPCCommandServer) Publish(ctx context.Context, r *PublishRequest) (*PublishRequestResponse, error) {
	// Create topic in log store
	err := s.logStore.Write(r.Topic, r.Shard, r.Key, r.Payload)

	if err != nil {
		log.Printf("publish: %v", err.Error())
		return nil, err
	}

	fmt.Println("Writing to kittens!" + string(r.Key) + ":" + string(r.Payload))
	// Return that the topic is created
	return &PublishRequestResponse{int32(200)}, nil
}

// ListenAndServe Starts the rpc server on the listen address
func (s *RPCCommandServer) ListenAndServe(laddr string) {
	// Create a listener at provided address
	fmt.Println("Running Command Server")
	lis, err := net.Listen("tcp", laddr)

	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
		return
	}

	// Start a new GRPC server and register the broker service.
	server := grpc.NewServer()
	RegisterCommandServerServer(server, s)
	// Start serving the command rpc server
	server.Serve(lis)
}
