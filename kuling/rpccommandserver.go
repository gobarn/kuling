package kuling

import (
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
	log.Println("command: Create Topic", r.Topic)
	// Create topic in log store
	err := s.logStore.CreateTopic(r.Topic, 1)

	if err != nil {
		return nil, err
	}

	log.Println("command: Created Topic", r.Topic)
	// Return that the topic is created
	return &CreateTopicResponse{int32(200), r.Topic}, nil
}

// Publish publishes a message
func (s *RPCCommandServer) Publish(ctx context.Context, r *PublishRequest) (*PublishRequestResponse, error) {
	log.Println("command: Publish on topic:", r.Topic)
	// Create topic in log store
	err := s.logStore.Append(r.Topic, r.Shard, r.Key, r.Payload)

	if err != nil {
		log.Println("command:", err.Error())
		return nil, err
	}

	// Return that the topic is created
	return &PublishRequestResponse{int32(200)}, nil
}

// ListenAndServe Starts the rpc server on the listen address
func (s *RPCCommandServer) ListenAndServe(laddr string) {
	// Create a listener at provided address
	lis, err := net.Listen("tcp", laddr)

	if err != nil {
		log.Fatalf("command: failed to listen: %v", err)
		return
	}

	log.Println("command: Listening on", laddr)

	// Start a new GRPC server and register the broker service.
	server := grpc.NewServer()
	RegisterCommandServerServer(server, s)
	// Start serving the command rpc server
	server.Serve(lis)
}
