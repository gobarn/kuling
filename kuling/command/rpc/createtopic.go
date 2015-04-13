package rpc

import (
	"fmt"
	"log"

	"github.com/fredrikbackstrom/kuling/kuling"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// Flag variables
var (
	rpcAddress string
	topic      string
	shard      string
	key        string
	payload    string
)

// Client Command will read from the server
var CreateTopicCommand = &cobra.Command{
	Use:   "create-topic",
	Short: "Create a new topic",
	Long:  "Create a new topic",
	Run: func(cmd *cobra.Command, args []string) {
		withClient(func(c kuling.CommandServerClient) {
			_, err := c.CreateTopic(context.Background(), &kuling.CreateTopicRequest{topic})

			if err != nil {
				fmt.Errorf("rpc: Could not create topic: %v", err)
			}
		})
	},
}

// Client Command will read from the server
var PublishSingleCommand = &cobra.Command{
	Use:   "publish",
	Short: "Publish message",
	Long:  "Publish a keyed message to the log store",
	Run: func(cmd *cobra.Command, args []string) {
		withClient(func(c kuling.CommandServerClient) {
			_, err := c.Publish(context.Background(), &kuling.PublishRequest{topic, shard, []byte(key), []byte(payload)})

			if err != nil {
				fmt.Errorf("rpc: Could not create topic: %v", err)
			}
		})
	},
}

// init sets up flags for the client commands
func initCreateTopicCommand() {
	// GLOBAL CLIENT VARS
	// host is available for all commands under server
	CreateTopicCommand.PersistentFlags().StringVarP(
		&rpcAddress,
		"rpc-address",
		"a",
		"localhost:7777",
		"Host where broker is running",
	)

	// port is available for all commands under server
	CreateTopicCommand.PersistentFlags().StringVarP(
		&topic,
		"topic",
		"t",
		"",
		"Topic to create",
	)

	// PUBLISH
	PublishSingleCommand.PersistentFlags().StringVarP(
		&rpcAddress,
		"rpc-address",
		"a",
		"localhost:7777",
		"Host where broker is running",
	)

	// port is available for all commands under server
	PublishSingleCommand.PersistentFlags().StringVarP(
		&topic,
		"topic",
		"t",
		"",
		"Topic to create",
	)

	// port is available for all commands under server
	PublishSingleCommand.PersistentFlags().StringVarP(
		&shard,
		"shard",
		"s",
		"",
		"Shard key",
	)

	// port is available for all commands under server
	PublishSingleCommand.PersistentFlags().StringVarP(
		&key,
		"key",
		"k",
		"",
		"Payload key",
	)

	// port is available for all commands under server
	PublishSingleCommand.PersistentFlags().StringVarP(
		&payload,
		"payload",
		"p",
		"",
		"Payload",
	)
}

// Helper function that connects to the broker. Takes a function
// that will carry out function on the broker client. Handles opening
// and closing of the connection.
func withClient(fn func(client kuling.CommandServerClient)) {
	// Dial a connection against the broker address
	conn, err := grpc.Dial(rpcAddress)

	if err != nil {
		log.Fatalf("fail to dial: %v", err)
		return
	}
	defer conn.Close()

	// Create client from
	client := kuling.NewCommandServerClient(conn)

	// Execcute function with client
	fn(client)
}
