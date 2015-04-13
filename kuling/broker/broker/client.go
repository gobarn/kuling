package broker

import (
	"fmt"
	"log"

	"github.com/fredrikbackstrom/kuling/kuling/broker"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// Flag variables
var (
	brokerAddress string
	topic         string
)

// Client Command will read from the server
var CreateTopicCommand = &cobra.Command{
	Use:   "create-topic",
	Short: "Create a new topic",
	Long:  "Create a new topic",
	Run: func(cmd *cobra.Command, args []string) {
		withClient(func(c broker.BrokerClient) {
			response, err := c.CreateTopic(context.Background(), &broker.CreateTopicRequest{topic, 30})

			if err != nil {
				log.Fatalf("fail to exeucte request: %v", err)
				return
			}

			// Check the status of the response.
			fmt.Println(response.Status)
		})
	},
}

// init sets up flags for the client commands
func initBrokerClientCommands() {
	// GLOBAL CLIENT VARS
	// host is available for all commands under server
	CreateTopicCommand.PersistentFlags().StringVarP(
		&brokerAddress,
		"broker-address",
		"a",
		"localhost:8888",
		"Host where broker is running",
	)

	// Initialize all commands with their specific variables.
	initCreateTopicCommand()
}

func initCreateTopicCommand() {
	// port is available for all commands under server
	CreateTopicCommand.PersistentFlags().StringVarP(
		&topic,
		"topic",
		"t",
		"",
		"Topic to create",
	)
}

// Helper function that connects to the broker. Takes a function
// that will carry out function on the broker client. Handles opening
// and closing of the connection.
func withClient(fn func(client broker.BrokerClient)) {
	// Dial a connection against the broker address
	conn, err := grpc.Dial(brokerAddress)

	if err != nil {
		log.Fatalf("fail to dial: %v", err)
		return
	}
	defer conn.Close()

	// Create client from
	client := broker.NewBrokerClient(conn)

	// Execcute function with client
	fn(client)
}
