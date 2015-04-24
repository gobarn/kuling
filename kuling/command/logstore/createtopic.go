package logstore

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
			// Execute create topic command against the kuling server
			_, err := c.CreateTopic(context.Background(), &kuling.CreateTopicRequest{topic})

			if err != nil {
				fmt.Errorf("server: Could not create topic: %v\n", err)
			}

			fmt.Println("server: OK")
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
			if len(topic) == 0 {
				fmt.Println("topic: Not provided")
				return
			}
			if len(shard) == 0 {
				fmt.Println("shard: Not provided")
				return
			}
			if len(key) == 0 {
				fmt.Println("key: Not provided")
				return
			}
			if len(payload) == 0 {
				fmt.Println("payload: Not provided")
				return
			}

			req := &kuling.PublishRequest{
				topic,
				shard,
				[]byte(key),
				[]byte(payload),
			}

			_, err := c.Publish(context.Background(), req)

			if err != nil {
				fmt.Errorf("server: Could not create topic: %v\n", err)
			}

			fmt.Println("server: OK")
		})
	},
}

// init sets up flags for the client commands
func initAdminRPC() {
	PublishSingleCommand.PersistentFlags().StringVarP(&rpcAddress, "rpc-address", "a", "localhost:7777", "Host where broker is running")
	PublishSingleCommand.PersistentFlags().StringVarP(&topic, "topic", "t", "", "Topic to create")

	PublishSingleCommand.Flags().StringVarP(&shard, "shard", "s", "", "Shard key")
	PublishSingleCommand.Flags().StringVarP(&key, "key", "k", "", "Payload key")
	PublishSingleCommand.Flags().StringVarP(&payload, "payload", "p", "", "Payload")

}

// Helper function that connects to the log server. Takes a function
// that will carry out command on the broker client. Handles opening
// and closing of the connection.
func withClient(clientCommand func(client kuling.CommandServerClient)) {
	// Dial a connection against the broker address
	conn, err := grpc.Dial(rpcAddress)

	if err != nil {
		log.Fatalf("command: Failed to dial: %v\n", err)
		return
	}
	defer conn.Close()

	// Create client from
	client := kuling.NewCommandServerClient(conn)

	// Execcute function with client
	clientCommand(client)
}
