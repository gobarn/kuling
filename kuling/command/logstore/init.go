package logstore

import (
	"log"

	"github.com/fredrikbackstrom/kuling/kuling"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

var (
	topic          string
	shard          string
	rpcAddress     string
	fetchAddress   string
	group          string
	startID        int
	maxNumMessages int
)

// LogStoreCmd root cmd for log store commands
var LogStoreCmd = &cobra.Command{
	Use:   "logstore",
	Short: "Logstore requests.\n",
	Long:  `Logstore requests\n`,
	Run:   nil,
}

// Broker command/broker init function that sets up
func init() {
	initAdminRPC()
	bootstrapFetch()
	bootstrapPublish()
	bootstrapServer()

	// Add all commands
	LogStoreCmd.AddCommand(FetchCmd, ServerCmd, CreateTopicCommand, PublishSingleCommand)
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
