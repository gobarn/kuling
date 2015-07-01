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
	message        string
	key            string
)

// ServerCmd root cmd for log store commands
var ServerCmd = &cobra.Command{
	Use:   "server",
	Short: "Server Commands\n",
	Long:  `Server Commands\n`,
	Run:   nil,
}

// Broker command/broker init function that sets up
func init() {
	initAdminRPC()
	bootstrapFetch()
	bootstrapServer()
	bootstrapAppend()

	// Add all commands
	ServerCmd.AddCommand(
		StandaloneServerCmd,
		CreateTopicCommand,
		FetchCmd,
		AppendCmd,
	)
}

// Helper function that connects to the log server. Takes a function
// that will carry out command on the broker client. Handles opening
// and closing of the connection.
func withClient(clientCommand func(client kuling.CommandServerClient)) {
	// Dial a connection against the broker address
	conn, err := grpc.Dial(rpcAddress)

	if err != nil {
		log.Fatalf("client: Failed to dial server: %v\n", err)
		return
	}
	defer conn.Close()

	// Create client from
	client := kuling.NewCommandServerClient(conn)

	// Execcute function with client
	clientCommand(client)
}
