package server

import "github.com/spf13/cobra"

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
	numShards      int
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
	bootstrapFetch()
	bootstrapServer()
	bootstrapAppend()
	bootstrapPing()
	bootstrapCreateTopic()

	// Add all commands
	ServerCmd.AddCommand(
		StandaloneServerCmd,
		FetchCmd,
		AppendCmd,
		PingCmd,
		CreateTopicCmd,
	)
}
