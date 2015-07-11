package client

import (
	"github.com/fredrikbackstrom/kuling/kuling"
	"github.com/spf13/cobra"
)

var (
	topic          string
	shard          string
	fetchAddress   string
	group          string
	startID        int
	maxNumMessages int
	message        string
	key            string
	numShards      int
)

// ServerCmd root cmd for log store commands
var ClientCmd = &cobra.Command{
	Use:   "client",
	Short: "Client Commands\n",
	Long:  `Client Commands\n`,
	Run:   nil,
}

// Broker command/broker init function that sets up
func init() {
	bootstrapFetch()
	bootstrapAppend()
	bootstrapCreateTopic()
	bootstrapShards()

	ClientCmd.PersistentFlags().StringVarP(
		&fetchAddress,
		"host",
		"a",
		kuling.DefaultFetchAddress,
		"Host where server is running",
	)

	// Add all commands
	ClientCmd.AddCommand(
		FetchCmd,
		AppendCmd,
		PingCmd,
		CreateTopicCmd,
		ListTopicsCmd,
		ListShardsCmd,
	)
}
