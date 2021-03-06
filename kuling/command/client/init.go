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
	client         string
	startID        int
	maxNumMessages int
	message        string
	key            string
	numShards      int
	iter           string
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
	bootstrapGet()
	bootstrapPut()
	bootstrapCreate()
	bootstrapDescribe()
	bootstrapIters()
	bootstrapCommit()

	ClientCmd.PersistentFlags().StringVarP(
		&fetchAddress,
		"host",
		"a",
		kuling.DefaultFetchAddress,
		"Host where server is running",
	)

	// Add all commands
	ClientCmd.AddCommand(
		pingCmd,
		createCmd,
		listCmd,
		describeCmd,
		putCmd,
		getCmd,
		itersCmd,
		commitCmd,
	)
}
