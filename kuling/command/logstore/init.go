package logstore

import "github.com/spf13/cobra"

var (
	topic string
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
	bootstrapServer()

	// Add all commands
	LogStoreCmd.AddCommand(FetchCmd, ServerCmd, CreateTopicCommand, PublishSingleCommand)
}
