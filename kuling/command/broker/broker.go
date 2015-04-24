package broker

import "github.com/spf13/cobra"

// Client Command will read from the server
var BokerRootCmd = &cobra.Command{
	Use:   "broker",
	Short: "Broker Commands",
	Long:  "Broker Commands",
	Run:   nil,
}

// Broker command/broker init function that sets up
func init() {
	initBrokerServerCommands()
	initBrokerClientCommands()

	BokerRootCmd.AddCommand(BrokerServerCmd, CreateTopicCommand)
}
