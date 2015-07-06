package server

import "github.com/spf13/cobra"

// ServerCmd root cmd for log store commands
var ServerCmd = &cobra.Command{
	Use:   "server",
	Short: "Server Commands\n",
	Long:  `Server Commands\n`,
	Run:   nil,
}

// Broker command/broker init function that sets up
func init() {
	bootstrapServer()

	// Add all commands
	ServerCmd.AddCommand(
		StandaloneServerCmd,
	)
}
