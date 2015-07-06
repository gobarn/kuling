package command

import (
	"fmt"

	"github.com/fredrikbackstrom/kuling/kuling/command/client"
	"github.com/fredrikbackstrom/kuling/kuling/command/server"
	"github.com/spf13/cobra"
)

// Is Application root command
// This command does nothing except serve as a root command for all other
// commands
var AppCmd = &cobra.Command{
	Long: "A Fast and Simple Event Store.\n\nKuling, a Swedish word meaning strong Wind. \nKuling aims at being fast while not sacrificing simplicity of deployment.",
}

// VersionCmd will print the current version of the application
var VersionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version of Kuling",
	Long:  "Print the version of Kuling",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Kuling Log Store v0.1 -- HEAD")
	},
}

// Bootstrap adds all sub commands to Tolinr
func init() {
	// Append all child commands to the application command
	AppCmd.AddCommand(VersionCmd)
	AppCmd.AddCommand(server.ServerCmd)
	AppCmd.AddCommand(client.ClientCmd)
}
