package logstore

import (
	"fmt"

	"github.com/fredrikbackstrom/kuling/kuling"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"
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

// init sets up flags for the client commands
func initAdminRPC() {
	CreateTopicCommand.PersistentFlags().StringVarP(&rpcAddress, "rpc-address", "a", kuling.DefaultCommandAddress, "Host where broker is running")
}
