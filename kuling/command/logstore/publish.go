package logstore

import (
	"fmt"

	"github.com/fredrikbackstrom/kuling/kuling"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"
)

// Flag variables
var (
	key     string
	payload string
)

// Client Command will read from the server
var PublishSingleCommand = &cobra.Command{
	Use:   "publish",
	Short: "Publish message",
	Long:  "Publish a keyed message to the log store",
	Run: func(cmd *cobra.Command, args []string) {
		withClient(func(c kuling.CommandServerClient) {
			if len(topic) == 0 {
				fmt.Println("topic: Not provided")
				return
			}
			if len(shard) == 0 {
				fmt.Println("shard: Not provided")
				return
			}
			if len(key) == 0 {
				fmt.Println("key: Not provided")
				return
			}
			if len(payload) == 0 {
				fmt.Println("payload: Not provided")
				return
			}

			req := &kuling.PublishRequest{
				topic,
				shard,
				[]byte(key),
				[]byte(payload),
			}

			_, err := c.Publish(context.Background(), req)

			if err != nil {
				fmt.Errorf("server: Could not create topic: %v\n", err)
			}

			fmt.Println("server: OK")
		})
	},
}

func bootstrapPublish() {
	PublishSingleCommand.PersistentFlags().StringVarP(&rpcAddress, "rpc-address", "a", kuling.DefaultCommandAddress, "Host where broker is running")
	PublishSingleCommand.PersistentFlags().StringVarP(&topic, "topic", "t", "", "Topic to create")

	PublishSingleCommand.Flags().StringVarP(&shard, "shard", "s", "", "Shard key")
	PublishSingleCommand.Flags().StringVarP(&key, "key", "k", "", "Payload key")
	PublishSingleCommand.Flags().StringVarP(&payload, "payload", "p", "", "Payload")
}
