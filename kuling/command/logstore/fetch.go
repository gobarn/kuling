package logstore

import (
	"fmt"

	"github.com/fredrikbackstrom/kuling/kuling"
	"github.com/spf13/cobra"
)

// FetchCmd will read from the server
var FetchCmd = &cobra.Command{
	Use:   "fetch",
	Short: "Fetch messages",
	Long:  "Performs a one time fetch of messages from the topic and fetches\nmessages starting from the start sequence id and reading a max number \nof messages. Note that you will not get back as many messages as max \nspecifies if that amount of messages does not exist. ",
	Run: func(cmd *cobra.Command, args []string) {
		client := kuling.NewLogStoreClient(fetchAddress)

		req := kuling.NewFetchRequest(
			topic,
			shard,
			int64(startID),
			int64(maxNumMessages))

		err := client.Fetch(req)

		if err != nil {
			fmt.Println(err)
		}
	},
}

// init sets up flags for the client commands
func bootstrapFetch() {
	// host is available for all commands under server
	FetchCmd.PersistentFlags().StringVarP(
		&fetchAddress,
		"host",
		"a",
		kuling.DefaultFetchAddress,
		"Host where server is running",
	)

	FetchCmd.PersistentFlags().StringVarP(
		&topic,
		"topic",
		"t",
		"",
		"Topic to stream messages from",
	)

	FetchCmd.PersistentFlags().StringVarP(
		&shard,
		"shard",
		"s",
		"",
		"Shard in the stream to read from",
	)

	FetchCmd.PersistentFlags().IntVarP(
		&startID,
		"offset-sequence-id",
		"o",
		0,
		"Sequence ID to start reading messages from",
	)

	FetchCmd.PersistentFlags().IntVarP(
		&maxNumMessages,
		"max-num-messages",
		"m",
		1,
		"Maximum messages to receive back",
	)
}
