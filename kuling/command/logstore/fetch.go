package logstore

import (
	"fmt"

	"github.com/fredrikbackstrom/kuling/kuling"
	"github.com/spf13/cobra"
)

// Flag variables
var (
	fetchAddress   string
	group          string
	startID        int
	maxNumMessages int
)

// FetchCmd will read from the server
var FetchCmd = &cobra.Command{
	Use:   "fetch",
	Short: "Fetch messages",
	Long:  "Performs a one time fetch of messages from the topic and fetches\nmessages starting from the start sequence id and reading a max number \nof messages. Note that you will not get back as many messages as max \nspecifies if that amount of messages does not exist. ",
	Run: func(cmd *cobra.Command, args []string) {
		client := kuling.NewLogStoreClient(fetchAddress)

		err := client.Fetch(kuling.NewFetchRequest(topic, int64(startID), int64(maxNumMessages)))

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
		kuling.DefaultCommandAddress,
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
		&group,
		"group",
		"g",
		"",
		"Group that this connecting client shall belong to",
	)

	FetchCmd.PersistentFlags().IntVarP(
		&startID,
		"start-sequence-id",
		"s",
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
