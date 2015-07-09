package client

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/fredrikbackstrom/kuling/kuling"
	"github.com/spf13/cobra"
)

// FetchCmd will read from the server
var FetchCmd = &cobra.Command{
	Use:   "fetch",
	Short: "Fetch messages",
	Long:  "Performs a one time fetch of messages from the topic and fetches\nmessages starting from the start sequence id and reading a max number \nof messages. Note that you will not get back as many messages as max \nspecifies if that amount of messages does not exist. ",
	Run: func(cmd *cobra.Command, args []string) {
		// TODO move this out to some help function for commands calling the server
		defer func() {
			if r := recover(); r != nil {
				if r == io.EOF {
					fmt.Println("Connection closed before reading response")
					os.Exit(1)
				} else {
					fmt.Printf("Recovered from panic %v\n", r)
				}
			}
		}()

		client, err := kuling.Dial(fetchAddress)
		defer client.Close()
		if err != nil {
			log.Println(err)
			os.Exit(0)
		}

		msgs, err := client.Fetch(topic, shard, int64(startID), int64(maxNumMessages))
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		for _, m := range msgs {
			fmt.Printf("key: %s payload: %s\n", string(m.Key), string(m.Payload))
		}
	},
}

// init sets up flags for the client commands
func bootstrapFetch() {

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
