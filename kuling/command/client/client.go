package client

import (
	"fmt"

	"github.com/fredrikbackstrom/kuling/kuling"
	"github.com/spf13/cobra"
)

// Flag variables
var (
	host           string
	port           int
	topic          string
	group          string
	startID        int
	maxNumMessages int
)

// Client Command will read from the server
var ClientCmd = &cobra.Command{
	Use:   "fetch",
	Short: "Fetch messages on topic",
	Long:  "Performs a one time fetch of messages from the topic and fetches\nmessages starting from the start sequence id and reading a max number \nof messages. Note that you will not get back as many messages as max \nspecifies if that amount of messages does not exist. ",
	Run: func(cmd *cobra.Command, args []string) {
		streamFrom()
	},
}

func streamFrom() {
	c := kuling.NewStreamClient(host, port)

	err := c.Fetch(kuling.NewFetchRequest(topic, int64(startID), int64(maxNumMessages)))

	if err != nil {
		fmt.Println(err)
	}
}

// init sets up flags for the client commands
func init() {
	// host is available for all commands under server
	ClientCmd.PersistentFlags().StringVarP(
		&host,
		"host",
		"a",
		"localhost",
		"Host where server is running",
	)

	// port is available for all commands under server
	ClientCmd.PersistentFlags().IntVarP(
		&port,
		"port",
		"p",
		9999,
		"Port that server serves from",
	)

	ClientCmd.PersistentFlags().StringVarP(
		&topic,
		"topic",
		"t",
		"",
		"Topic to stream messages from",
	)

	ClientCmd.PersistentFlags().StringVarP(
		&group,
		"group",
		"g",
		"",
		"Group that this connecting client shall belong to",
	)

	ClientCmd.PersistentFlags().IntVarP(
		&startID,
		"start-sequence-id",
		"s",
		0,
		"Sequence ID to start reading messages from",
	)

	ClientCmd.PersistentFlags().IntVarP(
		&maxNumMessages,
		"max-num-messages",
		"m",
		1,
		"Maximum messages to receive back",
	)
}
