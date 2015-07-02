package logstore

import (
	"bytes"
	"fmt"
	"io"
	"net"
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

		conn, err := net.Dial("tcp4", fetchAddress)
		defer conn.Close()

		if err != nil {
			panic(err)
		}

		cmdWriter := kuling.NewClientCommandWriter(conn)
		err = cmdWriter.WriteCommand(kuling.FetchCmd, topic, shard, fmt.Sprintf("%d", startID), fmt.Sprintf("%d", maxNumMessages))

		if err != nil {
			panic(err)
		}

		cmdResponseReader := kuling.NewClientCommandResponseReader(conn)

		resp, err := cmdResponseReader.ReadResponse(kuling.FetchCmd.ResponseType)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		} else if resp.Err != nil {
			fmt.Println(resp.Err)
			os.Exit(1)
		}

		msgReader := kuling.NewMessageReader(bytes.NewReader(resp.Blob))
		msgs, err := msgReader.ReadMessages()
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
