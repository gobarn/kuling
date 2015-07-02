package logstore

import (
	"fmt"
	"io"
	"os"

	"github.com/fredrikbackstrom/kuling/kuling"
	"github.com/spf13/cobra"
)

// AppendCmd will read from the server
var AppendCmd = &cobra.Command{
	Use:   "append",
	Short: "Append Message",
	Long:  "Append message to topic and shard",
	Run: func(cmd *cobra.Command, args []string) {
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
			fmt.Println(err)
			os.Exit(1)
		}

		msg, err := client.Append(topic, shard, key, message)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		fmt.Println(msg)
		os.Exit(0)
	},
}

// init sets up flags for the client commands
func bootstrapAppend() {
	// host is available for all commands under server
	AppendCmd.PersistentFlags().StringVarP(
		&fetchAddress,
		"host",
		"a",
		kuling.DefaultFetchAddress,
		"Host where server is running",
	)

	AppendCmd.PersistentFlags().StringVarP(
		&topic,
		"topic",
		"t",
		"",
		"Topic to stream messages from",
	)

	AppendCmd.PersistentFlags().StringVarP(
		&shard,
		"shard",
		"s",
		"",
		"Shard in the stream to read from",
	)

	AppendCmd.PersistentFlags().StringVarP(
		&key,
		"key",
		"k",
		"",
		"Key of message",
	)

	AppendCmd.PersistentFlags().StringVarP(
		&message,
		"message",
		"m",
		"",
		"Message to append",
	)
}
