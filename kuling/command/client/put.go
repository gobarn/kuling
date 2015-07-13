package client

import (
	"fmt"
	"io"
	"os"

	"github.com/fredrikbackstrom/kuling/kuling"
	"github.com/spf13/cobra"
)

var putCmd = &cobra.Command{
	Use:   "put",
	Short: "Put Message",
	Long:  "Put message to topic",
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

		msg, err := client.Put(topic, shard, []byte(key), []byte(message))
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		fmt.Println(msg)
		os.Exit(0)
	},
}

// init sets up flags for the client commands
func bootstrapPut() {

	putCmd.PersistentFlags().StringVarP(
		&topic,
		"topic",
		"t",
		"",
		"Topic to stream messages from",
	)

	putCmd.PersistentFlags().StringVarP(
		&shard,
		"shard",
		"s",
		"",
		"Shard in the stream to read from",
	)

	putCmd.PersistentFlags().StringVarP(
		&key,
		"key",
		"k",
		"",
		"Key of message",
	)

	putCmd.PersistentFlags().StringVarP(
		&message,
		"message",
		"m",
		"",
		"Message to append",
	)
}
