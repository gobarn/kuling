package client

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/fredrikbackstrom/kuling/kuling"
	"github.com/spf13/cobra"
)

var createCmd = &cobra.Command{
	Use:   "create",
	Short: "Create Topic",
	Long:  "Create Topic",
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

		msg, err := client.Create(topic, int64(numShards))
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		fmt.Println(msg)
	},
}

func bootstrapCreate() {
	createCmd.PersistentFlags().StringVarP(
		&topic,
		"topic",
		"t",
		"",
		"Name of topic to create",
	)

	createCmd.PersistentFlags().IntVarP(
		&numShards,
		"num-shards",
		"n",
		1,
		"The number of shards the topic shall have",
	)
}
