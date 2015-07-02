package logstore

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/fredrikbackstrom/kuling/kuling"
	"github.com/spf13/cobra"
)

// CreateTopicCmd will call the server and ask it to create a topic with
// given number of partitions
var CreateTopicCmd = &cobra.Command{
	Use:   "create-topic",
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

		msg, err := client.CreateTopic(topic, int64(numPartitions))
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		fmt.Println(msg)
	},
}

// init sets up flags for the client commands
func bootstrapCreateTopic() {
	// host is available for all commands under server
	CreateTopicCmd.PersistentFlags().StringVarP(
		&fetchAddress,
		"host",
		"a",
		kuling.DefaultFetchAddress,
		"Host where server is running",
	)

	CreateTopicCmd.PersistentFlags().StringVarP(
		&topic,
		"topic",
		"t",
		"",
		"Name of topic to create",
	)

	CreateTopicCmd.PersistentFlags().IntVarP(
		&numPartitions,
		"num-partitions",
		"n",
		1,
		"The number of partitions the topic shall have",
	)
}
