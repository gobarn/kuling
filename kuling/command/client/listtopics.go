package client

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/fredrikbackstrom/kuling/kuling"
	"github.com/spf13/cobra"
)

// ListTopicsCmd will read from the server
var ListTopicsCmd = &cobra.Command{
	Use:   "list",
	Short: "list",
	Long:  "List all topic names",
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

		topics, err := client.ListTopics()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		for _, t := range topics {
			fmt.Println(t)
		}
	},
}
