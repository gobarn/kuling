package client

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/fredrikbackstrom/kuling/kuling"
	"github.com/spf13/cobra"
)

var itersCmd = &cobra.Command{
	Use:   "iters",
	Short: "iters",
	Long:  "Get Iterators",
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

		c, err := kuling.Dial(fetchAddress)
		defer c.Close()
		if err != nil {
			log.Println(err)
			os.Exit(0)
		}

		iters, err := c.Iters(group, client, topic)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		for _, i := range iters {
			fmt.Println(i)
		}
	},
}

func bootstrapIters() {
	itersCmd.PersistentFlags().StringVarP(
		&group,
		"group",
		"g",
		"",
		"Group that the client iterator belongs to",
	)

	itersCmd.PersistentFlags().StringVarP(
		&client,
		"client",
		"c",
		"",
		"Client identifier",
	)

	itersCmd.PersistentFlags().StringVarP(
		&topic,
		"topic",
		"t",
		"",
		"Topic to stream messages from",
	)
}
