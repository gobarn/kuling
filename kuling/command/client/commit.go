package client

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/fredrikbackstrom/kuling/kuling"
	"github.com/spf13/cobra"
)

var commitCmd = &cobra.Command{
	Use:   "commit",
	Short: "commit",
	Long:  "Commit iterator",
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

		msg, err := c.Commit(iter, int64(startID))
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		fmt.Println(msg)
	},
}

func bootstrapCommit() {
	commitCmd.PersistentFlags().StringVarP(
		&iter,
		"iter",
		"i",
		"",
		"Iterator to commit",
	)

	commitCmd.PersistentFlags().IntVarP(
		&startID,
		"offset-sequence-id",
		"o",
		0,
		"Sequence ID to start reading messages from",
	)
}
