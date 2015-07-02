package logstore

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/fredrikbackstrom/kuling/kuling"
	"github.com/spf13/cobra"
)

// FetchCmd will read from the server
var PingCmd = &cobra.Command{
	Use:   "ping",
	Short: "Ping",
	Long:  "Ping Pong!",
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

		msg, err := client.Ping()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		fmt.Println(msg)
	},
}

// init sets up flags for the client commands
func bootstrapPing() {
	// host is available for all commands under server
	PingCmd.PersistentFlags().StringVarP(
		&fetchAddress,
		"host",
		"a",
		kuling.DefaultFetchAddress,
		"Host where server is running",
	)
}
