package server

import (
	"fmt"

	"github.com/fredrikbackstrom/kuling/kuling"
	"github.com/spf13/cobra"
)

// Flag variables
var (
	host    string
	port    int
	dataDir string
)

// Server Command will run a http server
var ServerCmd = &cobra.Command{
	Use:   "server",
	Short: "Execute operation on Kuling Server",
	Long:  "Execute operation on Kuling Server",
	Run: func(cmd *cobra.Command, args []string) {
		runServer()
	},
}

func runServer() {
	// Open the Kuling log store
	db := kuling.OpenLogStore(dataDir, 0700, 0600)
	defer db.Close()
	// TEMP, REMOVE
	go func() {
		for {
			select {
			case <-db.Closed():
				fmt.Println("Closed")
			}
		}
	}()

	for i := 0; i < 5000; i++ {
		writePayment(db)
	}

	// Create a new stream server and run it
	s := kuling.NewStreamServer(host, port, db)
	// Run in a blocking call
	s.ListenAndServe()
}

func writePayment(db kuling.LogStore) {
	err := db.Write("emails", "part_01", []byte("john@doe.com"), []byte("Has all the stuff"))

	if err != nil {
		panic(err)
	}
}

// init sets up flags for the server commands
func init() {
	// host is available for all commands under server
	ServerCmd.PersistentFlags().StringVarP(
		&host,
		"host",
		"a",
		"localhost",
		"host to serve from",
	)

	// port is available for all commands under server
	ServerCmd.PersistentFlags().IntVarP(
		&port,
		"port",
		"p",
		9999,
		"Port to serve from",
	)

	ServerCmd.PersistentFlags().StringVarP(
		&dataDir,
		"data-dir",
		"d",
		"/tmp/kuling",
		"Data directory for Kuling persisten storage",
	)
}
