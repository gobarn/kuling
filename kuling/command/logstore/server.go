package logstore

import (
	"log"
	"os"
	"os/signal"

	"github.com/fredrikbackstrom/kuling/kuling"
	"github.com/spf13/cobra"
)

// Flag variables
var (
	// Listen address for the log server
	listenAddress string
	// Admin listen address for RPC commands
	commandAddress string
	// data directory for the log store
	dataDir string
)

// Server Command will run a http server
var ServerCmd = &cobra.Command{
	Use:   "server",
	Short: "Execute operation on Kuling Server",
	Long:  "Execute operation on Kuling Server",
	Run: func(cmd *cobra.Command, args []string) {
		// Open the log store
		logStore := kuling.OpenLogStore(dataDir, 0700, 0600)

		// TEMP writes to get some data
		for i := 0; i < 50000; i++ {
			err := logStore.Write("emails", "part_01", []byte("john@doe.com"), []byte("Has all the stuff"))

			if err != nil {
				panic(err)
			}
		}

		// Run the server in a new go routine
		go runServer(logStore)
		go runRPCCommandServer(logStore)

		// All Traits have been successfully started, now block on the caller
		osSignals := make(chan os.Signal, 1)
		signal.Notify(osSignals, os.Interrupt)
		for {
			select {
			case sig := <-osSignals:
				if sig == os.Interrupt {
					// Received Interrupt Signal. Stop the scheduler, workers and then shut down.
					log.Println("command: Received exit signal, stopping server...")
					// Stop log store
					logStore.Close()
					// Wait for the log store to close down
					for {
						select {
						case <-logStore.Closed():
							log.Println("command: Closed")
							os.Exit(0)
						}
					}
				}
			}
		}
	},
}

func runServer(logStore kuling.LogStore) {
	// Create a new log server and run it
	logServer := kuling.NewLogServer(listenAddress, logStore)
	// Create a new RPC server and run it

	// Run in a blocking call
	logServer.ListenAndServe()
}

func runRPCCommandServer(logStore kuling.LogStore) {
	rpcServer := kuling.NewRPCCommandServer(logStore)
	rpcServer.ListenAndServe(commandAddress)
}

// init sets up flags for the server commands
func bootstrapServer() {
	// host is available for all commands under server
	ServerCmd.PersistentFlags().StringVarP(
		&listenAddress,
		"address",
		"a",
		kuling.DefaultFetchAddress,
		"Listen address for LogStore Server",
	)

	ServerCmd.PersistentFlags().StringVarP(
		&commandAddress,
		"command-address",
		"c",
		kuling.DefaultCommandAddress,
		"Listen address for Command RPC Server",
	)

	ServerCmd.PersistentFlags().StringVarP(
		&dataDir,
		"data-dir",
		"d",
		"/tmp/kuling",
		"Data directory for Kuling persisten storage",
	)
}
