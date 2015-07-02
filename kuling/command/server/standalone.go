package server

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

// Server Command will run server on one machine
var StandaloneServerCmd = &cobra.Command{
	Use:   "standalone",
	Short: "Start standalone server",
	Long:  "Start standalone server",
	Run: func(cmd *cobra.Command, args []string) {
		// Create global config
		c := &kuling.FSConfig{
			0755,
			0655,
			1024 * 1000 * 10, // 10MB
		}
		// Open the log store
		logStore := kuling.OpenFSTopicLogStore(dataDir, c)

		// CREATE TEMP TOPIC
		//if err := logStore.CreateTopic("emails", 10); err != nil {
		//			log.Fatal(err)
		//	}

		// TEMP writes to get some data
		//for i := 0; i < 100000; i++ {
		//err := logStore.Append("emails", "0", []byte(fmt.Sprintf("john@doe.com_%d", i)), []byte("Has all the stuff"))

		// if err != nil {
		// 	panic(err)
		// }
		//}

		// Run the server in a new go routine
		go runServer(logStore)

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
	logServer := kuling.NewStandaloneServer(listenAddress, logStore)
	// Create a new RPC server and run it

	// Run in a blocking call
	logServer.ListenAndServe()
}

// init sets up flags for the server commands
func bootstrapServer() {
	// host is available for all commands under server
	StandaloneServerCmd.PersistentFlags().StringVarP(
		&listenAddress,
		"address",
		"a",
		kuling.DefaultFetchAddress,
		"Listen address for LogStore Server",
	)

	StandaloneServerCmd.PersistentFlags().StringVarP(
		&commandAddress,
		"command-address",
		"c",
		kuling.DefaultCommandAddress,
		"Listen address for Command RPC Server",
	)

	StandaloneServerCmd.PersistentFlags().StringVarP(
		&dataDir,
		"data-dir",
		"d",
		"/tmp/kuling",
		"Data directory for Kuling persisten storage",
	)
}
