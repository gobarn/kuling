package server

import (
	"log"
	"os"
	"os/signal"
	"path"

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

		c := &kuling.Config{
			PermDirectories: 0755,
			PermData:        0655,
			SegmentMaxBytes: 1024 * 1000 * 10, // 10MB
		}

		logStore, err := kuling.OpenLogStore(dataDir, c)

		iterStore := kuling.OpenBoltIterStore(path.Join(dataDir, "broker.db"), c)
		broker := kuling.NewBroker(logStore, iterStore)

		if err != nil {
			log.Printf("standalone: could not start server: %s\n", err)
			os.Exit(1)
		}

		if _, err := logStore.CreateTopic("emails", 10); err != nil {
			// log.Fatal(err)
		}

		// for i := 0; i < 500000; i++ {
		// 	err := logStore.Append("emails", "0", []byte(fmt.Sprintf("john@doe.com_%d", i)), []byte("Has all the stuff"))
		//
		// 	if err != nil {
		// 		panic(err)
		// 	}
		// }

		// Run the server in a new go routine
		go runServer(logStore, broker)

		// All Traits have been successfully started, now block on the caller
		osSignals := make(chan os.Signal, 1)
		signal.Notify(osSignals, os.Interrupt)
		for {
			select {
			case sig := <-osSignals:
				if sig == os.Interrupt {
					// Received Interrupt Signal. Stop the scheduler, workers and then shut down.
					log.Println("standalone: received exit signal, stopping server...")
					// Stop log store
					logStore.Close()
					// Wait for the log store to close down
					for {
						select {
						case <-logStore.Closed():
							log.Println("standalone: closed")
							os.Exit(0)
						}
					}
				}
			}
		}
	},
}

func runServer(logStore *kuling.LogStore, broker *kuling.Broker) {
	// Create a new log server and run it
	kuling.ListenAndServeStandalone(listenAddress, logStore, broker)
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
		&dataDir,
		"data-dir",
		"d",
		"/tmp/kuling",
		"Data directory for Kuling persisten storage",
	)
}
