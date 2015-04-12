package broker

import (
	"fmt"
	"log"
	"net"

	"github.com/fredrikbackstrom/kuling/kuling/broker"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

// Flag variables
var (
	// Listen address for the broker server
	listenAddress string
	// Log Servers address where the log server is running. For now only one.
	logServerAddress string
)

// Server Command will run the broker server
var BrokerServerCmd = &cobra.Command{
	Use:   "server",
	Short: "Run Broker Server",
	Long:  "Run Broker Server",
	Run: func(cmd *cobra.Command, args []string) {
		runBroker()
	},
}

func runBroker() {
	// Create a listener at provided address
	fmt.Println("Running broker")
	lis, err := net.Listen("tcp", listenAddress)

	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
		return
	}

	// Start a new GRPC server and register the broker service.
	server := grpc.NewServer()
	broker.RegisterBrokerServer(server, broker.NewBroker())
	// Start serving.
	server.Serve(lis)
}

// init sets up flags for the broker commands
func initBrokerServerCommands() {
	// address where the broker should be running
	BrokerServerCmd.PersistentFlags().StringVarP(
		&listenAddress,
		"listen-address",
		"a",
		"localhost:8888",
		"Listen address",
	)

	BrokerServerCmd.PersistentFlags().StringVarP(
		&logServerAddress,
		"log-server-address",
		"l",
		"localhost:9999",
		"Log Server Address",
	)
}
