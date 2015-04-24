package broker

import (
	"log"
	"net"

	"github.com/fredrikbackstrom/kuling/kuling"
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
	// the path where the broker has it's files
	brokerPath string
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
	log.Println("broker: Running")

	lis, err := net.Listen("tcp", listenAddress)

	if err != nil {
		log.Fatalf("broker: Failed to listen: %v", err)
		return
	}

	// Start a new GRPC server and register the broker service.
	server := grpc.NewServer()
	b := broker.NewBroker(brokerPath, logServerAddress)
	broker.RegisterBrokerServer(server, b)
	// Start serving in blocking call
	server.Serve(lis)
}

// init sets up flags for the broker commands
func initBrokerServerCommands() {
	// address where the broker should be running
	BrokerServerCmd.PersistentFlags().StringVarP(
		&listenAddress,
		"listen-address",
		"a",
		kuling.DefaultBrokerAddress,
		"Listen address",
	)

	BrokerServerCmd.PersistentFlags().StringVarP(
		&logServerAddress,
		"log-server-address",
		"l",
		kuling.DefaultCommandAddress,
		"Log Server Address",
	)

	BrokerServerCmd.PersistentFlags().StringVarP(
		&brokerPath,
		"dir",
		"d",
		kuling.DefaultBrokerDir,
		"Broker data directory",
	)
}
