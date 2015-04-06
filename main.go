package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"

	"github.com/fredrikbackstrom/eventing/server/kuling"
)

var (
	dataDir    string
	brokerPort int
	streamPort int
)

func init() {
	flag.StringVar(&dataDir, "data-dir", "/tmp/kuling", "Data directory")
	flag.IntVar(&brokerPort, "broker-port", 9090, "Broker port")
	flag.IntVar(&streamPort, "stream-port", 9095, "Stream port")
}

func main() {
	if len(os.Args) == 1 {
		panic("No action given")
	}

	// Check if we are starting a server or a client
	action := os.Args[1]

	if action == "server" {
		// Open the Kuling log store
		db := kuling.Open(dataDir)
		defer db.Close()
		// TEMP, REMOVE
		writePayment(db)
		// Create a new stream server and run it
		s := kuling.NewStreamServer("localhost", streamPort, db)
		// Run in a blocking call
		s.ListenAndServe()

	} else if action == "client" {
		c := kuling.NewStreamClient("localhost", streamPort)

		c.Fetch("payments", "part_01", 0, 2)
	} else {
		panic("unknown action " + action)
	}
}

func writePayment(db *kuling.LogStore) {
	err := db.Write("payments", "part_01", []byte("apa@izettle.com"), []byte("My own payment"))

	if err != nil {
		panic(err)
	}
}

func runServer(db *kuling.LogStore) {
	// Write message to it
	err := db.Write("payments", "part_01", []byte("apa@izettle.com"), []byte("My own payment"))

	if err != nil {
		panic(err)
	}

	fmt.Println("READING")

	messages, err := db.Read("payments", 0, 10)

	for _, m := range messages {
		fmt.Println(string(m.Key) + " " + string(m.Payload))
	}

	copyTo, _ := os.OpenFile("/tmp/copy.txt", os.O_RDWR|os.O_CREATE, 0644)
	defer copyTo.Close()

	_, err = db.Copy("payments", 0, 2, copyTo)

	if err != nil {
		panic(err)
	}

	// Test read the copy file
	copyTo.Seek(0, 0)
	r := bufio.NewReader(copyTo)
	copiedM, err := kuling.ReadMessages(r)

	for _, m := range copiedM {
		fmt.Println("Copied: " + string(m.Key) + " " + string(m.Payload))
	}
}
