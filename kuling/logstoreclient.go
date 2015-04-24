package kuling

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"
)

// LogStoreClient client that can access and command a remote log store
type LogStoreClient struct {
	dialAddress string
}

// NewLogStoreClient Create new stream client
func NewLogStoreClient(address string) *LogStoreClient {
	return &LogStoreClient{address}
}

// Fetch a batch of messages from the topic and partition as well
func (c *LogStoreClient) Fetch(fr *FetchRequest) error {
	start := time.Now()

	fmt.Println("client: Connecting to " + c.dialAddress)

	// Dial the fetch server
	conn, err := net.Dial("tcp4", c.dialAddress)

	if err != nil {
		// Could not connecto to address
		fmt.Println("client: Error connecting", err)
	}

	// Make sure to close the connection after all is done
	defer conn.Close()

	// Send fetch request to server
	fetchReqWriter := NewFetchRequestWriter(conn)
	err = fetchReqWriter.WriteFetchRequest(fr.Topic, fr.StartSequenceID, fr.MaxNumMessages)

	if err != nil {
		// Could not write fetch request
		fmt.Println("client: Error writing request", err)
	}

	// Flush the action to the connection
	fetchReqWriter.Flush()

	// The first part of the response contains the status integer that tells us
	// if the request was OK.
	var status int32
	err = binary.Read(conn, binary.BigEndian, &status) // Reads 8
	if err != nil {
		// This means that we could not read the status integer in the response
		// either something is wrong with server or the internet failed to send us
		// the bytes, anyway we cannot continue.
		return errors.New("Bad response from server, could not read status")
	}

	// If the status is not 200 then we do not have a OK response from the
	// server
	if status != 200 {
		return errors.New("Server responded with status: " + strconv.Itoa(int(status)))
	}

	copiedM, err := ReadMessages(conn)

	fmt.Println("Reading all messages")

	for _, m := range copiedM {
		fmt.Println(string(m.Key) + " " + string(m.Payload))
	}

	fmt.Println(time.Since(start))

	return nil
}
