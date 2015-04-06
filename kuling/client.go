package kuling

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"strconv"
)

// StreamClient a
type StreamClient struct {
	host string
	port int
}

// NewStreamClient Create new stream client
func NewStreamClient(host string, port int) *StreamClient {
	return &StreamClient{host, port}
}

// Fetch a batch of messages from the topic and partition as well
func (c *StreamClient) Fetch(topic, partition string, startSequenceID, maxNumMessages int64) error {
	address := net.JoinHostPort(c.host, strconv.Itoa(c.port))
	fmt.Println("Connecting to " + address)
	conn, err := net.Dial("tcp4", address)

	if err != nil {
		// Could not connecto to address
		panic(err)
	}

	// Make sure to close the connection after all is done
	defer conn.Close()

	// Send fetch request to server
	writeFetcRequest(topic, startSequenceID, maxNumMessages, conn)

	var buf bytes.Buffer
	readBytes, err := io.Copy(&buf, conn)

	if err == io.EOF {
		// All OK, we have reached the end of the byte stream
		fmt.Println("ALl read")
	} else if err != nil {
		// Something went wrong when reading from the server
		panic(err)
	}

	//buffer := bytes.NewBuffer(make([]byte, 0))
	r := bufio.NewReader(&buf)
	copiedM, err := ReadMessages(r)

	fmt.Println("Reading all messages")

	fmt.Println(readBytes)

	for _, m := range copiedM {
		fmt.Println(string(m.Key) + " " + string(m.Payload))
	}

	return nil
}
