package kuling

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
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
func (c *StreamClient) Fetch(topic string, startSequenceID, maxNumMessages int64) error {
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

	// The first part of the response contains the status integer that tells us
	// if the request was OK.
	var status int32
	err = binary.Read(conn, binary.BigEndian, &status) // Reads 8
	if err != nil {
		// This means that we could not read the status integer in the response
		// either something is wrong with server or the internet failed to send us
		// the bytes, anyway we cannot continue
		return errors.New("Bad response from server, could not read status")
	}

	// If the status is not 200 then we do not have a OK response from the
	// server
	if status != 200 {
		return errors.New("Server responded with status: " + strconv.Itoa(int(status)))
	}

	// Copy the rest of the sent bytes into the buffer and parse.
	var buf bytes.Buffer
	readBytes, err := io.Copy(&buf, conn)

	if err == io.EOF {
		// All OK, we have reached the end of the byte stream
		fmt.Println("ALl read")
	} else if err != nil {
		// Something went wrong when reading from the server
		panic(err)
	}

	r := bufio.NewReader(&buf)
	copiedM, err := ReadMessages(r)

	fmt.Println("Reading all messages")

	fmt.Println(readBytes)

	for _, m := range copiedM {
		fmt.Println(string(m.Key) + " " + string(m.Payload))
	}

	return nil
}
