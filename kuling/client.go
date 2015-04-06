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
func (c *StreamClient) Fetch(topic, partition string, sequenceID, maxMessages int64) error {
	fmt.Println("Connecting")
	conn, err := net.Dial("tcp4", c.host+":"+strconv.Itoa(c.port))

	if err != nil {
		panic(err)
	}

	defer conn.Close()

	// _, err = conn.Write([]byte("all"))
	// fmt.Println("Write client")
	// if err != nil {
	// 	panic(err)
	// }

	//payload := make([]byte, 1024)
	//fmt.Println("Reading client")
	//_, err = conn.Read(payload)

	var buf bytes.Buffer
	readBytes, err := io.Copy(&buf, conn)

	if err == io.EOF {
		fmt.Println("ALl read")
	} else if err != nil {
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
