package kuling

import (
	"bytes"
	"fmt"
	"net"

	"github.com/fredrikbackstrom/kuling/kuling"
)

// Client client that can access and command a remote log store
type Client struct {
	conn net.Conn
}

// Dial connects to kuling server and returns the client connection
func Dial(address string) (*Client, error) {
	conn, err := net.Dial("tcp4", address)
	if err != nil {
		return nil, err
	}

	return &Client{conn}, nil
}

// Close connection to the server
func (c *Client) Close() error {
	return c.conn.Close()
}

// Fetch messages from the kuling server on the topic and partition starting
// from specified start id and getting max number of messaages. Note that
// the server have no obligation to return exactly the number of messages
// specified, only that it will never be more.
func (c *Client) Fetch(topic, partition string, startID, maxNumMessages, chunkSize int64) ([]*Message, error) {
	cmdWriter := kuling.NewClientCommandWriter(c.conn)
	if err = cmdWriter.WriteCommand(kuling.FetchCmd, topic, partition, fmt.Sprintf("%d", startID), fmt.Sprintf("%d", maxNumMessages)); err != nil {
		return err, nil
	}

	cmdResponseReader := kuling.NewClientCommandResponseReader(c.conn)

	resp, err := cmdResponseReader.ReadResponse(kuling.FetchCmd.ResponseType)
	if err != nil {
		return err, nil
	} else if resp.Err != nil {
		return resp.Err, nil
	}

	msgReader := kuling.NewMessageReader(bytes.NewReader(resp.Blob))
	msgs, err := msgReader.ReadMessages()
	if err != nil {
		return err, nil
	}

	return nil, msgs
}
