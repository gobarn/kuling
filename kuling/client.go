package kuling

import (
	"bytes"
	"io"
	"net"
	"os"
)

// Client client that can access and command a remote log store
type Client struct {
	conn net.Conn
	*CommandWriter
	*Reader
}

// Dial connects to kuling server and returns the client connection
func Dial(address string) (*Client, error) {
	conn, err := net.Dial("tcp4", address)
	if err != nil {
		return nil, err
	}

	mr := io.MultiWriter(conn, os.Stdout)

	return &Client{
			conn,
			NewCommandWriter(mr),
			NewReader(conn),
		},
		nil
}

// Close connection to the server
func (c *Client) Close() error {
	return c.conn.Close()
}

// Ping the server and expect a PONG back! You know...
func (c *Client) Ping() (string, error) {
	if err := c.WriteCommand("PING"); err != nil {
		return "", err
	}

	resp, err := c.Read()
	if err != nil {
		return "", err
	}

	return resp.(string), nil
}

// CreateTopic calls the server and asks it to create topic with given number
// of partitions
func (c *Client) CreateTopic(topic string, numPartitions int64) (string, error) {
	err := c.WriteCommand("CREATE_TOPIC", topic, numPartitions)
	if err != nil {
		return "", err
	}

	resp, err := c.Read()
	if err != nil {
		return "", err
	}

	return resp.(string), nil
}

// Append keyed message into partition of the topic
func (c *Client) Append(topic, partition string, key, message []byte) (string, error) {
	err := c.WriteCommand("APPEND", topic, partition, key, message)
	if err != nil {
		return "", err
	}

	resp, err := c.Read()
	if err != nil {
		return "", err
	}

	return resp.(string), nil
}

// Fetch messages from the kuling server on the topic and partition starting
// from specified start id and getting max number of messaages. Note that
// the server have no obligation to return exactly the number of messages
// specified, only that it will never be more.
func (c *Client) Fetch(topic, partition string, startID, maxNumMessages, chunkSize int64) ([]*Message, error) {
	if err := c.WriteCommand("FETCH", topic, partition, startID, maxNumMessages); err != nil {
		return nil, err
	}

	resp, err := c.Read()
	if err != nil {
		return nil, err
	}

	msgReader := NewMessageReader(bytes.NewReader(resp.([]byte)))
	msgs, err := msgReader.ReadMessages()
	if err != nil {
		return nil, err
	}

	return msgs, nil
}
