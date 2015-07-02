package kuling

import (
	"bytes"
	"fmt"
	"net"
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

// Ping the server and expect a PONG back! You know...
func (c *Client) Ping() (string, error) {
	cmdWriter := NewClientCommandWriter(c.conn)
	if err := cmdWriter.WriteCommand(PingCmd); err != nil {
		return "", err
	}

	cmdResponseReader := NewClientCommandResponseReader(c.conn)

	resp, err := cmdResponseReader.ReadResponse(PingCmd.ResponseType)
	if err != nil {
		return "", err
	} else if resp.Err != nil {
		return "", resp.Err
	}

	return resp.Msg, err
}

// CreateTopic calls the server and asks it to create topic with given number
// of partitions
func (c *Client) CreateTopic(topic string, numPartitions int64) (string, error) {
	cmdWriter := NewClientCommandWriter(c.conn)
	err := cmdWriter.WriteCommand(CreateTopicCmd, topic, fmt.Sprintf("%d", numPartitions))

	if err != nil {
		return "", err
	}

	cmdResponseReader := NewClientCommandResponseReader(c.conn)

	resp, err := cmdResponseReader.ReadResponse(CreateTopicCmd.ResponseType)
	if err != nil {
		return "", err
	} else if resp.Err != nil {
		return "", resp.Err
	}

	return resp.Msg, nil
}

// Append keyed message into partition of the topic
func (c *Client) Append(topic, partition string, key, message string) (string, error) {
	cmdWriter := NewClientCommandWriter(c.conn)
	err := cmdWriter.WriteCommand(AppendCmd, topic, partition, key, message)

	if err != nil {
		return "", err
	}

	cmdResponseReader := NewClientCommandResponseReader(c.conn)

	resp, err := cmdResponseReader.ReadResponse(PingCmd.ResponseType)
	if err != nil {
		return "", err
	} else if resp.Err != nil {
		return "", resp.Err
	}

	return resp.Msg, nil
}

// Fetch messages from the kuling server on the topic and partition starting
// from specified start id and getting max number of messaages. Note that
// the server have no obligation to return exactly the number of messages
// specified, only that it will never be more.
func (c *Client) Fetch(topic, partition string, startID, maxNumMessages, chunkSize int64) ([]*Message, error) {
	cmdWriter := NewClientCommandWriter(c.conn)
	if err := cmdWriter.WriteCommand(FetchCmd, topic, partition, fmt.Sprintf("%d", startID), fmt.Sprintf("%d", maxNumMessages)); err != nil {
		return nil, err
	}

	cmdResponseReader := NewClientCommandResponseReader(c.conn)

	resp, err := cmdResponseReader.ReadResponse(FetchCmd.ResponseType)
	if err != nil {
		return nil, err
	} else if resp.Err != nil {
		return nil, resp.Err
	}

	msgReader := NewMessageReader(bytes.NewReader(resp.Blob))
	msgs, err := msgReader.ReadMessages()
	if err != nil {
		return nil, err
	}

	return msgs, nil
}
