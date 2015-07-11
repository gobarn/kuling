package kuling

import (
	"bytes"
	"io"
	"net"
	"os"

	"github.com/fredrikbackstrom/kuling/kuling/resp"
)

// Client client that can access and command a remote log store
type Client struct {
	conn net.Conn
	*resp.Writer
	*resp.Reader
}

// Dial connects to kuling server and returns the client connection
func Dial(address string) (*Client, error) {
	conn, err := net.Dial("tcp4", address)
	if err != nil {
		return nil, err
	}

	// TODO remove multiwriter, just for debug!
	mw := io.MultiWriter(conn, os.Stdout)

	return &Client{
			conn,
			resp.NewWriter(mw),
			resp.NewReader(conn),
		},
		nil
}

// Close connection to the server
func (c *Client) Close() error {
	return c.conn.Close()
}

// Ping the server and expect a PONG back! You know...
func (c *Client) Ping() (string, error) {
	if err := c.WriteArray("PING"); err != nil {
		return "", err
	}

	resp, err := c.Read()
	if err != nil {
		return "", err
	}

	return resp.(string), nil
}

// CreateTopic calls the server and asks it to create topic with given number
// of shards
func (c *Client) CreateTopic(topic string, numShards int64) (string, error) {
	err := c.WriteArray("CREATE_TOPIC", topic, numShards)
	if err != nil {
		return "", err
	}

	resp, err := c.Read()
	if err != nil {
		return "", err
	}

	return resp.(string), nil
}

// ListTopics lists all topic names
func (c *Client) ListTopics() ([]string, error) {
	err := c.WriteArray("LIST_TOPICS")
	if err != nil {
		return nil, err
	}

	resp, err := c.Read()
	if err != nil {
		return nil, err
	}

	result := resp.([]interface{})
	topics := make([]string, len(result))
	for i, topic := range result {
		topics[i] = string(topic.([]byte))
	}

	return topics, nil
}

// ListShards list all shards for a topic
func (c *Client) ListShards(topic string) ([]string, error) {
	err := c.WriteArray("LIST_SHARDS", topic)
	if err != nil {
		return nil, err
	}

	resp, err := c.Read()
	if err != nil {
		return nil, err
	}

	result := resp.([]interface{})
	shards := make([]string, len(result))
	for i, shard := range result {
		shards[i] = string(shard.([]byte))
	}

	return shards, nil
}

// Append keyed message into shard of the topic
func (c *Client) Append(topic, shard string, key, message []byte) (string, error) {
	err := c.WriteArray("APPEND", topic, shard, key, message)
	if err != nil {
		return "", err
	}

	resp, err := c.Read()
	if err != nil {
		return "", err
	}

	return resp.(string), nil
}

// Fetch messages from the kuling server on the topic and shard starting
// from specified start id and getting max number of messaages. Note that
// the server have no obligation to return exactly the number of messages
// specified, only that it will never be more.
func (c *Client) Fetch(topic, shard string, startID, maxNumMessages int64) ([]*Message, error) {
	if err := c.WriteArray("FETCH", topic, shard, startID, maxNumMessages); err != nil {
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
