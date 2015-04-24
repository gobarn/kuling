package kuling

import (
	"bufio"
	"encoding/binary"
	"io"
)

// Helper methods for writing the binary format of a client issued
// fetch request and a server provided fetch request response.
//
// The binary format for the fetch request:
//
// Length: what
// int32: action
// int64: startSequenceID
// int64: maxMessagesToRead
// int32: len topic
// variable: topic
//
// The binary format for the fetch request response:
//
// int32: status
// variable: payload
//

// FetchRequest request from a client to the server that contain the topic to fetch
// messages from and a start message to read from as well as the max number
// of messages to get back.
type FetchRequest struct {
	Topic           string
	Shard           string
	StartSequenceID int64
	MaxNumMessages  int64
}

// FetchRequestWriter struct that knows how to write fetch requests
type FetchRequestWriter struct {
	*RequestHeaderWriter
	*bufio.Writer
}

// NewFetchRequest creates a new fetch request from the given information
func NewFetchRequest(topic, shard string, startSequenceID, maxNumMessages int64) *FetchRequest {
	return &FetchRequest{topic, shard, startSequenceID, maxNumMessages}
}

// NewFetchRequestWriter creates and returns a fetch request writer that will
// write the binary fetch message to the writer
func NewFetchRequestWriter(w io.Writer) *FetchRequestWriter {
	buf := bufio.NewWriter(w)
	return &FetchRequestWriter{NewRequestHeaderWriter(buf), buf}
}

// WriteFetchRequest writes the binary representation of the fetch request
// to the io writer
func (frw *FetchRequestWriter) WriteFetchRequest(topic, shard string, startSequenceID, maxNumMessages int64) RequestError {
	// Write action
	frw.WriteHeader(ActionFetch)
	// Write startSequenceID
	binary.Write(frw, binary.BigEndian, startSequenceID)
	// Write maxNumMessages
	binary.Write(frw, binary.BigEndian, maxNumMessages)
	// Write topic length
	topicBytes := []byte(topic)
	binary.Write(frw, binary.BigEndian, int32(len(topicBytes)))
	// Write payload
	frw.Write(topicBytes)
	// Write shard length
	shardBytes := []byte(shard)
	binary.Write(frw, binary.BigEndian, int32(len(shardBytes)))
	// Write shard
	frw.Write(shardBytes)

	// Write to the underlying io Writer
	err := frw.Flush()

	if err != nil {
		// If we could not write the action then return back
		return &RequestReadError{1, err.Error()}
	}

	return nil
}

// FetchRequestReader reads fetch request from underlying reader
type FetchRequestReader struct {
	*bufio.Reader
}

// NewFetchRequestReader creates a new fetch request reader that wrapps
// the io.Reader
func NewFetchRequestReader(r io.Reader) *FetchRequestReader {
	return &FetchRequestReader{bufio.NewReader(r)}
}

// ReadFetchRequest from reader and return the fetch request
func (r *FetchRequestReader) ReadFetchRequest() (*FetchRequest, RequestError) {
	// Fetch request, continue with reading the topic, start sequence ID and
	// max num messages
	var startSequenceID int64
	err := binary.Read(r, binary.BigEndian, &startSequenceID) // Reads 8

	if err != nil {
		// Write back that we could not read the start sequence ID
		return nil, &RequestReadError{ReqErrSequenceID, err.Error()}
	}

	var maxNumMessages int64
	err = binary.Read(r, binary.BigEndian, &maxNumMessages) // Reads 8

	if err != nil {
		// Write back that we could not read max num messages
		return nil, &RequestReadError{ReqErrMaxNumMessage, err.Error()}
	}

	var topicLength int32
	err = binary.Read(r, binary.BigEndian, &topicLength) // Reads 4

	if err != nil {
		// Write back that we could not read topic length
		return nil, &RequestReadError{ReqErrTopicLength, err.Error()}
	}

	topic := make([]byte, topicLength) // Reads len payload
	_, err = r.Read(topic)

	if err != nil {
		// Write back that we could not read the topic
		return nil, &RequestReadError{ReqErrTopic, err.Error()}
	}

	var shardLength int32
	err = binary.Read(r, binary.BigEndian, &shardLength) // Reads 4

	if err != nil {
		// Write back that we could not read topic length
		return nil, &RequestReadError{ReqErrShardLength, err.Error()}
	}

	shard := make([]byte, shardLength) // Reads len payload
	_, err = r.Read(shard)

	if err != nil {
		// Write back that we could not read the topic
		return nil, &RequestReadError{ReqErrShard, err.Error()}
	}

	// Success
	return &FetchRequest{
			string(topic),
			string(shard),
			startSequenceID,
			maxNumMessages,
		},
		nil
}
