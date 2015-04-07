package kuling

import (
	"bytes"
	"encoding/binary"
	"fmt"
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

const (
	// ReqFetch Action number for fetch request from the client
	ReqFetch = 200
	// StatusErrSequenceID indicates that the start sequence number could not be read
	StatusErrSequenceID = 205
)

// FetchRequest request from a client to the server that contain the topic to fetch
// messages from and a start message to read from as well as the max number
// of messages to get back.
type FetchRequest struct {
	Topic           string
	StartSequenceID int64
	MaxNumMessages  int64
}

// NewFetchRequest creates a new fetch request from the given information
func NewFetchRequest(topic string, startSequenceID, maxNumMessages int64) *FetchRequest {
	return &FetchRequest{topic, startSequenceID, maxNumMessages}
}

// NewFetchRequestFromReader creates a new fetch request read from a io.Reader
func NewFetchRequestFromReader(r io.Reader) (*FetchRequest, RequestError) {
	// Fetch request, continue with reading the topic, start sequence ID and
	// max num messages
	var startSequenceID int64
	err := binary.Read(r, binary.BigEndian, &startSequenceID) // Reads 8

	if err != nil {
		// Write back that we could not read the start sequence ID
		return nil, &RequestReadError{405, err.Error()}
	}

	fmt.Printf("Start %d\n", startSequenceID)

	var maxNumMessages int64
	err = binary.Read(r, binary.BigEndian, &maxNumMessages) // Reads 8

	if err != nil {
		// Write back that we could not read max num messages
		return nil, &RequestReadError{406, err.Error()}
	}

	fmt.Printf("Max %d\n", maxNumMessages)

	var topicLength int32
	err = binary.Read(r, binary.BigEndian, &topicLength) // Reads 4

	if err != nil {
		// Write back that we could not read topic length
		return nil, &RequestReadError{407, err.Error()}
	}

	fmt.Printf("TL %d\n", topicLength)

	topic := make([]byte, topicLength) // Reads len payload
	_, err = r.Read(topic)

	if err != nil {
		// Write back that we could not read the topic
		return nil, &RequestReadError{408, err.Error()}
	}

	fmt.Printf("Topic %s\n", topic)

	// Success
	return &FetchRequest{string(topic), startSequenceID, maxNumMessages}, nil
}

// WriteFetcRequest writes a fetch request to binary format to the io.Writer
func (fr *FetchRequest) WriteFetcRequest(w io.Writer) (int64, RequestError) {
	buffer := bytes.NewBuffer(make([]byte, 0))
	// Write action
	writeRequestAction(ReqFetch, w)
	// Write startSequenceID
	binary.Write(buffer, binary.BigEndian, fr.StartSequenceID)
	// Write maxNumMessages
	binary.Write(buffer, binary.BigEndian, fr.MaxNumMessages)
	// Write topic length
	topicBytes := []byte(fr.Topic)
	binary.Write(buffer, binary.BigEndian, int32(len(topicBytes)))
	// Write payload
	buffer.Write(topicBytes)

	bytesWritten, err := w.Write(buffer.Bytes())

	if err != nil {
		// If we could not write the action then return back
		return int64(bytesWritten), &RequestReadError{1, err.Error()}
	}

	return int64(bytesWritten), nil
}

// WriteFetchRequestResponse writes all the requested messages to the io writer
func (fr *FetchRequest) WriteFetchRequestResponse(ls *LogStore, r io.Reader, w io.Writer) (int64, ResponseError) {
	// Ask log store to copy the requested messages onto the io.Writer
	writeStatusResponse(200, w)
	numCopied, err := ls.Copy(string(fr.Topic), fr.StartSequenceID, fr.MaxNumMessages, w)

	if err != nil {
		// Let client know that we could not read from the DB.
		return numCopied, &ResponseWriteError{ReqFetch, 408, err.Error()}
	}

	fmt.Printf("Num copied %d\n", numCopied)

	return numCopied, nil
}
