package kuling

import (
	"encoding/binary"
	"errors"
	"io"
)

// Generic request constants that are usde by multiple requests and responses
const (
	// ActionWrite write a entry to the log store
	ActionWrite = 100
	// ActionFetch Action number for fetch request from the client
	ActionFetch = 200
	// StatusErrSequenceID indicates that the start sequence number could not be read
	StatusErrSequenceID = 205
	// StatusSuccess signals that the request was successfully handled by server
	StatusSuccess = 200
)

// ErrBadResponseFromServer Signals that the byte sent back from the client
// could not be read as they were expected to be read.
var ErrBadResponseFromServer = errors.New("Bad Response From Server")

// RequestError interface for read requests
type RequestError interface {
	Error() string
	Status() int
}

// RequestReadError error struct that
type RequestReadError struct {
	status  int
	message string
}

func (e *RequestReadError) Error() string {
	return e.message
}

// Status returns the status code of the read error
func (e *RequestReadError) Status() int {
	return e.status
}

// ResponseError interface for read requests
type ResponseError interface {
	Action() int
	Status() int
	Error() string
}

// ResponseWriteError error struct that
type ResponseWriteError struct {
	status  int
	action  int
	message string
}

func (e *ResponseWriteError) Error() string {
	return e.message
}

// Action of the response
func (e *ResponseWriteError) Action() int {
	return e.action
}

// Status returns the status code of the read error
func (e *ResponseWriteError) Status() int {
	return e.status
}

func readRequestAction(r io.Reader) (int, error) {
	// Read the action int32 from the first part of the message.
	var action int32
	err := binary.Read(r, binary.BigEndian, &action)
	return int(action), err
}

func writeRequestAction(action int, w io.Writer) error {
	return binary.Write(w, binary.BigEndian, int32(action))
}

// This writes a status integer that signals the status of the servers
// response
func writeStatusResponse(status int, w io.Writer) error {
	// Write status int
	return binary.Write(w, binary.BigEndian, int32(status))
}

// Read the status from a io Reader
func readStatusResponse(r io.Reader) (int32, error) {
	// Read the status int32 from the first part of the message.
	var status int32
	err := binary.Read(r, binary.BigEndian, &status)
	return status, err
}
