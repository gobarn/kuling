package kuling

import (
	"bufio"
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
	// StatusSuccess signals that the request was successfully handled by server
	StatusSuccess = 200
	// StatusErrSequenceID indicates that the start sequence number could not be read
	StatusErrSequenceID = 205
)

// RequestAction type
type RequestAction int32

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

// RequestHeaderWriter writes a request header to a io reader
type RequestHeaderWriter struct {
	*bufio.Writer
}

// NewRequestHeaderWriter creates a new request header writer that wrappes
// a io writer§
func NewRequestHeaderWriter(w io.Writer) *RequestHeaderWriter {
	return &RequestHeaderWriter{bufio.NewWriter(w)}
}

// WriteHeader writes the request header.
func (rhw *RequestHeaderWriter) WriteHeader(action RequestAction) error {
	return binary.Write(rhw, binary.BigEndian, action)
}

// RequestHeaderReader reads a request header
type RequestHeaderReader struct {
	io.Reader
}

// NewRequestHeaderReader Creates a new request header reader
func NewRequestHeaderReader(r io.Reader) *RequestHeaderReader {
	return &RequestHeaderReader{r}
}

// ReadRequestHeader reads the header of a request
func (rhr *RequestHeaderReader) ReadRequestHeader() (int, error) {
	// Read the action int32 from the first part of the message.
	var action int32
	err := binary.Read(rhr, binary.BigEndian, &action)
	return int(action), err
}

// // This writes a status integer that signals the status of the servers
// // response
func writeStatusResponse(status int, w io.Writer) error {
	// Write status int
	return binary.Write(w, binary.BigEndian, int32(status))
}
