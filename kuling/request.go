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
	// ReqSuccess signals that the request was successfully handled by server
	ReqSuccess = 200
	// ReqErr request not built correctly, some byte order is wrong
	ReqErr = 300
	// ReqErrSequenceID indicates that the start sequence number could not be read
	ReqErrSequenceID = 305
	// ReqErrMaxNumMessage indicates that the start sequence number could not be read
	ReqErrMaxNumMessage = 310
	// ReqErrTopicLength indicates that the start sequence number could not be read
	ReqErrTopicLength = 315
	// ReqErrTopic indicates that the start sequence number could not be read
	ReqErrTopic = 320
	// ReqErrShardLength indicates that the requests shard length byte is off
	ReqErrShardLength = 330
	// ReqErrShard indicates that the request shard value is off
	ReqErrShard = 335
	// StatusUnknownAction unknown action sent to server
	StatusUnknownAction = 500
)

// RequestAction type
type RequestAction int32

// ResponseStatus type
type ResponseStatus int32

// ErrBadResponseFromServer Signals that the byte sent back from the client
// could not be read as they were expected to be read.
var ErrBadResponseFromServer = errors.New("Bad Response From Server")

// RequestError interface for read requests
type RequestError interface {
	Error() string
	Status() ResponseStatus
}

// RequestReadError error struct that
type RequestReadError struct {
	status  ResponseStatus
	message string
}

func (e *RequestReadError) Error() string {
	return e.message
}

// Status returns the status code of the read error
func (e *RequestReadError) Status() ResponseStatus {
	return e.status
}

// ResponseError interface for read requests
type ResponseError interface {
	Action() RequestAction
	Status() ResponseStatus
	Error() string
}

// ResponseWriteError error struct that
type ResponseWriteError struct {
	status  ResponseStatus
	action  RequestAction
	message string
}

func (e *ResponseWriteError) Error() string {
	return e.message
}

// Action of the response
func (e *ResponseWriteError) Action() RequestAction {
	return e.action
}

// Status returns the status code of the read error
func (e *ResponseWriteError) Status() ResponseStatus {
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
func (rhr *RequestHeaderReader) ReadRequestHeader() (RequestAction, error) {
	// Read the action int32 from the first part of the message.
	var action RequestAction
	err := binary.Read(rhr, binary.BigEndian, &action)
	return action, err
}

// RequestResponseWriter writes responses
type RequestResponseWriter struct {
	io.Writer
}

// NewRequestResponseWriter creates a request response writer that
// wrapps the call to the io reader
func NewRequestResponseWriter(w io.Writer) *RequestResponseWriter {
	return &RequestResponseWriter{w}
}

// WriteHeader writes a success response header
func (fr *RequestResponseWriter) WriteHeader(status ResponseStatus) error {
	// Write int32 status number
	return binary.Write(fr, binary.BigEndian, status)
}
