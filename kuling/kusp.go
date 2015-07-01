package kuling

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

// KUSP (KUling Serialization Protocol)
// is the communication protocol that clients use to communicate with
// Kuling servers.
// The protocol is highly influenced by the simplicity and elegance of
// REDIS protocol http://redis.io/topics/protocol
//
// The format is human readable and easy to type into simple network
// tools such as Telnet.
//
// The format is in many ways equal to RESP but differes in the values
// sent back from the Kuling server. KUSP do not bother about primitive
// types such as integers and strings and instead sends raw binary blobs for
// the client to make sens of and as such the value returned from the server
// might not be understandable by any client directly if the binary array
// is not a string

// KUSP protocol bytes
const (
	crByte    byte = byte('\r')
	lfByte         = byte('\n')
	spaceByte      = byte(' ')
	errByte        = byte('-')
	okByte         = byte('+')
	countByte      = byte('*')
	sizeByte       = byte('$')
	numByte        = byte(':')
	trueByte       = byte('1')
	falseByte      = byte('0')
)

var crlfBytes = []byte("\r\n")

// ClientCommandResponseType defines the type of response a command will expect
type ClientCommandResponseType int

const (
	VIRTUAL ClientCommandResponseType = iota
	BOOLEAN
	NUMBER
	STRING
	STATUS
	BULK
	MULTI_BULK
)

// PingCmd sends a ping down to the server and expects a pong
var PingCmd = ClientCommand{Name: "PING", ResponseType: STRING}

// AppendCmd sends a append cmd to the server with topic, shard, key and message
var AppendCmd = ClientCommand{Name: "APPEND", ResponseType: STRING}

//
var FetchCmd = ClientCommand{Name: "FETCH", ResponseType: STRING}

// ClientCommand contains the name and arguments that a client want to
// send to a Kuling server
type ClientCommand struct {
	Name         string
	ResponseType ClientCommandResponseType
	Args         []string
}

// ClientCommandWriter writes client command KUSP format to a io writer
type ClientCommandWriter struct {
	*bufio.Writer
}

// NewClientCommandWriter createas a new client command writer that writes
// to the given io writer
func NewClientCommandWriter(w io.Writer) *ClientCommandWriter {
	return &ClientCommandWriter{bufio.NewWriter(w)}
}

// WriteCommand writes the KUSP encoding of the command. Client commands are always
// sent as arrays to the server, the first entry in the array is the command and the rest
// of the entries are the arguments of the command
func (w *ClientCommandWriter) WriteCommand(cmd ClientCommand, args ...string) error {
	nameBytes := []byte(cmd.Name)

	w.WriteByte(countByte)                        // *
	w.Write([]byte(strconv.Itoa(len(args) + 1)))  // <num>
	w.Write(crlfBytes)                            // \r\n
	w.WriteByte(sizeByte)                         // $
	w.Write([]byte(strconv.Itoa(len(nameBytes)))) // <num>
	w.Write(crlfBytes)                            // \r\n
	w.Write(nameBytes)                            // cmd.Name
	w.Write(crlfBytes)                            // \r\n

	// Write each argument in the array
	for _, s := range args {
		w.WriteByte(sizeByte)                 // $
		w.Write([]byte(strconv.Itoa(len(s)))) // <num>
		w.Write(crlfBytes)                    // \r\n
		w.Write([]byte(s))                    // argValue
		w.Write(crlfBytes)                    // \r\n
	}

	return w.Flush()
}

// ClientCommandReader writes client command KUSP format to a io writer
type ClientCommandReader struct {
	*bufio.Reader
}

// NewClientCommandReader creates a new client command reader
func NewClientCommandReader(r io.Reader) *ClientCommandReader {
	return &ClientCommandReader{bufio.NewReader(r)}
}

// ReadCommand parses the bytes from the reader into a Client command with the
// given name and arguments
func (r *ClientCommandReader) ReadCommand() (ClientCommand, error) {
	numArgsLine, _, err := r.ReadLine()
	if err != nil {
		// Could not read line, client sent bad communication
		return ClientCommand{}, fmt.Errorf("kusp: Client command is rubish")
	}

	// The number of elements in the forthcoming array that we expect that the
	// client has sent us
	numArgs, err := strconv.ParseInt(string(numArgsLine[1:]), 0, 64)

	args := make([]string, numArgs)

	for i := int64(0); i < numArgs; i++ {
		// Read argument length
		// We don't need to use this value for now
		_, _, err := r.ReadLine()
		arg, _, err := r.ReadLine()
		if err != nil {
			return ClientCommand{}, fmt.Errorf("kusp: Client command argument is rubish")
		}

		args[i] = string(arg)
	}

	// The first entry in the args list is the implicit name of the command and the rest
	// are true arguments
	return ClientCommand{Name: args[:1][0], Args: args[1:]}, nil
}

// ClientCommandResponse s
type ClientCommandResponse struct {
	Err  error
	Msg  string
	Blob []byte
}

// ClientCommandResponseWriter writes client command responses to io writer
type ClientCommandResponseWriter struct {
	*bufio.Writer
}

// NewClientCommandResponseWriter creates new response writer
func NewClientCommandResponseWriter(w io.Writer) *ClientCommandResponseWriter {
	return &ClientCommandResponseWriter{bufio.NewWriter(w)}
}

// WriteString writes a string response to the writer
func (w *ClientCommandResponseWriter) WriteString(s string) error {
	w.WriteByte(okByte)
	w.Write([]byte(s))
	w.Write(crlfBytes)

	return w.Flush()
}

// WriteError writes error response to the writer
func (w *ClientCommandResponseWriter) WriteError(errType, msg string) error {
	w.WriteByte(errByte)
	w.Write([]byte(errType))
	w.WriteByte(spaceByte)
	w.Write([]byte(msg))

	return w.Flush()
}

// ClientCommandResponseReader reads command responses from server
type ClientCommandResponseReader struct {
	*bufio.Reader
}

// NewClientCommandResponseReader creates a new client command response reader
// that will interpret the response from the server and create a corresponding
// struct for the type of response
func NewClientCommandResponseReader(r io.Reader) *ClientCommandResponseReader {
	return &ClientCommandResponseReader{bufio.NewReader(r)}
}

// ReadResponse reads a command response from the server
func (ccr *ClientCommandResponseReader) ReadResponse(respType ClientCommandResponseType) (ClientCommandResponse, error) {
	// Read all bytes until \r and \n has been found
	line, _, err := ccr.ReadLine()
	if err != nil {
		// Could not read or could not find the new line
		return ClientCommandResponse{}, err
	}

	// Check if the first sign indicates an error from the server
	if line[0] == errByte {
		return ClientCommandResponse{Err: fmt.Errorf("server: %s", line[1:])}, nil
	}

	// No error, use the response type to parse the expected response
	switch respType {
	case STRING:
		return ClientCommandResponse{Msg: string(line[1:])}, nil
	case BULK:
		// Read the response as byte array that was returned from the server
		// First line indicates the length of the byte array to read.
		// it is on the format:
		// $<len>\r\n
		// ...
		// \r\n
		typeInfo, _, err := ccr.ReadLine()
		if err != nil {
			// Could not read first line containing length of byte blob
			return ClientCommandResponse{}, err
		}

		// Read length as a integer parsed from it's string value
		bytesToRead, err := strconv.ParseInt(string(typeInfo[1:]), 0, 64)
		if err != nil {
			// Could not convert string integer to integer
			return ClientCommandResponse{}, fmt.Errorf("client: Could not convert bulk number of bytes to read to int %s", err)
		}

		blob := make([]byte, bytesToRead)
		bytesRead, err := ccr.Read(blob)
		if err != nil {
			// Error reading into byte array
			return ClientCommandResponse{}, fmt.Errorf("client: Unknown read error %s", err)
		}

		if int64(bytesRead) != bytesToRead {
			// The server tells us the exact bytes to read, if we could not do so
			// somethings is wrong
			return ClientCommandResponse{}, fmt.Errorf("client: Server number of bytes to read %d differs from read bytes %d", bytesToRead, bytesRead)
		}

		// Read the final new line, we don't really care about the result
		_, _, err = ccr.ReadLine()
		if err != nil {
			// No newline sent as the last value, this is incorrect protocol
			return ClientCommandResponse{}, fmt.Errorf("client: Server response contains no new line at end of value")
		}

		return ClientCommandResponse{Blob: blob}, nil
	default:
		return ClientCommandResponse{}, fmt.Errorf("client: Unknown response type %v", respType)
	}
}
