package kuling

import (
	"bufio"
	"bytes"
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

const (
	okReply   = "OK"
	pongReply = "PONG"
)

// Writer writes client command KUSP format to a io writer
type Writer struct {
	w *bufio.Writer
	// Scratch space for formatting argument length.
	// '*' or '$', length, "\r\n"
	lenScratch [32]byte

	// Scratch space for formatting integers and floats.
	numScratch [40]byte
}

// NewWriter createas a new client command writer that writes
// to the given io writer
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: bufio.NewWriter(w)}
}

func (c *Writer) writeLen(prefix byte, n int) {
	c.w.WriteByte(prefix)
	c.w.Write([]byte(strconv.Itoa(n)))
	c.w.Write(crlfBytes)
}

func (c *Writer) writeString(s string) {
	c.writeLen('$', len(s))
	c.w.WriteString(s)
	c.w.WriteString("\r\n")
}

func (c *Writer) writeBytes(p []byte) {
	c.writeLen('$', len(p))
	c.w.Write(p)
	c.w.WriteString("\r\n")
}

func (c *Writer) writeInt64(n int64) {
	c.w.WriteByte(':')
	c.w.Write(strconv.AppendInt(c.numScratch[:0], n, 10))
	c.w.Write(crlfBytes)
}

func (c *Writer) writeStatus(s string) {
	c.w.WriteByte('+')
	c.w.WriteString(s)
	c.w.WriteString("\r\n")
}

func (c *Writer) writeErr(s string) {
	c.w.WriteByte('-')
	c.w.WriteString(s)
	c.w.WriteString("\r\n")
}

func (c *Writer) writeInterface(i interface{}) {
	switch i := i.(type) {
	case string:
		c.writeString(i)
	case []byte:
		c.writeBytes(i)
	case int:
		c.writeInt64(int64(i))
	case int64:
		c.writeInt64(i)
	case bool:
		if i {
			c.writeString("1")
		} else {
			c.writeString("0")
		}
	case nil:
		c.writeString("")
	default:
		var buf bytes.Buffer
		fmt.Fprint(&buf, i)
		c.writeBytes(buf.Bytes())
	}
}

type CommandWriter struct {
	*Writer
}

func NewCommandWriter(w io.Writer) *CommandWriter {
	return &CommandWriter{NewWriter(w)}
}

// WriteCommand writes the encoding of the command. Client commands are always
// sent as arrays to the server, the first entry in the array is the command and the rest
// of the entries are the arguments of the command
func (c *CommandWriter) WriteCommand(cmd string, args ...interface{}) error {
	c.writeLen('*', 1+len(args))
	c.writeString(cmd)

	for _, arg := range args {
		err := c.w.Flush()

		if err != nil {
			return err
		}

		c.writeInterface(arg)
	}

	return c.w.Flush()
}

// ResponseWriter writes client command responses to io writer
type ResponseWriter struct {
	*Writer
}

// NewResponseWriter creates new response writer
func NewResponseWriter(w io.Writer) *ResponseWriter {
	return &ResponseWriter{NewWriter(w)}
}

// WriteStatus writes a string response to the writer
func (r *ResponseWriter) WriteStatus(s string) error {
	r.writeStatus(s)
	return r.w.Flush()
}

// WriteError writes error response to the writer
func (r *ResponseWriter) WriteError(errType, msg string) error {
	r.writeErr(fmt.Sprintf("%s %s", errType, msg))
	return r.w.Flush()
}

// WriteBulkStart writes the length information of the values that will
// be sent
func (r *ResponseWriter) WriteBulkStart(totalBytesToWrite int) error {
	r.writeLen(sizeByte, totalBytesToWrite)
	return r.w.Flush()
}

// WriteBulkEnd writes the ending parts of the communication to the client
func (r *ResponseWriter) WriteBulkEnd(totalBytesToWrite int) error {
	r.w.WriteString("\r\n")
	return r.w.Flush()
}

// Reader reads command responses from server
type Reader struct {
	r *bufio.Reader
}

// NewReader creates a new client command response reader
// that will interpret the response from the server and create a corresponding
// struct for the type of response
func NewReader(r io.Reader) *Reader {
	return &Reader{bufio.NewReader(r)}
}

func (ccr *Reader) Read() (interface{}, error) {
	line, _, err := ccr.r.ReadLine()

	if err != nil {
		return nil, err
	}

	if len(line) == 0 {
		return nil, fmt.Errorf("protocol: Empty response line received from server")
	}

	switch line[0] {
	case '+':
		switch {
		// Some optimizations due to frequent OK result
		case len(line) == 3 && line[1] == 'O' && line[2] == 'K':
			return okReply, nil
		case len(line) == 5 && line[1] == 'P' && line[2] == 'O' && line[3] == 'N' && line[4] == 'G':
			return pongReply, nil
		default:
			return string(line[1:]), nil
		}
	case '-':
		return nil, fmt.Errorf("%s", string(line[1:]))
	case ':':
		return parseInt(line[1:])
	case '$':
		// Length information line
		n, err := parseLen(line[1:])
		if n < 0 || err != nil {
			return nil, err
		}
		p := make([]byte, n)
		_, err = io.ReadFull(ccr.r, p)
		if err != nil {
			return nil, err
		}
		if line, _, err := ccr.r.ReadLine(); err != nil {
			return nil, err
		} else if len(line) != 0 {
			return nil, fmt.Errorf("protocol: Bad bulk string format")
		}
		return p, nil
	case '*':
		// Number of arguments line
		n, err := parseLen(line[1:])
		if n < 0 || err != nil {
			return nil, err
		}
		r := make([]interface{}, n)
		for i := range r {
			r[i], err = ccr.Read()
			if err != nil {
				return nil, err
			}
		}
		return r, nil
	}

	return nil, nil
}

// parseLen parses bulk string and array lengths.
func parseLen(p []byte) (int, error) {
	if len(p) == 0 {
		return -1, fmt.Errorf("protocol: Malformed length")
	}

	if p[0] == '-' && len(p) == 2 && p[1] == '1' {
		return -1, nil
	}

	var n int
	for _, b := range p {
		n *= 10
		if b < '0' || b > '9' {
			return -1, fmt.Errorf("protocol: Illegal bytes in length")
		}
		n += int(b - '0')
	}

	return n, nil
}

func parseInt(p []byte) (interface{}, error) {
	if len(p) == 0 {
		return 0, fmt.Errorf("protocol: Malformed integer")
	}

	var negate bool
	if p[0] == '-' {
		negate = true
		p = p[1:]
		if len(p) == 0 {
			return 0, fmt.Errorf("protocol: Malformed integer")
		}
	}

	var n int64
	for _, b := range p {
		n *= 10
		if b < '0' || b > '9' {
			return 0, fmt.Errorf("protocol: Illegal bytes in length")
		}
		n += int64(b - '0')
	}

	if negate {
		n = -n
	}

	return n, nil
}
