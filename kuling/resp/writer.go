package resp

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
)

// RESP - Redis Serialization Protocol
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

// NewWriter wraps the io Writer with buffered writer and
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: bufio.NewWriter(w)}
}

// WriteInterface takes any value and writes the RESP encoding of that
// value. Supported types are strings, []byte, int, int32, int64, bool, nil,
func (c *Writer) WriteInterface(i interface{}) error {
	switch i := i.(type) {
	case string:
		return c.WriteString(i)
	case []byte:
		return c.WriteBytes(i)
	case int:
		return c.WriteInt64(int64(i))
	case int64:
		return c.WriteInt64(i)
	case bool:
		if i {
			return c.WriteString("1")
		}
		return c.WriteString("0")
	case nil:
		return c.WriteString("")
	default:
		var buf bytes.Buffer
		fmt.Fprint(&buf, i)
		return c.WriteBytes(buf.Bytes())
	}
}

// WriteArray writes a header with the number of arguments in the array and
// then each element. Example:
//
// *2
// $3
// HEY
// $3
// YO!
func (c *Writer) WriteArray(args ...interface{}) error {
	// Write the lenght * indicator followed by the number of elements
	c.WriteInstruction('*', len(args))
	for _, arg := range args {
		if err := c.w.Flush(); err != nil {
			return err
		}

		c.WriteInterface(arg)
	}

	return c.w.Flush()
}

// WriteInstruction writes only the first instructing line. Instructions
// include but are not limited to *, $.
// *5\r\n
func (c *Writer) WriteInstruction(prefix byte, n int) error {
	c.w.WriteByte(prefix)
	c.w.Write([]byte(strconv.Itoa(n)))
	c.w.Write(crlfBytes)
	return c.w.Flush()
}

// WriteEnd writes the ending part of a result or instruction
// \r\n
func (c *Writer) WriteEnd() error {
	c.w.Write(crlfBytes)
	return c.w.Flush()
}

// WriteString writes a string value on a single line
// $5\r\n
// HELLO\r\n
func (c *Writer) WriteString(s string) error {
	c.WriteInstruction('$', len(s))
	c.w.WriteString(s)
	c.w.Write(crlfBytes)
	return c.w.Flush()
}

// WriteBytes writes a byte array value using a length instruction and the
// byte value.
// $150\r\n
// <bytes>\r\n
func (c *Writer) WriteBytes(p []byte) error {
	c.WriteInstruction('$', len(p))
	c.w.Write(p)
	c.w.Write(crlfBytes)
	return c.w.Flush()
}

// WriteInt64 writes a integer in textual format with the length of the
// string format integer as the instruction.
// $3\r\n
// 123\r\n
func (c *Writer) WriteInt64(n int64) error {
	c.w.WriteByte(':')
	c.w.Write(strconv.AppendInt(c.numScratch[:0], n, 10))
	c.w.Write(crlfBytes)
	return c.w.Flush()
}

// WriteStatus write a string that is prefixed with the OK byte + to indicate
// that the message is not an error.
// Not that the string cannot contain any newline
// +OK
func (c *Writer) WriteStatus(s string) error {
	c.w.WriteByte('+')
	c.w.WriteString(s)
	c.w.Write(crlfBytes)
	return c.w.Flush()
}

// WriteErr writes an err type and message prefixed with the error instruction -
// Note that the string and type cannot contain any new line
// -ERR not working\r\n
func (c *Writer) WriteErr(errType, msg string) error {
	c.w.WriteByte('-')
	c.w.WriteString(fmt.Sprintf("%s %s", errType, msg))
	c.w.Write(crlfBytes)
	return c.w.Flush()
}
