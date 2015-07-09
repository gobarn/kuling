package resp

import (
	"bufio"
	"fmt"
	"io"
)

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
		return nil, fmt.Errorf("protocol: empty response line received from server")
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
			return nil, fmt.Errorf("protocol: bad bulk string format")
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
		return -1, fmt.Errorf("protocol: malformed length")
	}

	if p[0] == '-' && len(p) == 2 && p[1] == '1' {
		return -1, nil
	}

	var n int
	for _, b := range p {
		n *= 10
		if b < '0' || b > '9' {
			return -1, fmt.Errorf("protocol: illegal bytes in length")
		}
		n += int(b - '0')
	}

	return n, nil
}

func parseInt(p []byte) (interface{}, error) {
	if len(p) == 0 {
		return 0, fmt.Errorf("protocol: malformed integer")
	}

	var negate bool
	if p[0] == '-' {
		negate = true
		p = p[1:]
		if len(p) == 0 {
			return 0, fmt.Errorf("protocol: malformed integer")
		}
	}

	var n int64
	for _, b := range p {
		n *= 10
		if b < '0' || b > '9' {
			return 0, fmt.Errorf("protocol: illegal bytes in length")
		}
		n += int64(b - '0')
	}

	if negate {
		n = -n
	}

	return n, nil
}
