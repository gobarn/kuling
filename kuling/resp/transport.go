package resp

import (
	"errors"
	"io"
	"io/ioutil"
	"net"
	"net/url"
)

// // Request coming from the client to a server
// type Request struct {
// 	Writer io.Writer
//
// }

type Request struct {
	URL    *url.URL
	Reader io.ReadCloser
}

type Response struct {
	io.Writer
}

var DefaultTransport = &Transport{}

type Transport struct{}

func (t *Transport) RoundTrip(req *Request) (*Response, error) {
	if req.URL == nil {
		return nil, errors.New("resp: nil request url")
	}
	if req.URL.Scheme != "tcp" {
		return nil, errors.New("resp: unsupported scheme")
	}

	host, port, err := net.SplitHostPort(req.URL.Host)
	if err != nil {
		host = req.URL.Host
		port = "7777"
	}

	// Pool connections!?
	conn, err := net.Dial("tcp", host+":"+port)
	if err != nil {
		return nil, err
	}

	if _, err := conn.Write(req.Reader); err != nil {
		return nil, err
	}

}

type Client struct {
	t Transport
}

func (c *Client) Do(req *Request) (*Response, error) {
	if req.URL == nil {
		req.Args.Close()
		return nil, fmt.Errof("resp: nil request url")
	}
	if req.Cmd == nil {
		req.Args.Close()
		return nil, fmt.Errof("resp: nil request cmd")
	}

	return nil, nil
}

func NewRequest(urlStr, cmd string, body io.Reader) (*Request, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}

	rc, ok := body.(io.ReadCloser)
	if !ok {
		rc := ioutil.NopCloser(body)
	}

	req := &Request{
		Cmd:  cmd,
		URL:  u,
		Args: rc,
	}

	return req, nil
}
