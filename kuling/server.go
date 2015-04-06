package kuling

import (
	"fmt"
	"net"
	"os"
	"strconv"
)

// StreamServer struct
type StreamServer struct {
	host string
	port int

	logStore *LogStore
}

// NewStreamServer creates a new stream server
func NewStreamServer(host string, port int, logStore *LogStore) *StreamServer {
	return &StreamServer{host, port, logStore}
}

// ListenAndServe the server
func (s *StreamServer) ListenAndServe() {
	address := net.JoinHostPort(s.host, strconv.Itoa(s.port))
	l, err := net.Listen("tcp", address)

	if err != nil {
		fmt.Println("Error listening:", err.Error())
		panic(err)
	}

	// Close the listener when the application closes.
	defer l.Close()

	fmt.Println("Listening on " + address)

	for {
		// Listen for an incoming connection.
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}

		// Handle connections in a new goroutine and close the connection after
		// the request has been handled
		go func() {
			defer conn.Close()
			readRequest(s.logStore, conn, conn)
		}()
	}
}
