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
	l, err := net.Listen("tcp", s.host+":"+strconv.Itoa(s.port))

	if err != nil {
		fmt.Println("Error listening:", err.Error())
		panic(err)
	}

	// Close the listener when the application closes.
	defer l.Close()
	fmt.Println("Listening on " + s.host + ":" + strconv.Itoa(s.port))
	for {
		// Listen for an incoming connection.
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		// Handle connections in a new goroutine.
		go s.handleRequest(conn)
	}
}

func (s *StreamServer) handleRequest(conn net.Conn) {
	fmt.Println("Copying to client")
	// Send a response back to person contacting us.
	numCopied, err := s.logStore.Copy("payments", 0, 100, conn)
	// Close the connection when you're done with it.

	if err != nil {
		panic(err)
	}

	fmt.Println(numCopied)

	conn.Close()
}
