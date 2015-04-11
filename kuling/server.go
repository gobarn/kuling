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

	logStore LogStore
}

// NewStreamServer creates a new stream server
func NewStreamServer(host string, port int, logStore LogStore) *StreamServer {
	return &StreamServer{host, port, logStore}
}

// ListenAndServe the server
func (s *StreamServer) ListenAndServe() {
	// Start a tcp listener on the host and port that were defined
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
			s.handleRequest(conn)
		}()
	}
}

// handleRequest by taking the incomming connection and reading the status
// integer that tells us what the request from the client wants.
func (s *StreamServer) handleRequest(conn net.Conn) {
	// Incoming request
	fmt.Println("Copying to client")

	// read the client requested action from the request
	action, err := readRequestAction(conn)

	if err != nil {
		// We could not read the action from the request. Return faulty request.
		writeStatusResponse(400, conn)
		return
	}

	fmt.Printf("Action %d\n", action)

	if action == ReqFetch {
		// Read the fetch request from the io.Reader
		req, err := NewFetchRequestFromReader(conn)
		// Check that the status of the read was OK, if not write back the status
		// to the client
		if err != nil {
			// Grab the status from the error and return it back to the client
			writeStatusResponse(err.Status(), conn)
			return
		}

		// Request read ok, now grab the data and write the response
		req.WriteFetchRequestResponse(s.logStore, conn, conn)
	} else {
		// unknown action code
		writeStatusResponse(409, conn)
		panic("Unknown action")
	}
}
