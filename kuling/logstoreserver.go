package kuling

import (
	"log"
	"net"
	"os"
)

// LogServer struct
type LogServer struct {
	// Listen address
	laddr string
	// Log Store to carry out commands on.
	logStore LogStore
}

// NewLogServer creates a new stream server
func NewLogServer(laddr string, logStore LogStore) *LogServer {
	return &LogServer{laddr, logStore}
}

// ListenAndServe the server
func (s *LogServer) ListenAndServe() {
	// Start a tcp listener on the host and port that were defined
	l, err := net.Listen("tcp", s.laddr)

	if err != nil {
		log.Println("fetch: Error listening:", err.Error())
		panic(err)
	}

	// Close the listener when the application closes.
	defer l.Close()

	log.Println("fetch: Listening on", s.laddr)

	for {
		// Listen for an incoming connection for ever.
		conn, err := l.Accept()
		if err != nil {
			log.Println("fetch: Error accepting:", err.Error())
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
func (s *LogServer) handleRequest(conn net.Conn) {
	// Reade request header
	requestHeaderReader := NewRequestHeaderReader(conn)
	requestAction, err := requestHeaderReader.ReadRequestHeader()

	if err != nil {
		// We could not read the action from the request. Return faulty request.
		writeStatusResponse(400, conn)
		return
	}

	if requestAction == ActionFetch {
		// Read the fetch request from the io.Reader
		requestReader := NewFetchRequestReader(conn)
		responseWriter := NewFetchRequestResponseWriter(conn)

		fetchRequest, err := requestReader.ReadFetchRequest()
		// Check that the status of the read was OK, if not write back the status
		// to the client
		if err != nil {
			// Grab the status from the error and return it back to the client
			writeStatusResponse(err.Status(), conn)
			return
		}

		// Write success response
		responseWriter.WriteSuccessResponse()
		// Copy log store chunk over to the connection
		bytesCopied, copyErr := s.logStore.Copy(fetchRequest.Topic, fetchRequest.StartSequenceID, fetchRequest.MaxNumMessages, responseWriter)

		if copyErr != nil {
			// Could not copy, now we have already written the success header... what to do..
			log.Println("fetch: Could not copy ", bytesCopied, " to connection")
		}
	} else {
		// unknown action code
		writeStatusResponse(409, conn)
		log.Println("fetch: Unknown action", requestAction)
	}
}
