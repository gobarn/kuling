package kuling

import (
	"log"
	"net"
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
	listen, err := net.Listen("tcp", s.laddr)

	if err != nil {
		log.Println("fetch: Error listening:", err.Error())
		panic(err)
	}

	// Close the listener when the application closes.
	defer listen.Close()

	log.Println("fetch: Listening on", s.laddr)

	for {
		// Listen for an incoming connection for ever.
		conn, err := listen.Accept()
		if err != nil {
			log.Println("fetch: Error accepting:", err.Error())
			continue
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
	responseWriter := NewRequestResponseWriter(conn)

	requestAction, err := requestHeaderReader.ReadRequestHeader()

	if err != nil {
		// We could not read the action from the request. Return faulty request.
		responseWriter.WriteHeader(ReqErr)
		return
	}

	if requestAction == ActionFetch {
		// Read the fetch request from the io.Reader
		fetchRequestReader := NewFetchRequestReader(conn)

		fetchRequest, err := fetchRequestReader.ReadFetchRequest()
		// Check that the status of the read was OK, if not write back the status
		// to the client
		if err != nil {
			// Grab the status from the error and return it back to the client
			responseWriter.WriteHeader(err.Status())
			return
		}

		// Write success response
		responseWriter.WriteHeader(ReqSuccess)
		// Copy log store chunk over to the connection
		_, copyErr := s.logStore.Copy(
			fetchRequest.Topic,
			fetchRequest.Shard,
			fetchRequest.StartSequenceID,
			fetchRequest.MaxNumMessages,
			responseWriter)

		if copyErr != nil {
			// Could not copy, now we have already written the success header... what to do..
			log.Printf("fetch: Could not copy: %s\n", copyErr)
		}
	} else {
		// unknown action code
		responseWriter.WriteHeader(StatusUnknownAction)
		log.Println("fetch: Unknown action", requestAction)
	}
}
