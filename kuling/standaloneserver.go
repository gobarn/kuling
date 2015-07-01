package kuling

import (
	"fmt"
	"log"
	"net"
	"strconv"
)

// LogServer struct
type LogServer struct {
	// Listen address
	laddr string
	// Log Store to carry out commands on.
	logStore LogStore
}

// NewStandaloneServer for standalone deployments on a single server.
// All reads and writes go to this one server and thus no coordination
// is needed between servers.
// Writing is simple as this one server owns all topics and segments.
// Reading coordination is done on this one server and is implemented
// with the read group coordinator
func NewStandaloneServer(laddr string, logStore LogStore) *LogServer {
	return &LogServer{laddr, logStore}
}

// ListenAndServe starts the server in a blocking call.
func (s *LogServer) ListenAndServe() {
	// Start a tcp listener on the host and port that were defined
	listen, err := net.Listen("tcp", s.laddr)

	if err != nil {
		log.Println("server: Error listening:", err.Error())
		panic(err)
	}

	// Close the listener when the application closes.
	defer listen.Close()

	log.Println("server: Listening on", s.laddr)

	for {
		// Listen for an incoming connection forever.
		conn, err := listen.Accept()
		if err != nil {
			log.Println("server: Error accepting:", err.Error())
			continue
		}

		// Handle connections in a new goroutine and close the connection after
		// the request has been handled
		go func() {
			defer conn.Close()
			err := s.handleKUSPRequest(conn)
			if err != nil {
				log.Printf("server: Unable to handle request: %s", err)
			}
		}()
	}
}

func (s *LogServer) handleKUSPRequest(conn net.Conn) error {
	// Guard against panic during request handling
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Error while handling request")
		}
	}()

	cmdReader := NewClientCommandReader(conn)
	respWriter := NewClientCommandResponseWriter(conn)

	cmd, err := cmdReader.ReadCommand()
	if err != nil {
		log.Printf("server: Unable to read client command: %s", err)
		err = respWriter.WriteError("PROTOCOL", "Unable to read client command")
		return err
	}

	log.Println(cmd)

	switch cmd.Name {
	case "PING":
		return respWriter.WriteString("PONG")
	case "APPEND":
		err = s.logStore.Append(cmd.Args[0], cmd.Args[1], []byte(cmd.Args[2]), []byte(cmd.Args[3]))
		if err != nil {
			err = respWriter.WriteError("COMMAND", fmt.Sprintf("%s", err))
			return err
		}

		return respWriter.WriteString("OK")
	case "FETCH":
		startSequenceID, err := strconv.ParseInt(cmd.Args[2], 0, 64)
		if err != nil {
			err = respWriter.WriteError("ARGUMENT", "offset not a number")
		}
		maxNumMessages, err := strconv.ParseInt(cmd.Args[3], 0, 64)
		if err != nil {
			err = respWriter.WriteError("ARGUMENT", "max messages not a number")
		}

		_, err = s.logStore.Copy(cmd.Args[0], cmd.Args[1], startSequenceID, maxNumMessages, conn)

		if err != nil {
			err = respWriter.WriteError("COMMAND", fmt.Sprint(err))
			return err
		}

		return respWriter.WriteString("OK")
	}

	err = respWriter.WriteError("UNKNOWN_COMMAND", cmd.Name)
	return err
}
