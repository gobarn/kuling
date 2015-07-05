package kuling

import (
	"fmt"
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
	cmdReader := NewReader(conn)
	respWriter := NewResponseWriter(conn)

	var cmdArray []interface{}
	var cmd string

	// Guard against panic during request handling
	defer func() {
		if r := recover(); r != nil {
			if cmd != "" {
				log.Println("Error while handling command", cmd, r)
				respWriter.WriteError("COMMAND", fmt.Sprintf("Client sent bad command (%s) or server had critical issue", cmd))
			} else {
				log.Println("Error while handling command", r)
				respWriter.WriteError("COMMAND", fmt.Sprint("Client sent bad request or server had critical issue"))
			}
		}
	}()

	resp, err := cmdReader.Read()
	if err != nil {
		log.Printf("server: Unable to read client command: %s", err)
		err = respWriter.WriteError("PROTOCOL", "Unable to read client command")
		return err
	}

	// The command is always an array
	cmdArray = resp.([]interface{})
	cmd = string(cmdArray[0].([]byte))

	switch cmd {
	case "PING":
		return respWriter.WriteStatus("OK")
	case "CREATE_TOPIC":
		_, err = s.logStore.CreateTopic(
			string(cmdArray[1].([]byte)),
			int(cmdArray[2].(int64)),
		)
		if err != nil {
			return respWriter.WriteError("ERR", err.Error())
		}
		return respWriter.WriteStatus("OK")
	case "APPEND":
		err = s.logStore.Append(
			string(cmdArray[1].([]byte)),
			string(cmdArray[2].([]byte)),
			cmdArray[3].([]byte),
			cmdArray[4].([]byte))
		if err != nil {
			return respWriter.WriteError("ERR", err.Error())
		}

		return respWriter.WriteStatus("OK")
	case "FETCH":
		topic := string(cmdArray[1].([]byte))
		shard := string(cmdArray[2].([]byte))
		startID := cmdArray[3].(int64)
		maxNumMessages := cmdArray[4].(int64)

		_, err = s.logStore.Copy(
			topic,
			shard,
			startID,
			maxNumMessages,
			conn,
			func(totalBytesToRead int64) { respWriter.WriteBulkStart(int(totalBytesToRead)) },
			func(totalBytesRead int64) { respWriter.WriteBulkEnd(int(totalBytesRead)) },
		)

		if err != nil {
			err = respWriter.WriteError("ERR", fmt.Sprintf("%s : %s", cmd, err))
			return err
		}

		return nil
	default:
		return respWriter.WriteError("UNKNOWN_COMMAND", cmd)
	}
}
