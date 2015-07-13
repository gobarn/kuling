package resp

import (
	"bufio"
	"io"
	"log"
	"net"
	"os"
)

// ResponseWriter writes client command responses to io writer
type ResponseWriter interface {
	WriteInterface(interface{}) error
	WriteArray(...interface{}) error

	WriteInstruction(prefix byte, n int) error
	WriteEnd() error

	WriteString(string) error
	WriteBytes([]byte) error
	WriteInt64(int64) error

	WriteStatus(string) error
	WriteErr(errType, msg string) error
}

// Handler for request.
type Handler interface {
	Serve(ResponseWriter, *Request)
}

// HandleFunc definition for functions that can handle cmd requests
type HandleFunc func(ResponseWriter, *Request)

// ServeMux multiplexes request by the command name to a specific handler
type ServeMux struct {
	handlers map[string]HandleFunc
}

// NewServeMux creates a new must handler
func NewServeMux() ServeMux {
	return ServeMux{make(map[string]HandleFunc)}
}

// Serve takes the cmd and checks if any handler is registered for that command.
// If no command is registered for the cmd then an err is sent back to the client.
func (m ServeMux) Serve(w ResponseWriter, r *Request) {
	if h, ok := m.handlers[r.Cmd]; ok {
		h(w, r)
		return
	}

	w.WriteErr("UNKNOWN_CMD", r.Cmd)
}

// Handle registers a handler for the cmd
func (m *ServeMux) Handle(cmd string, h Handler) {
	m.handlers[cmd] = func(w ResponseWriter, r *Request) {
		h.Serve(w, r)
	}
}

// HandleFunc registers a handler function for the cmd
func (m *ServeMux) HandleFunc(cmd string, f HandleFunc) {
	m.handlers[cmd] = f
}

// Server struct
type Server struct {
	Addr    string // Listen address
	Handler Handler
}

// ListenAndServe starts the server in a blocking call.
func (s *Server) ListenAndServe() {
	// Start a tcp listener on the host and port that were defined
	listen, err := net.Listen("tcp", s.Addr)

	if err != nil {
		log.Println("server: error listening:", err.Error())
		panic(err)
	}

	// Close the listener when the application closes.
	defer listen.Close()

	log.Println("server: listening on", s.Addr)

	for {
		// Listen for an incoming connection forever.
		conn, err := listen.Accept()
		if err != nil {
			log.Println("server: error accepting:", err.Error())
			continue
		}

		// Handle connections in a new goroutine and close the connection after
		// the request has been handled
		go func() {
			defer conn.Close()
			s.handleConn(conn)
		}()
	}
}

func (s *Server) handleConn(conn net.Conn) {
	r := NewReader(conn)
	// TODO remove multiwriter
	mw := io.MultiWriter(conn, os.Stdout)
	w := &Writer{w: bufio.NewWriter(mw)}

	resp, err := r.Read()
	if err != nil {
		log.Printf("server: unable to read client command: %s", err)
		err = w.WriteErr("PROTOCOL", "unable to read client command")
		if err != nil {
			log.Printf("server: unable to response with error to client: %s", err)
		}
		return
	}

	args := resp.([]interface{})
	if len(args) == 0 {
		log.Printf("server: client sent empty command")
		return
	}

	cmd := string(args[0].([]byte))

	s.Handler.Serve(w, &Request{conn, cmd, args[1:]})
}
