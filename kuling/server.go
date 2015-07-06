package kuling

import (
	"log"
	"net"
)

// Handler for request.
type Handler interface {
	Serve(w *ResponseWriter, cmd string, args []interface{})
}

// HandleFunc definition for functions that can handle cmd requests
type HandleFunc func(w *ResponseWriter, cmd string, args []interface{})

// MuxHandler multiplexes request by the command name to a specific handler
type MuxHandler struct {
	handlers map[string]HandleFunc
}

// NewMuxHandler creates a new must handler
func NewMuxHandler() MuxHandler {
	return MuxHandler{make(map[string]HandleFunc)}
}

// Serve takes the cmd and checks if any handler is registered for that command.
// If no command is registered for the cmd then an err is sent back to the client.
func (m MuxHandler) Serve(w *ResponseWriter, cmd string, args []interface{}) {
	if h, ok := m.handlers[cmd]; ok {
		h(w, cmd, args)
	}

	w.WriteError("UNKNOWN_CMD", cmd)
}

// Handle registers a handler for the cmd
func (m *MuxHandler) Handle(cmd string, h Handler) {
	m.handlers[cmd] = func(w *ResponseWriter, cmd string, args []interface{}) {
		h.Serve(w, cmd, args)
	}
}

// HandleFunc registers a handler function for the cmd
func (m *MuxHandler) HandleFunc(cmd string, f HandleFunc) {
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
		log.Println("server: Error listening:", err.Error())
		panic(err)
	}

	// Close the listener when the application closes.
	defer listen.Close()

	log.Println("server: Listening on", s.Addr)

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
			s.handleRequest(conn)
		}()
	}
}

func (s *Server) handleRequest(conn net.Conn) {
	r := NewReader(conn)
	w := NewResponseWriter(conn)

	resp, err := r.Read()
	if err != nil {
		log.Printf("server: unable to read client command: %s", err)
		err = w.WriteError("PROTOCOL", "unable to read client command")
		if err != nil {
			log.Printf("server: unable to response with error to client: %s", err)
		}
		return
	}

	args := resp.([]interface{})
	cmd := string(args[0].([]byte))

	s.Handler.Serve(w, cmd, args[1:])
}
