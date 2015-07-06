package kuling

import "fmt"

// ListenAndServeStandalone starts a standalone server on the address
func ListenAndServeStandalone(addr string, l LogStore) {
	m := NewMuxHandler()
	m.HandleFunc("PING", pingHandler)
	m.HandleFunc("CREATE_TOPIC", createTopicHandler(l))
	m.HandleFunc("APPEND", createAppendHandler(l))
	m.HandleFunc("FETCH", createFetchHandler(l))

	s := &Server{addr, m}
	s.ListenAndServe()
}

func pingHandler(w *ResponseWriter, cmd string, args []interface{}) {
	w.WriteStatus("PONG")
}

func createTopicHandler(l LogStore) HandleFunc {
	return func(w *ResponseWriter, cmd string, args []interface{}) {
		_, err := l.CreateTopic(
			string(args[0].([]byte)),
			int(args[1].(int64)),
		)
		if err != nil {
			w.WriteError("ERR", err.Error())
		}

		w.WriteStatus("OK")
	}
}

func createAppendHandler(l LogStore) HandleFunc {
	return func(w *ResponseWriter, cmd string, args []interface{}) {
		err := l.Append(
			string(args[0].([]byte)),
			string(args[1].([]byte)),
			args[2].([]byte),
			args[3].([]byte))
		if err != nil {
			w.WriteError("ERR", err.Error())
		}

		w.WriteStatus("OK")
	}
}

func createFetchHandler(l LogStore) HandleFunc {
	return func(w *ResponseWriter, cmd string, args []interface{}) {
		topic := string(args[0].([]byte))
		shard := string(args[1].([]byte))
		startID := args[2].(int64)
		maxNumMessages := args[3].(int64)

		_, err := l.Copy(
			topic,
			shard,
			startID,
			maxNumMessages,
			w,
			func(totalBytesToRead int64) { w.WriteArrayHeader(int(totalBytesToRead)) },
			func(totalBytesRead int64) { w.WriteArrayEnd(int(totalBytesRead)) },
		)

		if err != nil {
			w.WriteError("ERR", fmt.Sprintf("%s : %s", cmd, err))
		}
	}
}
