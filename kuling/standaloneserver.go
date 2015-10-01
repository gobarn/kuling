package kuling

import (
	"fmt"

	"github.com/fredrikbackstrom/kuling/kuling/resp"
)

// ListenAndServeStandalone starts a standalone server on the address
func ListenAndServeStandalone(addr string, l *LogStore) {
	m := resp.NewServeMux()
	m.HandleFunc("PING", pingHandler)

	m.HandleFunc("CREATE", createTopicHandler(l))
	m.HandleFunc("LIST", createListTopicsHandler(l))
	m.HandleFunc("DESCRIBE", createDescribeTopicHandler(l))

	m.HandleFunc("PUT", createAppendHandler(l))
	m.HandleFunc("GET", createFetchHandler(l))

	s := &resp.Server{addr, m}
	s.ListenAndServe()
}

func pingHandler(w resp.ResponseWriter, r *resp.Request) {
	w.WriteStatus("PONG")
}

func createTopicHandler(l *LogStore) resp.HandleFunc {
	return func(w resp.ResponseWriter, r *resp.Request) {
		_, err := l.CreateTopic(
			string(r.Args[0].([]byte)),
			int(r.Args[1].(int64)),
		)
		if err != nil {
			w.WriteErr("ERR", err.Error())
		}

		w.WriteStatus("OK")
	}
}

func createListTopicsHandler(l *LogStore) resp.HandleFunc {
	return func(w resp.ResponseWriter, r *resp.Request) {
		m := l.Topics()

		w.WriteInstruction('*', len(m))

		for k := range m {
			w.WriteString(k)
		}

		w.WriteEnd()
	}
}

func createDescribeTopicHandler(l *LogStore) resp.HandleFunc {
	return func(w resp.ResponseWriter, r *resp.Request) {
		shards, err := l.Shards(string(r.Args[0].([]byte)))
		if err != nil {
			fmt.Println(err)
		}

		w.WriteInterface(len(shards))
	}
}

func createAppendHandler(l *LogStore) resp.HandleFunc {
	return func(w resp.ResponseWriter, r *resp.Request) {
		err := l.Append(
			string(r.Args[0].([]byte)),
			string(r.Args[1].([]byte)),
			r.Args[2].([]byte),
			r.Args[3].([]byte))

		if err != nil {
			w.WriteErr("ERR", err.Error())
			return
		}

		w.WriteStatus("OK")
	}
}

func createFetchHandler(l *LogStore) resp.HandleFunc {
	return func(w resp.ResponseWriter, r *resp.Request) {
		topic := string(r.Args[0].([]byte))
		shard := string(r.Args[1].([]byte))
		startID := r.Args[2].(int64)
		maxNumMessages := r.Args[3].(int64)

		_, err := l.Copy(
			topic,
			shard,
			startID,
			maxNumMessages,
			r.Writer, // Special handling to get speed? from using the underlying raw connection
			func(totalBytesToRead int64) { w.WriteInstruction('$', int(totalBytesToRead)) },
			func(totalBytesRead int64) { w.WriteEnd() },
		)

		if err != nil {
			w.WriteErr("ERR", fmt.Sprintf("%s : %s", r.Cmd, err))
		}
	}
}
