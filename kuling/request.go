package kuling

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// ClientRequest request contains the topic and start sequence ID as well as the
// max number of messages to fetch
type ClientRequest struct {
	// The action that the request want to carry out
	action  int32
	payload []byte
}

func writeFetcRequest(topic string, startSequenceID, maxNumMessages int64, w io.Writer) (int64, error) {
	buffer := bytes.NewBuffer(make([]byte, 0))
	// Write action
	binary.Write(buffer, binary.BigEndian, int32(200))
	// Write startSequenceID
	binary.Write(buffer, binary.BigEndian, startSequenceID)
	// Write maxNumMessages
	binary.Write(buffer, binary.BigEndian, maxNumMessages)
	// Write topic length
	topicBytes := []byte(topic)
	binary.Write(buffer, binary.BigEndian, int32(len(topicBytes)))
	// Write payload
	buffer.Write(topicBytes)

	bytesWritte, err := w.Write(buffer.Bytes())

	if err != nil {
		// If we could not write the action then return back
		return 0, err
	}

	return int64(bytesWritte), nil
}

func readRequest(ls *LogStore, r io.Reader, w io.Writer) {
	// Incoming request
	fmt.Println("Copying to client")

	// Read the action int32 from the first part of the message
	var action int32
	err := binary.Read(r, binary.BigEndian, &action) // Reads 8

	if err != nil {
		// We could not read the action from the request. Return faulty request
		writeStatusResponse(404, w)
		return
	}

	fmt.Printf("Action %d\n", action)

	// TODO check that the action code actully exists
	if action == 200 {
		// Fetch request, continue with reading the topic, start sequence ID and
		// max num messages
		var startSequenceID int64
		err := binary.Read(r, binary.BigEndian, &startSequenceID) // Reads 8

		if err != nil {
			// Write back that we could not read the start sequence ID
			writeStatusResponse(405, w)
			panic(err)
		}

		fmt.Printf("Start %d\n", startSequenceID)

		var maxNumMessages int64
		err = binary.Read(r, binary.BigEndian, &maxNumMessages) // Reads 8

		if err != nil {
			// Write back that we could not read max num messages
			writeStatusResponse(406, w)
			panic(err)
		}

		fmt.Printf("Max %d\n", maxNumMessages)

		var topicLength int32
		err = binary.Read(r, binary.BigEndian, &topicLength) // Reads 4

		if err != nil {
			// Write back that we could not read topic length
			writeStatusResponse(407, w)
			panic(err)
		}

		fmt.Printf("TL %d\n", topicLength)

		topic := make([]byte, topicLength) // Reads len payload
		_, err = r.Read(topic)

		if err != nil {
			// Write back that we could not read the topic
			writeStatusResponse(408, w)
			panic(err)
		}

		fmt.Printf("Topic %s\n", topic)

		// Now we have read it all and are ready to read from the log store
		// Write succes response
		writeStatusResponse(200, w)
		// Write the messages from the topic to the connection
		numCopied, err := ls.Copy(string(topic), startSequenceID, maxNumMessages, w)

		if err != nil {
			// Let client know that we could not read from the DB
			writeStatusResponse(408, w)
			panic(err)
		}

		fmt.Printf("Num copied %d\n", numCopied)
	} else {
		// unknown action code
		writeStatusResponse(409, w)
		panic("Unknown action")
	}
}

func writeStatusResponse(status int, w io.Writer) {
	// Write checksum
	binary.Write(w, binary.BigEndian, int32(status))
}
