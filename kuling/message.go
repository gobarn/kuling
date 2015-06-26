package kuling

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"
)

var (
	// Constant table to use
	table = crc32.MakeTable(crc32.IEEE)
)

// Message a
type Message struct {
	Magic         byte   // 1
	SequenceID    int64  // 8
	Crc           int32  // 4
	KeyLength     int32  // 4
	Key           []byte // N
	PayloadLength int32  // 4
	Payload       []byte // N
}

// NewMessage creates a new message from a byte array payload
func NewMessage(sequenceID int64, key, payload []byte) *Message {
	c := crc32.New(table)
	c.Write(payload)

	return &Message{
		0,
		sequenceID,
		int32(crc32.Checksum(payload, table)),
		int32(len(key)),
		key,
		int32(len(payload)),
		payload,
	}
}

// CalculateMessageSize returns the size in bytes of the message
func CalculateMessageSize(key, payload []byte) int64 {
	return int64(1 + 8 + 4 + 4 + 4 + len(key) + len(payload))
}

// MessageWriter writes messages to a io Writer
type MessageWriter struct {
	*bufio.Writer
}

// NewMessageWriter creates a new message writer
func NewMessageWriter(w io.Writer) *MessageWriter {
	return &MessageWriter{bufio.NewWriter(w)}
}

// WriteMessage writes the message into the writer
func (w *MessageWriter) WriteMessage(m *Message) (int64, error) {
	// Write all fields into a buffer that we can flush. This gives us a
	// transaction againts the FS for the write

	// Write magic byte
	err := w.WriteByte(m.Magic)
	if err != nil {
		panic("Unable to write Magic")
	}

	// Write sequence ID
	err = binary.Write(w, binary.BigEndian, &m.SequenceID)
	if err != nil {
		panic("Unable to write sequenceID")
	}
	// Write checksum
	err = binary.Write(w, binary.BigEndian, &m.Crc)
	if err != nil {
		panic("Unable to write checksum")
	}
	// Write key length
	err = binary.Write(w, binary.BigEndian, &m.KeyLength)
	if err != nil {
		panic("Unable to write key length")
	}
	// Write key
	_, err = w.Write(m.Key)
	if err != nil {
		panic("Unable to write key")
	}
	// Write payload length
	err = binary.Write(w, binary.BigEndian, &m.PayloadLength)
	if err != nil {
		panic("Unable to write payload length")
	}
	// Write payload
	_, err = w.Write(m.Payload)
	if err != nil {
		panic("Unable to write payload")
	}

	// The total length of the written message
	totalLen := w.Buffered()

	// Flush the buffer to the writer
	err = w.Flush()

	if err != nil {
		// Could not commit the message to the writer
		return 0, err
	}

	return int64(totalLen), nil
}

// MessageReader struct for reading messages from a io.Reader
type MessageReader struct {
	io.Reader
}

// NewMessageReader creates a new message reader that can read from
// a io Reader
func NewMessageReader(r io.Reader) *MessageReader {
	return &MessageReader{r}
}

// ReadMessages parses a stream of messages into a parsed entity
func (r *MessageReader) ReadMessages() ([]*Message, error) {
	var messages []*Message

	for {
		m, err := r.ReadMessage()

		if err == io.EOF {
			return messages, nil
		}

		if err != nil {
			panic(err)
		}

		messages = append(messages, m)
	}
}

// ReadMessage reads a message file into a slice of messages
func (r *MessageReader) ReadMessage() (*Message, error) {
	// Read Magic
	var magic byte
	err := binary.Read(r, binary.BigEndian, &magic) // Reads 8
	if err != nil {
		return nil, err
	}

	// Sequence id
	var sequenceID int64
	err = binary.Read(r, binary.BigEndian, &sequenceID) // Reads 8
	if err != nil {
		return nil, err
	}

	// Crc
	var crc int32
	err = binary.Read(r, binary.BigEndian, &crc) // Reads 8
	if err != nil {
		return nil, err
	}

	// Key
	var keyLength int32
	err = binary.Read(r, binary.BigEndian, &keyLength) // Reads 8
	if err != nil {
		return nil, err
	}

	key := make([]byte, keyLength) // Reads len payload
	_, err = r.Read(key)

	// Payload
	var payloadLength int32
	err = binary.Read(r, binary.BigEndian, &payloadLength) // Reads 8
	if err != nil {
		return nil, err
	}

	payload := make([]byte, payloadLength) // Reads len payload
	_, err = r.Read(payload)

	if err != nil {
		return nil, err
	}

	return &Message{
			magic,
			sequenceID,
			crc,
			keyLength,
			key,
			payloadLength,
			payload,
		},
		nil
}
