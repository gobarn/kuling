package kuling

import (
	"encoding/binary"
	"hash/crc32"
	"io"
)

var (
	// Constant table to use
	table           = crc32.MakeTable(crc32.IEEE)
	headerLen int32 = 10
)

// Message a
type Message struct {
	Crc           int32
	KeyLength     int32
	Key           []byte
	PayloadLength int32
	Payload       []byte
}

// NewMessage creates a new message from a byte array payload
func NewMessage(key, payload []byte) *Message {
	c := crc32.New(table)
	c.Write(payload)

	return &Message{int32(crc32.Checksum(payload, table)), int32(len(key)), key, int32(len(payload)), payload}
}

// WriteMessage writes the message into the writer
func WriteMessage(w io.Writer, m *Message) (int64, error) {
	// Write checksum
	err := binary.Write(w, binary.BigEndian, &m.Crc)
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

	return int64(4 + 4 + 4 + m.KeyLength + m.PayloadLength), nil
}

// ReadMessages parses a stream of messages into a parsed entity
func ReadMessages(r io.Reader) ([]*Message, int, error) {
	var messages []*Message

	var readBytes = 0
	for {
		m, messageBytes, err := ReadMessage(r)

		if err == io.EOF {
			return messages, readBytes, nil
		}

		readBytes = readBytes + messageBytes

		if err != nil {
			panic(err)
		}

		messages = append(messages, m)
	}
}

// ReadMessage reads a message file into a slice of messages
func ReadMessage(r io.Reader) (*Message, int, error) {
	// Message header contains the length of the message

	var crc int32
	err := binary.Read(r, binary.BigEndian, &crc) // Reads 8
	if err != nil {
		return nil, 0, err
	}

	// Key
	var keyLength int32
	err = binary.Read(r, binary.BigEndian, &keyLength) // Reads 8
	if err != nil {
		return nil, 0, err
	}

	key := make([]byte, keyLength) // Reads len payload
	_, err = r.Read(key)

	// Payload
	var payloadLength int32
	err = binary.Read(r, binary.BigEndian, &payloadLength) // Reads 8
	if err != nil {
		return nil, 0, err
	}

	payload := make([]byte, payloadLength) // Reads len payload
	_, err = r.Read(payload)

	if err != nil {
		return nil, 0, err
	}

	return &Message{crc, keyLength, key, payloadLength, payload}, 8 + 8 + 8 + int(keyLength) + int(payloadLength), nil
}
