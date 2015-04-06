package kuling

import (
	"bufio"
	"bytes"
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
func WriteMessage(w *bufio.Writer, m *Message) (int64, error) {
	buffer := bytes.NewBuffer(make([]byte, 0))
	// Write checksum
	binary.Write(buffer, binary.BigEndian, &m.Crc)
	// Write key length
	binary.Write(buffer, binary.BigEndian, &m.KeyLength)
	// Write key
	buffer.Write(m.Key)
	// Write payload length
	binary.Write(buffer, binary.BigEndian, &m.PayloadLength)
	// Write payload
	buffer.Write(m.Payload)

	n, err := w.Write(buffer.Bytes())

	if err != nil {
		return 0, err
	}

	err = w.Flush()

	if err != nil {
		return 0, err
	}

	return int64(n), nil
}

// ReadMessages reads all messages in bufio
func ReadMessages(r *bufio.Reader) ([]*Message, error) {
	var messages []*Message

	for {
		m, _, err := ReadMessage(r)

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
func ReadMessage(r *bufio.Reader) (*Message, int, error) {
	// Message header contains the length of the message

	//magic, err := r.ReadByte() // Reads 1
	// if err != nil {
	// 	return nil, 0, err
	// }
	//
	// attributes, err := r.ReadByte() // Reads 1
	// if err != nil {
	// 	return nil, 0, err
	// }

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
