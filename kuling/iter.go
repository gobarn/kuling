package kuling

import (
	"fmt"
	"strconv"
	"strings"
)

// Iter has an ID for referenceing Iterators between users of the iterator.
// Iter is a forward iterator that reads a number of messages starting
// from the start sequence id and reading from there. It reads from a specific
// shard in a topic
type Iter struct {
	group  string // the group that owns the iterator
	topic  string // topic to read from
	shard  string // shard in the topic for this iterator
	offset int64  // current offset of the iterator
}

func createIter(group, topic, shard string) string {
	return fmt.Sprintf("%s/%s/%s/%d", group, topic, shard, 0)
}

func createIterID(group, topic, shard string) string {
	return fmt.Sprintf("%s/%s/%s", group, topic, shard)
}

func createIterFromIDAndOffset(iterID string, offset int64) string {
	return fmt.Sprintf("%s/%d", iterID, offset)
}

// ID of the iterator which is what makes the iterator unique.
func (i Iter) ID() string {
	return i.group + i.topic + i.shard
}

// IterEncode encodes the iterator into a base64 encoded string
func IterEncode(i Iter) (string, error) {
	return fmt.Sprintf("%s/%s/%s/%d", i.group, i.topic, i.shard, i.offset), nil
}

// IterDecode decodes a base64 string into an iterator
func IterDecode(iter string) (Iter, error) {
	arr := strings.Split(iter, "/")

	offset, err := strconv.ParseInt(arr[3], 0, 64)
	if err != nil {
		return Iter{}, err
	}

	it := Iter{
		group:  arr[0],
		topic:  arr[1],
		shard:  arr[2],
		offset: offset,
	}

	return it, nil
}
