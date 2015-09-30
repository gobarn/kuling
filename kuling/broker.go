package kuling

import (
	"encoding/base64"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"

	"github.com/serialx/hashring"
)

// Iter has an ID for referenceing Iterators between users of the iterator.
// Iter is a forward iterator that reads a number of messages starting
// from the start sequence id and reading from there. It reads from a specific
// shard in a topic
type Iter struct {
	group  string
	topic  string
	shard  string
	offset int64
}

// IterEncode encodes the iterator into a base64 encoded string
func IterEncode(i *Iter) (string, error) {
	return base64.URLEncoding.EncodeToString(
			[]byte(
				fmt.Sprintf("%s/%s/%s/%d", i.group, i.topic, i.shard, i.offset),
			),
		),
		nil
}

// IterDecode decodes a base64 string into an iterator
func IterDecode(iter string) (*Iter, error) {
	arr := strings.Split(iter, "/")

	s, err := strconv.ParseInt(arr[3], 0, 64)
	if err != nil {
		return nil, err
	}

	it := &Iter{
		arr[0],
		arr[1],
		arr[2],
		s,
	}

	return it, nil
}

//
// // Iterators interface for constructs that can create and
// // invalidate iterators
// type Iterators interface {
// 	Get(iter string) (*Iter, bool)
// 	Inc(iter string, numMessages int64) (string, error)
// }

// Grouper interface for type that can handle group memberships
type Grouper interface {
	Join(cid, gid string) error
	Report(cid, gid string) bool
	Leave(cid, gid string) error
}

// IterIssuer interface for types that can issue and invalidate iterators
type IterIssuer interface {
	Issue(id, topic, shard string, startSequenceID int64) (iter string, err error)
	Invalidate(iter string) error
}

// Peerer gives list of peer IP
type Peerer interface {
	Peers() ([]string, error)
}

// Sharder returns a list of shard id:s for a topic
type Sharder interface {
	Shards(t string) ([]string, error)
}

type Offsetter interface {
	GetOffset(group, topic, shard string) (int64, error)
	SetOffset(group, topic, shard string, offset int64) error
}

type group struct {
	id      string
	clients *hashring.HashRing
	iters   map[string]string
	*sync.RWMutex
}

type Broker struct {
	groups    map[string]group
	sharder   Sharder
	peerer    Peerer
	offsetter Offsetter
	issuer    IterIssuer
}

func (b *Broker) Commit(iter string) error {
	if it, err := IterDecode(iter); err == nil {
		if err := b.offsetter.SetOffset(it.group, it.topic, it.shard, it.offset); err != nil {
			log.Println("broker: error while commiting iterator", iter, err)
			return fmt.Errorf("broker: commit unsuccessfull")
		}

		return nil
	}

	return fmt.Errorf("broker: could not decode iterator %s", iter)
}

// Iterators gets a list of iterators that are owned by the client
// making the request. The iterators contain IP, shard,
func (b *Broker) Iterators(c, g, t string) ([]string, error) {
	grp, ok := b.groups[g]
	if !ok {
		log.Println("broker: created new group", g)
		clients := make([]string, 1)

		grp = group{
			g,
			hashring.New(clients),
			make(map[string]string),
			&sync.RWMutex{},
		}

		b.groups[g] = grp
	}

	log.Println("broker: client", c, "added to group", g)

	// todo fred need better locking, this is not working
	grp.Lock()
	defer grp.Unlock()

	grp.clients.AddNode(c)

	// invalidate all existing iterators
	// todo-fred: invalidate only iterators that are affected

	for _, iter := range grp.iters {
		if err := b.issuer.Invalidate(iter); err != nil {
			return nil, err
		}
	}

	shards, err := b.sharder.Shards(t)
	if err != nil {
		return nil, err
	}

	clientShards := make([]string, 1)
	for _, s := range shards {
		if sc, ok := grp.clients.GetNode(s); ok {
			if sc == c {
				o, err := b.offsetter.GetOffset(g, t, s)
				if err != nil {
					return nil, err
				}

				iter, err := IterEncode(&Iter{g, t, s, o})
				if err != nil {
					return nil, fmt.Errorf("broker: %s", err)
				}

				clientShards = append(clientShards, iter)
			}
		}
	}

	return clientShards, nil
}
