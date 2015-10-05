package kuling

import (
	"fmt"
	"sync"

	"stathat.com/c/consistent"
)

// Broker b
type Broker struct {
	// group name to consistent hash group
	groups map[string]*consistent.Consistent
	// inflight iterators. Iterator ID to iterator
	inflight     map[string]string
	inflightlock sync.RWMutex

	sharder   Sharder
	iterStore IterStore
}

// NewBroker creates a new broker
func NewBroker(sharder Sharder, iterStore IterStore) *Broker {
	if sharder == nil {
		panic("broker: cannot use nil sharder")
	}
	if iterStore == nil {
		panic("broker: cannot use nil iterstore")
	}

	return &Broker{
		make(map[string]*consistent.Consistent),
		make(map[string]string),
		sync.RWMutex{},
		sharder,
		iterStore,
	}
}

// Iters returns a set of iterators for the client.
func (b *Broker) Iters(group, client, topic string) ([]string, error) {
	var grp *consistent.Consistent
	var ok bool

	if grp, ok = b.groups[group]; !ok {
		grp = consistent.New()
		grp.Add(client)
		b.groups[group] = grp
	} else if !b.groupHasClient(grp, client) {
		grp.Add(client)
	}

	// For all shards in the topic find the shards that this client should
	// iterate over
	var shards map[string]*Shard
	var err error
	if shards, err = b.sharder.Shards(topic); err != nil {
		return nil, fmt.Errorf("broker: sharder did not return shards: %s", err)
	}

	// Get all persisted iterators for the grup and topic
	groupIters, err := b.iterStore.GetAll(group, topic)
	if err != nil {
		return nil, fmt.Errorf("broker: issue fetching group iters: %s", err)
	}

	var clientIters []string
	for shard := range shards {
		if shardOwner, _ := grp.Get(shard); shardOwner == client {

			iterID := createIterID(group, topic, shard)

			var iter string
			if offset, ok := groupIters[iterID]; ok {
				iter = createIterFromIDAndOffset(iterID, offset)
			} else {
				iter = createIterFromIDAndOffset(iterID, 0)
			}

			clientIters = append(clientIters, iter)
			b.inflightlock.Lock()
			b.inflight[iterID] = iter
			b.inflightlock.Unlock()
		}
	}

	return clientIters, nil
}

func (b *Broker) groupHasClient(g *consistent.Consistent, client string) bool {
	if len(g.Members()) == 0 {
		return false
	}

	for _, c := range g.Members() {
		if c == client {
			return true
		}
	}

	return false
}

// Commit c
func (b *Broker) Commit(iterID string, offset int64) (string, error) {
	if _, ok := b.inflight[iterID]; ok {
		if err := b.iterStore.Commit(iterID, offset); err != nil {
			return "", fmt.Errorf("broker: commit to iter store failed: %s", err)
		}

		return iterID, nil
	}

	return "", fmt.Errorf("broker: commiting iterator that is not in flight %v", iterID)
}
