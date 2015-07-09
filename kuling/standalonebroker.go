package kuling

type ShardAndIP struct {
	Shard string
	Addr  string
}

type Broker struct {
}

// GetShardIterators on a topic for a clientID belonging to a group.
func (b *Broker) GetShardIterators(clientID, group, topic string) ([]ShardAndIP, error) {
	return nil, nil
}
