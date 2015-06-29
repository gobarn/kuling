package kuling

import "fmt"

// ShardingStrategy strategy interface for fetching specific shard given
// shard name
type ShardingStrategy interface {
	Get(shard string) (Shard, error)
}

// FixedShardsShardingStrategy implements a sharding strategy by getting
// a fixed number of shards from start and never increase or decrease the
// number of shards.
type FixedShardsShardingStrategy struct {
	// the number of shards
	size int
	// map of shard key to shard
	shards map[string]Shard
	// factory function for creating shards
	factory func(name string) (Shard, error)
}

// NewFixedShardsShardingStrategy creates a new sharding strategy that has a
// fixed set of shards determined by the size parameter
func NewFixedShardsShardingStrategy(size int,
	factory func(name string) (Shard, error)) (*FixedShardsShardingStrategy, error) {
	var shards = make(map[string]Shard)

	for i := 0; i < size; i++ {
		shardKey := fmt.Sprintf("%d", i)
		shard, err := factory(shardKey)
		if err != nil {
			return nil, err
		}

		shards[shardKey] = shard
	}

	return &FixedShardsShardingStrategy{
			size,
			shards,
			factory},
		nil
}

// Get specific shard from the shard key. If the shard key do not exist
// an error is returned
func (fs *FixedShardsShardingStrategy) Get(shard string) (Shard, error) {
	if s, ok := fs.shards[shard]; ok {
		return s, nil
	}

	return nil, fmt.Errorf("topic: Unknown shard %s", shard)
}
