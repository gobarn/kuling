LIST : Describe system
->
<- [topic_names]

CREATE : Stream Create
->topic, :shards
<-OK/ERR

DESCRIBE : Topic shard description
->topic
<-:num_shards

PUT : Put records on topic
-> * topic, key, value
<- * :shard_num, :sequence_id

GET : Get records from topic
-> topic, :startSequenceID, :maxNumMessages
<- binary_messages


ITER_GET : Get iterator for topic shard given a group name, if the group does not exist it is created.
-> topic, :shard_num, group
<- :ownerID, :groupSequenceID
<- ERR/ERR_NOT_OWNER

ITER : Get next batch of messages given group iteration
-> :ownerID, :groupSequenceID
<- binary_messages

ITER_PUT : Put the groups current sequence ID in a topic
-> :ownerID, :groupSequnceID
