LIST : Describe system
->
<- [topic_names]

CREATE : Stream Create
->topic, :shards
<-OK/ERR

DESCRIBE : Topic shard description
->topic
<- [shardID:s]

PUT : Put records on topic
-> * topic, shardKey, data
<- OK/ERR

PUTS : Put records on topic with set sharding hash. This way the client can control
the place where a record is put to group records into one shard
-> * topic, shardKey, shardHash, data
<- OK/ERR

GET : Get records from topic
-> topic, shard, :startSequenceID, :maxNumMessages
<- binary_messages




THE ITERATORS SHOLD BE OWNED BY THE SERVER, NOT THE BROKER! BROKER SENDS
ITERATORS DOWN TO THE SERVER SO THERE IS NEVER SERVER->BROKER communication,
THE BROKER ISSUES ITERATORS AND TELLS THE CLIENT WHICH IP THAT HOLD THE ITERATORS
THIS IS POSSIBLE SINCE THE ITERATOR IS PER SHARD

ITERATION:

GRP_JOIN : client joins a group
-> group, client
<- OK/ERR

GRP_HB : Group heart beat
-> group, client

GRP_LEAVE : issue leave cmd for client
-> group, client
<- OK/ERR

ITER_GET : Get iterator for topic shard given a group name. There can only be one iterator per client/group/topic/shard
whenever a new client tries to get a iterator a rebalance occur on the server side which forces all other clients
to re-issue a new ITER_GET command as their ITER command will not return a nextIterator
-> topic, shard
<- iterator <iteartor = IP/shardID/sequenceNumber/maxNumMessages>
<- ERR/ERR_NOT_OWNER



ITER : Get next batch of messages given group iterator. The iterator contains the shard and the position in that shard
-> iterator
<- nextIterator, binary_messages



Iter:

* Only against Broker?
* Gets iterator from Broker for each Shard in a Topic it should read.
* Calls Broker for each call?
*


Describe:
-> List
   -> Topic
      -> Shards


ITERS request:
* From client to Broker
* Get list of iterators

ITER
* contains GET information (server-ip, topic, shard, offset)

GET (topic=cats,group=merchant-reports,client_id=<random_uuid>):
C -> B
C <-

ITER-IN-FLIGHT:
* Time for which the broker will keep a ITER in flight and not consider it used by the GROUP/CLIENT

Commit ITER + ACTUAL_NUM_MESSAGES_READ_BY_CLIENT:
* Stores the iterator for the GROUP
* Returns NEXT ITER
