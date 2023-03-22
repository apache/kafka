# Implementation

## 5.1 Network Layer {#networklayer .anchor-link}

The network layer is a fairly straight-forward NIO server, and will not
be described in great detail. The sendfile implementation is done by
giving the `TransferableRecords` interface a `writeTo` method. This
allows the file-backed message set to use the more efficient
`transferTo` implementation instead of an in-process buffered write. The
threading model is a single acceptor thread and *N* processor threads
which handle a fixed number of connections each. This design has been
pretty thoroughly tested
[elsewhere](https://web.archive.org/web/20120619234320/http://sna-projects.com/blog/2009/08/introducing-the-nio-socketserver-implementation/)
and found to be simple to implement and fast. The protocol is kept quite
simple to allow for future implementation of clients in other languages.

## 5.2 Messages {#messages .anchor-link}

Messages consist of a variable-length header, a variable-length opaque
key byte array and a variable-length opaque value byte array. The format
of the header is described in the following section. Leaving the key and
value opaque is the right decision: there is a great deal of progress
being made on serialization libraries right now, and any particular
choice is unlikely to be right for all uses. Needless to say a
particular application using Kafka would likely mandate a particular
serialization type as part of its usage. The `RecordBatch` interface is
simply an iterator over messages with specialized methods for bulk
reading and writing to an NIO `Channel`.

## 5.3 Message Format {#messageformat .anchor-link}

Messages (aka Records) are always written in batches. The technical term
for a batch of messages is a record batch, and a record batch contains
one or more records. In the degenerate case, we could have a record
batch containing a single record. Record batches and records have their
own headers. The format of each is described below.

### 5.3.1 Record Batch {#recordbatch .anchor-link}

The following is the on-disk format of a RecordBatch.

```
baseOffset: int64
batchLength: int32
partitionLeaderEpoch: int32
magic: int8 (current magic value is 2)
crc: int32
attributes: int16
    bit 0~2:
        0: no compression
        1: gzip
        2: snappy
        3: lz4
        4: zstd
    bit 3: timestampType
    bit 4: isTransactional (0 means not transactional)
    bit 5: isControlBatch (0 means not a control batch)
    bit 6: hasDeleteHorizonMs (0 means baseTimestamp is not set as the delete horizon for compaction)
    bit 7~15: unused
lastOffsetDelta: int32
baseTimestamp: int64
maxTimestamp: int64
producerId: int64
producerEpoch: int16
baseSequence: int32
records: [Record]
```

Note that when compression is enabled, the compressed record data is
serialized directly following the count of the number of records.

The CRC covers the data from the attributes to the end of the batch
(i.e. all the bytes that follow the CRC). It is located after the magic
byte, which means that clients must parse the magic byte before deciding
how to interpret the bytes between the batch length and the magic byte.
The partition leader epoch field is not included in the CRC computation
to avoid the need to recompute the CRC when this field is assigned for
every batch that is received by the broker. The CRC-32C (Castagnoli)
polynomial is used for the computation.

On compaction: unlike the older message formats, magic v2 and above
preserves the first and last offset/sequence numbers from the original
batch when the log is cleaned. This is required in order to be able to
restore the producer\'s state when the log is reloaded. If we did not
retain the last sequence number, for example, then after a partition
leader failure, the producer might see an OutOfSequence error. The base
sequence number must be preserved for duplicate checking (the broker
checks incoming Produce requests for duplicates by verifying that the
first and last sequence numbers of the incoming batch match the last
from that producer). As a result, it is possible to have empty batches
in the log when all the records in the batch are cleaned but batch is
still retained in order to preserve a producer\'s last sequence number.
One oddity here is that the baseTimestamp field is not preserved during
compaction, so it will change if the first record in the batch is
compacted away.

Compaction may also modify the baseTimestamp if the record batch
contains records with a null payload or aborted transaction markers. The
baseTimestamp will be set to the timestamp of when those records should
be deleted with the delete horizon attribute bit also set.

#### 5.3.1.1 Control Batches {#controlbatch .anchor-link}

A control batch contains a single record called the control record.
Control records should not be passed on to applications. Instead, they
are used by consumers to filter out aborted transactional messages.

The key of a control record conforms to the following schema:

```
version: int16 (current version is 0)
type: int16 (0 indicates an abort marker, 1 indicates a commit)
```

The schema for the value of a control record is dependent on the type.
The value is opaque to clients.

### 5.3.2 Record {#record .anchor-link}

Record level headers were introduced in Kafka 0.11.0. The on-disk format
of a record with Headers is delineated below.

```
length: varint
attributes: int8
    bit 0~7: unused
timestampDelta: varlong
offsetDelta: varint
keyLength: varint
key: byte[]
valueLen: varint
value: byte[]
Headers => [Header]
```

#### 5.3.2.1 Record Header {#recordheader .anchor-link}

```
headerKeyLength: varint
headerKey: String
headerValueLength: varint
Value: byte[]
```

We use the same varint encoding as Protobuf. More information on the
latter can be found
[here](https://developers.google.com/protocol-buffers/docs/encoding#varints).
The count of headers in a record is also encoded as a varint.

### 5.3.3 Old Message Format {#messageset .anchor-link}

Prior to Kafka 0.11, messages were transferred and stored in *message
sets*. In a message set, each message has its own metadata. Note that
although message sets are represented as an array, they are not preceded
by an int32 array size like other array elements in the protocol.

**Message Set:**

```
MessageSet (Version: 0) => [offset message_size message]
offset => INT64
message_size => INT32
message => crc magic_byte attributes key value
    crc => INT32
    magic_byte => INT8
    attributes => INT8
        bit 0~2:
            0: no compression
            1: gzip
            2: snappy
        bit 3~7: unused
    key => BYTES
    value => BYTES
```

```
MessageSet (Version: 1) => [offset message_size message]
offset => INT64
message_size => INT32
message => crc magic_byte attributes timestamp key value
    crc => INT32
    magic_byte => INT8
    attributes => INT8
        bit 0~2:
            0: no compression
            1: gzip
            2: snappy
            3: lz4
        bit 3: timestampType
            0: create time
            1: log append time
        bit 4~7: unused
    timestamp => INT64
    key => BYTES
    value => BYTES
```

In versions prior to Kafka 0.10, the only supported message format
version (which is indicated in the magic value) was 0. Message format
version 1 was introduced with timestamp support in version 0.10.

-   Similarly to version 2 above, the lowest bits of attributes
    represent the compression type.
-   In version 1, the producer should always set the timestamp type bit
    to 0. If the topic is configured to use log append time, (through
    either broker level config log.message.timestamp.type =
    LogAppendTime or topic level config message.timestamp.type =
    LogAppendTime), the broker will overwrite the timestamp type and the
    timestamp in the message set.
-   The highest bits of attributes must be set to 0.

In message format versions 0 and 1 Kafka supports recursive messages to
enable compression. In this case the message\'s attributes must be set
to indicate one of the compression types and the value field will
contain a message set compressed with that type. We often refer to the
nested messages as \"inner messages\" and the wrapping message as the
\"outer message.\" Note that the key should be null for the outer
message and its offset will be the offset of the last inner message.

When receiving recursive version 0 messages, the broker decompresses
them and each inner message is assigned an offset individually. In
version 1, to avoid server side re-compression, only the wrapper message
will be assigned an offset. The inner messages will have relative
offsets. The absolute offset can be computed using the offset from the
outer message, which corresponds to the offset assigned to the last
inner message.

The crc field contains the CRC32 (and not CRC-32C) of the subsequent
message bytes (i.e. from magic byte to the value).

## 5.4 Log {#log .anchor-link}

A log for a topic named \"my-topic\" with two partitions consists of two
directories (namely `my-topic-0` and `my-topic-1`) populated with data
files containing the messages for that topic. The format of the log
files is a sequence of \"log entries\"; each log entry is a 4 byte
integer *N* storing the message length which is followed by the *N*
message bytes. Each message is uniquely identified by a 64-bit integer
*offset* giving the byte position of the start of this message in the
stream of all messages ever sent to that topic on that partition. The
on-disk format of each message is given below. Each log file is named
with the offset of the first message it contains. So the first file
created will be 00000000000000000000.log, and each additional file will
have an integer name roughly *S* bytes from the previous file where *S*
is the max log file size given in the configuration.

The exact binary format for records is versioned and maintained as a
standard interface so record batches can be transferred between
producer, broker, and client without recopying or conversion when
desirable. The previous section included details about the on-disk
format of records.

The use of the message offset as the message id is unusual. Our original
idea was to use a GUID generated by the producer, and maintain a mapping
from GUID to offset on each broker. But since a consumer must maintain
an ID for each server, the global uniqueness of the GUID provides no
value. Furthermore, the complexity of maintaining the mapping from a
random id to an offset requires a heavy weight index structure which
must be synchronized with disk, essentially requiring a full persistent
random-access data structure. Thus to simplify the lookup structure we
decided to use a simple per-partition atomic counter which could be
coupled with the partition id and node id to uniquely identify a
message; this makes the lookup structure simpler, though multiple seeks
per consumer request are still likely. However once we settled on a
counter, the jump to directly using the offset seemed natural---both
after all are monotonically increasing integers unique to a partition.
Since the offset is hidden from the consumer API this decision is
ultimately an implementation detail and we went with the more efficient
approach.

![](kafka_log.png)

### Writes {#impl_writes .anchor-link}

The log allows serial appends which always go to the last file. This
file is rolled over to a fresh file when it reaches a configurable size
(say 1GB). The log takes two configuration parameters: *M*, which gives
the number of messages to write before forcing the OS to flush the file
to disk, and *S*, which gives a number of seconds after which a flush is
forced. This gives a durability guarantee of losing at most *M* messages
or *S* seconds of data in the event of a system crash.

### Reads {#impl_reads .anchor-link}

Reads are done by giving the 64-bit logical offset of a message and an
*S*-byte max chunk size. This will return an iterator over the messages
contained in the *S*-byte buffer. *S* is intended to be larger than any
single message, but in the event of an abnormally large message, the
read can be retried multiple times, each time doubling the buffer size,
until the message is read successfully. A maximum message and buffer
size can be specified to make the server reject messages larger than
some size, and to give a bound to the client on the maximum it needs to
ever read to get a complete message. It is likely that the read buffer
ends with a partial message, this is easily detected by the size
delimiting.

The actual process of reading from an offset requires first locating the
log segment file in which the data is stored, calculating the
file-specific offset from the global offset value, and then reading from
that file offset. The search is done as a simple binary search variation
against an in-memory range maintained for each file.

The log provides the capability of getting the most recently written
message to allow clients to start subscribing as of \"right now\". This
is also useful in the case the consumer fails to consume its data within
its SLA-specified number of days. In this case when the client attempts
to consume a non-existent offset it is given an OutOfRangeException and
can either reset itself or fail as appropriate to the use case.

The following is the format of the results sent to the consumer.

```
MessageSetSend (fetch result)

total length     : 4 bytes
error code       : 2 bytes
message 1        : x bytes
...
message n        : x bytes
```

```
MultiMessageSetSend (multiFetch result)

total length       : 4 bytes
error code         : 2 bytes
messageSetSend 1
...
messageSetSend n
```

### Deletes {#impl_deletes .anchor-link}

Data is deleted one log segment at a time. The log manager applies two
metrics to identify segments which are eligible for deletion: time and
size. For time-based policies, the record timestamps are considered,
with the largest timestamp in a segment file (order of records is not
relevant) defining the retention time for the entire segment. Size-based
retention is disabled by default. When enabled the log manager keeps
deleting the oldest segment file until the overall size of the partition
is within the configured limit again. If both policies are enabled at
the same time, a segment that is eligible for deletion due to either
policy will be deleted. To avoid locking reads while still allowing
deletes that modify the segment list we use a copy-on-write style
segment list implementation that provides consistent views to allow a
binary search to proceed on an immutable static snapshot view of the log
segments while deletes are progressing.

### Guarantees {#impl_guarantees .anchor-link}

The log provides a configuration parameter *M* which controls the
maximum number of messages that are written before forcing a flush to
disk. On startup a log recovery process is run that iterates over all
messages in the newest log segment and verifies that each message entry
is valid. A message entry is valid if the sum of its size and offset are
less than the length of the file AND the CRC32 of the message payload
matches the CRC stored with the message. In the event corruption is
detected the log is truncated to the last valid offset.

Note that two kinds of corruption must be handled: truncation in which
an unwritten block is lost due to a crash, and corruption in which a
nonsense block is ADDED to the file. The reason for this is that in
general the OS makes no guarantee of the write order between the file
inode and the actual block data so in addition to losing written data
the file can gain nonsense data if the inode is updated with a new size
but a crash occurs before the block containing that data is written. The
CRC detects this corner case, and prevents it from corrupting the log
(though the unwritten messages are, of course, lost).

## 5.5 Distribution {#distributionimpl .anchor-link}

### Consumer Offset Tracking {#impl_offsettracking .anchor-link}

Kafka consumer tracks the maximum offset it has consumed in each
partition and has the capability to commit offsets so that it can resume
from those offsets in the event of a restart. Kafka provides the option
to store all the offsets for a given consumer group in a designated
broker (for that group) called the group coordinator. i.e., any consumer
instance in that consumer group should send its offset commits and
fetches to that group coordinator (broker). Consumer groups are assigned
to coordinators based on their group names. A consumer can look up its
coordinator by issuing a FindCoordinatorRequest to any Kafka broker and
reading the FindCoordinatorResponse which will contain the coordinator
details. The consumer can then proceed to commit or fetch offsets from
the coordinator broker. In case the coordinator moves, the consumer will
need to rediscover the coordinator. Offset commits can be done
automatically or manually by consumer instance.

When the group coordinator receives an OffsetCommitRequest, it appends
the request to a special [compacted](../design#compaction) Kafka topic named
*\_\_consumer_offsets*. The broker sends a successful offset commit
response to the consumer only after all the replicas of the offsets
topic receive the offsets. In case the offsets fail to replicate within
a configurable timeout, the offset commit will fail and the consumer may
retry the commit after backing off. The brokers periodically compact the
offsets topic since it only needs to maintain the most recent offset
commit per partition. The coordinator also caches the offsets in an
in-memory table in order to serve offset fetches quickly.

When the coordinator receives an offset fetch request, it simply returns
the last committed offset vector from the offsets cache. In case
coordinator was just started or if it just became the coordinator for a
new set of consumer groups (by becoming a leader for a partition of the
offsets topic), it may need to load the offsets topic partition into the
cache. In this case, the offset fetch will fail with an
CoordinatorLoadInProgressException and the consumer may retry the
OffsetFetchRequest after backing off.

### ZooKeeper Directories {#impl_zookeeper .anchor-link}

The following gives the ZooKeeper structures and algorithms used for
co-ordination between consumers and brokers.

### Notation {#impl_zknotation .anchor-link}

When an element in a path is denoted `[xyz]`, that means that the value
of xyz is not fixed and there is in fact a ZooKeeper znode for each
possible value of xyz. For example `/topics/[topic]` would be a
directory named /topics containing a sub-directory for each topic name.
Numerical ranges are also given such as `[0...5]` to indicate the
subdirectories 0, 1, 2, 3, 4. An arrow `->` is used to indicate the
contents of a znode. For example `/hello -> world` would indicate a
znode /hello containing the value \"world\".

### Broker Node Registry {#impl_zkbroker .anchor-link}

```
/brokers/ids/[0...N] --> {"jmx_port":...,"timestamp":...,"endpoints":[...],"host":...,"version":...,"port":...} (ephemeral node)
```

This is a list of all present broker nodes, each of which provides a
unique logical broker id which identifies it to consumers (which must be
given as part of its configuration). On startup, a broker node registers
itself by creating a znode with the logical broker id under
/brokers/ids. The purpose of the logical broker id is to allow a broker
to be moved to a different physical machine without affecting consumers.
An attempt to register a broker id that is already in use (say because
two servers are configured with the same broker id) results in an error.

Since the broker registers itself in ZooKeeper using ephemeral znodes,
this registration is dynamic and will disappear if the broker is
shutdown or dies (thus notifying consumers it is no longer available).

### Broker Topic Registry {#impl_zktopic .anchor-link}

```
/brokers/topics/[topic]/partitions/[0...N]/state --> {"controller_epoch":...,"leader":...,"version":...,"leader_epoch":...,"isr":[...]} (ephemeral node)
```

Each broker registers itself under the topics it maintains and stores
the number of partitions for that topic.

### Cluster Id {#impl_clusterid .anchor-link}

The cluster id is a unique and immutable identifier assigned to a Kafka
cluster. The cluster id can have a maximum of 22 characters and the
allowed characters are defined by the regular expression
\[a-zA-Z0-9\_\\-\]+, which corresponds to the characters used by the
URL-safe Base64 variant with no padding. Conceptually, it is
auto-generated when a cluster is started for the first time.

Implementation-wise, it is generated when a broker with version 0.10.1
or later is successfully started for the first time. The broker tries to
get the cluster id from the `/cluster/id` znode during startup. If the
znode does not exist, the broker generates a new cluster id and creates
the znode with this cluster id.

### Broker node registration {#impl_brokerregistration .anchor-link}

The broker nodes are basically independent, so they only publish
information about what they have. When a broker joins, it registers
itself under the broker node registry directory and writes information
about its host name and port. The broker also register the list of
existing topics and their logical partitions in the broker topic
registry. New topics are registered dynamically when they are created on
the broker.
