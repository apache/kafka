/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import java.util.Properties
import kafka.message.{MessageSet, Message}
import kafka.consumer.ConsumerConfig
import kafka.utils.{VerifiableProperties, ZKConfig, Utils}

/**
 * Configuration settings for the kafka server
 */
class KafkaConfig private (val props: VerifiableProperties) extends ZKConfig(props) {

  def this(originalProps: Properties) {
    this(new VerifiableProperties(originalProps))
    props.verify()
  }

  private def getLogRetentionTimeMillis(): Long = {
    val millisInMinute = 60L * 1000L
    val millisInHour = 60L * millisInMinute
    
    if(props.containsKey("log.retention.ms")){
       props.getIntInRange("log.retention.ms", (1, Int.MaxValue))
    }
    else if(props.containsKey("log.retention.minutes")){
       millisInMinute * props.getIntInRange("log.retention.minutes", (1, Int.MaxValue))
    } 
    else {
       millisInHour * props.getIntInRange("log.retention.hours", 24*7, (1, Int.MaxValue))
    }
  }

  private def getLogRollTimeMillis(): Long = {
    val millisInHour = 60L * 60L * 1000L
    
    if(props.containsKey("log.roll.ms")){
       props.getIntInRange("log.roll.ms", (1, Int.MaxValue))
    }
    else {
       millisInHour * props.getIntInRange("log.roll.hours", 24*7, (1, Int.MaxValue))
    }
  }

  private def getLogRollTimeJitterMillis(): Long = {
    val millisInHour = 60L * 60L * 1000L

    if(props.containsKey("log.roll.jitter.ms")) {
      props.getIntInRange("log.roll.jitter.ms", (0, Int.MaxValue))
    }
    else {
      millisInHour * props.getIntInRange("log.roll.jitter.hours", 0, (0, Int.MaxValue))
    }
  }

  /*********** General Configuration ***********/

  /* the broker id for this server */
  val brokerId: Int = props.getIntInRange("broker.id", (0, Int.MaxValue))

  /* the maximum size of message that the server can receive */
  val messageMaxBytes = props.getIntInRange("message.max.bytes", 1000000 + MessageSet.LogOverhead, (0, Int.MaxValue))

  /* the number of network threads that the server uses for handling network requests */
  val numNetworkThreads = props.getIntInRange("num.network.threads", 3, (1, Int.MaxValue))

  /* the number of io threads that the server uses for carrying out network requests */
  val numIoThreads = props.getIntInRange("num.io.threads", 8, (1, Int.MaxValue))

  /* the number of threads to use for various background processing tasks */
  val backgroundThreads = props.getIntInRange("background.threads", 10, (1, Int.MaxValue))

  /* the number of queued requests allowed before blocking the network threads */
  val queuedMaxRequests = props.getIntInRange("queued.max.requests", 500, (1, Int.MaxValue))

  /*********** Socket Server Configuration ***********/

  /* the port to listen and accept connections on */
  val port: Int = props.getInt("port", 9092)

  /* hostname of broker. If this is set, it will only bind to this address. If this is not set,
   * it will bind to all interfaces */
  val hostName: String = props.getString("host.name", null)

  /* hostname to publish to ZooKeeper for clients to use. In IaaS environments, this may
   * need to be different from the interface to which the broker binds. If this is not set,
   * it will use the value for "host.name" if configured. Otherwise
   * it will use the value returned from java.net.InetAddress.getCanonicalHostName(). */
  val advertisedHostName: String = props.getString("advertised.host.name", hostName)

  /* the port to publish to ZooKeeper for clients to use. In IaaS environments, this may
   * need to be different from the port to which the broker binds. If this is not set,
   * it will publish the same port that the broker binds to. */
  val advertisedPort: Int = props.getInt("advertised.port", port)

  /* the SO_SNDBUFF buffer of the socket sever sockets */
  val socketSendBufferBytes: Int = props.getInt("socket.send.buffer.bytes", 100*1024)

  /* the SO_RCVBUFF buffer of the socket sever sockets */
  val socketReceiveBufferBytes: Int = props.getInt("socket.receive.buffer.bytes", 100*1024)

  /* the maximum number of bytes in a socket request */
  val socketRequestMaxBytes: Int = props.getIntInRange("socket.request.max.bytes", 100*1024*1024, (1, Int.MaxValue))
  
  /* the maximum number of connections we allow from each ip address */
  val maxConnectionsPerIp: Int = props.getIntInRange("max.connections.per.ip", Int.MaxValue, (1, Int.MaxValue))
  
  /* per-ip or hostname overrides to the default maximum number of connections */
  val maxConnectionsPerIpOverrides = props.getMap("max.connections.per.ip.overrides").map(entry => (entry._1, entry._2.toInt))

  /* idle connections timeout: the server socket processor threads close the connections that idle more than this */
  val connectionsMaxIdleMs = props.getLong("connections.max.idle.ms", 10*60*1000L)

  /*********** Log Configuration ***********/

  /* the default number of log partitions per topic */
  val numPartitions = props.getIntInRange("num.partitions", 1, (1, Int.MaxValue))

  /* the directories in which the log data is kept */
  val logDirs = Utils.parseCsvList(props.getString("log.dirs", props.getString("log.dir", "/tmp/kafka-logs")))
  require(logDirs.size > 0)

  /* the maximum size of a single log file */
  val logSegmentBytes = props.getIntInRange("log.segment.bytes", 1*1024*1024*1024, (Message.MinHeaderSize, Int.MaxValue))

  /* the maximum time before a new log segment is rolled out */
  val logRollTimeMillis = getLogRollTimeMillis

  /* the maximum jitter to subtract from logRollTimeMillis */
  val logRollTimeJitterMillis = getLogRollTimeJitterMillis

  /* the number of hours to keep a log file before deleting it */
  val logRetentionTimeMillis = getLogRetentionTimeMillis

  /* the maximum size of the log before deleting it */
  val logRetentionBytes = props.getLong("log.retention.bytes", -1)

  /* the frequency in minutes that the log cleaner checks whether any log is eligible for deletion */
  val logCleanupIntervalMs = props.getLongInRange("log.retention.check.interval.ms", 5*60*1000, (1, Long.MaxValue))

  /* the default cleanup policy for segments beyond the retention window, must be either "delete" or "compact" */
  val logCleanupPolicy = props.getString("log.cleanup.policy", "delete")

  /* the number of background threads to use for log cleaning */
  val logCleanerThreads = props.getIntInRange("log.cleaner.threads", 1, (0, Int.MaxValue))

  /* the log cleaner will be throttled so that the sum of its read and write i/o will be less than this value on average */
  val logCleanerIoMaxBytesPerSecond = props.getDouble("log.cleaner.io.max.bytes.per.second", Double.MaxValue)

  /* the total memory used for log deduplication across all cleaner threads */
  val logCleanerDedupeBufferSize = props.getLongInRange("log.cleaner.dedupe.buffer.size", 500*1024*1024L, (0, Long.MaxValue))
  require(logCleanerDedupeBufferSize / logCleanerThreads > 1024*1024, "log.cleaner.dedupe.buffer.size must be at least 1MB per cleaner thread.")

  /* the total memory used for log cleaner I/O buffers across all cleaner threads */
  val logCleanerIoBufferSize = props.getIntInRange("log.cleaner.io.buffer.size", 512*1024, (0, Int.MaxValue))

  /* log cleaner dedupe buffer load factor. The percentage full the dedupe buffer can become. A higher value
   * will allow more log to be cleaned at once but will lead to more hash collisions */
  val logCleanerDedupeBufferLoadFactor = props.getDouble("log.cleaner.io.buffer.load.factor", 0.9d)

  /* the amount of time to sleep when there are no logs to clean */
  val logCleanerBackoffMs = props.getLongInRange("log.cleaner.backoff.ms", 15*1000, (0L, Long.MaxValue))

  /* the minimum ratio of dirty log to total log for a log to eligible for cleaning */
  val logCleanerMinCleanRatio = props.getDouble("log.cleaner.min.cleanable.ratio", 0.5)

  /* should we enable log cleaning? */
  val logCleanerEnable = props.getBoolean("log.cleaner.enable", false)

  /* how long are delete records retained? */
  val logCleanerDeleteRetentionMs = props.getLong("log.cleaner.delete.retention.ms", 24 * 60 * 60 * 1000L)

  /* the maximum size in bytes of the offset index */
  val logIndexSizeMaxBytes = props.getIntInRange("log.index.size.max.bytes", 10*1024*1024, (4, Int.MaxValue))

  /* the interval with which we add an entry to the offset index */
  val logIndexIntervalBytes = props.getIntInRange("log.index.interval.bytes", 4096, (0, Int.MaxValue))

  /* the number of messages accumulated on a log partition before messages are flushed to disk */
  val logFlushIntervalMessages = props.getLongInRange("log.flush.interval.messages", Long.MaxValue, (1, Long.MaxValue))

  /* the amount of time to wait before deleting a file from the filesystem */
  val logDeleteDelayMs = props.getLongInRange("log.segment.delete.delay.ms", 60000, (0, Long.MaxValue))

  /* the frequency in ms that the log flusher checks whether any log needs to be flushed to disk */
  val logFlushSchedulerIntervalMs = props.getLong("log.flush.scheduler.interval.ms",  Long.MaxValue)

  /* the maximum time in ms that a message in any topic is kept in memory before flushed to disk */
  val logFlushIntervalMs = props.getLong("log.flush.interval.ms", logFlushSchedulerIntervalMs)

  /* the frequency with which we update the persistent record of the last flush which acts as the log recovery point */
  val logFlushOffsetCheckpointIntervalMs = props.getIntInRange("log.flush.offset.checkpoint.interval.ms", 60000, (0, Int.MaxValue))

  /* the number of threads per data directory to be used for log recovery at startup and flushing at shutdown */
  val numRecoveryThreadsPerDataDir = props.getIntInRange("num.recovery.threads.per.data.dir", 1, (1, Int.MaxValue))

  /* enable auto creation of topic on the server */
  val autoCreateTopicsEnable = props.getBoolean("auto.create.topics.enable", true)

  /* define the minimum number of replicas in ISR needed to satisfy a produce request with required.acks=-1 (or all) */
  val minInSyncReplicas = props.getIntInRange("min.insync.replicas",1,(1,Int.MaxValue))



  /*********** Replication configuration ***********/

  /* the socket timeout for controller-to-broker channels */
  val controllerSocketTimeoutMs = props.getInt("controller.socket.timeout.ms", 30000)

  /* the buffer size for controller-to-broker-channels */
  val controllerMessageQueueSize= props.getInt("controller.message.queue.size", Int.MaxValue)

  /* default replication factors for automatically created topics */
  val defaultReplicationFactor = props.getInt("default.replication.factor", 1)

  /* If a follower hasn't sent any fetch requests during this time, the leader will remove the follower from isr */
  val replicaLagTimeMaxMs = props.getLong("replica.lag.time.max.ms", 10000)

  /* If the lag in messages between a leader and a follower exceeds this number, the leader will remove the follower from isr */
  val replicaLagMaxMessages = props.getLong("replica.lag.max.messages", 4000)

  /* the socket timeout for network requests. Its value should be at least replica.fetch.wait.max.ms. */
  val replicaSocketTimeoutMs = props.getInt("replica.socket.timeout.ms", ConsumerConfig.SocketTimeout)
  require(replicaFetchWaitMaxMs <= replicaSocketTimeoutMs, "replica.socket.timeout.ms should always be at least replica.fetch.wait.max.ms" +
    " to prevent unnecessary socket timeouts")

  /* the socket receive buffer for network requests */
  val replicaSocketReceiveBufferBytes = props.getInt("replica.socket.receive.buffer.bytes", ConsumerConfig.SocketBufferSize)

  /* the number of byes of messages to attempt to fetch */
  val replicaFetchMaxBytes = props.getIntInRange("replica.fetch.max.bytes", ConsumerConfig.FetchSize, (messageMaxBytes, Int.MaxValue))

  /* max wait time for each fetcher request issued by follower replicas. This value should always be less than the
  *  replica.lag.time.max.ms at all times to prevent frequent shrinking of ISR for low throughput topics */
  val replicaFetchWaitMaxMs = props.getInt("replica.fetch.wait.max.ms", 500)
  require(replicaFetchWaitMaxMs <= replicaLagTimeMaxMs, "replica.fetch.wait.max.ms should always be at least replica.lag.time.max.ms" +
                                                        " to prevent frequent changes in ISR")

  /* minimum bytes expected for each fetch response. If not enough bytes, wait up to replicaMaxWaitTimeMs */
  val replicaFetchMinBytes = props.getInt("replica.fetch.min.bytes", 1)

  /* number of fetcher threads used to replicate messages from a source broker.
   * Increasing this value can increase the degree of I/O parallelism in the follower broker. */
  val numReplicaFetchers = props.getInt("num.replica.fetchers", 1)

  /* the frequency with which the high watermark is saved out to disk */
  val replicaHighWatermarkCheckpointIntervalMs = props.getLong("replica.high.watermark.checkpoint.interval.ms", 5000L)

  /* the purge interval (in number of requests) of the fetch request purgatory */
  val fetchPurgatoryPurgeIntervalRequests = props.getInt("fetch.purgatory.purge.interval.requests", 1000)

  /* the purge interval (in number of requests) of the producer request purgatory */
  val producerPurgatoryPurgeIntervalRequests = props.getInt("producer.purgatory.purge.interval.requests", 1000)

  /* Enables auto leader balancing. A background thread checks and triggers leader
   * balance if required at regular intervals */
  val autoLeaderRebalanceEnable = props.getBoolean("auto.leader.rebalance.enable", true)

  /* the ratio of leader imbalance allowed per broker. The controller would trigger a leader balance if it goes above
   * this value per broker. The value is specified in percentage. */
  val leaderImbalancePerBrokerPercentage = props.getInt("leader.imbalance.per.broker.percentage", 10)

  /* the frequency with which the partition rebalance check is triggered by the controller */
  val leaderImbalanceCheckIntervalSeconds = props.getInt("leader.imbalance.check.interval.seconds", 300)

  /* indicates whether to enable replicas not in the ISR set to be elected as leader as a last resort, even though
   * doing so may result in data loss */
  val uncleanLeaderElectionEnable = props.getBoolean("unclean.leader.election.enable", true)

  /*********** Controlled shutdown configuration ***********/

  /** Controlled shutdown can fail for multiple reasons. This determines the number of retries when such failure happens */
  val controlledShutdownMaxRetries = props.getInt("controlled.shutdown.max.retries", 3)

  /** Before each retry, the system needs time to recover from the state that caused the previous failure (Controller
    * fail over, replica lag etc). This config determines the amount of time to wait before retrying. */
  val controlledShutdownRetryBackoffMs = props.getInt("controlled.shutdown.retry.backoff.ms", 5000)

  /* enable controlled shutdown of the server */
  val controlledShutdownEnable = props.getBoolean("controlled.shutdown.enable", default = true)

  /*********** Offset management configuration ***********/

  /* the maximum size for a metadata entry associated with an offset commit */
  val offsetMetadataMaxSize = props.getInt("offset.metadata.max.bytes", OffsetManagerConfig.DefaultMaxMetadataSize)

  /** Batch size for reading from the offsets segments when loading offsets into the cache. */
  val offsetsLoadBufferSize = props.getIntInRange("offsets.load.buffer.size",
    OffsetManagerConfig.DefaultLoadBufferSize, (1, Integer.MAX_VALUE))

  /** The replication factor for the offsets topic (set higher to ensure availability). To
    * ensure that the effective replication factor of the offsets topic is the configured value,
    * the number of alive brokers has to be at least the replication factor at the time of the
    * first request for the offsets topic. If not, either the offsets topic creation will fail or
    * it will get a replication factor of min(alive brokers, configured replication factor) */
  val offsetsTopicReplicationFactor: Short = props.getShortInRange("offsets.topic.replication.factor",
    OffsetManagerConfig.DefaultOffsetsTopicReplicationFactor, (1, Short.MaxValue))

  /** The number of partitions for the offset commit topic (should not change after deployment). */
  val offsetsTopicPartitions: Int = props.getIntInRange("offsets.topic.num.partitions",
    OffsetManagerConfig.DefaultOffsetsTopicNumPartitions, (1, Integer.MAX_VALUE))

  /** The offsets topic segment bytes should be kept relatively small in order to facilitate faster log compaction and cache loads */
  val offsetsTopicSegmentBytes: Int = props.getIntInRange("offsets.topic.segment.bytes",
    OffsetManagerConfig.DefaultOffsetsTopicSegmentBytes, (1, Integer.MAX_VALUE))

  /** Compression codec for the offsets topic - compression may be used to achieve "atomic" commits. */
  val offsetsTopicCompressionCodec = props.getCompressionCodec("offsets.topic.compression.codec",
    OffsetManagerConfig.DefaultOffsetsTopicCompressionCodec)

  /** Offsets older than this retention period will be discarded. */
  val offsetsRetentionMinutes: Int = props.getIntInRange("offsets.retention.minutes", 24*60, (1, Integer.MAX_VALUE))

  /** Frequency at which to check for stale offsets. */
  val offsetsRetentionCheckIntervalMs: Long = props.getLongInRange("offsets.retention.check.interval.ms",
    OffsetManagerConfig.DefaultOffsetsRetentionCheckIntervalMs, (1, Long.MaxValue))

  /* Offset commit will be delayed until all replicas for the offsets topic receive the commit or this timeout is
   * reached. This is similar to the producer request timeout. */
   val offsetCommitTimeoutMs = props.getIntInRange("offsets.commit.timeout.ms",
    OffsetManagerConfig.DefaultOffsetCommitTimeoutMs, (1, Integer.MAX_VALUE))

  /** The required acks before the commit can be accepted. In general, the default (-1) should not be overridden. */
  val offsetCommitRequiredAcks = props.getShortInRange("offsets.commit.required.acks",
    OffsetManagerConfig.DefaultOffsetCommitRequiredAcks, (-1, offsetsTopicReplicationFactor))

  /* Enables delete topic. Delete topic through the admin tool will have no effect if this config is turned off */
  val deleteTopicEnable = props.getBoolean("delete.topic.enable", false)

}
