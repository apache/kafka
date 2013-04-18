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
import kafka.message.Message
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

  /*********** General Configuration ***********/
  
  /* the broker id for this server */
  val brokerId: Int = props.getIntInRange("broker.id", (0, Int.MaxValue))

  /* the maximum size of message that the server can receive */
  val messageMaxBytes = props.getIntInRange("message.max.bytes", 1000000, (0, Int.MaxValue))
  
  /* the number of network threads that the server uses for handling network requests */
  val numNetworkThreads = props.getIntInRange("num.network.threads", 3, (1, Int.MaxValue))

  /* the number of io threads that the server uses for carrying out network requests */
  val numIoThreads = props.getIntInRange("num.io.threads", 8, (1, Int.MaxValue))
  
  /* the number of threads to use for various background processing tasks */
  val backgroundThreads = props.getIntInRange("background.threads", 4, (1, Int.MaxValue))
  
  /* the number of queued requests allowed before blocking the network threads */
  val queuedMaxRequests = props.getIntInRange("queued.max.requests", 500, (1, Int.MaxValue))
  
  /*********** Socket Server Configuration ***********/
  
  /* the port to listen and accept connections on */
  val port: Int = props.getInt("port", 6667)

  /* hostname of broker. If this is set, it will only bind to this address. If this is not set,
   * it will bind to all interfaces, and publish one to ZK */
  val hostName: String = props.getString("host.name", null)

  /* the SO_SNDBUFF buffer of the socket sever sockets */
  val socketSendBufferBytes: Int = props.getInt("socket.send.buffer.bytes", 100*1024)
  
  /* the SO_RCVBUFF buffer of the socket sever sockets */
  val socketReceiveBufferBytes: Int = props.getInt("socket.receive.buffer.bytes", 100*1024)
  
  /* the maximum number of bytes in a socket request */
  val socketRequestMaxBytes: Int = props.getIntInRange("socket.request.max.bytes", 100*1024*1024, (1, Int.MaxValue))
  
  /*********** Log Configuration ***********/

  /* the default number of log partitions per topic */
  val numPartitions = props.getIntInRange("num.partitions", 1, (1, Int.MaxValue))
  
  /* the directories in which the log data is kept */
  val logDirs = Utils.parseCsvList(props.getString("log.dirs", props.getString("log.dir", "/tmp/kafka-logs")))
  require(logDirs.size > 0)
  
  /* the maximum size of a single log file */
  val logSegmentBytes = props.getIntInRange("log.segment.bytes", 1*1024*1024*1024, (Message.MinHeaderSize, Int.MaxValue))

  /* the maximum size of a single log file for some specific topic */
  val logSegmentBytesPerTopicMap = props.getMap("log.segment.bytes.per.topic", _.toInt > 0).mapValues(_.toInt)

  /* the maximum time before a new log segment is rolled out */
  val logRollHours = props.getIntInRange("log.roll.hours", 24*7, (1, Int.MaxValue))

  /* the number of hours before rolling out a new log segment for some specific topic */
  val logRollHoursPerTopicMap = props.getMap("log.roll.hours.per.topic", _.toInt > 0).mapValues(_.toInt)

  /* the number of hours to keep a log file before deleting it */
  val logRetentionHours = props.getIntInRange("log.retention.hours", 24*7, (1, Int.MaxValue))

  /* the number of hours to keep a log file before deleting it for some specific topic*/
  val logRetentionHoursPerTopicMap = props.getMap("log.retention.hours.per.topic", _.toInt > 0).mapValues(_.toInt)

  /* the maximum size of the log before deleting it */
  val logRetentionBytes = props.getLong("log.retention.bytes", -1)

  /* the maximum size of the log for some specific topic before deleting it */
  val logRetentionBytesPerTopicMap = props.getMap("log.retention.bytes.per.topic", _.toLong > 0).mapValues(_.toLong)
  
  /* the frequency in minutes that the log cleaner checks whether any log is eligible for deletion */
  val logCleanupIntervalMs = props.getLongInRange("log.retention.check.interval.ms", 5*60*1000, (1, Long.MaxValue))
  
  /* the default cleanup policy for segments beyond the retention window, must be either "delete" or "dedupe" */
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
  val logCleanerBackoffMs = props.getLongInRange("log.cleaner.backoff.ms", 30*1000, (0L, Long.MaxValue))
  
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
  val logFlushIntervalMessages = props.getIntInRange("log.flush.interval.messages", 10000, (1, Int.MaxValue))

  /* the amount of time to wait before deleting a file from the filesystem */
  val logDeleteDelayMs = props.getLongInRange("log.segment.delete.delay.ms", 60000, (0, Long.MaxValue))

  /* the maximum time in ms that a message in selected topics is kept in memory before flushed to disk, e.g., topic1:3000,topic2: 6000  */
  val logFlushIntervalMsPerTopicMap = props.getMap("log.flush.interval.ms.per.topic", _.toInt > 0).mapValues(_.toInt)

  /* the frequency in ms that the log flusher checks whether any log needs to be flushed to disk */
  val logFlushSchedulerIntervalMs = props.getInt("log.flush.scheduler.interval.ms",  3000)

  /* the maximum time in ms that a message in any topic is kept in memory before flushed to disk */
  val logFlushIntervalMs = props.getInt("log.flush.interval.ms", logFlushSchedulerIntervalMs)

  /* enable auto creation of topic on the server */
  val autoCreateTopicsEnable = props.getBoolean("auto.create.topics.enable", true)

  /*********** Replication configuration ***********/

  /* the socket timeout for controller-to-broker channels */
  val controllerSocketTimeoutMs = props.getInt("controller.socket.timeout.ms", 30000)

  /* the buffer size for controller-to-broker-channels */
  val controllerMessageQueueSize= props.getInt("controller.message.queue.size", 10)

  /* default replication factors for automatically created topics */
  val defaultReplicationFactor = props.getInt("default.replication.factor", 1)

  /* If a follower hasn't sent any fetch requests during this time, the leader will remove the follower from isr */
  val replicaLagTimeMaxMs = props.getLong("replica.lag.time.max.ms", 10000)

  /* If the lag in messages between a leader and a follower exceeds this number, the leader will remove the follower from isr */
  val replicaLagMaxMessages = props.getLong("replica.lag.max.messages", 4000)

  /* the socket timeout for network requests */
  val replicaSocketTimeoutMs = props.getInt("replica.socket.timeout.ms", ConsumerConfig.SocketTimeout)

  /* the socket receive buffer for network requests */
  val replicaSocketReceiveBufferBytes = props.getInt("replica.socket.receive.buffer.bytes", ConsumerConfig.SocketBufferSize)

  /* the number of byes of messages to attempt to fetch */
  val replicaFetchMaxBytes = props.getInt("replica.fetch.max.bytes", ConsumerConfig.FetchSize)

  /* max wait time for each fetcher request issued by follower replicas*/
  val replicaFetchWaitMaxMs = props.getInt("replica.fetch.wait.max.ms", 500)

  /* minimum bytes expected for each fetch response. If not enough bytes, wait up to replicaMaxWaitTimeMs */
  val replicaFetchMinBytes = props.getInt("replica.fetch.min.bytes", 1)

  /* number of fetcher threads used to replicate messages from a source broker.
   * Increasing this value can increase the degree of I/O parallelism in the follower broker. */
  val numReplicaFetchers = props.getInt("num.replica.fetchers", 1)
  
  /* the frequency with which the high watermark is saved out to disk */
  val replicaHighWatermarkCheckpointIntervalMs = props.getLong("replica.high.watermark.checkpoint.interval.ms", 5000L)

  /* the purge interval (in number of requests) of the fetch request purgatory */
  val fetchPurgatoryPurgeIntervalRequests = props.getInt("fetch.purgatory.purge.interval.requests", 10000)

  /* the purge interval (in number of requests) of the producer request purgatory */
  val producerPurgatoryPurgeIntervalRequests = props.getInt("producer.purgatory.purge.interval.requests", 10000)

  /*********** Misc configuration ***********/
  
  /* the maximum size for a metadata entry associated with an offset commit */
  val offsetMetadataMaxSize = props.getInt("offset.metadata.max.bytes", 1024)

 }
