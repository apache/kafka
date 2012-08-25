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
import kafka.utils.{Utils, ZKConfig}
import kafka.message.Message
import kafka.consumer.ConsumerConfig
import java.net.InetAddress



/**
 * Configuration settings for the kafka server
 */
class KafkaConfig(props: Properties) extends ZKConfig(props) {
  /* the port to listen and accept connections on */
  val port: Int = Utils.getInt(props, "port", 6667)

  /* hostname of broker. If not set, will pick up from the value returned from getLocalHost. If there are multiple interfaces getLocalHost may not be what you want. */
  val hostName: String = Utils.getString(props, "hostname", InetAddress.getLocalHost.getHostAddress)

  /* the broker id for this server */
  val brokerId: Int = Utils.getIntInRange(props, "brokerid", (0, Int.MaxValue))

  /* the SO_SNDBUFF buffer of the socket sever sockets */
  val socketSendBuffer: Int = Utils.getInt(props, "socket.send.buffer", 100*1024)
  
  /* the SO_RCVBUFF buffer of the socket sever sockets */
  val socketReceiveBuffer: Int = Utils.getInt(props, "socket.receive.buffer", 100*1024)
  
  /* the maximum number of bytes in a socket request */
  val maxSocketRequestSize: Int = Utils.getIntInRange(props, "max.socket.request.bytes", 100*1024*1024, (1, Int.MaxValue))
  
  /* the number of network threads that the server uses for handling network requests */
  val numNetworkThreads = Utils.getIntInRange(props, "network.threads", 3, (1, Int.MaxValue))

  /* the number of io threads that the server uses for carrying out network requests */
  val numIoThreads = Utils.getIntInRange(props, "io.threads", 8, (1, Int.MaxValue))
  
  /* the number of queued requests allowed before blocking the network threads */
  val numQueuedRequests = Utils.getIntInRange(props, "max.queued.requests", 500, (1, Int.MaxValue))

  /* the interval in which to measure performance statistics */
  val monitoringPeriodSecs = Utils.getIntInRange(props, "monitoring.period.secs", 600, (1, Int.MaxValue))
  
  /* the default number of log partitions per topic */
  val numPartitions = Utils.getIntInRange(props, "num.partitions", 1, (1, Int.MaxValue))
  
  /* the directory in which the log data is kept */
  val logDir = Utils.getString(props, "log.dir")
  
  /* the maximum size of a single log file */
  val logFileSize = Utils.getIntInRange(props, "log.file.size", 1*1024*1024*1024, (Message.MinHeaderSize, Int.MaxValue))

  /* the maximum size of a single log file for some specific topic */
  val logFileSizeMap = Utils.getTopicFileSize(Utils.getString(props, "topic.log.file.size", ""))

  /* the maximum time before a new log segment is rolled out */
  val logRollHours = Utils.getIntInRange(props, "log.roll.hours", 24*7, (1, Int.MaxValue))

  /* the number of hours before rolling out a new log segment for some specific topic */
  val logRollHoursMap = Utils.getTopicRollHours(Utils.getString(props, "topic.log.roll.hours", ""))

  /* the number of hours to keep a log file before deleting it */
  val logRetentionHours = Utils.getIntInRange(props, "log.retention.hours", 24*7, (1, Int.MaxValue))

  /* the number of hours to keep a log file before deleting it for some specific topic*/
  val logRetentionHoursMap = Utils.getTopicRetentionHours(Utils.getString(props, "topic.log.retention.hours", ""))

  /* the maximum size of the log before deleting it */
  val logRetentionSize = Utils.getLong(props, "log.retention.size", -1)

  /* the maximum size of the log for some specific topic before deleting it */
  val logRetentionSizeMap = Utils.getTopicRetentionSize(Utils.getString(props, "topic.log.retention.size", ""))

  /* the frequency in minutes that the log cleaner checks whether any log is eligible for deletion */
  val logCleanupIntervalMinutes = Utils.getIntInRange(props, "log.cleanup.interval.mins", 10, (1, Int.MaxValue))

  /* the number of messages accumulated on a log partition before messages are flushed to disk */
  val flushInterval = Utils.getIntInRange(props, "log.flush.interval", 500, (1, Int.MaxValue))

  /* the maximum time in ms that a message in selected topics is kept in memory before flushed to disk, e.g., topic1:3000,topic2: 6000  */
  val flushIntervalMap = Utils.getTopicFlushIntervals(Utils.getString(props, "topic.flush.intervals.ms", ""))

  /* the frequency in ms that the log flusher checks whether any log needs to be flushed to disk */
  val flushSchedulerThreadRate = Utils.getInt(props, "log.default.flush.scheduler.interval.ms",  3000)

  /* the maximum time in ms that a message in any topic is kept in memory before flushed to disk */
  val defaultFlushIntervalMs = Utils.getInt(props, "log.default.flush.interval.ms", flushSchedulerThreadRate)

   /* the number of partitions for selected topics, e.g., topic1:8,topic2:16 */
  val topicPartitionsMap = Utils.getTopicPartitions(Utils.getString(props, "topic.partition.count.map", ""))

  /* enable auto creation of topic on the server */
  val autoCreateTopics = Utils.getBoolean(props, "auto.create.topics", true)

  /**
   * Following properties are relevant to Kafka replication
   */

  /* the socket timeout for controller-to-broker channels */
  val controllerSocketTimeoutMs = Utils.getInt(props, "controller.socket.timeout.ms", 30000)

  /* the buffer size for controller-to-broker-channels */
  val controllerMessageQueueSize= Utils.getInt(props, "controller.message.queue.size", 10)


  /* default replication factors for automatically created topics */
  val defaultReplicationFactor = Utils.getInt(props, "default.replication.factor", 1)

  /* wait time in ms to allow the preferred replica for a partition to become the leader. This property is used during
  * leader election on all replicas minus the preferred replica */
  val preferredReplicaWaitTime = Utils.getLong(props, "preferred.replica.wait.time", 300)

  val replicaMaxLagTimeMs = Utils.getLong(props, "replica.max.lag.time.ms", 10000)

  val replicaMaxLagBytes = Utils.getLong(props, "replica.max.lag.bytes", 4000)

  /* size of the state change request queue in Zookeeper */
  val stateChangeQSize = Utils.getInt(props, "state.change.queue.size", 1000)

  /**
   * Config options relevant to a follower for a replica
   */
  /** the socket timeout for network requests */
  val replicaSocketTimeoutMs = Utils.getInt(props, "replica.socket.timeout.ms", ConsumerConfig.SocketTimeout)

  /** the socket receive buffer for network requests */
  val replicaSocketBufferSize = Utils.getInt(props, "replica.socket.buffersize", ConsumerConfig.SocketBufferSize)

  /** the number of byes of messages to attempt to fetch */
  val replicaFetchSize = Utils.getInt(props, "replica.fetch.size", ConsumerConfig.FetchSize)

  /** max wait time for each fetcher request issued by follower replicas*/
  val replicaMaxWaitTimeMs = Utils.getInt(props, "replica.fetch.wait.time.ms", 500)

  /** minimum bytes expected for each fetch response. If not enough bytes, wait up to replicaMaxWaitTimeMs */
  val replicaMinBytes = Utils.getInt(props, "replica.fetch.min.bytes", 4096)

  /* number of fetcher threads used to replicate messages from a source broker.
  *  Increasing this value can increase the degree of I/O parallelism in the follower broker. */
  val numReplicaFetchers = Utils.getInt(props, "replica.fetchers", 1)
 }
