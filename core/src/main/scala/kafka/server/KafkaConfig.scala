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
import java.net.InetAddress
import kafka.utils.{Utils, VerifiableProperties, ZKConfig}

/**
 * Configuration settings for the kafka server
 */
class KafkaConfig private (val props: VerifiableProperties) extends ZKConfig(props) {

  def this(originalProps: Properties) {
    this(new VerifiableProperties(originalProps))
  }

  def verify() = props.verify()
  
  /*********** General Configuration ***********/
  
  /* the broker id for this server */
  val brokerId: Int = props.getIntInRange("brokerid", (0, Int.MaxValue))

  /* the maximum size of message that the server can receive */
  val maxMessageSize = props.getIntInRange("max.message.size", 1000000, (0, Int.MaxValue))
  
  /* the number of network threads that the server uses for handling network requests */
  val numNetworkThreads = props.getIntInRange("network.threads", 3, (1, Int.MaxValue))

  /* the number of io threads that the server uses for carrying out network requests */
  val numIoThreads = props.getIntInRange("io.threads", 8, (1, Int.MaxValue))
  
  /* the number of queued requests allowed before blocking the network threads */
  val numQueuedRequests = props.getIntInRange("max.queued.requests", 500, (1, Int.MaxValue))
  
  /*********** Socket Server Configuration ***********/
  
  /* the port to listen and accept connections on */
  val port: Int = props.getInt("port", 6667)

  /* hostname of broker. If not set, will pick up from the value returned from getLocalHost. If there are multiple interfaces getLocalHost may not be what you want. */
  val hostName: String = props.getString("hostname", InetAddress.getLocalHost.getHostAddress)

  /* the SO_SNDBUFF buffer of the socket sever sockets */
  val socketSendBuffer: Int = props.getInt("socket.send.buffer", 100*1024)
  
  /* the SO_RCVBUFF buffer of the socket sever sockets */
  val socketReceiveBuffer: Int = props.getInt("socket.receive.buffer", 100*1024)
  
  /* the maximum number of bytes in a socket request */
  val maxSocketRequestSize: Int = props.getIntInRange("max.socket.request.bytes", 100*1024*1024, (1, Int.MaxValue))
  
  /*********** Log Configuration ***********/

  /* the default number of log partitions per topic */
  val numPartitions = props.getIntInRange("num.partitions", 1, (1, Int.MaxValue))
  
  /* the directory in which the log data is kept */
  val logDir = props.getString("log.dir")
  
  /* the maximum size of a single log file */
  val logFileSize = props.getIntInRange("log.file.size", 1*1024*1024*1024, (Message.MinHeaderSize, Int.MaxValue))

  /* the maximum size of a single log file for some specific topic */
  val logFileSizeMap = Utils.getTopicFileSize(props.getString("topic.log.file.size", ""))

  /* the maximum time before a new log segment is rolled out */
  val logRollHours = props.getIntInRange("log.roll.hours", 24*7, (1, Int.MaxValue))

  /* the number of hours before rolling out a new log segment for some specific topic */
  val logRollHoursMap = Utils.getTopicRollHours(props.getString("topic.log.roll.hours", ""))

  /* the number of hours to keep a log file before deleting it */
  val logRetentionHours = props.getIntInRange("log.retention.hours", 24*7, (1, Int.MaxValue))

  /* the number of hours to keep a log file before deleting it for some specific topic*/
  val logRetentionHoursMap = Utils.getTopicRetentionHours(props.getString("topic.log.retention.hours", ""))

  /* the maximum size of the log before deleting it */
  val logRetentionSize = props.getLong("log.retention.size", -1)

  /* the maximum size of the log for some specific topic before deleting it */
  val logRetentionSizeMap = Utils.getTopicRetentionSize(props.getString("topic.log.retention.size", ""))

  /* the frequency in minutes that the log cleaner checks whether any log is eligible for deletion */
  val logCleanupIntervalMinutes = props.getIntInRange("log.cleanup.interval.mins", 10, (1, Int.MaxValue))
  
  /* the maximum size in bytes of the offset index */
  val logIndexMaxSizeBytes = props.getIntInRange("log.index.max.size", 10*1024*1024, (4, Int.MaxValue))
  
  /* the interval with which we add an entry to the offset index */
  val logIndexIntervalBytes = props.getIntInRange("log.index.interval.bytes", 4096, (0, Int.MaxValue))

  /* the number of messages accumulated on a log partition before messages are flushed to disk */
  val flushInterval = props.getIntInRange("log.flush.interval", 500, (1, Int.MaxValue))

  /* the maximum time in ms that a message in selected topics is kept in memory before flushed to disk, e.g., topic1:3000,topic2: 6000  */
  val flushIntervalMap = Utils.getTopicFlushIntervals(props.getString("topic.flush.intervals.ms", ""))

  /* the frequency in ms that the log flusher checks whether any log needs to be flushed to disk */
  val flushSchedulerThreadRate = props.getInt("log.default.flush.scheduler.interval.ms",  3000)

  /* the maximum time in ms that a message in any topic is kept in memory before flushed to disk */
  val defaultFlushIntervalMs = props.getInt("log.default.flush.interval.ms", flushSchedulerThreadRate)

  /* enable auto creation of topic on the server */
  val autoCreateTopics = props.getBoolean("auto.create.topics", true)

  /*********** Replication configuration ***********/

  /* the socket timeout for controller-to-broker channels */
  val controllerSocketTimeoutMs = props.getInt("controller.socket.timeout.ms", 30000)

  /* the buffer size for controller-to-broker-channels */
  val controllerMessageQueueSize= props.getInt("controller.message.queue.size", 10)

  /* default replication factors for automatically created topics */
  val defaultReplicationFactor = props.getInt("default.replication.factor", 1)

  /* wait time in ms to allow the preferred replica for a partition to become the leader. This property is used during
  * leader election on all replicas minus the preferred replica */
  val preferredReplicaWaitTime = props.getLong("preferred.replica.wait.time", 300)

  val replicaMaxLagTimeMs = props.getLong("replica.max.lag.time.ms", 10000)

  val replicaMaxLagBytes = props.getLong("replica.max.lag.bytes", 4000)

  /* the socket timeout for network requests */
  val replicaSocketTimeoutMs = props.getInt("replica.socket.timeout.ms", ConsumerConfig.SocketTimeout)

  /* the socket receive buffer for network requests */
  val replicaSocketBufferSize = props.getInt("replica.socket.buffersize", ConsumerConfig.SocketBufferSize)

  /* the number of byes of messages to attempt to fetch */
  val replicaFetchSize = props.getInt("replica.fetch.size", ConsumerConfig.FetchSize)

  /* max wait time for each fetcher request issued by follower replicas*/
  val replicaMaxWaitTimeMs = props.getInt("replica.fetch.wait.time.ms", 500)

  /* minimum bytes expected for each fetch response. If not enough bytes, wait up to replicaMaxWaitTimeMs */
  val replicaMinBytes = props.getInt("replica.fetch.min.bytes", 4096)

  /* number of fetcher threads used to replicate messages from a source broker.
   * Increasing this value can increase the degree of I/O parallelism in the follower broker. */
  val numReplicaFetchers = props.getInt("replica.fetchers", 1)
 }
