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

package kafka.consumer

import java.util.Properties
import kafka.api.OffsetRequest
import kafka.utils._
import kafka.common.{InvalidConfigException, Config}

object ConsumerConfig extends Config {
  val RefreshMetadataBackoffMs = 200
  val SocketTimeout = 30 * 1000
  val SocketBufferSize = 64*1024
  val FetchSize = 1024 * 1024
  val MaxFetchSize = 10*FetchSize
  val NumConsumerFetchers = 1
  val DefaultFetcherBackoffMs = 1000
  val AutoCommit = true
  val AutoCommitInterval = 60 * 1000
  val MaxQueuedChunks = 2
  val MaxRebalanceRetries = 4
  val AutoOffsetReset = OffsetRequest.LargestTimeString
  val ConsumerTimeoutMs = -1
  val MinFetchBytes = 1
  val MaxFetchWaitMs = 100
  val MirrorTopicsWhitelist = ""
  val MirrorTopicsBlacklist = ""
  val MirrorConsumerNumThreads = 1
  val OffsetsChannelBackoffMs = 1000
  val OffsetsChannelSocketTimeoutMs = 10000
  val OffsetsCommitMaxRetries = 5
  val OffsetsStorage = "zookeeper"

  val MirrorTopicsWhitelistProp = "mirror.topics.whitelist"
  val MirrorTopicsBlacklistProp = "mirror.topics.blacklist"
  val ExcludeInternalTopics = true
  val DefaultPartitionAssignmentStrategy = "range" /* select between "range", and "roundrobin" */
  val MirrorConsumerNumThreadsProp = "mirror.consumer.numthreads"
  val DefaultClientId = ""

  def validate(config: ConsumerConfig) {
    validateClientId(config.clientId)
    validateGroupId(config.groupId)
    validateAutoOffsetReset(config.autoOffsetReset)
    validateOffsetsStorage(config.offsetsStorage)
    validatePartitionAssignmentStrategy(config.partitionAssignmentStrategy)
  }

  def validateClientId(clientId: String) {
    validateChars("client.id", clientId)
  }

  def validateGroupId(groupId: String) {
    validateChars("group.id", groupId)
  }

  def validateAutoOffsetReset(autoOffsetReset: String) {
    autoOffsetReset match {
      case OffsetRequest.SmallestTimeString =>
      case OffsetRequest.LargestTimeString =>
      case _ => throw new InvalidConfigException("Wrong value " + autoOffsetReset + " of auto.offset.reset in ConsumerConfig; " +
                                                 "Valid values are " + OffsetRequest.SmallestTimeString + " and " + OffsetRequest.LargestTimeString)
    }
  }

  def validateOffsetsStorage(storage: String) {
    storage match {
      case "zookeeper" =>
      case "kafka" =>
      case _ => throw new InvalidConfigException("Wrong value " + storage + " of offsets.storage in consumer config; " +
                                                 "Valid values are 'zookeeper' and 'kafka'")
    }
  }

  def validatePartitionAssignmentStrategy(strategy: String) {
    strategy match {
      case "range" =>
      case "roundrobin" =>
      case _ => throw new InvalidConfigException("Wrong value " + strategy + " of partition.assignment.strategy in consumer config; " +
        "Valid values are 'range' and 'roundrobin'")
    }
  }
}

class ConsumerConfig private (val props: VerifiableProperties) extends ZKConfig(props) {
  import ConsumerConfig._

  def this(originalProps: Properties) {
    this(new VerifiableProperties(originalProps))
    props.verify()
  }

  /** a string that uniquely identifies a set of consumers within the same consumer group */
  val groupId = props.getString("group.id")

  /** consumer id: generated automatically if not set.
   *  Set this explicitly for only testing purpose. */
  val consumerId: Option[String] = Option(props.getString("consumer.id", null))

  /** the socket timeout for network requests. Its value should be at least fetch.wait.max.ms. */
  val socketTimeoutMs = props.getInt("socket.timeout.ms", SocketTimeout)
  
  /** the socket receive buffer for network requests */
  val socketReceiveBufferBytes = props.getInt("socket.receive.buffer.bytes", SocketBufferSize)
  
  /** the number of bytes of messages to attempt to fetch */
  val fetchMessageMaxBytes = props.getInt("fetch.message.max.bytes", FetchSize)

  /** the number threads used to fetch data */
  val numConsumerFetchers = props.getInt("num.consumer.fetchers", NumConsumerFetchers)
  
  /** if true, periodically commit to zookeeper the offset of messages already fetched by the consumer */
  val autoCommitEnable = props.getBoolean("auto.commit.enable", AutoCommit)
  
  /** the frequency in ms that the consumer offsets are committed to zookeeper */
  val autoCommitIntervalMs = props.getInt("auto.commit.interval.ms", AutoCommitInterval)

  /** max number of message chunks buffered for consumption, each chunk can be up to fetch.message.max.bytes*/
  val queuedMaxMessages = props.getInt("queued.max.message.chunks", MaxQueuedChunks)

  /** max number of retries during rebalance */
  val rebalanceMaxRetries = props.getInt("rebalance.max.retries", MaxRebalanceRetries)
  
  /** the minimum amount of data the server should return for a fetch request. If insufficient data is available the request will block */
  val fetchMinBytes = props.getInt("fetch.min.bytes", MinFetchBytes)
  
  /** the maximum amount of time the server will block before answering the fetch request if there isn't sufficient data to immediately satisfy fetch.min.bytes */
  val fetchWaitMaxMs = props.getInt("fetch.wait.max.ms", MaxFetchWaitMs)
  require(fetchWaitMaxMs <= socketTimeoutMs, "socket.timeout.ms should always be at least fetch.wait.max.ms" +
    " to prevent unnecessary socket timeouts")
  
  /** backoff time between retries during rebalance */
  val rebalanceBackoffMs = props.getInt("rebalance.backoff.ms", zkSyncTimeMs)

  /** backoff time to refresh the leader of a partition after it loses the current leader */
  val refreshLeaderBackoffMs = props.getInt("refresh.leader.backoff.ms", RefreshMetadataBackoffMs)

  /** backoff time to reconnect the offsets channel or to retry offset fetches/commits */
  val offsetsChannelBackoffMs = props.getInt("offsets.channel.backoff.ms", OffsetsChannelBackoffMs)
  /** socket timeout to use when reading responses for Offset Fetch/Commit requests. This timeout will also be used for
   *  the ConsumerMetdata requests that are used to query for the offset coordinator. */
  val offsetsChannelSocketTimeoutMs = props.getInt("offsets.channel.socket.timeout.ms", OffsetsChannelSocketTimeoutMs)

  /** Retry the offset commit up to this many times on failure. This retry count only applies to offset commits during
    * shut-down. It does not apply to commits from the auto-commit thread. It also does not apply to attempts to query
    * for the offset coordinator before committing offsets. i.e., if a consumer metadata request fails for any reason,
    * it is retried and that retry does not count toward this limit. */
  val offsetsCommitMaxRetries = props.getInt("offsets.commit.max.retries", OffsetsCommitMaxRetries)

  /** Specify whether offsets should be committed to "zookeeper" (default) or "kafka" */
  val offsetsStorage = props.getString("offsets.storage", OffsetsStorage).toLowerCase

  /** If you are using "kafka" as offsets.storage, you can dual commit offsets to ZooKeeper (in addition to Kafka). This
    * is required during migration from zookeeper-based offset storage to kafka-based offset storage. With respect to any
    * given consumer group, it is safe to turn this off after all instances within that group have been migrated to
    * the new jar that commits offsets to the broker (instead of directly to ZooKeeper). */
  val dualCommitEnabled = props.getBoolean("dual.commit.enabled", if (offsetsStorage == "kafka") true else false)

  /* what to do if an offset is out of range.
     smallest : automatically reset the offset to the smallest offset
     largest : automatically reset the offset to the largest offset
     anything else: throw exception to the consumer */
  val autoOffsetReset = props.getString("auto.offset.reset", AutoOffsetReset)

  /** throw a timeout exception to the consumer if no message is available for consumption after the specified interval */
  val consumerTimeoutMs = props.getInt("consumer.timeout.ms", ConsumerTimeoutMs)

  /**
   * Client id is specified by the kafka consumer client, used to distinguish different clients
   */
  val clientId = props.getString("client.id", groupId)

  /** Whether messages from internal topics (such as offsets) should be exposed to the consumer. */
  val excludeInternalTopics = props.getBoolean("exclude.internal.topics", ExcludeInternalTopics)

  /** Select a strategy for assigning partitions to consumer streams. Possible values: range, roundrobin */
  val partitionAssignmentStrategy = props.getString("partition.assignment.strategy", DefaultPartitionAssignmentStrategy)
  
  validate(this)
}

