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
  val SocketTimeout = 30 * 1000
  val SocketBufferSize = 64*1024
  val FetchSize = 1024 * 1024
  val MaxFetchSize = 10*FetchSize
  val DefaultFetcherBackoffMs = 1000
  val AutoCommit = true
  val AutoCommitInterval = 60 * 1000
  val MaxQueuedChunks = 10
  val MaxRebalanceRetries = 4
  val AutoOffsetReset = OffsetRequest.SmallestTimeString
  val ConsumerTimeoutMs = -1
  val MinFetchBytes = 1
  val MaxFetchWaitMs = 100
  val MirrorTopicsWhitelist = ""
  val MirrorTopicsBlacklist = ""
  val MirrorConsumerNumThreads = 1

  val MirrorTopicsWhitelistProp = "mirror.topics.whitelist"
  val MirrorTopicsBlacklistProp = "mirror.topics.blacklist"
  val MirrorConsumerNumThreadsProp = "mirror.consumer.numthreads"
  val DefaultClientId = ""

  def validate(config: ConsumerConfig) {
    validateClientId(config.clientId)
    validateGroupId(config.groupId)
    validateAutoOffsetReset(config.autoOffsetReset)
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

  /** the socket timeout for network requests. The actual timeout set will be max.fetch.wait + socket.timeout.ms. */
  val socketTimeoutMs = props.getInt("socket.timeout.ms", SocketTimeout)
  
  /** the socket receive buffer for network requests */
  val socketReceiveBufferBytes = props.getInt("socket.receive.buffer.bytes", SocketBufferSize)
  
  /** the number of byes of messages to attempt to fetch */
  val fetchMessageMaxBytes = props.getInt("fetch.message.max.bytes", FetchSize)
  
  /** if true, periodically commit to zookeeper the offset of messages already fetched by the consumer */
  val autoCommitEnable = props.getBoolean("auto.commit.enable", AutoCommit)
  
  /** the frequency in ms that the consumer offsets are committed to zookeeper */
  val autoCommitIntervalMs = props.getInt("auto.commit.interval.ms", AutoCommitInterval)

  /** max number of messages buffered for consumption */
  val queuedMaxMessages = props.getInt("queued.max.messages", MaxQueuedChunks)

  /** max number of retries during rebalance */
  val rebalanceMaxRetries = props.getInt("rebalance.max.retries", MaxRebalanceRetries)
  
  /** the minimum amount of data the server should return for a fetch request. If insufficient data is available the request will block */
  val fetchMinBytes = props.getInt("fetch.min.bytes", MinFetchBytes)
  
  /** the maximum amount of time the server will block before answering the fetch request if there isn't sufficient data to immediately satisfy fetch.min.bytes */
  val fetchWaitMaxMs = props.getInt("fetch.wait.max.ms", MaxFetchWaitMs)
  
  /** backoff time between retries during rebalance */
  val rebalanceBackoffMs = props.getInt("rebalance.backoff.ms", zkSyncTimeMs)

  /** backoff time to refresh the leader of a partition after it loses the current leader */
  val refreshLeaderBackoffMs = props.getInt("refresh.leader.backoff.ms", 200)

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

  validate(this)
}

