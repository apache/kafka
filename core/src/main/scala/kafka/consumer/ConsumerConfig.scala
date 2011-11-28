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
import kafka.utils.{ZKConfig, Utils}
import kafka.api.OffsetRequest
import kafka.common.InvalidConfigException
object ConsumerConfig {
  val SocketTimeout = 30 * 1000
  val SocketBufferSize = 64*1024
  val FetchSize = 300 * 1024
  val MaxFetchSize = 10*FetchSize
  val BackoffIncrementMs = 1000
  val AutoCommit = true
  val AutoCommitInterval = 10 * 1000
  val MaxQueuedChunks = 100
  val MaxRebalanceRetries = 4
  val AutoOffsetReset = OffsetRequest.SmallestTimeString
  val ConsumerTimeoutMs = -1
  val MirrorTopicsWhitelist = ""
  val MirrorTopicsBlacklist = ""
  val MirrorConsumerNumThreads = 1

  val MirrorTopicsWhitelistProp = "mirror.topics.whitelist"
  val MirrorTopicsBlacklistProp = "mirror.topics.blacklist"
  val MirrorConsumerNumThreadsProp = "mirror.consumer.numthreads"
}

class ConsumerConfig(props: Properties) extends ZKConfig(props) {
  import ConsumerConfig._

  /** a string that uniquely identifies a set of consumers within the same consumer group */
  val groupId = Utils.getString(props, "groupid")

  /** consumer id: generated automatically if not set.
   *  Set this explicitly for only testing purpose. */
  val consumerId: Option[String] = /** TODO: can be written better in scala 2.8 */
    if (Utils.getString(props, "consumerid", null) != null) Some(Utils.getString(props, "consumerid")) else None

  /** the socket timeout for network requests */
  val socketTimeoutMs = Utils.getInt(props, "socket.timeout.ms", SocketTimeout)
  
  /** the socket receive buffer for network requests */
  val socketBufferSize = Utils.getInt(props, "socket.buffersize", SocketBufferSize)
  
  /** the number of byes of messages to attempt to fetch */
  val fetchSize = Utils.getInt(props, "fetch.size", FetchSize)
  
  /** the maximum allowable fetch size for a very large message */
  val maxFetchSize: Int = fetchSize * 10
  
  /** to avoid repeatedly polling a broker node which has no new data
      we will backoff every time we get an empty set from the broker*/
  val backoffIncrementMs: Long = Utils.getInt(props, "backoff.increment.ms", BackoffIncrementMs)
  
  /** if true, periodically commit to zookeeper the offset of messages already fetched by the consumer */
  val autoCommit = Utils.getBoolean(props, "autocommit.enable", AutoCommit)
  
  /** the frequency in ms that the consumer offsets are committed to zookeeper */
  val autoCommitIntervalMs = Utils.getInt(props, "autocommit.interval.ms", AutoCommitInterval)

  /** max number of messages buffered for consumption */
  val maxQueuedChunks = Utils.getInt(props, "queuedchunks.max", MaxQueuedChunks)

  /** max number of retries during rebalance */
  val maxRebalanceRetries = Utils.getInt(props, "rebalance.retries.max", MaxRebalanceRetries)

  /* what to do if an offset is out of range.
     smallest : automatically reset the offset to the smallest offset
     largest : automatically reset the offset to the largest offset
     anything else: throw exception to the consumer */
  val autoOffsetReset = Utils.getString(props, "autooffset.reset", AutoOffsetReset)

  /** throw a timeout exception to the consumer if no message is available for consumption after the specified interval */
  val consumerTimeoutMs = Utils.getInt(props, "consumer.timeout.ms", ConsumerTimeoutMs)

  /** Whitelist of topics for this mirror's embedded consumer to consume. At
   *  most one of whitelist/blacklist may be specified. */
  val mirrorTopicsWhitelist = Utils.getString(
    props, MirrorTopicsWhitelistProp, MirrorTopicsWhitelist)
 
  /** Topics to skip mirroring. At most one of whitelist/blacklist may be
   *  specified */
  val mirrorTopicsBlackList = Utils.getString(
    props, MirrorTopicsBlacklistProp, MirrorTopicsBlacklist)

  if (mirrorTopicsWhitelist.nonEmpty && mirrorTopicsBlackList.nonEmpty)
      throw new InvalidConfigException("The embedded consumer's mirror topics configuration can only contain one of blacklist or whitelist")

  val mirrorConsumerNumThreads = Utils.getInt(
    props, MirrorConsumerNumThreadsProp, MirrorConsumerNumThreads)
}

