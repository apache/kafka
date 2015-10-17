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

package kafka.coordinator

import kafka.common.TopicAndPartition
import kafka.utils.nonthreadsafe

/**
 * Consumer metadata contains the following metadata:
 *
 * Heartbeat metadata:
 * 1. negotiated heartbeat session timeout
 * 2. timestamp of the latest heartbeat
 *
 * Subscription metadata:
 * 1. subscribed topics
 * 2. assigned partitions for the subscribed topics
 *
 * In addition, it also contains the following state information:
 *
 * 1. Awaiting rebalance callback: when the consumer group is in the prepare-rebalance state,
 *                                 its rebalance callback will be kept in the metadata if the
 *                                 consumer has sent the join group request
 */
@nonthreadsafe
private[coordinator] class ConsumerMetadata(val consumerId: String,
                                            val groupId: String,
                                            var topics: Set[String],
                                            val sessionTimeoutMs: Int) {

  var awaitingRebalanceCallback: (Set[TopicAndPartition], String, Int, Short) => Unit = null
  var assignedTopicPartitions = Set.empty[TopicAndPartition]
  var latestHeartbeat: Long = -1
  var isLeaving: Boolean = false
}
