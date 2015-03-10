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

import scala.collection.mutable
import java.util.concurrent.atomic.AtomicInteger

sealed trait GroupStates { def state: Byte }

/**
 * Consumer group is preparing start rebalance
 *
 * action: respond consumer heartbeat with error code,
 * transition: all known consumers has re-joined group => UnderRebalance
 */
case object PrepareRebalance extends GroupStates { val state: Byte = 1 }

/**
 * Consumer group is under rebalance
 *
 * action: send the join-group response with new assignment
 * transition: all consumers has heartbeat with the new generation id => Fetching
 *             new consumer join-group received => PrepareRebalance
 */
case object UnderRebalance extends GroupStates { val state: Byte = 2 }

/**
 * Consumer group is fetching data
 *
 * action: respond consumer heartbeat normally
 * transition: consumer failure detected via heartbeat => PrepareRebalance
 *             consumer join-group received => PrepareRebalance
 *             zookeeper watcher fired => PrepareRebalance
 */
case object Fetching extends GroupStates { val state: Byte = 3 }

case class GroupState() {
  @volatile var currentState: Byte = PrepareRebalance.state
}

/* Group registry contains the following metadata of a registered group in the coordinator:
 *
 *  Membership metadata:
 *  1. List of consumers registered in this group
 *  2. Partition assignment strategy for this group
 *
 *  State metadata:
 *  1. Current group state
 *  2. Current group generation id
 */
class GroupRegistry(val groupId: String,
                    val partitionAssignmentStrategy: String) {

  val memberRegistries = new mutable.HashMap[String, ConsumerRegistry]()

  val state: GroupState = new GroupState()

  val generationId = new AtomicInteger(1)

  val nextConsumerId = new AtomicInteger(1)

  def generateNextConsumerId = groupId + "-" + nextConsumerId.getAndIncrement
}

