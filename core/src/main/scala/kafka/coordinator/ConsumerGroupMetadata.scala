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

import kafka.utils.nonthreadsafe

import java.util.UUID

import collection.mutable

private[coordinator] sealed trait GroupState { def state: Byte }

/**
 * Consumer group is preparing to rebalance
 *
 * action: respond to heartbeats with an ILLEGAL GENERATION error code
 * transition: some consumers have joined by the timeout => Rebalancing
 *             all consumers have left the group => Dead
 */
private[coordinator] case object PreparingRebalance extends GroupState { val state: Byte = 1 }

/**
 * Consumer group is rebalancing
 *
 * action: compute the group's partition assignment
 *         send the join-group response with new partition assignment when rebalance is complete
 * transition: partition assignment has been computed => Stable
 */
private[coordinator] case object Rebalancing extends GroupState { val state: Byte = 2 }

/**
 * Consumer group is stable
 *
 * action: respond to consumer heartbeats normally
 * transition: consumer failure detected via heartbeat => PreparingRebalance
 *             consumer join-group received => PreparingRebalance
 *             zookeeper topic watcher fired => PreparingRebalance
 */
private[coordinator] case object Stable extends GroupState { val state: Byte = 3 }

/**
 * Consumer group has no more members
 *
 * action: none
 * transition: none
 */
private[coordinator] case object Dead extends GroupState { val state: Byte = 4 }


/**
 * Group contains the following metadata:
 *
 *  Membership metadata:
 *  1. Consumers registered in this group
 *  2. Partition assignment strategy for this group
 *
 *  State metadata:
 *  1. group state
 *  2. generation id
 */
@nonthreadsafe
private[coordinator] class ConsumerGroupMetadata(val groupId: String,
                                                 val partitionAssignmentStrategy: String) {

  private val validPreviousStates: Map[GroupState, Set[GroupState]] =
    Map(Dead -> Set(PreparingRebalance),
      Stable -> Set(Rebalancing),
      PreparingRebalance -> Set(Stable),
      Rebalancing -> Set(PreparingRebalance))

  private val consumers = new mutable.HashMap[String, ConsumerMetadata]
  private var state: GroupState = Stable
  var generationId = 0

  def is(groupState: GroupState) = state == groupState
  def has(consumerId: String) = consumers.contains(consumerId)
  def get(consumerId: String) = consumers(consumerId)

  def add(consumerId: String, consumer: ConsumerMetadata) {
    consumers.put(consumerId, consumer)
  }

  def remove(consumerId: String) {
    consumers.remove(consumerId)
  }

  def isEmpty = consumers.isEmpty

  def topicsPerConsumer = consumers.mapValues(_.topics).toMap

  def topics = consumers.values.flatMap(_.topics).toSet

  def notYetRejoinedConsumers = consumers.values.filter(_.awaitingRebalanceCallback == null).toList

  def allConsumers = consumers.values.toList

  def rebalanceTimeout = consumers.values.foldLeft(0) {(timeout, consumer) =>
    timeout.max(consumer.sessionTimeoutMs)
  }

  // TODO: decide if ids should be predictable or random
  def generateNextConsumerId = UUID.randomUUID().toString

  def canRebalance = state == Stable

  def transitionTo(groupState: GroupState) {
    assertValidTransition(groupState)
    state = groupState
  }

  private def assertValidTransition(targetState: GroupState) {
    if (!validPreviousStates(targetState).contains(state))
      throw new IllegalStateException("Group %s should be in the %s states before moving to %s state. Instead it is in %s state"
        .format(groupId, validPreviousStates(targetState).mkString(","), targetState, state))
  }
}