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

import org.apache.kafka.common.protocol.Errors

import collection.mutable

private[coordinator] sealed trait GroupState { def state: Byte }

/**
 * Group is preparing to rebalance
 *
 * action: respond to heartbeats with an ILLEGAL GENERATION error code
 * transition: some members have joined by the timeout => Rebalancing
 *             all members have left the group => Dead
 */
private[coordinator] case object PreparingRebalance extends GroupState { val state: Byte = 1 }


/**
 * Group is awaiting state assignment from the leader
 *
 * action: respond to heartbeats with REBALANCE_IN_PROGRESS to have them rejoin
 * transition: state assignment received from leader => Stable
 *             join group with new member or new metadata => PreparingRebalance
 *             member failure detected => PreparingRebalance
 */
private[coordinator] case object AwaitingSync extends GroupState { val state: Byte = 5}

/**
 * Group is stable
 *
 * action: respond to member heartbeats normally
 * transition: member failure detected via heartbeat => PreparingRebalance
 *             member join-group received => PreparingRebalance
 */
private[coordinator] case object Stable extends GroupState { val state: Byte = 3 }

/**
 * Group has no more members
 *
 * action: none
 * transition: none
 */
private[coordinator] case object Dead extends GroupState { val state: Byte = 4 }


private object GroupMetadata {
  private val validPreviousStates: Map[GroupState, Set[GroupState]] =
    Map(Dead -> Set(PreparingRebalance),
      AwaitingSync -> Set(PreparingRebalance),
      Stable -> Set(AwaitingSync),
      PreparingRebalance -> Set(Stable, AwaitingSync))
}

/**
 * Group contains the following metadata:
 *
 *  Membership metadata:
 *  1. Members registered in this group
 *  2. Current protocol assigned to the group (e.g. partition assignment strategy for consumers)
 *  3. Protocol metadata associated with group members
 *
 *  State metadata:
 *  1. group state
 *  2. generation id
 */
@nonthreadsafe
private[coordinator] class GroupMetadata(val groupId: String, val protocol: String) {

  private val members = new mutable.HashMap[String, MemberMetadata]
  private var state: GroupState = Stable
  var generationId = 0
  var leaderId: String = null
  var subProtocol: String = null

  def is(groupState: GroupState) = state == groupState
  def not(groupState: GroupState) = state != groupState
  def has(memberId: String) = members.contains(memberId)
  def get(memberId: String) = members(memberId)

  def add(memberId: String, member: MemberMetadata) {
    if (leaderId == null)
      leaderId = memberId
    members.put(memberId, member)
  }

  def remove(memberId: String) {
    members.remove(memberId)
    if (memberId == leaderId) {
      leaderId = if (members.isEmpty) {
        null
      } else {
        members.keys.head
      }
    }
  }

  def currentState = state

  def isEmpty = members.isEmpty

  def notYetRejoinedMembers = members.values.filter(_.awaitingJoinCallback == null).toList

  def allMembers = members.values.toList

  def rebalanceTimeout = members.values.foldLeft(0) {(timeout, member) =>
    timeout.max(member.sessionTimeoutMs)
  }

  // TODO: decide if ids should be predictable or random
  def generateNextMemberId = UUID.randomUUID().toString

  def canRebalance = state == Stable || state == AwaitingSync

  def transitionTo(groupState: GroupState) {
    assertValidTransition(groupState)
    state = groupState
  }

  private def selectProtocol: String = {
    // select the protocol for this group which is supported by all members
    val candidates = candidateProtocols

    if (candidates.isEmpty)
      throw new IllegalStateException("Attempt to create group with inconsistent protocols")

    // let each member vote for one of the protocols and choose the one with the most votes
    val votes: List[(String, Int)] = allMembers
      .map(_.vote(candidates))
      .groupBy(identity)
      .mapValues(_.size)
      .toList

    votes.maxBy(_._2)._1
  }

  private def candidateProtocols = {
    // get the set of protocols that are commonly supported by all members
    allMembers
      .map(_.subProtocols.toSet)
      .reduceLeft((commonProtocols, protocols) => commonProtocols & protocols)
  }

  def supportsProtocols(memberProtocols: Set[String]) = {
    isEmpty || (memberProtocols & candidateProtocols).nonEmpty
  }

  def initNextGeneration = {
    assert(notYetRejoinedMembers == List.empty[MemberMetadata])
    generationId += 1
    subProtocol = selectProtocol
    transitionTo(AwaitingSync)
  }

  def currentMemberMetadata: Map[String, Array[Byte]] = {
    if (is(Dead) || is(PreparingRebalance))
      throw new IllegalStateException("Cannot obtain member metadata for group in state %s".format(state))
    members.map{ case (memberId, memberMetadata) => (memberId, memberMetadata.metadata)}.toMap
  }

  private def assertValidTransition(targetState: GroupState) {
    if (!GroupMetadata.validPreviousStates(targetState).contains(state))
      throw new IllegalStateException("Group %s should be in the %s states before moving to %s state. Instead it is in %s state"
        .format(groupId, GroupMetadata.validPreviousStates(targetState).mkString(","), targetState, state))
  }
}