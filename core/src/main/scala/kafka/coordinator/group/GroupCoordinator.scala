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
package kafka.coordinator.group

import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import kafka.common.OffsetAndMetadata
import kafka.log.LogConfig
import kafka.message.ProducerCompressionCodec
import kafka.server._
import kafka.utils.Logging
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.RecordBatch.{NO_PRODUCER_EPOCH, NO_PRODUCER_ID}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.Time

import scala.collection.{Map, Seq, immutable}
import scala.math.max

/**
 * GroupCoordinator handles general group membership and offset management.
 *
 * Each Kafka server instantiates a coordinator which is responsible for a set of
 * groups. Groups are assigned to coordinators based on their group names.
 * <p>
 * <b>Delayed operation locking notes:</b>
 * Delayed operations in GroupCoordinator use `group` as the delayed operation
 * lock. ReplicaManager.appendRecords may be invoked while holding the group lock
 * used by its callback.  The delayed callback may acquire the group lock
 * since the delayed operation is completed only if the group lock can be acquired.
 */
class GroupCoordinator(val brokerId: Int,
                       val groupConfig: GroupConfig,
                       val offsetConfig: OffsetConfig,
                       val groupManager: GroupMetadataManager,
                       val heartbeatPurgatory: DelayedOperationPurgatory[DelayedHeartbeat],
                       val joinPurgatory: DelayedOperationPurgatory[DelayedJoin],
                       time: Time) extends Logging {
  import GroupCoordinator._

  type JoinCallback = JoinGroupResult => Unit
  type SyncCallback = (Array[Byte], Errors) => Unit

  this.logIdent = "[GroupCoordinator " + brokerId + "]: "

  private val isActive = new AtomicBoolean(false)

  def offsetsTopicConfigs: Properties = {
    val props = new Properties
    props.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)
    props.put(LogConfig.SegmentBytesProp, offsetConfig.offsetsTopicSegmentBytes.toString)
    props.put(LogConfig.CompressionTypeProp, ProducerCompressionCodec.name)
    props
  }

  /**
   * NOTE: If a group lock and metadataLock are simultaneously needed,
   * be sure to acquire the group lock before metadataLock to prevent deadlock
   */

  /**
   * Startup logic executed at the same time when the server starts up.
   */
  def startup(enableMetadataExpiration: Boolean = true) {
    info("Starting up.")
    groupManager.startup(enableMetadataExpiration)
    isActive.set(true)
    info("Startup complete.")
  }

  /**
   * Shutdown logic executed at the same time when server shuts down.
   * Ordering of actions should be reversed from the startup process.
   */
  def shutdown() {
    info("Shutting down.")
    isActive.set(false)
    groupManager.shutdown()
    heartbeatPurgatory.shutdown()
    joinPurgatory.shutdown()
    info("Shutdown complete.")
  }

  def handleJoinGroup(groupId: String,
                      memberId: String,
                      requireKnownMemberId: Boolean,
                      clientId: String,
                      clientHost: String,
                      rebalanceTimeoutMs: Int,
                      sessionTimeoutMs: Int,
                      protocolType: String,
                      protocols: List[(String, Array[Byte])],
                      responseCallback: JoinCallback): Unit = {
    validateGroupStatus(groupId, ApiKeys.JOIN_GROUP).foreach { error =>
      responseCallback(joinError(memberId, error))
      return
    }

    if (sessionTimeoutMs < groupConfig.groupMinSessionTimeoutMs ||
      sessionTimeoutMs > groupConfig.groupMaxSessionTimeoutMs) {
      responseCallback(joinError(memberId, Errors.INVALID_SESSION_TIMEOUT))
    } else {
      val isUnknownMember = memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID
      groupManager.getGroup(groupId) match {
        case None =>
          // only try to create the group if the group is UNKNOWN AND
          // the member id is UNKNOWN, if member is specified but group does not
          // exist we should reject the request.
          if (isUnknownMember) {
            val group = groupManager.addGroup(new GroupMetadata(groupId, Empty, time))
            doUnknownJoinGroup(group, requireKnownMemberId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, protocols, responseCallback)
          } else {
            responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID))
          }

        case Some(group) =>
          group.inLock {
            if ((groupIsOverCapacity(group)
                  && group.has(memberId) && !group.get(memberId).isAwaitingJoin) // oversized group, need to shed members that haven't joined yet
                || (isUnknownMember && group.size >= groupConfig.groupMaxSize)) {
              group.remove(memberId)
              responseCallback(joinError(JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.GROUP_MAX_SIZE_REACHED))
            } else if (isUnknownMember) {
              doUnknownJoinGroup(group, requireKnownMemberId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, protocols, responseCallback)
            } else {
              doJoinGroup(group, memberId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, protocols, responseCallback)
            }

            // attempt to complete JoinGroup
            if (group.is(PreparingRebalance)) {
              joinPurgatory.checkAndComplete(GroupKey(group.groupId))
            }
          }
      }
    }
  }

  private def doUnknownJoinGroup(group: GroupMetadata,
                                 requireKnownMemberId: Boolean,
                                 clientId: String,
                                 clientHost: String,
                                 rebalanceTimeoutMs: Int,
                                 sessionTimeoutMs: Int,
                                 protocolType: String,
                                 protocols: List[(String, Array[Byte])],
                                 responseCallback: JoinCallback): Unit = {
    group.inLock {
      if (group.is(Dead)) {
        // if the group is marked as dead, it means some other thread has just removed the group
        // from the coordinator metadata; it is likely that the group has migrated to some other
        // coordinator OR the group is in a transient unstable phase. Let the member retry
        // joining without the specified member id.
        responseCallback(joinError(JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.UNKNOWN_MEMBER_ID))
      } else if (!group.supportsProtocols(protocolType, MemberMetadata.plainProtocolSet(protocols))) {
        responseCallback(joinError(JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.INCONSISTENT_GROUP_PROTOCOL))
      } else {
        val newMemberId = clientId + "-" + group.generateMemberIdSuffix

        if (requireKnownMemberId) {
          // If member id required, register the member in the pending member list
          // and send back a response to call for another join group request with allocated member id.
          group.addPendingMember(newMemberId)
          addPendingMemberExpiration(group, newMemberId, sessionTimeoutMs)
          responseCallback(joinError(newMemberId, Errors.MEMBER_ID_REQUIRED))
        } else {
          addMemberAndRebalance(rebalanceTimeoutMs, sessionTimeoutMs, newMemberId, clientId, clientHost, protocolType,
            protocols, group, responseCallback)
        }
      }
    }
  }
  
  private def doJoinGroup(group: GroupMetadata,
                          memberId: String,
                          clientId: String,
                          clientHost: String,
                          rebalanceTimeoutMs: Int,
                          sessionTimeoutMs: Int,
                          protocolType: String,
                          protocols: List[(String, Array[Byte])],
                          responseCallback: JoinCallback) {
    group.inLock {
      if (group.is(Dead)) {
        // if the group is marked as dead, it means some other thread has just removed the group
        // from the coordinator metadata; this is likely that the group has migrated to some other
        // coordinator OR the group is in a transient unstable phase. Let the member retry
        // joining without the specified member id.
        responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID))
      } else if (!group.supportsProtocols(protocolType, MemberMetadata.plainProtocolSet(protocols))) {
        responseCallback(joinError(memberId, Errors.INCONSISTENT_GROUP_PROTOCOL))
      } else if (group.isPendingMember(memberId)) {
        // A rejoining pending member will be accepted.
        addMemberAndRebalance(rebalanceTimeoutMs, sessionTimeoutMs, memberId, clientId, clientHost, protocolType,
          protocols, group, responseCallback)
      } else if (!group.has(memberId)) {
        // if the member trying to register with a un-recognized id, send the response to let
        // it reset its member id and retry.
        responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID))
      } else {
        group.currentState match {
          case PreparingRebalance =>
            val member = group.get(memberId)
            updateMemberAndRebalance(group, member, protocols, responseCallback)

          case CompletingRebalance =>
            val member = group.get(memberId)
            if (member.matches(protocols)) {
              // member is joining with the same metadata (which could be because it failed to
              // receive the initial JoinGroup response), so just return current group information
              // for the current generation.
              responseCallback(JoinGroupResult(
                members = if (group.isLeader(memberId)) {
                  group.currentMemberMetadata
                } else {
                  Map.empty
                },
                memberId = memberId,
                generationId = group.generationId,
                subProtocol = group.protocolOrNull,
                leaderId = group.leaderOrNull,
                error = Errors.NONE))
            } else {
              // member has changed metadata, so force a rebalance
              updateMemberAndRebalance(group, member, protocols, responseCallback)
            }

          case Stable =>
            val member = group.get(memberId)
            if (group.isLeader(memberId) || !member.matches(protocols)) {
              // force a rebalance if a member has changed metadata or if the leader sends JoinGroup.
              // The latter allows the leader to trigger rebalances for changes affecting assignment
              // which do not affect the member metadata (such as topic metadata changes for the consumer)
              updateMemberAndRebalance(group, member, protocols, responseCallback)
            } else {
              // for followers with no actual change to their metadata, just return group information
              // for the current generation which will allow them to issue SyncGroup
              responseCallback(JoinGroupResult(
                members = Map.empty,
                memberId = memberId,
                generationId = group.generationId,
                subProtocol = group.protocolOrNull,
                leaderId = group.leaderOrNull,
                error = Errors.NONE))
            }

          case Empty | Dead =>
            // Group reaches unexpected state. Let the joining member reset their generation and rejoin.
            warn(s"Attempt to add rejoining member $memberId of group ${group.groupId} in " +
              s"unexpected group state ${group.currentState}")
            responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID))
        }
      }
    }
  }

  def handleSyncGroup(groupId: String,
                      generation: Int,
                      memberId: String,
                      groupAssignment: Map[String, Array[Byte]],
                      responseCallback: SyncCallback): Unit = {
    validateGroupStatus(groupId, ApiKeys.SYNC_GROUP) match {
      case Some(error) if error == Errors.COORDINATOR_LOAD_IN_PROGRESS =>
        // The coordinator is loading, which means we've lost the state of the active rebalance and the
        // group will need to start over at JoinGroup. By returning rebalance in progress, the consumer
        // will attempt to rejoin without needing to rediscover the coordinator. Note that we cannot
        // return COORDINATOR_LOAD_IN_PROGRESS since older clients do not expect the error.
        responseCallback(Array.empty, Errors.REBALANCE_IN_PROGRESS)

      case Some(error) => responseCallback(Array.empty, error)

      case None =>
        groupManager.getGroup(groupId) match {
          case None => responseCallback(Array.empty, Errors.UNKNOWN_MEMBER_ID)
          case Some(group) => doSyncGroup(group, generation, memberId, groupAssignment, responseCallback)
        }
    }
  }

  private def doSyncGroup(group: GroupMetadata,
                          generationId: Int,
                          memberId: String,
                          groupAssignment: Map[String, Array[Byte]],
                          responseCallback: SyncCallback) {
    group.inLock {
      if (!group.has(memberId)) {
        responseCallback(Array.empty, Errors.UNKNOWN_MEMBER_ID)
      } else if (generationId != group.generationId) {
        responseCallback(Array.empty, Errors.ILLEGAL_GENERATION)
      } else {
        group.currentState match {
          case Empty | Dead =>
            responseCallback(Array.empty, Errors.UNKNOWN_MEMBER_ID)

          case PreparingRebalance =>
            responseCallback(Array.empty, Errors.REBALANCE_IN_PROGRESS)

          case CompletingRebalance =>
            group.get(memberId).awaitingSyncCallback = responseCallback

            // if this is the leader, then we can attempt to persist state and transition to stable
            if (group.isLeader(memberId)) {
              info(s"Assignment received from leader for group ${group.groupId} for generation ${group.generationId}")

              // fill any missing members with an empty assignment
              val missing = group.allMembers -- groupAssignment.keySet
              val assignment = groupAssignment ++ missing.map(_ -> Array.empty[Byte]).toMap

              groupManager.storeGroup(group, assignment, (error: Errors) => {
                group.inLock {
                  // another member may have joined the group while we were awaiting this callback,
                  // so we must ensure we are still in the CompletingRebalance state and the same generation
                  // when it gets invoked. if we have transitioned to another state, then do nothing
                  if (group.is(CompletingRebalance) && generationId == group.generationId) {
                    if (error != Errors.NONE) {
                      resetAndPropagateAssignmentError(group, error)
                      maybePrepareRebalance(group, s"error when storing group assignment during SyncGroup (member: $memberId)")
                    } else {
                      setAndPropagateAssignment(group, assignment)
                      group.transitionTo(Stable)
                    }
                  }
                }
              })
            }

          case Stable =>
            // if the group is stable, we just return the current assignment
            val memberMetadata = group.get(memberId)
            responseCallback(memberMetadata.assignment, Errors.NONE)
            completeAndScheduleNextHeartbeatExpiration(group, group.get(memberId))
        }
      }
    }
  }

  def handleLeaveGroup(groupId: String, memberId: String, responseCallback: Errors => Unit): Unit = {
    validateGroupStatus(groupId, ApiKeys.LEAVE_GROUP).foreach { error =>
      responseCallback(error)
      return
    }

    groupManager.getGroup(groupId) match {
      case None =>
        // if the group is marked as dead, it means some other thread has just removed the group
        // from the coordinator metadata; it is likely that the group has migrated to some other
        // coordinator OR the group is in a transient unstable phase. Let the consumer to retry
        // joining without specified consumer id,
        responseCallback(Errors.UNKNOWN_MEMBER_ID)

      case Some(group) =>
        group.inLock {
          if (group.is(Dead)) {
            responseCallback(Errors.UNKNOWN_MEMBER_ID)
          } else if (group.isPendingMember(memberId)) {
            // if a pending member is leaving, it needs to be removed from the pending list, heartbeat cancelled
            // and if necessary, prompt a JoinGroup completion.
            info(s"Pending member $memberId is leaving group ${group.groupId}.")
            removePendingMemberAndUpdateGroup(group, memberId)
            heartbeatPurgatory.checkAndComplete(MemberKey(group.groupId, memberId))
            responseCallback(Errors.NONE)
          } else if (!group.has(memberId)) {
            responseCallback(Errors.UNKNOWN_MEMBER_ID)
          } else {
            val member = group.get(memberId)
            removeHeartbeatForLeavingMember(group, member)
            info(s"Member ${member.memberId} in group ${group.groupId} has left, removing it from the group")
            removeMemberAndUpdateGroup(group, member, s"removing member $memberId on LeaveGroup")
            responseCallback(Errors.NONE)
          }
        }
    }
  }

  def handleDeleteGroups(groupIds: Set[String]): Map[String, Errors] = {
    var groupErrors: Map[String, Errors] = Map()
    var groupsEligibleForDeletion: Seq[GroupMetadata] = Seq()

    groupIds.foreach { groupId =>
      validateGroupStatus(groupId, ApiKeys.DELETE_GROUPS) match {
        case Some(error) =>
          groupErrors += groupId -> error

        case None =>
          groupManager.getGroup(groupId) match {
            case None =>
              groupErrors += groupId ->
                (if (groupManager.groupNotExists(groupId)) Errors.GROUP_ID_NOT_FOUND else Errors.NOT_COORDINATOR)
            case Some(group) =>
              group.inLock {
                group.currentState match {
                  case Dead =>
                    groupErrors += groupId ->
                      (if (groupManager.groupNotExists(groupId)) Errors.GROUP_ID_NOT_FOUND else Errors.NOT_COORDINATOR)
                  case Empty =>
                    group.transitionTo(Dead)
                    groupsEligibleForDeletion :+= group
                  case _ =>
                    groupErrors += groupId -> Errors.NON_EMPTY_GROUP
                }
              }
          }
      }
    }

    if (groupsEligibleForDeletion.nonEmpty) {
      val offsetsRemoved = groupManager.cleanupGroupMetadata(groupsEligibleForDeletion, _.removeAllOffsets())
      groupErrors ++= groupsEligibleForDeletion.map(_.groupId -> Errors.NONE).toMap
      info(s"The following groups were deleted: ${groupsEligibleForDeletion.map(_.groupId).mkString(", ")}. " +
        s"A total of $offsetsRemoved offsets were removed.")
    }

    groupErrors
  }

  def handleHeartbeat(groupId: String,
                      memberId: String,
                      generationId: Int,
                      responseCallback: Errors => Unit) {
    validateGroupStatus(groupId, ApiKeys.HEARTBEAT).foreach { error =>
      if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS)
        // the group is still loading, so respond just blindly
        responseCallback(Errors.NONE)
      else
        responseCallback(error)
      return
    }

    groupManager.getGroup(groupId) match {
      case None =>
        responseCallback(Errors.UNKNOWN_MEMBER_ID)

      case Some(group) => group.inLock {
        group.currentState match {
          case Dead =>
            // if the group is marked as dead, it means some other thread has just removed the group
            // from the coordinator metadata; it is likely that the group has migrated to some other
            // coordinator OR the group is in a transient unstable phase. Let the member retry
            // joining without the specified member id.
            responseCallback(Errors.UNKNOWN_MEMBER_ID)

          case Empty =>
            responseCallback(Errors.UNKNOWN_MEMBER_ID)

          case CompletingRebalance =>
            if (!group.has(memberId))
              responseCallback(Errors.UNKNOWN_MEMBER_ID)
            else
              responseCallback(Errors.REBALANCE_IN_PROGRESS)

          case PreparingRebalance =>
            if (!group.has(memberId)) {
              responseCallback(Errors.UNKNOWN_MEMBER_ID)
            } else if (generationId != group.generationId) {
              responseCallback(Errors.ILLEGAL_GENERATION)
            } else {
              val member = group.get(memberId)
              completeAndScheduleNextHeartbeatExpiration(group, member)
              responseCallback(Errors.REBALANCE_IN_PROGRESS)
            }

          case Stable =>
            if (!group.has(memberId)) {
              responseCallback(Errors.UNKNOWN_MEMBER_ID)
            } else if (generationId != group.generationId) {
              responseCallback(Errors.ILLEGAL_GENERATION)
            } else {
              val member = group.get(memberId)
              completeAndScheduleNextHeartbeatExpiration(group, member)
              responseCallback(Errors.NONE)
            }
        }
      }
    }
  }

  def handleTxnCommitOffsets(groupId: String,
                             producerId: Long,
                             producerEpoch: Short,
                             offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                             responseCallback: immutable.Map[TopicPartition, Errors] => Unit): Unit = {
    validateGroupStatus(groupId, ApiKeys.TXN_OFFSET_COMMIT) match {
      case Some(error) => responseCallback(offsetMetadata.mapValues(_ => error))
      case None =>
        val group = groupManager.getGroup(groupId).getOrElse {
          groupManager.addGroup(new GroupMetadata(groupId, Empty, time))
        }
        doCommitOffsets(group, NoMemberId, NoGeneration, producerId, producerEpoch, offsetMetadata, responseCallback)
    }
  }

  def handleCommitOffsets(groupId: String,
                          memberId: String,
                          generationId: Int,
                          offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                          responseCallback: immutable.Map[TopicPartition, Errors] => Unit) {
    validateGroupStatus(groupId, ApiKeys.OFFSET_COMMIT) match {
      case Some(error) => responseCallback(offsetMetadata.mapValues(_ => error))
      case None =>
        groupManager.getGroup(groupId) match {
          case None =>
            if (generationId < 0) {
              // the group is not relying on Kafka for group management, so allow the commit
              val group = groupManager.addGroup(new GroupMetadata(groupId, Empty, time))
              doCommitOffsets(group, memberId, generationId, NO_PRODUCER_ID, NO_PRODUCER_EPOCH,
                offsetMetadata, responseCallback)
            } else {
              // or this is a request coming from an older generation. either way, reject the commit
              responseCallback(offsetMetadata.mapValues(_ => Errors.ILLEGAL_GENERATION))
            }

          case Some(group) =>
            doCommitOffsets(group, memberId, generationId, NO_PRODUCER_ID, NO_PRODUCER_EPOCH,
              offsetMetadata, responseCallback)
        }
    }
  }

  def scheduleHandleTxnCompletion(producerId: Long,
                                  offsetsPartitions: Iterable[TopicPartition],
                                  transactionResult: TransactionResult) {
    require(offsetsPartitions.forall(_.topic == Topic.GROUP_METADATA_TOPIC_NAME))
    val isCommit = transactionResult == TransactionResult.COMMIT
    groupManager.scheduleHandleTxnCompletion(producerId, offsetsPartitions.map(_.partition).toSet, isCommit)
  }

  private def doCommitOffsets(group: GroupMetadata,
                              memberId: String,
                              generationId: Int,
                              producerId: Long,
                              producerEpoch: Short,
                              offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                              responseCallback: immutable.Map[TopicPartition, Errors] => Unit) {
    group.inLock {
      if (group.is(Dead)) {
        responseCallback(offsetMetadata.mapValues(_ => Errors.UNKNOWN_MEMBER_ID))
      } else if ((generationId < 0 && group.is(Empty)) || (producerId != NO_PRODUCER_ID)) {
        // The group is only using Kafka to store offsets.
        // Also, for transactional offset commits we don't need to validate group membership and the generation.
        groupManager.storeOffsets(group, memberId, offsetMetadata, responseCallback, producerId, producerEpoch)
      } else if (group.is(CompletingRebalance)) {
        responseCallback(offsetMetadata.mapValues(_ => Errors.REBALANCE_IN_PROGRESS))
      } else if (!group.has(memberId)) {
        responseCallback(offsetMetadata.mapValues(_ => Errors.UNKNOWN_MEMBER_ID))
      } else if (generationId != group.generationId) {
        responseCallback(offsetMetadata.mapValues(_ => Errors.ILLEGAL_GENERATION))
      } else {
        val member = group.get(memberId)
        completeAndScheduleNextHeartbeatExpiration(group, member)
        groupManager.storeOffsets(group, memberId, offsetMetadata, responseCallback)
      }
    }
  }

  def handleFetchOffsets(groupId: String, partitions: Option[Seq[TopicPartition]] = None):
  (Errors, Map[TopicPartition, OffsetFetchResponse.PartitionData]) = {

    validateGroupStatus(groupId, ApiKeys.OFFSET_FETCH) match {
      case Some(error) => error -> Map.empty
      case None =>
        // return offsets blindly regardless the current group state since the group may be using
        // Kafka commit storage without automatic group management
        (Errors.NONE, groupManager.getOffsets(groupId, partitions))
    }
  }

  def handleListGroups(): (Errors, List[GroupOverview]) = {
    if (!isActive.get) {
      (Errors.COORDINATOR_NOT_AVAILABLE, List[GroupOverview]())
    } else {
      val errorCode = if (groupManager.isLoading) Errors.COORDINATOR_LOAD_IN_PROGRESS else Errors.NONE
      (errorCode, groupManager.currentGroups.map(_.overview).toList)
    }
  }

  def handleDescribeGroup(groupId: String): (Errors, GroupSummary) = {
    validateGroupStatus(groupId, ApiKeys.DESCRIBE_GROUPS) match {
      case Some(error) => (error, GroupCoordinator.EmptyGroup)
      case None =>
        groupManager.getGroup(groupId) match {
          case None => (Errors.NONE, GroupCoordinator.DeadGroup)
          case Some(group) =>
            group.inLock {
              (Errors.NONE, group.summary)
            }
        }
    }
  }

  def handleDeletedPartitions(topicPartitions: Seq[TopicPartition]) {
    val offsetsRemoved = groupManager.cleanupGroupMetadata(groupManager.currentGroups, group => {
      group.removeOffsets(topicPartitions)
    })
    info(s"Removed $offsetsRemoved offsets associated with deleted partitions: ${topicPartitions.mkString(", ")}.")
  }

  private def isValidGroupId(groupId: String, api: ApiKeys): Boolean = {
    api match {
      case ApiKeys.OFFSET_COMMIT | ApiKeys.OFFSET_FETCH | ApiKeys.DESCRIBE_GROUPS | ApiKeys.DELETE_GROUPS =>
        // For backwards compatibility, we support the offset commit APIs for the empty groupId, and also
        // in DescribeGroups and DeleteGroups so that users can view and delete state of all groups.
        groupId != null
      case _ =>
        // The remaining APIs are groups using Kafka for group coordination and must have a non-empty groupId
        groupId != null && !groupId.isEmpty
    }
  }

  /**
   * Check that the groupId is valid, assigned to this coordinator and that the group has been loaded.
   */
  private def validateGroupStatus(groupId: String, api: ApiKeys): Option[Errors] = {
    if (!isValidGroupId(groupId, api))
      Some(Errors.INVALID_GROUP_ID)
    else if (!isActive.get)
      Some(Errors.COORDINATOR_NOT_AVAILABLE)
    else if (isCoordinatorLoadInProgress(groupId))
      Some(Errors.COORDINATOR_LOAD_IN_PROGRESS)
    else if (!isCoordinatorForGroup(groupId))
      Some(Errors.NOT_COORDINATOR)
    else
      None
  }

  private def onGroupUnloaded(group: GroupMetadata) {
    group.inLock {
      info(s"Unloading group metadata for ${group.groupId} with generation ${group.generationId}")
      val previousState = group.currentState
      group.transitionTo(Dead)

      previousState match {
        case Empty | Dead =>
        case PreparingRebalance =>
          for (member <- group.allMemberMetadata) {
            group.maybeInvokeJoinCallback(member, joinError(member.memberId, Errors.NOT_COORDINATOR))
          }

          joinPurgatory.checkAndComplete(GroupKey(group.groupId))

        case Stable | CompletingRebalance =>
          for (member <- group.allMemberMetadata) {
            if (member.awaitingSyncCallback != null) {
              member.awaitingSyncCallback(Array.empty[Byte], Errors.NOT_COORDINATOR)
              member.awaitingSyncCallback = null
            }
            heartbeatPurgatory.checkAndComplete(MemberKey(member.groupId, member.memberId))
          }
      }
    }
  }

  private def onGroupLoaded(group: GroupMetadata) {
    group.inLock {
      info(s"Loading group metadata for ${group.groupId} with generation ${group.generationId}")
      assert(group.is(Stable) || group.is(Empty))
      if (groupIsOverCapacity(group)) {
        prepareRebalance(group, s"Freshly-loaded group is over capacity ($groupConfig.groupMaxSize). Rebalacing in order to give a chance for consumers to commit offsets")
      }

      group.allMemberMetadata.foreach(completeAndScheduleNextHeartbeatExpiration(group, _))
    }
  }

  def handleGroupImmigration(offsetTopicPartitionId: Int) {
    groupManager.scheduleLoadGroupAndOffsets(offsetTopicPartitionId, onGroupLoaded)
  }

  def handleGroupEmigration(offsetTopicPartitionId: Int) {
    groupManager.removeGroupsForPartition(offsetTopicPartitionId, onGroupUnloaded)
  }

  private def setAndPropagateAssignment(group: GroupMetadata, assignment: Map[String, Array[Byte]]) {
    assert(group.is(CompletingRebalance))
    group.allMemberMetadata.foreach(member => member.assignment = assignment(member.memberId))
    propagateAssignment(group, Errors.NONE)
  }

  private def resetAndPropagateAssignmentError(group: GroupMetadata, error: Errors) {
    assert(group.is(CompletingRebalance))
    group.allMemberMetadata.foreach(_.assignment = Array.empty[Byte])
    propagateAssignment(group, error)
  }

  private def propagateAssignment(group: GroupMetadata, error: Errors) {
    for (member <- group.allMemberMetadata) {
      if (member.awaitingSyncCallback != null) {
        member.awaitingSyncCallback(member.assignment, error)
        member.awaitingSyncCallback = null

        // reset the session timeout for members after propagating the member's assignment.
        // This is because if any member's session expired while we were still awaiting either
        // the leader sync group or the storage callback, its expiration will be ignored and no
        // future heartbeat expectations will not be scheduled.
        completeAndScheduleNextHeartbeatExpiration(group, member)
      }
    }
  }

  private def joinError(memberId: String, error: Errors): JoinGroupResult = {
    JoinGroupResult(
      members = Map.empty,
      memberId = memberId,
      generationId = GroupCoordinator.NoGeneration,
      subProtocol = GroupCoordinator.NoProtocol,
      leaderId = GroupCoordinator.NoLeader,
      error = error)
  }

  /**
   * Complete existing DelayedHeartbeats for the given member and schedule the next one
   */
  private def completeAndScheduleNextHeartbeatExpiration(group: GroupMetadata, member: MemberMetadata) {
    completeAndScheduleNextExpiration(group, member, member.sessionTimeoutMs)
  }

  private def completeAndScheduleNextExpiration(group: GroupMetadata, member: MemberMetadata, timeoutMs: Long) {
    // complete current heartbeat expectation
    member.latestHeartbeat = time.milliseconds()
    val memberKey = MemberKey(member.groupId, member.memberId)
    heartbeatPurgatory.checkAndComplete(memberKey)

    // reschedule the next heartbeat expiration deadline
    val deadline = member.latestHeartbeat + timeoutMs
    val delayedHeartbeat = new DelayedHeartbeat(this, group, member.memberId, isPending = false, deadline, timeoutMs)
    heartbeatPurgatory.tryCompleteElseWatch(delayedHeartbeat, Seq(memberKey))
  }

  /**
    * Add pending member expiration to heartbeat purgatory
    */
  private def addPendingMemberExpiration(group: GroupMetadata, pendingMemberId: String, timeoutMs: Long) {
    val pendingMemberKey = MemberKey(group.groupId, pendingMemberId)
    val deadline = time.milliseconds() + timeoutMs
    val delayedHeartbeat = new DelayedHeartbeat(this, group, pendingMemberId, isPending = true, deadline, timeoutMs)
    heartbeatPurgatory.tryCompleteElseWatch(delayedHeartbeat, Seq(pendingMemberKey))
  }

  private def removeHeartbeatForLeavingMember(group: GroupMetadata, member: MemberMetadata) {
    member.isLeaving = true
    val memberKey = MemberKey(member.groupId, member.memberId)
    heartbeatPurgatory.checkAndComplete(memberKey)
  }

  private def addMemberAndRebalance(rebalanceTimeoutMs: Int,
                                    sessionTimeoutMs: Int,
                                    memberId: String,
                                    clientId: String,
                                    clientHost: String,
                                    protocolType: String,
                                    protocols: List[(String, Array[Byte])],
                                    group: GroupMetadata,
                                    callback: JoinCallback) {
    val member = new MemberMetadata(memberId, group.groupId, clientId, clientHost, rebalanceTimeoutMs,
      sessionTimeoutMs, protocolType, protocols)

    member.isNew = true

    // update the newMemberAdded flag to indicate that the join group can be further delayed
    if (group.is(PreparingRebalance) && group.generationId == 0)
      group.newMemberAdded = true

    group.add(member, callback)

    // The session timeout does not affect new members since they do not have their memberId and
    // cannot send heartbeats. Furthermore, we cannot detect disconnects because sockets are muted
    // while the JoinGroup is in purgatory. If the client does disconnect (e.g. because of a request
    // timeout during a long rebalance), they may simply retry which will lead to a lot of defunct
    // members in the rebalance. To prevent this going on indefinitely, we timeout JoinGroup requests
    // for new members. If the new member is still there, we expect it to retry.
    completeAndScheduleNextExpiration(group, member, NewMemberJoinTimeoutMs)

    group.removePendingMember(memberId)
    maybePrepareRebalance(group, s"Adding new member $memberId")
  }

  private def updateMemberAndRebalance(group: GroupMetadata,
                                       member: MemberMetadata,
                                       protocols: List[(String, Array[Byte])],
                                       callback: JoinCallback) {
    group.updateMember(member, protocols, callback)
    maybePrepareRebalance(group, s"Updating metadata for member ${member.memberId}")
  }

  private def maybePrepareRebalance(group: GroupMetadata, reason: String) {
    group.inLock {
      if (group.canRebalance)
        prepareRebalance(group, reason)
    }
  }

  private def prepareRebalance(group: GroupMetadata, reason: String) {
    // if any members are awaiting sync, cancel their request and have them rejoin
    if (group.is(CompletingRebalance))
      resetAndPropagateAssignmentError(group, Errors.REBALANCE_IN_PROGRESS)

    val delayedRebalance = if (group.is(Empty))
      new InitialDelayedJoin(this,
        joinPurgatory,
        group,
        groupConfig.groupInitialRebalanceDelayMs,
        groupConfig.groupInitialRebalanceDelayMs,
        max(group.rebalanceTimeoutMs - groupConfig.groupInitialRebalanceDelayMs, 0))
    else
      new DelayedJoin(this, group, group.rebalanceTimeoutMs)

    group.transitionTo(PreparingRebalance)

    info(s"Preparing to rebalance group ${group.groupId} in state ${group.currentState} with old generation " +
      s"${group.generationId} (${Topic.GROUP_METADATA_TOPIC_NAME}-${partitionFor(group.groupId)}) (reason: $reason)")

    val groupKey = GroupKey(group.groupId)
    joinPurgatory.tryCompleteElseWatch(delayedRebalance, Seq(groupKey))
  }

  private def removeMemberAndUpdateGroup(group: GroupMetadata, member: MemberMetadata, reason: String) {
    // New members may timeout with a pending JoinGroup while the group is still rebalancing, so we have
    // to invoke the callback before removing the member. We return UNKNOWN_MEMBER_ID so that the consumer
    // will retry the JoinGroup request if is still active.
    group.maybeInvokeJoinCallback(member, joinError(NoMemberId, Errors.UNKNOWN_MEMBER_ID))

    group.remove(member.memberId)

    group.currentState match {
      case Dead | Empty =>
      case Stable | CompletingRebalance => maybePrepareRebalance(group, reason)
      case PreparingRebalance => joinPurgatory.checkAndComplete(GroupKey(group.groupId))
    }
  }

  private def removePendingMemberAndUpdateGroup(group: GroupMetadata, memberId: String) {
    group.removePendingMember(memberId)

    if (group.is(PreparingRebalance)) {
      joinPurgatory.checkAndComplete(GroupKey(group.groupId))
    }
  }

  def tryCompleteJoin(group: GroupMetadata, forceComplete: () => Boolean) = {
    group.inLock {
      if (group.hasAllMembersJoined)
        forceComplete()
      else false
    }
  }

  def onExpireJoin() {
    // TODO: add metrics for restabilize timeouts
  }

  def onCompleteJoin(group: GroupMetadata) {
    group.inLock {
      // remove any members who haven't joined the group yet
      group.notYetRejoinedMembers.foreach { failedMember =>
        removeHeartbeatForLeavingMember(group, failedMember)
        group.remove(failedMember.memberId)
        // TODO: cut the socket connection to the client
      }

      if (!group.is(Dead)) {
        group.initNextGeneration()
        if (group.is(Empty)) {
          info(s"Group ${group.groupId} with generation ${group.generationId} is now empty " +
            s"(${Topic.GROUP_METADATA_TOPIC_NAME}-${partitionFor(group.groupId)})")

          groupManager.storeGroup(group, Map.empty, error => {
            if (error != Errors.NONE) {
              // we failed to write the empty group metadata. If the broker fails before another rebalance,
              // the previous generation written to the log will become active again (and most likely timeout).
              // This should be safe since there are no active members in an empty generation, so we just warn.
              warn(s"Failed to write empty metadata for group ${group.groupId}: ${error.message}")
            }
          })
        } else {
          info(s"Stabilized group ${group.groupId} generation ${group.generationId} " +
            s"(${Topic.GROUP_METADATA_TOPIC_NAME}-${partitionFor(group.groupId)})")

          // trigger the awaiting join group response callback for all the members after rebalancing
          for (member <- group.allMemberMetadata) {
            assert(member.awaitingJoinCallback != null)
            val joinResult = JoinGroupResult(
              members = if (group.isLeader(member.memberId)) {
                group.currentMemberMetadata
              } else {
                Map.empty
              },
              memberId = member.memberId,
              generationId = group.generationId,
              subProtocol = group.protocolOrNull,
              leaderId = group.leaderOrNull,
              error = Errors.NONE)

            group.maybeInvokeJoinCallback(member, joinResult)
            completeAndScheduleNextHeartbeatExpiration(group, member)
            member.isNew = false
          }
        }
      }
    }
  }

  def tryCompleteHeartbeat(group: GroupMetadata, memberId: String, isPending: Boolean, heartbeatDeadline: Long, forceComplete: () => Boolean) = {
    group.inLock {
      if (isPending) {
        // complete the heartbeat if the member has joined the group
        if (group.has(memberId)) {
          forceComplete()
        } else false
      }
      else {
        val member = group.get(memberId)
        if (member.shouldKeepAlive(heartbeatDeadline) || member.isLeaving) {
          forceComplete()
        } else false
      }
    }
  }

  def onExpireHeartbeat(group: GroupMetadata, memberId: String, isPending: Boolean, heartbeatDeadline: Long) {
    group.inLock {
      if (isPending) {
        info(s"Pending member $memberId in group ${group.groupId} has been removed after session timeout expiration.")
        removePendingMemberAndUpdateGroup(group, memberId)
      } else if (!group.has(memberId)) {
        debug(s"Member $memberId has already been removed from the group.")
      } else {
        val member = group.get(memberId)
        if (!member.shouldKeepAlive(heartbeatDeadline)) {
          info(s"Member ${member.memberId} in group ${group.groupId} has failed, removing it from the group")
          removeMemberAndUpdateGroup(group, member, s"removing member ${member.memberId} on heartbeat expiration")
        }
      }
    }
  }

  def onCompleteHeartbeat() {
    // TODO: add metrics for complete heartbeats
  }

  def partitionFor(group: String): Int = groupManager.partitionFor(group)

  private def groupIsOverCapacity(group: GroupMetadata): Boolean = {
    group.size > groupConfig.groupMaxSize
  }

  private def isCoordinatorForGroup(groupId: String) = groupManager.isGroupLocal(groupId)

  private def isCoordinatorLoadInProgress(groupId: String) = groupManager.isGroupLoading(groupId)
}

object GroupCoordinator {

  val NoState = ""
  val NoProtocolType = ""
  val NoProtocol = ""
  val NoLeader = ""
  val NoGeneration = -1
  val NoMemberId = ""
  val NoMembers = List[MemberSummary]()
  val EmptyGroup = GroupSummary(NoState, NoProtocolType, NoProtocol, NoMembers)
  val DeadGroup = GroupSummary(Dead.toString, NoProtocolType, NoProtocol, NoMembers)
  val NewMemberJoinTimeoutMs: Int = 5 * 60 * 1000

  def apply(config: KafkaConfig,
            zkClient: KafkaZkClient,
            replicaManager: ReplicaManager,
            time: Time): GroupCoordinator = {
    val heartbeatPurgatory = DelayedOperationPurgatory[DelayedHeartbeat]("Heartbeat", config.brokerId)
    val joinPurgatory = DelayedOperationPurgatory[DelayedJoin]("Rebalance", config.brokerId)
    apply(config, zkClient, replicaManager, heartbeatPurgatory, joinPurgatory, time)
  }

  private[group] def offsetConfig(config: KafkaConfig) = OffsetConfig(
    maxMetadataSize = config.offsetMetadataMaxSize,
    loadBufferSize = config.offsetsLoadBufferSize,
    offsetsRetentionMs = config.offsetsRetentionMinutes * 60L * 1000L,
    offsetsRetentionCheckIntervalMs = config.offsetsRetentionCheckIntervalMs,
    offsetsTopicNumPartitions = config.offsetsTopicPartitions,
    offsetsTopicSegmentBytes = config.offsetsTopicSegmentBytes,
    offsetsTopicReplicationFactor = config.offsetsTopicReplicationFactor,
    offsetsTopicCompressionCodec = config.offsetsTopicCompressionCodec,
    offsetCommitTimeoutMs = config.offsetCommitTimeoutMs,
    offsetCommitRequiredAcks = config.offsetCommitRequiredAcks
  )

  def apply(config: KafkaConfig,
            zkClient: KafkaZkClient,
            replicaManager: ReplicaManager,
            heartbeatPurgatory: DelayedOperationPurgatory[DelayedHeartbeat],
            joinPurgatory: DelayedOperationPurgatory[DelayedJoin],
            time: Time): GroupCoordinator = {
    val offsetConfig = this.offsetConfig(config)
    val groupConfig = GroupConfig(groupMinSessionTimeoutMs = config.groupMinSessionTimeoutMs,
      groupMaxSessionTimeoutMs = config.groupMaxSessionTimeoutMs,
      groupMaxSize = config.groupMaxSize,
      groupInitialRebalanceDelayMs = config.groupInitialRebalanceDelay)

    val groupMetadataManager = new GroupMetadataManager(config.brokerId, config.interBrokerProtocolVersion,
      offsetConfig, replicaManager, zkClient, time)
    new GroupCoordinator(config.brokerId, groupConfig, offsetConfig, groupMetadataManager, heartbeatPurgatory, joinPurgatory, time)
  }

}

case class GroupConfig(groupMinSessionTimeoutMs: Int,
                       groupMaxSessionTimeoutMs: Int,
                       groupMaxSize: Int,
                       groupInitialRebalanceDelayMs: Int)

case class JoinGroupResult(members: Map[String, Array[Byte]],
                           memberId: String,
                           generationId: Int,
                           subProtocol: String,
                           leaderId: String,
                           error: Errors)
