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
package kafka.controller

import collection._
import collection.JavaConversions._
import java.util.concurrent.atomic.AtomicBoolean
import kafka.common.{TopicAndPartition, StateChangeFailedException}
import kafka.utils.{ZkUtils, Logging}
import org.I0Itec.zkclient.IZkChildListener
import org.apache.log4j.Logger
import kafka.controller.Callbacks._
import kafka.utils.Utils._

/**
 * This class represents the state machine for replicas. It defines the states that a replica can be in, and
 * transitions to move the replica to another legal state. The different states that a replica can be in are -
 * 1. NewReplica        : The controller can create new replicas during partition reassignment. In this state, a
 *                        replica can only get become follower state change request.  Valid previous
 *                        state is NonExistentReplica
 * 2. OnlineReplica     : Once a replica is started and part of the assigned replicas for its partition, it is in this
 *                        state. In this state, it can get either become leader or become follower state change requests.
 *                        Valid previous state are NewReplica, OnlineReplica or OfflineReplica
 * 3. OfflineReplica    : If a replica dies, it moves to this state. This happens when the broker hosting the replica
 *                        is down. Valid previous state are NewReplica, OnlineReplica
 * 4. ReplicaDeletionStarted: If replica deletion starts, it is moved to this state. Valid previous state is OfflineReplica
 * 5. ReplicaDeletionSuccessful: If replica responds with no error code in response to a delete replica request, it is
 *                        moved to this state. Valid previous state is ReplicaDeletionStarted
 * 6. ReplicaDeletionFailed: If replica deletion fails, it is moved to this state. Valid previous state is ReplicaDeletionStarted
 * 7. NonExistentReplica: If a replica is deleted successfully, it is moved to this state. Valid previous state is
 *                        ReplicaDeletionSuccessful
 */
class ReplicaStateMachine(controller: KafkaController) extends Logging {
  private val controllerContext = controller.controllerContext
  private val controllerId = controller.config.brokerId
  private val zkClient = controllerContext.zkClient
  var replicaState: mutable.Map[PartitionAndReplica, ReplicaState] = mutable.Map.empty
  val brokerRequestBatch = new ControllerBrokerRequestBatch(controller)
  private val hasStarted = new AtomicBoolean(false)
  this.logIdent = "[Replica state machine on controller " + controller.config.brokerId + "]: "
  private val stateChangeLogger = Logger.getLogger(KafkaController.stateChangeLogger)

  /**
   * Invoked on successful controller election. First registers a broker change listener since that triggers all
   * state transitions for replicas. Initializes the state of replicas for all partitions by reading from zookeeper.
   * Then triggers the OnlineReplica state change for all replicas.
   */
  def startup() {
    // initialize replica state
    initializeReplicaState()
    hasStarted.set(true)
    // move all Online replicas to Online
    handleStateChanges(controllerContext.allLiveReplicas(), OnlineReplica)
    info("Started replica state machine with initial state -> " + replicaState.toString())
  }

  // register broker change listener
  def registerListeners() {
    registerBrokerChangeListener()
  }

  /**
   * Invoked on controller shutdown.
   */
  def shutdown() {
    hasStarted.set(false)
    replicaState.clear()
  }

  /**
   * This API is invoked by the broker change controller callbacks and the startup API of the state machine
   * @param replicas     The list of replicas (brokers) that need to be transitioned to the target state
   * @param targetState  The state that the replicas should be moved to
   * The controller's allLeaders cache should have been updated before this
   */
  def handleStateChanges(replicas: Set[PartitionAndReplica], targetState: ReplicaState,
                         callbacks: Callbacks = (new CallbackBuilder).build) {
    if(replicas.size > 0) {
      info("Invoking state change to %s for replicas %s".format(targetState, replicas.mkString(",")))
      try {
        brokerRequestBatch.newBatch()
        replicas.foreach(r => handleStateChange(r, targetState, callbacks))
        brokerRequestBatch.sendRequestsToBrokers(controller.epoch, controllerContext.correlationId.getAndIncrement)
      }catch {
        case e: Throwable => error("Error while moving some replicas to %s state".format(targetState), e)
      }
    }
  }

  /**
   * This API exercises the replica's state machine. It ensures that every state transition happens from a legal
   * previous state to the target state. Valid state transitions are:
   * NonExistentReplica --> NewReplica
   * --send LeaderAndIsr request with current leader and isr to the new replica and UpdateMetadata request for the
   *   partition to every live broker
   *
   * NewReplica -> OnlineReplica
   * --add the new replica to the assigned replica list if needed
   *
   * OnlineReplica,OfflineReplica -> OnlineReplica
   * --send LeaderAndIsr request with current leader and isr to the new replica and UpdateMetadata request for the
   *   partition to every live broker
   *
   * NewReplica,OnlineReplica,OfflineReplica,ReplicaDeletionFailed -> OfflineReplica
   * --send StopReplicaRequest to the replica (w/o deletion)
   * --remove this replica from the isr and send LeaderAndIsr request (with new isr) to the leader replica and
   *   UpdateMetadata request for the partition to every live broker.
   *
   * OfflineReplica -> ReplicaDeletionStarted
   * --send StopReplicaRequest to the replica (with deletion)
   *
   * ReplicaDeletionStarted -> ReplicaDeletionSuccessful
   * -- mark the state of the replica in the state machine
   *
   * ReplicaDeletionStarted -> ReplicaDeletionFailed
   * -- mark the state of the replica in the state machine
   *
   * ReplicaDeletionSuccessful -> NonExistentReplica
   * -- remove the replica from the in memory partition replica assignment cache


   * @param partitionAndReplica The replica for which the state transition is invoked
   * @param targetState The end state that the replica should be moved to
   */
  def handleStateChange(partitionAndReplica: PartitionAndReplica, targetState: ReplicaState,
                        callbacks: Callbacks) {
    val topic = partitionAndReplica.topic
    val partition = partitionAndReplica.partition
    val replicaId = partitionAndReplica.replica
    val topicAndPartition = TopicAndPartition(topic, partition)
    if (!hasStarted.get)
      throw new StateChangeFailedException(("Controller %d epoch %d initiated state change of replica %d for partition %s " +
                                            "to %s failed because replica state machine has not started")
                                              .format(controllerId, controller.epoch, replicaId, topicAndPartition, targetState))
    try {
      replicaState.getOrElseUpdate(partitionAndReplica, NonExistentReplica)
      val replicaAssignment = controllerContext.partitionReplicaAssignment(topicAndPartition)
      targetState match {
        case NewReplica =>
          assertValidPreviousStates(partitionAndReplica, List(NonExistentReplica), targetState)
          // start replica as a follower to the current leader for its partition
          val leaderIsrAndControllerEpochOpt = ZkUtils.getLeaderIsrAndEpochForPartition(zkClient, topic, partition)
          leaderIsrAndControllerEpochOpt match {
            case Some(leaderIsrAndControllerEpoch) =>
              if(leaderIsrAndControllerEpoch.leaderAndIsr.leader == replicaId)
                throw new StateChangeFailedException("Replica %d for partition %s cannot be moved to NewReplica"
                  .format(replicaId, topicAndPartition) + "state as it is being requested to become leader")
              brokerRequestBatch.addLeaderAndIsrRequestForBrokers(List(replicaId),
                                                                  topic, partition, leaderIsrAndControllerEpoch,
                                                                  replicaAssignment)
            case None => // new leader request will be sent to this replica when one gets elected
          }
          replicaState.put(partitionAndReplica, NewReplica)
          stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s to NewReplica"
                                    .format(controllerId, controller.epoch, replicaId, topicAndPartition))
        case ReplicaDeletionStarted =>
          assertValidPreviousStates(partitionAndReplica, List(OfflineReplica), targetState)
          replicaState.put(partitionAndReplica, ReplicaDeletionStarted)
          // send stop replica command
          brokerRequestBatch.addStopReplicaRequestForBrokers(List(replicaId), topic, partition, deletePartition = true,
            callbacks.stopReplicaResponseCallback)
          stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s to ReplicaDeletionStarted"
            .format(controllerId, controller.epoch, replicaId, topicAndPartition))
        case ReplicaDeletionFailed =>
          assertValidPreviousStates(partitionAndReplica, List(ReplicaDeletionStarted), targetState)
          replicaState.put(partitionAndReplica, ReplicaDeletionFailed)
          stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s to ReplicaDeletionFailed"
            .format(controllerId, controller.epoch, replicaId, topicAndPartition))
        case ReplicaDeletionSuccessful =>
          assertValidPreviousStates(partitionAndReplica, List(ReplicaDeletionStarted), targetState)
          replicaState.put(partitionAndReplica, ReplicaDeletionSuccessful)
          stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s to ReplicaDeletionSuccessful"
            .format(controllerId, controller.epoch, replicaId, topicAndPartition))
        case NonExistentReplica =>
          assertValidPreviousStates(partitionAndReplica, List(ReplicaDeletionSuccessful), targetState)
          // remove this replica from the assigned replicas list for its partition
          val currentAssignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
          controllerContext.partitionReplicaAssignment.put(topicAndPartition, currentAssignedReplicas.filterNot(_ == replicaId))
          replicaState.remove(partitionAndReplica)
          stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s to NonExistentReplica"
            .format(controllerId, controller.epoch, replicaId, topicAndPartition))
        case OnlineReplica =>
          assertValidPreviousStates(partitionAndReplica,
            List(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionFailed), targetState)
          replicaState(partitionAndReplica) match {
            case NewReplica =>
              // add this replica to the assigned replicas list for its partition
              val currentAssignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
              if(!currentAssignedReplicas.contains(replicaId))
                controllerContext.partitionReplicaAssignment.put(topicAndPartition, currentAssignedReplicas :+ replicaId)
              stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s to OnlineReplica"
                                        .format(controllerId, controller.epoch, replicaId, topicAndPartition))
            case _ =>
              // check if the leader for this partition ever existed
              controllerContext.partitionLeadershipInfo.get(topicAndPartition) match {
                case Some(leaderIsrAndControllerEpoch) =>
                  brokerRequestBatch.addLeaderAndIsrRequestForBrokers(List(replicaId), topic, partition, leaderIsrAndControllerEpoch,
                    replicaAssignment)
                  replicaState.put(partitionAndReplica, OnlineReplica)
                  stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s to OnlineReplica"
                    .format(controllerId, controller.epoch, replicaId, topicAndPartition))
                case None => // that means the partition was never in OnlinePartition state, this means the broker never
                  // started a log for that partition and does not have a high watermark value for this partition
              }
          }
          replicaState.put(partitionAndReplica, OnlineReplica)
        case OfflineReplica =>
          assertValidPreviousStates(partitionAndReplica,
            List(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionFailed), targetState)
          // send stop replica command to the replica so that it stops fetching from the leader
          brokerRequestBatch.addStopReplicaRequestForBrokers(List(replicaId), topic, partition, deletePartition = false)
          // As an optimization, the controller removes dead replicas from the ISR
          val leaderAndIsrIsEmpty: Boolean =
            controllerContext.partitionLeadershipInfo.get(topicAndPartition) match {
              case Some(currLeaderIsrAndControllerEpoch) =>
                controller.removeReplicaFromIsr(topic, partition, replicaId) match {
                  case Some(updatedLeaderIsrAndControllerEpoch) =>
                    // send the shrunk ISR state change request only to the leader
                    brokerRequestBatch.addLeaderAndIsrRequestForBrokers(List(updatedLeaderIsrAndControllerEpoch.leaderAndIsr.leader),
                      topic, partition, updatedLeaderIsrAndControllerEpoch, replicaAssignment)
                    replicaState.put(partitionAndReplica, OfflineReplica)
                    stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s to OfflineReplica"
                      .format(controllerId, controller.epoch, replicaId, topicAndPartition))
                    false
                  case None =>
                    true
                }
              case None =>
                true
            }
          if (leaderAndIsrIsEmpty)
            throw new StateChangeFailedException(
              "Failed to change state of replica %d for partition %s since the leader and isr path in zookeeper is empty"
              .format(replicaId, topicAndPartition))
      }
    }
    catch {
      case t: Throwable =>
        stateChangeLogger.error("Controller %d epoch %d initiated state change of replica %d for partition [%s,%d] to %s failed"
                                  .format(controllerId, controller.epoch, replicaId, topic, partition, targetState), t)
    }
  }

  def areAllReplicasForTopicDeleted(topic: String): Boolean = {
    val replicasForTopic = controller.controllerContext.replicasForTopic(topic)
    val replicaStatesForTopic = replicasForTopic.map(r => (r, replicaState(r))).toMap
    debug("Are all replicas for topic %s deleted %s".format(topic, replicaStatesForTopic))
    replicaStatesForTopic.foldLeft(true)((deletionState, r) => deletionState && r._2 == ReplicaDeletionSuccessful)
  }

  def isAtLeastOneReplicaInDeletionStartedState(topic: String): Boolean = {
    val replicasForTopic = controller.controllerContext.replicasForTopic(topic)
    val replicaStatesForTopic = replicasForTopic.map(r => (r, replicaState(r))).toMap
    replicaStatesForTopic.foldLeft(false)((deletionState, r) => deletionState || r._2 == ReplicaDeletionStarted)
  }

  def replicasInState(topic: String, state: ReplicaState): Set[PartitionAndReplica] = {
    replicaState.filter(r => r._1.topic.equals(topic) && r._2 == state).keySet
  }

  def replicasInDeletionStates(topic: String): Set[PartitionAndReplica] = {
    val deletionStates = Set(ReplicaDeletionStarted, ReplicaDeletionSuccessful, ReplicaDeletionFailed)
    replicaState.filter(r => r._1.topic.equals(topic) && deletionStates.contains(r._2)).keySet
  }

  private def assertValidPreviousStates(partitionAndReplica: PartitionAndReplica, fromStates: Seq[ReplicaState],
                                        targetState: ReplicaState) {
    assert(fromStates.contains(replicaState(partitionAndReplica)),
      "Replica %s should be in the %s states before moving to %s state"
        .format(partitionAndReplica, fromStates.mkString(","), targetState) +
        ". Instead it is in %s state".format(replicaState(partitionAndReplica)))
  }

  private def registerBrokerChangeListener() = {
    zkClient.subscribeChildChanges(ZkUtils.BrokerIdsPath, new BrokerChangeListener())
  }

  /**
   * Invoked on startup of the replica's state machine to set the initial state for replicas of all existing partitions
   * in zookeeper
   */
  private def initializeReplicaState() {
    for((topicPartition, assignedReplicas) <- controllerContext.partitionReplicaAssignment) {
      val topic = topicPartition.topic
      val partition = topicPartition.partition
      assignedReplicas.foreach { replicaId =>
        val partitionAndReplica = PartitionAndReplica(topic, partition, replicaId)
        controllerContext.liveBrokerIds.contains(replicaId) match {
          case true => replicaState.put(partitionAndReplica, OnlineReplica)
          case false =>
            // mark replicas on dead brokers as failed for topic deletion, if they belong to a topic to be deleted.
            // This is required during controller failover since during controller failover a broker can go down,
            // so the replicas on that broker should be moved to ReplicaDeletionFailed to be on the safer side.
            replicaState.put(partitionAndReplica, ReplicaDeletionFailed)
        }
      }
    }
  }

  def partitionsAssignedToBroker(topics: Seq[String], brokerId: Int):Seq[TopicAndPartition] = {
    controllerContext.partitionReplicaAssignment.filter(_._2.contains(brokerId)).keySet.toSeq
  }

  /**
   * This is the zookeeper listener that triggers all the state transitions for a replica
   */
  class BrokerChangeListener() extends IZkChildListener with Logging {
    this.logIdent = "[BrokerChangeListener on Controller " + controller.config.brokerId + "]: "
    def handleChildChange(parentPath : String, currentBrokerList : java.util.List[String]) {
      info("Broker change listener fired for path %s with children %s".format(parentPath, currentBrokerList.mkString(",")))
      inLock(controllerContext.controllerLock) {
        if (hasStarted.get) {
          ControllerStats.leaderElectionTimer.time {
            try {
              val curBrokerIds = currentBrokerList.map(_.toInt).toSet
              val newBrokerIds = curBrokerIds -- controllerContext.liveOrShuttingDownBrokerIds
              val newBrokerInfo = newBrokerIds.map(ZkUtils.getBrokerInfo(zkClient, _))
              val newBrokers = newBrokerInfo.filter(_.isDefined).map(_.get)
              val deadBrokerIds = controllerContext.liveOrShuttingDownBrokerIds -- curBrokerIds
              controllerContext.liveBrokers = curBrokerIds.map(ZkUtils.getBrokerInfo(zkClient, _)).filter(_.isDefined).map(_.get)
              info("Newly added brokers: %s, deleted brokers: %s, all live brokers: %s"
                .format(newBrokerIds.mkString(","), deadBrokerIds.mkString(","), controllerContext.liveBrokerIds.mkString(",")))
              newBrokers.foreach(controllerContext.controllerChannelManager.addBroker(_))
              deadBrokerIds.foreach(controllerContext.controllerChannelManager.removeBroker(_))
              if(newBrokerIds.size > 0)
                controller.onBrokerStartup(newBrokerIds.toSeq)
              if(deadBrokerIds.size > 0)
                controller.onBrokerFailure(deadBrokerIds.toSeq)
            } catch {
              case e: Throwable => error("Error while handling broker changes", e)
            }
          }
        }
      }
    }
  }
}

sealed trait ReplicaState { def state: Byte }
case object NewReplica extends ReplicaState { val state: Byte = 1 }
case object OnlineReplica extends ReplicaState { val state: Byte = 2 }
case object OfflineReplica extends ReplicaState { val state: Byte = 3 }
case object ReplicaDeletionStarted extends ReplicaState { val state: Byte = 4}
case object ReplicaDeletionSuccessful extends ReplicaState { val state: Byte = 5}
case object ReplicaDeletionFailed extends ReplicaState { val state: Byte = 6}
case object NonExistentReplica extends ReplicaState { val state: Byte = 7 }


