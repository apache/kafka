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
 * 4. NonExistentReplica: If a replica is deleted, it is moved to this state. Valid previous state is OfflineReplica
 */
class ReplicaStateMachine(controller: KafkaController) extends Logging {
  private val controllerContext = controller.controllerContext
  private val controllerId = controller.config.brokerId
  private val zkClient = controllerContext.zkClient
  var replicaState: mutable.Map[(String, Int, Int), ReplicaState] = mutable.Map.empty
  val brokerRequestBatch = new ControllerBrokerRequestBatch(controller.sendRequest, controller.config.brokerId)
  private val isShuttingDown = new AtomicBoolean(false)
  this.logIdent = "[Replica state machine on controller " + controller.config.brokerId + "]: "
  private val stateChangeLogger = Logger.getLogger(KafkaController.stateChangeLogger)

  /**
   * Invoked on successful controller election. First registers a broker change listener since that triggers all
   * state transitions for replicas. Initializes the state of replicas for all partitions by reading from zookeeper.
   * Then triggers the OnlineReplica state change for all replicas.
   */
  def startup() {
    isShuttingDown.set(false)
    // initialize replica state
    initializeReplicaState()
    // move all Online replicas to Online
    handleStateChanges(ZkUtils.getAllReplicasOnBroker(zkClient, controllerContext.allTopics.toSeq,
      controllerContext.liveBrokerIds.toSeq), OnlineReplica)
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
    isShuttingDown.compareAndSet(false, true)
    replicaState.clear()
  }

  /**
   * This API is invoked by the broker change controller callbacks and the startup API of the state machine
   * @param replicas     The list of replicas (brokers) that need to be transitioned to the target state
   * @param targetState  The state that the replicas should be moved to
   * The controller's allLeaders cache should have been updated before this
   */
  def handleStateChanges(replicas: Set[PartitionAndReplica], targetState: ReplicaState) {
    info("Invoking state change to %s for replicas %s".format(targetState, replicas.mkString(",")))
    try {
      brokerRequestBatch.newBatch()
      replicas.foreach(r => handleStateChange(r.topic, r.partition, r.replica, targetState))
      brokerRequestBatch.sendRequestsToBrokers(controller.epoch, controllerContext.correlationId.getAndIncrement, controllerContext.liveBrokers)
    }catch {
      case e => error("Error while moving some replicas to %s state".format(targetState), e)
    }
  }

  /**
   * This API exercises the replica's state machine. It ensures that every state transition happens from a legal
   * previous state to the target state.
   * @param topic       The topic of the replica for which the state transition is invoked
   * @param partition   The partition of the replica for which the state transition is invoked
   * @param replicaId   The replica for which the state transition is invoked
   * @param targetState The end state that the replica should be moved to
   */
  def handleStateChange(topic: String, partition: Int, replicaId: Int, targetState: ReplicaState) {
    val topicAndPartition = TopicAndPartition(topic, partition)
    try {
      replicaState.getOrElseUpdate((topic, partition, replicaId), NonExistentReplica)
      val replicaAssignment = controllerContext.partitionReplicaAssignment(topicAndPartition)
      targetState match {
        case NewReplica =>
          assertValidPreviousStates(topic, partition, replicaId, List(NonExistentReplica), targetState)
          // start replica as a follower to the current leader for its partition
          val leaderIsrAndControllerEpochOpt = ZkUtils.getLeaderIsrAndEpochForPartition(zkClient, topic, partition)
          leaderIsrAndControllerEpochOpt match {
            case Some(leaderIsrAndControllerEpoch) =>
              if(leaderIsrAndControllerEpoch.leaderAndIsr.leader == replicaId)
                throw new StateChangeFailedException("Replica %d for partition %s cannot be moved to NewReplica"
                  .format(replicaId, topicAndPartition) + "state as it is being requested to become leader")
              brokerRequestBatch.addLeaderAndIsrRequestForBrokers(List(replicaId),
                                                                  topic, partition, leaderIsrAndControllerEpoch, replicaAssignment.size)
            case None => // new leader request will be sent to this replica when one gets elected
          }
          replicaState.put((topic, partition, replicaId), NewReplica)
          stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s to NewReplica"
                                    .format(controllerId, controller.epoch, replicaId, topicAndPartition))
        case NonExistentReplica =>
          assertValidPreviousStates(topic, partition, replicaId, List(OfflineReplica), targetState)
          // send stop replica command
          brokerRequestBatch.addStopReplicaRequestForBrokers(List(replicaId), topic, partition, deletePartition = true)
          // remove this replica from the assigned replicas list for its partition
          val currentAssignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
          controllerContext.partitionReplicaAssignment.put(topicAndPartition, currentAssignedReplicas.filterNot(_ == replicaId))
          replicaState.remove((topic, partition, replicaId))
          stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s to NonExistentReplica"
                                    .format(controllerId, controller.epoch, replicaId, topicAndPartition))
        case OnlineReplica =>
          assertValidPreviousStates(topic, partition, replicaId, List(NewReplica, OnlineReplica, OfflineReplica), targetState)
          replicaState((topic, partition, replicaId)) match {
            case NewReplica =>
              // add this replica to the assigned replicas list for its partition
              val currentAssignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
              controllerContext.partitionReplicaAssignment.put(topicAndPartition, currentAssignedReplicas :+ replicaId)
              stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s to OnlineReplica"
                                        .format(controllerId, controller.epoch, replicaId, topicAndPartition))
            case _ =>
              // check if the leader for this partition is alive or even exists
                controllerContext.allLeaders.get(topicAndPartition) match {
                case Some(leaderIsrAndControllerEpoch) =>
                  controllerContext.liveBrokerIds.contains(leaderIsrAndControllerEpoch.leaderAndIsr.leader) match {
                    case true => // leader is alive
                      brokerRequestBatch.addLeaderAndIsrRequestForBrokers(List(replicaId),
                                                                          topic, partition, leaderIsrAndControllerEpoch,
                                                                          replicaAssignment.size)
                      replicaState.put((topic, partition, replicaId), OnlineReplica)
                      stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s to OnlineReplica"
                                                .format(controllerId, controller.epoch, replicaId, topicAndPartition))
                    case false => // ignore partitions whose leader is not alive
                  }
                case None => // ignore partitions who don't have a leader yet
              }
          }
          replicaState.put((topic, partition, replicaId), OnlineReplica)
        case OfflineReplica =>
          assertValidPreviousStates(topic, partition, replicaId, List(NewReplica, OnlineReplica), targetState)
          // As an optimization, the controller removes dead replicas from the ISR
          val leaderAndIsrIsEmpty: Boolean =
            controllerContext.allLeaders.get(topicAndPartition) match {
              case Some(currLeaderIsrAndControllerEpoch) =>
                if (currLeaderIsrAndControllerEpoch.leaderAndIsr.isr.contains(replicaId))
                  controller.removeReplicaFromIsr(topic, partition, replicaId) match {
                    case Some(updatedLeaderIsrAndControllerEpoch) =>
                      // send the shrunk ISR state change request only to the leader
                      brokerRequestBatch.addLeaderAndIsrRequestForBrokers(List(updatedLeaderIsrAndControllerEpoch.leaderAndIsr.leader),
                        topic, partition, updatedLeaderIsrAndControllerEpoch,
                        replicaAssignment.size)
                      replicaState.put((topic, partition, replicaId), OfflineReplica)
                      stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s to OfflineReplica"
                                                .format(controllerId, controller.epoch, replicaId, topicAndPartition))
                      false
                    case None =>
                      true
                  }
                else false
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

  private def assertValidPreviousStates(topic: String, partition: Int, replicaId: Int, fromStates: Seq[ReplicaState],
                                        targetState: ReplicaState) {
    assert(fromStates.contains(replicaState((topic, partition, replicaId))),
      "Replica %s for partition [%s,%d] should be in the %s states before moving to %s state"
        .format(replicaId, topic, partition, fromStates.mkString(","), targetState) +
        ". Instead it is in %s state".format(replicaState((topic, partition, replicaId))))
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
        controllerContext.liveBrokerIds.contains(replicaId) match {
          case true => replicaState.put((topic, partition, replicaId), OnlineReplica)
          case false => replicaState.put((topic, partition, replicaId), OfflineReplica)
        }
      }
    }
  }

  def getPartitionsAssignedToBroker(topics: Seq[String], brokerId: Int):Seq[TopicAndPartition] = {
    controllerContext.partitionReplicaAssignment.filter(_._2.contains(brokerId)).keySet.toSeq
  }

  /**
   * This is the zookeeper listener that triggers all the state transitions for a replica
   */
  class BrokerChangeListener() extends IZkChildListener with Logging {
    this.logIdent = "[BrokerChangeListener on Controller " + controller.config.brokerId + "]: "
    def handleChildChange(parentPath : String, currentBrokerList : java.util.List[String]) {
      ControllerStats.leaderElectionTimer.time {
        info("Broker change listener fired for path %s with children %s".format(parentPath, currentBrokerList.mkString(",")))
        if(!isShuttingDown.get()) {
          controllerContext.controllerLock synchronized {
            try {
              val curBrokerIds = currentBrokerList.map(_.toInt).toSet
              val newBrokerIds = curBrokerIds -- controllerContext.liveOrShuttingDownBrokerIds
              val newBrokers = newBrokerIds.map(ZkUtils.getBrokerInfo(zkClient, _)).filter(_.isDefined).map(_.get)
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
              case e => error("Error while handling broker changes", e)
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
case object NonExistentReplica extends ReplicaState { val state: Byte = 4 }


