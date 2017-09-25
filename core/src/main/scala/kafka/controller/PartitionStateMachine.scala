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

import kafka.api.LeaderAndIsr
import kafka.common.{LeaderElectionNotNeededException, NoReplicaOnlineException, StateChangeFailedException, TopicAndPartition}
import kafka.controller.Callbacks.CallbackBuilder
import kafka.utils.ZkUtils._
import kafka.utils.{Logging, ReplicationUtils}
import org.I0Itec.zkclient.exception.ZkNodeExistsException

import scala.collection._

/**
 * This class represents the state machine for partitions. It defines the states that a partition can be in, and
 * transitions to move the partition to another legal state. The different states that a partition can be in are -
 * 1. NonExistentPartition: This state indicates that the partition was either never created or was created and then
 *                          deleted. Valid previous state, if one exists, is OfflinePartition
 * 2. NewPartition        : After creation, the partition is in the NewPartition state. In this state, the partition should have
 *                          replicas assigned to it, but no leader/isr yet. Valid previous states are NonExistentPartition
 * 3. OnlinePartition     : Once a leader is elected for a partition, it is in the OnlinePartition state.
 *                          Valid previous states are NewPartition/OfflinePartition
 * 4. OfflinePartition    : If, after successful leader election, the leader for partition dies, then the partition
 *                          moves to the OfflinePartition state. Valid previous states are NewPartition/OnlinePartition
 */
class PartitionStateMachine(controller: KafkaController, stateChangeLogger: StateChangeLogger) extends Logging {

  private val controllerContext = controller.controllerContext
  private val controllerId = controller.config.brokerId
  private val zkUtils = controllerContext.zkUtils
  private val partitionState: mutable.Map[TopicAndPartition, PartitionState] = mutable.Map.empty
  private val brokerRequestBatch = new ControllerBrokerRequestBatch(controller, stateChangeLogger)
  private val noOpPartitionLeaderSelector = new NoOpLeaderSelector(controllerContext)

  this.logIdent = s"[PartitionStateMachine controllerId=$controllerId] "

  /**
   * Invoked on successful controller election. First registers a topic change listener since that triggers all
   * state transitions for partitions. Initializes the state of partitions by reading from zookeeper. Then triggers
   * the OnlinePartition state change for all new or offline partitions.
   */
  def startup() {
    initializePartitionState()
    triggerOnlinePartitionStateChange()

    info(s"Started partition state machine with initial state -> $partitionState")
  }

  /**
   * Invoked on controller shutdown.
   */
  def shutdown() {
    partitionState.clear()

    info("Stopped partition state machine")
  }

  /**
   * This API invokes the OnlinePartition state change on all partitions in either the NewPartition or OfflinePartition
   * state. This is called on a successful controller election and on broker changes
   */
  def triggerOnlinePartitionStateChange() {
    try {
      brokerRequestBatch.newBatch()
      // try to move all partitions in NewPartition or OfflinePartition state to OnlinePartition state except partitions
      // that belong to topics to be deleted
      for ((topicAndPartition, partitionState) <- partitionState
          if !controller.topicDeletionManager.isTopicQueuedUpForDeletion(topicAndPartition.topic)) {
        if (partitionState.equals(OfflinePartition) || partitionState.equals(NewPartition))
          handleStateChange(topicAndPartition.topic, topicAndPartition.partition, OnlinePartition, controller.offlinePartitionSelector,
                            (new CallbackBuilder).build)
      }
      brokerRequestBatch.sendRequestsToBrokers(controller.epoch)
    } catch {
      case e: Throwable => error("Error while moving some partitions to the online state", e)
      // TODO: It is not enough to bail out and log an error, it is important to trigger leader election for those partitions
    }
  }

  def partitionsInState(state: PartitionState): Set[TopicAndPartition] = {
    partitionState.filter(p => p._2 == state).keySet
  }

  /**
   * This API is invoked by the partition change zookeeper listener
   * @param partitions   The list of partitions that need to be transitioned to the target state
   * @param targetState  The state that the partitions should be moved to
   */
  def handleStateChanges(partitions: Set[TopicAndPartition], targetState: PartitionState,
                         leaderSelector: PartitionLeaderSelector = noOpPartitionLeaderSelector,
                         callbacks: Callbacks = (new CallbackBuilder).build) {
    info("Invoking state change to %s for partitions %s".format(targetState, partitions.mkString(",")))
    try {
      brokerRequestBatch.newBatch()
      partitions.foreach { topicAndPartition =>
        handleStateChange(topicAndPartition.topic, topicAndPartition.partition, targetState, leaderSelector, callbacks)
      }
      brokerRequestBatch.sendRequestsToBrokers(controller.epoch)
    } catch {
      case e: Throwable => error("Error while moving some partitions to %s state".format(targetState), e)
      // TODO: It is not enough to bail out and log an error, it is important to trigger state changes for those partitions
    }
  }

  /**
   * This API exercises the partition's state machine. It ensures that every state transition happens from a legal
   * previous state to the target state. Valid state transitions are:
   * NonExistentPartition -> NewPartition:
   * --load assigned replicas from ZK to controller cache
   *
   * NewPartition -> OnlinePartition
   * --assign first live replica as the leader and all live replicas as the isr; write leader and isr to ZK for this partition
   * --send LeaderAndIsr request to every live replica and UpdateMetadata request to every live broker
   *
   * OnlinePartition,OfflinePartition -> OnlinePartition
   * --select new leader and isr for this partition and a set of replicas to receive the LeaderAndIsr request, and write leader and isr to ZK
   * --for this partition, send LeaderAndIsr request to every receiving replica and UpdateMetadata request to every live broker
   *
   * NewPartition,OnlinePartition,OfflinePartition -> OfflinePartition
   * --nothing other than marking partition state as Offline
   *
   * OfflinePartition -> NonExistentPartition
   * --nothing other than marking the partition state as NonExistentPartition
   * @param topic       The topic of the partition for which the state transition is invoked
   * @param partition   The partition for which the state transition is invoked
   * @param targetState The end state that the partition should be moved to
   */
  private def handleStateChange(topic: String, partition: Int, targetState: PartitionState,
                                leaderSelector: PartitionLeaderSelector,
                                callbacks: Callbacks) {
    val topicAndPartition = TopicAndPartition(topic, partition)
    val currState = partitionState.getOrElseUpdate(topicAndPartition, NonExistentPartition)
    val stateChangeLog = stateChangeLogger.withControllerEpoch(controller.epoch)
    try {
      assertValidTransition(topicAndPartition, targetState)
      targetState match {
        case NewPartition =>
          partitionState.put(topicAndPartition, NewPartition)
          val assignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition).mkString(",")
          stateChangeLog.trace(s"Changed partition $topicAndPartition state from $currState to $targetState with " +
            s"assigned replicas $assignedReplicas")
          // post: partition has been assigned replicas
        case OnlinePartition =>
          partitionState(topicAndPartition) match {
            case NewPartition =>
              // initialize leader and isr path for new partition
              initializeLeaderAndIsrForPartition(topicAndPartition)
            case OfflinePartition =>
              electLeaderForPartition(topic, partition, leaderSelector)
            case OnlinePartition => // invoked when the leader needs to be re-elected
              electLeaderForPartition(topic, partition, leaderSelector)
            case _ => // should never come here since illegal previous states are checked above
          }
          partitionState.put(topicAndPartition, OnlinePartition)
          val leader = controllerContext.partitionLeadershipInfo(topicAndPartition).leaderAndIsr.leader
          stateChangeLog.trace(s"Changed partition $topicAndPartition from $currState to $targetState with leader $leader")
           // post: partition has a leader
        case OfflinePartition =>
          // should be called when the leader for a partition is no longer alive
          stateChangeLog.trace(s"Changed partition $topicAndPartition state from $currState to $targetState")
          partitionState.put(topicAndPartition, OfflinePartition)
          // post: partition has no alive leader
        case NonExistentPartition =>
          stateChangeLogger.trace(s"Changed partition $topicAndPartition state from $currState to $targetState")
          partitionState.put(topicAndPartition, NonExistentPartition)
          // post: partition state is deleted from all brokers and zookeeper
      }
    } catch {
      case t: Throwable =>
        stateChangeLog.error(s"Initiated state change for partition $topicAndPartition from $currState to $targetState failed",
          t)
    }
  }

  /**
   * Invoked on startup of the partition's state machine to set the initial state for all existing partitions in
   * zookeeper
   */
  private def initializePartitionState() {
    for (topicPartition <- controllerContext.partitionReplicaAssignment.keys) {
      // check if leader and isr path exists for partition. If not, then it is in NEW state
      controllerContext.partitionLeadershipInfo.get(topicPartition) match {
        case Some(currentLeaderIsrAndEpoch) =>
          // else, check if the leader for partition is alive. If yes, it is in Online state, else it is in Offline state
          if (controllerContext.isReplicaOnline(currentLeaderIsrAndEpoch.leaderAndIsr.leader, topicPartition))
            // leader is alive
            partitionState.put(topicPartition, OnlinePartition)
          else
            partitionState.put(topicPartition, OfflinePartition)
        case None =>
          partitionState.put(topicPartition, NewPartition)
      }
    }
  }

  private def assertValidTransition(topicAndPartition: TopicAndPartition, targetState: PartitionState): Unit = {
    if (!targetState.validPreviousStates.contains(partitionState(topicAndPartition)))
      throw new IllegalStateException("Partition %s should be in the %s states before moving to %s state"
        .format(topicAndPartition, targetState.validPreviousStates.mkString(","), targetState) + ". Instead it is in %s state"
        .format(partitionState(topicAndPartition)))
  }

  /**
   * Invoked on the NewPartition->OnlinePartition state change. When a partition is in the New state, it does not have
   * a leader and isr path in zookeeper. Once the partition moves to the OnlinePartition state, its leader and isr
   * path gets initialized and it never goes back to the NewPartition state. From here, it can only go to the
   * OfflinePartition state.
   * @param topicAndPartition   The topic/partition whose leader and isr path is to be initialized
   */
  private def initializeLeaderAndIsrForPartition(topicAndPartition: TopicAndPartition) = {
    val replicaAssignment = controllerContext.partitionReplicaAssignment(topicAndPartition).toList
    val liveAssignedReplicas = replicaAssignment.filter(r => controllerContext.isReplicaOnline(r, topicAndPartition))
    val stateChangeLog = stateChangeLogger.withControllerEpoch(controller.epoch)
    liveAssignedReplicas.headOption match {
      case None =>
        val failMsg = s"Encountered error during state change of partition $topicAndPartition from New to Online, " +
          s"assigned replicas are [${replicaAssignment.mkString(",")}], live brokers are " +
          s"[${controllerContext.liveBrokerIds}]. No assigned replica is alive."
        stateChangeLog.error(failMsg)
        throw new StateChangeFailedException(stateChangeLog.messageWithPrefix(failMsg))

      // leader is the first replica in the list of assigned replicas
      case Some(leader) =>
        debug(s"Live assigned replicas for partition $topicAndPartition are: [$liveAssignedReplicas]")
        val leaderAndIsr = LeaderAndIsr(leader, liveAssignedReplicas)
        val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controller.epoch)
        debug(s"Initializing leader and isr for partition $topicAndPartition to $leaderIsrAndControllerEpoch")

        try {
          zkUtils.createPersistentPath(
            getTopicPartitionLeaderAndIsrPath(topicAndPartition.topic, topicAndPartition.partition),
            zkUtils.leaderAndIsrZkData(leaderAndIsr, controller.epoch)
          )
          // NOTE: the above write can fail only if the current controller lost its zk session and the new controller
          // took over and initialized this partition. This can happen if the current controller went into a long
          // GC pause
          controllerContext.partitionLeadershipInfo.put(topicAndPartition, leaderIsrAndControllerEpoch)
          brokerRequestBatch.addLeaderAndIsrRequestForBrokers(
            liveAssignedReplicas,
            topicAndPartition.topic,
            topicAndPartition.partition,
            leaderIsrAndControllerEpoch,
            replicaAssignment,
            isNew = true
          )
        } catch {
          case _: ZkNodeExistsException =>
            // read the controller epoch
            val leaderIsrAndEpoch = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, topicAndPartition.topic,
              topicAndPartition.partition).get

            val failMsg = s"Encountered error while changing partition $topicAndPartition's state from New to Online " +
              s"since LeaderAndIsr path already exists with value ${leaderIsrAndEpoch.leaderAndIsr} and controller " +
              s"epoch ${leaderIsrAndEpoch.controllerEpoch}"
            stateChangeLog.error(failMsg)
            throw new StateChangeFailedException(stateChangeLog.messageWithPrefix(failMsg))
        }
    }
  }

  /**
   * Invoked on the OfflinePartition,OnlinePartition->OnlinePartition state change.
   * It invokes the leader election API to elect a leader for the input offline partition
   * @param topic               The topic of the offline partition
   * @param partition           The offline partition
   * @param leaderSelector      Specific leader selector (e.g., offline/reassigned/etc.)
   */
  def electLeaderForPartition(topic: String, partition: Int, leaderSelector: PartitionLeaderSelector) {
    val topicAndPartition = TopicAndPartition(topic, partition)
    val stateChangeLog = stateChangeLogger.withControllerEpoch(controller.epoch)
    // handle leader election for the partitions whose leader is no longer alive
    stateChangeLog.trace(s"Started leader election for partition $topicAndPartition")
    try {
      var zookeeperPathUpdateSucceeded: Boolean = false
      var newLeaderAndIsr: LeaderAndIsr = null
      var replicasForThisPartition: Seq[Int] = Seq.empty[Int]
      while(!zookeeperPathUpdateSucceeded) {
        val currentLeaderIsrAndEpoch = getLeaderIsrAndEpochOrThrowException(topic, partition)
        val currentLeaderAndIsr = currentLeaderIsrAndEpoch.leaderAndIsr
        val controllerEpoch = currentLeaderIsrAndEpoch.controllerEpoch
        if (controllerEpoch > controller.epoch) {
          val failMsg = s"Aborted leader election for partition $topicAndPartition since the LeaderAndIsr path was " +
            s"already written by another controller. This probably means that the current controller $controllerId went " +
            s"through a soft failure and another controller was elected with epoch $controllerEpoch."
          stateChangeLog.error(failMsg)
          throw new StateChangeFailedException(stateChangeLog.messageWithPrefix(failMsg))
        }
        // elect new leader or throw exception
        val (leaderAndIsr, replicas) = leaderSelector.selectLeader(topicAndPartition, currentLeaderAndIsr)
        val (updateSucceeded, newVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, topic, partition,
          leaderAndIsr, controller.epoch, currentLeaderAndIsr.zkVersion)
        newLeaderAndIsr = leaderAndIsr.withZkVersion(newVersion)
        zookeeperPathUpdateSucceeded = updateSucceeded
        replicasForThisPartition = replicas
      }
      val newLeaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(newLeaderAndIsr, controller.epoch)
      // update the leader cache
      controllerContext.partitionLeadershipInfo.put(TopicAndPartition(topic, partition), newLeaderIsrAndControllerEpoch)
      stateChangeLog.trace(s"Elected leader ${newLeaderAndIsr.leader} for Offline partition $topicAndPartition")
      val replicas = controllerContext.partitionReplicaAssignment(TopicAndPartition(topic, partition))
      // store new leader and isr info in cache
      brokerRequestBatch.addLeaderAndIsrRequestForBrokers(replicasForThisPartition, topic, partition,
        newLeaderIsrAndControllerEpoch, replicas)
    } catch {
      case _: LeaderElectionNotNeededException => // swallow
      case nroe: NoReplicaOnlineException => throw nroe
      case sce: Throwable =>
        val failMsg = s"Encountered error while electing leader for partition $topicAndPartition due to: ${sce.getMessage}"
        stateChangeLog.error(failMsg)
        throw new StateChangeFailedException(stateChangeLog.messageWithPrefix(failMsg), sce)
    }
    debug(s"After leader election, leader cache is updated to ${controllerContext.partitionLeadershipInfo}")
  }

  private def getLeaderIsrAndEpochOrThrowException(topic: String, partition: Int): LeaderIsrAndControllerEpoch = {
    val topicAndPartition = TopicAndPartition(topic, partition)
    ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, topic, partition) match {
      case Some(currentLeaderIsrAndEpoch) => currentLeaderIsrAndEpoch
      case None => throw new StateChangeFailedException("LeaderAndIsr information doesn't exist for " +
        s"partition $topicAndPartition in ${partitionState(topicAndPartition)} state")
    }
  }
}

sealed trait PartitionState {
  def state: Byte
  def validPreviousStates: Set[PartitionState]
}

case object NewPartition extends PartitionState {
  val state: Byte = 0
  val validPreviousStates: Set[PartitionState] = Set(NonExistentPartition)
}

case object OnlinePartition extends PartitionState {
  val state: Byte = 1
  val validPreviousStates: Set[PartitionState] = Set(NewPartition, OnlinePartition, OfflinePartition)
}

case object OfflinePartition extends PartitionState {
  val state: Byte = 2
  val validPreviousStates: Set[PartitionState] = Set(NewPartition, OnlinePartition, OfflinePartition)
}

case object NonExistentPartition extends PartitionState {
  val state: Byte = 3
  val validPreviousStates: Set[PartitionState] = Set(OfflinePartition)
}
