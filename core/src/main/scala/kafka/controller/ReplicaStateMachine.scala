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
import kafka.utils.{ZkUtils, Logging}
import collection.JavaConversions._
import kafka.api.LeaderAndIsr
import kafka.common.StateChangeFailedException
import java.util.concurrent.atomic.AtomicBoolean
import org.I0Itec.zkclient.IZkChildListener

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
  this.logIdent = "[Replica state machine on Controller " + controller.config.brokerId + "]: "
  private val controllerContext = controller.controllerContext
  private val zkClient = controllerContext.zkClient
  var replicaState: mutable.Map[(String, Int, Int), ReplicaState] = mutable.Map.empty
  val brokerRequestBatch = new ControllerBrokerRequestBatch(controller.sendRequest)
  private var isShuttingDown = new AtomicBoolean(false)

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
   * @param brokerIds    The list of brokers that need to be transitioned to the target state
   * @param targetState  The state that the replicas should be moved to
   * The controller's allLeaders cache should have been updated before this
   */
  def handleStateChanges(replicas: Seq[PartitionAndReplica], targetState: ReplicaState) {
    info("Invoking state change to %s for replicas %s".format(targetState, replicas.mkString(",")))
    try {
      brokerRequestBatch.newBatch()
      replicas.foreach(r => handleStateChange(r.topic, r.partition, r.replica, targetState))
      brokerRequestBatch.sendRequestsToBrokers()
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
    try {
      replicaState.getOrElseUpdate((topic, partition, replicaId), NonExistentReplica)
      targetState match {
        case NewReplica =>
          assertValidPreviousStates(topic, partition, replicaId, List(NonExistentReplica), targetState)
          // start replica as a follower to the current leader for its partition
          val leaderAndIsrOpt = ZkUtils.getLeaderAndIsrForPartition(zkClient, topic, partition)
          leaderAndIsrOpt match {
            case Some(leaderAndIsr) =>
              if(leaderAndIsr.leader == replicaId)
                throw new StateChangeFailedException("Replica %d for partition [%s, %d] cannot be moved to NewReplica"
                  .format(replicaId, topic, partition) + "state as it is being requested to become leader")
              brokerRequestBatch.addLeaderAndIsrRequestForBrokers(List(replicaId), topic, partition, leaderAndIsr)
            case None => // new leader request will be sent to this replica when one gets elected
          }
          replicaState.put((topic, partition, replicaId), NewReplica)
          info("Replica %d for partition [%s, %d] state changed to NewReplica".format(replicaId, topic, partition))
        case NonExistentReplica =>
          assertValidPreviousStates(topic, partition, replicaId, List(OfflineReplica), targetState)
          // send stop replica command
          brokerRequestBatch.addStopReplicaRequestForBrokers(List(replicaId), topic, partition)
          // remove this replica from the assigned replicas list for its partition
          val currentAssignedReplicas = controllerContext.partitionReplicaAssignment((topic, partition))
          controllerContext.partitionReplicaAssignment.put((topic, partition),
            currentAssignedReplicas.filterNot(_ == replicaId))
          info("Replica %d for partition [%s, %d] state changed to NonExistentReplica".format(replicaId, topic, partition))
          replicaState.remove((topic, partition, replicaId))
        case OnlineReplica =>
          assertValidPreviousStates(topic, partition, replicaId, List(NewReplica, OnlineReplica, OfflineReplica), targetState)
          replicaState((topic, partition, replicaId)) match {
            case NewReplica =>
              // add this replica to the assigned replicas list for its partition
              val currentAssignedReplicas = controllerContext.partitionReplicaAssignment((topic, partition))
              controllerContext.partitionReplicaAssignment.put((topic, partition), currentAssignedReplicas :+ replicaId)
              info("Replica %d for partition [%s, %d] state changed to OnlineReplica".format(replicaId, topic, partition))
            case _ =>
              // check if the leader for this partition is alive or even exists
              // NOTE: technically, we could get the leader from the allLeaders cache, but we need to read zookeeper
              // for the ISR anyways
              val leaderAndIsrOpt = ZkUtils.getLeaderAndIsrForPartition(zkClient, topic, partition)
              leaderAndIsrOpt match {
                case Some(leaderAndIsr) =>
                  controllerContext.liveBrokerIds.contains(leaderAndIsr.leader) match {
                    case true => // leader is alive
                      brokerRequestBatch.addLeaderAndIsrRequestForBrokers(List(replicaId), topic, partition, leaderAndIsr)
                      replicaState.put((topic, partition, replicaId), OnlineReplica)
                      info("Replica %d for partition [%s, %d] state changed to OnlineReplica".format(replicaId, topic, partition))
                    case false => // ignore partitions whose leader is not alive
                  }
                case None => // ignore partitions who don't have a leader yet
              }
          }
          replicaState.put((topic, partition, replicaId), OnlineReplica)
        case OfflineReplica =>
          assertValidPreviousStates(topic, partition, replicaId, List(NewReplica, OnlineReplica), targetState)
          // As an optimization, the controller removes dead replicas from the ISR
          var zookeeperPathUpdateSucceeded: Boolean = false
          var newLeaderAndIsr: LeaderAndIsr = null
          while(!zookeeperPathUpdateSucceeded) {
            // refresh leader and isr from zookeeper again
            val leaderAndIsrOpt = ZkUtils.getLeaderAndIsrForPartition(zkClient, topic, partition)
            leaderAndIsrOpt match {
              case Some(leaderAndIsr) => // increment the leader epoch even if the ISR changes
                newLeaderAndIsr = new LeaderAndIsr(leaderAndIsr.leader, leaderAndIsr.leaderEpoch + 1,
                  leaderAndIsr.isr.filter(b => b != replicaId), leaderAndIsr.zkVersion + 1)
                info("New leader and ISR for partition [%s, %d] is %s".format(topic, partition, newLeaderAndIsr.toString()))
                // update the new leadership decision in zookeeper or retry
                val (updateSucceeded, newVersion) = ZkUtils.conditionalUpdatePersistentPath(zkClient,
                  ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partition), newLeaderAndIsr.toString,
                  leaderAndIsr.zkVersion)
                newLeaderAndIsr.zkVersion = newVersion
                zookeeperPathUpdateSucceeded = updateSucceeded
              case None => throw new StateChangeFailedException("Failed to change state of replica %d".format(replicaId) +
                " for partition [%s, %d] since the leader and isr path in zookeeper is empty".format(topic, partition))
            }
          }
          // send the shrunk ISR state change request only to the leader
          brokerRequestBatch.addLeaderAndIsrRequestForBrokers(List(newLeaderAndIsr.leader), topic, partition, newLeaderAndIsr)
          // update the local leader and isr cache
          controllerContext.allLeaders.put((topic, partition), newLeaderAndIsr.leader)
          replicaState.put((topic, partition, replicaId), OfflineReplica)
          info("Replica %d for partition [%s, %d] state changed to OfflineReplica".format(replicaId, topic, partition))
          info("Removed offline replica %d from ISR for partition [%s, %d]".format(replicaId, topic, partition))
      }
    }catch {
      case e => error("Error while changing state of replica %d for partition ".format(replicaId) +
        "[%s, %d] to %s".format(topic, partition, targetState), e)
    }
  }

  private def assertValidPreviousStates(topic: String, partition: Int, replicaId: Int, fromStates: Seq[ReplicaState],
                                        targetState: ReplicaState) {
    assert(fromStates.contains(replicaState((topic, partition, replicaId))),
      "Replica %s for partition [%s, %d] should be in the %s states before moving to %s state"
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
      val topic = topicPartition._1
      val partition = topicPartition._2
      assignedReplicas.foreach { replicaId =>
        controllerContext.liveBrokerIds.contains(replicaId) match {
          case true => replicaState.put((topic, partition, replicaId), OnlineReplica)
          case false => replicaState.put((topic, partition, replicaId), OfflineReplica)
        }
      }
    }
  }

  def getPartitionsAssignedToBroker(topics: Seq[String], brokerId: Int):Seq[(String, Int)] = {
    controllerContext.partitionReplicaAssignment.filter(_._2.contains(brokerId)).keySet.toSeq
  }

  /**
   * This is the zookeeper listener that triggers all the state transitions for a replica
   */
  class BrokerChangeListener() extends IZkChildListener with Logging {
    this.logIdent = "[BrokerChangeListener on Controller " + controller.config.brokerId + "]: "
    def handleChildChange(parentPath : String, currentBrokerList : java.util.List[String]) {
      ControllerStat.leaderElectionTimer.time {
        info("Broker change listener fired for path %s with children %s".format(parentPath, currentBrokerList.mkString(",")))
        if(!isShuttingDown.get()) {
          controllerContext.controllerLock synchronized {
            try {
              val curBrokerIds = currentBrokerList.map(_.toInt).toSet
              val newBrokerIds = curBrokerIds -- controllerContext.liveBrokerIds
              val newBrokers = newBrokerIds.map(ZkUtils.getBrokerInfo(zkClient, _)).filter(_.isDefined).map(_.get)
              val deadBrokerIds = controllerContext.liveBrokerIds -- curBrokerIds
              controllerContext.liveBrokers = curBrokerIds.map(ZkUtils.getBrokerInfo(zkClient, _)).filter(_.isDefined).map(_.get)
              controllerContext.liveBrokerIds = controllerContext.liveBrokers.map(_.id)
              info("Newly added brokers: %s, deleted brokers: %s, all brokers: %s"
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


