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
import kafka.api.LeaderAndIsr
import kafka.utils.{Logging, ZkUtils}
import org.I0Itec.zkclient.IZkChildListener
import collection.JavaConversions._
import kafka.common.{StateChangeFailedException, PartitionOfflineException, KafkaException}
import java.util.concurrent.atomic.AtomicBoolean
import org.I0Itec.zkclient.exception.ZkNodeExistsException

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
class PartitionStateMachine(controller: KafkaController) extends Logging {
  this.logIdent = "[Partition state machine on Controller " + controller.config.brokerId + "]: "
  private val controllerContext = controller.controllerContext
  private val zkClient = controllerContext.zkClient
  var partitionState: mutable.Map[(String, Int), PartitionState] = mutable.Map.empty
  val brokerRequestBatch = new ControllerBrokerRequestBatch(controller.sendRequest)
  private var isShuttingDown = new AtomicBoolean(false)

  /**
   * Invoked on successful controller election. First registers a topic change listener since that triggers all
   * state transitions for partitions. Initializes the state of partitions by reading from zookeeper. Then triggers
   * the OnlinePartition state change for all new or offline partitions.
   */
  def startup() {
    isShuttingDown.set(false)
    // initialize partition state
    initializePartitionState()
    // try to move partitions to online state
    triggerOnlinePartitionStateChange()
    info("Started partition state machine with initial state -> " + partitionState.toString())
  }

  // register topic and partition change listeners
  def registerListeners() {
    registerTopicChangeListener()
  }

  /**
   * Invoked on controller shutdown.
   */
  def shutdown() {
    isShuttingDown.compareAndSet(false, true)
    partitionState.clear()
  }

  /**
   * This API invokes the OnlinePartition state change on all partitions in either the NewPartition or OfflinePartition
   * state. This is called on a successful controller election and on broker changes
   */
  def triggerOnlinePartitionStateChange() {
    try {
      brokerRequestBatch.newBatch()
      // try to move all partitions in NewPartition or OfflinePartition state to OnlinePartition state
      partitionState.filter(partitionAndState =>
        partitionAndState._2.equals(OfflinePartition) || partitionAndState._2.equals(NewPartition)).foreach {
        partitionAndState => handleStateChange(partitionAndState._1._1, partitionAndState._1._2, OnlinePartition)
      }
      brokerRequestBatch.sendRequestsToBrokers()
    }catch {
      case e => error("Error while moving some partitions to the online state", e)
    }
  }

  /**
   * This API is invoked by the partition change zookeeper listener
   * @param partitions   The list of partitions that need to be transitioned to the target state
   * @param targetState  The state that the partitions should be moved to
   */
  def handleStateChanges(partitions: Seq[(String, Int)], targetState: PartitionState) {
    info("Invoking state change to %s for partitions %s".format(targetState, partitions.mkString(",")))
    try {
      brokerRequestBatch.newBatch()
      partitions.foreach { topicAndPartition =>
        handleStateChange(topicAndPartition._1, topicAndPartition._2, targetState)
      }
      brokerRequestBatch.sendRequestsToBrokers()
    }catch {
      case e => error("Error while moving some partitions to %s state".format(targetState), e)
    }
  }

  /**
   * This API exercises the partition's state machine. It ensures that every state transition happens from a legal
   * previous state to the target state.
   * @param topic       The topic of the partition for which the state transition is invoked
   * @param partition   The partition for which the state transition is invoked
   * @param targetState The end state that the partition should be moved to
   */
  private def handleStateChange(topic: String, partition: Int, targetState: PartitionState) {
    try {
      partitionState.getOrElseUpdate((topic, partition), NonExistentPartition)
      targetState match {
        case NewPartition =>
          // pre: partition did not exist before this
          // post: partition has been assigned replicas
          assertValidPreviousStates(topic, partition, List(NonExistentPartition), NewPartition)
          assignReplicasToPartitions(topic, partition)
          partitionState.put((topic, partition), NewPartition)
          info("Partition [%s, %d] state changed from NotExists to New with assigned replicas ".format(topic, partition) +
            "%s".format(controllerContext.partitionReplicaAssignment(topic, partition).mkString(",")))
        case OnlinePartition =>
          // pre: partition should be in New state
          assertValidPreviousStates(topic, partition, List(NewPartition, OfflinePartition), OnlinePartition)
          partitionState(topic, partition) match {
            case NewPartition =>
              // initialize leader and isr path for new partition
              initializeLeaderAndIsrForPartition(topic, partition, brokerRequestBatch)
            case OfflinePartition =>
              electLeaderForOfflinePartition(topic, partition, brokerRequestBatch)
            case _ => // should never come here since illegal previous states are checked above
          }
          info("Partition [%s, %d] state changed from %s to Online with leader %d".format(topic, partition,
            partitionState(topic, partition), controllerContext.allLeaders(topic, partition)))
          partitionState.put((topic, partition), OnlinePartition)
           // post: partition has a leader
        case OfflinePartition =>
          // pre: partition should be in Online state
          assertValidPreviousStates(topic, partition, List(NewPartition, OnlinePartition), OfflinePartition)
          // should be called when the leader for a partition is no longer alive
          info("Partition [%s, %d] state changed from Online to Offline".format(topic, partition))
          partitionState.put((topic, partition), OfflinePartition)
          // post: partition has no alive leader
        case NonExistentPartition =>
          // pre: partition could be in either of the above states
          assertValidPreviousStates(topic, partition, List(OfflinePartition), NonExistentPartition)
          info("Partition [%s, %d] state changed from Offline to NotExists".format(topic, partition))
          partitionState.put((topic, partition), NonExistentPartition)
          // post: partition state is deleted from all brokers and zookeeper
      }
    }catch {
      case e => error("State change for partition [%s, %d] ".format(topic, partition) +
        "from %s to %s failed".format(partitionState(topic, partition), targetState), e)
    }
  }

  /**
   * Invoked on startup of the partition's state machine to set the initial state for all existing partitions in
   * zookeeper
   */
  private def initializePartitionState() {
    for((topicPartition, replicaAssignment) <- controllerContext.partitionReplicaAssignment) {
      val topic = topicPartition._1
      val partition = topicPartition._2
      // check if leader and isr path exists for partition. If not, then it is in NEW state
      ZkUtils.getLeaderAndIsrForPartition(zkClient, topic, partition) match {
        case Some(currentLeaderAndIsr) =>
          // else, check if the leader for partition is alive. If yes, it is in Online state, else it is in Offline state
          controllerContext.liveBrokerIds.contains(currentLeaderAndIsr.leader) match {
            case true => // leader is alive
              partitionState.put(topicPartition, OnlinePartition)
            case false =>
              partitionState.put(topicPartition, OfflinePartition)
          }
        case None =>
          partitionState.put(topicPartition, NewPartition)
      }
    }
  }

  private def assertValidPreviousStates(topic: String, partition: Int, fromStates: Seq[PartitionState],
                                        targetState: PartitionState) {
    if(!fromStates.contains(partitionState((topic, partition))))
      throw new IllegalStateException("Partition [%s, %d] should be in the %s states before moving to %s state"
        .format(topic, partition, fromStates.mkString(","), targetState) + ". Instead it is in %s state"
        .format(partitionState((topic, partition))))
  }

  /**
   * Invoked on the NonExistentPartition->NewPartition state transition to update the controller's cache with the
   * partition's replica assignment.
   * @topic     The topic of the partition whose replica assignment is to be cached
   * @partition The partition whose replica assignment is to be cached
   */
  private def assignReplicasToPartitions(topic: String, partition: Int) {
    val assignedReplicas = ZkUtils.getReplicasForPartition(controllerContext.zkClient, topic, partition)
    controllerContext.partitionReplicaAssignment += (topic, partition) -> assignedReplicas
  }

  /**
   * Invoked on the NewPartition->OnlinePartition state change. When a partition is in the New state, it does not have
   * a leader and isr path in zookeeper. Once the partition moves to the OnlinePartition state, it's leader and isr
   * path gets initialized and it never goes back to the NewPartition state. From here, it can only go to the
   * OfflinePartition state.
   * @topic               The topic of the partition whose leader and isr path is to be initialized
   * @partition           The partition whose leader and isr path is to be initialized
   * @brokerRequestBatch  The object that holds the leader and isr requests to be sent to each broker as a result of
   *                      this state change
   */
  private def initializeLeaderAndIsrForPartition(topic: String, partition: Int,
                                                 brokerRequestBatch: ControllerBrokerRequestBatch) {
    debug("Initializing leader and isr for partition [%s, %d]".format(topic, partition))
    val replicaAssignment = controllerContext.partitionReplicaAssignment((topic, partition))
    val liveAssignedReplicas = replicaAssignment.filter(r => controllerContext.liveBrokerIds.contains(r))
    liveAssignedReplicas.size match {
      case 0 =>
        ControllerStat.offlinePartitionRate.mark()
        throw new StateChangeFailedException(("During state change of partition (%s, %d) from NEW to ONLINE, assigned replicas are " +
          "[%s], live brokers are [%s]. No assigned replica is alive").format(topic, partition,
          replicaAssignment.mkString(","), controllerContext.liveBrokerIds))
      case _ =>
        debug("Live assigned replicas for partition [%s, %d] are: [%s]".format(topic, partition, liveAssignedReplicas))
        // make the first replica in the list of assigned replicas, the leader
        val leader = liveAssignedReplicas.head
        val leaderAndIsr = new LeaderAndIsr(leader, liveAssignedReplicas.toList)
        try {
          ZkUtils.createPersistentPath(controllerContext.zkClient,
            ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partition), leaderAndIsr.toString())
          // TODO: the above write can fail only if the current controller lost its zk session and the new controller
          // took over and initialized this partition. This can happen if the current controller went into a long
          // GC pause
          brokerRequestBatch.addRequestForBrokers(liveAssignedReplicas, topic, partition, leaderAndIsr, replicaAssignment.size)
          controllerContext.allLeaders.put((topic, partition), leaderAndIsr.leader)
          partitionState.put((topic, partition), OnlinePartition)
        }catch {
          case e: ZkNodeExistsException =>
            ControllerStat.offlinePartitionRate.mark()
            throw new StateChangeFailedException("Error while changing partition [%s, %d]'s state from New to Online"
              .format(topic, partition) + " since Leader and ISR path already exists")
        }
    }
  }

  /**
   * Invoked on the OfflinePartition->OnlinePartition state change. It invokes the leader election API to elect a leader
   * for the input offline partition
   * @topic               The topic of the offline partition
   * @partition           The offline partition
   * @brokerRequestBatch  The object that holds the leader and isr requests to be sent to each broker as a result of
   *                      this state change
   */
  private def electLeaderForOfflinePartition(topic: String, partition: Int,
                                             brokerRequestBatch: ControllerBrokerRequestBatch) {
    /** handle leader election for the partitions whose leader is no longer alive **/
    info("Electing leader for Offline partition [%s, %d]".format(topic, partition))
    try {
      controllerContext.partitionReplicaAssignment.get((topic, partition)) match {
        case Some(assignedReplicas) =>
          val liveAssignedReplicasToThisPartition = assignedReplicas.filter(r => controllerContext.liveBrokerIds.contains(r))
          try {
            // elect new leader or throw exception
            val newLeaderAndIsr = electLeaderForPartition(topic, partition, assignedReplicas)
            info("Elected leader %d for Offline partition [%s, %d]".format(newLeaderAndIsr.leader, topic, partition))
            // store new leader and isr info in cache
            brokerRequestBatch.addRequestForBrokers(liveAssignedReplicasToThisPartition, topic, partition,
              newLeaderAndIsr, assignedReplicas.size)
          }catch {
            case e => throw new StateChangeFailedException(("Error while electing leader for partition" +
              " [%s, %d]").format(topic, partition), e)
          }
        case None => throw new KafkaException(("While handling broker changes, the " +
          "partition [%s, %d] doesn't have assigned replicas. The replica assignment for other partitions is %s")
          .format(topic, partition, controllerContext.partitionReplicaAssignment))
      }
    }catch {
      case e => throw new PartitionOfflineException("All replicas for partition [%s, %d] are dead."
        .format(topic, partition) + " Marking this partition offline")
    }
    debug("After leader election, leader cache is updated to %s".format(controllerContext.allLeaders.map(l => (l._1, l._2))))
  }

  /**
   * @param topic                      The topic of the partition whose leader needs to be elected
   * @param partition                  The partition whose leader needs to be elected
   * @param assignedReplicas           The list of replicas assigned to the input partition
   * @throws PartitionOfflineException If no replica in the assigned replicas list is alive
   * This API selects a new leader for the input partition -
   * 1. If at least one broker from the isr is alive, it picks a broker from the isr as the new leader
   * 2. Else, it picks some alive broker from the assigned replica list as the new leader
   * 3. If no broker in the assigned replica list is alive, it throws PartitionOfflineException
   * Once the leader is successfully registered in zookeeper, it updates the allLeaders cache
   */
  private def electLeaderForPartition(topic: String, partition: Int, assignedReplicas: Seq[Int]):LeaderAndIsr = {
    var zookeeperPathUpdateSucceeded: Boolean = false
    var newLeaderAndIsr: LeaderAndIsr = null
    while(!zookeeperPathUpdateSucceeded) {
      newLeaderAndIsr = ZkUtils.getLeaderAndIsrForPartition(zkClient, topic, partition) match {
        case Some(currentLeaderAndIsr) =>
          var newLeaderAndIsr: LeaderAndIsr = currentLeaderAndIsr
          val liveAssignedReplicasToThisPartition = assignedReplicas.filter(r => controllerContext.liveBrokerIds.contains(r))
          val liveBrokersInIsr = currentLeaderAndIsr.isr.filter(r => controllerContext.liveBrokerIds.contains(r))
          val currentLeaderEpoch = currentLeaderAndIsr.leaderEpoch
          val currentLeaderIsrZkPathVersion = currentLeaderAndIsr.zkVersion
          debug("Leader, epoch, ISR and zkPathVersion for partition (%s, %d) are: [%d], [%d], [%s], [%d]"
            .format(topic, partition, currentLeaderAndIsr.leader, currentLeaderEpoch, currentLeaderAndIsr.isr,
            currentLeaderIsrZkPathVersion))
          newLeaderAndIsr = liveBrokersInIsr.isEmpty match {
            case true =>
              debug("No broker is ISR is alive, picking the leader from the alive assigned replicas: %s"
                .format(liveAssignedReplicasToThisPartition.mkString(",")))
              liveAssignedReplicasToThisPartition.isEmpty match {
                case true =>
                  ControllerStat.offlinePartitionRate.mark()
                  throw new PartitionOfflineException(("No replica for partition " +
                  "([%s, %d]) is alive. Live brokers are: [%s],".format(topic, partition, controllerContext.liveBrokerIds)) +
                  " Assigned replicas are: [%s]".format(assignedReplicas))
                case false =>
                  ControllerStat.uncleanLeaderElectionRate.mark()
                  val newLeader = liveAssignedReplicasToThisPartition.head
                  warn("No broker in ISR is alive, elected leader from the alive replicas is [%s], ".format(newLeader) +
                    "There's potential data loss")
                  new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, List(newLeader), currentLeaderIsrZkPathVersion + 1)
              }
            case false =>
              val newLeader = liveBrokersInIsr.head
              debug("Some broker in ISR is alive, selecting the leader from the ISR: " + newLeader)
              new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, liveBrokersInIsr.toList, currentLeaderIsrZkPathVersion + 1)
          }
          info("New leader and ISR for partition [%s, %d] is %s".format(topic, partition, newLeaderAndIsr.toString()))
          // update the new leadership decision in zookeeper or retry
          val (updateSucceeded, newVersion) = ZkUtils.conditionalUpdatePersistentPath(zkClient,
            ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partition),
            newLeaderAndIsr.toString(), currentLeaderAndIsr.zkVersion)
          newLeaderAndIsr.zkVersion = newVersion
          zookeeperPathUpdateSucceeded = updateSucceeded
          newLeaderAndIsr
        case None =>
          throw new StateChangeFailedException("On broker changes, " +
            "there's no leaderAndISR information for partition (%s, %d) in zookeeper".format(topic, partition))
      }
    }
    // update the leader cache
    controllerContext.allLeaders.put((topic, partition), newLeaderAndIsr.leader)
    newLeaderAndIsr
  }

  private def registerTopicChangeListener() = {
    zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath, new TopicChangeListener())
  }

  def registerPartitionChangeListener(topic: String) = {
    zkClient.subscribeChildChanges(ZkUtils.getTopicPath(topic), new PartitionChangeListener(topic))
  }

  /**
   * This is the zookeeper listener that triggers all the state transitions for a partition
   */
  class TopicChangeListener extends IZkChildListener with Logging {
    this.logIdent = "[TopicChangeListener on Controller " + controller.config.brokerId + "]: "

    @throws(classOf[Exception])
    def handleChildChange(parentPath : String, children : java.util.List[String]) {
      if(!isShuttingDown.get()) {
        controllerContext.controllerLock synchronized {
          try {
            debug("Topic change listener fired for path %s with children %s".format(parentPath, children.mkString(",")))
            val currentChildren = JavaConversions.asBuffer(children).toSet
            val newTopics = currentChildren -- controllerContext.allTopics
            val deletedTopics = controllerContext.allTopics -- currentChildren
            //        val deletedPartitionReplicaAssignment = replicaAssignment.filter(p => deletedTopics.contains(p._1._1))
            controllerContext.allTopics = currentChildren

            val addedPartitionReplicaAssignment = ZkUtils.getReplicaAssignmentForTopics(zkClient, newTopics.toSeq)
            controllerContext.partitionReplicaAssignment = controllerContext.partitionReplicaAssignment.filter(p => !deletedTopics.contains(p._1._1))
            controllerContext.partitionReplicaAssignment.++=(addedPartitionReplicaAssignment)
            info("New topics: [%s], deleted topics: [%s], new partition replica assignment [%s]".format(newTopics,
              deletedTopics, addedPartitionReplicaAssignment))
            if(newTopics.size > 0)
              controller.onNewTopicCreation(newTopics, addedPartitionReplicaAssignment.keySet.toSeq)
          } catch {
            case e => error("Error while handling new topic", e )
          }
          // TODO: kafka-330  Handle deleted topics
          // handleDeletedTopics(deletedTopics, deletedPartitionReplicaAssignment)
        }
      }
    }
  }

  class PartitionChangeListener(topic: String) extends IZkChildListener with Logging {
    this.logIdent = "[Controller " + controller.config.brokerId + "], "

    @throws(classOf[Exception])
    def handleChildChange(parentPath : String, children : java.util.List[String]) {
      controllerContext.controllerLock synchronized {
        // TODO: To be completed as part of KAFKA-41
      }
    }
  }
}

sealed trait PartitionState { def state: Byte }
case object NewPartition extends PartitionState { val state: Byte = 0 }
case object OnlinePartition extends PartitionState { val state: Byte = 1 }
case object OfflinePartition extends PartitionState { val state: Byte = 2 }
case object NonExistentPartition extends PartitionState { val state: Byte = 3 }


