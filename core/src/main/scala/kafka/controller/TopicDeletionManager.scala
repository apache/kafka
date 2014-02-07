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

import collection.mutable
import kafka.utils.{ShutdownableThread, Logging, ZkUtils}
import kafka.utils.Utils._
import collection.Set
import kafka.common.{ErrorMapping, TopicAndPartition}
import kafka.api.{StopReplicaResponse, RequestOrResponse}

/**
 * This manages the state machine for topic deletion.
 * 1. TopicCommand issues topic deletion by creating a new admin path /admin/delete_topics/<topic>
 * 2. The controller listens for child changes on /admin/delete_topic and starts topic deletion for the respective topics
 * 3. The controller has a background thread that handles topic deletion. The purpose of having this background thread
 *    is to accommodate the TTL feature, when we have it. This thread is signaled whenever deletion for a topic needs to
 *    be started or resumed. Currently, a topic's deletion can be started only by the onPartitionDeletion callback on the
 *    controller. In the future, it can be triggered based on the configured TTL for the topic. A topic's deletion will
 *    be halted in the following scenarios -
 *    3.1 broker hosting one of the replicas for that topic goes down
 *    3.2 partition reassignment for partitions of that topic is in progress
 *    3.3 preferred replica election for partitions of that topic is in progress
 *    (though this is not strictly required since it holds the controller lock for the entire duration from start to end)
 * 4. Topic deletion is resumed when -
 *    4.1 broker hosting one of the replicas for that topic is started
 *    4.2 preferred replica election for partitions of that topic completes
 *    4.3 partition reassignment for partitions of that topic completes
 * 5. Every replica for a topic being deleted is in either of the 3 states -
 *    5.1 TopicDeletionStarted (Replica enters TopicDeletionStarted phase when the onPartitionDeletion callback is invoked.
 *        This happens when the child change watch for /admin/delete_topics fires on the controller. As part of this state
 *        change, the controller sends StopReplicaRequests to all replicas. It registers a callback for the
 *        StopReplicaResponse when deletePartition=true thereby invoking a callback when a response for delete replica
 *        is received from every replica)
 *    5.2 TopicDeletionSuccessful (deleteTopicStopReplicaCallback() moves replicas from
 *        TopicDeletionStarted->TopicDeletionSuccessful depending on the error codes in StopReplicaResponse)
 *    5.3 TopicDeletionFailed. (deleteTopicStopReplicaCallback() moves replicas from
 *        TopicDeletionStarted->TopicDeletionFailed depending on the error codes in StopReplicaResponse.
 *        In general, if a broker dies and if it hosted replicas for topics being deleted, the controller marks the
 *        respective replicas in TopicDeletionFailed state in the onBrokerFailure callback. The reason is that if a
 *        broker fails before the request is sent and after the replica is in TopicDeletionStarted state,
 *        it is possible that the replica will mistakenly remain in TopicDeletionStarted state and topic deletion
 *        will not be retried when the broker comes back up.)
 * 6. The delete topic thread marks a topic successfully deleted only if all replicas are in TopicDeletionSuccessful
 *    state and it starts the topic deletion teardown mode where it deletes all topic state from the controllerContext
 *    as well as from zookeeper. This is the only time the /brokers/topics/<topic> path gets deleted. On the other hand,
 *    if no replica is in TopicDeletionStarted state and at least one replica is in TopicDeletionFailed state, then
 *    it marks the topic for deletion retry.
 * @param controller
 * @param initialTopicsToBeDeleted The topics that are queued up for deletion in zookeeper at the time of controller failover
 * @param initialHaltedTopicsForDeletion The topics for which deletion is halted due to any of the conditions mentioned in #3 above
 */
class TopicDeletionManager(controller: KafkaController,
                           initialTopicsToBeDeleted: Set[String] = Set.empty,
                           initialHaltedTopicsForDeletion: Set[String] = Set.empty) extends Logging {
  val controllerContext = controller.controllerContext
  val partitionStateMachine = controller.partitionStateMachine
  val replicaStateMachine = controller.replicaStateMachine
  var topicsToBeDeleted: mutable.Set[String] = mutable.Set.empty[String] ++ initialTopicsToBeDeleted
  var haltedTopicsForDeletion: mutable.Set[String] = mutable.Set.empty[String] ++
    (initialHaltedTopicsForDeletion & initialTopicsToBeDeleted)
  val deleteTopicsCond = controllerContext.controllerLock.newCondition()
  var deleteTopicStateChanged: Boolean = false
  var deleteTopicsThread: DeleteTopicsThread = null

  /**
   * Invoked at the end of new controller initiation
   */
  def start() {
    deleteTopicsThread = new DeleteTopicsThread()
    deleteTopicStateChanged = true
    deleteTopicsThread.start()
  }

  /**
   * Invoked when the current controller resigns. At this time, all state for topic deletion should be cleared
   */
  def shutdown() {
    deleteTopicsThread.shutdown()
    topicsToBeDeleted.clear()
    haltedTopicsForDeletion.clear()
  }

  /**
   * Invoked by the child change listener on /admin/delete_topics to queue up the topics for deletion. The topic gets added
   * to the topicsToBeDeleted list and only gets removed from the list when the topic deletion has completed successfully
   * i.e. all replicas of all partitions of that topic are deleted successfully.
   * @param topics Topics that should be deleted
   */
  def enqueueTopicsForDeletion(topics: Set[String]) {
    topicsToBeDeleted ++= topics
    resumeTopicDeletionThread()
  }

  /**
   * Invoked when any event that can possibly resume topic deletion occurs. These events include -
   * 1. New broker starts up. Any replicas belonging to topics queued up for deletion can be deleted since the broker is up
   * 2. Partition reassignment completes. Any partitions belonging to topics queued up for deletion finished reassignment
   * 3. Preferred replica election completes. Any partitions belonging to topics queued up for deletion finished
   *    preferred replica election
   * @param topics Topics for which deletion can be resumed
   */
  def resumeDeletionForTopics(topics: Set[String] = Set.empty) {
    val topicsToResumeDeletion = topics & topicsToBeDeleted
    if(topicsToResumeDeletion.size > 0) {
      haltedTopicsForDeletion --= topicsToResumeDeletion
      resumeTopicDeletionThread()
    }
  }

  /**
   * Invoked when a broker that hosts replicas for topics to be deleted goes down. Also invoked when the callback for
   * StopReplicaResponse receives an error code for the replicas of a topic to be deleted. As part of this, the replicas
   * are moved from ReplicaDeletionStarted to ReplicaDeletionFailed state. Also, the topic is added to the list of topics
   * for which deletion is halted until further notice. The delete topic thread is notified so it can retry topic deletion
   * if it has received a response for all replicas of a topic to be deleted
   * @param replicas Replicas for which deletion has failed
   */
  def failReplicaDeletion(replicas: Set[PartitionAndReplica]) {
    val replicasThatFailedToDelete = replicas.filter(r => isTopicQueuedUpForDeletion(r.topic))
    if(replicasThatFailedToDelete.size > 0) {
      val topics = replicasThatFailedToDelete.map(_.topic)
      debug("Deletion failed for replicas %s. Halting deletion for topics %s"
        .format(replicasThatFailedToDelete.mkString(","), topics))
      controller.replicaStateMachine.handleStateChanges(replicasThatFailedToDelete, ReplicaDeletionFailed)
      haltTopicDeletion(topics)
      resumeTopicDeletionThread()
    }
  }

  /**
   * Halt delete topic if -
   * 1. replicas being down
   * 2. partition reassignment in progress for some partitions of the topic
   * 3. preferred replica election in progress for some partitions of the topic
   * @param topics Topics for which deletion should be halted. No op if the topic is was not previously queued up for deletion
   */
  def haltTopicDeletion(topics: Set[String]) {
    val newTopicsToHaltDeletion = topicsToBeDeleted & topics
    haltedTopicsForDeletion ++= newTopicsToHaltDeletion
    if(newTopicsToHaltDeletion.size > 0)
      info("Halted deletion of topics %s".format(newTopicsToHaltDeletion.mkString(",")))
  }

  def isTopicDeletionHalted(topic: String): Boolean = {
    haltedTopicsForDeletion.contains(topic)
  }

  def isTopicDeletionInProgress(topic: String): Boolean = {
    controller.replicaStateMachine.isAtLeastOneReplicaInDeletionStartedState(topic)
  }

  def isTopicQueuedUpForDeletion(topic: String): Boolean = {
    topicsToBeDeleted.contains(topic)
  }

  /**
   * Invoked by the delete-topic-thread to wait until events that either trigger, restart or halt topic deletion occur.
   * controllerLock should be acquired before invoking this API
   */
  private def awaitTopicDeletionNotification() {
    while(!deleteTopicStateChanged) {
      info("Waiting for signal to start or continue topic deletion")
      deleteTopicsCond.await()
    }
    deleteTopicStateChanged = false
  }

  /**
   * Signals the delete-topic-thread to process topic deletion
   */
  private def resumeTopicDeletionThread() {
    deleteTopicStateChanged = true
    deleteTopicsCond.signal()
  }

  /**
   * Invoked by the StopReplicaResponse callback when it receives no error code for a replica of a topic to be deleted.
   * As part of this, the replicas are moved from ReplicaDeletionStarted to ReplicaDeletionSuccessful state. The delete
   * topic thread is notified so it can tear down the topic if all replicas of a topic have been successfully deleted
   * @param replicas Replicas that were successfully deleted by the broker
   */
  private def completeReplicaDeletion(replicas: Set[PartitionAndReplica]) {
    val successfullyDeletedReplicas = replicas.filter(r => isTopicQueuedUpForDeletion(r.topic))
    debug("Deletion successfully completed for replicas %s".format(successfullyDeletedReplicas.mkString(",")))
    controller.replicaStateMachine.handleStateChanges(successfullyDeletedReplicas, ReplicaDeletionSuccessful)
    resumeTopicDeletionThread()
  }

  /**
   * Topic deletion can be retried if -
   * 1. Topic deletion is not already complete
   * 2. Topic deletion is currently not in progress for that topic
   * 3. Topic deletion is currently halted for that topic
   * @param topic Topic
   * @return Whether or not deletion can be retried for the topic
   */
  private def isTopicEligibleForDeletion(topic: String): Boolean = {
    topicsToBeDeleted.contains(topic) && (!isTopicDeletionInProgress(topic) && !isTopicDeletionHalted(topic))
  }

  /**
   * If the topic is queued for deletion but deletion is not currently under progress, then deletion is retried for that topic
   * To ensure a successful retry, reset states for respective replicas from ReplicaDeletionFailed to OfflineReplica state
   *@param topic Topic for which deletion should be retried
   */
  private def markTopicForDeletionRetry(topic: String) {
    // reset replica states from ReplicaDeletionFailed to OfflineReplica
    val failedReplicas = controller.replicaStateMachine.replicasInState(topic, ReplicaDeletionFailed)
    controller.replicaStateMachine.handleStateChanges(failedReplicas, OfflineReplica)
  }

  private def completeDeleteTopic(topic: String) {
    val replicasForDeletedTopic = controller.replicaStateMachine.replicasInState(topic, ReplicaDeletionSuccessful)
    // controller will remove this replica from the state machine as well as its partition assignment cache
    replicaStateMachine.handleStateChanges(replicasForDeletedTopic, NonExistentReplica)
    val partitionsForDeletedTopic = controllerContext.partitionsForTopic(topic)
    // move respective partition to OfflinePartition and NonExistentPartition state
    partitionStateMachine.handleStateChanges(partitionsForDeletedTopic, OfflinePartition)
    partitionStateMachine.handleStateChanges(partitionsForDeletedTopic, NonExistentPartition)
    topicsToBeDeleted -= topic
    controllerContext.zkClient.deleteRecursive(ZkUtils.getTopicPath(topic))
    controllerContext.zkClient.deleteRecursive(ZkUtils.getTopicConfigPath(topic))
    controllerContext.zkClient.delete(ZkUtils.getDeleteTopicPath(topic))
    controllerContext.removeTopic(topic)
  }

  /**
   * This callback is invoked by the DeleteTopics thread with the list of topics to be deleted
   * It invokes the delete partition callback for all partitions of a topic
   */
  private def onTopicDeletion(topics: Set[String]) {
    info("Topic deletion callback for %s".format(topics.mkString(",")))
    val partitionReplicaAssignmentByTopic = controllerContext.partitionReplicaAssignment.groupBy(p => p._1.topic)
    topics.foreach { topic =>
      onPartitionDeletion(partitionReplicaAssignmentByTopic(topic).map(_._1).toSet)
    }
  }

  /**
   * Invoked by the onPartitionDeletion callback. It is the 2nd step of topic deletion, the first being sending
   * UpdateMetadata requests to all brokers to start rejecting requests for deleted topics. As part of starting deletion,
   * the topics are added to the in progress list. As long as a topic is in the in progress list, deletion for that topic
   * is never retried. A topic is removed from the in progress list when
   * 1. Either the topic is successfully deleted OR
   * 2. No replica for the topic is in ReplicaDeletionStarted state and at least one replica is in ReplicaDeletionFailed state
   * If the topic is queued for deletion but deletion is not currently under progress, then deletion is retried for that topic
   * As part of starting deletion, all replicas are moved to the ReplicaDeletionStarted state where the controller sends
   * the replicas a StopReplicaRequest (delete=true)
   * This callback does the following things -
   * 1. Send metadata request to all brokers excluding the topics to be deleted
   * 2. Move all dead replicas directly to ReplicaDeletionFailed state. Also halt the deletion of respective topics if
   *    some replicas are dead since it won't complete successfully anyway
   * 3. Move all alive replicas to ReplicaDeletionStarted state so they can be deleted successfully
   *@param replicasForTopicsToBeDeleted
   */
  private def startReplicaDeletion(replicasForTopicsToBeDeleted: Set[PartitionAndReplica]) {
    replicasForTopicsToBeDeleted.groupBy(_.topic).foreach { case(topic, replicas) =>
      // send update metadata so that brokers stop serving data
      controller.sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq)
      var aliveReplicasForTopic = controllerContext.allLiveReplicas().filter(p => p.topic.equals(topic))
      val deadReplicasForTopic = replicasForTopicsToBeDeleted -- aliveReplicasForTopic
      val successfullyDeletedReplicas = controller.replicaStateMachine.replicasInState(topic, ReplicaDeletionSuccessful)
      val replicasForDeletionRetry = aliveReplicasForTopic -- successfullyDeletedReplicas
      // move dead replicas directly to failed state
      replicaStateMachine.handleStateChanges(deadReplicasForTopic, ReplicaDeletionFailed)
      // send stop replica to all followers that are not in the OfflineReplica state so they stop sending fetch requests to the leader
      replicaStateMachine.handleStateChanges(replicasForDeletionRetry, OfflineReplica)
      debug("Deletion started for replicas %s".format(replicasForDeletionRetry.mkString(",")))
      controller.replicaStateMachine.handleStateChanges(replicasForDeletionRetry, ReplicaDeletionStarted,
        new Callbacks.CallbackBuilder().stopReplicaCallback(deleteTopicStopReplicaCallback).build)
      if(deadReplicasForTopic.size > 0)
        haltTopicDeletion(Set(topic))
    }
  }

  /**
   * This callback is invoked by the delete topic callback with the list of partitions for topics to be deleted
   * It does the following -
   * 1. Send UpdateMetadataRequest to all live brokers (that are not shutting down) with all partitions except those for
   *    which the topics are being deleted. The brokers start rejecting all client requests with UnknownTopicOrPartitionException
   * 2. Move all replicas for the partitions to OfflineReplica state. This will send StopReplicaRequest to the replicas
   *    and LeaderAndIsrRequest to the leader with the shrunk ISR. When the leader replica itself is moved to OfflineReplica state,
   *    it will skip sending the LeaderAndIsrRequest since the leader will be updated to -1
   * 3. Move all replicas to ReplicaDeletionStarted state. This will send StopReplicaRequest with deletePartition=true. And
   *    will delete all persistent data from all replicas of the respective partitions
   */
  private def onPartitionDeletion(partitionsToBeDeleted: Set[TopicAndPartition]) {
    info("Partition deletion callback for %s".format(partitionsToBeDeleted.mkString(",")))
    val replicasPerPartition = controllerContext.replicasForPartition(partitionsToBeDeleted)
    startReplicaDeletion(replicasPerPartition)
  }

  private def deleteTopicStopReplicaCallback(stopReplicaResponseObj: RequestOrResponse, replicaId: Int) {
    val stopReplicaResponse = stopReplicaResponseObj.asInstanceOf[StopReplicaResponse]
    debug("Delete topic callback invoked for %s".format(stopReplicaResponse))
    val partitionsInError = if(stopReplicaResponse.errorCode != ErrorMapping.NoError) {
      stopReplicaResponse.responseMap.keySet
    } else
      stopReplicaResponse.responseMap.filter(p => p._2 != ErrorMapping.NoError).map(_._1).toSet
    val replicasInError = partitionsInError.map(p => PartitionAndReplica(p.topic, p.partition, replicaId))
    inLock(controllerContext.controllerLock) {
      // move all the failed replicas to ReplicaDeletionFailed
      failReplicaDeletion(replicasInError)
      if(replicasInError.size != stopReplicaResponse.responseMap.size) {
        // some replicas could have been successfully deleted
        val deletedReplicas = stopReplicaResponse.responseMap.keySet -- partitionsInError
        completeReplicaDeletion(deletedReplicas.map(p => PartitionAndReplica(p.topic, p.partition, replicaId)))
      }
    }
  }

  class DeleteTopicsThread() extends ShutdownableThread("delete-topics-thread") {
    val zkClient = controllerContext.zkClient
    override def doWork() {
      inLock(controllerContext.controllerLock) {
        awaitTopicDeletionNotification()
        val topicsQueuedForDeletion = Set.empty[String] ++ topicsToBeDeleted
        if(topicsQueuedForDeletion.size > 0)
          info("Handling deletion for topics " + topicsQueuedForDeletion.mkString(","))
        topicsQueuedForDeletion.foreach { topic =>
          // if all replicas are marked as deleted successfully, then topic deletion is done
          if(controller.replicaStateMachine.areAllReplicasForTopicDeleted(topic)) {
            // clear up all state for this topic from controller cache and zookeeper
            completeDeleteTopic(topic)
            info("Deletion of topic %s successfully completed".format(topic))
          } else {
            if(controller.replicaStateMachine.isAtLeastOneReplicaInDeletionStartedState(topic)) {
              // ignore since topic deletion is in progress
              val replicasInDeletionStartedState = controller.replicaStateMachine.replicasInState(topic, ReplicaDeletionStarted)
              val replicaIds = replicasInDeletionStartedState.map(_.replica)
              val partitions = replicasInDeletionStartedState.map(r => TopicAndPartition(r.topic, r.partition))
              info("Deletion for replicas %s for partition %s of topic %s in progress".format(replicaIds.mkString(","),
                partitions.mkString(","), topic))
            } else {
              // if you come here, then no replica is in TopicDeletionStarted and all replicas are not in
              // TopicDeletionSuccessful. That means, there is at least one failed replica, which means topic deletion
              // should be retried
              val replicasInTopicDeletionFailedState = controller.replicaStateMachine.replicasInState(topic, ReplicaDeletionFailed)
              // mark topic for deletion retry
              markTopicForDeletionRetry(topic)
              info("Retrying delete topic for topic %s since replicas %s were not successfully deleted"
                .format(topic, replicasInTopicDeletionFailedState.mkString(",")))
            }
          }
          // Try delete topic if it is eligible for deletion.
          if(isTopicEligibleForDeletion(topic)) {
            info("Deletion of topic %s (re)started".format(topic))
            // topic deletion will be kicked off
            onTopicDeletion(Set(topic))
          } else if(isTopicDeletionHalted(topic)) {
            info("Not retrying deletion of topic %s at this time since it is halted".format(topic))
          }
        }
      }
    }
  }
}

