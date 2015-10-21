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

import kafka.common.{OffsetMetadataAndError, OffsetAndMetadata, TopicAndPartition}
import kafka.message.UncompressedCodec
import kafka.log.LogConfig
import kafka.server._
import kafka.utils._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.JoinGroupRequest

import org.I0Itec.zkclient.ZkClient
import java.util.concurrent.atomic.AtomicBoolean
import java.util.Properties
import scala.collection.{Map, Seq, immutable}

case class GroupManagerConfig(consumerMinSessionTimeoutMs: Int,
                              consumerMaxSessionTimeoutMs: Int)

/**
 * ConsumerCoordinator handles consumer group and consumer offset management.
 *
 * Each Kafka server instantiates a coordinator which is responsible for a set of
 * consumer groups. Consumer groups are assigned to coordinators based on their
 * group names.
 */
class ConsumerCoordinator(val brokerId: Int,
                          val groupConfig: GroupManagerConfig,
                          val offsetConfig: OffsetManagerConfig,
                          private val offsetManager: OffsetManager,
                          zkUtils: ZkUtils) extends Logging {

  this.logIdent = "[ConsumerCoordinator " + brokerId + "]: "

  private val isActive = new AtomicBoolean(false)

  private var heartbeatPurgatory: DelayedOperationPurgatory[DelayedHeartbeat] = null
  private var rebalancePurgatory: DelayedOperationPurgatory[DelayedRebalance] = null
  private var coordinatorMetadata: CoordinatorMetadata = null

  def this(brokerId: Int,
           groupConfig: GroupManagerConfig,
           offsetConfig: OffsetManagerConfig,
           replicaManager: ReplicaManager,
           zkUtils: ZkUtils,
           scheduler: KafkaScheduler) = this(brokerId, groupConfig, offsetConfig,
    new OffsetManager(offsetConfig, replicaManager, zkUtils, scheduler), zkUtils)

  def offsetsTopicConfigs: Properties = {
    val props = new Properties
    props.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)
    props.put(LogConfig.SegmentBytesProp, offsetConfig.offsetsTopicSegmentBytes.toString)
    props.put(LogConfig.CompressionTypeProp, UncompressedCodec.name)
    props
  }

  /**
   * NOTE: If a group lock and metadataLock are simultaneously needed,
   * be sure to acquire the group lock before metadataLock to prevent deadlock
   */

  /**
   * Startup logic executed at the same time when the server starts up.
   */
  def startup() {
    info("Starting up.")
    heartbeatPurgatory = new DelayedOperationPurgatory[DelayedHeartbeat]("Heartbeat", brokerId)
    rebalancePurgatory = new DelayedOperationPurgatory[DelayedRebalance]("Rebalance", brokerId)
    coordinatorMetadata = new CoordinatorMetadata(brokerId, zkUtils, maybePrepareRebalance)
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
    offsetManager.shutdown()
    coordinatorMetadata.shutdown()
    heartbeatPurgatory.shutdown()
    rebalancePurgatory.shutdown()
    info("Shutdown complete.")
  }

  def handleJoinGroup(groupId: String,
                      consumerId: String,
                      topics: Set[String],
                      sessionTimeoutMs: Int,
                      partitionAssignmentStrategy: String,
                      responseCallback:(Set[TopicAndPartition], String, Int, Short) => Unit) {
    if (!isActive.get) {
      responseCallback(Set.empty, consumerId, 0, Errors.CONSUMER_COORDINATOR_NOT_AVAILABLE.code)
    } else if (!isCoordinatorForGroup(groupId)) {
      responseCallback(Set.empty, consumerId, 0, Errors.NOT_COORDINATOR_FOR_CONSUMER.code)
    } else if (!PartitionAssignor.strategies.contains(partitionAssignmentStrategy)) {
      responseCallback(Set.empty, consumerId, 0, Errors.UNKNOWN_PARTITION_ASSIGNMENT_STRATEGY.code)
    } else if (sessionTimeoutMs < groupConfig.consumerMinSessionTimeoutMs ||
               sessionTimeoutMs > groupConfig.consumerMaxSessionTimeoutMs) {
      responseCallback(Set.empty, consumerId, 0, Errors.INVALID_SESSION_TIMEOUT.code)
    } else {
      // only try to create the group if the group is not unknown AND
      // the consumer id is UNKNOWN, if consumer is specified but group does not
      // exist we should reject the request
      var group = coordinatorMetadata.getGroup(groupId)
      if (group == null) {
        if (consumerId != JoinGroupRequest.UNKNOWN_CONSUMER_ID) {
          responseCallback(Set.empty, consumerId, 0, Errors.UNKNOWN_CONSUMER_ID.code)
        } else {
          group = coordinatorMetadata.addGroup(groupId, partitionAssignmentStrategy)
          doJoinGroup(group, consumerId, topics, sessionTimeoutMs, partitionAssignmentStrategy, responseCallback)
        }
      } else {
        doJoinGroup(group, consumerId, topics, sessionTimeoutMs, partitionAssignmentStrategy, responseCallback)
      }
    }
  }

  private def doJoinGroup(group: ConsumerGroupMetadata,
                          consumerId: String,
                          topics: Set[String],
                          sessionTimeoutMs: Int,
                          partitionAssignmentStrategy: String,
                          responseCallback:(Set[TopicAndPartition], String, Int, Short) => Unit) {
    group synchronized {
      if (group.is(Dead)) {
        // if the group is marked as dead, it means some other thread has just removed the group
        // from the coordinator metadata; this is likely that the group has migrated to some other
        // coordinator OR the group is in a transient unstable phase. Let the consumer to retry
        // joining without specified consumer id,
        responseCallback(Set.empty, consumerId, 0, Errors.UNKNOWN_CONSUMER_ID.code)
      } else if (partitionAssignmentStrategy != group.partitionAssignmentStrategy) {
        responseCallback(Set.empty, consumerId, 0, Errors.INCONSISTENT_PARTITION_ASSIGNMENT_STRATEGY.code)
      } else if (consumerId != JoinGroupRequest.UNKNOWN_CONSUMER_ID && !group.has(consumerId)) {
        // if the consumer trying to register with a un-recognized id, send the response to let
        // it reset its consumer id and retry
        responseCallback(Set.empty, consumerId, 0, Errors.UNKNOWN_CONSUMER_ID.code)
      } else if (group.has(consumerId) && group.is(Stable) && topics == group.get(consumerId).topics) {
        /*
         * if an existing consumer sends a JoinGroupRequest with no changes while the group is stable,
         * just treat it like a heartbeat and return their currently assigned partitions.
         */
        val consumer = group.get(consumerId)
        completeAndScheduleNextHeartbeatExpiration(group, consumer)
        responseCallback(consumer.assignedTopicPartitions, consumerId, group.generationId, Errors.NONE.code)
      } else {
        val consumer = if (consumerId == JoinGroupRequest.UNKNOWN_CONSUMER_ID) {
          // if the consumer id is unknown, register this consumer to the group
          val generatedConsumerId = group.generateNextConsumerId
          val consumer = addConsumer(generatedConsumerId, topics, sessionTimeoutMs, group)
          maybePrepareRebalance(group)
          consumer
        } else {
          val consumer = group.get(consumerId)
          if (topics != consumer.topics) {
            // existing consumer changed its subscribed topics
            updateConsumer(group, consumer, topics)
            maybePrepareRebalance(group)
            consumer
          } else {
            // existing consumer rejoining a group due to rebalance
            consumer
          }
        }

        consumer.awaitingRebalanceCallback = responseCallback

        if (group.is(PreparingRebalance))
          rebalancePurgatory.checkAndComplete(ConsumerGroupKey(group.groupId))
      }
    }
  }

  def handleLeaveGroup(groupId: String, consumerId: String, responseCallback: Short => Unit) {
    if (!isActive.get) {
      responseCallback(Errors.CONSUMER_COORDINATOR_NOT_AVAILABLE.code)
    } else if (!isCoordinatorForGroup(groupId)) {
      responseCallback(Errors.NOT_COORDINATOR_FOR_CONSUMER.code)
    } else {
      val group = coordinatorMetadata.getGroup(groupId)
      if (group == null) {
        // if the group is marked as dead, it means some other thread has just removed the group
        // from the coordinator metadata; this is likely that the group has migrated to some other
        // coordinator OR the group is in a transient unstable phase. Let the consumer to retry
        // joining without specified consumer id,
        responseCallback(Errors.UNKNOWN_CONSUMER_ID.code)
      } else {
        group synchronized {
          if (group.is(Dead)) {
            responseCallback(Errors.UNKNOWN_CONSUMER_ID.code)
          } else if (!group.has(consumerId)) {
            responseCallback(Errors.UNKNOWN_CONSUMER_ID.code)
          } else {
            val consumer = group.get(consumerId)
            removeHeartbeatForLeavingConsumer(group, consumer)
            onConsumerFailure(group, consumer)
            responseCallback(Errors.NONE.code)
            if (group.is(PreparingRebalance))
              rebalancePurgatory.checkAndComplete(ConsumerGroupKey(group.groupId))
          }
        }
      }
    }
  }

  def handleHeartbeat(groupId: String,
                      consumerId: String,
                      generationId: Int,
                      responseCallback: Short => Unit) {
    if (!isActive.get) {
      responseCallback(Errors.CONSUMER_COORDINATOR_NOT_AVAILABLE.code)
    } else if (!isCoordinatorForGroup(groupId)) {
      responseCallback(Errors.NOT_COORDINATOR_FOR_CONSUMER.code)
    } else {
      val group = coordinatorMetadata.getGroup(groupId)
      if (group == null) {
        // if the group is marked as dead, it means some other thread has just removed the group
        // from the coordinator metadata; this is likely that the group has migrated to some other
        // coordinator OR the group is in a transient unstable phase. Let the consumer to retry
        // joining without specified consumer id,
        responseCallback(Errors.UNKNOWN_CONSUMER_ID.code)
      } else {
        group synchronized {
          if (group.is(Dead)) {
            responseCallback(Errors.UNKNOWN_CONSUMER_ID.code)
          } else if (!group.has(consumerId)) {
            responseCallback(Errors.UNKNOWN_CONSUMER_ID.code)
          } else if (generationId != group.generationId) {
            responseCallback(Errors.ILLEGAL_GENERATION.code)
          } else if (!group.is(Stable)) {
            responseCallback(Errors.REBALANCE_IN_PROGRESS.code)
          } else {
            val consumer = group.get(consumerId)
            completeAndScheduleNextHeartbeatExpiration(group, consumer)
            responseCallback(Errors.NONE.code)
          }
        }
      }
    }
  }

  def handleCommitOffsets(groupId: String,
                          consumerId: String,
                          generationId: Int,
                          offsetMetadata: immutable.Map[TopicAndPartition, OffsetAndMetadata],
                          responseCallback: immutable.Map[TopicAndPartition, Short] => Unit) {
    if (!isActive.get) {
      responseCallback(offsetMetadata.mapValues(_ => Errors.CONSUMER_COORDINATOR_NOT_AVAILABLE.code))
    } else if (!isCoordinatorForGroup(groupId)) {
      responseCallback(offsetMetadata.mapValues(_ => Errors.NOT_COORDINATOR_FOR_CONSUMER.code))
    } else {
      val group = coordinatorMetadata.getGroup(groupId)
      if (group == null) {
        if (generationId < 0)
          // the group is not relying on Kafka for partition management, so allow the commit
          offsetManager.storeOffsets(groupId, consumerId, generationId, offsetMetadata, responseCallback)
        else
          // the group has failed over to this coordinator (which will be handled in KAFKA-2017),
          // or this is a request coming from an older generation. either way, reject the commit
          responseCallback(offsetMetadata.mapValues(_ => Errors.ILLEGAL_GENERATION.code))
      } else {
        group synchronized {
          if (group.is(Dead)) {
            responseCallback(offsetMetadata.mapValues(_ => Errors.UNKNOWN_CONSUMER_ID.code))
          } else if (!group.has(consumerId)) {
            responseCallback(offsetMetadata.mapValues(_ => Errors.UNKNOWN_CONSUMER_ID.code))
          } else if (generationId != group.generationId) {
            responseCallback(offsetMetadata.mapValues(_ => Errors.ILLEGAL_GENERATION.code))
          } else if (!offsetMetadata.keySet.subsetOf(group.get(consumerId).assignedTopicPartitions)) {
            responseCallback(offsetMetadata.mapValues(_ => Errors.COMMITTING_PARTITIONS_NOT_ASSIGNED.code))
          } else {
            offsetManager.storeOffsets(groupId, consumerId, generationId, offsetMetadata, responseCallback)
          }
        }
      }
    }
  }

  def handleFetchOffsets(groupId: String,
                         partitions: Seq[TopicAndPartition]): Map[TopicAndPartition, OffsetMetadataAndError] = {
    if (!isActive.get) {
      partitions.map {case topicAndPartition => (topicAndPartition, OffsetMetadataAndError.NotCoordinatorForGroup)}.toMap
    } else if (!isCoordinatorForGroup(groupId)) {
      partitions.map {case topicAndPartition => (topicAndPartition, OffsetMetadataAndError.NotCoordinatorForGroup)}.toMap
    } else {
      val group = coordinatorMetadata.getGroup(groupId)
      if (group == null) {
        // if the group does not exist, it means this group is not relying
        // on Kafka for partition management, and hence never send join-group
        // request to the coordinator before; in this case blindly fetch the offsets
        offsetManager.getOffsets(groupId, partitions)
      } else {
        group synchronized {
          if (group.is(Dead)) {
            partitions.map {case topicAndPartition => (topicAndPartition, OffsetMetadataAndError.UnknownConsumer)}.toMap
          } else {
            offsetManager.getOffsets(groupId, partitions)
          }
        }
      }
    }
  }

  def handleGroupImmigration(offsetTopicPartitionId: Int) = {
    // TODO we may need to add more logic in KAFKA-2017
    offsetManager.loadOffsetsFromLog(offsetTopicPartitionId)
  }

  def handleGroupEmigration(offsetTopicPartitionId: Int) = {
    // TODO we may need to add more logic in KAFKA-2017
    offsetManager.removeOffsetsFromCacheForPartition(offsetTopicPartitionId)
  }

  /**
   * Complete existing DelayedHeartbeats for the given consumer and schedule the next one
   */
  private def completeAndScheduleNextHeartbeatExpiration(group: ConsumerGroupMetadata, consumer: ConsumerMetadata) {
    // complete current heartbeat expectation
    consumer.latestHeartbeat = SystemTime.milliseconds
    val consumerKey = ConsumerKey(consumer.groupId, consumer.consumerId)
    heartbeatPurgatory.checkAndComplete(consumerKey)

    // reschedule the next heartbeat expiration deadline
    val newHeartbeatDeadline = consumer.latestHeartbeat + consumer.sessionTimeoutMs
    val delayedHeartbeat = new DelayedHeartbeat(this, group, consumer, newHeartbeatDeadline, consumer.sessionTimeoutMs)
    heartbeatPurgatory.tryCompleteElseWatch(delayedHeartbeat, Seq(consumerKey))
  }

  private def removeHeartbeatForLeavingConsumer(group: ConsumerGroupMetadata, consumer: ConsumerMetadata) {
    consumer.isLeaving = true
    val consumerKey = ConsumerKey(consumer.groupId, consumer.consumerId)
    heartbeatPurgatory.checkAndComplete(consumerKey)
  }

  private def addConsumer(consumerId: String,
                          topics: Set[String],
                          sessionTimeoutMs: Int,
                          group: ConsumerGroupMetadata) = {
    val consumer = new ConsumerMetadata(consumerId, group.groupId, topics, sessionTimeoutMs)
    val topicsToBind = topics -- group.topics
    group.add(consumer.consumerId, consumer)
    coordinatorMetadata.bindGroupToTopics(group.groupId, topicsToBind)
    consumer
  }

  private def removeConsumer(group: ConsumerGroupMetadata, consumer: ConsumerMetadata) {
    group.remove(consumer.consumerId)
    val topicsToUnbind = consumer.topics -- group.topics
    coordinatorMetadata.unbindGroupFromTopics(group.groupId, topicsToUnbind)
  }

  private def updateConsumer(group: ConsumerGroupMetadata, consumer: ConsumerMetadata, topics: Set[String]) {
    val topicsToBind = topics -- group.topics
    group.remove(consumer.consumerId)
    val topicsToUnbind = consumer.topics -- (group.topics ++ topics)
    group.add(consumer.consumerId, consumer)
    consumer.topics = topics
    coordinatorMetadata.bindAndUnbindGroupFromTopics(group.groupId, topicsToBind, topicsToUnbind)
  }

  private def maybePrepareRebalance(group: ConsumerGroupMetadata) {
    group synchronized {
      if (group.canRebalance)
        prepareRebalance(group)
    }
  }

  private def prepareRebalance(group: ConsumerGroupMetadata) {
    group.transitionTo(PreparingRebalance)
    info("Preparing to rebalance group %s with old generation %s".format(group.groupId, group.generationId))

    val rebalanceTimeout = group.rebalanceTimeout
    val delayedRebalance = new DelayedRebalance(this, group, rebalanceTimeout)
    val consumerGroupKey = ConsumerGroupKey(group.groupId)
    rebalancePurgatory.tryCompleteElseWatch(delayedRebalance, Seq(consumerGroupKey))
  }

  private def rebalance(group: ConsumerGroupMetadata) {
    assert(group.notYetRejoinedConsumers == List.empty[ConsumerMetadata])

    group.transitionTo(Rebalancing)
    group.generationId += 1

    info("Rebalancing group %s with new generation %s".format(group.groupId, group.generationId))

    val assignedPartitionsPerConsumer = reassignPartitions(group)
    trace("Rebalance for group %s generation %s has assigned partitions: %s"
          .format(group.groupId, group.generationId, assignedPartitionsPerConsumer))

    group.transitionTo(Stable)
    info("Stabilized group %s generation %s".format(group.groupId, group.generationId))
  }

  private def onConsumerFailure(group: ConsumerGroupMetadata, consumer: ConsumerMetadata) {
    trace("Consumer %s in group %s has failed".format(consumer.consumerId, group.groupId))
    removeConsumer(group, consumer)
    maybePrepareRebalance(group)
  }

  private def reassignPartitions(group: ConsumerGroupMetadata) = {
    val assignor = PartitionAssignor.createInstance(group.partitionAssignmentStrategy)
    val topicsPerConsumer = group.topicsPerConsumer
    val partitionsPerTopic = coordinatorMetadata.partitionsPerTopic
    val assignedPartitionsPerConsumer = assignor.assign(topicsPerConsumer, partitionsPerTopic)
    assignedPartitionsPerConsumer.foreach { case (consumerId, partitions) =>
      group.get(consumerId).assignedTopicPartitions = partitions
    }
    assignedPartitionsPerConsumer
  }

  def tryCompleteRebalance(group: ConsumerGroupMetadata, forceComplete: () => Boolean) = {
    group synchronized {
      if (group.notYetRejoinedConsumers.isEmpty)
        forceComplete()
      else false
    }
  }

  def onExpirationRebalance() {
    // TODO: add metrics for rebalance timeouts
  }

  def onCompleteRebalance(group: ConsumerGroupMetadata) {
    group synchronized {
      val failedConsumers = group.notYetRejoinedConsumers
      if (group.isEmpty || !failedConsumers.isEmpty) {
        failedConsumers.foreach { failedConsumer =>
          removeConsumer(group, failedConsumer)
          // TODO: cut the socket connection to the consumer
        }

        if (group.isEmpty) {
          group.transitionTo(Dead)
          info("Group %s generation %s is dead and removed".format(group.groupId, group.generationId))
          coordinatorMetadata.removeGroup(group.groupId, group.topics)
        }
      }
      if (!group.is(Dead)) {
        // assign partitions to existing consumers of the group according to the partitioning strategy
        rebalance(group)

        // trigger the awaiting join group response callback for all the consumers after rebalancing
        for (consumer <- group.allConsumers) {
          assert(consumer.awaitingRebalanceCallback != null)
          consumer.awaitingRebalanceCallback(consumer.assignedTopicPartitions, consumer.consumerId, group.generationId, Errors.NONE.code)
          consumer.awaitingRebalanceCallback = null
          completeAndScheduleNextHeartbeatExpiration(group, consumer)
        }
      }
    }
  }

  def tryCompleteHeartbeat(group: ConsumerGroupMetadata, consumer: ConsumerMetadata, heartbeatDeadline: Long, forceComplete: () => Boolean) = {
    group synchronized {
      if (shouldKeepConsumerAlive(consumer, heartbeatDeadline) || consumer.isLeaving)
        forceComplete()
      else false
    }
  }

  def onExpirationHeartbeat(group: ConsumerGroupMetadata, consumer: ConsumerMetadata, heartbeatDeadline: Long) {
    group synchronized {
      if (!shouldKeepConsumerAlive(consumer, heartbeatDeadline))
        onConsumerFailure(group, consumer)
    }
  }

  def onCompleteHeartbeat() {
    // TODO: add metrics for complete heartbeats
  }

  def partitionFor(group: String): Int = offsetManager.partitionFor(group)

  private def shouldKeepConsumerAlive(consumer: ConsumerMetadata, heartbeatDeadline: Long) =
    consumer.awaitingRebalanceCallback != null || consumer.latestHeartbeat + consumer.sessionTimeoutMs > heartbeatDeadline

  private def isCoordinatorForGroup(groupId: String) = offsetManager.leaderIsLocal(offsetManager.partitionFor(groupId))
}

object ConsumerCoordinator {

  val OffsetsTopicName = "__consumer_offsets"

  def create(config: KafkaConfig,
             zkUtils: ZkUtils,
             replicaManager: ReplicaManager,
             kafkaScheduler: KafkaScheduler): ConsumerCoordinator = {
    val offsetConfig = OffsetManagerConfig(maxMetadataSize = config.offsetMetadataMaxSize,
      loadBufferSize = config.offsetsLoadBufferSize,
      offsetsRetentionMs = config.offsetsRetentionMinutes * 60 * 1000L,
      offsetsRetentionCheckIntervalMs = config.offsetsRetentionCheckIntervalMs,
      offsetsTopicNumPartitions = config.offsetsTopicPartitions,
      offsetsTopicReplicationFactor = config.offsetsTopicReplicationFactor,
      offsetCommitTimeoutMs = config.offsetCommitTimeoutMs,
      offsetCommitRequiredAcks = config.offsetCommitRequiredAcks)
    val groupConfig = GroupManagerConfig(consumerMinSessionTimeoutMs = config.consumerMinSessionTimeoutMs,
      consumerMaxSessionTimeoutMs = config.consumerMaxSessionTimeoutMs)

    new ConsumerCoordinator(config.brokerId, groupConfig, offsetConfig, replicaManager, zkUtils, kafkaScheduler)
  }

  def create(config: KafkaConfig,
             zkUtils: ZkUtils,
             offsetManager: OffsetManager): ConsumerCoordinator = {
    val offsetConfig = OffsetManagerConfig(maxMetadataSize = config.offsetMetadataMaxSize,
      loadBufferSize = config.offsetsLoadBufferSize,
      offsetsRetentionMs = config.offsetsRetentionMinutes * 60 * 1000L,
      offsetsRetentionCheckIntervalMs = config.offsetsRetentionCheckIntervalMs,
      offsetsTopicNumPartitions = config.offsetsTopicPartitions,
      offsetsTopicReplicationFactor = config.offsetsTopicReplicationFactor,
      offsetCommitTimeoutMs = config.offsetCommitTimeoutMs,
      offsetCommitRequiredAcks = config.offsetCommitRequiredAcks)
    val groupConfig = GroupManagerConfig(consumerMinSessionTimeoutMs = config.consumerMinSessionTimeoutMs,
      consumerMaxSessionTimeoutMs = config.consumerMaxSessionTimeoutMs)

    new ConsumerCoordinator(config.brokerId, groupConfig, offsetConfig, offsetManager, zkUtils)
  }
}
