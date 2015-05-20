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

import kafka.common.TopicAndPartition
import kafka.server._
import kafka.utils._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.JoinGroupRequest

import org.I0Itec.zkclient.ZkClient
import java.util.concurrent.atomic.AtomicBoolean

// TODO: expose MinSessionTimeoutMs and MaxSessionTimeoutMs in broker configs
object ConsumerCoordinator {
  private val MinSessionTimeoutMs = 6000
  private val MaxSessionTimeoutMs = 30000
}

/**
 * ConsumerCoordinator handles consumer group and consumer offset management.
 *
 * Each Kafka server instantiates a coordinator which is responsible for a set of
 * consumer groups. Consumer groups are assigned to coordinators based on their
 * group names.
 */
class ConsumerCoordinator(val config: KafkaConfig,
                          val zkClient: ZkClient,
                          val offsetManager: OffsetManager) extends Logging {
  import ConsumerCoordinator._

  this.logIdent = "[ConsumerCoordinator " + config.brokerId + "]: "

  private val isActive = new AtomicBoolean(false)

  private var heartbeatPurgatory: DelayedOperationPurgatory[DelayedHeartbeat] = null
  private var rebalancePurgatory: DelayedOperationPurgatory[DelayedRebalance] = null
  private var coordinatorMetadata: CoordinatorMetadata = null

  /**
   * NOTE: If a group lock and coordinatorLock are simultaneously needed,
   * be sure to acquire the group lock before coordinatorLock to prevent deadlock
   */

  /**
   * Startup logic executed at the same time when the server starts up.
   */
  def startup() {
    info("Starting up.")
    heartbeatPurgatory = new DelayedOperationPurgatory[DelayedHeartbeat]("Heartbeat", config.brokerId)
    rebalancePurgatory = new DelayedOperationPurgatory[DelayedRebalance]("Rebalance", config.brokerId)
    coordinatorMetadata = new CoordinatorMetadata(config, zkClient, maybePrepareRebalance)
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
    } else if (sessionTimeoutMs < MinSessionTimeoutMs || sessionTimeoutMs > MaxSessionTimeoutMs) {
      responseCallback(Set.empty, consumerId, 0, Errors.INVALID_SESSION_TIMEOUT.code)
    } else {
      val group = coordinatorMetadata.getGroup(groupId)
      if (group == null) {
        if (consumerId != JoinGroupRequest.UNKNOWN_CONSUMER_ID) {
          responseCallback(Set.empty, consumerId, 0, Errors.UNKNOWN_CONSUMER_ID.code)
        } else {
          val group = coordinatorMetadata.addGroup(groupId, partitionAssignmentStrategy)
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
        responseCallback(Set.empty, consumerId, 0, Errors.UNKNOWN_CONSUMER_ID.code)
      } else if (partitionAssignmentStrategy != group.partitionAssignmentStrategy) {
        responseCallback(Set.empty, consumerId, 0, Errors.INCONSISTENT_PARTITION_ASSIGNMENT_STRATEGY.code)
      } else if (consumerId != JoinGroupRequest.UNKNOWN_CONSUMER_ID && !group.has(consumerId)) {
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
        responseCallback(Errors.UNKNOWN_CONSUMER_ID.code)
      } else {
        group synchronized {
          if (group.is(Dead)) {
            responseCallback(Errors.UNKNOWN_CONSUMER_ID.code)
          } else if (!group.has(consumerId)) {
            responseCallback(Errors.UNKNOWN_CONSUMER_ID.code)
          } else if (generationId != group.generationId) {
            responseCallback(Errors.ILLEGAL_GENERATION.code)
          } else {
            val consumer = group.get(consumerId)
            completeAndScheduleNextHeartbeatExpiration(group, consumer)
            responseCallback(Errors.NONE.code)
          }
        }
      }
    }
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
    val topicsToUnbind = consumer.topics -- group.topics
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
    group.generationId += 1
    info("Preparing to rebalance group %s generation %s".format(group.groupId, group.generationId))

    val rebalanceTimeout = group.rebalanceTimeout
    val delayedRebalance = new DelayedRebalance(this, group, rebalanceTimeout)
    val consumerGroupKey = ConsumerGroupKey(group.groupId)
    rebalancePurgatory.tryCompleteElseWatch(delayedRebalance, Seq(consumerGroupKey))
  }

  private def rebalance(group: ConsumerGroupMetadata) {
    assert(group.notYetRejoinedConsumers == List.empty[ConsumerMetadata])

    group.transitionTo(Rebalancing)
    info("Rebalancing group %s generation %s".format(group.groupId, group.generationId))

    val assignedPartitionsPerConsumer = reassignPartitions(group)
    trace("Rebalance for group %s generation %s has assigned partitions: %s"
          .format(group.groupId, group.generationId, assignedPartitionsPerConsumer))

    group.transitionTo(Stable)
    info("Stabilized group %s generation %s".format(group.groupId, group.generationId))
  }

  private def onConsumerHeartbeatExpired(group: ConsumerGroupMetadata, consumer: ConsumerMetadata) {
    trace("Consumer %s in group %s has failed".format(consumer.consumerId, group.groupId))
    removeConsumer(group, consumer)
    maybePrepareRebalance(group)
  }

  private def isCoordinatorForGroup(groupId: String) = offsetManager.leaderIsLocal(offsetManager.partitionFor(groupId))

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
      if (group.notYetRejoinedConsumers == List.empty[ConsumerMetadata])
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
          info("Group %s generation %s is dead".format(group.groupId, group.generationId))
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
      if (shouldKeepConsumerAlive(consumer, heartbeatDeadline))
        forceComplete()
      else false
    }
  }

  def onExpirationHeartbeat(group: ConsumerGroupMetadata, consumer: ConsumerMetadata, heartbeatDeadline: Long) {
    group synchronized {
      if (!shouldKeepConsumerAlive(consumer, heartbeatDeadline))
        onConsumerHeartbeatExpired(group, consumer)
    }
  }

  def onCompleteHeartbeat() {}

  private def shouldKeepConsumerAlive(consumer: ConsumerMetadata, heartbeatDeadline: Long) =
    consumer.awaitingRebalanceCallback != null || consumer.latestHeartbeat + consumer.sessionTimeoutMs > heartbeatDeadline
}
