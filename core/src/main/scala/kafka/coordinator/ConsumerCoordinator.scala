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

import org.apache.kafka.common.protocol.Errors

import kafka.common.TopicAndPartition
import kafka.server._
import kafka.utils._

import scala.collection.mutable.HashMap

import org.I0Itec.zkclient.{IZkChildListener, ZkClient}
import org.apache.kafka.common.requests.JoinGroupRequest


/**
 * Kafka coordinator handles consumer group and consumer offset management.
 *
 * Each Kafka server instantiates a coordinator, which is responsible for a set of
 * consumer groups; the consumer groups are assigned to coordinators based on their
 * group names.
 */
class ConsumerCoordinator(val config: KafkaConfig,
                          val zkClient: ZkClient) extends Logging {

  this.logIdent = "[Kafka Coordinator " + config.brokerId + "]: "

  /* zookeeper listener for topic-partition changes */
  private val topicPartitionChangeListeners = new HashMap[String, TopicPartitionChangeListener]

  /* the consumer group registry cache */
  // TODO: access to this map needs to be synchronized
  private val consumerGroupRegistries = new HashMap[String, GroupRegistry]

  /* the list of subscribed groups per topic */
  // TODO: access to this map needs to be synchronized
  private val consumerGroupsPerTopic = new HashMap[String, List[String]]

  /* the delayed operation purgatory for heartbeat-based failure detection */
  private var heartbeatPurgatory: DelayedOperationPurgatory[DelayedHeartbeat] = null

  /* the delayed operation purgatory for handling join-group requests */
  private var joinGroupPurgatory: DelayedOperationPurgatory[DelayedJoinGroup] = null

  /* the delayed operation purgatory for preparing rebalance process */
  private var rebalancePurgatory: DelayedOperationPurgatory[DelayedRebalance] = null

  /* latest consumer heartbeat bucket's end timestamp in milliseconds */
  private var latestHeartbeatBucketEndMs: Long = SystemTime.milliseconds

  /**
   * Start-up logic executed at the same time when the server starts up.
   */
  def startup() {

    // Initialize consumer group registries and heartbeat bucket metadata
    latestHeartbeatBucketEndMs = SystemTime.milliseconds

    // Initialize purgatories for delayed heartbeat, join-group and rebalance operations
    heartbeatPurgatory = new DelayedOperationPurgatory[DelayedHeartbeat](purgatoryName = "Heartbeat", brokerId = config.brokerId)
    joinGroupPurgatory = new DelayedOperationPurgatory[DelayedJoinGroup](purgatoryName = "JoinGroup", brokerId = config.brokerId)
    rebalancePurgatory = new DelayedOperationPurgatory[DelayedRebalance](purgatoryName = "Rebalance", brokerId = config.brokerId)

  }

  /**
   * Shut-down logic executed at the same time when server shuts down,
   * ordering of actions should be reversed from the start-up process
   *
   */
  def shutdown() {

    // De-register all Zookeeper listeners for topic-partition changes
    for (topic <- topicPartitionChangeListeners.keys)  {
      deregisterTopicChangeListener(topic)
    }
    topicPartitionChangeListeners.clear()

    // Shutdown purgatories for delayed heartbeat, join-group and rebalance operations
    heartbeatPurgatory.shutdown()
    joinGroupPurgatory.shutdown()
    rebalancePurgatory.shutdown()

    // Clean up consumer group registries metadata
    consumerGroupRegistries.clear()
    consumerGroupsPerTopic.clear()
  }

  /**
   * Process a join-group request from a consumer to join as a new group member
   */
  def consumerJoinGroup(groupId: String,
                        consumerId: String,
                        topics: List[String],
                        sessionTimeoutMs: Int,
                        partitionAssignmentStrategy: String,
                        responseCallback:(List[TopicAndPartition], Int, Short) => Unit ) {

    // if the group does not exist yet, create one
    if (!consumerGroupRegistries.contains(groupId))
      createNewGroup(groupId, partitionAssignmentStrategy)

    val groupRegistry = consumerGroupRegistries(groupId)

    // if the consumer id is unknown or it does exists in
    // the group yet, register this consumer to the group
    if (consumerId.equals(JoinGroupRequest.UNKNOWN_CONSUMER_ID)) {
      createNewConsumer(groupId, groupRegistry.generateNextConsumerId, topics, sessionTimeoutMs)
    } else if (!groupRegistry.memberRegistries.contains(consumerId)) {
      createNewConsumer(groupId, consumerId, topics, sessionTimeoutMs)
    }

    // add a delayed join-group operation to the purgatory
    // TODO

    // if the current group is under rebalance process,
    // check if the delayed rebalance operation can be finished
    // TODO

    // TODO --------------------------------------------------------------
    // TODO: this is just a stub for new consumer testing,
    // TODO: needs to be replaced with the logic above
    // TODO --------------------------------------------------------------
    // just return all the partitions of the subscribed topics
    val partitionIdsPerTopic = ZkUtils.getPartitionsForTopics(zkClient, topics)
    val partitions = partitionIdsPerTopic.flatMap{ case (topic, partitionIds) =>
      partitionIds.map(partition => {
        TopicAndPartition(topic, partition)
      })
    }.toList

    responseCallback(partitions, 1 /* generation id */, Errors.NONE.code)

    info("Handled join-group from consumer " + consumerId + " to group " + groupId)
  }

  /**
   * Process a heartbeat request from a consumer
   */
  def consumerHeartbeat(groupId: String,
                        consumerId: String,
                        generationId: Int,
                        responseCallback: Short => Unit) {

    // check that the group already exists
    // TODO

    // check that the consumer has already registered for the group
    // TODO

    // check if the consumer generation id is correct
    // TODO

    // remove the consumer from its current heartbeat bucket, and add it back to the corresponding bucket
    // TODO

    // create the heartbeat response, if partition rebalance is triggered set the corresponding error code
    // TODO

    info("Handled heartbeat of consumer " + consumerId + " from group " + groupId)

    // TODO --------------------------------------------------------------
    // TODO: this is just a stub for new consumer testing,
    // TODO: needs to be replaced with the logic above
    // TODO --------------------------------------------------------------
    // check if the consumer already exist, if yes return OK,
    // otherwise return illegal generation error
    if (consumerGroupRegistries.contains(groupId)
      && consumerGroupRegistries(groupId).memberRegistries.contains(consumerId))
      responseCallback(Errors.NONE.code)
    else
      responseCallback(Errors.ILLEGAL_GENERATION.code)
  }

  /**
   * Create a new consumer
   */
  private def createNewConsumer(groupId: String,
                                consumerId: String,
                                topics: List[String],
                                sessionTimeoutMs: Int) {
    debug("Registering consumer " + consumerId + " for group " + groupId)

    // create the new consumer registry entry
    val consumerRegistry = new ConsumerRegistry(groupId, consumerId, topics, sessionTimeoutMs)

    consumerGroupRegistries(groupId).memberRegistries.put(consumerId, consumerRegistry)

    // check if the partition assignment strategy is consistent with the group
    // TODO

    // add the group to the subscribed topics
    // TODO

    // schedule heartbeat tasks for the consumer
    // TODO

    // add the member registry entry to the group
    // TODO

    // start preparing group partition rebalance
    // TODO

    info("Registered consumer " + consumerId + " for group " + groupId)
  }

  /**
   * Create a new consumer group in the registry
   */
  private def createNewGroup(groupId: String, partitionAssignmentStrategy: String) {
    debug("Creating new group " + groupId)

    val groupRegistry = new GroupRegistry(groupId, partitionAssignmentStrategy)

    consumerGroupRegistries.put(groupId, groupRegistry)

    info("Created new group registry " + groupId)
  }

  /**
   * Callback invoked when a consumer's heartbeat has expired
   */
  private def onConsumerHeartbeatExpired(groupId: String, consumerId: String) {

    // if the consumer does not exist in group registry anymore, do nothing
    // TODO

    // record heartbeat failure
    // TODO

    // if the maximum failures has been reached, mark consumer as failed
    // TODO
  }

  /**
   * Callback invoked when a consumer is marked as failed
   */
  private def onConsumerFailure(groupId: String, consumerId: String) {

    // remove the consumer from its group registry metadata
    // TODO

    // cut the socket connection to the consumer
    // TODO: howto ??

    // if the group has no consumer members any more, remove the group
    // otherwise start preparing group partition rebalance
    // TODO

  }

  /**
   * Prepare partition rebalance for the group
   */
  private def prepareRebalance(groupId: String) {

    // try to change the group state to PrepareRebalance

    // add a task to the delayed rebalance purgatory

    // TODO
  }

  /**
   * Start partition rebalance for the group
   */
  private def startRebalance(groupId: String) {

    // try to change the group state to UnderRebalance

    // compute new assignment based on the strategy

    // send back the join-group response

    // TODO
  }

  /**
   * Fail current partition rebalance for the group
   */

  /**
   * Register ZK listeners for topic-partition changes
   */
  private def registerTopicChangeListener(topic: String) = {
    if (!topicPartitionChangeListeners.contains(topic)) {
      val listener = new TopicPartitionChangeListener(config)
      topicPartitionChangeListeners.put(topic, listener)
      ZkUtils.makeSurePersistentPathExists(zkClient, ZkUtils.getTopicPath(topic))
      zkClient.subscribeChildChanges(ZkUtils.getTopicPath(topic), listener)
    }
  }

  /**
   * De-register ZK listeners for topic-partition changes
   */
  private def deregisterTopicChangeListener(topic: String) = {
    val listener = topicPartitionChangeListeners.get(topic).get
    zkClient.unsubscribeChildChanges(ZkUtils.getTopicPath(topic), listener)
    topicPartitionChangeListeners.remove(topic)
  }

  /**
   * Zookeeper listener that catch topic-partition changes
   */
  class TopicPartitionChangeListener(val config: KafkaConfig) extends IZkChildListener with Logging {

    this.logIdent = "[TopicChangeListener on coordinator " + config.brokerId + "]: "

    /**
     * Try to trigger a rebalance for each group subscribed in the changed topic
     *
     * @throws Exception
     *            On any error.
     */
    def handleChildChange(parentPath: String , curChilds: java.util.List[String]) {
      debug("Fired for path %s with children %s".format(parentPath, curChilds))

      // get the topic
      val topic = parentPath.split("/").last

      // get groups that subscribed to this topic
      val groups = consumerGroupsPerTopic.get(topic).get

      for (groupId <- groups) {
        prepareRebalance(groupId)
      }
    }
  }
}


