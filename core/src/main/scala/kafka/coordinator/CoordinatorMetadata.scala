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

import kafka.server.KafkaConfig
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.utils.{threadsafe, ZkUtils, Logging}
import kafka.utils.ZkUtils._
import org.I0Itec.zkclient.{ZkClient, IZkDataListener}

import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable

/**
 * CoordinatorMetadata manages group and topic metadata.
 * It delegates all group logic to the callers.
 */
@threadsafe
private[coordinator] class CoordinatorMetadata(brokerId: Int,
                                               zkUtils: ZkUtils,
                                               maybePrepareRebalance: ConsumerGroupMetadata => Unit) {

  /**
   * NOTE: If a group lock and metadataLock are simultaneously needed,
   * be sure to acquire the group lock before metadataLock to prevent deadlock
   */
  private val metadataLock = new ReentrantReadWriteLock()

  /**
   * These should be guarded by metadataLock
   */
  private val groups = new mutable.HashMap[String, ConsumerGroupMetadata]
  private val groupsPerTopic = new mutable.HashMap[String, Set[String]]
  private val topicPartitionCounts = new mutable.HashMap[String, Int]
  private val topicPartitionChangeListeners = new mutable.HashMap[String, TopicPartitionChangeListener]

  def shutdown() {
    inWriteLock(metadataLock) {
      topicPartitionChangeListeners.keys.foreach(deregisterTopicPartitionChangeListener)
      topicPartitionChangeListeners.clear()
      groups.clear()
      groupsPerTopic.clear()
      topicPartitionCounts.clear()
    }
  }

  def partitionsPerTopic = {
    inReadLock(metadataLock) {
      topicPartitionCounts.toMap
    }
  }

  /**
   * Get the group associated with the given groupId, or null if not found
   */
  def getGroup(groupId: String) = {
    inReadLock(metadataLock) {
      groups.get(groupId).orNull
    }
  }

  /**
   * Add a group or get the group associated with the given groupId if it already exists
   */
  def addGroup(groupId: String, partitionAssignmentStrategy: String) = {
    inWriteLock(metadataLock) {
      groups.getOrElseUpdate(groupId, new ConsumerGroupMetadata(groupId, partitionAssignmentStrategy))
    }
  }

  /**
   * Remove all metadata associated with the group, including its topics
   * @param groupId the groupId of the group we are removing
   * @param topicsForGroup topics that consumers in the group were subscribed to
   */
  def removeGroup(groupId: String, topicsForGroup: Set[String]) {
    inWriteLock(metadataLock) {
      topicsForGroup.foreach(topic => unbindGroupFromTopics(groupId, topicsForGroup))
      groups.remove(groupId)
    }
  }

  /**
   * Add the given group to the set of groups interested in
   * topic partition changes for the given topics
   */
  def bindGroupToTopics(groupId: String, topics: Set[String]) {
    inWriteLock(metadataLock) {
      require(groups.contains(groupId), "CoordinatorMetadata can only bind existing groups")
      topics.foreach(topic => bindGroupToTopic(groupId, topic))
    }
  }

  /**
   * Remove the given group from the set of groups interested in
   * topic partition changes for the given topics
   */
  def unbindGroupFromTopics(groupId: String, topics: Set[String]) {
    inWriteLock(metadataLock) {
      require(groups.contains(groupId), "CoordinatorMetadata can only unbind existing groups")
      topics.foreach(topic => unbindGroupFromTopic(groupId, topic))
    }
  }

  /**
   * Add the given group to the set of groups interested in the topicsToBind and
   * remove the given group from the set of groups interested in the topicsToUnbind
   */
  def bindAndUnbindGroupFromTopics(groupId: String, topicsToBind: Set[String], topicsToUnbind: Set[String]) {
    inWriteLock(metadataLock) {
      require(groups.contains(groupId), "CoordinatorMetadata can only update topic bindings for existing groups")
      topicsToBind.foreach(topic => bindGroupToTopic(groupId, topic))
      topicsToUnbind.foreach(topic => unbindGroupFromTopic(groupId, topic))
    }
  }

  private def isListeningToTopic(topic: String) = topicPartitionChangeListeners.contains(topic)

  private def bindGroupToTopic(groupId: String, topic: String) {
    if (isListeningToTopic(topic)) {
      val currentGroupsForTopic = groupsPerTopic(topic)
      groupsPerTopic.put(topic, currentGroupsForTopic + groupId)
    }
    else {
      groupsPerTopic.put(topic, Set(groupId))
      topicPartitionCounts.put(topic, getTopicPartitionCountFromZK(topic))
      registerTopicPartitionChangeListener(topic)
    }
  }

  private def unbindGroupFromTopic(groupId: String, topic: String) {
    if (isListeningToTopic(topic)) {
      val remainingGroupsForTopic = groupsPerTopic(topic) - groupId
      if (remainingGroupsForTopic.isEmpty) {
        // no other group cares about the topic, so erase all metadata associated with the topic
        groupsPerTopic.remove(topic)
        topicPartitionCounts.remove(topic)
        deregisterTopicPartitionChangeListener(topic)
      } else {
        groupsPerTopic.put(topic, remainingGroupsForTopic)
      }
    }
  }

  private def getTopicPartitionCountFromZK(topic: String) = {
    val topicData = zkUtils.getPartitionAssignmentForTopics(Seq(topic))
    topicData(topic).size
  }

  private def registerTopicPartitionChangeListener(topic: String) {
    val listener = new TopicPartitionChangeListener
    topicPartitionChangeListeners.put(topic, listener)
    zkUtils.zkClient.subscribeDataChanges(getTopicPath(topic), listener)
  }

  private def deregisterTopicPartitionChangeListener(topic: String) {
    val listener = topicPartitionChangeListeners(topic)
    zkUtils.zkClient.unsubscribeDataChanges(getTopicPath(topic), listener)
    topicPartitionChangeListeners.remove(topic)
  }

  /**
   * Zookeeper listener to handle topic partition changes
   */
  class TopicPartitionChangeListener extends IZkDataListener with Logging {
    this.logIdent = "[TopicPartitionChangeListener on Coordinator " + brokerId + "]: "

    override def handleDataChange(dataPath: String, data: Object) {
      info("Handling data change for path: %s data: %s".format(dataPath, data))
      val topic = topicFromDataPath(dataPath)
      val numPartitions = getTopicPartitionCountFromZK(topic)

      val groupsToRebalance = inWriteLock(metadataLock) {
        /*
         * This condition exists because a consumer can leave and modify CoordinatorMetadata state
         * while ZkClient begins handling the data change but before we acquire the metadataLock.
         */
        if (isListeningToTopic(topic)) {
          topicPartitionCounts.put(topic, numPartitions)
          groupsPerTopic(topic).map(groupId => groups(groupId))
        }
        else Set.empty[ConsumerGroupMetadata]
      }
      groupsToRebalance.foreach(maybePrepareRebalance)
    }

    override def handleDataDeleted(dataPath: String) {
      info("Handling data delete for path: %s".format(dataPath))
      val topic = topicFromDataPath(dataPath)
      val groupsToRebalance = inWriteLock(metadataLock) {
        /*
         * This condition exists because a consumer can leave and modify CoordinatorMetadata state
         * while ZkClient begins handling the data delete but before we acquire the metadataLock.
         */
        if (isListeningToTopic(topic)) {
          topicPartitionCounts.put(topic, 0)
          groupsPerTopic(topic).map(groupId => groups(groupId))
        }
        else Set.empty[ConsumerGroupMetadata]
      }
      groupsToRebalance.foreach(maybePrepareRebalance)
    }

    private def topicFromDataPath(dataPath: String) = {
      val nodes = dataPath.split("/")
      nodes.last
    }
  }
}
