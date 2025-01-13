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
package kafka.zk

import java.util.Properties
import kafka.cluster.Broker
import kafka.controller.{LeaderIsrAndControllerEpoch, ReplicaAssignment}
import kafka.utils.Logging
import kafka.zk.TopicZNode.TopicIdReplicaAssignment
import kafka.zookeeper._
import org.apache.kafka.common.security.token.delegation.{DelegationToken, TokenInformation}
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.metadata.LeaderAndIsr
import org.apache.kafka.storage.internals.log.LogConfig

import scala.collection.{Map, Seq}

/**
 * Provides higher level Kafka-specific operations on top of the pipelined [[kafka.zookeeper.ZooKeeperClient]].
 *
 * Implementation note: this class includes methods for various components (Controller, Configs, Old Consumer, etc.)
 * and returns instances of classes from the calling packages in some cases. This is not ideal, but it made it
 * easier to migrate away from `ZkUtils` (since removed). We should revisit this. We should also consider whether a
 * monolithic [[kafka.zk.ZkData]] is the way to go.
 */
class KafkaZkClient() extends AutoCloseable with Logging {
  import KafkaZkClient._

  /**
   * Create a sequential persistent path. That is, the znode will not be automatically deleted upon client's disconnect
   * and a monotonically increasing number will be appended to its name.
   *
   * @param path the path to create (with the monotonically increasing number appended)
   * @param data the znode data
   * @return the created path (including the appended monotonically increasing number)
   */
  private[kafka] def createSequentialPersistentPath(path: String, data: Array[Byte]): String = {
    throw new UnsupportedOperationException()
  }

  /**
    * Registers the broker in zookeeper and return the broker epoch.
    * @param brokerInfo payload of the broker znode
    * @return broker epoch (znode create transaction id)
    */
  def registerBroker(brokerInfo: BrokerInfo): Long = {
    throw new UnsupportedOperationException()
  }

  /**
   * Registers a given broker in zookeeper as the controller and increments controller epoch.
   * @param controllerId the id of the broker that is to be registered as the controller.
   * @return the (updated controller epoch, epoch zkVersion) tuple
   */
  def registerControllerAndIncrementControllerEpoch(controllerId: Int): (Int, Int) = {
    throw new UnsupportedOperationException()
  }

  def updateBrokerInfo(brokerInfo: BrokerInfo): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
   * Gets topic partition states for the given partitions.
   * @param partitions the partitions for which we want to get states.
   * @return sequence of GetDataResponses whose contexts are the partitions they are associated with.
   */
  def getTopicPartitionStatesRaw(partitions: Seq[TopicPartition]): Seq[GetDataResponse] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Sets topic partition states for the given partitions.
   * @param leaderIsrAndControllerEpochs the partition states of each partition whose state we wish to set.
   * @param expectedControllerEpochZkVersion expected controller epoch zkVersion.
   * @return sequence of SetDataResponse whose contexts are the partitions they are associated with.
   */
  def setTopicPartitionStatesRaw(leaderIsrAndControllerEpochs: Map[TopicPartition, LeaderIsrAndControllerEpoch], expectedControllerEpochZkVersion: Int): Seq[SetDataResponse] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Creates topic partition state znodes for the given partitions.
   * @param leaderIsrAndControllerEpochs the partition states of each partition whose state we wish to set.
   * @param expectedControllerEpochZkVersion expected controller epoch zkVersion.
   * @return sequence of CreateResponse whose contexts are the partitions they are associated with.
   */
  def createTopicPartitionStatesRaw(leaderIsrAndControllerEpochs: Map[TopicPartition, LeaderIsrAndControllerEpoch], expectedControllerEpochZkVersion: Int): Seq[CreateResponse] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Sets the controller epoch conditioned on the given epochZkVersion.
   * @param epoch the epoch to set
   * @param epochZkVersion the expected version number of the epoch znode.
   * @return SetDataResponse
   */
  def setControllerEpochRaw(epoch: Int, epochZkVersion: Int): SetDataResponse = {
    throw new UnsupportedOperationException()
  }

  /**
   * Creates the controller epoch znode.
   * @param epoch the epoch to set
   * @return CreateResponse
   */
  def createControllerEpochRaw(epoch: Int): CreateResponse = {
    throw new UnsupportedOperationException()
  }

  /**
   * Update the partition states of multiple partitions in zookeeper.
   * @param leaderAndIsrs The partition states to update.
   * @param controllerEpoch The current controller epoch.
   * @param expectedControllerEpochZkVersion expected controller epoch zkVersion.
   * @return UpdateLeaderAndIsrResult instance containing per partition results.
   */
  def updateLeaderAndIsr(
    leaderAndIsrs: Map[TopicPartition, LeaderAndIsr],
    controllerEpoch: Int,
    expectedControllerEpochZkVersion: Int
  ): UpdateLeaderAndIsrResult = {
    throw new UnsupportedOperationException()
  }

  /**
   * Get log configs that merge local configs with topic-level configs in zookeeper.
   * @param topics The topics to get log configs for.
   * @param config The local configs.
   * @return A tuple of two values:
   *         1. The successfully gathered log configs
   *         2. Exceptions corresponding to failed log config lookups.
   */
  def getLogConfigs(
    topics: Set[String],
    config: java.util.Map[String, AnyRef]
  ): (Map[String, LogConfig], Map[String, Exception]) = {
    throw new UnsupportedOperationException()
  }

  /**
   * Get entity configs for a given entity name
   * @param rootEntityType entity type
   * @param sanitizedEntityName entity name
   * @return The successfully gathered log configs
   */
  def getEntityConfigs(rootEntityType: String, sanitizedEntityName: String): Properties = {
    throw new UnsupportedOperationException()
  }

  def getEntitiesConfigs(rootEntityType: String, sanitizedEntityNames: Set[String]): Map[String, Properties] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Sets or creates the entity znode path with the given configs depending
   * on whether it already exists or not.
   *
   * If enableEntityConfigControllerCheck is set, this method will ensure that a ZK controller is defined and
   * that it is not modified within the duration of this call. This is done to prevent configs from being
   * created or modified while the ZK to KRaft migration is taking place.
   *
   * The only case where enableEntityConfigControllerCheck should be false is when being called by ConfigCommand,
   * i.e., "kafka-configs.sh --zookeeper". This is an old behavior we have kept around to allow users to setup
   * SCRAM credentials and other configs before the cluster is started for the first time.
   *
   * If this is method is called concurrently, the last writer wins. In cases where we update configs and then
   * partition assignment (i.e. create topic), it's possible for one thread to set this and the other to set the
   * partition assignment. As such, the recommendation is to never call create topic for the same topic with different
   * configs/partition assignment concurrently.
   *
   * @param rootEntityType entity type
   * @param sanitizedEntityName entity name
   * @throws KeeperException if there is an error while setting or creating the znode
   */
  def setOrCreateEntityConfigs(rootEntityType: String, sanitizedEntityName: String, config: Properties): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
   * Returns all the entities for a given entityType
   * @param entityType entity type
   * @return List of all entity names
   */
  def getAllEntitiesWithConfig(entityType: String): Seq[String] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Creates config change notification
   * @param sanitizedEntityPath  sanitizedEntityPath path to write
   * @throws KeeperException if there is an error while setting or creating the znode
   */
  def createConfigChangeNotification(sanitizedEntityPath: String): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
   * Gets all brokers in the cluster.
   * @return sequence of brokers in the cluster.
   */
  def getAllBrokersInCluster: Seq[Broker] = {
    throw new UnsupportedOperationException()
  }

  /**
    * Gets all brokers with broker epoch in the cluster.
    * @return map of broker to epoch in the cluster.
    */
  def getAllBrokerAndEpochsInCluster: Map[Broker, Long] = {
    throw new UnsupportedOperationException()
  }

  /**
    * Get a broker from ZK
    * @return an optional Broker
    */
  def getBroker(brokerId: Int): Option[Broker] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Gets the list of sorted broker Ids
   */
  def getSortedBrokerList: Seq[Int] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Gets all topics in the cluster.
   * @param registerWatch indicates if a watch must be registered or not
   * @return sequence of topics in the cluster.
   */
  def getAllTopicsInCluster(registerWatch: Boolean = false): Set[String] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Checks the topic existence
   * @param topicName the name of the topic to check
   * @return true if topic exists else false
   */
  def topicExists(topicName: String): Boolean = {
    throw new UnsupportedOperationException()
  }

  /**
   * Adds a topic ID to existing topic and replica assignments
   * @param topicIdReplicaAssignments the TopicIDReplicaAssignments to add a topic ID to
   * @return the updated TopicIdReplicaAssignments including the newly created topic IDs
   */
  def setTopicIds(topicIdReplicaAssignments: collection.Set[TopicIdReplicaAssignment],
                  expectedControllerEpochZkVersion: Int): Set[TopicIdReplicaAssignment] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Sets the topic znode with the given assignment.
   * @param topic the topic whose assignment is being set.
   * @param topicId unique topic ID for the topic if the version supports it
   * @param assignment the partition to replica mapping to set for the given topic
   * @param expectedControllerEpochZkVersion expected controller epoch zkVersion.
   * @return SetDataResponse
   */
  def setTopicAssignmentRaw(topic: String,
                            topicId: Option[Uuid],
                            assignment: collection.Map[TopicPartition, ReplicaAssignment],
                            expectedControllerEpochZkVersion: Int): SetDataResponse = {
    throw new UnsupportedOperationException()
  }

  /**
   * Sets the topic znode with the given assignment.
   * @param topic the topic whose assignment is being set.
   * @param topicId unique topic ID for the topic if the version supports it
   * @param assignment the partition to replica mapping to set for the given topic
   * @param expectedControllerEpochZkVersion expected controller epoch zkVersion.
   * @throws KeeperException if there is an error while setting assignment
   */
  def setTopicAssignment(topic: String,
                         topicId: Option[Uuid],
                         assignment: Map[TopicPartition, ReplicaAssignment],
                         expectedControllerEpochZkVersion: Int = ZkVersion.MatchAnyVersion): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
   * Create the topic znode with the given assignment.
   * @param topic the topic whose assignment is being set.
   * @param topicId unique topic ID for the topic if the version supports it
   * @param assignment the partition to replica mapping to set for the given topic
   * @throws KeeperException if there is an error while creating assignment
   */
  def createTopicAssignment(topic: String, topicId: Option[Uuid], assignment: Map[TopicPartition, Seq[Int]]): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
   * Gets the log dir event notifications as strings. These strings are the znode names and not the absolute znode path.
   * @return sequence of znode names and not the absolute znode path.
   */
  def getAllLogDirEventNotifications: Seq[String] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Reads each of the log dir event notifications associated with the given sequence numbers and extracts the broker ids.
   * @param sequenceNumbers the sequence numbers associated with the log dir event notifications.
   * @return broker ids associated with the given log dir event notifications.
   */
  def getBrokerIdsFromLogDirEvents(sequenceNumbers: Seq[String]): Seq[Int] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Deletes all log dir event notifications.
   * @param expectedControllerEpochZkVersion expected controller epoch zkVersion.
   */
  def deleteLogDirEventNotifications(expectedControllerEpochZkVersion: Int): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
   * Deletes the log dir event notifications associated with the given sequence numbers.
   * @param sequenceNumbers the sequence numbers associated with the log dir event notifications to be deleted.
   * @param expectedControllerEpochZkVersion expected controller epoch zkVersion.
   */
  def deleteLogDirEventNotifications(sequenceNumbers: Seq[String], expectedControllerEpochZkVersion: Int): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
   * Gets the topic IDs for the given topics.
   * @param topics the topics we wish to retrieve the Topic IDs for
   * @return the Topic IDs
   */
  def getTopicIdsForTopics(topics: Set[String]): Map[String, Uuid] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Gets the replica assignments for the given topics.
   * This function does not return information about which replicas are being added or removed from the assignment.
   * @param topics the topics whose partitions we wish to get the assignments for.
   * @return the replica assignment for each partition from the given topics.
   */
  def getReplicaAssignmentForTopics(topics: Set[String]): Map[TopicPartition, Seq[Int]] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Gets the TopicID and replica assignments for the given topics.
   * @param topics the topics whose partitions we wish to get the assignments for.
   * @return the TopicIdReplicaAssignment for each partition for the given topics.
   */
  def getReplicaAssignmentAndTopicIdForTopics(topics: Set[String]): Set[TopicIdReplicaAssignment] = {
    throw new UnsupportedOperationException()
  }

  /**
    * Gets the replica assignments for the given topics.
    * @param topics the topics whose partitions we wish to get the assignments for.
    * @return the full replica assignment for each partition from the given topics.
    */
  def getFullReplicaAssignmentForTopics(topics: Set[String]): Map[TopicPartition, ReplicaAssignment] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Gets partition the assignments for the given topics.
   * @param topics the topics whose partitions we wish to get the assignments for.
   * @return the partition assignment for each partition from the given topics.
   */
  def getPartitionAssignmentForTopics(topics: Set[String]): Map[String, Map[Int, ReplicaAssignment]] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Gets the partition numbers for the given topics
   * @param topics the topics whose partitions we wish to get.
   * @return the partition array for each topic from the given topics.
   */
  def getPartitionsForTopics(topics: Set[String]): Map[String, Seq[Int]] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Gets the partition count for a given topic
   * @param topic The topic to get partition count for.
   * @return  optional integer that is Some if the topic exists and None otherwise.
   */
  def getTopicPartitionCount(topic: String): Option[Int] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Gets the assigned replicas for a specific topic and partition
   * @param topicPartition TopicAndPartition to get assigned replicas for .
   * @return List of assigned replicas
   */
  def getReplicasForPartition(topicPartition: TopicPartition): Seq[Int] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Gets all partitions in the cluster
   * @return all partitions in the cluster
   */
  def getAllPartitions: Set[TopicPartition] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Gets the data and version at the given zk path
   * @param path zk node path
   * @return A tuple of 2 elements, where first element is zk node data as an array of bytes
   *         and second element is zk node version.
   *         returns (None, ZkVersion.UnknownVersion) if node doesn't exist and throws exception for any error
   */
  def getDataAndVersion(path: String): (Option[Array[Byte]], Int) = {
    throw new UnsupportedOperationException()
  }

  /**
   * Gets the data and Stat at the given zk path
   * @param path zk node path
   * @return A tuple of 2 elements, where first element is zk node data as an array of bytes
   *         and second element is zk node stats.
   *         returns (None, ZkStat.NoStat) if node doesn't exists and throws exception for any error
   */
  def getDataAndStat(path: String): (Option[Array[Byte]], Stat) = {
    throw new UnsupportedOperationException()
  }

  /**
   * Gets all the child nodes at a given zk node path
   * @param path the path to check
   * @return list of child node names
   */
  def getChildren(path : String): Seq[String] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Conditional update the persistent path data, return (true, newVersion) if it succeeds, otherwise (the path doesn't
   * exist, the current version is not the expected version, etc.) return (false, ZkVersion.UnknownVersion)
   *
   * When there is a ConnectionLossException during the conditional update, ZookeeperClient will retry the update and may fail
   * since the previous update may have succeeded (but the stored zkVersion no longer matches the expected one).
   * In this case, we will run the optionalChecker to further check if the previous write did indeed succeeded.
   */
  def conditionalUpdatePath(path: String, data: Array[Byte], expectVersion: Int,
                            optionalChecker: Option[(KafkaZkClient, String, Array[Byte]) => (Boolean,Int)] = None): (Boolean, Int) = {
    throw new UnsupportedOperationException()
  }

  /**
   * Creates the delete topic znode.
   * @param topicName topic name
   * @throws KeeperException if there is an error while setting or creating the znode
   */
  def createDeleteTopicPath(topicName: String): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
   * Checks if topic is marked for deletion
   * @param topic the name of the topic to check
   * @return true if topic is marked for deletion, else false
   */
  def isTopicMarkedForDeletion(topic: String): Boolean = {
    throw new UnsupportedOperationException()
  }

  /**
   * Get all topics marked for deletion.
   * @return sequence of topics marked for deletion.
   */
  def getTopicDeletions: Seq[String] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Remove the given topics from the topics marked for deletion.
   * @param topics the topics to remove.
   * @param expectedControllerEpochZkVersion expected controller epoch zkVersion.
   */
  def deleteTopicDeletions(topics: Seq[String], expectedControllerEpochZkVersion: Int): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
   * Returns all reassignments.
   * @return the reassignments for each partition.
   */
  def getPartitionReassignment: collection.Map[TopicPartition, Seq[Int]] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Sets or creates the partition reassignment znode with the given reassignment depending on whether it already
   * exists or not.
   *
   * @param reassignment the reassignment to set on the reassignment znode
   * @param expectedControllerEpochZkVersion expected controller epoch zkVersion.
   * @throws KeeperException if there is an error while setting or creating the znode
   * @deprecated Use the PartitionReassignment Kafka API instead
   */
  @Deprecated
  def setOrCreatePartitionReassignment(reassignment: collection.Map[TopicPartition, Seq[Int]], expectedControllerEpochZkVersion: Int): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
   * Creates the partition reassignment znode with the given reassignment.
   * @param reassignment the reassignment to set on the reassignment znode.
   * @throws KeeperException if there is an error while creating the znode.
   */
  def createPartitionReassignment(reassignment: Map[TopicPartition, Seq[Int]]): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
   * Deletes the partition reassignment znode.
   * @param expectedControllerEpochZkVersion expected controller epoch zkVersion.
   */
  def deletePartitionReassignment(expectedControllerEpochZkVersion: Int): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
   * Checks if reassign partitions is in progress.
   * @return true if reassign partitions is in progress, else false.
   */
  def reassignPartitionsInProgress: Boolean = {
    throw new UnsupportedOperationException()
  }

  /**
   * Gets topic partition states for the given partitions.
   * @param partitions the partitions for which we want to get states.
   * @return map containing LeaderIsrAndControllerEpoch of each partition for we were able to lookup the partition state.
   */
  def getTopicPartitionStates(partitions: Seq[TopicPartition]): Map[TopicPartition, LeaderIsrAndControllerEpoch] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Gets topic partition state for the given partition.
   * @param partition the partition for which we want to get state.
   * @return LeaderIsrAndControllerEpoch of the partition state if exists, else None
   */
  def getTopicPartitionState(partition: TopicPartition): Option[LeaderIsrAndControllerEpoch] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Gets the leader for a given partition
   * @param partition The partition for which we want to get leader.
   * @return optional integer if the leader exists and None otherwise.
   */
  def getLeaderForPartition(partition: TopicPartition): Option[Int] =
    throw new UnsupportedOperationException()

  /**
   * Gets the in-sync replicas (ISR) for a specific topicPartition
   * @param partition The partition for which we want to get ISR.
   * @return optional ISR if exists and None otherwise
   */
  def getInSyncReplicasForPartition(partition: TopicPartition): Option[Seq[Int]] =
    throw new UnsupportedOperationException()


  /**
   * Gets the leader epoch for a specific topicPartition
   * @param partition The partition for which we want to get the leader epoch
   * @return optional integer if the leader exists and None otherwise
   */
  def getEpochForPartition(partition: TopicPartition): Option[Int] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Gets the isr change notifications as strings. These strings are the znode names and not the absolute znode path.
   * @return sequence of znode names and not the absolute znode path.
   */
  def getAllIsrChangeNotifications: Seq[String] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Reads each of the isr change notifications associated with the given sequence numbers and extracts the partitions.
   * @param sequenceNumbers the sequence numbers associated with the isr change notifications.
   * @return partitions associated with the given isr change notifications.
   */
  def getPartitionsFromIsrChangeNotifications(sequenceNumbers: Seq[String]): Seq[TopicPartition] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Deletes all isr change notifications.
   * @param expectedControllerEpochZkVersion expected controller epoch zkVersion.
   */
  def deleteIsrChangeNotifications(expectedControllerEpochZkVersion: Int): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
   * Deletes the isr change notifications associated with the given sequence numbers.
   * @param sequenceNumbers the sequence numbers associated with the isr change notifications to be deleted.
   * @param expectedControllerEpochZkVersion expected controller epoch zkVersion.
   */
  def deleteIsrChangeNotifications(sequenceNumbers: Seq[String], expectedControllerEpochZkVersion: Int): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
   * Creates preferred replica election znode with partitions undergoing election
   * @param partitions the set of partitions
   * @throws KeeperException if there is an error while creating the znode
   */
  def createPreferredReplicaElection(partitions: Set[TopicPartition]): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
   * Gets the partitions marked for preferred replica election.
   * @return sequence of partitions.
   */
  def getPreferredReplicaElection: Set[TopicPartition] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Deletes the preferred replica election znode.
   * @param expectedControllerEpochZkVersion expected controller epoch zkVersion.
   */
  def deletePreferredReplicaElection(expectedControllerEpochZkVersion: Int): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
   * Gets the controller id.
   * @return optional integer that is Some if the controller znode exists and can be parsed and None otherwise.
   */
  def getControllerId: Option[Int] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Deletes the controller znode.
   * @param expectedControllerEpochZkVersion expected controller epoch zkVersion.
   */
  def deleteController(expectedControllerEpochZkVersion: Int): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
   * Gets the controller epoch.
   * @return optional (Int, Stat) that is Some if the controller epoch path exists and None otherwise.
   */
  def getControllerEpoch: Option[(Int, Stat)] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Recursively deletes the topic znode.
   * @param topic the topic whose topic znode we wish to delete.
   * @param expectedControllerEpochZkVersion expected controller epoch zkVersion.
   */
  def deleteTopicZNode(topic: String, expectedControllerEpochZkVersion: Int): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
   * Deletes the topic configs for the given topics.
   * @param topics the topics whose configs we wish to delete.
   * @param expectedControllerEpochZkVersion expected controller epoch zkVersion.
   */
  def deleteTopicConfigs(topics: Seq[String], expectedControllerEpochZkVersion: Int): Unit = {
    throw new UnsupportedOperationException()
  }

  def propagateLogDirEvent(brokerId: Int): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
   * Deletes the zk node recursively
   * @param path path to delete
   * @param expectedControllerEpochZkVersion expected controller epoch zkVersion.
   * @param recursiveDelete enable recursive delete
   * @return KeeperException if there is an error while deleting the path
   */
  def deletePath(path: String, expectedControllerEpochZkVersion: Int = ZkVersion.MatchAnyVersion, recursiveDelete: Boolean = true): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
   * Creates the required zk nodes for Delegation Token storage
   */
  def createDelegationTokenPaths(): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
   * Creates Delegation Token change notification message
   * @param tokenId token Id
   */
  def createTokenChangeNotification(tokenId: String): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
   * Sets or creates token info znode with the given token details depending on whether it already
   * exists or not.
   *
   * @param token the token to set on the token znode
   * @throws KeeperException if there is an error while setting or creating the znode
   */
  def setOrCreateDelegationToken(token: DelegationToken): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
   * Gets the Delegation Token Info
   * @return optional TokenInfo that is Some if the token znode exists and can be parsed and None otherwise.
   */
  def getDelegationTokenInfo(delegationTokenId: String): Option[TokenInformation] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Deletes the given Delegation token node
   * @param delegationTokenId
   * @return delete status
   */
  def deleteDelegationToken(delegationTokenId: String): Boolean = {
    throw new UnsupportedOperationException()
  }

  /**
   * This registers a ZNodeChangeHandler and attempts to register a watcher with an ExistsRequest, which allows data
   * watcher registrations on paths which might not even exist.
   *
   * @param zNodeChangeHandler
   * @return `true` if the path exists or `false` if it does not
   * @throws KeeperException if an error is returned by ZooKeeper
   */
  def registerZNodeChangeHandlerAndCheckExistence(zNodeChangeHandler: ZNodeChangeHandler): Boolean = {
    throw new UnsupportedOperationException()
  }

  /**
   * See ZooKeeperClient.registerZNodeChangeHandler
   * @param zNodeChangeHandler
   */
  def registerZNodeChangeHandler(zNodeChangeHandler: ZNodeChangeHandler): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
   * See ZooKeeperClient.unregisterZNodeChangeHandler
   * @param path
   */
  def unregisterZNodeChangeHandler(path: String): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
   * See ZooKeeperClient.registerZNodeChildChangeHandler
   * @param zNodeChildChangeHandler
   */
  def registerZNodeChildChangeHandler(zNodeChildChangeHandler: ZNodeChildChangeHandler): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
   * See ZooKeeperClient.unregisterZNodeChildChangeHandler
   * @param path
   */
  def unregisterZNodeChildChangeHandler(path: String): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
   *
   * @param stateChangeHandler
   */
  def registerStateChangeHandler(stateChangeHandler: StateChangeHandler): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
   *
   * @param name
   */
  def unregisterStateChangeHandler(name: String): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
   * Close the underlying ZooKeeperClient.
   */
  def close(): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
   * Get the committed offset for a topic partition and group
   * @param group the group we wish to get offset for
   * @param topicPartition the topic partition we wish to get the offset for
   * @return optional long that is Some if there was an offset committed for topic partition, group and None otherwise.
   */
  def getConsumerOffset(group: String, topicPartition: TopicPartition): Option[Long] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Set the committed offset for a topic partition and group
   * @param group the group whose offset is being set
   * @param topicPartition the topic partition whose offset is being set
   * @param offset the offset value
   */
  def setOrCreateConsumerOffset(group: String, topicPartition: TopicPartition, offset: Long): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
    * Get the cluster id.
    * @return optional cluster id in String.
    */
  def getClusterId: Option[String] = {
    throw new UnsupportedOperationException()
  }

  /**
    * Return the ACLs of the node of the given path
    * @param path the given path for the node
    * @return the ACL array of the given node.
    */
  def getAcl(path: String): Seq[ACL] = {
    throw new UnsupportedOperationException()
  }

  /**
    * sets the ACLs to the node of the given path
    * @param path the given path for the node
    * @param acl the given acl for the node
    */
  def setAcl(path: String, acl: Seq[ACL]): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
    * Create the cluster Id. If the cluster id already exists, return the current cluster id.
    * @return  cluster id
    */
  def createOrGetClusterId(proposedClusterId: String): String = {
    throw new UnsupportedOperationException()
  }

  /**
    * Generate a broker id by updating the broker sequence id path in ZK and return the version of the path.
    * The version is incremented by one on every update starting from 1.
    * @return sequence number as the broker id
    */
  def generateBrokerSequenceId(): Int = {
    throw new UnsupportedOperationException()
  }

  /**
    * Pre-create top level paths in ZK if needed.
    */
  def createTopLevelPaths(): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
    * Make sure a persistent path exists in ZK.
    * @param path
    */
  def makeSurePersistentPathExists(path: String): Unit = {
    throw new UnsupportedOperationException()
  }

  def createFeatureZNode(nodeContents: FeatureZNode): Unit = {
    throw new UnsupportedOperationException()
  }

  def updateFeatureZNode(nodeContents: FeatureZNode): Int = {
    throw new UnsupportedOperationException()
  }

  /**
   * Deletes the given zk path recursively
   * @param path
   * @param expectedControllerEpochZkVersion expected controller epoch zkVersion.
   * @return true if path gets deleted successfully, false if root path doesn't exist
   * @throws KeeperException if there is an error while deleting the znodes
   */
  def deleteRecursive(path: String, expectedControllerEpochZkVersion: Int = ZkVersion.MatchAnyVersion): Boolean = {
    throw new UnsupportedOperationException()
  }

  def pathExists(path: String): Boolean = {
    throw new UnsupportedOperationException()
  }

  def defaultAcls(path: String): Seq[ACL] = {
    throw new UnsupportedOperationException()
  }
}

object KafkaZkClient {
  /**
   * @param finishedPartitions Partitions that finished either in successfully
   *                      updated partition states or failed with an exception.
   * @param partitionsToRetry The partitions that we should retry due to a zookeeper BADVERSION conflict. Version conflicts
   *                      can occur if the partition leader updated partition state while the controller attempted to
   *                      update partition state.
   */
  case class UpdateLeaderAndIsrResult(
    finishedPartitions: Map[TopicPartition, Either[Exception, LeaderAndIsr]],
    partitionsToRetry: Seq[TopicPartition]
  )
}
