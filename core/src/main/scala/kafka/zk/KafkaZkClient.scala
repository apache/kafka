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
import kafka.controller.ReplicaAssignment
import kafka.utils.Logging
import org.apache.kafka.common.security.token.delegation.{DelegationToken, TokenInformation}
import org.apache.kafka.common.{TopicPartition, Uuid}

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

  /**
   * Get entity configs for a given entity name
   * @param rootEntityType entity type
   * @param sanitizedEntityName entity name
   * @return The successfully gathered log configs
   */
  def getEntityConfigs(rootEntityType: String, sanitizedEntityName: String): Properties = {
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
    * Gets the replica assignments for the given topics.
    * @param topics the topics whose partitions we wish to get the assignments for.
    * @return the full replica assignment for each partition from the given topics.
    */
  def getFullReplicaAssignmentForTopics(topics: Set[String]): Map[TopicPartition, ReplicaAssignment] = {
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
   * Gets all partitions in the cluster
   * @return all partitions in the cluster
   */
  def getAllPartitions: Set[TopicPartition] = {
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
   * Gets the controller id.
   * @return optional integer that is Some if the controller znode exists and can be parsed and None otherwise.
   */
  def getControllerId: Option[Int] = {
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
   * Close the underlying ZooKeeperClient.
   */
  def close(): Unit = {
    throw new UnsupportedOperationException()
  }
}
