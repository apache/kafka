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

package kafka.server

import kafka.server.metadata.{KRaftMetadataCache, ZkMetadataCache}
import org.apache.kafka.admin.BrokerMetadata
import org.apache.kafka.common.message.{MetadataResponseData, UpdateMetadataRequestData}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.{Cluster, Node, TopicPartition, Uuid}
import org.apache.kafka.server.BrokerFeatures
import org.apache.kafka.server.common.{FinalizedFeatures, KRaftVersion, MetadataVersion}

import java.util
import java.util.function.Supplier
import scala.collection._

/**
 * Used to represent the controller id cached in the metadata cache of the broker. This trait is
 * extended to represent if the controller is KRaft controller or Zk controller.
 */
sealed trait CachedControllerId {
  val id: Int
}

case class ZkCachedControllerId(id: Int) extends CachedControllerId
case class KRaftCachedControllerId(id: Int) extends CachedControllerId

trait MetadataCache {
  /**
   * Return topic metadata for a given set of topics and listener. See KafkaApis#handleTopicMetadataRequest for details
   * on the use of the two boolean flags.
   *
   * @param topics                      The set of topics.
   * @param listenerName                The listener name.
   * @param errorUnavailableEndpoints   If true, we return an error on unavailable brokers. This is used to support
   *                                    MetadataResponse version 0.
   * @param errorUnavailableListeners   If true, return LEADER_NOT_AVAILABLE if the listener is not found on the leader.
   *                                    This is used for MetadataResponse versions 0-5.
   * @return                            A collection of topic metadata.
   */
  def getTopicMetadata(
    topics: collection.Set[String],
    listenerName: ListenerName,
    errorUnavailableEndpoints: Boolean = false,
    errorUnavailableListeners: Boolean = false): collection.Seq[MetadataResponseData.MetadataResponseTopic]

  def getAllTopics(): collection.Set[String]

  def getTopicPartitions(topicName: String): collection.Set[TopicPartition]

  def hasAliveBroker(brokerId: Int): Boolean

  def getAliveBrokers(): Iterable[BrokerMetadata]

  def getTopicId(topicName: String): Uuid

  def getTopicName(topicId: Uuid): Option[String]

  def getAliveBrokerNode(brokerId: Int, listenerName: ListenerName): Option[Node]

  def getAliveBrokerNodes(listenerName: ListenerName): Iterable[Node]

  def getBrokerNodes(listenerName: ListenerName): Iterable[Node]

  def getPartitionInfo(topic: String, partitionId: Int): Option[UpdateMetadataRequestData.UpdateMetadataPartitionState]

  /**
   * Return the number of partitions in the given topic, or None if the given topic does not exist.
   */
  def numPartitions(topic: String): Option[Int]

  def topicNamesToIds(): util.Map[String, Uuid]

  def topicIdsToNames(): util.Map[Uuid, String]

  def topicIdInfo(): (util.Map[String, Uuid], util.Map[Uuid, String])

  /**
   * Get a partition leader's endpoint
   *
   * @return  If the leader is known, and the listener name is available, return Some(node). If the leader is known,
   *          but the listener is unavailable, return Some(Node.NO_NODE). Otherwise, if the leader is not known,
   *          return None
   */
  def getPartitionLeaderEndpoint(topic: String, partitionId: Int, listenerName: ListenerName): Option[Node]

  def getPartitionReplicaEndpoints(tp: TopicPartition, listenerName: ListenerName): Map[Int, Node]

  def getControllerId: Option[CachedControllerId]

  def getClusterMetadata(clusterId: String, listenerName: ListenerName): Cluster

  def contains(topic: String): Boolean

  def contains(tp: TopicPartition): Boolean

  def metadataVersion(): MetadataVersion

  def getRandomAliveBrokerId: Option[Int]

  def features(): FinalizedFeatures
}

object MetadataCache {
  def zkMetadataCache(brokerId: Int,
                      metadataVersion: MetadataVersion,
                      brokerFeatures: BrokerFeatures = BrokerFeatures.createEmpty())
  : ZkMetadataCache = {
    new ZkMetadataCache(brokerId, metadataVersion, brokerFeatures)
  }

  def kRaftMetadataCache(
    brokerId: Int,
    kraftVersionSupplier: Supplier[KRaftVersion]
  ): KRaftMetadataCache = {
    new KRaftMetadataCache(brokerId, kraftVersionSupplier)
  }
}
