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

import kafka.server.metadata.{ConfigRepository, KRaftMetadataCache}
import org.apache.kafka.admin.BrokerMetadata
import org.apache.kafka.common.message.{DescribeClientQuotasRequestData, DescribeClientQuotasResponseData, DescribeTopicPartitionsResponseData, DescribeUserScramCredentialsRequestData, DescribeUserScramCredentialsResponseData, MetadataResponseData}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.{Cluster, Node, TopicPartition, Uuid}
import org.apache.kafka.metadata.LeaderAndIsr
import org.apache.kafka.server.common.{FinalizedFeatures, KRaftVersion, MetadataVersion}

import java.util
import java.util.function.Supplier
import scala.collection._

trait MetadataCache extends ConfigRepository {
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

  def getAliveBrokerEpoch(brokerId: Int): Option[Long]

  def isBrokerFenced(brokerId: Int): Boolean

  def isBrokerShuttingDown(brokerId: Int): Boolean

  def getTopicId(topicName: String): Uuid

  def getTopicName(topicId: Uuid): Option[String]

  def getAliveBrokerNode(brokerId: Int, listenerName: ListenerName): Option[Node]

  def getAliveBrokerNodes(listenerName: ListenerName): Iterable[Node]

  def getBrokerNodes(listenerName: ListenerName): Iterable[Node]

  def getLeaderAndIsr(topic: String, partitionId: Int): Option[LeaderAndIsr]

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

  def getClusterMetadata(clusterId: String, listenerName: ListenerName): Cluster

  def contains(topic: String): Boolean

  def contains(tp: TopicPartition): Boolean

  def metadataVersion(): MetadataVersion

  def getRandomAliveBrokerId: Option[Int]

  def features(): FinalizedFeatures

  def describeClientQuotas(request: DescribeClientQuotasRequestData): DescribeClientQuotasResponseData

  def describeScramCredentials(request: DescribeUserScramCredentialsRequestData): DescribeUserScramCredentialsResponseData

  /**
   * Get the topic metadata for the given topics.
   *
   * The quota is used to limit the number of partitions to return. The NextTopicPartition field points to the first
   * partition can't be returned due the limit.
   * If a topic can't return any partition due to quota limit reached, this topic will not be included in the response.
   *
   * Note, the topics should be sorted in alphabetical order. The topics in the DescribeTopicPartitionsResponseData
   * will also be sorted in alphabetical order.
   *
   * @param topics                        The iterator of topics and their corresponding first partition id to fetch.
   * @param listenerName                  The listener name.
   * @param topicPartitionStartIndex      The start partition index for the first topic
   * @param maximumNumberOfPartitions     The max number of partitions to return.
   * @param ignoreTopicsWithExceptions    Whether ignore the topics with exception.
   */
  def describeTopicResponse(
    topics: Iterator[String],
    listenerName: ListenerName,
    topicPartitionStartIndex: String => Int,
    maximumNumberOfPartitions: Int,
    ignoreTopicsWithExceptions: Boolean
  ): DescribeTopicPartitionsResponseData
}

object MetadataCache {
  def kRaftMetadataCache(
    brokerId: Int,
    kraftVersionSupplier: Supplier[KRaftVersion]
  ): KRaftMetadataCache = {
    new KRaftMetadataCache(brokerId, kraftVersionSupplier)
  }
}
