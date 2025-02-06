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

import kafka.server.metadata.KRaftMetadataCache
import org.apache.kafka.admin.BrokerMetadata
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.{DescribeClientQuotasRequestData, DescribeClientQuotasResponseData, DescribeTopicPartitionsResponseData, DescribeUserScramCredentialsRequestData, DescribeUserScramCredentialsResponseData, MetadataResponseData}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.{Cluster, Node, PartitionInfo, TopicPartition, Uuid}
import org.apache.kafka.image.MetadataImage
import org.apache.kafka.metadata.{BrokerRegistration, ConfigRepository, LeaderAndIsr, PartitionRegistration}
import org.apache.kafka.server.common.{FinalizedFeatures, KRaftVersion, MetadataVersion}

import java.util
import java.util.Collections
import java.util.concurrent.ThreadLocalRandom
import java.util.function.Supplier
import scala.collection._
import scala.jdk.CollectionConverters.CollectionHasAsScala

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

  def toCluster(clusterId: String, image: MetadataImage): Cluster = {
    val brokerToNodes = new util.HashMap[Integer, util.List[Node]]
    image.cluster().brokers()
      .values().stream()
      .filter(broker => !broker.fenced())
      .forEach { broker => brokerToNodes.put(broker.id(), broker.nodes()) }

    def getNodes(id: Int): util.List[Node] = brokerToNodes.get(id)

    val partitionInfos = new util.ArrayList[PartitionInfo]
    val internalTopics = new util.HashSet[String]

    def toArray(replicas: Array[Int]): Array[Node] = {
      util.Arrays.stream(replicas)
        .mapToObj(replica => getNodes(replica))
        .flatMap(replica => replica.stream()).toArray(size => new Array[Node](size))
    }

    val topicImages = image.topics().topicsByName().values()
    if (topicImages != null) {
      topicImages.forEach { topic =>
        topic.partitions().forEach { (key, value) =>
          val partitionId = key
          val partition = value
          val nodes = getNodes(partition.leader)
          if (nodes != null) {
            nodes.forEach(node => {
              partitionInfos.add(new PartitionInfo(topic.name(),
                partitionId,
                node,
                toArray(partition.replicas),
                toArray(partition.isr),
                getOfflineReplicas(image, partition).stream()
                  .map(replica => getNodes(replica))
                  .flatMap(replica => replica.stream()).toArray(size => new Array[Node](size))))
            })
            if (Topic.isInternal(topic.name())) {
              internalTopics.add(topic.name())
            }
          }
        }
      }
    }

    val controllerNode = getNodes(getRandomAliveBroker(image).getOrElse(-1)) match {
      case null => Node.noNode()
      case nodes => nodes.get(0)
    }
    // Note: the constructor of Cluster does not allow us to reference unregistered nodes.
    // So, for example, if partition foo-0 has replicas [1, 2] but broker 2 is not
    // registered, we pass its replicas as [1, -1]. This doesn't make a lot of sense, but
    // we are duplicating the behavior of ZkMetadataCache, for now.
    new Cluster(clusterId, brokerToNodes.values().stream().flatMap(n => n.stream()).collect(util.stream.Collectors.toList()),
      partitionInfos, Collections.emptySet(), internalTopics, controllerNode)
  }

  private def getOfflineReplicas(image: MetadataImage,
                                 partition: PartitionRegistration,
                                 listenerName: ListenerName = null): util.List[Integer] = {
    val offlineReplicas = new util.ArrayList[Integer](0)
    for (brokerId <- partition.replicas) {
      Option(image.cluster().broker(brokerId)) match {
        case None => offlineReplicas.add(brokerId)
        case Some(broker) => if (listenerName == null || isReplicaOffline(partition, listenerName, broker)) {
          offlineReplicas.add(brokerId)
        }
      }
    }
    offlineReplicas
  }

  private def isReplicaOffline(partition: PartitionRegistration, listenerName: ListenerName, broker: BrokerRegistration) =
    broker.fenced() || !broker.listeners().containsKey(listenerName.value()) || isReplicaInOfflineDir(broker, partition)

  private def isReplicaInOfflineDir(broker: BrokerRegistration, partition: PartitionRegistration): Boolean =
    !broker.hasOnlineDir(partition.directory(broker.id()))

  private def getRandomAliveBroker(image: MetadataImage): Option[Int] = {
    val aliveBrokers = getAliveBrokers(image).toList
    if (aliveBrokers.isEmpty) {
      None
    } else {
      Some(aliveBrokers(ThreadLocalRandom.current().nextInt(aliveBrokers.size)).id)
    }
  }

  private def getAliveBrokers(image: MetadataImage): Iterable[BrokerMetadata] = {
    image.cluster().brokers().values().asScala.filterNot(_.fenced()).
      map(b => new BrokerMetadata(b.id, b.rack))
  }
}
