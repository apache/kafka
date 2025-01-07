/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server.metadata

import kafka.server.{CachedControllerId, KRaftCachedControllerId, MetadataCache}
import kafka.utils.Logging
import org.apache.kafka.admin.BrokerMetadata
import org.apache.kafka.common._
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.{Cursor, DescribeTopicPartitionsResponsePartition, DescribeTopicPartitionsResponseTopic}
import org.apache.kafka.common.message.MetadataResponseData.{MetadataResponsePartition, MetadataResponseTopic}
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataPartitionState
import org.apache.kafka.common.message._
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.image.MetadataImage
import org.apache.kafka.metadata.{BrokerRegistration, PartitionRegistration, Replicas}
import org.apache.kafka.server.common.{FinalizedFeatures, KRaftVersion, MetadataVersion}

import java.util
import java.util.concurrent.ThreadLocalRandom
import java.util.function.Supplier
import java.util.{Collections, Properties}
import scala.collection.mutable.ListBuffer
import scala.collection.{Map, Seq, Set, mutable}
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters.RichOptional
import scala.util.control.Breaks._


class KRaftMetadataCache(
  val brokerId: Int,
  val kraftVersionSupplier: Supplier[KRaftVersion]
) extends MetadataCache with Logging with ConfigRepository {
  this.logIdent = s"[MetadataCache brokerId=$brokerId] "

  // This is the cache state. Every MetadataImage instance is immutable, and updates
  // replace this value with a completely new one. This means reads (which are not under
  // any lock) need to grab the value of this variable once, and retain that read copy for
  // the duration of their operation. Multiple reads of this value risk getting different
  // image values.
  @volatile private var _currentImage: MetadataImage = MetadataImage.EMPTY

  // This method is the main hotspot when it comes to the performance of metadata requests,
  // we should be careful about adding additional logic here.
  // filterUnavailableEndpoints exists to support v0 MetadataResponses
  private def maybeFilterAliveReplicas(image: MetadataImage,
                                       brokers: Array[Int],
                                       listenerName: ListenerName,
                                       filterUnavailableEndpoints: Boolean): java.util.List[Integer] = {
    if (!filterUnavailableEndpoints) {
      Replicas.toList(brokers)
    } else {
      val res = new util.ArrayList[Integer](brokers.length)
      for (brokerId <- brokers) {
        Option(image.cluster().broker(brokerId)).foreach { b =>
          if (!b.fenced() && b.listeners().containsKey(listenerName.value())) {
            res.add(brokerId)
          }
        }
      }
      res
    }
  }

  def currentImage(): MetadataImage = _currentImage

  // errorUnavailableEndpoints exists to support v0 MetadataResponses
  // If errorUnavailableListeners=true, return LISTENER_NOT_FOUND if listener is missing on the broker.
  // Otherwise, return LEADER_NOT_AVAILABLE for broker unavailable and missing listener (Metadata response v5 and below).
  private def getPartitionMetadata(image: MetadataImage, topicName: String, listenerName: ListenerName, errorUnavailableEndpoints: Boolean,
                                   errorUnavailableListeners: Boolean): Option[Iterator[MetadataResponsePartition]] = {
    Option(image.topics().getTopic(topicName)) match {
      case None => None
      case Some(topic) => Some(topic.partitions().entrySet().asScala.map { entry =>
        val partitionId = entry.getKey
        val partition = entry.getValue
        val filteredReplicas = maybeFilterAliveReplicas(image, partition.replicas,
          listenerName, errorUnavailableEndpoints)
        val filteredIsr = maybeFilterAliveReplicas(image, partition.isr, listenerName,
          errorUnavailableEndpoints)
        val offlineReplicas = getOfflineReplicas(image, partition, listenerName)
        val maybeLeader = getAliveEndpoint(image, partition.leader, listenerName)
        maybeLeader match {
          case None =>
            val error = if (!image.cluster().brokers.containsKey(partition.leader)) {
              debug(s"Error while fetching metadata for $topicName-$partitionId: leader not available")
              Errors.LEADER_NOT_AVAILABLE
            } else {
              debug(s"Error while fetching metadata for $topicName-$partitionId: listener $listenerName " +
                s"not found on leader ${partition.leader}")
              if (errorUnavailableListeners) Errors.LISTENER_NOT_FOUND else Errors.LEADER_NOT_AVAILABLE
            }
            new MetadataResponsePartition()
              .setErrorCode(error.code)
              .setPartitionIndex(partitionId)
              .setLeaderId(MetadataResponse.NO_LEADER_ID)
              .setLeaderEpoch(partition.leaderEpoch)
              .setReplicaNodes(filteredReplicas)
              .setIsrNodes(filteredIsr)
              .setOfflineReplicas(offlineReplicas)
          case Some(leader) =>
            val error = if (filteredReplicas.size < partition.replicas.length) {
              debug(s"Error while fetching metadata for $topicName-$partitionId: replica information not available for " +
                s"following brokers ${partition.replicas.filterNot(filteredReplicas.contains).mkString(",")}")
              Errors.REPLICA_NOT_AVAILABLE
            } else if (filteredIsr.size < partition.isr.length) {
              debug(s"Error while fetching metadata for $topicName-$partitionId: in sync replica information not available for " +
                s"following brokers ${partition.isr.filterNot(filteredIsr.contains).mkString(",")}")
              Errors.REPLICA_NOT_AVAILABLE
            } else {
              Errors.NONE
            }

            new MetadataResponsePartition()
              .setErrorCode(error.code)
              .setPartitionIndex(partitionId)
              .setLeaderId(leader.id())
              .setLeaderEpoch(partition.leaderEpoch)
              .setReplicaNodes(filteredReplicas)
              .setIsrNodes(filteredIsr)
              .setOfflineReplicas(offlineReplicas)
        }
      }.iterator)
    }
  }

  /**
   * Return topic partition metadata for the given topic, listener and index range. Also, return the next partition
   * index that is not included in the result.
   *
   * @param image                       The metadata image
   * @param topicName                   The name of the topic.
   * @param listenerName                The listener name.
   * @param startIndex                  The smallest index of the partitions to be included in the result.
   * @param upperIndex                  The upper limit of the index of the partitions to be included in the result.
   *                                    Note that, the upper index can be larger than the largest partition index in
   *                                    this topic.
   * @return                            A collection of topic partition metadata and next partition index (-1 means
   *                                    no next partition).
   */
  private def getPartitionMetadataForDescribeTopicResponse(
    image: MetadataImage,
    topicName: String,
    listenerName: ListenerName,
    startIndex: Int,
    maxCount: Int
  ): (Option[List[DescribeTopicPartitionsResponsePartition]], Int) = {
    Option(image.topics().getTopic(topicName)) match {
      case None => (None, -1)
      case Some(topic) => {
        val result = new ListBuffer[DescribeTopicPartitionsResponsePartition]()
        val partitions = topic.partitions().keySet()
        val upperIndex = topic.partitions().size().min(startIndex + maxCount)
        val nextIndex = if (upperIndex < partitions.size()) upperIndex else -1
        for (partitionId <- startIndex until upperIndex) {
          topic.partitions().get(partitionId) match {
            case partition : PartitionRegistration => {
              val filteredReplicas = maybeFilterAliveReplicas(image, partition.replicas,
                listenerName, filterUnavailableEndpoints = false)
              val filteredIsr = maybeFilterAliveReplicas(image, partition.isr, listenerName, filterUnavailableEndpoints = false)
              val offlineReplicas = getOfflineReplicas(image, partition, listenerName)
              val maybeLeader = getAliveEndpoint(image, partition.leader, listenerName)
              maybeLeader match {
                case None =>
                  result.append(new DescribeTopicPartitionsResponsePartition()
                    .setPartitionIndex(partitionId)
                    .setLeaderId(MetadataResponse.NO_LEADER_ID)
                    .setLeaderEpoch(partition.leaderEpoch)
                    .setReplicaNodes(filteredReplicas)
                    .setIsrNodes(filteredIsr)
                    .setOfflineReplicas(offlineReplicas)
                    .setEligibleLeaderReplicas(Replicas.toList(partition.elr))
                    .setLastKnownElr(Replicas.toList(partition.lastKnownElr)))
                case Some(leader) =>
                  result.append(new DescribeTopicPartitionsResponsePartition()
                    .setPartitionIndex(partitionId)
                    .setLeaderId(leader.id())
                    .setLeaderEpoch(partition.leaderEpoch)
                    .setReplicaNodes(filteredReplicas)
                    .setIsrNodes(filteredIsr)
                    .setOfflineReplicas(offlineReplicas)
                    .setEligibleLeaderReplicas(Replicas.toList(partition.elr))
                    .setLastKnownElr(Replicas.toList(partition.lastKnownElr)))
              }
            }
            case _ => warn(s"The partition $partitionId does not exist for $topicName")
          }
        }
        (Some(result.toList), nextIndex)
      }
    }
  }

  private def getOfflineReplicas(image: MetadataImage,
                                 partition: PartitionRegistration,
                                 listenerName: ListenerName): util.List[Integer] = {
    val offlineReplicas = new util.ArrayList[Integer](0)
    for (brokerId <- partition.replicas) {
      Option(image.cluster().broker(brokerId)) match {
        case None => offlineReplicas.add(brokerId)
        case Some(broker) => if (isReplicaOffline(partition, listenerName, broker)) {
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

  /**
   * Get the endpoint matching the provided listener if the broker is alive. Note that listeners can
   * be added dynamically, so a broker with a missing listener could be a transient error.
   *
   * @return None if broker is not alive or if the broker does not have a listener named `listenerName`.
   */
  private def getAliveEndpoint(image: MetadataImage, id: Int, listenerName: ListenerName): Option[Node] = {
    Option(image.cluster().broker(id)).flatMap(_.node(listenerName.value()).toScala)
  }

  // errorUnavailableEndpoints exists to support v0 MetadataResponses
  override def getTopicMetadata(topics: Set[String],
                                listenerName: ListenerName,
                                errorUnavailableEndpoints: Boolean = false,
                                errorUnavailableListeners: Boolean = false): Seq[MetadataResponseTopic] = {
    val image = _currentImage
    topics.toSeq.flatMap { topic =>
      getPartitionMetadata(image, topic, listenerName, errorUnavailableEndpoints, errorUnavailableListeners).map { partitionMetadata =>
        new MetadataResponseTopic()
          .setErrorCode(Errors.NONE.code)
          .setName(topic)
          .setTopicId(Option(image.topics().getTopic(topic).id()).getOrElse(Uuid.ZERO_UUID))
          .setIsInternal(Topic.isInternal(topic))
          .setPartitions(partitionMetadata.toBuffer.asJava)
      }
    }
  }

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
   * @param firstTopicPartitionStartIndex The start partition index for the first topic
   * @param maximumNumberOfPartitions     The max number of partitions to return.
   * @param ignoreTopicsWithExceptions    Whether ignore the topics with exception.
   */
  def getTopicMetadataForDescribeTopicResponse(
    topics: Iterator[String],
    listenerName: ListenerName,
    topicPartitionStartIndex: String => Int,
    maximumNumberOfPartitions: Int,
    ignoreTopicsWithExceptions: Boolean
  ): DescribeTopicPartitionsResponseData = {
    val image = _currentImage
    var remaining = maximumNumberOfPartitions
    val result = new DescribeTopicPartitionsResponseData()
    breakable {
      topics.foreach { topicName =>
        if (remaining > 0) {
          val (partitionResponse, nextPartition) =
            getPartitionMetadataForDescribeTopicResponse(
              image, topicName, listenerName, topicPartitionStartIndex(topicName), remaining
            )
          partitionResponse.map(partitions => {
            val response = new DescribeTopicPartitionsResponseTopic()
              .setErrorCode(Errors.NONE.code)
              .setName(topicName)
              .setTopicId(Option(image.topics().getTopic(topicName).id()).getOrElse(Uuid.ZERO_UUID))
              .setIsInternal(Topic.isInternal(topicName))
              .setPartitions(partitions.asJava)
            result.topics().add(response)

            if (nextPartition != -1) {
              result.setNextCursor(new Cursor()
                .setTopicName(topicName)
                .setPartitionIndex(nextPartition)
              )
              break()
            }
            remaining -= partitions.size
          })

          if (!ignoreTopicsWithExceptions && partitionResponse.isEmpty) {
            val error = try {
              Topic.validate(topicName)
              Errors.UNKNOWN_TOPIC_OR_PARTITION
            } catch {
              case _: InvalidTopicException =>
                Errors.INVALID_TOPIC_EXCEPTION
            }
            result.topics().add(new DescribeTopicPartitionsResponseTopic()
              .setErrorCode(error.code())
              .setName(topicName)
              .setTopicId(getTopicId(topicName))
              .setIsInternal(Topic.isInternal(topicName)))
          }
        } else if (remaining == 0) {
          // The cursor should point to the beginning of the current topic. All the partitions in the previous topic
          // should be fulfilled. Note that, if a partition is pointed in the NextTopicPartition, it does not mean
          // this topic exists.
          result.setNextCursor(new Cursor()
            .setTopicName(topicName)
            .setPartitionIndex(0))
          break()
        }
      }
    }
    result
  }

  override def getAllTopics(): Set[String] = _currentImage.topics().topicsByName().keySet().asScala

  override def getTopicPartitions(topicName: String): Set[TopicPartition] = {
    Option(_currentImage.topics().getTopic(topicName)) match {
      case None => Set.empty
      case Some(topic) => topic.partitions().keySet().asScala.map(new TopicPartition(topicName, _))
    }
  }

  override def getTopicId(topicName: String): Uuid = _currentImage.topics().topicsByName().asScala.get(topicName).map(_.id()).getOrElse(Uuid.ZERO_UUID)

  override def getTopicName(topicId: Uuid): Option[String] = _currentImage.topics().topicsById.asScala.get(topicId).map(_.name())

  override def hasAliveBroker(brokerId: Int): Boolean = {
    Option(_currentImage.cluster.broker(brokerId)).count(!_.fenced()) == 1
  }

  def isBrokerFenced(brokerId: Int): Boolean = {
    Option(_currentImage.cluster.broker(brokerId)).count(_.fenced) == 1
  }

  def isBrokerShuttingDown(brokerId: Int): Boolean = {
    Option(_currentImage.cluster.broker(brokerId)).count(_.inControlledShutdown) == 1
  }

  override def getAliveBrokers(): Iterable[BrokerMetadata] = getAliveBrokers(_currentImage)

  private def getAliveBrokers(image: MetadataImage): Iterable[BrokerMetadata] = {
    image.cluster().brokers().values().asScala.filterNot(_.fenced()).
      map(b => new BrokerMetadata(b.id, b.rack))
  }

  override def getAliveBrokerNode(brokerId: Int, listenerName: ListenerName): Option[Node] = {
    Option(_currentImage.cluster().broker(brokerId)).filterNot(_.fenced()).
      flatMap(_.node(listenerName.value()).toScala)
  }

  override def getAliveBrokerNodes(listenerName: ListenerName): Seq[Node] = {
    _currentImage.cluster().brokers().values().asScala.filterNot(_.fenced()).
      flatMap(_.node(listenerName.value()).toScala).toSeq
  }

  override def getBrokerNodes(listenerName: ListenerName): Seq[Node] = {
    _currentImage.cluster().brokers().values().asScala.flatMap(_.node(listenerName.value()).asScala).toSeq
  }

  // Does NOT include offline replica metadata
  override def getPartitionInfo(topicName: String, partitionId: Int): Option[UpdateMetadataPartitionState] = {
    Option(_currentImage.topics().getTopic(topicName)).
      flatMap(topic => Option(topic.partitions().get(partitionId))).
      flatMap(partition => Some(new UpdateMetadataPartitionState().
        setTopicName(topicName).
        setPartitionIndex(partitionId).
        setControllerEpoch(-1). // Controller epoch is not stored in the cache.
        setLeader(partition.leader).
        setLeaderEpoch(partition.leaderEpoch).
        setIsr(Replicas.toList(partition.isr)).
        setZkVersion(partition.partitionEpoch).
        setReplicas(Replicas.toList(partition.replicas))))
  }

  override def numPartitions(topicName: String): Option[Int] = {
    Option(_currentImage.topics().getTopic(topicName)).
      map(topic => topic.partitions().size())
  }

  override def topicNamesToIds(): util.Map[String, Uuid] = _currentImage.topics.topicNameToIdView()

  override def topicIdsToNames(): util.Map[Uuid, String] = _currentImage.topics.topicIdToNameView()

  override def topicIdInfo(): (util.Map[String, Uuid], util.Map[Uuid, String]) = {
    val image = _currentImage
    (image.topics.topicNameToIdView(), image.topics.topicIdToNameView())
  }

  // if the leader is not known, return None;
  // if the leader is known and corresponding node is available, return Some(node)
  // if the leader is known but corresponding node with the listener name is not available, return Some(NO_NODE)
  override def getPartitionLeaderEndpoint(topicName: String, partitionId: Int, listenerName: ListenerName): Option[Node] = {
    val image = _currentImage
    Option(image.topics().getTopic(topicName)) match {
      case None => None
      case Some(topic) => Option(topic.partitions().get(partitionId)) match {
        case None => None
        case Some(partition) => Option(image.cluster().broker(partition.leader)) match {
          case None => Some(Node.noNode)
          case Some(broker) => Some(broker.node(listenerName.value()).orElse(Node.noNode()))
        }
      }
    }
  }

  override def getPartitionReplicaEndpoints(tp: TopicPartition, listenerName: ListenerName): Map[Int, Node] = {
    val image = _currentImage
    val result = new mutable.HashMap[Int, Node]()
    Option(image.topics().getTopic(tp.topic())).foreach { topic =>
      topic.partitions().values().forEach { partition =>
        partition.replicas.foreach { replicaId =>
          val broker = image.cluster().broker(replicaId)
          if (broker != null && !broker.fenced()) {
            broker.node(listenerName.value).ifPresent { node =>
              if (!node.isEmpty)
                result.put(replicaId, node)
            }
          }
        }
      }
    }
    result
  }

  /**
   * Choose a random broker node to report as the controller. We do this because we want
   * the client to send requests destined for the controller to a random broker.
   * Clients do not have direct access to the controller in the KRaft world, as explained
   * in KIP-590.
   */
  override def getControllerId: Option[CachedControllerId] =
    getRandomAliveBroker(_currentImage).map(KRaftCachedControllerId)

  override def getRandomAliveBrokerId: Option[Int] = {
    getRandomAliveBroker(_currentImage)
  }

  private def getRandomAliveBroker(image: MetadataImage): Option[Int] = {
    val aliveBrokers = getAliveBrokers(image).toList
    if (aliveBrokers.isEmpty) {
      None
    } else {
      Some(aliveBrokers(ThreadLocalRandom.current().nextInt(aliveBrokers.size)).id)
    }
  }

  def getAliveBrokerEpoch(brokerId: Int): Option[Long] = {
    Option(_currentImage.cluster().broker(brokerId)).filterNot(_.fenced()).
      map(brokerRegistration => brokerRegistration.epoch())
  }

  override def getClusterMetadata(clusterId: String, listenerName: ListenerName): Cluster = {
    val image = _currentImage
    val nodes = new util.HashMap[Integer, Node]
    image.cluster().brokers().values().forEach { broker =>
      if (!broker.fenced()) {
        broker.node(listenerName.value()).toScala.foreach { node =>
          nodes.put(broker.id(), node)
        }
      }
    }

    def node(id: Int): Node = {
      Option(nodes.get(id)).getOrElse(Node.noNode())
    }

    val partitionInfos = new util.ArrayList[PartitionInfo]
    val internalTopics = new util.HashSet[String]

    image.topics().topicsByName().values().forEach { topic =>
      topic.partitions().forEach { (key, value) =>
        val partitionId = key
        val partition = value
        partitionInfos.add(new PartitionInfo(topic.name(),
          partitionId,
          node(partition.leader),
          partition.replicas.map(replica => node(replica)),
          partition.isr.map(replica => node(replica)),
          getOfflineReplicas(image, partition, listenerName).asScala.
            map(replica => node(replica)).toArray))
        if (Topic.isInternal(topic.name())) {
          internalTopics.add(topic.name())
        }
      }
    }
    val controllerNode = node(getRandomAliveBroker(image).getOrElse(-1))
    // Note: the constructor of Cluster does not allow us to reference unregistered nodes.
    // So, for example, if partition foo-0 has replicas [1, 2] but broker 2 is not
    // registered, we pass its replicas as [1, -1]. This doesn't make a lot of sense, but
    // we are duplicating the behavior of ZkMetadataCache, for now.
    new Cluster(clusterId, nodes.values(),
      partitionInfos, Collections.emptySet(), internalTopics, controllerNode)
  }

  override def contains(topicName: String): Boolean =
    _currentImage.topics().topicsByName().containsKey(topicName)

  override def contains(tp: TopicPartition): Boolean = {
    Option(_currentImage.topics().getTopic(tp.topic())) match {
      case None => false
      case Some(topic) => topic.partitions().containsKey(tp.partition())
    }
  }

  def setImage(newImage: MetadataImage): Unit = {
    _currentImage = newImage
  }

  def getImage(): MetadataImage = {
    _currentImage
  }

  override def config(configResource: ConfigResource): Properties =
    _currentImage.configs().configProperties(configResource)

  def describeClientQuotas(request: DescribeClientQuotasRequestData): DescribeClientQuotasResponseData = {
    _currentImage.clientQuotas().describe(request)
  }

  def describeScramCredentials(request: DescribeUserScramCredentialsRequestData): DescribeUserScramCredentialsResponseData = {
    _currentImage.scram().describe(request)
  }

  override def metadataVersion(): MetadataVersion = _currentImage.features().metadataVersion()

  override def features(): FinalizedFeatures = {
    val image = _currentImage
    val finalizedFeatures = new java.util.HashMap[String, java.lang.Short](image.features().finalizedVersions())
    val kraftVersionLevel = kraftVersionSupplier.get().featureLevel()
    if (kraftVersionLevel > 0) {
      finalizedFeatures.put(KRaftVersion.FEATURE_NAME, kraftVersionLevel)
    }
    new FinalizedFeatures(image.features().metadataVersion(),
      finalizedFeatures,
      image.highestOffsetAndEpoch().offset,
      true)
  }
}

