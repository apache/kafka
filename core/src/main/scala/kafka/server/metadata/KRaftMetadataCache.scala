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

import kafka.controller.StateChangeLogger
import kafka.server.MetadataCache
import kafka.utils.Logging
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.MetadataResponseData.{MetadataResponsePartition, MetadataResponseTopic}
import org.apache.kafka.common.{Cluster, Node, PartitionInfo, TopicPartition, Uuid}
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataPartitionState
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.image.MetadataImage
import java.util
import java.util.{Collections, Properties}
import java.util.concurrent.ThreadLocalRandom

import kafka.admin.BrokerMetadata
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.message.{DescribeClientQuotasRequestData, DescribeClientQuotasResponseData}
import org.apache.kafka.metadata.{PartitionRegistration, Replicas}

import scala.collection.{Seq, Set, mutable}
import scala.jdk.CollectionConverters._
import scala.compat.java8.OptionConverters._


class KRaftMetadataCache(val brokerId: Int) extends MetadataCache with Logging with ConfigRepository {
  this.logIdent = s"[MetadataCache brokerId=$brokerId] "

  // This is the cache state. Every MetadataImage instance is immutable, and updates
  // replace this value with a completely new one. This means reads (which are not under
  // any lock) need to grab the value of this variable once, and retain that read copy for
  // the duration of their operation. Multiple reads of this value risk getting different
  // image values.
  @volatile private var _currentImage: MetadataImage = MetadataImage.EMPTY

  private val stateChangeLogger = new StateChangeLogger(brokerId, inControllerContext = false, None)

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
              debug(s"Error while fetching metadata for ${topicName}-${partitionId}: leader not available")
              Errors.LEADER_NOT_AVAILABLE
            } else {
              debug(s"Error while fetching metadata for ${topicName}-${partitionId}: listener $listenerName " +
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
            val error = if (filteredReplicas.size < partition.replicas.size) {
              debug(s"Error while fetching metadata for ${topicName}-${partitionId}: replica information not available for " +
                s"following brokers ${partition.replicas.filterNot(filteredReplicas.contains).mkString(",")}")
              Errors.REPLICA_NOT_AVAILABLE
            } else if (filteredIsr.size < partition.isr.size) {
              debug(s"Error while fetching metadata for ${topicName}-${partitionId}: in sync replica information not available for " +
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

  private def getOfflineReplicas(image: MetadataImage,
                                 partition: PartitionRegistration,
                                 listenerName: ListenerName): util.List[Integer] = {
    // TODO: in order to really implement this correctly, we would need JBOD support.
    // That would require us to track which replicas were offline on a per-replica basis.
    // See KAFKA-13005.
    val offlineReplicas = new util.ArrayList[Integer](0)
    for (brokerId <- partition.replicas) {
      Option(image.cluster().broker(brokerId)) match {
        case None => offlineReplicas.add(brokerId)
        case Some(broker) => if (broker.fenced() || !broker.listeners().containsKey(listenerName.value())) {
          offlineReplicas.add(brokerId)
        }
      }
    }
    offlineReplicas
  }

  /**
   * Get the endpoint matching the provided listener if the broker is alive. Note that listeners can
   * be added dynamically, so a broker with a missing listener could be a transient error.
   *
   * @return None if broker is not alive or if the broker does not have a listener named `listenerName`.
   */
  private def getAliveEndpoint(image: MetadataImage, id: Int, listenerName: ListenerName): Option[Node] = {
    Option(image.cluster().broker(id)).flatMap(_.node(listenerName.value()).asScala)
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
    Option(_currentImage.cluster().broker(brokerId)).count(!_.fenced()) == 1
  }

  override def getAliveBrokers(): Iterable[BrokerMetadata] = getAliveBrokers(_currentImage)

  private def getAliveBrokers(image: MetadataImage): Iterable[BrokerMetadata] = {
    image.cluster().brokers().values().asScala.filter(!_.fenced()).
      map(b => BrokerMetadata(b.id, b.rack.asScala))
  }

  override def getAliveBrokerNode(brokerId: Int, listenerName: ListenerName): Option[Node] = {
    Option(_currentImage.cluster().broker(brokerId)).
      flatMap(_.node(listenerName.value()).asScala)
  }

  override def getAliveBrokerNodes(listenerName: ListenerName): Seq[Node] = {
    _currentImage.cluster().brokers().values().asScala.filter(!_.fenced()).
      flatMap(_.node(listenerName.value()).asScala).toSeq
  }

  override def getPartitionInfo(topicName: String, partitionId: Int): Option[UpdateMetadataPartitionState] = {
    Option(_currentImage.topics().getTopic(topicName)).
      flatMap(topic => Some(topic.partitions().get(partitionId))).
      flatMap(partition => Some(new UpdateMetadataPartitionState().
        setTopicName(topicName).
        setPartitionIndex(partitionId).
        setControllerEpoch(-1). // Controller epoch is not stored in the cache.
        setLeader(partition.leader).
        setLeaderEpoch(partition.leaderEpoch).
        setIsr(Replicas.toList(partition.isr)).
        setZkVersion(partition.partitionEpoch)))
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
      topic.partitions().values().forEach { case partition =>
        partition.replicas.map { case replicaId =>
          result.put(replicaId, Option(image.cluster().broker(replicaId)) match {
            case None => Node.noNode()
            case Some(broker) => broker.node(listenerName.value()).asScala.getOrElse(Node.noNode())
          })
        }
      }
    }
    result.toMap
  }

  override def getControllerId: Option[Int] = getRandomAliveBroker(_currentImage)

  /**
   * Choose a random broker node to report as the controller. We do this because we want
   * the client to send requests destined for the controller to a random broker.
   * Clients do not have direct access to the controller in the KRaft world, as explained
   * in KIP-590.
   */
  private def getRandomAliveBroker(image: MetadataImage): Option[Int] = {
    val aliveBrokers = getAliveBrokers(image).toList
    if (aliveBrokers.size == 0) {
      None
    } else {
      Some(aliveBrokers(ThreadLocalRandom.current().nextInt(aliveBrokers.size)).id)
    }
  }

  override def getClusterMetadata(clusterId: String, listenerName: ListenerName): Cluster = {
    val image = _currentImage
    val nodes = new util.HashMap[Integer, Node]
    image.cluster().brokers().values().forEach { broker =>
      if (!broker.fenced()) {
        broker.node(listenerName.value()).asScala.foreach { node =>
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
      topic.partitions().entrySet().forEach { entry =>
        val partitionId = entry.getKey()
        val partition = entry.getValue()
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

  def stateChangeTraceEnabled(): Boolean = {
    stateChangeLogger.isTraceEnabled
  }

  def logStateChangeTrace(str: String): Unit = {
    stateChangeLogger.trace(str)
  }

  override def contains(topicName: String): Boolean =
    _currentImage.topics().topicsByName().containsKey(topicName)

  override def contains(tp: TopicPartition): Boolean = {
    Option(_currentImage.topics().getTopic(tp.topic())) match {
      case None => false
      case Some(topic) => topic.partitions().containsKey(tp.partition())
    }
  }

  def setImage(newImage: MetadataImage): Unit = _currentImage = newImage

  override def config(configResource: ConfigResource): Properties =
    _currentImage.configs().configProperties(configResource)

  def describeClientQuotas(request: DescribeClientQuotasRequestData): DescribeClientQuotasResponseData = {
    _currentImage.clientQuotas().describe(request)
  }
}
