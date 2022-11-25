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

package kafka.migration

import kafka.api.LeaderAndIsr
import kafka.cluster.{Broker, EndPoint}
import kafka.controller.{AbstractControllerBrokerRequestBatch, ControllerBrokerRequestMetadata, ControllerChannelManager, LeaderIsrAndControllerEpoch, StateChangeLogger}
import kafka.server.KafkaConfig
import org.apache.kafka.common.feature.{Features, SupportedVersionRange}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.{Endpoint, TopicPartition, Uuid}
import org.apache.kafka.common.requests.{AbstractControlRequest, AbstractResponse, LeaderAndIsrResponse, StopReplicaResponse, UpdateMetadataResponse}
import org.apache.kafka.image.MetadataImage
import org.apache.kafka.metadata.VersionRange

import scala.compat.java8.OptionConverters._
import scala.jdk.CollectionConverters._

class KRaftControllerBrokerRequestMetadata(image: MetadataImage) extends
  ControllerBrokerRequestMetadata {
  override def isTopicDeletionInProgress(topicName: String): Boolean = {
    !image.topics().topicsByName().containsKey(topicName)
  }

  override def topicIds: collection.Map[String, Uuid] = {
    image.topics().topicsByName().asScala.map { case (name, topic) =>
      name -> topic.id()
    }.toMap
  }

  override def liveBrokerIdAndEpochs: collection.Map[Int, Long] = {
    image.cluster().brokers().asScala.map { case (brokerId, broker) =>
      brokerId.intValue() -> broker.epoch()
    }
  }

  override def liveOrShuttingDownBrokers: collection.Set[Broker] = {
    def toEndPoint(ep: Endpoint): EndPoint = {
      EndPoint(ep.host(), ep.port(), ListenerName.normalised(ep.listenerName().orElse("")), ep.securityProtocol())
    }

    def supportedFeatures(features: java.util.Map[String, VersionRange]): java.util.Map[String, SupportedVersionRange] = {
      features.asScala.map { case (name, range) =>
        name -> new SupportedVersionRange(range.min(), range.max())
      }.asJava
    }

    image.cluster().brokers().asScala.map { case (brokerId, broker) =>
      Broker(brokerId, broker.listeners().values().asScala.map(toEndPoint).toSeq, broker.rack().asScala,
        Features.supportedFeatures(supportedFeatures(broker.supportedFeatures())))
    }.toSet
  }

  override def isTopicQueuedUpForDeletion(topic: String): Boolean = {
    !image.topics().topicsByName().containsKey(topic)
  }

  override def isReplicaOnline(brokerId: Int, partition: TopicPartition): Boolean = {
    val brokerOnline = image.cluster().containsBroker(brokerId)
    brokerOnline && partitionReplicaAssignment(partition).contains(brokerId)
  }

  override def partitionReplicaAssignment(tp: TopicPartition): collection.Seq[Int] = {
    image.topics().topicsByName().asScala.get(tp.topic()) match {
      case Some(topic) => topic.partitions().asScala.get(tp.partition()) match {
        case Some(partition) => partition.replicas.toSeq
        case None => collection.Seq.empty
      }
      case None => collection.Seq.empty
    }
  }

  override def leaderEpoch(topicPartition: TopicPartition): Int = {
    // Topic is deleted use a special sentinel -2 to the indicate the same.
    if (isTopicQueuedUpForDeletion(topicPartition.topic())) {
      LeaderAndIsr.EpochDuringDelete
    } else {
      image.topics().topicsByName.asScala.get(topicPartition.topic()) match {
        case Some(topic) => topic.partitions().asScala.get(topicPartition.partition()) match {
          case Some(partition) => partition.leaderEpoch
          case None => LeaderAndIsr.NoEpoch
        }
        case None =>LeaderAndIsr.NoEpoch
      }
    }
  }

  override def liveOrShuttingDownBrokerIds: collection.Set[Int] = {
    liveBrokerIdAndEpochs.keySet
  }

  override def getImage: MetadataImage = image

  override def partitionLeadershipInfo(topicPartition: TopicPartition): Option[LeaderIsrAndControllerEpoch] = {
    image.topics().topicsByName().asScala.get(topicPartition.topic()) match {
      case Some(topic) => topic.partitions().asScala.get(topicPartition.partition()) match {
        case Some(partition) =>
          val leaderAndIsr = LeaderAndIsr(partition.leader, partition.leaderEpoch, partition.isr.toList,
            partition.leaderRecoveryState, partition.partitionEpoch)
          Some(LeaderIsrAndControllerEpoch(leaderAndIsr, image.highestOffsetAndEpoch().epoch()))
        case None => None
      }
      case None => None
    }
  }
}

class KRaftControllerBrokerRequestBatch(config: KafkaConfig,
                                        metadataProvider: () => ControllerBrokerRequestMetadata,
                                        controllerChannelManager: ControllerChannelManager,
                                        stateChangeLogger: StateChangeLogger)
  extends AbstractControllerBrokerRequestBatch(config, metadataProvider, stateChangeLogger, kraftController = true) {

  override def sendRequest(brokerId: Int,
                           request: AbstractControlRequest.Builder[_ <: AbstractControlRequest],
                           callback: AbstractResponse => Unit): Unit = {
    controllerChannelManager.sendRequest(brokerId, request, callback)
  }

  // TODO: Right now, the retry mechanism on failed response is to send a full leaderAndIsr and
  //  updateMetadata request periodically during the migration. If this behavior changes, add
  //  hooks into the migration driver to handle the failed response.
  override def handleLeaderAndIsrResponse(response: LeaderAndIsrResponse, broker: Int): Unit = {}

  override def handleUpdateMetadataResponse(response: UpdateMetadataResponse, broker: Int): Unit = {}

  override def handleStopReplicaResponse(stopReplicaResponse: StopReplicaResponse, brokerId: Int,
                                         partitionErrorsForDeletingTopics: Map[TopicPartition, Errors]): Unit = {}
}
