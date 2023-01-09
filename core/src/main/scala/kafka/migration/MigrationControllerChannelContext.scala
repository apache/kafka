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
import kafka.cluster.Broker
import kafka.controller.{ControllerChannelContext, LeaderIsrAndControllerEpoch}
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.image.MetadataImage

import scala.jdk.CollectionConverters._

object MigrationControllerChannelContext {
  def isReplicaOnline(image: MetadataImage, brokerId: Int, replicaAssignment: Set[Int]): Boolean = {
    val brokerOnline = image.cluster().containsBroker(brokerId)
    brokerOnline && replicaAssignment.contains(brokerId)
  }

  def partitionReplicaAssignment(image: MetadataImage, tp: TopicPartition): collection.Seq[Int] = {
    image.topics().topicsByName().asScala.get(tp.topic()) match {
      case Some(topic) => topic.partitions().asScala.get(tp.partition()) match {
        case Some(partition) => partition.replicas.toSeq
        case None => collection.Seq.empty
      }
      case None => collection.Seq.empty
    }
  }

  def partitionLeadershipInfo(image: MetadataImage, topicPartition: TopicPartition): Option[LeaderIsrAndControllerEpoch] = {
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

sealed class MigrationControllerChannelContext(
  val image: MetadataImage
) extends ControllerChannelContext {
  override def isTopicDeletionInProgress(topicName: String): Boolean = {
    !image.topics().topicsByName().containsKey(topicName)
  }

  override val topicIds: collection.Map[String, Uuid] = {
    image.topics().topicsByName().asScala.map {
      case (name, topic) => name -> topic.id()
    }.toMap
  }

  override val liveBrokerIdAndEpochs: collection.Map[Int, Long] = {
    image.cluster().brokers().asScala.map {
      case (brokerId, broker) => brokerId.intValue() -> broker.epoch()
    }
  }

  override val liveOrShuttingDownBrokers: collection.Set[Broker] = {
    image.cluster().brokers().asScala.values.map { registration =>
      Broker.fromBrokerRegistration(registration)
    }.toSet
  }

  override def isTopicQueuedUpForDeletion(topic: String): Boolean = {
    !image.topics().topicsByName().containsKey(topic)
  }

  override def isReplicaOnline(brokerId: Int, partition: TopicPartition): Boolean = {
    MigrationControllerChannelContext.isReplicaOnline(
      image, brokerId, partitionReplicaAssignment(partition).toSet)
  }

  override def partitionReplicaAssignment(tp: TopicPartition): collection.Seq[Int] = {
    MigrationControllerChannelContext.partitionReplicaAssignment(image, tp)
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
        case None => LeaderAndIsr.NoEpoch
      }
    }
  }

  override val liveOrShuttingDownBrokerIds: collection.Set[Int] = liveBrokerIdAndEpochs.keySet

  override def partitionLeadershipInfo(topicPartition: TopicPartition): Option[LeaderIsrAndControllerEpoch] = {
    MigrationControllerChannelContext.partitionLeadershipInfo(image, topicPartition)
  }
}