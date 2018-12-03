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

package kafka.controller

import kafka.cluster.Broker
import org.apache.kafka.common.TopicPartition

import scala.collection.{Seq, Set, mutable}

class ControllerContext {
  val stats = new ControllerStats

  var controllerChannelManager: ControllerChannelManager = null

  var shuttingDownBrokerIds: mutable.Set[Int] = mutable.Set.empty
  var epoch: Int = KafkaController.InitialControllerEpoch
  var epochZkVersion: Int = KafkaController.InitialControllerEpochZkVersion
  var allTopics: Set[String] = Set.empty
  private val partitionReplicaAssignmentUnderlying: mutable.Map[String, mutable.Map[Int, Seq[Int]]] = mutable.Map.empty
  val partitionLeadershipInfo: mutable.Map[TopicPartition, LeaderIsrAndControllerEpoch] = mutable.Map.empty
  val partitionsBeingReassigned: mutable.Map[TopicPartition, ReassignedPartitionsContext] = mutable.Map.empty
  val replicasOnOfflineDirs: mutable.Map[Int, Set[TopicPartition]] = mutable.Map.empty

  private var liveBrokersUnderlying: Set[Broker] = Set.empty
  private var liveBrokerIdAndEpochsUnderlying: Map[Int, Long] = Map.empty

  def partitionReplicaAssignment(topicPartition: TopicPartition): Seq[Int] = {
    partitionReplicaAssignmentUnderlying.getOrElse(topicPartition.topic, mutable.Map.empty)
      .getOrElse(topicPartition.partition, Seq.empty)
  }

  private def clearTopicsState(): Unit = {
    allTopics = Set.empty
    partitionReplicaAssignmentUnderlying.clear()
    partitionLeadershipInfo.clear()
    partitionsBeingReassigned.clear()
    replicasOnOfflineDirs.clear()
  }

  def updatePartitionReplicaAssignment(topicPartition: TopicPartition, newReplicas: Seq[Int]): Unit = {
    partitionReplicaAssignmentUnderlying.getOrElseUpdate(topicPartition.topic, mutable.Map.empty)
      .put(topicPartition.partition, newReplicas)
  }

  def partitionReplicaAssignmentForTopic(topic : String): Map[TopicPartition, Seq[Int]] = {
    partitionReplicaAssignmentUnderlying.getOrElse(topic, Map.empty).map {
      case (partition, replicas) => (new TopicPartition(topic, partition), replicas)
    }.toMap
  }

  def allPartitions: Set[TopicPartition] = {
    partitionReplicaAssignmentUnderlying.flatMap {
      case (topic, topicReplicaAssignment) => topicReplicaAssignment.map {
        case (partition, _) => new TopicPartition(topic, partition)
      }
    }.toSet
  }

  def setLiveBrokerAndEpochs(brokerAndEpochs: Map[Broker, Long]) {
    liveBrokersUnderlying = brokerAndEpochs.keySet
    liveBrokerIdAndEpochsUnderlying =
      brokerAndEpochs map { case (broker, brokerEpoch) => (broker.id, brokerEpoch)}
  }

  def addLiveBrokersAndEpochs(brokerAndEpochs: Map[Broker, Long]): Unit = {
    liveBrokersUnderlying = liveBrokersUnderlying ++ brokerAndEpochs.keySet
    liveBrokerIdAndEpochsUnderlying = liveBrokerIdAndEpochsUnderlying ++
      (brokerAndEpochs map { case (broker, brokerEpoch) => (broker.id, brokerEpoch)})
  }

  def removeLiveBrokersAndEpochs(brokerIds : Set[Int]): Unit = {
    liveBrokersUnderlying = liveBrokersUnderlying.filter(broker => !brokerIds.contains(broker.id))
    liveBrokerIdAndEpochsUnderlying = liveBrokerIdAndEpochsUnderlying.filterKeys(id => !brokerIds.contains(id))
  }

  def updateBrokerMetadata(oldMetadata: Option[Broker], newMetadata: Option[Broker]): Unit = {
    liveBrokersUnderlying = liveBrokersUnderlying -- oldMetadata ++ newMetadata
  }

  // getter
  def liveBrokers = liveBrokersUnderlying.filter(broker => !shuttingDownBrokerIds.contains(broker.id))
  def liveBrokerIds = liveBrokerIdAndEpochsUnderlying.keySet -- shuttingDownBrokerIds

  def liveOrShuttingDownBrokerIds = liveBrokerIdAndEpochsUnderlying.keySet
  def liveOrShuttingDownBrokers = liveBrokersUnderlying

  def liveBrokerIdAndEpochs = liveBrokerIdAndEpochsUnderlying

  def partitionsOnBroker(brokerId: Int): Set[TopicPartition] = {
    partitionReplicaAssignmentUnderlying.flatMap {
      case (topic, topicReplicaAssignment) => topicReplicaAssignment.filter {
        case (_, replicas) => replicas.contains(brokerId)
      }.map {
        case (partition, _) => new TopicPartition(topic, partition)
      }
    }.toSet
  }

  def isReplicaOnline(brokerId: Int, topicPartition: TopicPartition, includeShuttingDownBrokers: Boolean = false): Boolean = {
    val brokerOnline = {
      if (includeShuttingDownBrokers) liveOrShuttingDownBrokerIds.contains(brokerId)
      else liveBrokerIds.contains(brokerId)
    }
    brokerOnline && !replicasOnOfflineDirs.getOrElse(brokerId, Set.empty).contains(topicPartition)
  }

  def replicasOnBrokers(brokerIds: Set[Int]): Set[PartitionAndReplica] = {
    brokerIds.flatMap { brokerId =>
      partitionReplicaAssignmentUnderlying.flatMap {
        case (topic, topicReplicaAssignment) => topicReplicaAssignment.collect {
          case (partition, replicas)  if replicas.contains(brokerId) =>
            PartitionAndReplica(new TopicPartition(topic, partition), brokerId)
        }
      }
    }
  }

  def replicasForTopic(topic: String): Set[PartitionAndReplica] = {
    partitionReplicaAssignmentUnderlying.getOrElse(topic, mutable.Map.empty).flatMap {
      case (partition, replicas) => replicas.map(r => PartitionAndReplica(new TopicPartition(topic, partition), r))
    }.toSet
  }

  def partitionsForTopic(topic: String): collection.Set[TopicPartition] = {
    partitionReplicaAssignmentUnderlying.getOrElse(topic, mutable.Map.empty).map {
      case (partition, _) => new TopicPartition(topic, partition)
    }.toSet
  }

  def allLiveReplicas(): Set[PartitionAndReplica] = {
    replicasOnBrokers(liveBrokerIds).filter { partitionAndReplica =>
      isReplicaOnline(partitionAndReplica.replica, partitionAndReplica.topicPartition)
    }
  }

  def replicasForPartition(partitions: collection.Set[TopicPartition]): collection.Set[PartitionAndReplica] = {
    partitions.flatMap { p =>
      val replicas = partitionReplicaAssignment(p)
      replicas.map(PartitionAndReplica(p, _))
    }
  }

  def resetContext(): Unit = {
    if (controllerChannelManager != null) {
      controllerChannelManager.shutdown()
      controllerChannelManager = null
    }
    shuttingDownBrokerIds.clear()
    epoch = 0
    epochZkVersion = 0
    clearTopicsState()
    setLiveBrokerAndEpochs(Map.empty)
  }

  def removeTopic(topic: String): Unit = {
    allTopics -= topic
    partitionReplicaAssignmentUnderlying.remove(topic)
    partitionLeadershipInfo.foreach {
      case (topicPartition, _) if topicPartition.topic == topic => partitionLeadershipInfo.remove(topicPartition)
      case _ =>
    }
  }
}
