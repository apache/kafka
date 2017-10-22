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
import kafka.common.TopicAndPartition

import scala.collection.{Seq, Set, mutable}

class ControllerContext {
  val stats = new ControllerStats

  var controllerChannelManager: ControllerChannelManager = null

  var shuttingDownBrokerIds: mutable.Set[Int] = mutable.Set.empty
  var epoch: Int = KafkaController.InitialControllerEpoch - 1
  var epochZkVersion: Int = KafkaController.InitialControllerEpochZkVersion - 1
  var allTopics: Set[String] = Set.empty
  var partitionReplicaAssignment: mutable.Map[TopicAndPartition, Seq[Int]] = mutable.Map.empty
  var partitionLeadershipInfo: mutable.Map[TopicAndPartition, LeaderIsrAndControllerEpoch] = mutable.Map.empty
  val partitionsBeingReassigned: mutable.Map[TopicAndPartition, ReassignedPartitionsContext] = mutable.Map.empty
  val replicasOnOfflineDirs: mutable.Map[Int, Set[TopicAndPartition]] = mutable.Map.empty

  private var liveBrokersUnderlying: Set[Broker] = Set.empty
  private var liveBrokerIdsUnderlying: Set[Int] = Set.empty

  // setter
  def liveBrokers_=(brokers: Set[Broker]) {
    liveBrokersUnderlying = brokers
    liveBrokerIdsUnderlying = liveBrokersUnderlying.map(_.id)
  }

  // getter
  def liveBrokers = liveBrokersUnderlying.filter(broker => !shuttingDownBrokerIds.contains(broker.id))
  def liveBrokerIds = liveBrokerIdsUnderlying -- shuttingDownBrokerIds

  def liveOrShuttingDownBrokerIds = liveBrokerIdsUnderlying
  def liveOrShuttingDownBrokers = liveBrokersUnderlying

  def partitionsOnBroker(brokerId: Int): Set[TopicAndPartition] = {
    partitionReplicaAssignment.collect {
      case (topicAndPartition, replicas) if replicas.contains(brokerId) => topicAndPartition
    }.toSet
  }

  def isReplicaOnline(brokerId: Int, topicAndPartition: TopicAndPartition, includeShuttingDownBrokers: Boolean = false): Boolean = {
    val brokerOnline = {
      if (includeShuttingDownBrokers) liveOrShuttingDownBrokerIds.contains(brokerId)
      else liveBrokerIds.contains(brokerId)
    }
    brokerOnline && !replicasOnOfflineDirs.getOrElse(brokerId, Set.empty).contains(topicAndPartition)
  }

  def replicasOnBrokers(brokerIds: Set[Int]): Set[PartitionAndReplica] = {
    brokerIds.flatMap { brokerId =>
      partitionReplicaAssignment.collect {
        case (topicAndPartition, replicas) if replicas.contains(brokerId) =>
          PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition, brokerId)
      }
    }.toSet
  }

  def replicasForTopic(topic: String): Set[PartitionAndReplica] = {
    partitionReplicaAssignment
      .filter { case (topicAndPartition, _) => topicAndPartition.topic == topic }
      .flatMap { case (topicAndPartition, replicas) =>
        replicas.map { r =>
          PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition, r)
        }
      }.toSet
  }

  def partitionsForTopic(topic: String): collection.Set[TopicAndPartition] =
    partitionReplicaAssignment.keySet.filter(topicAndPartition => topicAndPartition.topic == topic)

  def allLiveReplicas(): Set[PartitionAndReplica] = {
    replicasOnBrokers(liveBrokerIds).filter { partitionAndReplica =>
      isReplicaOnline(partitionAndReplica.replica, TopicAndPartition(partitionAndReplica.topic, partitionAndReplica.partition))
    }
  }

  def replicasForPartition(partitions: collection.Set[TopicAndPartition]): collection.Set[PartitionAndReplica] = {
    partitions.flatMap { p =>
      val replicas = partitionReplicaAssignment(p)
      replicas.map(r => PartitionAndReplica(p.topic, p.partition, r))
    }
  }

  def removeTopic(topic: String) = {
    partitionLeadershipInfo = partitionLeadershipInfo.filter { case (topicAndPartition, _) => topicAndPartition.topic != topic }
    partitionReplicaAssignment = partitionReplicaAssignment.filter { case (topicAndPartition, _) => topicAndPartition.topic != topic }
    allTopics -= topic
  }

}
