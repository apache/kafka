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

import kafka.cluster.{Partition, Replica}
import kafka.log.LogManager
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.{Logging, Pool}
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{KafkaStorageException, NotLeaderForPartitionException, ReplicaNotAvailableException, UnknownTopicOrPartitionException}
import org.apache.kafka.common.utils.Time

object PartitionManager {
  def apply(config: KafkaConfig,
            time: Time,
            zkClient: KafkaZkClient,
            logManager: LogManager,
            metadataCache: MetadataCache,
            delayedOperationManager: DelayedOperationManager,
            isrManager: IsrManager) =
    new PartitionManager(config, time, zkClient, logManager, metadataCache, delayedOperationManager, isrManager)
}

class PartitionManager(config: KafkaConfig,
                       time: Time,
                       zkClient: KafkaZkClient,
                       logManager: LogManager,
                       metadataCache: MetadataCache,
                       delayedOperationManager: DelayedOperationManager,
                       isrManager: IsrManager) extends Logging with KafkaMetricsGroup {
  private val localBrokerId = config.brokerId

  private val allPartitions = new Pool[TopicPartition, Partition](valueFactory = Some(tp =>
    new Partition(tp.topic, tp.partition, time, config, zkClient, logManager, metadataCache, delayedOperationManager, isrManager)))

  def size(): Int =
    allPartitions.size

  def getReplica(topicPartition: TopicPartition, replicaId: Int): Option[Replica] =
    nonOfflinePartition(topicPartition).flatMap(_.getReplica(replicaId))

  def getReplica(tp: TopicPartition): Option[Replica] = getReplica(tp, localBrokerId)

  def getReplicaOrException(topicPartition: TopicPartition, brokerId: Int): Replica = {
    getPartition(topicPartition) match {
      case Some(partition) =>
        if (partition eq ReplicaManager.OfflinePartition)
          throw new KafkaStorageException(s"Replica $brokerId is in an offline log directory for partition $topicPartition")
        else
          partition.getReplica(brokerId).getOrElse(
            throw new ReplicaNotAvailableException(s"Replica $brokerId is not available for partition $topicPartition"))
      case None =>
        throw new ReplicaNotAvailableException(s"Replica $brokerId is not available for partition $topicPartition")
    }
  }

  def getReplicaOrException(topicPartition: TopicPartition): Replica = getReplicaOrException(topicPartition, localBrokerId)

  def getOrCreatePartition(topicPartition: TopicPartition): Partition =
    allPartitions.getAndMaybePut(topicPartition)

  def getPartition(topicPartition: TopicPartition): Option[Partition] =
    Option(allPartitions.get(topicPartition))

  def putPartition(topicPartition: TopicPartition, partition: Partition): Partition =
    allPartitions.put(topicPartition, partition)

  def nonOfflinePartition(topicPartition: TopicPartition): Option[Partition] =
    getPartition(topicPartition).filter(_ ne ReplicaManager.OfflinePartition)

  def topicHasPartitions(topic: String) : Boolean =
    allPartitions.values.exists(partition => topic == partition.topic)

  def removePartition(topicPartition: TopicPartition) : Partition =
    allPartitions.remove(topicPartition)

  def nonOfflinePartitionsIterator: Iterator[Partition] =
    allPartitions.values.iterator.filter(_ ne ReplicaManager.OfflinePartition)

  def offlinePartitionsIterator: Iterator[Partition] =
    allPartitions.values.iterator.filter(_ eq ReplicaManager.OfflinePartition)

  def getPartitionAndLeaderReplicaIfLocal(topicPartition: TopicPartition): (Partition, Replica) = {
    val partitionOpt = getPartition(topicPartition)
    partitionOpt match {
      case None if metadataCache.contains(topicPartition) =>
        // The topic exists, but this broker is no longer a replica of it, so we return NOT_LEADER which
        // forces clients to refresh metadata to find the new location. This can happen, for example,
        // during a partition reassignment if a produce request from the client is sent to a broker after
        // the local replica has been deleted.
        throw new NotLeaderForPartitionException(s"Broker $localBrokerId is not a replica of $topicPartition")

      case None =>
        throw new UnknownTopicOrPartitionException(s"Partition $topicPartition doesn't exist")

      case Some(partition) =>
        if (partition eq ReplicaManager.OfflinePartition)
          throw new KafkaStorageException(s"Partition $topicPartition is in an offline log directory on broker $localBrokerId")
        else partition.leaderReplicaIfLocal match {
          case Some(leaderReplica) => (partition, leaderReplica)
          case None =>
            throw new NotLeaderForPartitionException(s"Leader not local for partition $topicPartition on broker $localBrokerId")
        }
    }
  }

  def getLeaderReplicaIfLocal(topicPartition: TopicPartition): Replica =  {
    val (_, replica) = getPartitionAndLeaderReplicaIfLocal(topicPartition)
    replica
  }
}