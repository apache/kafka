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

import kafka.cluster.Partition
import kafka.controller.StateChangeLogger
import kafka.log.Log
import kafka.server.checkpoints.OffsetCheckpoints
import kafka.server.metadata.{MetadataBrokers, MetadataPartition}
import kafka.utils.Implicits.MapExtensionMethods
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.errors.KafkaStorageException

import scala.collection.{Map, Set, mutable}

trait RaftReplicaChangeDelegateHelper {
  def stateChangeLogger: StateChangeLogger
  def replicaFetcherManager: ReplicaFetcherManager
  def replicaAlterLogDirsManager: ReplicaAlterLogDirsManager
  def markDeferred(state: HostedPartition.Deferred): Unit
  def getLogDir(topicPartition: TopicPartition): Option[String]
  def error(msg: => String, e: => Throwable): Unit
  def markOffline(topicPartition: TopicPartition): Unit
  def markOnline(partition: Partition): Unit
  def completeDelayedFetchOrProduceRequests(topicPartition: TopicPartition): Unit
  def isShuttingDown: Boolean
  def initialFetchOffset(log: Log): Long
  def config: KafkaConfig
}

class RaftReplicaChangeDelegate(helper: RaftReplicaChangeDelegateHelper) {
  def makeDeferred(partitionsNewMap: Map[Partition, Boolean],
                   metadataOffset: Long): Unit = {
    val traceLoggingEnabled = helper.stateChangeLogger.isTraceEnabled
    if (traceLoggingEnabled)
      partitionsNewMap.forKeyValue { (partition, isNew) =>
        helper.stateChangeLogger.trace(s"Metadata batch $metadataOffset: starting the " +
          s"become-deferred transition for partition ${partition.topicPartition} isNew=$isNew")
      }

    // Stop fetchers for all the partitions
    helper.replicaFetcherManager.removeFetcherForPartitions(partitionsNewMap.keySet.map(_.topicPartition))
    helper.stateChangeLogger.info(s"Metadata batch $metadataOffset: as part of become-deferred request, " +
      s"stopped any fetchers for ${partitionsNewMap.size} partitions")
    // mark all the partitions as deferred
    partitionsNewMap.forKeyValue((partition, isNew) => helper.markDeferred(HostedPartition.Deferred(partition, isNew)))

    helper.replicaFetcherManager.shutdownIdleFetcherThreads()
    helper.replicaAlterLogDirsManager.shutdownIdleFetcherThreads()

    if (traceLoggingEnabled)
      partitionsNewMap.keys.foreach { partition =>
        helper.stateChangeLogger.trace(s"Completed batch $metadataOffset become-deferred " +
          s"transition for partition ${partition.topicPartition}")
      }
  }

  def makeLeaders(prevPartitionsAlreadyExisting: Set[MetadataPartition],
                  partitionStates: Map[Partition, MetadataPartition],
                  highWatermarkCheckpoints: OffsetCheckpoints,
                  metadataOffset: Option[Long],
                  topicIds: String => Option[Uuid]): Set[Partition] = {
    val partitionsMadeLeaders = mutable.Set[Partition]()
    val traceLoggingEnabled = helper.stateChangeLogger.isTraceEnabled
    val deferredBatches = metadataOffset.isEmpty
    val topLevelLogPrefix = if (deferredBatches)
      "Metadata batch <multiple deferred>"
    else
      s"Metadata batch ${metadataOffset.get}"
    try {
      // First stop fetchers for all the partitions
      helper.replicaFetcherManager.removeFetcherForPartitions(partitionStates.keySet.map(_.topicPartition))
      helper.stateChangeLogger.info(s"$topLevelLogPrefix: stopped ${partitionStates.size} fetcher(s)")
      // Update the partition information to be the leader
      partitionStates.forKeyValue { (partition, state) =>
        val topicPartition = partition.topicPartition
        val partitionLogMsgPrefix = if (deferredBatches)
          s"Apply deferred leader partition $topicPartition"
        else
          s"Metadata batch ${metadataOffset.get} $topicPartition"
        try {
          val isrState = state.toLeaderAndIsrPartitionState(
            !prevPartitionsAlreadyExisting(state))
          if (partition.makeLeader(isrState, highWatermarkCheckpoints, topicIds(partition.topic))) {
            partitionsMadeLeaders += partition
            if (traceLoggingEnabled) {
              helper.stateChangeLogger.trace(s"$partitionLogMsgPrefix: completed the become-leader state change.")
            }
          } else {
            helper.stateChangeLogger.info(s"$partitionLogMsgPrefix: skipped the " +
              "become-leader state change since it is already the leader.")
          }
        } catch {
          case e: KafkaStorageException =>
            helper.stateChangeLogger.error(s"$partitionLogMsgPrefix: unable to make " +
              s"leader because the replica for the partition is offline due to disk error $e")
            val dirOpt = helper.getLogDir(topicPartition)
            helper.error(s"Error while making broker the leader for partition $partition in dir $dirOpt", e)
            helper.markOffline(topicPartition)
        }
      }
    } catch {
      case e: Throwable =>
        helper.stateChangeLogger.error(s"$topLevelLogPrefix: error while processing batch.", e)
        // Re-throw the exception for it to be caught in BrokerMetadataListener
        throw e
    }
    partitionsMadeLeaders
  }

  def makeFollowers(prevPartitionsAlreadyExisting: Set[MetadataPartition],
                    currentBrokers: MetadataBrokers,
                    partitionStates: Map[Partition, MetadataPartition],
                    highWatermarkCheckpoints: OffsetCheckpoints,
                    metadataOffset: Option[Long],
                    topicIds: String => Option[Uuid]): Set[Partition] = {
    val traceLoggingEnabled = helper.stateChangeLogger.isTraceEnabled
    val deferredBatches = metadataOffset.isEmpty
    val topLevelLogPrefix = if (deferredBatches)
      "Metadata batch <multiple deferred>"
    else
      s"Metadata batch ${metadataOffset.get}"
    if (traceLoggingEnabled) {
      partitionStates.forKeyValue { (partition, state) =>
        val topicPartition = partition.topicPartition
        val partitionLogMsgPrefix = if (deferredBatches)
          s"Apply deferred follower partition $topicPartition"
        else
          s"Metadata batch ${metadataOffset.get} $topicPartition"
        helper.stateChangeLogger.trace(s"$partitionLogMsgPrefix: starting the " +
          s"become-follower transition with leader ${state.leaderId}")
      }
    }

    val partitionsMadeFollower: mutable.Set[Partition] = mutable.Set()
    // all brokers, including both alive and not
    val acceptableLeaderBrokerIds = currentBrokers.iterator().map(broker => broker.id).toSet
    val allBrokersByIdMap = currentBrokers.iterator().map(broker => broker.id -> broker).toMap
    try {
      partitionStates.forKeyValue { (partition, state) =>
        val topicPartition = partition.topicPartition
        val partitionLogMsgPrefix = if (deferredBatches)
          s"Apply deferred follower partition $topicPartition"
        else
          s"Metadata batch ${metadataOffset.get} $topicPartition"
        try {
          val isNew = !prevPartitionsAlreadyExisting(state)
          if (!acceptableLeaderBrokerIds.contains(state.leaderId)) {
            // The leader broker should always be present in the metadata cache.
            // If not, we should record the error message and abort the transition process for this partition
            helper.stateChangeLogger.error(s"$partitionLogMsgPrefix: cannot become follower " +
              s"since the new leader ${state.leaderId} is unavailable.")
            // Create the local replica even if the leader is unavailable. This is required to ensure that we include
            // the partition's high watermark in the checkpoint file (see KAFKA-1647)
            partition.createLogIfNotExists(isNew, isFutureReplica = false, highWatermarkCheckpoints, topicIds(partition.topic))
          } else {
            val isrState = state.toLeaderAndIsrPartitionState(isNew)
            if (partition.makeFollower(isrState, highWatermarkCheckpoints, topicIds(partition.topic))) {
              partitionsMadeFollower += partition
              if (traceLoggingEnabled) {
                helper.stateChangeLogger.trace(s"$partitionLogMsgPrefix: completed the " +
                  s"become-follower state change with new leader ${state.leaderId}.")
              }
            } else {
              helper.stateChangeLogger.info(s"$partitionLogMsgPrefix: skipped the " +
                s"become-follower state change since " +
                s"the new leader ${state.leaderId} is the same as the old leader.")
            }
          }
        } catch {
          case e: KafkaStorageException =>
            helper.stateChangeLogger.error(s"$partitionLogMsgPrefix: unable to complete the " +
              s"become-follower state change since the " +
              s"replica for the partition is offline due to disk error $e")
            val dirOpt = helper.getLogDir(partition.topicPartition)
            helper.error(s"Error while making broker the follower with leader ${state.leaderId} in dir $dirOpt", e)
            helper.markOffline(topicPartition)
        }
      }

      if (partitionsMadeFollower.nonEmpty) {
        helper.replicaFetcherManager.removeFetcherForPartitions(partitionsMadeFollower.map(_.topicPartition))
        helper.stateChangeLogger.info(s"$topLevelLogPrefix: stopped followers for ${partitionsMadeFollower.size} partitions")

        partitionsMadeFollower.foreach { partition =>
          helper.completeDelayedFetchOrProduceRequests(partition.topicPartition)
        }

        if (helper.isShuttingDown) {
          if (traceLoggingEnabled) {
            partitionsMadeFollower.foreach { partition =>
              val topicPartition = partition.topicPartition
              val partitionLogMsgPrefix = if (deferredBatches)
                s"Apply deferred follower partition $topicPartition"
              else
                s"Metadata batch ${metadataOffset.get} $topicPartition"
              helper.stateChangeLogger.trace(s"$partitionLogMsgPrefix: skipped the " +
                s"adding-fetcher step of the become-follower state for " +
                s"$topicPartition since we are shutting down.")
            }
          }
        } else {
          // we do not need to check if the leader exists again since this has been done at the beginning of this process
          val partitionsToMakeFollowerWithLeaderAndOffset = partitionsMadeFollower.map { partition =>
            val leader = allBrokersByIdMap(partition.leaderReplicaIdOpt.get).brokerEndPoint(helper.config.interBrokerListenerName)
            val log = partition.localLogOrException
            val fetchOffset = helper.initialFetchOffset(log)
            if (deferredBatches) {
              helper.markOnline(partition)
            }
            partition.topicPartition -> InitialFetchState(leader, partition.getLeaderEpoch, fetchOffset)
          }.toMap

          helper.replicaFetcherManager.addFetcherForPartitions(partitionsToMakeFollowerWithLeaderAndOffset)
        }
      }
    } catch {
      case e: Throwable =>
        helper.stateChangeLogger.error(s"$topLevelLogPrefix: error while processing batch", e)
        // Re-throw the exception for it to be caught in BrokerMetadataListener
        throw e
    }

    if (traceLoggingEnabled)
      partitionsMadeFollower.foreach { partition =>
        val topicPartition = partition.topicPartition
        val state = partitionStates(partition)
        val partitionLogMsgPrefix = if (deferredBatches)
          s"Apply deferred follower partition $topicPartition"
        else
          s"Metadata batch ${metadataOffset.get} $topicPartition"
        helper.stateChangeLogger.trace(s"$partitionLogMsgPrefix: completed become-follower " +
          s"transition for partition $topicPartition with new leader ${state.leaderId}")
      }

    partitionsMadeFollower
  }
}
