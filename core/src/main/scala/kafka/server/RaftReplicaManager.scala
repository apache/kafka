/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import java.util.concurrent.atomic.AtomicBoolean

import kafka.cluster.Partition
import kafka.controller.StateChangeLogger
import kafka.log.{Log, LogManager}
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.checkpoints.LazyOffsetCheckpoints
import kafka.server.metadata.{ConfigRepository, MetadataImage, MetadataImageBuilder, MetadataPartition, RaftMetadataCache}
import kafka.utils.Scheduler
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.errors.{InconsistentTopicIdException, KafkaStorageException}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Time

import scala.collection.{Set, mutable}

class RaftReplicaManager(config: KafkaConfig,
                         metrics: Metrics,
                         time: Time,
                         scheduler: Scheduler,
                         logManager: LogManager,
                         isShuttingDown: AtomicBoolean,
                         quotaManagers: QuotaManagers,
                         brokerTopicStats: BrokerTopicStats,
                         metadataCache: RaftMetadataCache,
                         logDirFailureChannel: LogDirFailureChannel,
                         delayedProducePurgatory: DelayedOperationPurgatory[DelayedProduce],
                         delayedFetchPurgatory: DelayedOperationPurgatory[DelayedFetch],
                         delayedDeleteRecordsPurgatory: DelayedOperationPurgatory[DelayedDeleteRecords],
                         delayedElectLeaderPurgatory: DelayedOperationPurgatory[DelayedElectLeader],
                         threadNamePrefix: Option[String],
                         configRepository: ConfigRepository,
                         alterIsrManager: AlterIsrManager) extends ReplicaManager(
  config, metrics, time, None, scheduler, logManager, isShuttingDown, quotaManagers,
  brokerTopicStats, metadataCache, logDirFailureChannel, delayedProducePurgatory, delayedFetchPurgatory,
  delayedDeleteRecordsPurgatory, delayedElectLeaderPurgatory, threadNamePrefix, configRepository, alterIsrManager) {

  def this(config: KafkaConfig,
           metrics: Metrics,
           time: Time,
           scheduler: Scheduler,
           logManager: LogManager,
           isShuttingDown: AtomicBoolean,
           quotaManagers: QuotaManagers,
           brokerTopicStats: BrokerTopicStats,
           metadataCache: RaftMetadataCache,
           logDirFailureChannel: LogDirFailureChannel,
           alterIsrManager: AlterIsrManager,
           configRepository: ConfigRepository,
           threadNamePrefix: Option[String] = None) = {
    this(config, metrics, time, scheduler, logManager, isShuttingDown,
      quotaManagers, brokerTopicStats, metadataCache, logDirFailureChannel,
      DelayedOperationPurgatory[DelayedProduce](
        purgatoryName = "Produce", brokerId = config.brokerId,
        purgeInterval = config.producerPurgatoryPurgeIntervalRequests),
      DelayedOperationPurgatory[DelayedFetch](
        purgatoryName = "Fetch", brokerId = config.brokerId,
        purgeInterval = config.fetchPurgatoryPurgeIntervalRequests),
      DelayedOperationPurgatory[DelayedDeleteRecords](
        purgatoryName = "DeleteRecords", brokerId = config.brokerId,
        purgeInterval = config.deleteRecordsPurgatoryPurgeIntervalRequests),
      DelayedOperationPurgatory[DelayedElectLeader](
        purgatoryName = "ElectLeader", brokerId = config.brokerId),
      threadNamePrefix, configRepository, alterIsrManager)
  }

  if (config.requiresZookeeper) {
    throw new IllegalStateException(s"Cannot use ${getClass.getSimpleName} when using ZooKeeper")
  }

  class RaftReplicaManagerChangeDelegateHelper(raftReplicaManager: RaftReplicaManager) extends RaftReplicaChangeDelegateHelper {
    override def completeDelayedFetchOrProduceRequests(topicPartition: TopicPartition): Unit = raftReplicaManager.completeDelayedFetchOrProduceRequests(topicPartition)

    override def config: KafkaConfig = raftReplicaManager.config

    override def error(msg: => String, e: => Throwable): Unit = raftReplicaManager.error(msg, e)

    override def getLogDir(topicPartition: TopicPartition): Option[String] = raftReplicaManager.getLogDir(topicPartition)

    override def initialFetchOffset(log: Log): Long = raftReplicaManager.initialFetchOffset(log)

    override def isShuttingDown: Boolean = raftReplicaManager.isShuttingDown.get

    override def markDeferred(state: HostedPartition.Deferred): Unit = raftReplicaManager.markPartitionDeferred(state)

    override def markOffline(topicPartition: TopicPartition): Unit = raftReplicaManager.markPartitionOffline(topicPartition)

    override def markOnline(partition: Partition): Unit = raftReplicaManager.allPartitions.put(partition.topicPartition, HostedPartition.Online(partition))

    override def replicaAlterLogDirsManager: ReplicaAlterLogDirsManager = raftReplicaManager.replicaAlterLogDirsManager

    override def replicaFetcherManager: ReplicaFetcherManager = raftReplicaManager.replicaFetcherManager

    override def stateChangeLogger: StateChangeLogger = raftReplicaManager.stateChangeLogger
  }

  // visible/overwriteable for testing, generally will not change otherwise
  private[server] var delegate = new RaftReplicaChangeDelegate(new RaftReplicaManagerChangeDelegateHelper(this))

  // Changes are initially deferred when using a Raft-based metadata quorum, and they may flip-flop to not
  // being deferred and being deferred again thereafter as the broker (re)acquires/loses its lease.
  // Changes are never deferred when using ZooKeeper.  When true, this indicates that we should transition
  // online partitions to the deferred state if we see a metadata update for that partition.
  private var deferringMetadataChanges: Boolean = true
  stateChangeLogger.debug(s"Metadata changes are initially being deferred")

  def beginMetadataChangeDeferral(): Unit = {
    replicaStateChangeLock synchronized {
      deferringMetadataChanges = true
      stateChangeLogger.info(s"Metadata changes are now being deferred")
    }
  }

  def endMetadataChangeDeferral(onLeadershipChange: (Iterable[Partition], Iterable[Partition]) => Unit): Unit = {
    val startMs = time.milliseconds()
    var partitionsMadeFollower = Set.empty[Partition]
    var partitionsMadeLeader = Set.empty[Partition]
    replicaStateChangeLock synchronized {
      stateChangeLogger.info(s"Applying deferred metadata changes")
      val highWatermarkCheckpoints = new LazyOffsetCheckpoints(this.highWatermarkCheckpoints)
      val metadataImage = metadataCache.currentImage()
      val brokers = metadataImage.brokers
      try {
        val leaderPartitionStates = mutable.Map[Partition, MetadataPartition]()
        val followerPartitionStates = mutable.Map[Partition, MetadataPartition]()
        val partitionsAlreadyExisting = mutable.Set[MetadataPartition]()
        deferredPartitionsIterator.foreach { deferredPartition =>
          val partition = deferredPartition.partition
          val state = cachedState(metadataImage, partition)
          if (state.leaderId == localBrokerId) {
            leaderPartitionStates.put(partition, state)
          } else {
            followerPartitionStates.put(partition, state)
          }
          if (!deferredPartition.isNew) {
            partitionsAlreadyExisting += state
          }
        }
        if (leaderPartitionStates.nonEmpty)
          partitionsMadeLeader = delegate.makeLeaders(partitionsAlreadyExisting, leaderPartitionStates, highWatermarkCheckpoints, None, metadataImage.topicNameToId)
        if (followerPartitionStates.nonEmpty)
          partitionsMadeFollower = delegate.makeFollowers(partitionsAlreadyExisting, brokers, followerPartitionStates, highWatermarkCheckpoints, None, metadataImage.topicNameToId)

        // We need to transition anything that hasn't transitioned from Deferred to Offline to the Online state.
        deferredPartitionsIterator.foreach { deferredPartition =>
          val partition = deferredPartition.partition
          allPartitions.put(partition.topicPartition, HostedPartition.Online(partition))
        }

        updateLeaderAndFollowerMetrics(partitionsMadeFollower.map(_.topic).toSet)

        maybeAddLogDirFetchers(partitionsMadeFollower, highWatermarkCheckpoints, metadataImage.topicNameToId)

        replicaFetcherManager.shutdownIdleFetcherThreads()
        replicaAlterLogDirsManager.shutdownIdleFetcherThreads()
        if (partitionsMadeLeader.nonEmpty || partitionsMadeFollower.nonEmpty) {
          onLeadershipChange(partitionsMadeLeader, partitionsMadeFollower)
        }
      } catch {
        case e: Throwable =>
          deferredPartitionsIterator.foreach { metadata =>
            val partition = metadata.partition
            val state = cachedState(metadataImage, partition)
            val topicPartition = partition.topicPartition
            val leader = state.leaderId == localBrokerId
            val leaderOrFollower = if (leader) "leader" else "follower"
            val partitionLogMsgPrefix = s"Apply deferred $leaderOrFollower partition $topicPartition"
            stateChangeLogger.error(s"$partitionLogMsgPrefix: error while applying deferred metadata.", e)
          }
          stateChangeLogger.info(s"Applied ${partitionsMadeLeader.size + partitionsMadeFollower.size} deferred partitions prior to the error: " +
            s"${partitionsMadeLeader.size} leader(s) and ${partitionsMadeFollower.size} follower(s)")
          // Re-throw the exception for it to be caught in BrokerMetadataListener
          throw e
      }
      deferringMetadataChanges = false
    }
    val endMs = time.milliseconds()
    val elapsedMs = endMs - startMs
    stateChangeLogger.info(s"Applied ${partitionsMadeLeader.size + partitionsMadeFollower.size} deferred partitions: " +
      s"${partitionsMadeLeader.size} leader(s) and ${partitionsMadeFollower.size} follower(s)" +
      s"in $elapsedMs ms")
    stateChangeLogger.info("Metadata changes are no longer being deferred")
  }

  /**
   * Handle changes made by a batch of metadata log records.
   *
   * @param imageBuilder       The MetadataImage builder.
   * @param metadataOffset     The last offset in the batch of records.
   * @param onLeadershipChange The callbacks to invoke when leadership changes.
   */
  def handleMetadataRecords(imageBuilder: MetadataImageBuilder,
                            metadataOffset: Long,
                            onLeadershipChange: (Iterable[Partition], Iterable[Partition]) => Unit): Unit = {
    val startMs = time.milliseconds()
    val builder = imageBuilder.partitionsBuilder()
    replicaStateChangeLock synchronized {
      stateChangeLogger.info(("Metadata batch %d: %d local partition(s) changed, %d " +
        "local partition(s) removed.").format(metadataOffset, builder.localChanged().size,
        builder.localRemoved().size))
      if (stateChangeLogger.isTraceEnabled) {
        builder.localChanged().foreach { state =>
          stateChangeLogger.trace(s"Metadata batch $metadataOffset: locally changed: ${state}")
        }
        builder.localRemoved().foreach { state =>
          stateChangeLogger.trace(s"Metadata batch $metadataOffset: locally removed: ${state}")
        }
      }
      if (deferringMetadataChanges) {
        val prevPartitions = imageBuilder.prevImage.partitions
        // partitionChangesToBeDeferred maps each partition to be deferred to whether it is new (i.e. existed before deferral began)
        val partitionChangesToBeDeferred = mutable.HashMap[Partition, Boolean]()
        builder.localChanged().foreach { currentState =>
          val topicPartition = currentState.toTopicPartition
          val (partition, priorDeferredMetadata) = getPartition(topicPartition) match {
            case HostedPartition.Offline =>
              stateChangeLogger.warn(s"Ignoring handlePartitionChanges at $metadataOffset " +
                s"for partition $topicPartition as the local replica for the partition is " +
                "in an offline log directory")
              (None, None)

            case HostedPartition.Online(partition) => (Some(partition), None)
            case deferred@HostedPartition.Deferred(partition, _) => (Some(partition), Some(deferred))

            case HostedPartition.None =>
              // Create the partition instance since it does not yet exist
              (Some(Partition(topicPartition, time, configRepository, this)), None)
          }
          partition.foreach { partition =>
            checkTopicId(builder.topicNameToId(partition.topic), partition.topicId, partition.topicPartition)
            val isNew = priorDeferredMetadata match {
              case Some(alreadyDeferred) => alreadyDeferred.isNew
              case _ => prevPartitions.topicPartition(topicPartition.topic(), topicPartition.partition()).isEmpty
            }
            partitionChangesToBeDeferred.put(partition, isNew)
          }
        }

        stateChangeLogger.info(s"Deferring metadata changes for ${partitionChangesToBeDeferred.size} partition(s)")
        if (partitionChangesToBeDeferred.nonEmpty) {
          delegate.makeDeferred(partitionChangesToBeDeferred, metadataOffset)
        }
      } else { // not deferring changes, so make leaders/followers accordingly
        val partitionsToBeLeader = mutable.HashMap[Partition, MetadataPartition]()
        val partitionsToBeFollower = mutable.HashMap[Partition, MetadataPartition]()
        builder.localChanged().foreach { currentState =>
          val topicPartition = currentState.toTopicPartition
          val partition = getPartition(topicPartition) match {
            case HostedPartition.Offline =>
              stateChangeLogger.warn(s"Ignoring handlePartitionChanges at $metadataOffset " +
                s"for partition $topicPartition as the local replica for the partition is " +
                "in an offline log directory")
              None

            case HostedPartition.Online(partition) => Some(partition)
            case _: HostedPartition.Deferred => throw new IllegalStateException(
              s"There should never be deferred partition metadata when we aren't deferring changes: $topicPartition")

            case HostedPartition.None =>
              // it's a partition that we don't know about yet, so create it and mark it online
              val partition = Partition(topicPartition, time, configRepository, this)
              allPartitions.putIfNotExists(topicPartition, HostedPartition.Online(partition))
              Some(partition)
          }
          partition.foreach { partition =>
            checkTopicId(builder.topicNameToId(partition.topic), partition.topicId, partition.topicPartition)
            if (currentState.leaderId == localBrokerId) {
              partitionsToBeLeader.put(partition, currentState)
            } else {
              partitionsToBeFollower.put(partition, currentState)
            }
          }
        }

        val prevPartitions = imageBuilder.prevImage.partitions
        val changedPartitionsPreviouslyExisting = mutable.Set[MetadataPartition]()
        builder.localChanged().foreach(metadataPartition =>
          prevPartitions.topicPartition(metadataPartition.topicName, metadataPartition.partitionIndex).foreach(
            changedPartitionsPreviouslyExisting.add))
        val nextBrokers = imageBuilder.brokers()
        val highWatermarkCheckpoints = new LazyOffsetCheckpoints(this.highWatermarkCheckpoints)
        val partitionsBecomeLeader = if (partitionsToBeLeader.nonEmpty)
          delegate.makeLeaders(changedPartitionsPreviouslyExisting, partitionsToBeLeader, highWatermarkCheckpoints,
            Some(metadataOffset), builder.topicNameToId)
        else
          Set.empty[Partition]
        val partitionsBecomeFollower = if (partitionsToBeFollower.nonEmpty)
          delegate.makeFollowers(changedPartitionsPreviouslyExisting, nextBrokers, partitionsToBeFollower, highWatermarkCheckpoints,
            Some(metadataOffset), builder.topicNameToId)
        else
          Set.empty[Partition]
        updateLeaderAndFollowerMetrics(partitionsBecomeFollower.map(_.topic).toSet)

        builder.localChanged().foreach { state =>
          val topicPartition = state.toTopicPartition
          /*
          * If there is offline log directory, a Partition object may have been created by getOrCreatePartition()
          * before getOrCreateReplica() failed to create local replica due to KafkaStorageException.
          * In this case ReplicaManager.allPartitions will map this topic-partition to an empty Partition object.
          * we need to map this topic-partition to OfflinePartition instead.
          */
          if (localLog(topicPartition).isEmpty) {
            markPartitionOffline(topicPartition)
          }
        }

        maybeAddLogDirFetchers(partitionsBecomeFollower, highWatermarkCheckpoints, builder.topicNameToId)

        replicaFetcherManager.shutdownIdleFetcherThreads()
        replicaAlterLogDirsManager.shutdownIdleFetcherThreads()
        onLeadershipChange(partitionsBecomeLeader, partitionsBecomeFollower)
        stateChangeLogger.info(s"Metadata batch $metadataOffset: applied ${partitionsBecomeLeader.size + partitionsBecomeFollower.size} partitions: " +
          s"${partitionsBecomeLeader.size} leader(s) and ${partitionsBecomeFollower.size} follower(s)")
      }
      // TODO: we should move aside log directories which have been deleted rather than
      // purging them from the disk immediately.
      if (builder.localRemoved().nonEmpty) {
        // we schedule removal immediately even if we are deferring changes
        stopPartitions(builder.localRemoved().map(_.toTopicPartition -> true).toMap).foreach { case (topicPartition, e) =>
          if (e.isInstanceOf[KafkaStorageException]) {
            stateChangeLogger.error(s"Metadata batch $metadataOffset: unable to delete " +
              s"${topicPartition} as the local replica for the partition is in an offline " +
              "log directory")
          } else {
            stateChangeLogger.error(s"Metadata batch $metadataOffset: unable to delete " +
              s"${topicPartition} due to an unexpected ${e.getClass.getName} exception: " +
              s"${e.getMessage}")
          }
        }
      }
    }
    val endMs = time.milliseconds()
    val elapsedMs = endMs - startMs
    stateChangeLogger.info(s"Metadata batch $metadataOffset: handled replica changes " +
      s"in ${elapsedMs} ms")
  }

  def markPartitionDeferred(partition: Partition, isNew: Boolean): Unit = {
    markPartitionDeferred(HostedPartition.Deferred(partition, isNew))
  }

  private def markPartitionDeferred(state: HostedPartition.Deferred): Unit = replicaStateChangeLock synchronized {
    allPartitions.put(state.partition.topicPartition, state)
  }

  // An iterator over all deferred partitions. This is a weakly consistent iterator; a partition made off/online
  // after the iterator has been constructed could still be returned by this iterator.
  private def deferredPartitionsIterator: Iterator[HostedPartition.Deferred] = {
    allPartitions.values.iterator.flatMap {
      case deferred: HostedPartition.Deferred => Some(deferred)
      case _ => None
    }
  }

  private def cachedState(metadataImage: MetadataImage, partition: Partition): MetadataPartition = {
    metadataImage.partitions.topicPartition(partition.topic, partition.partitionId).getOrElse(
      throw new IllegalStateException(s"Partition has metadata changes but does not exist in the metadata cache: ${partition.topicPartition}"))
  }

  /**
   * Checks if the topic ID received from the MetadataPartitionsBuilder is consistent with the topic ID in the log.
   * If the log does not exist, logTopicIdOpt will be None. In this case, the ID is not inconsistent.
   *
   * @param receivedTopicIdOpt the topic ID received from the MetadataRecords if it exists
   * @param logTopicIdOpt the topic ID in the log if the log exists
   * @param topicPartition the topicPartition for the Partition being checked
   * @throws InconsistentTopicIdException if the topic ids are not consistent
   * @throws IllegalArgumentException if the MetadataPartitionsBuilder did not have a topic ID associated with the topic
   */
  private def checkTopicId(receivedTopicIdOpt: Option[Uuid], logTopicIdOpt: Option[Uuid], topicPartition: TopicPartition): Unit = {
    receivedTopicIdOpt match {
      case Some(receivedTopicId) =>
        logTopicIdOpt.foreach { logTopicId =>
          if (receivedTopicId != logTopicId) {
            // not sure if we need both the logger and the error thrown
            stateChangeLogger.error(s"Topic ID in memory: $logTopicId does not" +
              s" match the topic ID for partition $topicPartition received: " +
              s"$receivedTopicId.")
            throw new InconsistentTopicIdException(s"Topic partition $topicPartition had an inconsistent topic ID.")
          }
        }
      case None => throw new IllegalStateException(s"Topic partition $topicPartition is missing a topic ID")
    }
  }
}
