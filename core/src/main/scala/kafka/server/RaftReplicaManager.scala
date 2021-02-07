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
import kafka.utils.Implicits.MapExtensionMethods
import kafka.utils.Scheduler
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.KafkaStorageException
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

  class RaftReplicaManagerChangeDelegateHelper(raftReplicaManager: RaftReplicaManager) extends RaftReplicaChangeDelegateHelper {
    override def stateChangeLogger: StateChangeLogger = raftReplicaManager.stateChangeLogger

    override def replicaFetcherManager: ReplicaFetcherManager = raftReplicaManager.replicaFetcherManager

    override def replicaAlterLogDirsManager: ReplicaAlterLogDirsManager = raftReplicaManager.replicaAlterLogDirsManager

    override def markDeferred(state: HostedPartition.Deferred): Unit = raftReplicaManager.allPartitions.put(state.partition.topicPartition, state)

    override def getLogDir(topicPartition: TopicPartition): Option[String] = raftReplicaManager.getLogDir(topicPartition)

    override def error(msg: => String, e: => Throwable): Unit = raftReplicaManager.error(msg, e)

    override def markOffline(topicPartition: TopicPartition): Unit = raftReplicaManager.markPartitionOffline(topicPartition)

    override def completeDelayedFetchOrProduceRequests(topicPartition: TopicPartition): Unit = raftReplicaManager.completeDelayedFetchOrProduceRequests(topicPartition)

    override def isShuttingDown: Boolean = raftReplicaManager.isShuttingDown.get

    override def initialFetchOffset(log: Log): Long = raftReplicaManager.initialFetchOffset(log)

    override def config: KafkaConfig = raftReplicaManager.config
  }

  val delegate = new RaftReplicaChangeDelegate(new RaftReplicaManagerChangeDelegateHelper(this))

  if (config.requiresZookeeper) {
    throw new IllegalStateException(s"Cannot use ${getClass.getSimpleName} when using ZooKeeper")
  }

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

  def endMetadataChangeDeferral(): Unit = {
    val startMs = time.milliseconds()
    val partitionsMadeFollower = mutable.Set[Partition]()
    val partitionsMadeLeader = mutable.Set[Partition]()
    replicaStateChangeLock synchronized {
      stateChangeLogger.info(s"Applying deferred metadata changes")
      val highWatermarkCheckpoints = new LazyOffsetCheckpoints(this.highWatermarkCheckpoints)
      val leadershipChangeCallbacks = {
        mutable.Map[(Iterable[Partition], Iterable[Partition]) => Unit, (mutable.Set[Partition], mutable.Set[Partition])]()
      }
      val metadataImage = metadataCache.currentImage()
      val brokers = metadataImage.brokers
      try {
        val leaderPartitionStates = mutable.Map[Partition, MetadataPartition]()
        val followerPartitionStates = mutable.Map[Partition, MetadataPartition]()
        val partitionsAlreadyExisting = mutable.Set[MetadataPartition]()
        deferredPartitionsIterator.foreach { deferredPartition =>
          val partition = deferredPartition.partition
          val state = cachedState(metadataImage, partition)
          sanityCheckStateWasButIsNoLongerDeferred(state.toTopicPartition, state)
          if (state.leaderId == localBrokerId) {
            leaderPartitionStates.put(partition, state)
          } else {
            followerPartitionStates.put(partition, state)
          }
          if (!deferredPartition.isNew) {
            partitionsAlreadyExisting += state
          }
        }

        val partitionsMadeLeader = delegate.makeLeaders(partitionsAlreadyExisting, leaderPartitionStates,
          highWatermarkCheckpoints, MetadataPartition.OffsetNeverDeferred)
        val partitionsMadeFollower = delegate.makeFollowers(partitionsAlreadyExisting,
          brokers, followerPartitionStates,
          highWatermarkCheckpoints, MetadataPartition.OffsetNeverDeferred)

        // We need to transition anything that hasn't transitioned from Deferred to Offline to the Online state.
        // We also need to identify the leadership change callback(s) to invoke
        deferredPartitionsIterator.foreach { deferredPartition =>
          val partition = deferredPartition.partition
          val state = cachedState(metadataImage, partition)
          val topicPartition = partition.topicPartition
          // identify for callback if necessary
          if (state.leaderId == localBrokerId) {
            if (partitionsMadeLeader.contains(partition)) {
              leadershipChangeCallbacks.getOrElseUpdate(
                deferredPartition.onLeadershipChange, (mutable.Set(), mutable.Set()))._1 += partition
            }
          } else if (partitionsMadeFollower.contains(partition)) {
            leadershipChangeCallbacks.getOrElseUpdate(
              deferredPartition.onLeadershipChange, (mutable.Set(), mutable.Set()))._2 += partition
          }
          // transition from Deferred to Online
          allPartitions.put(topicPartition, HostedPartition.Online(partition))
        }

        updateLeaderAndFollowerMetrics(partitionsMadeFollower.map(_.topic).toSet)

        maybeAddLogDirFetchers(partitionsMadeFollower, highWatermarkCheckpoints)

        replicaFetcherManager.shutdownIdleFetcherThreads()
        replicaAlterLogDirsManager.shutdownIdleFetcherThreads()
        leadershipChangeCallbacks.forKeyValue { (onLeadershipChange, leaderAndFollowerPartitions) =>
          onLeadershipChange(leaderAndFollowerPartitions._1, leaderAndFollowerPartitions._2)
        }
      } catch {
        case e: Throwable =>
          deferredPartitionsIterator.foreach { metadata =>
            val partition = metadata.partition
            val state = cachedState(metadataImage, partition)
            val topicPartition = partition.topicPartition
            val mostRecentMetadataOffset = state.largestDeferredOffsetEverSeen
            val leader = state.leaderId == localBrokerId
            val leaderOrFollower = if (leader) "leader" else "follower"
            val partitionLogMsgPrefix = s"Apply deferred $leaderOrFollower partition $topicPartition last seen in metadata batch $mostRecentMetadataOffset"
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
          sanityCheckStateDeferredAtOffset(topicPartition, currentState, metadataOffset)
          val (partition, priorDeferredMetadata) = getPartition(topicPartition) match {
            case HostedPartition.Offline =>
              stateChangeLogger.warn(s"Ignoring handlePartitionChanges at $metadataOffset " +
                s"for partition $topicPartition as the local replica for the partition is " +
                "in an offline log directory")
              (None, None)

            case HostedPartition.Online(partition) => (Some(partition), None)
            case deferred@HostedPartition.Deferred(partition, _, _) => (Some(partition), Some(deferred))

            case HostedPartition.None =>
              // Create the partition instance since it does not yet exist
              (Some(Partition(topicPartition, time, configRepository, this)), None)
          }
          partition.foreach { partition =>
            val isNew = priorDeferredMetadata match {
              case Some(alreadyDeferred) => alreadyDeferred.isNew
              case _ => prevPartitions.topicPartition(topicPartition.topic(), topicPartition.partition()).isEmpty
            }
            partitionChangesToBeDeferred.put(partition, isNew)
          }
        }

        stateChangeLogger.info(s"Deferring metadata changes for ${partitionChangesToBeDeferred.size} partition(s)")
        if (partitionChangesToBeDeferred.nonEmpty) {
          delegate.makeDeferred(partitionChangesToBeDeferred, metadataOffset, onLeadershipChange)
        }
      } else { // not deferring changes, so make leaders/followers accordingly
        val partitionsToBeLeader = mutable.HashMap[Partition, MetadataPartition]()
        val partitionsToBeFollower = mutable.HashMap[Partition, MetadataPartition]()
        builder.localChanged().foreach { currentState =>
          val topicPartition = currentState.toTopicPartition
          sanityCheckStateNotDeferred(topicPartition, currentState)
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
        val nextBrokers = imageBuilder.nextBrokers()
        val highWatermarkCheckpoints = new LazyOffsetCheckpoints(this.highWatermarkCheckpoints)
        val partitionsBecomeLeader = if (partitionsToBeLeader.nonEmpty)
          delegate.makeLeaders(changedPartitionsPreviouslyExisting, partitionsToBeLeader, highWatermarkCheckpoints,
            metadataOffset)
        else
          Set.empty[Partition]
        val partitionsBecomeFollower = if (partitionsToBeFollower.nonEmpty)
          delegate.makeFollowers(changedPartitionsPreviouslyExisting, nextBrokers, partitionsToBeFollower, highWatermarkCheckpoints,
            metadataOffset)
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

        maybeAddLogDirFetchers(partitionsBecomeFollower, highWatermarkCheckpoints)

        replicaFetcherManager.shutdownIdleFetcherThreads()
        replicaAlterLogDirsManager.shutdownIdleFetcherThreads()
        onLeadershipChange(partitionsBecomeLeader, partitionsBecomeFollower)
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

  private def sanityCheckStateNotDeferred(topicPartition: TopicPartition, state: MetadataPartition): Unit = {
    if (state.isCurrentlyDeferringChanges) {
      throw new IllegalStateException("We are not deferring partition changes but the metadata cache says " +
        s"the partition is deferring changes: $topicPartition")
    }
  }

  private def sanityCheckStateDeferredAtOffset(topicPartition: TopicPartition, state: MetadataPartition, currentMetadataOffset: Long): Unit = {
    if (!state.isCurrentlyDeferringChanges) {
      throw new IllegalStateException("We are deferring partition changes but the metadata cache says " +
        s"the partition is not deferring changes: $topicPartition")
    }
    if (state.largestDeferredOffsetEverSeen != currentMetadataOffset) {
      throw new IllegalStateException(s"We are deferring partition changes at offset $currentMetadataOffset but the metadata cache says " +
        s"the most recent offset at which there was a deferral was offset ${state.largestDeferredOffsetEverSeen}: $topicPartition")
    }
  }

  private def sanityCheckStateWasButIsNoLongerDeferred(topicPartition: TopicPartition, state: MetadataPartition): Unit = {
    if (state.isCurrentlyDeferringChanges) {
      throw new IllegalStateException("We are applying deferred partition changes but the metadata cache says " +
        s"the partition is still deferring changes: $topicPartition")
    }
    if (state.largestDeferredOffsetEverSeen <= 0) {
      throw new IllegalStateException("We are applying deferred partition changes but the metadata cache says " +
        s"the partition has never seen a deferred offset: $topicPartition")
    }
  }
}
