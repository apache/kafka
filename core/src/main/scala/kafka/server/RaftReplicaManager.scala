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

import java.util.concurrent.atomic.AtomicBoolean

import kafka.cluster.Partition
import kafka.log.LogManager
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.checkpoints.{LazyOffsetCheckpoints, OffsetCheckpoints}
import kafka.server.metadata.{ConfigRepository, MetadataBrokers, MetadataImage, MetadataImageBuilder, MetadataPartition, RaftMetadataCache}
import kafka.utils.Implicits.MapExtensionMethods
import kafka.utils.Scheduler
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Time

import scala.collection.{Map, Set, mutable}

class RaftReplicaManager(config: KafkaConfig,
                         metrics: Metrics,
                         time: Time,
                         scheduler: Scheduler,
                         logManager: LogManager,
                         isShuttingDown: AtomicBoolean,
                         quotaManagers: QuotaManagers,
                         brokerTopicStats: BrokerTopicStats,
                         metadataCache: MetadataCache,
                         logDirFailureChannel: LogDirFailureChannel,
                         delayedProducePurgatory: DelayedOperationPurgatory[DelayedProduce],
                         delayedFetchPurgatory: DelayedOperationPurgatory[DelayedFetch],
                         delayedDeleteRecordsPurgatory: DelayedOperationPurgatory[DelayedDeleteRecords],
                         delayedElectLeaderPurgatory: DelayedOperationPurgatory[DelayedElectLeader],
                         threadNamePrefix: Option[String],
                         configRepository: ConfigRepository,
                         alterIsrManager: AlterIsrManager,
                         raftMetadataCache: RaftMetadataCache) extends ReplicaManager(
  config, metrics, time, None, scheduler, logManager, isShuttingDown, quotaManagers,
  brokerTopicStats, metadataCache, logDirFailureChannel, delayedProducePurgatory, delayedFetchPurgatory,
  delayedDeleteRecordsPurgatory, delayedElectLeaderPurgatory, threadNamePrefix, configRepository, alterIsrManager) {

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
      val metadataImage = raftMetadataCache.currentImage()
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

        val partitionsMadeLeader = makeLeaders(partitionsAlreadyExisting, leaderPartitionStates,
          highWatermarkCheckpoints, MetadataPartition.OffsetNeverDeferred)
        val partitionsMadeFollower = makeFollowers(partitionsAlreadyExisting,
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
   * @param imageBuilder        The MetadataImage builder.
   * @param metadataOffset      The last offset in the batch of records.
   * @param onLeadershipChange  The callbacks to invoke when leadership changes.
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
          makeDeferred(partitionChangesToBeDeferred, metadataOffset, onLeadershipChange)
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
          makeLeaders(changedPartitionsPreviouslyExisting, partitionsToBeLeader, highWatermarkCheckpoints,
            metadataOffset)
        else
          Set.empty[Partition]
        val partitionsBecomeFollower = if (partitionsToBeFollower.nonEmpty)
          makeFollowers(changedPartitionsPreviouslyExisting, nextBrokers, partitionsToBeFollower, highWatermarkCheckpoints,
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

  private def makeDeferred(partitionStates: Map[Partition, Boolean],
                           metadataOffset: Long,
                           onLeadershipChange: (Iterable[Partition], Iterable[Partition]) => Unit) : Unit = {
    val traceLoggingEnabled = stateChangeLogger.isTraceEnabled
    if (traceLoggingEnabled)
      partitionStates.forKeyValue { (partition, stateAndMetadata) =>
        stateChangeLogger.trace(s"Metadata batch $metadataOffset: starting the " +
          s"become-deferred transition for partition ${partition.topicPartition}")
      }

    // Stop fetchers for all the partitions
    replicaFetcherManager.removeFetcherForPartitions(partitionStates.keySet.map(_.topicPartition))
    stateChangeLogger.info(s"Metadata batch $metadataOffset: as part of become-deferred request, " +
      s"stopped any fetchers for ${partitionStates.size} partitions")
    // mark all the partitions as deferred
    partitionStates.forKeyValue((partition, isNew) => allPartitions.put(partition.topicPartition,
      HostedPartition.Deferred(partition, isNew, onLeadershipChange)))

    replicaFetcherManager.shutdownIdleFetcherThreads()
    replicaAlterLogDirsManager.shutdownIdleFetcherThreads()

    if (traceLoggingEnabled)
      partitionStates.keys.foreach { partition =>
        stateChangeLogger.trace(s"Completed batch $metadataOffset become-deferred " +
          s"transition for partition ${partition.topicPartition}")
      }
  }

  private def makeLeaders(prevPartitionsAlreadyExisting: Set[MetadataPartition],
                          partitionStates: Map[Partition, MetadataPartition],
                          highWatermarkCheckpoints: OffsetCheckpoints,
                          metadataOffset: Long): Set[Partition] = {
    val partitionsMadeLeaders = mutable.Set[Partition]()
    val traceLoggingEnabled = stateChangeLogger.isTraceEnabled
    val deferredBatches = metadataOffset == MetadataPartition.OffsetNeverDeferred
    val topLevelLogPrefix = if (deferredBatches)
      "Metadata batch <multiple deferred>"
    else
      s"Metadata batch $metadataOffset"
    try {
      // First stop fetchers for all the partitions
      replicaFetcherManager.removeFetcherForPartitions(partitionStates.keySet.map(_.topicPartition))
      stateChangeLogger.info(s"$topLevelLogPrefix: stopped ${partitionStates.size} fetcher(s)")
      // Update the partition information to be the leader
      partitionStates.forKeyValue { (partition, state) =>
        val topicPartition = partition.topicPartition
        val partitionLogMsgPrefix = if (deferredBatches)
          s"Apply deferred leader partition $topicPartition last seen in metadata batch ${state.largestDeferredOffsetEverSeen}"
        else
          s"Metadata batch $metadataOffset $topicPartition"
        try {
          val isrState = state.toLeaderAndIsrPartitionState(
            !prevPartitionsAlreadyExisting(state))
          if (partition.makeLeader(isrState, highWatermarkCheckpoints)) {
            partitionsMadeLeaders += partition
            if (traceLoggingEnabled) {
              stateChangeLogger.trace(s"$partitionLogMsgPrefix: completed the become-leader state change.")
            }
          } else {
            stateChangeLogger.info(s"$partitionLogMsgPrefix: skipped the " +
              "become-leader state change since it is already the leader.")
          }
        } catch {
          case e: KafkaStorageException =>
            stateChangeLogger.error(s"$partitionLogMsgPrefix: unable to make " +
              s"leader because the replica for the partition is offline due to disk error $e")
            val dirOpt = getLogDir(topicPartition)
            error(s"Error while making broker the leader for partition $partition in dir $dirOpt", e)
            markPartitionOffline(topicPartition)
        }
      }
    } catch {
      case e: Throwable =>
        stateChangeLogger.error(s"$topLevelLogPrefix: error while processing batch.", e)
        // Re-throw the exception for it to be caught in BrokerMetadataListener
        throw e
    }
    partitionsMadeLeaders
  }

  private def makeFollowers(prevPartitionsAlreadyExisting: Set[MetadataPartition],
                            currentBrokers: MetadataBrokers,
                            partitionStates: Map[Partition, MetadataPartition],
                            highWatermarkCheckpoints: OffsetCheckpoints,
                            metadataOffset: Long): Set[Partition] = {
    val traceLoggingEnabled = stateChangeLogger.isTraceEnabled
    val deferredBatches = metadataOffset == MetadataPartition.OffsetNeverDeferred
    val topLevelLogPrefix = if (deferredBatches)
      "Metadata batch <multiple deferred>"
    else
      s"Metadata batch $metadataOffset"
    if (traceLoggingEnabled) {
      partitionStates.forKeyValue { (partition, state) =>
        val topicPartition = partition.topicPartition
        val partitionLogMsgPrefix = if (deferredBatches)
          s"Apply deferred follower partition $topicPartition last seen in metadata batch ${state.largestDeferredOffsetEverSeen}"
        else
          s"Metadata batch $metadataOffset $topicPartition"
        stateChangeLogger.trace(s"$partitionLogMsgPrefix: starting the " +
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
          s"Apply deferred follower partition $topicPartition last seen in metadata batch ${state.largestDeferredOffsetEverSeen}"
        else
          s"Metadata batch $metadataOffset $topicPartition"
        try {
          val isNew = !prevPartitionsAlreadyExisting(state)
          if (!acceptableLeaderBrokerIds.contains(state.leaderId)) {
            // The leader broker should always be present in the metadata cache.
            // If not, we should record the error message and abort the transition process for this partition
            stateChangeLogger.error(s"$partitionLogMsgPrefix: cannot become follower " +
              s"since the new leader ${state.leaderId} is unavailable.")
            // Create the local replica even if the leader is unavailable. This is required to ensure that we include
            // the partition's high watermark in the checkpoint file (see KAFKA-1647)
            partition.createLogIfNotExists(isNew, isFutureReplica = false, highWatermarkCheckpoints)
          } else {
            val isrState = state.toLeaderAndIsrPartitionState(isNew)
            if (partition.makeFollower(isrState, highWatermarkCheckpoints)) {
              partitionsMadeFollower += partition
              if (traceLoggingEnabled) {
                stateChangeLogger.trace(s"$partitionLogMsgPrefix: completed the " +
                  s"become-follower state change with new leader ${state.leaderId}.")
              }
            } else {
              stateChangeLogger.info(s"$partitionLogMsgPrefix: skipped the " +
                s"become-follower state change since " +
                s"the new leader ${state.leaderId} is the same as the old leader.")
            }
          }
        } catch {
          case e: KafkaStorageException =>
            stateChangeLogger.error(s"$partitionLogMsgPrefix: unable to complete the " +
              s"become-follower state change since the " +
              s"replica for the partition is offline due to disk error $e")
            val dirOpt = getLogDir(partition.topicPartition)
            error(s"Error while making broker the follower with leader ${state.leaderId} in dir $dirOpt", e)
            markPartitionOffline(topicPartition)
        }
      }

      if (partitionsMadeFollower.nonEmpty) {
        replicaFetcherManager.removeFetcherForPartitions(partitionsMadeFollower.map(_.topicPartition))
        stateChangeLogger.info(s"$topLevelLogPrefix: stopped followers for ${partitionsMadeFollower.size} partitions")

        partitionsMadeFollower.foreach { partition =>
          completeDelayedFetchOrProduceRequests(partition.topicPartition)
        }

        if (isShuttingDown.get()) {
          if (traceLoggingEnabled) {
            partitionsMadeFollower.foreach { partition =>
              val topicPartition = partition.topicPartition
              val state = partitionStates(partition)
              val partitionLogMsgPrefix = if (deferredBatches)
                s"Apply deferred follower partition $topicPartition last seen in metadata batch ${state.largestDeferredOffsetEverSeen}"
              else
                s"Metadata batch $metadataOffset $topicPartition"
              stateChangeLogger.trace(s"$partitionLogMsgPrefix: skipped the " +
                s"adding-fetcher step of the become-follower state for " +
                s"$topicPartition since we are shutting down.")
            }
          }
        } else {
          // we do not need to check if the leader exists again since this has been done at the beginning of this process
          val partitionsToMakeFollowerWithLeaderAndOffset = partitionsMadeFollower.map { partition =>
            val leader = allBrokersByIdMap(partition.leaderReplicaIdOpt.get).brokerEndPoint(config.interBrokerListenerName)
            val log = partition.localLogOrException
            val fetchOffset = initialFetchOffset(log)
            partition.topicPartition -> InitialFetchState(leader, partition.getLeaderEpoch, fetchOffset)
          }.toMap

          replicaFetcherManager.addFetcherForPartitions(partitionsToMakeFollowerWithLeaderAndOffset)
        }
      }
    } catch {
      case e: Throwable =>
        stateChangeLogger.error(s"$topLevelLogPrefix: error while processing batch", e)
        // Re-throw the exception for it to be caught in BrokerMetadataListener
        throw e
    }

    if (traceLoggingEnabled)
      partitionsMadeFollower.foreach { partition =>
        val topicPartition = partition.topicPartition
        val state = partitionStates(partition)
        val partitionLogMsgPrefix = if (deferredBatches)
          s"Apply deferred follower partition $topicPartition last seen in metadata batch ${state.largestDeferredOffsetEverSeen}"
        else
          s"Metadata batch $metadataOffset $topicPartition"
        stateChangeLogger.trace(s"$partitionLogMsgPrefix: completed become-follower " +
          s"transition for partition $topicPartition with new leader ${state.leaderId}")
      }

    partitionsMadeFollower
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
