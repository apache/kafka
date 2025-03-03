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

package kafka.server.metadata

import java.util.{OptionalInt, Properties}
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.log.LogManager
import kafka.server.{KafkaConfig, ReplicaManager}
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.coordinator.group.GroupCoordinator
import org.apache.kafka.coordinator.share.ShareCoordinator
import org.apache.kafka.image.loader.LoaderManifest
import org.apache.kafka.image.publisher.MetadataPublisher
import org.apache.kafka.image.{MetadataDelta, MetadataImage, TopicDelta}
import org.apache.kafka.server.common.RequestLocal
import org.apache.kafka.server.fault.FaultHandler

import java.util.concurrent.CompletableFuture
import scala.collection.mutable
import scala.jdk.CollectionConverters._


object BrokerMetadataPublisher extends Logging {
  /**
   * Given a topic name, find out if it changed. Note: if a topic named X was deleted and
   * then re-created, this method will return just the re-creation. The deletion will show
   * up in deletedTopicIds and must be handled separately.
   *
   * @param topicName   The topic name.
   * @param newImage    The new metadata image.
   * @param delta       The metadata delta to search.
   *
   * @return            The delta, or None if appropriate.
   */
  def getTopicDelta(topicName: String,
                    newImage: MetadataImage,
                    delta: MetadataDelta): Option[TopicDelta] = {
    Option(newImage.topics().getTopic(topicName)).flatMap {
      topicImage => Option(delta.topicsDelta()).flatMap {
        topicDelta => Option(topicDelta.changedTopic(topicImage.id()))
      }
    }
  }
}

class BrokerMetadataPublisher(
  config: KafkaConfig,
  metadataCache: KRaftMetadataCache,
  logManager: LogManager,
  replicaManager: ReplicaManager,
  groupCoordinator: GroupCoordinator,
  txnCoordinator: TransactionCoordinator,
  shareCoordinator: Option[ShareCoordinator],
  var dynamicConfigPublisher: DynamicConfigPublisher,
  dynamicClientQuotaPublisher: DynamicClientQuotaPublisher,
  scramPublisher: ScramPublisher,
  delegationTokenPublisher: DelegationTokenPublisher,
  aclPublisher: AclPublisher,
  fatalFaultHandler: FaultHandler,
  metadataPublishingFaultHandler: FaultHandler,
) extends MetadataPublisher with Logging {
  logIdent = s"[BrokerMetadataPublisher id=${config.nodeId}] "

  import BrokerMetadataPublisher._

  /**
   * The broker ID.
   */
  val brokerId: Int = config.nodeId

  /**
   * True if this is the first time we have published metadata.
   */
  var _firstPublish = true

  /**
   * A future that is completed when we first publish.
   */
  val firstPublishFuture = new CompletableFuture[Void]

  override def name(): String = "BrokerMetadataPublisher"

  override def onMetadataUpdate(
    delta: MetadataDelta,
    newImage: MetadataImage,
    manifest: LoaderManifest
  ): Unit = {
    val highestOffsetAndEpoch = newImage.highestOffsetAndEpoch()

    val deltaName = if (_firstPublish) {
      s"initial MetadataDelta up to ${highestOffsetAndEpoch.offset}"
    } else {
      s"MetadataDelta up to ${highestOffsetAndEpoch.offset}"
    }
    try {
      if (isTraceEnabled) {
        trace(s"Publishing delta $delta with highest offset $highestOffsetAndEpoch")
      }

      // Publish the new metadata image to the metadata cache.
      metadataCache.setImage(newImage)

      val metadataVersionLogMsg = s"metadata.version ${newImage.features().metadataVersion()}"

      if (_firstPublish) {
        info(s"Publishing initial metadata at offset $highestOffsetAndEpoch with $metadataVersionLogMsg.")

        // If this is the first metadata update we are applying, initialize the managers
        // first (but after setting up the metadata cache).
        initializeManagers(newImage)
      } else if (isDebugEnabled) {
        debug(s"Publishing metadata at offset $highestOffsetAndEpoch with $metadataVersionLogMsg.")
      }

      // Apply topic deltas.
      Option(delta.topicsDelta()).foreach { topicsDelta =>
        try {
          // Notify the replica manager about changes to topics.
          replicaManager.applyDelta(topicsDelta, newImage)
        } catch {
          case t: Throwable => metadataPublishingFaultHandler.handleFault("Error applying topics " +
            s"delta in $deltaName", t)
        }
        try {
          // Update the group coordinator of local changes
          updateCoordinator(newImage,
            delta,
            Topic.GROUP_METADATA_TOPIC_NAME,
            groupCoordinator.onElection,
            (partitionIndex, leaderEpochOpt) => groupCoordinator.onResignation(partitionIndex, toOptionalInt(leaderEpochOpt))
          )
        } catch {
          case t: Throwable => metadataPublishingFaultHandler.handleFault("Error updating group " +
            s"coordinator with local changes in $deltaName", t)
        }
        try {
          // Update the transaction coordinator of local changes
          updateCoordinator(newImage,
            delta,
            Topic.TRANSACTION_STATE_TOPIC_NAME,
            txnCoordinator.onElection,
            txnCoordinator.onResignation)
        } catch {
          case t: Throwable => metadataPublishingFaultHandler.handleFault("Error updating txn " +
            s"coordinator with local changes in $deltaName", t)
        }
        if (shareCoordinator.isDefined) {
          try {
            updateCoordinator(newImage,
              delta,
              Topic.SHARE_GROUP_STATE_TOPIC_NAME,
              shareCoordinator.get.onElection,
              (partitionIndex, leaderEpochOpt) => shareCoordinator.get.onResignation(partitionIndex, toOptionalInt(leaderEpochOpt))
            )
          } catch {
            case t: Throwable => metadataPublishingFaultHandler.handleFault("Error updating share " +
              s"coordinator with local changes in $deltaName", t)
          }
        }
        try {
          // Notify the group coordinator about deleted topics.
          val deletedTopicPartitions = new mutable.ArrayBuffer[TopicPartition]()
          topicsDelta.deletedTopicIds().forEach { id =>
            val topicImage = topicsDelta.image().getTopic(id)
            topicImage.partitions().keySet().forEach {
              id => deletedTopicPartitions += new TopicPartition(topicImage.name(), id)
            }
          }
          if (deletedTopicPartitions.nonEmpty) {
            groupCoordinator.onPartitionsDeleted(deletedTopicPartitions.asJava, RequestLocal.noCaching.bufferSupplier)
          }
        } catch {
          case t: Throwable => metadataPublishingFaultHandler.handleFault("Error updating group " +
            s"coordinator with deleted partitions in $deltaName", t)
        }
      }

      // Apply configuration deltas.
      dynamicConfigPublisher.onMetadataUpdate(delta, newImage)

      // Apply client quotas delta.
      dynamicClientQuotaPublisher.onMetadataUpdate(delta, newImage)

      // Apply SCRAM delta.
      scramPublisher.onMetadataUpdate(delta, newImage)

      // Apply DelegationToken delta.
      delegationTokenPublisher.onMetadataUpdate(delta, newImage)

      // Apply ACL delta.
      aclPublisher.onMetadataUpdate(delta, newImage, manifest)

      try {
        // Propagate the new image to the group coordinator.
        groupCoordinator.onNewMetadataImage(newImage, delta)
      } catch {
        case t: Throwable => metadataPublishingFaultHandler.handleFault("Error updating group " +
          s"coordinator with local changes in $deltaName", t)
      }

      try {
        // Propagate the new image to the share coordinator.
        shareCoordinator.foreach(coordinator => coordinator.onNewMetadataImage(newImage, delta))
      } catch {
        case t: Throwable => metadataPublishingFaultHandler.handleFault("Error updating share " +
          s"coordinator with local changes in $deltaName", t)
      }

      if (_firstPublish) {
        finishInitializingReplicaManager()
      }
    } catch {
      case t: Throwable => metadataPublishingFaultHandler.handleFault("Uncaught exception while " +
        s"publishing broker metadata from $deltaName", t)
    } finally {
      _firstPublish = false
      firstPublishFuture.complete(null)
    }
  }

  private def toOptionalInt(option: Option[Int]): OptionalInt = {
    option match {
      case Some(leaderEpoch) => OptionalInt.of(leaderEpoch)
      case None => OptionalInt.empty
    }
  }

  def reloadUpdatedFilesWithoutConfigChange(props: Properties): Unit = {
    config.dynamicConfig.reloadUpdatedFilesWithoutConfigChange(props)
  }

  /**
   * Update the coordinator of local replica changes: election and resignation.
   *
   * @param image latest metadata image
   * @param delta metadata delta from the previous image and the latest image
   * @param topicName name of the topic associated with the coordinator
   * @param election function to call on election; the first parameter is the partition id;
   *                 the second parameter is the leader epoch
   * @param resignation function to call on resignation; the first parameter is the partition id;
   *                    the second parameter is the leader epoch
   */
  def updateCoordinator(
    image: MetadataImage,
    delta: MetadataDelta,
    topicName: String,
    election: (Int, Int) => Unit,
    resignation: (Int, Option[Int]) => Unit
  ): Unit = {
    // Handle the case where the topic was deleted
    Option(delta.topicsDelta()).foreach { topicsDelta =>
      if (topicsDelta.topicWasDeleted(topicName)) {
        topicsDelta.image.getTopic(topicName).partitions.entrySet.forEach { entry =>
          if (entry.getValue.leader == brokerId) {
            resignation(entry.getKey, Some(entry.getValue.leaderEpoch))
          }
        }
      }
    }

    // Handle the case where the replica was reassigned, made a leader or made a follower
    getTopicDelta(topicName, image, delta).foreach { topicDelta =>
      val changes = topicDelta.localChanges(brokerId)

      changes.deletes.forEach { topicPartition =>
        resignation(topicPartition.partition, None)
      }
      changes.electedLeaders.forEach { (topicPartition, partitionInfo) =>
        election(topicPartition.partition, partitionInfo.partition.leaderEpoch)
      }
      changes.followers.forEach { (topicPartition, partitionInfo) =>
        resignation(topicPartition.partition, Some(partitionInfo.partition.leaderEpoch))
      }
    }
  }

  private def initializeManagers(newImage: MetadataImage): Unit = {
    try {
      // Start log manager, which will perform (potentially lengthy)
      // recovery-from-unclean-shutdown if required.
      logManager.startup(
        metadataCache.getAllTopics(),
        isStray = log => LogManager.isStrayKraftReplica(brokerId, newImage.topics(), log)
      )

      // Rename all future replicas which are in the same directory as the
      // one assigned by the controller. This can only happen due to a disk
      // failure and broker shutdown after the directory assignment has been
      // updated in the controller but before the future replica could be
      // promoted.
      // See KAFKA-16082 for details.
      logManager.recoverAbandonedFutureLogs(brokerId, newImage.topics())

      // Make the LogCleaner available for reconfiguration. We can't do this prior to this
      // point because LogManager#startup creates the LogCleaner object, if
      // log.cleaner.enable is true. TODO: improve this (see KAFKA-13610)
      Option(logManager.cleaner).foreach(config.dynamicConfig.addBrokerReconfigurable)
    } catch {
      case t: Throwable => fatalFaultHandler.handleFault("Error starting LogManager", t)
    }
    try {
      // Start the replica manager.
      replicaManager.startup()
    } catch {
      case t: Throwable => fatalFaultHandler.handleFault("Error starting ReplicaManager", t)
    }
    try {
      // Start the group coordinator.
      groupCoordinator.startup(() => metadataCache.numPartitions(Topic.GROUP_METADATA_TOPIC_NAME)
        .getOrElse(config.groupCoordinatorConfig.offsetsTopicPartitions))
    } catch {
      case t: Throwable => fatalFaultHandler.handleFault("Error starting GroupCoordinator", t)
    }
    try {
      // Start the transaction coordinator.
      txnCoordinator.startup(() => metadataCache.numPartitions(
        Topic.TRANSACTION_STATE_TOPIC_NAME).getOrElse(config.transactionLogConfig.transactionTopicPartitions))
    } catch {
      case t: Throwable => fatalFaultHandler.handleFault("Error starting TransactionCoordinator", t)
    }
    if (config.shareGroupConfig.isShareGroupEnabled && shareCoordinator.isDefined) {
      try {
        // Start the share coordinator.
        shareCoordinator.get.startup(() => metadataCache.numPartitions(
          Topic.SHARE_GROUP_STATE_TOPIC_NAME).getOrElse(config.shareCoordinatorConfig.shareCoordinatorStateTopicNumPartitions()))
      } catch {
        case t: Throwable => fatalFaultHandler.handleFault("Error starting Share coordinator", t)
      }
    }
  }

  private def finishInitializingReplicaManager(): Unit = {
    try {
      // Make sure that the high water mark checkpoint thread is running for the replica
      // manager.
      replicaManager.startHighWatermarkCheckPointThread()
    } catch {
      case t: Throwable => metadataPublishingFaultHandler.handleFault("Error starting high " +
        "watermark checkpoint thread during startup", t)
    }
  }

  override def close(): Unit = {
    firstPublishFuture.completeExceptionally(new TimeoutException())
  }
}
