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
import kafka.log.{LogManager, UnifiedLog}
import kafka.server.{KafkaConfig, ReplicaManager, RequestLocal}
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.coordinator.group.GroupCoordinator
import org.apache.kafka.image.loader.LoaderManifest
import org.apache.kafka.image.publisher.MetadataPublisher
import org.apache.kafka.image.{MetadataDelta, MetadataImage, TopicDelta, TopicsImage}
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

  /**
   * Find logs which should not be on the current broker, according to the metadata image.
   *
   * @param brokerId        The ID of the current broker.
   * @param newTopicsImage  The new topics image after broker has been reloaded
   * @param logs            A collection of Log objects.
   *
   * @return          The topic partitions which are no longer needed on this broker.
   */
  def findStrayPartitions(brokerId: Int,
                          newTopicsImage: TopicsImage,
                          logs: Iterable[UnifiedLog]): Iterable[TopicPartition] = {
    logs.flatMap { log =>
      val topicId = log.topicId.getOrElse {
        throw new RuntimeException(s"The log dir $log does not have a topic ID, " +
          "which is not allowed when running in KRaft mode.")
      }

      val partitionId = log.topicPartition.partition()
      Option(newTopicsImage.getPartition(topicId, partitionId)) match {
        case Some(partition) =>
          if (!partition.replicas.contains(brokerId)) {
            info(s"Found stray log dir $log: the current replica assignment ${partition.replicas} " +
              s"does not contain the local brokerId $brokerId.")
            Some(log.topicPartition)
          } else {
            None
          }

        case None =>
          info(s"Found stray log dir $log: the topicId $topicId does not exist in the metadata image")
          Some(log.topicPartition)
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
  var dynamicConfigPublisher: DynamicConfigPublisher,
  dynamicClientQuotaPublisher: DynamicClientQuotaPublisher,
  scramPublisher: ScramPublisher,
  delegationTokenPublisher: DelegationTokenPublisher,
  aclPublisher: AclPublisher,
  fatalFaultHandler: FaultHandler,
  metadataPublishingFaultHandler: FaultHandler
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
        initializeManagers()
      } else if (isDebugEnabled) {
        debug(s"Publishing metadata at offset $highestOffsetAndEpoch with $metadataVersionLogMsg.")
      }

      Option(delta.featuresDelta()).foreach { featuresDelta =>
        featuresDelta.metadataVersionChange().ifPresent{ metadataVersion =>
          info(s"Updating metadata.version to ${metadataVersion.featureLevel()} at offset $highestOffsetAndEpoch.")
        }
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
            groupCoordinator.onPartitionsDeleted(deletedTopicPartitions.asJava, RequestLocal.NoCaching.bufferSupplier)
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

      if (_firstPublish) {
        finishInitializingReplicaManager(newImage)
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
      changes.leaders.forEach { (topicPartition, partitionInfo) =>
        election(topicPartition.partition, partitionInfo.partition.leaderEpoch)
      }
      changes.followers.forEach { (topicPartition, partitionInfo) =>
        resignation(topicPartition.partition, Some(partitionInfo.partition.leaderEpoch))
      }
    }
  }

  private def initializeManagers(): Unit = {
    try {
      // Start log manager, which will perform (potentially lengthy)
      // recovery-from-unclean-shutdown if required.
      logManager.startup(metadataCache.getAllTopics())

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
        .getOrElse(config.offsetsTopicPartitions))
    } catch {
      case t: Throwable => fatalFaultHandler.handleFault("Error starting GroupCoordinator", t)
    }
    try {
      // Start the transaction coordinator.
      txnCoordinator.startup(() => metadataCache.numPartitions(
        Topic.TRANSACTION_STATE_TOPIC_NAME).getOrElse(config.transactionTopicPartitions))
    } catch {
      case t: Throwable => fatalFaultHandler.handleFault("Error starting TransactionCoordinator", t)
    }
  }

  private def finishInitializingReplicaManager(newImage: MetadataImage): Unit = {
    try {
      // Delete log directories which we're not supposed to have, according to the
      // latest metadata. This is only necessary to do when we're first starting up. If
      // we have to load a snapshot later, these topics will appear in deletedTopicIds.
      val strayPartitions = findStrayPartitions(brokerId, newImage.topics, logManager.allLogs)
      if (strayPartitions.nonEmpty) {
        replicaManager.deleteStrayReplicas(strayPartitions)
      }
    } catch {
      case t: Throwable => metadataPublishingFaultHandler.handleFault("Error deleting stray " +
        "partitions during startup", t)
    }
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
