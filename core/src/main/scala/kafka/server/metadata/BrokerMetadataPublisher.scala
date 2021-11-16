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

import kafka.coordinator.group.GroupCoordinator
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.log.{UnifiedLog, LogManager}
import kafka.server.ConfigType
import kafka.server.{ConfigEntityName, ConfigHandler, FinalizedFeatureCache, KafkaConfig, ReplicaManager, RequestLocal}
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.image.{MetadataDelta, MetadataImage, TopicDelta, TopicsImage}

import scala.collection.mutable


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

class BrokerMetadataPublisher(conf: KafkaConfig,
                              metadataCache: KRaftMetadataCache,
                              logManager: LogManager,
                              replicaManager: ReplicaManager,
                              groupCoordinator: GroupCoordinator,
                              txnCoordinator: TransactionCoordinator,
                              clientQuotaMetadataManager: ClientQuotaMetadataManager,
                              featureCache: FinalizedFeatureCache,
                              dynamicConfigHandlers: Map[String, ConfigHandler]) extends MetadataPublisher with Logging {
  logIdent = s"[BrokerMetadataPublisher id=${conf.nodeId}] "

  import BrokerMetadataPublisher._

  /**
   * The broker ID.
   */
  val brokerId = conf.nodeId

  /**
   * True if this is the first time we have published metadata.
   */
  var _firstPublish = true

  override def publish(delta: MetadataDelta, newImage: MetadataImage): Unit = {
    val highestOffsetAndEpoch = newImage.highestOffsetAndEpoch()

    try {
      trace(s"Publishing delta $delta with highest offset $highestOffsetAndEpoch")

      // Publish the new metadata image to the metadata cache.
      metadataCache.setImage(newImage)

      if (_firstPublish) {
        info(s"Publishing initial metadata at offset $highestOffsetAndEpoch.")

        // If this is the first metadata update we are applying, initialize the managers
        // first (but after setting up the metadata cache).
        initializeManagers()
      } else if (isDebugEnabled) {
        debug(s"Publishing metadata at offset $highestOffsetAndEpoch.")
      }

      // Apply feature deltas.
      Option(delta.featuresDelta()).foreach { featuresDelta =>
        featureCache.update(featuresDelta, highestOffsetAndEpoch.offset)
      }

      // Apply topic deltas.
      Option(delta.topicsDelta()).foreach { topicsDelta =>
        // Notify the replica manager about changes to topics.
        replicaManager.applyDelta(topicsDelta, newImage)

        // Handle the case where the old consumer offsets topic was deleted.
        if (topicsDelta.topicWasDeleted(Topic.GROUP_METADATA_TOPIC_NAME)) {
          topicsDelta.image().getTopic(Topic.GROUP_METADATA_TOPIC_NAME).partitions().entrySet().forEach {
            entry =>
              if (entry.getValue().leader == brokerId) {
                groupCoordinator.onResignation(entry.getKey(), Some(entry.getValue().leaderEpoch))
              }
          }
        }
        // Handle the case where we have new local leaders or followers for the consumer
        // offsets topic.
        getTopicDelta(Topic.GROUP_METADATA_TOPIC_NAME, newImage, delta).foreach { topicDelta =>
          val changes = topicDelta.localChanges(brokerId)

          changes.deletes.forEach { topicPartition =>
            groupCoordinator.onResignation(topicPartition.partition, None)
          }
          changes.leaders.forEach { (topicPartition, partitionInfo) =>
            groupCoordinator.onElection(topicPartition.partition, partitionInfo.partition.leaderEpoch)
          }
          changes.followers.forEach { (topicPartition, partitionInfo) =>
            groupCoordinator.onResignation(topicPartition.partition, Some(partitionInfo.partition.leaderEpoch))
          }
        }

        // Handle the case where the old transaction state topic was deleted.
        if (topicsDelta.topicWasDeleted(Topic.TRANSACTION_STATE_TOPIC_NAME)) {
          topicsDelta.image().getTopic(Topic.TRANSACTION_STATE_TOPIC_NAME).partitions().entrySet().forEach {
            entry =>
              if (entry.getValue().leader == brokerId) {
                txnCoordinator.onResignation(entry.getKey(), Some(entry.getValue().leaderEpoch))
              }
          }
        }
        // If the transaction state topic changed in a way that's relevant to this broker,
        // notify the transaction coordinator.
        getTopicDelta(Topic.TRANSACTION_STATE_TOPIC_NAME, newImage, delta).foreach { topicDelta =>
          val changes = topicDelta.localChanges(brokerId)

          changes.deletes.forEach { topicPartition =>
            txnCoordinator.onResignation(topicPartition.partition, None)
          }
          changes.leaders.forEach { (topicPartition, partitionInfo) =>
            txnCoordinator.onElection(topicPartition.partition, partitionInfo.partition.leaderEpoch)
          }
          changes.followers.forEach { (topicPartition, partitionInfo) =>
            txnCoordinator.onResignation(topicPartition.partition, Some(partitionInfo.partition.leaderEpoch))
          }
        }

        // Notify the group coordinator about deleted topics.
        val deletedTopicPartitions = new mutable.ArrayBuffer[TopicPartition]()
        topicsDelta.deletedTopicIds().forEach { id =>
          val topicImage = topicsDelta.image().getTopic(id)
          topicImage.partitions().keySet().forEach {
            id => deletedTopicPartitions += new TopicPartition(topicImage.name(), id)
          }
        }
        if (deletedTopicPartitions.nonEmpty) {
          groupCoordinator.handleDeletedPartitions(deletedTopicPartitions, RequestLocal.NoCaching)
        }
      }

      // Apply configuration deltas.
      Option(delta.configsDelta()).foreach { configsDelta =>
        configsDelta.changes().keySet().forEach { configResource =>
          val tag = configResource.`type`() match {
            case ConfigResource.Type.TOPIC => Some(ConfigType.Topic)
            case ConfigResource.Type.BROKER => Some(ConfigType.Broker)
            case _ => None
          }
          tag.foreach { t =>
            val newProperties = newImage.configs().configProperties(configResource)
            val maybeDefaultName = configResource.name() match {
              case "" => ConfigEntityName.Default
              case k => k
            }
            dynamicConfigHandlers(t).processConfigChanges(maybeDefaultName, newProperties)
          }
        }
      }

      // Apply client quotas delta.
      Option(delta.clientQuotasDelta()).foreach { clientQuotasDelta =>
        clientQuotaMetadataManager.update(clientQuotasDelta)
      }

      if (_firstPublish) {
        finishInitializingReplicaManager(newImage)
      }
    } catch {
      case t: Throwable => error(s"Error publishing broker metadata at $highestOffsetAndEpoch", t)
        throw t
    } finally {
      _firstPublish = false
    }
  }

  private def initializeManagers(): Unit = {
    // Start log manager, which will perform (potentially lengthy)
    // recovery-from-unclean-shutdown if required.
    logManager.startup(metadataCache.getAllTopics())

    // Start the replica manager.
    replicaManager.startup()

    // Start the group coordinator.
    groupCoordinator.startup(() => metadataCache.numPartitions(
      Topic.GROUP_METADATA_TOPIC_NAME).getOrElse(conf.offsetsTopicPartitions))

    // Start the transaction coordinator.
    txnCoordinator.startup(() => metadataCache.numPartitions(
      Topic.TRANSACTION_STATE_TOPIC_NAME).getOrElse(conf.transactionTopicPartitions))
  }

  private def finishInitializingReplicaManager(newImage: MetadataImage): Unit = {
    // Delete log directories which we're not supposed to have, according to the
    // latest metadata. This is only necessary to do when we're first starting up. If
    // we have to load a snapshot later, these topics will appear in deletedTopicIds.
    val strayPartitions = findStrayPartitions(brokerId, newImage.topics, logManager.allLogs)
    if (strayPartitions.nonEmpty) {
      replicaManager.deleteStrayReplicas(strayPartitions)
    }

    // Make sure that the high water mark checkpoint thread is running for the replica
    // manager.
    replicaManager.startHighWatermarkCheckPointThread()
  }
}
