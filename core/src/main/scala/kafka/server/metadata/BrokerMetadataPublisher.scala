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
import kafka.log.{LogManager, UnifiedLog}
import kafka.server.ConfigAdminManager.toLoggableProps
import kafka.server.{ConfigEntityName, ConfigHandler, ConfigType, FinalizedFeatureCache, KafkaConfig, ReplicaManager, RequestLocal}
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigResource.Type.{BROKER, TOPIC}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.image.{MetadataDelta, MetadataImage, TopicDelta, TopicsImage}
import org.apache.kafka.metadata.authorizer.ClusterMetadataAuthorizer
import org.apache.kafka.server.authorizer.Authorizer

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
                              dynamicConfigHandlers: Map[String, ConfigHandler],
                              private val _authorizer: Option[Authorizer]) extends MetadataPublisher with Logging {
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

        // Update the group coordinator of local changes
        updateCoordinator(
          newImage,
          delta,
          Topic.GROUP_METADATA_TOPIC_NAME,
          groupCoordinator.onElection,
          groupCoordinator.onResignation
        )

        // Update the transaction coordinator of local changes
        updateCoordinator(
          newImage,
          delta,
          Topic.TRANSACTION_STATE_TOPIC_NAME,
          txnCoordinator.onElection,
          txnCoordinator.onResignation
        )

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
        configsDelta.changes().keySet().forEach { resource =>
          val props = newImage.configs().configProperties(resource)
          resource.`type`() match {
            case TOPIC =>
              // Apply changes to a topic's dynamic configuration.
              info(s"Updating topic ${resource.name()} with new configuration : " +
                toLoggableProps(resource, props).mkString(","))
              dynamicConfigHandlers(ConfigType.Topic).
                processConfigChanges(resource.name(), props)
              conf.dynamicConfig.reloadUpdatedFilesWithoutConfigChange(props)
            case BROKER => if (resource.name().isEmpty) {
              // Apply changes to "cluster configs" (also known as default BROKER configs).
              // These are stored in KRaft with an empty name field.
              info(s"Updating cluster configuration : " +
                toLoggableProps(resource, props).mkString(","))
              dynamicConfigHandlers(ConfigType.Broker).
                processConfigChanges(ConfigEntityName.Default, props)
            } else if (resource.name().equals(brokerId.toString)) {
              // Apply changes to this broker's dynamic configuration.
              info(s"Updating broker ${brokerId} with new configuration : " +
                toLoggableProps(resource, props).mkString(","))
              dynamicConfigHandlers(ConfigType.Broker).
                processConfigChanges(resource.name(), props)
            }
            case _ => // nothing to do
          }
        }
      }

      // Apply client quotas delta.
      Option(delta.clientQuotasDelta()).foreach { clientQuotasDelta =>
        clientQuotaMetadataManager.update(clientQuotasDelta)
      }

      // Apply changes to ACLs. This needs to be handled carefully because while we are
      // applying these changes, the Authorizer is continuing to return authorization
      // results in other threads. We never want to expose an invalid state. For example,
      // if the user created a DENY ALL acl and then created an ALLOW ACL for topic foo,
      // we want to apply those changes in that order, not the reverse order! Otherwise
      // there could be a window during which incorrect authorization results are returned.
      Option(delta.aclsDelta()).foreach( aclsDelta =>
        _authorizer match {
          case Some(authorizer: ClusterMetadataAuthorizer) => if (aclsDelta.isSnapshotDelta()) {
            // If the delta resulted from a snapshot load, we want to apply the new changes
            // all at once using ClusterMetadataAuthorizer#loadSnapshot. If this is the
            // first snapshot load, it will also complete the futures returned by
           // Authorizer#start (which we wait for before processing RPCs).
            authorizer.loadSnapshot(newImage.acls().acls())
          } else {
            // Because the changes map is a LinkedHashMap, the deltas will be returned in
            // the order they were performed.
            aclsDelta.changes().entrySet().forEach(e =>
              if (e.getValue().isPresent()) {
                authorizer.addAcl(e.getKey(), e.getValue().get())
              } else {
                authorizer.removeAcl(e.getKey())
              })
          }
          case _ => // No ClusterMetadataAuthorizer is configured. There is nothing to do.
        })

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
  private def updateCoordinator(
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
    // Start log manager, which will perform (potentially lengthy)
    // recovery-from-unclean-shutdown if required.
    logManager.startup(metadataCache.getAllTopics())

    // Make the LogCleaner available for reconfiguration. We can't do this prior to this
    // point because LogManager#startup creates the LogCleaner object, if
    // log.cleaner.enable is true. TODO: improve this (see KAFKA-13610)
    Option(logManager.cleaner).foreach(conf.dynamicConfig.addBrokerReconfigurable)

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
