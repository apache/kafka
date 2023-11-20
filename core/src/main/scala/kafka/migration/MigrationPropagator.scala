/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.migration

import kafka.api.LeaderAndIsr
import kafka.cluster.Broker
import kafka.controller.{ControllerChannelContext, ControllerChannelManager, ReplicaAssignment, StateChangeLogger}
import kafka.server.KafkaConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.requests.AbstractControlRequest
import org.apache.kafka.common.utils.Time
import org.apache.kafka.image.{ClusterImage, MetadataDelta, MetadataImage, TopicsImage}
import org.apache.kafka.metadata.PartitionRegistration
import org.apache.kafka.metadata.migration.LegacyPropagator

import java.util
import scala.jdk.CollectionConverters._
import scala.compat.java8.OptionConverters._

object MigrationPropagator {
  def calculateBrokerChanges(prevClusterImage: ClusterImage, clusterImage: ClusterImage): (Set[Broker], Set[Broker]) = {
    val prevBrokers = prevClusterImage.brokers().values().asScala
      .filter(_.isMigratingZkBroker)
      .filterNot(_.fenced)
      .map(Broker.fromBrokerRegistration)
      .toSet

    val aliveBrokers = clusterImage.brokers().values().asScala
      .filter(_.isMigratingZkBroker)
      .filterNot(_.fenced)
      .map(Broker.fromBrokerRegistration)
      .toSet

    val addedBrokers = aliveBrokers -- prevBrokers
    val removedBrokers = prevBrokers -- aliveBrokers
    (addedBrokers, removedBrokers)
  }
}

class MigrationPropagator(
  nodeId: Int,
  config: KafkaConfig
) extends LegacyPropagator {
  @volatile private var _image = MetadataImage.EMPTY
  val stateChangeLogger = new StateChangeLogger(nodeId, inControllerContext = true, None)
  val channelManager = new ControllerChannelManager(
    () => _image.highestOffsetAndEpoch().epoch(),
    config,
    Time.SYSTEM,
    new Metrics(),
    stateChangeLogger
  )

  val requestBatch = new MigrationPropagatorBatch(
    config,
    metadataProvider,
    () => _image.features().metadataVersion(),
    channelManager,
    stateChangeLogger
  )

  private def metadataProvider(): ControllerChannelContext = {
    new MigrationControllerChannelContext(_image)
  }

  def startup(): Unit = {
    channelManager.startup(Set.empty)
  }

  def shutdown(): Unit = {
    clear()
    channelManager.shutdown()
  }

  override def publishMetadata(image: MetadataImage): Unit = {
    val oldImage = _image

    val (addedBrokers, removedBrokers) = MigrationPropagator.calculateBrokerChanges(oldImage.cluster(), image.cluster())
    if (addedBrokers.nonEmpty || removedBrokers.nonEmpty) {
      stateChangeLogger.logger.info(s"Adding brokers $addedBrokers, removing brokers $removedBrokers.")
    }
    removedBrokers.foreach(broker => channelManager.removeBroker(broker.id))
    addedBrokers.foreach(broker => channelManager.addBroker(broker))
    _image = image
  }

  /**
   * A very expensive function that creates a map with an entry for every partition that exists, from
   * (topic name, partition index) to partition registration.
   */
  def materializePartitions(topicsImage: TopicsImage): util.Map[TopicPartition, PartitionRegistration] = {
    val result = new util.HashMap[TopicPartition, PartitionRegistration]()
    topicsImage.topicsById().values().forEach(topic => {
      topic.partitions().forEach((key, value) => result.put(new TopicPartition(topic.name(), key), value));
    })
    result
  }

  override def sendRPCsToBrokersFromMetadataDelta(delta: MetadataDelta, image: MetadataImage,
                                                  zkControllerEpoch: Int): Unit = {
    publishMetadata(image)
    requestBatch.newBatch()

    delta.getOrCreateTopicsDelta()
    delta.getOrCreateClusterDelta()

    val changedZkBrokers = delta.clusterDelta().changedBrokers().values().asScala.map(_.asScala).filter {
      case None => false
      case Some(registration) => registration.isMigratingZkBroker && !registration.fenced()
    }.map(_.get.id()).toSet

    val zkBrokers = image.cluster().brokers().values().asScala.filter(_.isMigratingZkBroker).map(_.id()).toSet
    val oldZkBrokers = zkBrokers -- changedZkBrokers
    val brokersChanged = !delta.clusterDelta().changedBrokers().isEmpty

    // First send metadata about the live/dead brokers to all the zk brokers.
    if (changedZkBrokers.nonEmpty) {
      // Update new Zk brokers about all the metadata.
      requestBatch.addUpdateMetadataRequestForBrokers(changedZkBrokers.toSeq, materializePartitions(image.topics()).asScala.keySet)
    }
    if (brokersChanged) {
      requestBatch.addUpdateMetadataRequestForBrokers(oldZkBrokers.toSeq)
    }
    requestBatch.sendRequestsToBrokers(zkControllerEpoch)
    requestBatch.newBatch()
    requestBatch.setUpdateType(AbstractControlRequest.Type.INCREMENTAL)

    // Now send LISR, UMR and StopReplica requests for both new zk brokers and existing zk
    // brokers based on the topic changes.
    if (changedZkBrokers.nonEmpty) {
      // For new the brokers, check if there are partition assignments and add LISR appropriately.
      materializePartitions(image.topics()).asScala.foreach { case (tp, partitionRegistration) =>
        val replicas = partitionRegistration.replicas.toSet
        val leaderIsrAndControllerEpochOpt = MigrationControllerChannelContext.partitionLeadershipInfo(image, tp)
        val newBrokersWithReplicas = replicas.intersect(changedZkBrokers)
        if (newBrokersWithReplicas.nonEmpty) {
          leaderIsrAndControllerEpochOpt match {
            case Some(leaderIsrAndControllerEpoch) =>
              val replicaAssignment = ReplicaAssignment(partitionRegistration.replicas,
                partitionRegistration.addingReplicas, partitionRegistration.removingReplicas)
              requestBatch.addLeaderAndIsrRequestForBrokers(newBrokersWithReplicas.toSeq, tp,
                leaderIsrAndControllerEpoch, replicaAssignment, isNew = true)
            case None =>
          }
        }
      }
    }

    // If there are changes in topic metadata, let's send UMR about the changes to the old Zk brokers.
    if (!delta.topicsDelta().deletedTopicIds().isEmpty || !delta.topicsDelta().changedTopics().isEmpty) {
      requestBatch.addUpdateMetadataRequestForBrokers(oldZkBrokers.toSeq)
    }

    // Handle deleted topics by sending appropriate StopReplica and UMR requests to the brokers.
    delta.topicsDelta().deletedTopicIds().asScala.foreach { deletedTopicId =>
      val deletedTopic = delta.image().topics().getTopic(deletedTopicId)
      deletedTopic.partitions().asScala.foreach { case (partition, partitionRegistration) =>
        val tp = new TopicPartition(deletedTopic.name(), partition)
        val offlineReplicas = partitionRegistration.replicas.filter {
          MigrationControllerChannelContext.isReplicaOnline(image, _, partitionRegistration.replicas.toSet)
        }
        val deletedLeaderAndIsr = LeaderAndIsr.duringDelete(partitionRegistration.isr.toList)
        requestBatch.addStopReplicaRequestForBrokers(partitionRegistration.replicas, tp, deletePartition = true)
        requestBatch.addUpdateMetadataRequestForBrokers(
          oldZkBrokers.toSeq, zkControllerEpoch, tp, deletedLeaderAndIsr.leader, deletedLeaderAndIsr.leaderEpoch,
          deletedLeaderAndIsr.partitionEpoch, deletedLeaderAndIsr.isr, partitionRegistration.replicas, offlineReplicas)
      }
    }

    // Handle changes in other topics and send appropriate LeaderAndIsr and UMR requests to the
    // brokers.
    delta.topicsDelta().changedTopics().asScala.foreach { case (_, topicDelta) =>
      topicDelta.partitionChanges().asScala.foreach { case (partition, partitionRegistration) =>
        val tp = new TopicPartition(topicDelta.name(), partition)

        // Check for replica leadership changes.
        val leaderIsrAndControllerEpochOpt = MigrationControllerChannelContext.partitionLeadershipInfo(image, tp)
        leaderIsrAndControllerEpochOpt match {
          case Some(leaderIsrAndControllerEpoch) =>
            val replicaAssignment = ReplicaAssignment(partitionRegistration.replicas,
              partitionRegistration.addingReplicas, partitionRegistration.removingReplicas)
            requestBatch.addLeaderAndIsrRequestForBrokers(replicaAssignment.replicas, tp,
              leaderIsrAndControllerEpoch, replicaAssignment, isNew = true)
          case None =>
        }

        // Check for removed replicas.
        val oldReplicas =
          Option(delta.image().topics().getPartition(topicDelta.id(), tp.partition()))
            .map(_.replicas.toSet)
            .getOrElse(Set.empty)
        val newReplicas = partitionRegistration.replicas.toSet
        val removedReplicas = oldReplicas -- newReplicas
        if (removedReplicas.nonEmpty) {
          requestBatch.addStopReplicaRequestForBrokers(removedReplicas.toSeq, tp, deletePartition = false)
        }
      }
    }
    // Send all the accumulated requests to the broker.
    requestBatch.sendRequestsToBrokers(zkControllerEpoch)
  }

  override def sendRPCsToBrokersFromMetadataImage(image: MetadataImage, zkControllerEpoch: Int): Unit = {
    publishMetadata(image)

    val zkBrokers = image.cluster().brokers().values().asScala.filter(_.isMigratingZkBroker).map(_.id()).toSeq
    val partitions = materializePartitions(image.topics())
    // First send all the metadata before sending any other requests to make sure subsequent
    // requests are handled correctly.
    requestBatch.newBatch()
    requestBatch.addUpdateMetadataRequestForBrokers(zkBrokers, partitions.keySet.asScala)
    requestBatch.sendRequestsToBrokers(zkControllerEpoch)

    requestBatch.newBatch()
    requestBatch.setUpdateType(AbstractControlRequest.Type.FULL)
    // When we need to send RPCs from the image, we're sending 'full' requests meaning we let
    // every broker know about all the metadata and all the LISR requests it needs to handle.
    // Note that we cannot send StopReplica requests from the image. We don't have any state
    // about brokers that host a replica but are not part of the replica set known by the Controller.
    partitions.asScala.foreach{ case (tp, partitionRegistration) =>
      val leaderIsrAndControllerEpochOpt = MigrationControllerChannelContext.partitionLeadershipInfo(image, tp)
      leaderIsrAndControllerEpochOpt match {
        case Some(leaderIsrAndControllerEpoch) =>
          val replicaAssignment = ReplicaAssignment(partitionRegistration.replicas,
            partitionRegistration.addingReplicas, partitionRegistration.removingReplicas)
          requestBatch.addLeaderAndIsrRequestForBrokers(replicaAssignment.replicas, tp,
            leaderIsrAndControllerEpoch, replicaAssignment, isNew = true)
        case None => None
      }
    }
    requestBatch.sendRequestsToBrokers(zkControllerEpoch)
  }

  override def clear(): Unit = {
    requestBatch.clear()
  }
}
