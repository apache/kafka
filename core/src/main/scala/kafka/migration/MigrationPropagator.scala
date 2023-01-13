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
import org.apache.kafka.common.utils.Time
import org.apache.kafka.image.{MetadataDelta, MetadataImage}
import org.apache.kafka.metadata.migration.LegacyPropagator
import org.apache.kafka.server.common.MetadataVersion

import java.util
import scala.jdk.CollectionConverters._

class MigrationPropagator(
  nodeId: Int,
  config: KafkaConfig
) extends LegacyPropagator {
  @volatile private var _image = MetadataImage.EMPTY
  @volatile private var metadataVersion = MetadataVersion.IBP_3_4_IV0
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
    () => metadataVersion,
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
    val addedBrokers = new util.HashSet[Integer](image.cluster().brokers().keySet())
    addedBrokers.removeAll(oldImage.cluster().brokers().keySet())
    val removedBrokers = new util.HashSet[Integer](oldImage.cluster().brokers().keySet())
    removedBrokers.removeAll(image.cluster().brokers().keySet())

    removedBrokers.asScala.foreach(id => channelManager.removeBroker(id))
    addedBrokers.asScala.foreach(id =>
      channelManager.addBroker(Broker.fromBrokerRegistration(image.cluster().broker(id))))
    _image = image
  }

  override def sendRPCsToBrokersFromMetadataDelta(delta: MetadataDelta, image: MetadataImage,
                                                  zkControllerEpoch: Int): Unit = {
    publishMetadata(image)
    requestBatch.newBatch()

    delta.getOrCreateTopicsDelta()
    delta.getOrCreateClusterDelta()

    val changedZkBrokers = delta.clusterDelta().liveZkBrokerIdChanges().asScala.map(_.toInt).toSet
    val zkBrokers = image.cluster().zkBrokers().keySet().asScala.map(_.toInt).toSet
    val oldZkBrokers = zkBrokers -- changedZkBrokers
    val brokersChanged = !delta.clusterDelta().changedBrokers().isEmpty

    if (changedZkBrokers.nonEmpty) {
      // Update new Zk brokers about all the metadata.
      requestBatch.addUpdateMetadataRequestForBrokers(changedZkBrokers.toSeq, image.topics().partitions().keySet().asScala)
      // Send these requests first to make sure, we don't add all the partition metadata to the
      // old brokers as well.
      requestBatch.sendRequestsToBrokers(zkControllerEpoch)
      requestBatch.newBatch()

      // For new the brokers, check if there are partition assignments and add LISR appropriately.
      image.topics().partitions().asScala.foreach { case (tp, partitionRegistration) =>
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

    // If there are new brokers (including KRaft brokers) or if there are changes in topic
    // metadata, let's send UMR about the changes to the old Zk brokers.
    if (brokersChanged || !delta.topicsDelta().deletedTopicIds().isEmpty || !delta.topicsDelta().changedTopics().isEmpty) {
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
    requestBatch.newBatch()

    // When we need to send RPCs from the image, we're sending 'full' requests meaning we let
    // every broker know about all the metadata and all the LISR requests it needs to handle.
    // Note that we cannot send StopReplica requests from the image. We don't have any state
    // about brokers that host a replica but are not part of the replica set known by the Controller.
    val zkBrokers = image.cluster().zkBrokers().keySet().asScala.map(_.toInt).toSeq
    val partitions = image.topics().partitions()
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
    requestBatch.addUpdateMetadataRequestForBrokers(zkBrokers, partitions.keySet().asScala)
    requestBatch.sendRequestsToBrokers(zkControllerEpoch)
  }

  override def clear(): Unit = {
    requestBatch.clear()
  }

  override def setMetadataVersion(newMetadataVersion: MetadataVersion): Unit = {
    metadataVersion = newMetadataVersion
  }
}
