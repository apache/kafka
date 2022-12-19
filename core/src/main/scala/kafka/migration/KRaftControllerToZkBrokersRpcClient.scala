package kafka.migration

import kafka.api.LeaderAndIsr
import kafka.cluster.Broker
import kafka.controller.{AbstractControllerBrokerRequestBatch, ControllerBrokerRequestMetadata, ControllerChannelManager, LeaderIsrAndControllerEpoch, ReplicaAssignment, StateChangeLogger}
import kafka.server.KafkaConfig
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.Time
import org.apache.kafka.image.{MetadataDelta, MetadataImage}
import org.apache.kafka.metadata.migration.BrokersRpcClient

import java.util
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

object KRaftControllerBrokerRequestMetadata {
  def isReplicaOnline(image: MetadataImage, brokerId: Int, replicaAssignment: Set[Int]): Boolean = {
    val brokerOnline = image.cluster().containsBroker(brokerId)
    brokerOnline && replicaAssignment.contains(brokerId)
  }

  def partitionReplicaAssignment(image: MetadataImage, tp: TopicPartition): collection.Seq[Int] = {
    image.topics().topicsByName().asScala.get(tp.topic()) match {
      case Some(topic) => topic.partitions().asScala.get(tp.partition()) match {
        case Some(partition) => partition.replicas.toSeq
        case None => collection.Seq.empty
      }
      case None => collection.Seq.empty
    }
  }

  def partitionLeadershipInfo(image: MetadataImage, topicPartition: TopicPartition): Option[LeaderIsrAndControllerEpoch] = {
    image.topics().topicsByName().asScala.get(topicPartition.topic()) match {
      case Some(topic) => topic.partitions().asScala.get(topicPartition.partition()) match {
        case Some(partition) =>
          val leaderAndIsr = LeaderAndIsr(partition.leader, partition.leaderEpoch, partition.isr.toList,
            partition.leaderRecoveryState, partition.partitionEpoch)
          Some(LeaderIsrAndControllerEpoch(leaderAndIsr, image.highestOffsetAndEpoch().epoch()))
        case None => None
      }
      case None => None
    }
  }
}

sealed class KRaftControllerBrokerRequestMetadata(val image: MetadataImage) extends
  ControllerBrokerRequestMetadata {
  override def isTopicDeletionInProgress(topicName: String): Boolean = {
    !image.topics().topicsByName().containsKey(topicName)
  }

  override val topicIds: collection.Map[String, Uuid] = {
    image.topics().topicsByName().asScala.map {
      case (name, topic) => name -> topic.id()
    }.toMap
  }

  override val liveBrokerIdAndEpochs: collection.Map[Int, Long] = {
    image.cluster().zkBrokers().asScala.map {
      case (brokerId, broker) => brokerId.intValue() -> broker.epoch()
    }
  }

  override val liveOrShuttingDownBrokers: collection.Set[Broker] = {
    image.cluster().zkBrokers().asScala.values.map { registration =>
      Broker.fromBrokerRegistration(registration)
    }.toSet
  }

  override def isTopicQueuedUpForDeletion(topic: String): Boolean = {
    !image.topics().topicsByName().containsKey(topic)
  }

  override def isReplicaOnline(brokerId: Int, partition: TopicPartition): Boolean = {
    KRaftControllerBrokerRequestMetadata.isReplicaOnline(
      image, brokerId, partitionReplicaAssignment(partition).toSet)
  }

  override def partitionReplicaAssignment(tp: TopicPartition): collection.Seq[Int] = {
    KRaftControllerBrokerRequestMetadata.partitionReplicaAssignment(image, tp)
  }

  override def leaderEpoch(topicPartition: TopicPartition): Int = {
    // Topic is deleted use a special sentinel -2 to the indicate the same.
    if (isTopicQueuedUpForDeletion(topicPartition.topic())) {
      LeaderAndIsr.EpochDuringDelete
    } else {
      image.topics().topicsByName.asScala.get(topicPartition.topic()) match {
        case Some(topic) => topic.partitions().asScala.get(topicPartition.partition()) match {
          case Some(partition) => partition.leaderEpoch
          case None => LeaderAndIsr.NoEpoch
        }
        case None => LeaderAndIsr.NoEpoch
      }
    }
  }

  override val liveOrShuttingDownBrokerIds: collection.Set[Int] = liveBrokerIdAndEpochs.keySet

  override def partitionLeadershipInfo(topicPartition: TopicPartition): Option[LeaderIsrAndControllerEpoch] = {
    KRaftControllerBrokerRequestMetadata.partitionLeadershipInfo(image, topicPartition)
  }
}

sealed class KRaftControllerBrokerRequestBatch(
  config: KafkaConfig,
  metadataProvider: () => ControllerBrokerRequestMetadata,
  controllerChannelManager: ControllerChannelManager,
  stateChangeLogger: StateChangeLogger
) extends AbstractControllerBrokerRequestBatch(
  config,
  metadataProvider,
  stateChangeLogger,
  kraftController = true
) {

  override def sendRequest(brokerId: Int, request: AbstractControlRequest.Builder[_ <: AbstractControlRequest], callback: AbstractResponse => Unit): Unit = {
    controllerChannelManager.sendRequest(brokerId, request, callback)
  }

  override def handleLeaderAndIsrResponse(response: LeaderAndIsrResponse, broker: Int): Unit = {
    if (response.error != Errors.NONE) {
      stateChangeLogger.error(s"Received error ${response.error} in LeaderAndIsr " +
        s"response $response from broker $broker")
      return
    }
    val partitionErrors = response.partitionErrors(
      metadataProvider().topicIds.map { case (id, string) => (string, id) }.asJava)
    val offlineReplicas = new ArrayBuffer[TopicPartition]()
    partitionErrors.forEach{ case(tp, error) =>
      if (error == Errors.KAFKA_STORAGE_ERROR) {
        offlineReplicas += tp
      }
    }
    if (offlineReplicas.nonEmpty) {
      stateChangeLogger.error(s"Found ${offlineReplicas.mkString(",")} on broker $broker as offline")
    }
  }

  override def handleUpdateMetadataResponse(response: UpdateMetadataResponse, broker: Int): Unit = {
    if (response.error != Errors.NONE) {
      stateChangeLogger.error(s"Received error ${response.error} in UpdateMetadata " +
        s"response $response from broker $broker")
    }
  }

  override def handleStopReplicaResponse(response: StopReplicaResponse, broker: Int,
                                         partitionErrorsForDeletingTopics: Map[TopicPartition, Errors]): Unit = {
    if (response.error() != Errors.NONE) {
      stateChangeLogger.error(s"Received error ${response.error} in StopReplica " +
        s"response $response from broker $broker")
    }
    partitionErrorsForDeletingTopics.foreach{ case(tp, error) =>
      if (error != Errors.NONE) {
        stateChangeLogger.error(s"Received error $error in StopReplica request for partition $tp " +
          s"from broker $broker")
      }
    }
  }
}

class KRaftControllerToZkBrokersRpcClient(
  nodeId: Int,
  config: KafkaConfig
) extends BrokersRpcClient {
  @volatile private var _image = MetadataImage.EMPTY
  val stateChangeLogger = new StateChangeLogger(nodeId, inControllerContext = false, None)
  val channelManager = new ControllerChannelManager(
    () => _image.highestOffsetAndEpoch().epoch(),
    config,
    Time.SYSTEM,
    new Metrics(),
    stateChangeLogger
  )

  val requestBatch = new KRaftControllerBrokerRequestBatch(
    config,
    metadataProvider,
    channelManager,
    stateChangeLogger
  )

  private def metadataProvider(): ControllerBrokerRequestMetadata = {
    new KRaftControllerBrokerRequestMetadata(_image)
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
                                                  controllerEpoch: Int): Unit = {
    publishMetadata(image)
    requestBatch.newBatch()

    val newZkBrokers = delta.clusterDelta().newZkBrokers().asScala.map(_.toInt).toSet
    val zkBrokers = image.cluster().zkBrokers().keySet().asScala.map(_.toInt).toSet
    val oldZkBrokers = zkBrokers -- newZkBrokers
    val newBrokersFound = !delta.clusterDelta().newBrokers().isEmpty

    if (newZkBrokers.nonEmpty) {
      // Update new Zk brokers about all the metadata.
      requestBatch.addUpdateMetadataRequestForBrokers(newZkBrokers.toSeq, image.topics().partitions().keySet().asScala)
      // Send these requests first to make sure, we don't add all the partition metadata to the
      // old brokers as well.
      requestBatch.sendRequestsToBrokers(controllerEpoch)
      requestBatch.newBatch()

      // For new the brokers, check if there are partition assignments and add LISR appropriately.
      image.topics().partitions().asScala.foreach { case (tp, partitionRegistration) =>
        val replicas = partitionRegistration.replicas.toSet
        val leaderIsrAndControllerEpochOpt = KRaftControllerBrokerRequestMetadata.partitionLeadershipInfo(image, tp)
        val newBrokersWithReplicas = replicas.intersect(newZkBrokers)
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
    if (newBrokersFound || !delta.topicsDelta().deletedTopicIds().isEmpty || !delta.topicsDelta().changedTopics().isEmpty) {
      requestBatch.addUpdateMetadataRequestForBrokers(oldZkBrokers.toSeq)
    }

    // Handle deleted topics by sending appropriate StopReplica and UMR requests to the brokers.
    delta.topicsDelta().deletedTopicIds().asScala.foreach { deletedTopicId =>
      val deletedTopic = delta.image().topics().getTopic(deletedTopicId)
      deletedTopic.partitions().asScala.foreach { case (partition, partitionRegistration) =>
        val tp = new TopicPartition(deletedTopic.name(), partition)
        val offlineReplicas = partitionRegistration.replicas.filter {
          KRaftControllerBrokerRequestMetadata.isReplicaOnline(image, _, partitionRegistration.replicas.toSet)
        }
        val deletedLeaderAndIsr = LeaderAndIsr.duringDelete(partitionRegistration.isr.toList)
        requestBatch.addStopReplicaRequestForBrokers(partitionRegistration.replicas, tp, deletePartition = true)
        requestBatch.addUpdateMetadataRequestForBrokers(
          oldZkBrokers.toSeq, controllerEpoch, tp, deletedLeaderAndIsr.leader, deletedLeaderAndIsr.leaderEpoch,
          deletedLeaderAndIsr.partitionEpoch, deletedLeaderAndIsr.isr, partitionRegistration.replicas, offlineReplicas)
      }
    }

    // Handle changes in other topics and send appropriate LeaderAndIsr and UMR requests to the
    // brokers.
    delta.topicsDelta().changedTopics().asScala.foreach { case (_, topicDelta) =>
      topicDelta.partitionChanges().asScala.foreach { case (partition, partitionRegistration) =>
        val tp = new TopicPartition(topicDelta.name(), partition)

        // Check for replica leadership changes.
        val leaderIsrAndControllerEpochOpt = KRaftControllerBrokerRequestMetadata.partitionLeadershipInfo(image, tp)
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
    requestBatch.sendRequestsToBrokers(controllerEpoch)
  }

  override def sendRPCsToBrokersFromMetadataImage(image: MetadataImage, controllerEpoch: Int): Unit = {
    publishMetadata(image)
    requestBatch.newBatch()

    // When we need to send RPCs from the image, we're sending 'full' requests meaning we let
    // zk every broker know about all the metadata and all the LISR requests it needs to handle.
    // Note that we cannot send StopReplica requests from image because we don't have any state
    // about brokers that host a replica but not part of the replica set known by the Controller.
    val zkBrokers = image.cluster().zkBrokers().keySet().asScala.map(_.toInt).toSeq
    val partitions = image.topics().partitions()
    partitions.asScala.foreach{ case (tp, partitionRegistration) =>
      val leaderIsrAndControllerEpochOpt = KRaftControllerBrokerRequestMetadata.partitionLeadershipInfo(image, tp)
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
    requestBatch.sendRequestsToBrokers(controllerEpoch)
  }

  override def clear(): Unit = {
    requestBatch.clear()
  }
}
