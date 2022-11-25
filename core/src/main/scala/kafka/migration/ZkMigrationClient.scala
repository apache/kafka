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
import kafka.controller.{ControllerChannelManager, LeaderIsrAndControllerEpoch, ReplicaAssignment, StateChangeLogger}
import kafka.migration.ZkMigrationClient.brokerToBrokerRegistration
import kafka.server.{ConfigEntityName, ConfigType, KafkaConfig, ZkAdminManager}
import kafka.utils.Logging
import kafka.zk.TopicZNode.TopicIdReplicaAssignment
import kafka.zk._
import kafka.zookeeper._
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.ControllerMovedException
import org.apache.kafka.common.metadata.ClientQuotaRecord.EntityData
import org.apache.kafka.common.metadata._
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.image.MetadataDelta
import org.apache.kafka.metadata.{BrokerRegistration, PartitionRegistration, VersionRange}
import org.apache.kafka.migration._
import org.apache.kafka.server.common.{ApiMessageAndVersion, MetadataVersion}
import org.apache.zookeeper.CreateMode

import java.util
import java.util.function.Consumer
import java.util.{Collections, Optional}
import scala.collection.Seq
import scala.jdk.CollectionConverters._


object ZkMigrationClient {
  // TODO: This is temporary. Zk brokers will send BrokerRegistration request with IBP 3.4 to
  //  KRaft controller. KRaft controller will only accept such ZkBroker registrations.
  def brokerToBrokerRegistration(broker: Broker, epoch: Long): ZkBrokerRegistration = {
      val registration = new BrokerRegistration(broker.id, epoch, Uuid.ZERO_UUID,
        Collections.emptyMap(), Collections.emptyMap[String, VersionRange],
        Optional.empty(), false, false, MetadataVersion.IBP_3_4_IV0)
      new ZkBrokerRegistration(registration, "3.4", null, true)
  }
}

class ZkMigrationClient(config: KafkaConfig,
                        zkClient: KafkaZkClient,
                        controllerChannelManager: ControllerChannelManager,
                        stateChangeLogger: StateChangeLogger) extends
  MigrationClient with Logging {
  var controllerToBrokerRequestBatch: KRaftControllerBrokerRequestBatch = _

  def claimControllerLeadership(kraftControllerId: Int, kraftControllerEpoch: Int): ZkControllerState = {
    val epochZkVersionOpt = zkClient.tryRegisterKRaftControllerAsActiveController(kraftControllerId, kraftControllerEpoch)
    if (epochZkVersionOpt.isDefined) {
      new ZkControllerState(kraftControllerId, kraftControllerEpoch, epochZkVersionOpt.get)
    } else {
      throw new ControllerMovedException("Cannot claim controller leadership, the controller has moved.")
    }
  }

  def migrateTopics(metadataVersion: MetadataVersion,
                    recordConsumer: Consumer[util.List[ApiMessageAndVersion]],
                    brokerIdConsumer: Consumer[Integer]): Unit = {
    val topics = zkClient.getAllTopicsInCluster()
    val topicConfigs = zkClient.getEntitiesConfigs(ConfigType.Topic, topics)
    val replicaAssignmentAndTopicIds = zkClient.getReplicaAssignmentAndTopicIdForTopics(topics)
    replicaAssignmentAndTopicIds.foreach { case TopicIdReplicaAssignment(topic, topicIdOpt, assignments) =>
      val partitions = assignments.keys.toSeq
      val leaderIsrAndControllerEpochs = zkClient.getTopicPartitionStates(partitions)
      val topicBatch = new util.ArrayList[ApiMessageAndVersion]()
      topicBatch.add(new ApiMessageAndVersion(new TopicRecord()
        .setName(topic)
        .setTopicId(topicIdOpt.get), TopicRecord.HIGHEST_SUPPORTED_VERSION))

      assignments.foreach { case (topicPartition, replicaAssignment) =>
        replicaAssignment.replicas.foreach(brokerIdConsumer.accept(_))
        replicaAssignment.addingReplicas.foreach(brokerIdConsumer.accept(_))

        val leaderIsrAndEpoch = leaderIsrAndControllerEpochs(topicPartition)
        topicBatch.add(new ApiMessageAndVersion(new PartitionRecord()
          .setTopicId(topicIdOpt.get)
          .setPartitionId(topicPartition.partition)
          .setReplicas(replicaAssignment.replicas.map(Integer.valueOf).asJava)
          .setAddingReplicas(replicaAssignment.addingReplicas.map(Integer.valueOf).asJava)
          .setRemovingReplicas(replicaAssignment.removingReplicas.map(Integer.valueOf).asJava)
          .setIsr(leaderIsrAndEpoch.leaderAndIsr.isr.map(Integer.valueOf).asJava)
          .setLeader(leaderIsrAndEpoch.leaderAndIsr.leader)
          .setLeaderEpoch(leaderIsrAndEpoch.leaderAndIsr.leaderEpoch)
          .setPartitionEpoch(leaderIsrAndEpoch.leaderAndIsr.partitionEpoch)
          .setLeaderRecoveryState(leaderIsrAndEpoch.leaderAndIsr.leaderRecoveryState.value()), PartitionRecord.HIGHEST_SUPPORTED_VERSION))
      }

      val props = topicConfigs(topic)
      props.forEach { case (key: Object, value: Object) =>
        topicBatch.add(new ApiMessageAndVersion(new ConfigRecord()
          .setResourceType(ConfigResource.Type.TOPIC.id)
          .setResourceName(topic)
          .setName(key.toString)
          .setValue(value.toString), ConfigRecord.HIGHEST_SUPPORTED_VERSION))
      }

      recordConsumer.accept(topicBatch)
    }
  }

  def migrateBrokerConfigs(metadataVersion: MetadataVersion,
                           recordConsumer: Consumer[util.List[ApiMessageAndVersion]]): Unit = {
    val brokerEntities = zkClient.getAllEntitiesWithConfig(ConfigType.Broker)
    val batch = new util.ArrayList[ApiMessageAndVersion]()
    zkClient.getEntitiesConfigs(ConfigType.Broker, brokerEntities.toSet).foreach { case (broker, props) =>
      val brokerResource = if (broker == ConfigEntityName.Default) {
        ""
      } else {
        broker
      }
      props.forEach { case (key: Object, value: Object) =>
        batch.add(new ApiMessageAndVersion(new ConfigRecord()
          .setResourceType(ConfigResource.Type.BROKER.id)
          .setResourceName(brokerResource)
          .setName(key.toString)
          .setValue(value.toString), ConfigRecord.HIGHEST_SUPPORTED_VERSION))
      }
    }
    recordConsumer.accept(batch)
  }

  def migrateClientQuotas(metadataVersion: MetadataVersion,
                          recordConsumer: Consumer[util.List[ApiMessageAndVersion]]): Unit = {
    val adminZkClient = new AdminZkClient(zkClient)

    def migrateEntityType(entityType: String): Unit = {
      adminZkClient.fetchAllEntityConfigs(entityType).foreach { case (name, props) =>
        val entity = new EntityData().setEntityType(entityType).setEntityName(name)
        val batch = new util.ArrayList[ApiMessageAndVersion]()
        ZkAdminManager.clientQuotaPropsToDoubleMap(props.asScala).foreach { case (key: String, value: Double) =>
          batch.add(new ApiMessageAndVersion(new ClientQuotaRecord()
            .setEntity(List(entity).asJava)
            .setKey(key)
            .setValue(value), ClientQuotaRecord.HIGHEST_SUPPORTED_VERSION))
        }
        recordConsumer.accept(batch)
      }
    }

    migrateEntityType(ConfigType.User)
    migrateEntityType(ConfigType.Client)
    adminZkClient.fetchAllChildEntityConfigs(ConfigType.User, ConfigType.Client).foreach { case (name, props) =>
      // Lifted from ZkAdminManager
      val components = name.split("/")
      if (components.size != 3 || components(1) != "clients")
        throw new IllegalArgumentException(s"Unexpected config path: ${name}")
      val entity = List(
        new EntityData().setEntityType(ConfigType.User).setEntityName(components(0)),
        new EntityData().setEntityType(ConfigType.Client).setEntityName(components(2))
      )

      val batch = new util.ArrayList[ApiMessageAndVersion]()
      ZkAdminManager.clientQuotaPropsToDoubleMap(props.asScala).foreach { case (key: String, value: Double) =>
        batch.add(new ApiMessageAndVersion(new ClientQuotaRecord()
          .setEntity(entity.asJava)
          .setKey(key)
          .setValue(value), ClientQuotaRecord.HIGHEST_SUPPORTED_VERSION))
      }
      recordConsumer.accept(batch)
    }

    migrateEntityType(ConfigType.Ip)
  }

  def migrateProducerId(metadataVersion: MetadataVersion,
                        recordConsumer: Consumer[util.List[ApiMessageAndVersion]]): Unit = {
    val (dataOpt, _) = zkClient.getDataAndVersion(ProducerIdBlockZNode.path)
    dataOpt match {
      case Some(data) =>
        val producerIdBlock = ProducerIdBlockZNode.parseProducerIdBlockData(data)
        recordConsumer.accept(List(new ApiMessageAndVersion(new ProducerIdsRecord()
          .setBrokerEpoch(-1)
          .setBrokerId(producerIdBlock.assignedBrokerId)
          .setNextProducerId(producerIdBlock.firstProducerId), ProducerIdsRecord.HIGHEST_SUPPORTED_VERSION)).asJava)
      case None => // Nothing to migrate
    }
  }

  override def readAllMetadata(batchConsumer: Consumer[util.List[ApiMessageAndVersion]], brokerIdConsumer: Consumer[Integer]): Unit = {
    migrateTopics(MetadataVersion.latest(), batchConsumer, brokerIdConsumer)
    migrateBrokerConfigs(MetadataVersion.latest(), batchConsumer)
    migrateClientQuotas(MetadataVersion.latest(), batchConsumer)
    migrateProducerId(MetadataVersion.latest(), batchConsumer)
  }

  override def watchZkBrokerRegistrations(listener: MigrationClient.BrokerRegistrationListener): Unit = {
    val brokersHandler = new ZNodeChildChangeHandler() {
      override val path: String = BrokerIdsZNode.path

      override def handleChildChange(): Unit = listener.onBrokersChange()
    }
    System.err.println("Adding /brokers watch")
    zkClient.registerZNodeChildChangeHandler(brokersHandler)

    def brokerHandler(brokerId: Int): ZNodeChangeHandler = {
      new ZNodeChangeHandler() {
        override val path: String = BrokerIdZNode.path(brokerId)

        override def handleDataChange(): Unit = listener.onBrokerChange(brokerId)
      }
    }

    val curBrokerAndEpochs = zkClient.getAllBrokerAndEpochsInCluster()
    curBrokerAndEpochs.foreach { case (broker, _) =>
      System.err.println(s"Adding /brokers/${broker.id} watch")
      zkClient.registerZNodeChangeHandlerAndCheckExistence(brokerHandler(broker.id))
    }

    listener.onBrokersChange()
  }

  override def readBrokerRegistration(brokerId: Int): Optional[ZkBrokerRegistration] = {
    val brokerAndEpoch = zkClient.getAllBrokerAndEpochsInCluster(Seq(brokerId))
    if (brokerAndEpoch.isEmpty) {
      Optional.empty()
    } else {
      Optional.of(brokerToBrokerRegistration(brokerAndEpoch.head._1, brokerAndEpoch.head._2))
    }
  }

  override def readBrokerIds(): util.Set[Integer] = {
    zkClient.getSortedBrokerList.map(Integer.valueOf).toSet.asJava
  }

  override def readBrokerIdsFromTopicAssignments(): util.Set[Integer] = {
    val brokerIds = new util.HashSet[Integer]()
    zkClient.getReplicaAssignmentForTopics(zkClient.getAllTopicsInCluster()).foreach { case (tp, assingments) =>
      assingments.foreach(brokerIds.add(_))
    }
    Collections.unmodifiableSet(brokerIds)
  }

  override def addZkBroker(brokerId: Int): Unit = {
    val brokerAndEpoch = zkClient.getAllBrokerAndEpochsInCluster(Seq(brokerId))
    controllerChannelManager.addBroker(brokerAndEpoch.head._1)
  }

  override def removeZkBroker(brokerId: Int): Unit = {
    controllerChannelManager.removeBroker(brokerId)
  }

  override def getOrCreateMigrationRecoveryState(initialState: MigrationRecoveryState): MigrationRecoveryState = {
    zkClient.getOrCreateMigrationState(initialState)
  }

  override def setMigrationRecoveryState(state: MigrationRecoveryState): MigrationRecoveryState = {
    zkClient.updateMigrationState(state)
  }

  override def createTopic(topicName: String, topicId: Uuid, partitions: util.Map[Integer, PartitionRegistration], state: MigrationRecoveryState): MigrationRecoveryState = {
    val assignments = partitions.asScala.map { case (partitionId, partition) =>
      new TopicPartition(topicName, partitionId) -> ReplicaAssignment(partition.replicas, partition.addingReplicas, partition.removingReplicas)
    }

    val createTopicZNode = {
      val path = TopicZNode.path(topicName)
      CreateRequest(
        path,
        TopicZNode.encode(Some(topicId), assignments),
        zkClient.defaultAcls(path),
        CreateMode.PERSISTENT)
    }
    val createPartitionsZNode = {
      val path = TopicPartitionsZNode.path(topicName)
      CreateRequest(
        path,
        null,
        zkClient.defaultAcls(path),
        CreateMode.PERSISTENT)
    }

    val createPartitionZNodeReqs = partitions.asScala.flatMap { case (partitionId, partition) =>
      val topicPartition = new TopicPartition(topicName, partitionId)
      Seq(
        createTopicPartition(topicPartition),
        createTopicPartitionState(topicPartition, partition, state.kraftControllerEpoch())
      )
    }

    val requests = Seq(createTopicZNode, createPartitionsZNode) ++ createPartitionZNodeReqs
    val (migrationZkVersion, responses) = zkClient.retryMigrationRequestsUntilConnected(requests, state.controllerZkVersion(), state)
    responses.foreach(System.err.println)
    state.withZkVersion(migrationZkVersion)
  }

  private def createTopicPartition(topicPartition: TopicPartition): CreateRequest = {
    val path = TopicPartitionZNode.path(topicPartition)
    CreateRequest(path, null, zkClient.defaultAcls(path), CreateMode.PERSISTENT, Some(topicPartition))
  }

  private def createTopicPartitionState(topicPartition: TopicPartition, partitionRegistration: PartitionRegistration, controllerEpoch: Int): CreateRequest = {
    val path = TopicPartitionStateZNode.path(topicPartition)
    val data = TopicPartitionStateZNode.encode(LeaderIsrAndControllerEpoch(new LeaderAndIsr(
      partitionRegistration.leader,
      partitionRegistration.leaderEpoch,
      partitionRegistration.isr.toList,
      partitionRegistration.leaderRecoveryState,
      partitionRegistration.partitionEpoch), controllerEpoch))
    CreateRequest(path, data, zkClient.defaultAcls(path), CreateMode.PERSISTENT, Some(topicPartition))
  }

  private def updateTopicPartitionState(topicPartition: TopicPartition, partitionRegistration: PartitionRegistration, controllerEpoch: Int): SetDataRequest = {
    val path = TopicPartitionStateZNode.path(topicPartition)
    val data = TopicPartitionStateZNode.encode(LeaderIsrAndControllerEpoch(new LeaderAndIsr(
      partitionRegistration.leader,
      partitionRegistration.leaderEpoch,
      partitionRegistration.isr.toList,
      partitionRegistration.leaderRecoveryState,
      partitionRegistration.partitionEpoch), controllerEpoch))
    SetDataRequest(path, data, ZkVersion.MatchAnyVersion, Some(topicPartition))
  }

  override def updateTopicPartitions(topicPartitions: util.Map[String, util.Map[Integer, PartitionRegistration]],
                                     state: MigrationRecoveryState): MigrationRecoveryState = {
    val requests = topicPartitions.asScala.flatMap { case (topicName, partitionRegistrations) =>
      partitionRegistrations.asScala.flatMap { case (partitionId, partitionRegistration) =>
        val topicPartition = new TopicPartition(topicName, partitionId)
        Seq(updateTopicPartitionState(topicPartition, partitionRegistration, state.kraftControllerEpoch()))
      }
    }
    if (requests.isEmpty) {
      state
    } else {
      val (migrationZkVersion, responses) = zkClient.retryMigrationRequestsUntilConnected(requests.toSeq, state.controllerZkVersion(), state)
      responses.foreach(System.err.println)
      state.withZkVersion(migrationZkVersion)
    }
  }

  override def initializeForBrokerRpcs(driver: KRaftMigrationDriver): Unit  ={
    this.controllerToBrokerRequestBatch = new KRaftControllerBrokerRequestBatch(
      config,
      () => new KRaftControllerBrokerRequestMetadata(driver.listener().image()),
      controllerChannelManager,
      stateChangeLogger)
  }

  override def sendRequestsForBrokersFromImage(controllerEpoch: Int): Unit = {
    val metadata = controllerToBrokerRequestBatch.newBatch()
    val image = metadata.getImage

    // We need to send full ISR, updateMetadata requests from image.
    val zkBrokers = image.cluster().zkBrokers().keySet().asScala.map(_.toInt).toSeq
    val partitions = image.topics().partitions()

    // For each partition with leader, add leaderAndIsr
    partitions.asScala.foreach { case (tp, partitionRegistration) =>
      // TODO: Add isFull field to leaderAndIsr.
      val leaderIsrAndControllerEpochOpt = metadata.partitionLeadershipInfo(tp)
      leaderIsrAndControllerEpochOpt match {
        case Some(leaderIsrAndControllerEpoch) =>
          val replicaAssignment = ReplicaAssignment(partitionRegistration.replicas,
            partitionRegistration.addingReplicas, partitionRegistration.removingReplicas)
          controllerToBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(
            replicaAssignment.replicas, tp, leaderIsrAndControllerEpoch, replicaAssignment, isNew = true)
          if (replicaAssignment.removingReplicas.nonEmpty) {
            controllerToBrokerRequestBatch.addStopReplicaRequestForBrokers(replicaAssignment.removingReplicas, tp, deletePartition = false)
          }
        case None => None
      }
    }

    // Add the metadata to all the brokers.
    controllerToBrokerRequestBatch.addUpdateMetadataRequestForBrokers(zkBrokers)

    controllerToBrokerRequestBatch.sendRequestsToBrokers(controllerEpoch)
  }

  override def sendRequestsForBrokersFromDelta(delta: MetadataDelta, controllerEpoch: Int): Unit = {
    val metadata = controllerToBrokerRequestBatch.newBatch()
    val image = metadata.getImage

    // Check for new Zk brokers.
    val newZkBrokers = delta.clusterDelta().newZkBrokers().asScala.map(_.toInt).toSet
    val zkBrokers = image.cluster().zkBrokers().keySet().asScala.map(_.toInt).toSet
    val oldZkBrokers = zkBrokers -- newZkBrokers
    val newBrokersFound = !delta.clusterDelta().newBrokers().isEmpty

    // Send updateMetadata request to new Zk brokers.
    if (newZkBrokers.nonEmpty) {
      image.topics().partitions().asScala.keys
      controllerToBrokerRequestBatch.addUpdateMetadataRequestForBrokers(
        newZkBrokers.toSeq, image.topics().partitions().keySet().asScala)
      controllerToBrokerRequestBatch.sendRequestsToBrokers(controllerEpoch)
    }

    // Send leaderAndIsr requests to appropriate brokers and updateMetadata based on the changes
    // to all the old Zk brokers.
    controllerToBrokerRequestBatch.newBatch(Some(metadata))
    if (newBrokersFound || !delta.topicsDelta().deletedTopicIds().isEmpty || !delta.topicsDelta().changedTopics().isEmpty) {
      controllerToBrokerRequestBatch.addUpdateMetadataRequestForBrokers(oldZkBrokers.toSeq)
    }

    delta.topicsDelta().deletedTopicIds().asScala.foreach { deletedTopicId =>
      val deletedTopic = delta.image().topics().getTopic(deletedTopicId)
      deletedTopic.partitions().asScala.foreach { case (partition, partitionRegistration) =>
        val tp = new TopicPartition(deletedTopic.name(), partition)
        val offlineReplicas = partitionRegistration.replicas.filter(!metadata.isReplicaOnline(_, tp))
        val deletedLeaderAndIsr = LeaderAndIsr.duringDelete(partitionRegistration.isr.toList)
        controllerToBrokerRequestBatch.addStopReplicaRequestForBrokers(partitionRegistration.replicas, tp, deletePartition = true)
        controllerToBrokerRequestBatch.addUpdateMetadataRequestForBrokers(
          oldZkBrokers.toSeq, controllerEpoch, tp, deletedLeaderAndIsr.leader, deletedLeaderAndIsr.leaderEpoch,
          deletedLeaderAndIsr.partitionEpoch, deletedLeaderAndIsr.isr, partitionRegistration.replicas, offlineReplicas)
      }
    }

    delta.topicsDelta().changedTopics().asScala.foreach { case (_, topicDelta) =>
      topicDelta.partitionChanges().asScala.foreach { case (partition, partitionRegistration) =>
        val tp = new TopicPartition(topicDelta.name(), partition)
        val leaderIsrAndControllerEpochOpt = metadata.partitionLeadershipInfo(tp)
        leaderIsrAndControllerEpochOpt match {
          case Some(leaderIsrAndControllerEpoch) =>
            val replicaAssignment = ReplicaAssignment(partitionRegistration.replicas,
              partitionRegistration.addingReplicas, partitionRegistration. removingReplicas)
            controllerToBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(
              replicaAssignment.replicas, tp, leaderIsrAndControllerEpoch, replicaAssignment, isNew = true)
            if (replicaAssignment.removingReplicas.nonEmpty) {
              controllerToBrokerRequestBatch.addStopReplicaRequestForBrokers(replicaAssignment.removingReplicas, tp, deletePartition = false)
            }
          case None => None
        }
      }
    }

    controllerToBrokerRequestBatch.sendRequestsToBrokers(controllerEpoch)
  }
}
