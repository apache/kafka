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
package kafka.zk

import kafka.api.LeaderAndIsr
import kafka.controller.{LeaderIsrAndControllerEpoch, ReplicaAssignment}
import kafka.server.{ConfigEntityName, ConfigType, ZkAdminManager}
import kafka.utils.Logging
import kafka.zk.TopicZNode.TopicIdReplicaAssignment
import kafka.zookeeper._
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.ControllerMovedException
import org.apache.kafka.common.metadata.ClientQuotaRecord.EntityData
import org.apache.kafka.common.metadata._
import org.apache.kafka.common.quota.ClientQuotaEntity
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.metadata.{LeaderRecoveryState, PartitionRegistration}
import org.apache.kafka.metadata.migration.{MigrationClient, ZkMigrationLeadershipState}
import org.apache.kafka.server.common.{ApiMessageAndVersion, MetadataVersion, ProducerIdsBlock}
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.{CreateMode, KeeperException}

import java.util
import java.util.Properties
import java.util.function.Consumer
import scala.collection.Seq
import scala.jdk.CollectionConverters._


class ZkMigrationClient(zkClient: KafkaZkClient) extends MigrationClient with Logging {

  override def getOrCreateMigrationRecoveryState(initialState: ZkMigrationLeadershipState): ZkMigrationLeadershipState = {
    zkClient.createTopLevelPaths()
    zkClient.getOrCreateMigrationState(initialState)
  }

  override def setMigrationRecoveryState(state: ZkMigrationLeadershipState): ZkMigrationLeadershipState = {
    zkClient.updateMigrationState(state)
  }

  override def claimControllerLeadership(state: ZkMigrationLeadershipState): ZkMigrationLeadershipState = {
    zkClient.tryRegisterKRaftControllerAsActiveController(state.kraftControllerId(), state.kraftControllerEpoch()) match {
      case SuccessfulRegistrationResult(_, controllerEpochZkVersion) => state.withControllerZkVersion(controllerEpochZkVersion)
      case FailedRegistrationResult() => state.withControllerZkVersion(-1)
    }
  }

  override def releaseControllerLeadership(state: ZkMigrationLeadershipState): ZkMigrationLeadershipState = {
    try {
      zkClient.deleteController(state.controllerZkVersion())
      state.withControllerZkVersion(-1)
    } catch {
      case _: ControllerMovedException =>
        // If the controller moved, no need to release
        state.withControllerZkVersion(-1)
      case t: Throwable =>
        throw new RuntimeException("Could not release controller leadership due to underlying error", t)
    }
  }

  def migrateTopics(metadataVersion: MetadataVersion,
                    recordConsumer: Consumer[util.List[ApiMessageAndVersion]],
                    brokerIdConsumer: Consumer[Integer]): Unit = {
    val topics = zkClient.getAllTopicsInCluster()
    val topicConfigs = zkClient.getEntitiesConfigs(ConfigType.Topic, topics)
    val replicaAssignmentAndTopicIds = zkClient.getReplicaAssignmentAndTopicIdForTopics(topics)
    replicaAssignmentAndTopicIds.foreach { case TopicIdReplicaAssignment(topic, topicIdOpt, partitionAssignments) =>
      val partitions = partitionAssignments.keys.toSeq
      val leaderIsrAndControllerEpochs = zkClient.getTopicPartitionStates(partitions)
      val topicBatch = new util.ArrayList[ApiMessageAndVersion]()
      topicBatch.add(new ApiMessageAndVersion(new TopicRecord()
        .setName(topic)
        .setTopicId(topicIdOpt.get), TopicRecord.HIGHEST_SUPPORTED_VERSION))

      partitionAssignments.foreach { case (topicPartition, replicaAssignment) =>
        replicaAssignment.replicas.foreach(brokerIdConsumer.accept(_))
        replicaAssignment.addingReplicas.foreach(brokerIdConsumer.accept(_))
        val replicaList = replicaAssignment.replicas.map(Integer.valueOf).asJava
        val record = new PartitionRecord()
          .setTopicId(topicIdOpt.get)
          .setPartitionId(topicPartition.partition)
          .setReplicas(replicaList)
          .setAddingReplicas(replicaAssignment.addingReplicas.map(Integer.valueOf).asJava)
          .setRemovingReplicas(replicaAssignment.removingReplicas.map(Integer.valueOf).asJava)
        leaderIsrAndControllerEpochs.get(topicPartition) match {
          case Some(leaderIsrAndEpoch) => record
              .setIsr(leaderIsrAndEpoch.leaderAndIsr.isr.map(Integer.valueOf).asJava)
              .setLeader(leaderIsrAndEpoch.leaderAndIsr.leader)
              .setLeaderEpoch(leaderIsrAndEpoch.leaderAndIsr.leaderEpoch)
              .setPartitionEpoch(leaderIsrAndEpoch.leaderAndIsr.partitionEpoch)
              .setLeaderRecoveryState(leaderIsrAndEpoch.leaderAndIsr.leaderRecoveryState.value())
          case None =>
            warn(s"Could not find partition state in ZK for $topicPartition. Initializing this partition " +
              s"with ISR={$replicaList} and leaderEpoch=0.")
            record
              .setIsr(replicaList)
              .setLeader(replicaList.get(0))
              .setLeaderEpoch(0)
              .setPartitionEpoch(0)
              .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED.value())
        }
        topicBatch.add(new ApiMessageAndVersion(record, PartitionRecord.HIGHEST_SUPPORTED_VERSION))
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
    if (!batch.isEmpty) {
      recordConsumer.accept(batch)
    }
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
      // Taken from ZkAdminManager
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

  override def readAllMetadata(batchConsumer: Consumer[util.List[ApiMessageAndVersion]],
                               brokerIdConsumer: Consumer[Integer]): Unit = {
    migrateTopics(MetadataVersion.latest(), batchConsumer, brokerIdConsumer)
    migrateBrokerConfigs(MetadataVersion.latest(), batchConsumer)
    migrateClientQuotas(MetadataVersion.latest(), batchConsumer)
    migrateProducerId(MetadataVersion.latest(), batchConsumer)
  }

  override def readBrokerIds(): util.Set[Integer] = {
    zkClient.getSortedBrokerList.map(Integer.valueOf).toSet.asJava
  }

  override def readBrokerIdsFromTopicAssignments(): util.Set[Integer] = {
    val topics = zkClient.getAllTopicsInCluster()
    val replicaAssignmentAndTopicIds = zkClient.getReplicaAssignmentAndTopicIdForTopics(topics)
    val brokersWithAssignments = new util.HashSet[Integer]()
    replicaAssignmentAndTopicIds.foreach { case TopicIdReplicaAssignment(_, _, assignments) =>
      assignments.values.foreach { assignment =>
        assignment.replicas.foreach { brokerId => brokersWithAssignments.add(brokerId) }
      }
    }
    brokersWithAssignments
  }

  override def createTopic(topicName: String,
                           topicId: Uuid,
                           partitions: util.Map[Integer, PartitionRegistration],
                           state: ZkMigrationLeadershipState): ZkMigrationLeadershipState = {
    val assignments = partitions.asScala.map { case (partitionId, partition) =>
      new TopicPartition(topicName, partitionId) ->
        ReplicaAssignment(partition.replicas, partition.addingReplicas, partition.removingReplicas)
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
    val (migrationZkVersion, _) = zkClient.retryMigrationRequestsUntilConnected(requests, state)
    state.withMigrationZkVersion(migrationZkVersion)
  }

  private def createTopicPartition(topicPartition: TopicPartition): CreateRequest = {
    val path = TopicPartitionZNode.path(topicPartition)
    CreateRequest(path, null, zkClient.defaultAcls(path), CreateMode.PERSISTENT, Some(topicPartition))
  }

  private def partitionStatePathAndData(topicPartition: TopicPartition,
                                        partitionRegistration: PartitionRegistration,
                                        controllerEpoch: Int): (String, Array[Byte]) = {
    val path = TopicPartitionStateZNode.path(topicPartition)
    val data = TopicPartitionStateZNode.encode(LeaderIsrAndControllerEpoch(new LeaderAndIsr(
      partitionRegistration.leader,
      partitionRegistration.leaderEpoch,
      partitionRegistration.isr.toList,
      partitionRegistration.leaderRecoveryState,
      partitionRegistration.partitionEpoch), controllerEpoch))
    (path, data)
  }

  private def createTopicPartitionState(topicPartition: TopicPartition,
                                        partitionRegistration: PartitionRegistration,
                                        controllerEpoch: Int): CreateRequest = {
    val (path, data) = partitionStatePathAndData(topicPartition, partitionRegistration, controllerEpoch)
    CreateRequest(path, data, zkClient.defaultAcls(path), CreateMode.PERSISTENT, Some(topicPartition))
  }

  private def updateTopicPartitionState(topicPartition: TopicPartition,
                                        partitionRegistration: PartitionRegistration,
                                        controllerEpoch: Int): SetDataRequest = {
    val (path, data) = partitionStatePathAndData(topicPartition, partitionRegistration, controllerEpoch)
    SetDataRequest(path, data, ZkVersion.MatchAnyVersion, Some(topicPartition))
  }

  override def updateTopicPartitions(topicPartitions: util.Map[String, util.Map[Integer, PartitionRegistration]],
                                     state: ZkMigrationLeadershipState): ZkMigrationLeadershipState = {
    val requests = topicPartitions.asScala.flatMap { case (topicName, partitionRegistrations) =>
      partitionRegistrations.asScala.flatMap { case (partitionId, partitionRegistration) =>
        val topicPartition = new TopicPartition(topicName, partitionId)
        Seq(updateTopicPartitionState(topicPartition, partitionRegistration, state.kraftControllerEpoch()))
      }
    }
    if (requests.isEmpty) {
      state
    } else {
      val (migrationZkVersion, _) = zkClient.retryMigrationRequestsUntilConnected(requests.toSeq, state)
      state.withMigrationZkVersion(migrationZkVersion)
    }
  }

  // Try to update an entity config and the migration state. If NoNode is encountered, it probably means we
  // need to recursively create the parent ZNode. In this case, return None.
  def tryWriteEntityConfig(entityType: String,
                           path: String,
                           props: Properties,
                           create: Boolean,
                           state: ZkMigrationLeadershipState): Option[ZkMigrationLeadershipState] = {
    val configData = ConfigEntityZNode.encode(props)

    val requests = if (create) {
      Seq(CreateRequest(ConfigEntityZNode.path(entityType, path), configData, zkClient.defaultAcls(path), CreateMode.PERSISTENT))
    } else {
      Seq(SetDataRequest(ConfigEntityZNode.path(entityType, path), configData, ZkVersion.MatchAnyVersion))
    }
    val (migrationZkVersion, responses) = zkClient.retryMigrationRequestsUntilConnected(requests, state)
    if (!create && responses.head.resultCode.equals(Code.NONODE)) {
      // Not fatal. Just means we need to Create this node instead of SetData
      None
    } else if (responses.head.resultCode.equals(Code.OK)) {
      Some(state.withMigrationZkVersion(migrationZkVersion))
    } else {
      throw KeeperException.create(responses.head.resultCode, path)
    }
  }

  def writeClientQuotas(entity: ClientQuotaEntity,
                        quotas: util.Map[String, Double],
                        state: ZkMigrationLeadershipState): ZkMigrationLeadershipState = {
    val entityMap = entity.entries().asScala
    val hasUser = entityMap.contains(ConfigType.User)
    val hasClient = entityMap.contains(ConfigType.Client)
    val hasIp = entityMap.contains(ConfigType.Ip)
    val props = new Properties()
    // We store client quota values as strings in the ZK JSON
    quotas.forEach { case (key, value) => props.put(key, value.toString) }
    val (configType, path) = if (hasUser && !hasClient) {
      (Some(ConfigType.User), Some(entityMap(ConfigType.User)))
    } else if (hasUser && hasClient) {
      (Some(ConfigType.User), Some(s"${entityMap(ConfigType.User)}/clients/${entityMap(ConfigType.Client)}"))
    } else if (hasClient) {
      (Some(ConfigType.Client), Some(entityMap(ConfigType.Client)))
    } else if (hasIp) {
      (Some(ConfigType.Ip), Some(entityMap(ConfigType.Ip)))
    } else {
      (None, None)
    }

    if (path.isEmpty) {
      error(s"Skipping unknown client quota entity $entity")
      return state
    }

    // Try to write the client quota configs once with create=false, and again with create=true if the first operation fails
    tryWriteEntityConfig(configType.get, path.get, props, create=false, state) match {
      case Some(newState) =>
        newState
      case None =>
        // If we didn't update the migration state, we failed to write the client quota. Try again
        // after recursively create its parent znodes
        val createPath = if (hasUser && hasClient) {
          s"${ConfigEntityTypeZNode.path(configType.get)}/${entityMap(ConfigType.User)}/clients"
        } else {
          ConfigEntityTypeZNode.path(configType.get)
        }
        zkClient.createRecursive(createPath, throwIfPathExists=false)
        debug(s"Recursively creating ZNode $createPath and attempting to write $entity quotas a second time.")

        tryWriteEntityConfig(configType.get, path.get, props, create=true, state) match {
          case Some(newStateSecondTry) => newStateSecondTry
          case None => throw new RuntimeException(
            s"Could not write client quotas for $entity on second attempt when using Create instead of SetData")
        }
    }
  }

  def writeProducerId(nextProducerId: Long, state: ZkMigrationLeadershipState): ZkMigrationLeadershipState = {
    val newProducerIdBlockData = ProducerIdBlockZNode.generateProducerIdBlockJson(
      new ProducerIdsBlock(-1, nextProducerId, ProducerIdsBlock.PRODUCER_ID_BLOCK_SIZE))

    val request = SetDataRequest(ProducerIdBlockZNode.path, newProducerIdBlockData, ZkVersion.MatchAnyVersion)
    val (migrationZkVersion, _) = zkClient.retryMigrationRequestsUntilConnected(Seq(request), state)
    state.withMigrationZkVersion(migrationZkVersion)
  }

  def writeConfigs(resource: ConfigResource,
                   configs: util.Map[String, String],
                   state: ZkMigrationLeadershipState): ZkMigrationLeadershipState = {
    val configType = resource.`type`() match {
      case ConfigResource.Type.BROKER => Some(ConfigType.Broker)
      case ConfigResource.Type.TOPIC => Some(ConfigType.Topic)
      case _ => None
    }

    val configName = resource.name()
    if (configType.isDefined) {
      val props = new Properties()
      configs.forEach { case (key, value) => props.put(key, value) }
      tryWriteEntityConfig(configType.get, configName, props, create=false, state) match {
        case Some(newState) =>
          newState
        case None =>
          val createPath = ConfigEntityTypeZNode.path(configType.get)
          debug(s"Recursively creating ZNode $createPath and attempting to write $resource configs a second time.")
          zkClient.createRecursive(createPath, throwIfPathExists=false)

          tryWriteEntityConfig(configType.get, configName, props, create=true, state) match {
            case Some(newStateSecondTry) => newStateSecondTry
            case None => throw new RuntimeException(
              s"Could not write ${configType.get} configs on second attempt when using Create instead of SetData.")
          }
      }
    } else {
      debug(s"Not updating ZK for $resource since it is not a Broker or Topic entity.")
      state
    }
  }
}
