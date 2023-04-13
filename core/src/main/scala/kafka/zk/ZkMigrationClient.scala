/*
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
import kafka.security.authorizer.{AclAuthorizer, AclEntry}
import kafka.security.authorizer.AclAuthorizer.{ResourceOrdering, VersionedAcls}
import kafka.server.{ConfigEntityName, ConfigType, DynamicBrokerConfig, ZkAdminManager}
import kafka.utils.{Logging, PasswordEncoder}
import kafka.zk.TopicZNode.TopicIdReplicaAssignment
import kafka.zookeeper._
import org.apache.kafka.common.acl.AccessControlEntry
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.ControllerMovedException
import org.apache.kafka.common.metadata.ClientQuotaRecord.EntityData
import org.apache.kafka.common.metadata._
import org.apache.kafka.common.quota.ClientQuotaEntity
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.metadata.{LeaderRecoveryState, PartitionRegistration}
import org.apache.kafka.metadata.migration.{MigrationClient, MigrationClientAuthException, MigrationClientException, ZkMigrationLeadershipState}
import org.apache.kafka.server.common.{ApiMessageAndVersion, ProducerIdsBlock}
import org.apache.zookeeper.KeeperException.{AuthFailedException, Code, NoAuthException, SessionClosedRequireAuthException}
import org.apache.zookeeper.{CreateMode, KeeperException}

import java.util
import java.util.Properties
import java.util.function.{BiConsumer, Consumer}
import scala.collection.Seq
import scala.jdk.CollectionConverters._

object ZkMigrationClient {
  val MaxBatchSize = 100
}

/**
 * Migration client in KRaft controller responsible for handling communication to Zookeeper and
 * the ZkBrokers present in the cluster. Methods that directly use KafkaZkClient should use the wrapZkException
 * wrapper function in order to translate KeeperExceptions into something usable by the caller.
 */
class ZkMigrationClient(
  zkClient: KafkaZkClient,
  zkConfigEncoder: PasswordEncoder
) extends MigrationClient with Logging {

  /**
   * Wrap a function such that any KeeperExceptions is captured and converted to a MigrationClientException.
   * Any authentication related exception is converted to a MigrationClientAuthException which may be treated
   * differently by the caller.
   */
  @throws(classOf[MigrationClientException])
  private def wrapZkException[T](fn: => T): T = {
    try {
      fn
    } catch {
      case e @ (_: MigrationClientException | _: MigrationClientAuthException) => throw e
      case e @ (_: AuthFailedException | _: NoAuthException | _: SessionClosedRequireAuthException) =>
        // We don't expect authentication errors to be recoverable, so treat them differently
        throw new MigrationClientAuthException(e)
      case e: KeeperException => throw new MigrationClientException(e)
    }
  }

  override def getOrCreateMigrationRecoveryState(
    initialState: ZkMigrationLeadershipState
  ): ZkMigrationLeadershipState = wrapZkException {
      zkClient.createTopLevelPaths()
      zkClient.createAclPaths()
      zkClient.getOrCreateMigrationState(initialState)
    }

  override def setMigrationRecoveryState(
    state: ZkMigrationLeadershipState
  ): ZkMigrationLeadershipState = wrapZkException {
    zkClient.updateMigrationState(state)
  }

  override def claimControllerLeadership(
    state: ZkMigrationLeadershipState
  ): ZkMigrationLeadershipState = wrapZkException {
    zkClient.tryRegisterKRaftControllerAsActiveController(state.kraftControllerId(), state.kraftControllerEpoch()) match {
      case SuccessfulRegistrationResult(controllerEpoch, controllerEpochZkVersion) =>
        state.withZkController(controllerEpoch, controllerEpochZkVersion)
      case FailedRegistrationResult() => state.withUnknownZkController()
    }
  }

  override def releaseControllerLeadership(
    state: ZkMigrationLeadershipState
  ): ZkMigrationLeadershipState = wrapZkException {
    try {
      zkClient.deleteController(state.zkControllerEpochZkVersion())
      state.withUnknownZkController()
    } catch {
      case _: ControllerMovedException =>
        // If the controller moved, no need to release
        state.withUnknownZkController()
      case t: Throwable =>
        throw new MigrationClientException("Could not release controller leadership due to underlying error", t)
    }
  }

  def migrateTopics(
    recordConsumer: Consumer[util.List[ApiMessageAndVersion]],
    brokerIdConsumer: Consumer[Integer]
  ): Unit = wrapZkException {
    val topics = zkClient.getAllTopicsInCluster()
    val topicConfigs = zkClient.getEntitiesConfigs(ConfigType.Topic, topics)
    val replicaAssignmentAndTopicIds = zkClient.getReplicaAssignmentAndTopicIdForTopics(topics)
    replicaAssignmentAndTopicIds.foreach { case TopicIdReplicaAssignment(topic, topicIdOpt, partitionAssignments) =>
      val partitions = partitionAssignments.keys.toSeq
      val leaderIsrAndControllerEpochs = zkClient.getTopicPartitionStates(partitions)
      val topicBatch = new util.ArrayList[ApiMessageAndVersion]()
      topicBatch.add(new ApiMessageAndVersion(new TopicRecord()
        .setName(topic)
        .setTopicId(topicIdOpt.get), 0.toShort))

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
        topicBatch.add(new ApiMessageAndVersion(record, 0.toShort))
      }

      val props = topicConfigs(topic)
      props.forEach { case (key: Object, value: Object) =>
        topicBatch.add(new ApiMessageAndVersion(new ConfigRecord()
          .setResourceType(ConfigResource.Type.TOPIC.id)
          .setResourceName(topic)
          .setName(key.toString)
          .setValue(value.toString), 0.toShort))
      }
      recordConsumer.accept(topicBatch)
    }
  }

  def migrateBrokerConfigs(
    recordConsumer: Consumer[util.List[ApiMessageAndVersion]]
  ): Unit = wrapZkException {
    val batch = new util.ArrayList[ApiMessageAndVersion]()

    val brokerEntities = zkClient.getAllEntitiesWithConfig(ConfigType.Broker)
    zkClient.getEntitiesConfigs(ConfigType.Broker, brokerEntities.toSet).foreach { case (broker, props) =>
      val brokerResource = if (broker == ConfigEntityName.Default) {
        ""
      } else {
        broker
      }
      props.asScala.foreach { case (key, value) =>
        val newValue = if (DynamicBrokerConfig.isPasswordConfig(key))
          zkConfigEncoder.decode(value).value
        else
          value

        batch.add(new ApiMessageAndVersion(new ConfigRecord()
          .setResourceType(ConfigResource.Type.BROKER.id)
          .setResourceName(brokerResource)
          .setName(key)
          .setValue(newValue), 0.toShort))
      }
    }
    if (!batch.isEmpty) {
      recordConsumer.accept(batch)
    }
  }

  def migrateClientQuotas(
    recordConsumer: Consumer[util.List[ApiMessageAndVersion]]
  ): Unit = wrapZkException {
    val adminZkClient = new AdminZkClient(zkClient)

    def migrateEntityType(entityType: String): Unit = {
      adminZkClient.fetchAllEntityConfigs(entityType).foreach { case (name, props) =>
        val entity = new EntityData().setEntityType(entityType).setEntityName(name)
        val batch = new util.ArrayList[ApiMessageAndVersion]()
        ZkAdminManager.clientQuotaPropsToDoubleMap(props.asScala).foreach { case (key: String, value: Double) =>
          batch.add(new ApiMessageAndVersion(new ClientQuotaRecord()
            .setEntity(List(entity).asJava)
            .setKey(key)
            .setValue(value), 0.toShort))
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
          .setValue(value), 0.toShort))
      }
      recordConsumer.accept(batch)
    }

    migrateEntityType(ConfigType.Ip)
  }

  def migrateProducerId(
    recordConsumer: Consumer[util.List[ApiMessageAndVersion]]
  ): Unit = wrapZkException {
    val (dataOpt, _) = zkClient.getDataAndVersion(ProducerIdBlockZNode.path)
    dataOpt match {
      case Some(data) =>
        val producerIdBlock = ProducerIdBlockZNode.parseProducerIdBlockData(data)
        recordConsumer.accept(List(new ApiMessageAndVersion(new ProducerIdsRecord()
          .setBrokerEpoch(-1)
          .setBrokerId(producerIdBlock.assignedBrokerId)
          .setNextProducerId(producerIdBlock.firstProducerId()), 0.toShort)).asJava)
      case None => // Nothing to migrate
    }
  }

  override def iterateAcls(aclConsumer: BiConsumer[ResourcePattern, util.Set[AccessControlEntry]]): Unit = {
    // This is probably fairly inefficient, but it preserves the semantics from AclAuthorizer (which is non-trivial)
    var allAcls = new scala.collection.immutable.TreeMap[ResourcePattern, VersionedAcls]()(new ResourceOrdering)
    def updateAcls(resourcePattern: ResourcePattern, versionedAcls: VersionedAcls): Unit = {
      allAcls = allAcls.updated(resourcePattern, versionedAcls)
    }
    AclAuthorizer.loadAllAcls(zkClient, this, updateAcls)
    allAcls.foreach { case (resourcePattern, versionedAcls) =>
      aclConsumer.accept(resourcePattern, versionedAcls.acls.map(_.ace).asJava)
    }
  }

  def migrateAcls(recordConsumer: Consumer[util.List[ApiMessageAndVersion]]): Unit = {
    iterateAcls(new util.function.BiConsumer[ResourcePattern, util.Set[AccessControlEntry]]() {
      override def accept(resourcePattern: ResourcePattern, acls: util.Set[AccessControlEntry]): Unit = {
        val batch = new util.ArrayList[ApiMessageAndVersion]()
        acls.asScala.foreach { entry =>
          batch.add(new ApiMessageAndVersion(new AccessControlEntryRecord()
            .setId(Uuid.randomUuid())
            .setResourceType(resourcePattern.resourceType().code())
            .setResourceName(resourcePattern.name())
            .setPatternType(resourcePattern.patternType().code())
            .setPrincipal(entry.principal())
            .setHost(entry.host())
            .setOperation(entry.operation().code())
            .setPermissionType(entry.permissionType().code()), AccessControlEntryRecord.HIGHEST_SUPPORTED_VERSION))
          if (batch.size() == ZkMigrationClient.MaxBatchSize) {
            recordConsumer.accept(batch)
            batch.clear()
          }
        }
        if (!batch.isEmpty) {
          recordConsumer.accept(batch)
        }
      }
    })
  }

  override def readAllMetadata(
    batchConsumer: Consumer[util.List[ApiMessageAndVersion]],
    brokerIdConsumer: Consumer[Integer]
  ): Unit = {
    migrateTopics(batchConsumer, brokerIdConsumer)
    migrateBrokerConfigs(batchConsumer)
    migrateClientQuotas(batchConsumer)
    migrateProducerId(batchConsumer)
    migrateAcls(batchConsumer)
  }

  override def readBrokerIds(): util.Set[Integer] = wrapZkException {
    new util.HashSet[Integer](zkClient.getSortedBrokerList.map(Integer.valueOf).toSet.asJava)
  }

  override def readBrokerIdsFromTopicAssignments(): util.Set[Integer] = wrapZkException {
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

  override def createTopic(
    topicName: String,
    topicId: Uuid,
    partitions: util.Map[Integer, PartitionRegistration],
    state: ZkMigrationLeadershipState
  ): ZkMigrationLeadershipState = wrapZkException {
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
    val (migrationZkVersion, responses) = zkClient.retryMigrationRequestsUntilConnected(requests, state)
    val resultCodes = responses.map { response => response.path -> response.resultCode }.toMap
    if (resultCodes(TopicZNode.path(topicName)).equals(Code.NODEEXISTS)) {
      // topic already created, just return
      state
    } else if (resultCodes.forall { case (_, code) => code.equals(Code.OK) } ) {
      // ok
      state.withMigrationZkVersion(migrationZkVersion)
    } else {
      // not ok
      throw new MigrationClientException(s"Failed to create or update topic $topicName. ZK operation had results $resultCodes")
    }
  }

  private def createTopicPartition(
    topicPartition: TopicPartition
  ): CreateRequest = wrapZkException {
    val path = TopicPartitionZNode.path(topicPartition)
    CreateRequest(path, null, zkClient.defaultAcls(path), CreateMode.PERSISTENT, Some(topicPartition))
  }

  private def partitionStatePathAndData(
    topicPartition: TopicPartition,
    partitionRegistration: PartitionRegistration,
    controllerEpoch: Int
  ): (String, Array[Byte]) = {
    val path = TopicPartitionStateZNode.path(topicPartition)
    val data = TopicPartitionStateZNode.encode(LeaderIsrAndControllerEpoch(LeaderAndIsr(
      partitionRegistration.leader,
      partitionRegistration.leaderEpoch,
      partitionRegistration.isr.toList,
      partitionRegistration.leaderRecoveryState,
      partitionRegistration.partitionEpoch), controllerEpoch))
    (path, data)
  }

  private def createTopicPartitionState(
    topicPartition: TopicPartition,
    partitionRegistration: PartitionRegistration,
    controllerEpoch: Int
  ): CreateRequest = {
    val (path, data) = partitionStatePathAndData(topicPartition, partitionRegistration, controllerEpoch)
    CreateRequest(path, data, zkClient.defaultAcls(path), CreateMode.PERSISTENT, Some(topicPartition))
  }

  private def updateTopicPartitionState(
    topicPartition: TopicPartition,
    partitionRegistration: PartitionRegistration,
    controllerEpoch: Int
  ): SetDataRequest = {
    val (path, data) = partitionStatePathAndData(topicPartition, partitionRegistration, controllerEpoch)
    SetDataRequest(path, data, ZkVersion.MatchAnyVersion, Some(topicPartition))
  }

  override def updateTopicPartitions(
    topicPartitions: util.Map[String, util.Map[Integer, PartitionRegistration]],
    state: ZkMigrationLeadershipState
  ): ZkMigrationLeadershipState = wrapZkException {
    val requests = topicPartitions.asScala.flatMap { case (topicName, partitionRegistrations) =>
      partitionRegistrations.asScala.flatMap { case (partitionId, partitionRegistration) =>
        val topicPartition = new TopicPartition(topicName, partitionId)
        Seq(updateTopicPartitionState(topicPartition, partitionRegistration, state.kraftControllerEpoch()))
      }
    }
    if (requests.isEmpty) {
      state
    } else {
      val (migrationZkVersion, responses) = zkClient.retryMigrationRequestsUntilConnected(requests.toSeq, state)
      val resultCodes = responses.map { response => response.path -> response.resultCode }.toMap
      if (resultCodes.forall { case (_, code) => code.equals(Code.OK) } ) {
        state.withMigrationZkVersion(migrationZkVersion)
      } else {
        throw new MigrationClientException(s"Failed to update partition states: $topicPartitions. ZK transaction had results $resultCodes")
      }
    }
  }

  // Try to update an entity config and the migration state. If NoNode is encountered, it probably means we
  // need to recursively create the parent ZNode. In this case, return None.
  def tryWriteEntityConfig(
    entityType: String,
    path: String,
    props: Properties,
    create: Boolean,
    state: ZkMigrationLeadershipState
  ): Option[ZkMigrationLeadershipState] = wrapZkException {
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

  override def writeClientQuotas(
    entity: util.Map[String, String],
    quotas: util.Map[String, java.lang.Double],
    state: ZkMigrationLeadershipState
  ): ZkMigrationLeadershipState = wrapZkException {
    val entityMap = entity.asScala
    val hasUser = entityMap.contains(ClientQuotaEntity.USER)
    val hasClient = entityMap.contains(ClientQuotaEntity.CLIENT_ID)
    val hasIp = entityMap.contains(ClientQuotaEntity.IP)
    val props = new Properties()
    // We store client quota values as strings in the ZK JSON
    quotas.forEach { case (key, value) => props.put(key, value.toString) }
    val (configType, path) = if (hasUser && !hasClient) {
      (Some(ConfigType.User), Some(entityMap(ClientQuotaEntity.USER)))
    } else if (hasUser && hasClient) {
      (Some(ConfigType.User), Some(s"${entityMap(ClientQuotaEntity.USER)}/clients/${entityMap(ClientQuotaEntity.CLIENT_ID)}"))
    } else if (hasClient) {
      (Some(ConfigType.Client), Some(entityMap(ClientQuotaEntity.CLIENT_ID)))
    } else if (hasIp) {
      (Some(ConfigType.Ip), Some(entityMap(ClientQuotaEntity.IP)))
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
          s"${ConfigEntityTypeZNode.path(configType.get)}/${entityMap(ClientQuotaEntity.USER)}/clients"
        } else {
          ConfigEntityTypeZNode.path(configType.get)
        }
        zkClient.createRecursive(createPath, throwIfPathExists=false)
        debug(s"Recursively creating ZNode $createPath and attempting to write $entity quotas a second time.")

        tryWriteEntityConfig(configType.get, path.get, props, create=true, state) match {
          case Some(newStateSecondTry) => newStateSecondTry
          case None => throw new MigrationClientException(
            s"Could not write client quotas for $entity on second attempt when using Create instead of SetData")
        }
    }
  }

  override def writeProducerId(
    nextProducerId: Long,
    state: ZkMigrationLeadershipState
  ): ZkMigrationLeadershipState = wrapZkException {
    val newProducerIdBlockData = ProducerIdBlockZNode.generateProducerIdBlockJson(
      new ProducerIdsBlock(-1, nextProducerId, ProducerIdsBlock.PRODUCER_ID_BLOCK_SIZE))

    val request = SetDataRequest(ProducerIdBlockZNode.path, newProducerIdBlockData, ZkVersion.MatchAnyVersion)
    val (migrationZkVersion, _) = zkClient.retryMigrationRequestsUntilConnected(Seq(request), state)
    state.withMigrationZkVersion(migrationZkVersion)
  }

  override def writeConfigs(
    resource: ConfigResource,
    configs: util.Map[String, String],
    state: ZkMigrationLeadershipState
  ): ZkMigrationLeadershipState = wrapZkException {
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
            case None => throw new MigrationClientException(
              s"Could not write ${configType.get} configs on second attempt when using Create instead of SetData.")
          }
      }
    } else {
      debug(s"Not updating ZK for $resource since it is not a Broker or Topic entity.")
      state
    }
  }

  private def aclChangeNotificationRequest(resourcePattern: ResourcePattern): CreateRequest = {
    // ZK broker needs the ACL change notification znode to be updated in order to process the new ACLs
    val aclChange = ZkAclStore(resourcePattern.patternType).changeStore.createChangeNode(resourcePattern)
    CreateRequest(aclChange.path, aclChange.bytes, zkClient.defaultAcls(aclChange.path), CreateMode.PERSISTENT_SEQUENTIAL)
  }

  private def tryWriteAcls(
    resourcePattern: ResourcePattern,
    aclEntries: Set[AclEntry],
    create: Boolean,
    state: ZkMigrationLeadershipState
  ): Option[ZkMigrationLeadershipState] = wrapZkException {
    val aclData = ResourceZNode.encode(aclEntries)

    val request = if (create) {
      val path = ResourceZNode.path(resourcePattern)
      CreateRequest(path, aclData, zkClient.defaultAcls(path), CreateMode.PERSISTENT)
    } else {
      SetDataRequest(ResourceZNode.path(resourcePattern), aclData, ZkVersion.MatchAnyVersion)
    }

    val (migrationZkVersion, responses) = zkClient.retryMigrationRequestsUntilConnected(Seq(request), state)
    if (responses.head.resultCode.equals(Code.NONODE)) {
      // Need to call this method again with create=true
      None
    } else {
      // Write the ACL notification outside of a metadata multi-op
      zkClient.retryRequestUntilConnected(aclChangeNotificationRequest(resourcePattern))
      Some(state.withMigrationZkVersion(migrationZkVersion))
    }
  }

  override def writeAddedAcls(
    resourcePattern: ResourcePattern,
    newAcls: util.List[AccessControlEntry],
    state: ZkMigrationLeadershipState
  ): ZkMigrationLeadershipState = {

    val existingAcls = AclAuthorizer.getAclsFromZk(zkClient, resourcePattern)
    val addedAcls = newAcls.asScala.map(new AclEntry(_)).toSet
    val updatedAcls = existingAcls.acls ++ addedAcls

    tryWriteAcls(resourcePattern, updatedAcls, create=false, state) match {
      case Some(newState) => newState
      case None => tryWriteAcls(resourcePattern, updatedAcls, create=true, state) match {
        case Some(newState) => newState
        case None => throw new MigrationClientException(s"Could not write ACLs for resource pattern $resourcePattern")
      }
    }
  }

  override def removeDeletedAcls(
    resourcePattern: ResourcePattern,
    deletedAcls: util.List[AccessControlEntry],
    state: ZkMigrationLeadershipState
  ): ZkMigrationLeadershipState = wrapZkException {

    val existingAcls = AclAuthorizer.getAclsFromZk(zkClient, resourcePattern)
    val removedAcls = deletedAcls.asScala.map(new AclEntry(_)).toSet
    val remainingAcls = existingAcls.acls -- removedAcls

    val request = if (remainingAcls.isEmpty) {
      DeleteRequest(ResourceZNode.path(resourcePattern), ZkVersion.MatchAnyVersion)
    } else {
      val aclData = ResourceZNode.encode(remainingAcls)
      SetDataRequest(ResourceZNode.path(resourcePattern), aclData, ZkVersion.MatchAnyVersion)
    }

    val (migrationZkVersion, responses) = zkClient.retryMigrationRequestsUntilConnected(Seq(request), state)
    if (responses.head.resultCode.equals(Code.OK) || responses.head.resultCode.equals(Code.NONODE)) {
      // Write the ACL notification outside of a metadata multi-op
      zkClient.retryRequestUntilConnected(aclChangeNotificationRequest(resourcePattern))
      state.withMigrationZkVersion(migrationZkVersion)
    } else {
      throw new MigrationClientException(s"Could not delete ACL for resource pattern $resourcePattern")
    }
  }
}
