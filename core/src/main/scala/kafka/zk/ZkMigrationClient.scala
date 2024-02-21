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

import kafka.utils.Logging
import kafka.zk.ZkMigrationClient.wrapZkException
import kafka.zk.migration.{ZkAclMigrationClient, ZkConfigMigrationClient, ZkDelegationTokenMigrationClient, ZkTopicMigrationClient}
import kafka.zookeeper._
import org.apache.kafka.clients.admin.ScramMechanism
import org.apache.kafka.common.acl.AccessControlEntry
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.ControllerMovedException
import org.apache.kafka.common.metadata._
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.security.scram.ScramCredential
import org.apache.kafka.common.{TopicIdPartition, Uuid}
import org.apache.kafka.metadata.DelegationTokenData
import org.apache.kafka.metadata.PartitionRegistration
import org.apache.kafka.metadata.migration.ConfigMigrationClient.ClientQuotaVisitor
import org.apache.kafka.metadata.migration.TopicMigrationClient.{TopicVisitor, TopicVisitorInterest}
import org.apache.kafka.metadata.migration._
import org.apache.kafka.security.PasswordEncoder
import org.apache.kafka.server.common.{ApiMessageAndVersion, ProducerIdsBlock}
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.{AuthFailedException, NoAuthException, SessionClosedRequireAuthException}

import java.{lang, util}
import java.util.function.Consumer
import scala.collection.Seq
import scala.compat.java8.OptionConverters._
import scala.jdk.CollectionConverters._

object ZkMigrationClient {

  private val MaxBatchSize = 100

  def apply(
    zkClient: KafkaZkClient,
    zkConfigEncoder: PasswordEncoder
  ): ZkMigrationClient = {
    val topicClient = new ZkTopicMigrationClient(zkClient)
    val configClient = new ZkConfigMigrationClient(zkClient, zkConfigEncoder)
    val aclClient = new ZkAclMigrationClient(zkClient)
    val delegationTokenClient = new ZkDelegationTokenMigrationClient(zkClient)
    new ZkMigrationClient(zkClient, topicClient, configClient, aclClient, delegationTokenClient)
  }

  /**
   * Wrap a function such that any KeeperExceptions is captured and converted to a MigrationClientException.
   * Any authentication related exception is converted to a MigrationClientAuthException which may be treated
   * differently by the caller.
   */
  @throws(classOf[MigrationClientException])
  def wrapZkException[T](fn: => T): T = {
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

  @throws(classOf[MigrationClientException])
  def logAndRethrow[T](logger: Logging, msg: String)(fn: => T): T = {
    try {
      fn
    } catch {
      case e: Throwable =>
        logger.error(msg, e)
        throw e
    }
  }
}


/**
 * Migration client in KRaft controller responsible for handling communication to Zookeeper and
 * the ZkBrokers present in the cluster. Methods that directly use KafkaZkClient should use the wrapZkException
 * wrapper function in order to translate KeeperExceptions into something usable by the caller.
 */
class ZkMigrationClient(
  zkClient: KafkaZkClient,
  topicClient: TopicMigrationClient,
  configClient: ConfigMigrationClient,
  aclClient: AclMigrationClient,
  delegationTokenClient: DelegationTokenMigrationClient
) extends MigrationClient with Logging {

  override def getOrCreateMigrationRecoveryState(
    initialState: ZkMigrationLeadershipState
  ): ZkMigrationLeadershipState = wrapZkException {
      zkClient.createTopLevelPaths()
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
    var topicBatch = new util.ArrayList[ApiMessageAndVersion]()
    topicClient.iterateTopics(
      util.EnumSet.allOf(classOf[TopicVisitorInterest]),
      new TopicVisitor() {
        override def visitTopic(topicName: String, topicId: Uuid, assignments: util.Map[Integer, util.List[Integer]]): Unit = {
          if (!topicBatch.isEmpty) {
            recordConsumer.accept(topicBatch)
            topicBatch = new util.ArrayList[ApiMessageAndVersion]()
          }

          topicBatch.add(new ApiMessageAndVersion(new TopicRecord()
            .setName(topicName)
            .setTopicId(topicId), 0.toShort))

          // This breaks the abstraction a bit, but the topic configs belong in the topic batch
          // when migrating topics and the logic for reading configs lives elsewhere
          configClient.readTopicConfigs(topicName, (topicConfigs: util.Map[String, String]) => {
            topicConfigs.forEach((key: Any, value: Any) => {
              topicBatch.add(new ApiMessageAndVersion(new ConfigRecord()
                .setResourceType(ConfigResource.Type.TOPIC.id)
                .setResourceName(topicName)
                .setName(key.toString)
                .setValue(value.toString), 0.toShort))
            })
          })
        }

        override def visitPartition(topicIdPartition: TopicIdPartition, partitionRegistration: PartitionRegistration): Unit = {
          val record = new PartitionRecord()
            .setTopicId(topicIdPartition.topicId())
            .setPartitionId(topicIdPartition.partition())
            .setReplicas(partitionRegistration.replicas.map(Integer.valueOf).toList.asJava)
            .setAddingReplicas(partitionRegistration.addingReplicas.map(Integer.valueOf).toList.asJava)
            .setRemovingReplicas(partitionRegistration.removingReplicas.map(Integer.valueOf).toList.asJava)
            .setIsr(partitionRegistration.isr.map(Integer.valueOf).toList.asJava)
            .setLeader(partitionRegistration.leader)
            .setLeaderEpoch(partitionRegistration.leaderEpoch)
            .setPartitionEpoch(partitionRegistration.partitionEpoch)
            .setLeaderRecoveryState(partitionRegistration.leaderRecoveryState.value())
          partitionRegistration.replicas.foreach(brokerIdConsumer.accept(_))
          partitionRegistration.addingReplicas.foreach(brokerIdConsumer.accept(_))
          topicBatch.add(new ApiMessageAndVersion(record, 0.toShort))
        }
      }
    )

    if (!topicBatch.isEmpty) {
      recordConsumer.accept(topicBatch)
    }
  }

  def migrateBrokerConfigs(
    recordConsumer: Consumer[util.List[ApiMessageAndVersion]],
    brokerIdConsumer: Consumer[Integer]
  ): Unit = wrapZkException {
    configClient.iterateBrokerConfigs((broker, props) => {
      if (broker.nonEmpty) {
        brokerIdConsumer.accept(Integer.valueOf(broker))
      }
      val batch = new util.ArrayList[ApiMessageAndVersion]()
      props.forEach((key, value) => {
        batch.add(new ApiMessageAndVersion(new ConfigRecord()
          .setResourceType(ConfigResource.Type.BROKER.id)
          .setResourceName(broker)
          .setName(key)
          .setValue(value), 0.toShort))
      })
      if (!batch.isEmpty) {
        recordConsumer.accept(batch)
      }
    })
  }

  def migrateClientQuotas(
    recordConsumer: Consumer[util.List[ApiMessageAndVersion]]
  ): Unit = wrapZkException {
    configClient.iterateClientQuotas(new ClientQuotaVisitor {
      override def visitClientQuota(
        entityDataList: util.List[ClientQuotaRecord.EntityData],
        quotas: util.Map[String, lang.Double]
      ): Unit = {
        val batch = new util.ArrayList[ApiMessageAndVersion]()
        quotas.forEach((key, value) => {
          batch.add(new ApiMessageAndVersion(new ClientQuotaRecord()
            .setEntity(entityDataList)
            .setKey(key)
            .setValue(value), 0.toShort))
        })
        recordConsumer.accept(batch)
      }

      override def visitScramCredential(
        userName: String,
        scramMechanism: ScramMechanism,
        scramCredential: ScramCredential
      ): Unit = {
        val batch = new util.ArrayList[ApiMessageAndVersion]()
        batch.add(new ApiMessageAndVersion(new UserScramCredentialRecord()
          .setName(userName)
          .setMechanism(scramMechanism.`type`)
          .setSalt(scramCredential.salt)
          .setStoredKey(scramCredential.storedKey)
          .setServerKey(scramCredential.serverKey)
          .setIterations(scramCredential.iterations), 0.toShort))
        recordConsumer.accept(batch)
      }
    })
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
          .setNextProducerId(producerIdBlock.nextBlockFirstId()), 0.toShort)).asJava)
      case None => // Nothing to migrate
    }
  }

  def migrateAcls(recordConsumer: Consumer[util.List[ApiMessageAndVersion]]): Unit = {
    aclClient.iterateAcls(new util.function.BiConsumer[ResourcePattern, util.Set[AccessControlEntry]]() {
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

  private def migrateDelegationTokens(
    recordConsumer: Consumer[util.List[ApiMessageAndVersion]]
  ): Unit = wrapZkException {
    val batch = new util.ArrayList[ApiMessageAndVersion]()
    val tokens = zkClient.getChildren(DelegationTokensZNode.path)
    for (tokenId <- tokens) {
      zkClient.getDelegationTokenInfo(tokenId) match {
        case Some(tokenInformation) =>
          val newDelegationTokenData = new DelegationTokenData(tokenInformation)
          batch.add(new ApiMessageAndVersion(newDelegationTokenData.toRecord(), 0.toShort))
        case None =>
      }
    }
    if (!batch.isEmpty) {
      recordConsumer.accept(batch)
    }
  }

  override def readAllMetadata(
    batchConsumer: Consumer[util.List[ApiMessageAndVersion]],
    brokerIdConsumer: Consumer[Integer]
  ): Unit = {
    migrateTopics(batchConsumer, brokerIdConsumer)
    migrateBrokerConfigs(batchConsumer, brokerIdConsumer)
    migrateClientQuotas(batchConsumer)
    migrateProducerId(batchConsumer)
    migrateAcls(batchConsumer)
    migrateDelegationTokens(batchConsumer)
  }

  override def readBrokerIds(): util.Set[Integer] = wrapZkException {
    new util.HashSet[Integer](zkClient.getSortedBrokerList.map(Integer.valueOf).toSet.asJava)
  }

  override def readProducerId(): util.Optional[ProducerIdsBlock] = {
    val (dataOpt, _) = zkClient.getDataAndVersion(ProducerIdBlockZNode.path)
    dataOpt.map(ProducerIdBlockZNode.parseProducerIdBlockData).asJava
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

  override def topicClient(): TopicMigrationClient = topicClient

  override def configClient(): ConfigMigrationClient = configClient

  override def aclClient(): AclMigrationClient = aclClient

  override def delegationTokenClient(): DelegationTokenMigrationClient = delegationTokenClient
}
