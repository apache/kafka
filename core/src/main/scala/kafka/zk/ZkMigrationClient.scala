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

import kafka.server.{ConfigEntityName, ConfigType, DynamicBrokerConfig, ZkAdminManager}
import kafka.utils.{Logging, PasswordEncoder}
import kafka.zk.TopicZNode.TopicIdReplicaAssignment
import kafka.zk.ZkMigrationClient.wrapZkException
import kafka.zk.migration.{ZkConfigMigrationClient, ZkTopicMigrationClient}
import kafka.zookeeper._
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.ControllerMovedException
import org.apache.kafka.common.metadata.ClientQuotaRecord.EntityData
import org.apache.kafka.common.metadata._
import org.apache.kafka.common.{TopicIdPartition, Uuid}
import org.apache.kafka.metadata.PartitionRegistration
import org.apache.kafka.metadata.migration.TopicMigrationClient.TopicVisitor
import org.apache.kafka.metadata.migration._
import org.apache.kafka.server.common.{ApiMessageAndVersion, ProducerIdsBlock}
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.{AuthFailedException, NoAuthException, SessionClosedRequireAuthException}

import java.util
import java.util.Properties
import java.util.function.Consumer
import scala.collection.Seq
import scala.jdk.CollectionConverters._

object ZkMigrationClient {
  def apply(
    zkClient: KafkaZkClient,
    zkConfigEncoder: PasswordEncoder
  ): ZkMigrationClient = {
    val topicClient = new ZkTopicMigrationClient(zkClient)
    val configClient = new ZkConfigMigrationClient(zkClient, zkConfigEncoder)
    new ZkMigrationClient(zkClient, topicClient, configClient)
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
      case e@(_: MigrationClientException | _: MigrationClientAuthException) => throw e
      case e@(_: AuthFailedException | _: NoAuthException | _: SessionClosedRequireAuthException) =>
        // We don't expect authentication errors to be recoverable, so treat them differently
        throw new MigrationClientAuthException(e)
      case e: KeeperException => throw new MigrationClientException(e)
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
    topicClient.iterateTopics(new TopicVisitor() {
      override def visitTopic(topicName: String, topicId: Uuid, assignments: util.Map[Integer, util.List[Integer]]): Unit = {
        if (!topicBatch.isEmpty) {
          recordConsumer.accept(topicBatch)
          topicBatch = new util.ArrayList[ApiMessageAndVersion]()
        }

        topicBatch.add(new ApiMessageAndVersion(new TopicRecord()
          .setName(topicName)
          .setTopicId(topicId), 0.toShort))
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

      override def visitConfigs(topicName: String, topicProps: Properties): Unit = {
        topicProps.forEach((key: Any, value: Any) => {
          topicBatch.add(new ApiMessageAndVersion(new ConfigRecord()
            .setResourceType(ConfigResource.Type.TOPIC.id)
            .setResourceName(topicName)
            .setName(key.toString)
            .setValue(value.toString), 0.toShort))
        })
      }
    })

    if (!topicBatch.isEmpty) {
      recordConsumer.accept(topicBatch)
    }
  }

  def migrateBrokerConfigs(
    recordConsumer: Consumer[util.List[ApiMessageAndVersion]]
  ): Unit = wrapZkException {
    configClient.iterateBrokerConfigs((broker, props) => {
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
    configClient.iterateClientQuotas((entity, props) => {
      val batch = new util.ArrayList[ApiMessageAndVersion]()
      props.forEach((key, value) => {
        batch.add(new ApiMessageAndVersion(new ClientQuotaRecord()
          .setEntity(entity)
          .setKey(key)
          .setValue(value), 0.toShort))
      })
      if (!batch.isEmpty) {
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
          .setNextProducerId(producerIdBlock.firstProducerId()), 0.toShort)).asJava)
      case None => // Nothing to migrate
    }
  }

  override def readAllMetadata(
    batchConsumer: Consumer[util.List[ApiMessageAndVersion]],
    brokerIdConsumer: Consumer[Integer]
  ): Unit = {
    migrateTopics(batchConsumer, brokerIdConsumer)
    migrateBrokerConfigs(batchConsumer)
    migrateClientQuotas(batchConsumer)
    migrateProducerId(batchConsumer)
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
}
