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
import kafka.controller.LeaderIsrAndControllerEpoch
import kafka.coordinator.transaction.ProducerIdManager
import kafka.server.{ConfigType, QuorumTestHarness, ZkAdminManager}
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.config.internals.QuotaConfigs
import org.apache.kafka.common.errors.ControllerMovedException
import org.apache.kafka.common.metadata.{ConfigRecord, MetadataRecordType, ProducerIdsRecord}
import org.apache.kafka.common.quota.ClientQuotaEntity
import org.apache.kafka.common.utils.Time
import org.apache.kafka.metadata.{LeaderRecoveryState, PartitionRegistration}
import org.apache.kafka.metadata.migration.ZkMigrationLeadershipState
import org.apache.kafka.server.common.{ApiMessageAndVersion, MetadataVersion}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue, fail}
import org.junit.jupiter.api.{BeforeEach, Test, TestInfo}

import java.util.Properties
import scala.collection.Map
import scala.jdk.CollectionConverters._

/**
 * ZooKeeper integration tests that verify the interoperability of KafkaZkClient and ZkMigrationClient.
 */
class ZkMigrationClientTest extends QuorumTestHarness {

  private val InitialControllerEpoch: Int = 42
  private val InitialKRaftEpoch: Int = 0

  private var migrationClient: ZkMigrationClient = _

  private var migrationState: ZkMigrationLeadershipState = _

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    zkClient.createControllerEpochRaw(1)

    migrationClient = new ZkMigrationClient(zkClient)
    migrationState = initialMigrationState
    migrationState = migrationClient.getOrCreateMigrationRecoveryState(migrationState)
   }

  private def initialMigrationState: ZkMigrationLeadershipState = {
    val (_, stat) = zkClient.getControllerEpoch.get
    new ZkMigrationLeadershipState(3000, InitialControllerEpoch, 100, InitialKRaftEpoch, Time.SYSTEM.milliseconds(), -1, stat.getVersion)
  }

  @Test
  def testMigrateEmptyZk(): Unit = {
    val brokers = new java.util.ArrayList[Integer]()
    val batches = new java.util.ArrayList[java.util.List[ApiMessageAndVersion]]()

    migrationClient.readAllMetadata(batch => batches.add(batch), brokerId => brokers.add(brokerId))
    assertEquals(0, brokers.size())
    assertEquals(0, batches.size())
  }

  @Test
  def testEmptyWrite(): Unit = {
    val (zkVersion, responses) = zkClient.retryMigrationRequestsUntilConnected(Seq(), migrationState)
    assertEquals(migrationState.migrationZkVersion(), zkVersion)
    assertTrue(responses.isEmpty)
  }

  @Test
  def testUpdateExistingPartitions(): Unit = {
    // Create a topic and partition state in ZK like KafkaController would
    val assignment = Map(
      new TopicPartition("test", 0) -> List(0, 1, 2),
      new TopicPartition("test", 1) -> List(1, 2, 3)
    )
    zkClient.createTopicAssignment("test", Some(Uuid.randomUuid()), assignment)

    val leaderAndIsrs = Map(
      new TopicPartition("test", 0) -> LeaderIsrAndControllerEpoch(
        new LeaderAndIsr(0, 5, List(0, 1, 2), LeaderRecoveryState.RECOVERED, -1), 1),
      new TopicPartition("test", 1) -> LeaderIsrAndControllerEpoch(
        new LeaderAndIsr(1, 5, List(1, 2, 3), LeaderRecoveryState.RECOVERED, -1), 1)
    )
    zkClient.createTopicPartitionStatesRaw(leaderAndIsrs, 0)

    // Now verify that we can update it with migration client
    assertEquals(0, migrationState.migrationZkVersion())

    val partitions = Map(
      0 -> new PartitionRegistration(Array(0, 1, 2), Array(1, 2), Array(), Array(), 1, LeaderRecoveryState.RECOVERED, 6, -1),
      1 -> new PartitionRegistration(Array(1, 2, 3), Array(3), Array(), Array(), 3, LeaderRecoveryState.RECOVERED, 7, -1)
    ).map { case (k, v) => Integer.valueOf(k) -> v }.asJava
    migrationState = migrationClient.updateTopicPartitions(Map("test" -> partitions).asJava, migrationState)
    assertEquals(1, migrationState.migrationZkVersion())

    // Read back with Zk client
    val partition0 = zkClient.getTopicPartitionState(new TopicPartition("test", 0)).get.leaderAndIsr
    assertEquals(1, partition0.leader)
    assertEquals(6, partition0.leaderEpoch)
    assertEquals(List(1, 2), partition0.isr)

    val partition1 = zkClient.getTopicPartitionState(new TopicPartition("test", 1)).get.leaderAndIsr
    assertEquals(3, partition1.leader)
    assertEquals(7, partition1.leaderEpoch)
    assertEquals(List(3), partition1.isr)
  }

  @Test
  def testCreateNewPartitions(): Unit = {
    assertEquals(0, migrationState.migrationZkVersion())

    val partitions = Map(
      0 -> new PartitionRegistration(Array(0, 1, 2), Array(0, 1, 2), Array(), Array(), 0, LeaderRecoveryState.RECOVERED, 0, -1),
      1 -> new PartitionRegistration(Array(1, 2, 3), Array(1, 2, 3), Array(), Array(), 1, LeaderRecoveryState.RECOVERED, 0, -1)
    ).map { case (k, v) => Integer.valueOf(k) -> v }.asJava
    migrationState = migrationClient.createTopic("test", Uuid.randomUuid(), partitions, migrationState)
    assertEquals(1, migrationState.migrationZkVersion())

    // Read back with Zk client
    val partition0 = zkClient.getTopicPartitionState(new TopicPartition("test", 0)).get.leaderAndIsr
    assertEquals(0, partition0.leader)
    assertEquals(0, partition0.leaderEpoch)
    assertEquals(List(0, 1, 2), partition0.isr)

    val partition1 = zkClient.getTopicPartitionState(new TopicPartition("test", 1)).get.leaderAndIsr
    assertEquals(1, partition1.leader)
    assertEquals(0, partition1.leaderEpoch)
    assertEquals(List(1, 2, 3), partition1.isr)
  }

  // Write Client Quotas using ZkMigrationClient and read them back using AdminZkClient
  private def writeClientQuotaAndVerify(migrationClient: ZkMigrationClient,
                                        adminZkClient: AdminZkClient,
                                        migrationState: ZkMigrationLeadershipState,
                                        entity: Map[String, String],
                                        quotas: Map[String, Double],
                                        zkEntityType: String,
                                        zkEntityName: String): ZkMigrationLeadershipState = {
    val nextMigrationState = migrationClient.writeClientQuotas(
      new ClientQuotaEntity(entity.asJava),
      quotas.asJava,
      migrationState)
    val newProps = ZkAdminManager.clientQuotaPropsToDoubleMap(
      adminZkClient.fetchEntityConfig(zkEntityType, zkEntityName).asScala)
    assertEquals(quotas, newProps)
    nextMigrationState
  }


  @Test
  def testWriteExistingClientQuotas(): Unit = {
    val props = new Properties()
    props.put(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, "100000")
    adminZkClient.changeConfigs(ConfigType.User, "user1", props)
    adminZkClient.changeConfigs(ConfigType.User, "user1/clients/clientA", props)

    assertEquals(0, migrationState.migrationZkVersion())
    migrationState = writeClientQuotaAndVerify(migrationClient, adminZkClient, migrationState,
      Map(ConfigType.User -> "user1"),
      Map(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG -> 20000.0),
      ConfigType.User, "user1")
    assertEquals(1, migrationState.migrationZkVersion())

    migrationState = writeClientQuotaAndVerify(migrationClient, adminZkClient, migrationState,
      Map(ConfigType.User -> "user1"),
      Map(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG -> 10000.0),
      ConfigType.User, "user1")
    assertEquals(2, migrationState.migrationZkVersion())

    migrationState = writeClientQuotaAndVerify(migrationClient, adminZkClient, migrationState,
      Map(ConfigType.User -> "user1"),
      Map.empty,
      ConfigType.User, "user1")
    assertEquals(3, migrationState.migrationZkVersion())

    migrationState = writeClientQuotaAndVerify(migrationClient, adminZkClient, migrationState,
      Map(ConfigType.User -> "user1"),
      Map(QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG -> 100.0),
      ConfigType.User, "user1")
    assertEquals(4, migrationState.migrationZkVersion())
  }

  @Test
  def testWriteNewClientQuotas(): Unit = {
    assertEquals(0, migrationState.migrationZkVersion())
    migrationState = writeClientQuotaAndVerify(migrationClient, adminZkClient, migrationState,
      Map(ConfigType.User -> "user2"),
      Map(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG -> 20000.0, QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG -> 100.0),
      ConfigType.User, "user2")

    assertEquals(1, migrationState.migrationZkVersion())

    migrationState = writeClientQuotaAndVerify(migrationClient, adminZkClient, migrationState,
      Map(ConfigType.User -> "user2", ConfigType.Client -> "clientA"),
      Map(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG -> 10000.0, QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG -> 200.0),
      ConfigType.User, "user2/clients/clientA")

    assertEquals(2, migrationState.migrationZkVersion())
  }

  @Test
  def testClaimAbsentController(): Unit = {
    assertEquals(0, migrationState.migrationZkVersion())
    migrationState = migrationClient.claimControllerLeadership(migrationState)
    assertEquals(1, migrationState.controllerZkVersion())
  }

  @Test
  def testExistingKRaftControllerClaim(): Unit = {
    assertEquals(0, migrationState.migrationZkVersion())
    migrationState = migrationClient.claimControllerLeadership(migrationState)
    assertEquals(1, migrationState.controllerZkVersion())

    // We don't require a KRaft controller to release the controller in ZK before another KRaft controller
    // can claim it. This is because KRaft leadership comes from Raft and we are just synchronizing it to ZK.
    var otherNodeState = new ZkMigrationLeadershipState(3001, 43, 100, 42, Time.SYSTEM.milliseconds(), -1, -1)
    otherNodeState = migrationClient.claimControllerLeadership(otherNodeState)
    assertEquals(2, otherNodeState.controllerZkVersion())
    assertEquals(3001, otherNodeState.kraftControllerId())
    assertEquals(43, otherNodeState.kraftControllerEpoch())
  }

  @Test
  def testNonIncreasingKRaftEpoch(): Unit = {
    assertEquals(0, migrationState.migrationZkVersion())

    migrationState = migrationState.withNewKRaftController(3001, InitialControllerEpoch)
    migrationState = migrationClient.claimControllerLeadership(migrationState)
    assertEquals(1, migrationState.controllerZkVersion())

    migrationState = migrationState.withNewKRaftController(3001, InitialControllerEpoch - 1)
    val t1 = assertThrows(classOf[ControllerMovedException], () => migrationClient.claimControllerLeadership(migrationState))
    assertEquals("Cannot register KRaft controller 3001 with epoch 41 as the current controller register in ZK has the same or newer epoch 42.", t1.getMessage)

    migrationState = migrationState.withNewKRaftController(3001, InitialControllerEpoch)
    val t2 = assertThrows(classOf[ControllerMovedException], () => migrationClient.claimControllerLeadership(migrationState))
    assertEquals("Cannot register KRaft controller 3001 with epoch 42 as the current controller register in ZK has the same or newer epoch 42.", t2.getMessage)

    migrationState = migrationState.withNewKRaftController(3001, 100)
    migrationState = migrationClient.claimControllerLeadership(migrationState)
    assertEquals(migrationState.kraftControllerEpoch(), 100)
    assertEquals(migrationState.kraftControllerId(), 3001)
  }

  @Test
  def testClaimAndReleaseExistingController(): Unit = {
    assertEquals(0, migrationState.migrationZkVersion())

    val (epoch, zkVersion) = zkClient.registerControllerAndIncrementControllerEpoch(100)
    assertEquals(epoch, 2)
    assertEquals(zkVersion, 1)

    migrationState = migrationClient.claimControllerLeadership(migrationState)
    assertEquals(2, migrationState.controllerZkVersion())
    zkClient.getControllerEpoch match {
      case Some((zkEpoch, stat)) =>
        assertEquals(3, zkEpoch)
        assertEquals(2, stat.getVersion)
      case None => fail()
    }
    assertEquals(3000, zkClient.getControllerId.get)
    assertThrows(classOf[ControllerMovedException], () => zkClient.registerControllerAndIncrementControllerEpoch(100))

    migrationState = migrationClient.releaseControllerLeadership(migrationState)
    val (epoch1, zkVersion1) = zkClient.registerControllerAndIncrementControllerEpoch(100)
    assertEquals(epoch1, 4)
    assertEquals(zkVersion1, 3)
  }

  @Test
  def testReadAndWriteProducerId(): Unit = {
    def generateNextProducerIdWithZkAndRead(): Long = {
      // Generate a producer ID in ZK
      val manager = ProducerIdManager.zk(1, zkClient)
      manager.generateProducerId()

      val records = new java.util.ArrayList[java.util.List[ApiMessageAndVersion]]()
      migrationClient.migrateProducerId(MetadataVersion.latest(), batch => records.add(batch))
      assertEquals(1, records.size())
      assertEquals(1, records.get(0).size())

      val record = records.get(0).get(0).message().asInstanceOf[ProducerIdsRecord]
      record.nextProducerId()
    }

    // Initialize with ZK ProducerIdManager
    assertEquals(0, generateNextProducerIdWithZkAndRead())

    // Update next producer ID via migration client
    migrationState = migrationClient.writeProducerId(6000, migrationState)
    assertEquals(1, migrationState.migrationZkVersion())

    // Switch back to ZK, it should provision the next block
    assertEquals(7000, generateNextProducerIdWithZkAndRead())
  }

  @Test
  def testMigrateTopicConfigs(): Unit = {
    val props = new Properties()
    props.put(TopicConfig.FLUSH_MS_CONFIG, "60000")
    props.put(TopicConfig.RETENTION_MS_CONFIG, "300000")
    adminZkClient.createTopicWithAssignment("test", props, Map(0 -> Seq(0, 1, 2), 1 -> Seq(1, 2, 0), 2 -> Seq(2, 0, 1)), usesTopicId = true)

    val brokers = new java.util.ArrayList[Integer]()
    val batches = new java.util.ArrayList[java.util.List[ApiMessageAndVersion]]()
    migrationClient.migrateTopics(MetadataVersion.latest(), batch => batches.add(batch), brokerId => brokers.add(brokerId))
    assertEquals(1, batches.size())
    val configs = batches.get(0)
      .asScala
      .map {_.message()}
      .filter(message => MetadataRecordType.fromId(message.apiKey()).equals(MetadataRecordType.CONFIG_RECORD))
      .map {_.asInstanceOf[ConfigRecord]}
      .toSeq
    assertEquals(2, configs.size)
    assertEquals(TopicConfig.FLUSH_MS_CONFIG, configs.head.name())
    assertEquals("60000", configs.head.value())
    assertEquals(TopicConfig.RETENTION_MS_CONFIG, configs.last.name())
    assertEquals("300000", configs.last.value())
  }

  @Test
  def testWriteNewTopicConfigs(): Unit = {
    migrationState = migrationClient.writeConfigs(new ConfigResource(ConfigResource.Type.TOPIC, "test"),
      java.util.Collections.singletonMap(TopicConfig.SEGMENT_MS_CONFIG, "100000"), migrationState)
    assertEquals(1, migrationState.migrationZkVersion())

    val newProps = zkClient.getEntityConfigs(ConfigType.Topic, "test")
    assertEquals(1, newProps.size())
    assertEquals("100000", newProps.getProperty(TopicConfig.SEGMENT_MS_CONFIG))
  }

  @Test
  def testWriteExistingTopicConfigs(): Unit = {
    val props = new Properties()
    props.put(TopicConfig.FLUSH_MS_CONFIG, "60000")
    props.put(TopicConfig.RETENTION_MS_CONFIG, "300000")
    zkClient.setOrCreateEntityConfigs(ConfigType.Topic, "test", props)

    migrationState = migrationClient.writeConfigs(new ConfigResource(ConfigResource.Type.TOPIC, "test"),
      java.util.Collections.singletonMap(TopicConfig.SEGMENT_MS_CONFIG, "100000"), migrationState)
    assertEquals(1, migrationState.migrationZkVersion())

    val newProps = zkClient.getEntityConfigs(ConfigType.Topic, "test")
    assertEquals(1, newProps.size())
    assertEquals("100000", newProps.getProperty(TopicConfig.SEGMENT_MS_CONFIG))
  }
}
