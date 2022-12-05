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
import kafka.server.{ConfigType, QuorumTestHarness, ZkAdminManager}
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.config.internals.QuotaConfigs
import org.apache.kafka.common.quota.ClientQuotaEntity
import org.apache.kafka.common.utils.Time
import org.apache.kafka.metadata.{LeaderRecoveryState, PartitionRegistration}
import org.apache.kafka.metadata.migration.ZkMigrationLeadershipState
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{BeforeEach, Test, TestInfo}

import java.util.Properties
import scala.collection.Map
import scala.jdk.CollectionConverters._

/**
 * ZooKeeper integration tests that verify the interoperability of KafkaZkClient and ZkMigrationClient.
 */
class ZkMigrationClientTest extends QuorumTestHarness {

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    zkClient.createControllerEpochRaw(1)
   }

  private def initialMigrationState: ZkMigrationLeadershipState = {
    val (_, stat) = zkClient.getControllerEpoch.get
    new ZkMigrationLeadershipState(3000, 42, 100, 42, Time.SYSTEM.milliseconds(), -1, stat.getVersion)
  }

  @Test
  def testEmptyWrite(): Unit = {
    val migrationClient = new ZkMigrationClient(zkClient)
    var migrationState = initialMigrationState
    migrationState = migrationClient.getOrCreateMigrationRecoveryState(migrationState)
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
    val migrationClient = new ZkMigrationClient(zkClient)
    var migrationState = initialMigrationState
    migrationState = migrationClient.getOrCreateMigrationRecoveryState(migrationState)
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
    val migrationClient = new ZkMigrationClient(zkClient)
    var migrationState = initialMigrationState
    migrationState = migrationClient.getOrCreateMigrationRecoveryState(migrationState)
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
    val nextMigrationState = migrationClient.updateClientQuotas(
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
    val migrationClient = new ZkMigrationClient(zkClient)
    val props = new Properties()
    props.put(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 100000L)
    adminZkClient.changeConfigs(ConfigType.User, "user1", props)
    adminZkClient.changeConfigs(ConfigType.User, "user1/clients/clientA", props)

    var migrationState = initialMigrationState
    migrationState = migrationClient.getOrCreateMigrationRecoveryState(migrationState)
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
    val migrationClient = new ZkMigrationClient(zkClient)
    val adminZkClient = new AdminZkClient(zkClient)

    var migrationState = initialMigrationState
    migrationState = migrationClient.getOrCreateMigrationRecoveryState(migrationState)
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
}
