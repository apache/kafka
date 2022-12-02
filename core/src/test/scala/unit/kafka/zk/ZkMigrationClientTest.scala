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

import kafka.server.{ConfigType, QuorumTestHarness, ZkAdminManager}
import org.apache.kafka.common.config.internals.QuotaConfigs
import org.apache.kafka.common.quota.ClientQuotaEntity
import org.apache.kafka.common.utils.Time
import org.apache.kafka.metadata.migration.ZkMigrationLeadershipState
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{BeforeEach, Test, TestInfo}

import java.util.Properties
import scala.jdk.CollectionConverters._

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
    val adminZkClient = new AdminZkClient(zkClient)
    val props = new Properties()
    props.put(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 100000L)
    adminZkClient.changeConfigs(ConfigType.User, "user1", props)

    var migrationState = initialMigrationState
    migrationState = zkClient.getOrCreateMigrationState(migrationState)
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
    migrationState = zkClient.getOrCreateMigrationState(migrationState)
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
