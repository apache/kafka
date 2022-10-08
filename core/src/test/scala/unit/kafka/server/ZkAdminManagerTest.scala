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

package kafka.server

import java.util.{Collections, Properties, HashMap}
import kafka.server.metadata.ZkConfigRepository
import kafka.utils.TestUtils
import kafka.zk.{AdminZkClient, ConfigEntityTypeZNode, KafkaZkClient}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.internals.QuotaConfigs
import org.apache.kafka.common.message.DescribeConfigsRequestData
import org.apache.kafka.common.message.DescribeConfigsResponseData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.quota.{ClientQuotaAlteration, ClientQuotaEntity}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.mockito.Mockito.{mock, when}

import scala.jdk.CollectionConverters._

class ZkAdminManagerTest extends QuorumTestHarness {

  private val kafkaZkClient: KafkaZkClient = mock(classOf[KafkaZkClient])
  private val metrics = new Metrics()
  private val brokerId = 1
  private val topic = "topic-1"
  private val metadataCache: MetadataCache = mock(classOf[MetadataCache])
  private val producerByteRate = 1024
  private val ipConnectionRate = 10
  private var adminManager: ZkAdminManager = _

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    adminManager = new ZkAdminManager(createKafkaConfig(), metrics, metadataCache, super.zkClient)
  }

  @AfterEach
  override def tearDown(): Unit = {
    metrics.close()
    super.tearDown()
  }

  def createConfigHelper(metadataCache: MetadataCache, zkClient: KafkaZkClient): ConfigHelper = {
    val props = TestUtils.createBrokerConfig(brokerId, "zk")
    new ConfigHelper(metadataCache, KafkaConfig.fromProps(props), new ZkConfigRepository(new AdminZkClient(zkClient)))
  }

  def createKafkaConfig(): KafkaConfig = {
    val props = TestUtils.createBrokerConfig(brokerId, "zk")
    KafkaConfig(props, doLog = false)
  }

  @Test
  def testDescribeConfigsWithNullConfigurationKeys(): Unit = {
    when(kafkaZkClient.getEntityConfigs(ConfigType.Topic, topic)).thenReturn(TestUtils.createBrokerConfig(brokerId, "zk"))
    when(metadataCache.contains(topic)).thenReturn(true)

    val resources = List(new DescribeConfigsRequestData.DescribeConfigsResource()
      .setResourceName(topic)
      .setResourceType(ConfigResource.Type.TOPIC.id)
      .setConfigurationKeys(null))
    val configHelper = createConfigHelper(metadataCache, kafkaZkClient)
    val results: List[DescribeConfigsResponseData.DescribeConfigsResult] = configHelper.describeConfigs(resources, true, true)
    assertEquals(Errors.NONE.code, results.head.errorCode())
    assertFalse(results.head.configs().isEmpty, "Should return configs")
  }

  @Test
  def testDescribeConfigsWithEmptyConfigurationKeys(): Unit = {
    when(kafkaZkClient.getEntityConfigs(ConfigType.Topic, topic)).thenReturn(TestUtils.createBrokerConfig(brokerId, "zk"))
    when(metadataCache.contains(topic)).thenReturn(true)

    val resources = List(new DescribeConfigsRequestData.DescribeConfigsResource()
      .setResourceName(topic)
      .setResourceType(ConfigResource.Type.TOPIC.id))
    val configHelper = createConfigHelper(metadataCache, kafkaZkClient)
    val results: List[DescribeConfigsResponseData.DescribeConfigsResult] = configHelper.describeConfigs(resources, true, true)
    assertEquals(Errors.NONE.code, results.head.errorCode())
    assertFalse(results.head.configs().isEmpty, "Should return configs")
  }

  @Test
  def testDescribeConfigsWithConfigurationKeys(): Unit = {
    when(kafkaZkClient.getEntityConfigs(ConfigType.Topic, topic)).thenReturn(TestUtils.createBrokerConfig(brokerId, "zk"))
    when(metadataCache.contains(topic)).thenReturn(true)

    val resources = List(new DescribeConfigsRequestData.DescribeConfigsResource()
      .setResourceName(topic)
      .setResourceType(ConfigResource.Type.TOPIC.id)
      .setConfigurationKeys(List("retention.ms", "retention.bytes", "segment.bytes").asJava)
    )
    val configHelper = createConfigHelper(metadataCache, kafkaZkClient)
    val results: List[DescribeConfigsResponseData.DescribeConfigsResult] = configHelper.describeConfigs(resources, true, true)
    assertEquals(Errors.NONE.code, results.head.errorCode())
    val resultConfigKeys = results.head.configs().asScala.map(r => r.name()).toSet
    assertEquals(Set("retention.ms", "retention.bytes", "segment.bytes"), resultConfigKeys)
  }

  @Test
  def testDescribeConfigsWithDocumentation(): Unit = {
    when(kafkaZkClient.getEntityConfigs(ConfigType.Topic, topic)).thenReturn(new Properties)
    when(kafkaZkClient.getEntityConfigs(ConfigType.Broker, brokerId.toString)).thenReturn(new Properties)
    when(metadataCache.contains(topic)).thenReturn(true)

    val configHelper = createConfigHelper(metadataCache, kafkaZkClient)

    val resources = List(
      new DescribeConfigsRequestData.DescribeConfigsResource()
        .setResourceName(topic)
        .setResourceType(ConfigResource.Type.TOPIC.id),
      new DescribeConfigsRequestData.DescribeConfigsResource()
        .setResourceName(brokerId.toString)
        .setResourceType(ConfigResource.Type.BROKER.id))

    val results: List[DescribeConfigsResponseData.DescribeConfigsResult] = configHelper.describeConfigs(resources, true, true)
    assertEquals(2, results.size)
    results.foreach(r => {
      assertEquals(Errors.NONE.code, r.errorCode)
      assertFalse(r.configs.isEmpty, "Should return configs")
      r.configs.forEach(c => {
        assertNotNull(c.documentation, s"Config ${c.name} should have non null documentation")
        assertNotEquals(s"Config ${c.name} should have non blank documentation", "", c.documentation.trim)
      })
    })
  }

  @Test
  def testAlterClientQuotasWithUserAndClientId(): Unit = {
    val alterEntityMap = new HashMap[String, String]()
    alterEntityMap.put(ClientQuotaEntity.USER, "user01")
    alterEntityMap.put(ClientQuotaEntity.CLIENT_ID, "client01")
    val quotaEntity = new ClientQuotaEntity(alterEntityMap)
    adminManager.alterClientQuotas(Seq(new ClientQuotaAlteration(quotaEntity, Collections.singleton(
      new ClientQuotaAlteration.Op(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, producerByteRate)))), validateOnly = false)
    val props = zkClient.getEntityConfigs(ConfigType.User, "user01/clients/client01")
    assertEquals(props.getProperty(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG).toInt, producerByteRate)

    adminManager.alterClientQuotas(Seq(new ClientQuotaAlteration(quotaEntity, Collections.singleton(
      new ClientQuotaAlteration.Op(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, null)))), validateOnly = false)
    val users = zkClient.getChildren(ConfigEntityTypeZNode.path(ConfigType.User))
    assert(users.isEmpty)
  }

  @Test
  def testAlterClientQuotasWithUser(): Unit = {
    val alterEntityMap = new HashMap[String, String]()
    alterEntityMap.put(ClientQuotaEntity.USER, "user01")
    val quotaEntity = new ClientQuotaEntity(alterEntityMap)
    adminManager.alterClientQuotas(Seq(new ClientQuotaAlteration(quotaEntity, Collections.singleton(
      new ClientQuotaAlteration.Op(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, producerByteRate)))), validateOnly = false)
    val props = zkClient.getEntityConfigs(ConfigType.User, "user01")
    assertEquals(props.getProperty(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG).toInt, producerByteRate)

    adminManager.alterClientQuotas(Seq(new ClientQuotaAlteration(quotaEntity, Collections.singleton(
      new ClientQuotaAlteration.Op(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, null)))), validateOnly = false)
    val users = zkClient.getChildren(ConfigEntityTypeZNode.path(ConfigType.User))
    assert(users.isEmpty)
  }

  @Test
  def testAlterClientQuotasWithClientId(): Unit = {
    val alterEntityMap = new HashMap[String, String]()
    alterEntityMap.put(ClientQuotaEntity.CLIENT_ID, "client01")
    val quotaEntity = new ClientQuotaEntity(alterEntityMap)
    adminManager.alterClientQuotas(Seq(new ClientQuotaAlteration(quotaEntity, Collections.singleton(
      new ClientQuotaAlteration.Op(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, producerByteRate)))), validateOnly = false)
    val props = zkClient.getEntityConfigs(ConfigType.Client, "client01")
    assertEquals(props.getProperty(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG).toInt, producerByteRate)

    adminManager.alterClientQuotas(Seq(new ClientQuotaAlteration(quotaEntity, Collections.singleton(
      new ClientQuotaAlteration.Op(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, null)))), validateOnly = false)
    val clients = zkClient.getChildren(ConfigEntityTypeZNode.path(ConfigType.Client))
    assert(clients.isEmpty)
  }

  @Test
  def testAlterClientQuotasWithIp(): Unit = {
    val alterEntityMap = new HashMap[String, String]()
    alterEntityMap.put(ClientQuotaEntity.IP, "127.0.0.1")
    val quotaEntity = new ClientQuotaEntity(alterEntityMap)
    adminManager.alterClientQuotas(Seq(new ClientQuotaAlteration(quotaEntity, Collections.singleton(
      new ClientQuotaAlteration.Op(QuotaConfigs.IP_CONNECTION_RATE_OVERRIDE_CONFIG, ipConnectionRate)))), validateOnly = false)
    val props = zkClient.getEntityConfigs(ConfigType.Ip, "127.0.0.1")
    assertEquals(props.getProperty(QuotaConfigs.IP_CONNECTION_RATE_OVERRIDE_CONFIG).toInt, ipConnectionRate)

    adminManager.alterClientQuotas(Seq(new ClientQuotaAlteration(quotaEntity, Collections.singleton(
      new ClientQuotaAlteration.Op(QuotaConfigs.IP_CONNECTION_RATE_OVERRIDE_CONFIG, null)))), validateOnly = false)
    val ips = zkClient.getChildren(ConfigEntityTypeZNode.path(ConfigType.Ip))
    assert(ips.isEmpty)
  }
}
