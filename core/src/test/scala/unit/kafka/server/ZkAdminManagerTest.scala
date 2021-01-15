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

import kafka.zk.{AdminZkClient}
import kafka.utils.TestUtils
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.clients.admin.{AlterConfigOp, ConfigEntry}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.message.DescribeConfigsRequestData.DescribeConfigsResource
import org.apache.kafka.common.message.{DescribeConfigsRequestData, DescribeConfigsResponseData}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.junit.jupiter.api.{AfterEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotEquals, assertNotNull, fail}
import java.util.Properties

import kafka.server.metadata.ZkConfigRepository
import org.apache.kafka.common.requests.AlterConfigsRequest
import org.easymock.EasyMock

import scala.jdk.CollectionConverters._

class ZkAdminManagerTest {

  private val zkClient: KafkaZkClient = EasyMock.createNiceMock(classOf[KafkaZkClient])
  private val metrics = new Metrics()
  private val brokerId = 1
  private val topic = "topic-1"
  private val metadataCache: MetadataCache = EasyMock.createNiceMock(classOf[MetadataCache])

  @AfterEach
  def tearDown(): Unit = {
    metrics.close()
  }

  def createConfigHelper(metadataCache: MetadataCache, zkClient: KafkaZkClient): ConfigHelper = {
    val props = TestUtils.createBrokerConfig(brokerId, "zk")
    new ConfigHelper(metadataCache, KafkaConfig.fromProps(props), new ZkConfigRepository(new AdminZkClient(zkClient)))
  }

  def createAdminManager(): ZkAdminManager = {
    val props = TestUtils.createBrokerConfig(brokerId, "zk")
    new ZkAdminManager(KafkaConfig.fromProps(props), metrics, metadataCache, zkClient)
  }

  @Test
  def testDescribeConfigsWithNullConfigurationKeys(): Unit = {
    EasyMock.expect(zkClient.getEntityConfigs(ConfigType.Topic, topic)).andReturn(TestUtils.createBrokerConfig(brokerId, "zk"))
    EasyMock.expect(metadataCache.contains(topic)).andReturn(true)

    EasyMock.replay(zkClient, metadataCache)

    val resources = List(new DescribeConfigsRequestData.DescribeConfigsResource()
      .setResourceName(topic)
      .setResourceType(ConfigResource.Type.TOPIC.id)
      .setConfigurationKeys(null))
    val configHelper = createConfigHelper(metadataCache, zkClient)
    val results: List[DescribeConfigsResponseData.DescribeConfigsResult] = configHelper.describeConfigs(resources, true, true)
    assertEquals(Errors.NONE.code, results.head.errorCode())
    assertFalse(results.head.configs().isEmpty, "Should return configs")
    EasyMock.verify(zkClient, metadataCache)
  }

  @Test
  def testDescribeConfigsWithEmptyConfigurationKeys(): Unit = {
    EasyMock.expect(zkClient.getEntityConfigs(ConfigType.Topic, topic)).andReturn(TestUtils.createBrokerConfig(brokerId, "zk"))
    EasyMock.expect(metadataCache.contains(topic)).andReturn(true)

    EasyMock.replay(zkClient, metadataCache)

    val resources = List(new DescribeConfigsRequestData.DescribeConfigsResource()
      .setResourceName(topic)
      .setResourceType(ConfigResource.Type.TOPIC.id))
    val configHelper = createConfigHelper(metadataCache, zkClient)
    val results: List[DescribeConfigsResponseData.DescribeConfigsResult] = configHelper.describeConfigs(resources, true, true)
    assertEquals(Errors.NONE.code, results.head.errorCode())
    assertFalse(results.head.configs().isEmpty, "Should return configs")
    EasyMock.verify(zkClient, metadataCache)
  }

  @Test
  def testDescribeConfigsWithDocumentation(): Unit = {
    EasyMock.expect(zkClient.getEntityConfigs(ConfigType.Topic, topic)).andReturn(new Properties)
    EasyMock.expect(metadataCache.contains(topic)).andReturn(true)
    EasyMock.replay(zkClient, metadataCache)

    val configHelper = createConfigHelper(metadataCache, zkClient)

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

    EasyMock.verify(zkClient, metadataCache)
  }

  @Test
  def testDescribeConfigsWithDefaultEntityNames(): Unit = {
    val defaultEntityName = ""

    EasyMock.replay(zkClient, metadataCache)

    val resources = ConfigResource.Type.values.filterNot(t => t == ConfigResource.Type.UNKNOWN).map(resourceType => {
      new DescribeConfigsResource().setResourceType(resourceType.id).setResourceName(defaultEntityName)
    }).toList
    val configHelper = createConfigHelper(metadataCache, zkClient)
    val results = configHelper.describeConfigs(resources, false, false)
    results.foreach { result =>
      assertEquals(defaultEntityName, result.resourceName)
      val resourceType = ConfigResource.Type.forId(result.resourceType)
      resourceType match {
        case ConfigResource.Type.BROKER =>
          assertEquals(Errors.NONE, Errors.forCode(result.errorCode))
          assertEquals("", result.errorMessage)
        case ConfigResource.Type.BROKER_LOGGER =>
          assertEquals(Errors.INVALID_REQUEST, Errors.forCode(result.errorCode))
          assertEquals("Broker id must not be empty", result.errorMessage)
        case ConfigResource.Type.TOPIC =>
          assertEquals(Errors.INVALID_TOPIC_EXCEPTION, Errors.forCode(result.errorCode))
          assertEquals("Default configs are not supported for topic entities.", result.errorMessage)
        case _ => fail(s"Unhandled resource type $resourceType in test")
      }
    }
    EasyMock.verify(zkClient, metadataCache)
  }

  @Test
  def testIncrementalAlterConfigsWithDefaultEntityNames(): Unit = {
    val defaultEntityName = ""
    EasyMock.expect(zkClient.getEntityConfigs(ConfigType.Broker, ConfigEntityName.Default)).andReturn(new Properties)
    EasyMock.replay(zkClient, metadataCache)

    val resources = ConfigResource.Type.values.filterNot(t => t == ConfigResource.Type.UNKNOWN).map(resourceType => {
      new ConfigResource(resourceType, defaultEntityName) -> Seq(resourceType match {
        case ConfigResource.Type.BROKER => new AlterConfigOp(new ConfigEntry("log.segment.bytes", "1024"), OpType.SET)
        case ConfigResource.Type.BROKER_LOGGER => new AlterConfigOp(new ConfigEntry("kafka", "INFO"), OpType.SET)
        case ConfigResource.Type.TOPIC => new AlterConfigOp(new ConfigEntry("segment.bytes", "1024"), OpType.SET)
        case _ => throw new AssertionError(s"Unhandled resource type $resourceType in test")
      })
    }).toMap
    val adminManager = createAdminManager()
    val results = adminManager.incrementalAlterConfigs(resources, validateOnly = true)
    results.foreach { case (configResource, apiError) => {
      assertEquals(defaultEntityName, configResource.name)
      val resourceType = configResource.`type`
      resourceType match {
        case ConfigResource.Type.BROKER =>
          assertEquals(Errors.NONE, apiError.error)
          assertEquals(null, apiError.message)
        case ConfigResource.Type.BROKER_LOGGER =>
          assertEquals(Errors.NONE, apiError.error)
          assertEquals(null, apiError.message)
        case ConfigResource.Type.TOPIC =>
          assertEquals(Errors.INVALID_REQUEST, apiError.error())
          assertEquals("Default configs are not supported for topic entities.", apiError.message)
        case _ => fail(s"Unhandled resource type $resourceType in test")
      }
    }}
    EasyMock.verify(zkClient, metadataCache)
  }

  @Test
  def testAlterConfigsWithDefaultEntityNames(): Unit = {
    val defaultEntityName = ""
    EasyMock.replay(zkClient, metadataCache)

    val resources = ConfigResource.Type.values.filterNot(t => t == ConfigResource.Type.UNKNOWN).map(resourceType => {
      new ConfigResource(resourceType, defaultEntityName) -> new AlterConfigsRequest.Config(List(resourceType match {
        case ConfigResource.Type.BROKER => new AlterConfigsRequest.ConfigEntry("log.segment.bytes", "1024")
        case ConfigResource.Type.BROKER_LOGGER => new AlterConfigsRequest.ConfigEntry("kafka", "INFO")
        case ConfigResource.Type.TOPIC => new AlterConfigsRequest.ConfigEntry("segment.bytes", "1024")
        case _ => throw new AssertionError(s"Unhandled resource type $resourceType in test")
      }).asJava)
    }).toMap
    val adminManager = createAdminManager()
    val results = adminManager.alterConfigs(resources, validateOnly = true)
    results.foreach { case (configResource, err) => {
      assertEquals(defaultEntityName, configResource.name)
      val resourceType = configResource.`type`
      resourceType match {
        case ConfigResource.Type.BROKER =>
          assertEquals(Errors.NONE, err.error)
          assertEquals(null, err.message)
        case ConfigResource.Type.BROKER_LOGGER =>
          assertEquals(Errors.INVALID_REQUEST, err.error)
          assertEquals("AlterConfigs is only supported for topics and brokers, but resource type is BROKER_LOGGER", err.message)
        case ConfigResource.Type.TOPIC =>
          assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, err.error)
          assertEquals("Default configs are not supported for topic entities.", err.message)
        case _ => fail(s"Unhandled resource type $resourceType in test")
      }
    }}
    EasyMock.verify(zkClient, metadataCache)
  }
}
