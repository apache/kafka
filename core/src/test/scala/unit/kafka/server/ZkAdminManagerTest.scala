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

import kafka.zk.KafkaZkClient
import org.apache.kafka.common.metrics.Metrics
import org.easymock.EasyMock
import kafka.utils.TestUtils
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.message.DescribeConfigsRequestData
import org.apache.kafka.common.message.DescribeConfigsResponseData
import org.apache.kafka.common.protocol.Errors

import org.junit.jupiter.api.{AfterEach, Test}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNotEquals
import java.util.Properties

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
    val adminManager = createAdminManager()
    val results: List[DescribeConfigsResponseData.DescribeConfigsResult] = adminManager.describeConfigs(resources, true, true)
    assertEquals(Errors.NONE.code, results.head.errorCode())
    assertFalse(results.head.configs().isEmpty, "Should return configs")
  }

  @Test
  def testDescribeConfigsWithEmptyConfigurationKeys(): Unit = {
    EasyMock.expect(zkClient.getEntityConfigs(ConfigType.Topic, topic)).andReturn(TestUtils.createBrokerConfig(brokerId, "zk"))
    EasyMock.expect(metadataCache.contains(topic)).andReturn(true)

    EasyMock.replay(zkClient, metadataCache)

    val resources = List(new DescribeConfigsRequestData.DescribeConfigsResource()
      .setResourceName(topic)
      .setResourceType(ConfigResource.Type.TOPIC.id))
    val adminManager = createAdminManager()
    val results: List[DescribeConfigsResponseData.DescribeConfigsResult] = adminManager.describeConfigs(resources, true, true)
    assertEquals(Errors.NONE.code, results.head.errorCode())
    assertFalse(results.head.configs().isEmpty, "Should return configs")
  }

  @Test
  def testDescribeConfigsWithDocumentation(): Unit = {
    EasyMock.expect(zkClient.getEntityConfigs(ConfigType.Topic, topic)).andReturn(new Properties)
    EasyMock.expect(zkClient.getEntityConfigs(ConfigType.Broker, brokerId.toString)).andReturn(new Properties)
    EasyMock.expect(metadataCache.contains(topic)).andReturn(true)
    EasyMock.replay(zkClient, metadataCache)

    val adminManager = createAdminManager()

    val resources = List(
      new DescribeConfigsRequestData.DescribeConfigsResource()
        .setResourceName(topic)
        .setResourceType(ConfigResource.Type.TOPIC.id),
      new DescribeConfigsRequestData.DescribeConfigsResource()
        .setResourceName(brokerId.toString)
        .setResourceType(ConfigResource.Type.BROKER.id))

    val results: List[DescribeConfigsResponseData.DescribeConfigsResult] = adminManager.describeConfigs(resources, true, true)
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
}
