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

import org.junit.{After, Test}
import org.junit.Assert.assertEquals

class AdminManagerTest {

  private val zkClient: KafkaZkClient = EasyMock.createNiceMock(classOf[KafkaZkClient])
  private val metrics = new Metrics()
  private val brokerId = 1
  private val topic = "topic-1"
  private val metadataCache: MetadataCache = EasyMock.createNiceMock(classOf[MetadataCache])

  @After
  def tearDown(): Unit = {
    metrics.close()
  }

  def createAdminManager(): AdminManager = {
    val props = TestUtils.createBrokerConfig(brokerId, "zk")
    new AdminManager(KafkaConfig.fromProps(props), metrics, metadataCache, zkClient)
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
    assertEquals(results.head.errorCode(), Errors.NONE.code)
  }
}
