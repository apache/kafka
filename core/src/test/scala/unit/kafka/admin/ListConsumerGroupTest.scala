/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.admin

import java.util.Properties

import org.easymock.EasyMock
import org.junit.Before
import org.junit.Test

import kafka.admin.ConsumerGroupCommand.ConsumerGroupCommandOptions
import kafka.admin.ConsumerGroupCommand.ZkConsumerGroupService
import kafka.consumer.OldConsumer
import kafka.consumer.Whitelist
import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils


class ListConsumerGroupTest extends KafkaServerTestHarness {

  val overridingProps = new Properties()
  val topic = "foo"
  val topicFilter = Whitelist(topic)
  val group = "test.group"
  val props = new Properties

  // configure the servers and clients
  override def generateConfigs =
    TestUtils.createBrokerConfigs(1, zkConnect, enableControlledShutdown = false).map(KafkaConfig.fromProps(_, overridingProps))

  @Before
  override def setUp() {
    super.setUp()

    AdminUtils.createTopic(zkUtils, topic, 1, 1)
    props.setProperty("group.id", group)
    props.setProperty("zookeeper.connect", zkConnect)
  }

  @Test
  def testListGroupWithNoExistingGroup() {
    val opts = new ConsumerGroupCommandOptions(Array("--zookeeper", zkConnect))
    val consumerGroupCommand = new ZkConsumerGroupService(opts)
    try {
      assert(consumerGroupCommand.listGroups().isEmpty)
    } finally {
      consumerGroupCommand.close()
    }
  }

  @Test
  def testListGroupWithSomeGroups() {
    // mocks
    val consumer1Mock = EasyMock.createMockBuilder(classOf[OldConsumer]).withConstructor(topicFilter, props).createMock()
    props.setProperty("group.id", "some.other.group")
    val consumer2Mock = EasyMock.createMockBuilder(classOf[OldConsumer]).withConstructor(topicFilter, props).createMock()

    // stubs
    val opts = new ConsumerGroupCommandOptions(Array("--zookeeper", zkConnect))
    val consumerGroupCommand = new ZkConsumerGroupService(opts)

    // simulation
    EasyMock.replay(consumer1Mock)
    EasyMock.replay(consumer2Mock)

    // action/test
    TestUtils.waitUntilTrue(() => {
        val groups = consumerGroupCommand.listGroups()
        groups.size == 2 && groups.contains(group)
      }, "Expected a different list group results.")

    // cleanup
    consumerGroupCommand.close()
    consumer1Mock.stop()
    consumer2Mock.stop()
  }
}
