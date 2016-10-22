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


class DescribeConsumerGroupTest extends KafkaServerTestHarness {

  val overridingProps = new Properties()
  val topic = "foo"
  val topicFilter = new Whitelist(topic)
  val group = "test.group"
  val props = new Properties

  // configure the servers and clients
  override def generateConfigs() = TestUtils.createBrokerConfigs(1, zkConnect, enableControlledShutdown = false).map(KafkaConfig.fromProps(_, overridingProps))

  @Before
  override def setUp() {
    super.setUp()

    AdminUtils.createTopic(zkUtils, topic, 1, 1)
    props.setProperty("group.id", group)
    props.setProperty("zookeeper.connect", zkConnect)
  }

  @Test
  def testDescribeNonExistingGroup() {
    // mocks
    val consumerMock = EasyMock.createMockBuilder(classOf[OldConsumer]).withConstructor(topicFilter, props).createMock()

    // stubs
    val opts = new ConsumerGroupCommandOptions(Array("--zookeeper", zkConnect, "--describe", "--group", "missing.group"))
    val consumerGroupCommand = new ZkConsumerGroupService(opts)

    // simulation
    EasyMock.replay(consumerMock)

    // action/test
    TestUtils.waitUntilTrue(() => {
        !consumerGroupCommand.describeGroup()._2.isDefined
      }, "Expected no rows in describe group results.")

    // cleanup
    consumerGroupCommand.close()
    consumerMock.stop()
  }

  @Test
  def testDescribeExistingGroup() {
    // mocks
    val consumerMock = EasyMock.createMockBuilder(classOf[OldConsumer]).withConstructor(topicFilter, props).createMock()

    // stubs
    val opts = new ConsumerGroupCommandOptions(Array("--zookeeper", zkConnect, "--describe", "--group", group))
    val consumerGroupCommand = new ZkConsumerGroupService(opts)

    // simulation
    EasyMock.replay(consumerMock)

    // action/test
    TestUtils.waitUntilTrue(() => {
        val (state, assignments) = consumerGroupCommand.describeGroup()
        assignments.isDefined &&
        assignments.get.filter(_.group == group).size == 1 &&
        assignments.get.filter(_.group == group).head.consumerId.isDefined
      }, "Expected rows and a member id column in describe group results.")

    // cleanup
    consumerGroupCommand.close()
    consumerMock.stop()
  }

  @Test
  def testDescribeConsumersWithNoAssignedPartitions() {
    // mocks
    val consumer1Mock = EasyMock.createMockBuilder(classOf[OldConsumer]).withConstructor(topicFilter, props).createMock()
    val consumer2Mock = EasyMock.createMockBuilder(classOf[OldConsumer]).withConstructor(topicFilter, props).createMock()

    // stubs
    val opts = new ConsumerGroupCommandOptions(Array("--zookeeper", zkConnect, "--describe", "--group", group))
    val consumerGroupCommand = new ZkConsumerGroupService(opts)

    EasyMock.replay(consumer1Mock)
    EasyMock.replay(consumer2Mock)

    // action/test
    TestUtils.waitUntilTrue(() => {
        val (state, assignments) = consumerGroupCommand.describeGroup()
        assignments.isDefined &&
        assignments.get.filter(_.group == group).size == 2 &&
        assignments.get.filter{ x => x.group == group && x.partition.isDefined}.size == 1 &&
        assignments.get.filter{ x => x.group == group && !x.partition.isDefined}.size == 1
      }, "Expected rows for consumers with no assigned partitions in describe group results.")

    // cleanup
    consumerGroupCommand.close()
    consumer1Mock.stop()
    consumer2Mock.stop()
  }
}
