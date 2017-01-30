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

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.Collections
import java.util.Properties

import org.easymock.EasyMock
import org.junit.Before
import org.junit.Test

import kafka.admin.ConsumerGroupCommand.ConsumerGroupCommandOptions
import kafka.admin.ConsumerGroupCommand.KafkaConsumerGroupService
import kafka.admin.ConsumerGroupCommand.ZkConsumerGroupService
import kafka.consumer.OldConsumer
import kafka.consumer.Whitelist
import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils

import org.apache.kafka.common.errors.GroupCoordinatorNotAvailableException
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.clients.consumer.KafkaConsumer


class DescribeConsumerGroupTest extends KafkaServerTestHarness {

  val overridingProps = new Properties()
  val topic = "foo"
  val topicFilter = Whitelist(topic)
  val group = "test.group"
  val props = new Properties

  // configure the servers and clients
  override def generateConfigs() = TestUtils.createBrokerConfigs(1, zkConnect, enableControlledShutdown = false).map(KafkaConfig.fromProps(_, overridingProps))

  @Before
  override def setUp() {
    super.setUp()

    AdminUtils.createTopic(zkUtils, topic, 1, 1)
    props.setProperty("group.id", group)
  }

  @Test
  def testDescribeNonExistingGroup() {
    // mocks
    props.setProperty("zookeeper.connect", zkConnect)
    val consumerMock = EasyMock.createMockBuilder(classOf[OldConsumer]).withConstructor(topicFilter, props).createMock()

    // stubs
    val opts = new ConsumerGroupCommandOptions(Array("--zookeeper", zkConnect, "--describe", "--group", "missing.group"))
    val consumerGroupCommand = new ZkConsumerGroupService(opts)

    // simulation
    EasyMock.replay(consumerMock)

    // action/test
    TestUtils.waitUntilTrue(() => consumerGroupCommand.describeGroup()._2.isEmpty, "Expected no rows in describe group results.")

    // cleanup
    consumerGroupCommand.close()
    consumerMock.stop()
  }

  @Test
  def testDescribeExistingGroup() {
    // mocks
    props.setProperty("zookeeper.connect", zkConnect)
    val consumerMock = EasyMock.createMockBuilder(classOf[OldConsumer]).withConstructor(topicFilter, props).createMock()

    // stubs
    val opts = new ConsumerGroupCommandOptions(Array("--zookeeper", zkConnect, "--describe", "--group", group))
    val consumerGroupCommand = new ZkConsumerGroupService(opts)

    // simulation
    EasyMock.replay(consumerMock)

    // action/test
    TestUtils.waitUntilTrue(() => {
        val (_, assignments) = consumerGroupCommand.describeGroup()
        assignments.isDefined &&
        assignments.get.count(_.group == group) == 1 &&
        assignments.get.filter(_.group == group).head.consumerId.exists(_.trim != ConsumerGroupCommand.MISSING_COLUMN_VALUE)
      }, "Expected rows and a consumer id column in describe group results.")

    // cleanup
    consumerGroupCommand.close()
    consumerMock.stop()
  }

  @Test
  def testDescribeExistingGroupWithNoMembers() {
    // mocks
    props.setProperty("zookeeper.connect", zkConnect)
    val consumerMock = EasyMock.createMockBuilder(classOf[OldConsumer]).withConstructor(topicFilter, props).createMock()

    // stubs
    val opts = new ConsumerGroupCommandOptions(Array("--zookeeper", zkConnect, "--describe", "--group", group))
    val consumerGroupCommand = new ZkConsumerGroupService(opts)

    // simulation
    EasyMock.replay(consumerMock)

    // action/test
    TestUtils.waitUntilTrue(() => {
        val (_, assignments) = consumerGroupCommand.describeGroup()
        assignments.isDefined &&
        assignments.get.count(_.group == group) == 1 &&
        assignments.get.filter(_.group == group).head.consumerId.exists(_.trim != ConsumerGroupCommand.MISSING_COLUMN_VALUE)
      }, "Expected rows and a consumer id column in describe group results.")
    consumerMock.stop()

    TestUtils.waitUntilTrue(() => {
        val (_, assignments) = consumerGroupCommand.describeGroup()
        assignments.isDefined &&
        assignments.get.count(_.group == group) == 1 &&
        assignments.get.filter(_.group == group).head.consumerId.exists(_.trim == ConsumerGroupCommand.MISSING_COLUMN_VALUE) // the member should be gone
      }, "Expected no active member in describe group results.")

    // cleanup
    consumerGroupCommand.close()
  }

  @Test
  def testDescribeConsumersWithNoAssignedPartitions() {
    // mocks
    props.setProperty("zookeeper.connect", zkConnect)
    val consumer1Mock = EasyMock.createMockBuilder(classOf[OldConsumer]).withConstructor(topicFilter, props).createMock()
    val consumer2Mock = EasyMock.createMockBuilder(classOf[OldConsumer]).withConstructor(topicFilter, props).createMock()

    // stubs
    val opts = new ConsumerGroupCommandOptions(Array("--zookeeper", zkConnect, "--describe", "--group", group))
    val consumerGroupCommand = new ZkConsumerGroupService(opts)

    EasyMock.replay(consumer1Mock)
    EasyMock.replay(consumer2Mock)

    // action/test
    TestUtils.waitUntilTrue(() => {
        val (_, assignments) = consumerGroupCommand.describeGroup()
        assignments.isDefined &&
        assignments.get.count(_.group == group) == 2 &&
        assignments.get.count { x => x.group == group && x.partition.isDefined } == 1 &&
        assignments.get.count { x => x.group == group && !x.partition.isDefined } == 1
      }, "Expected rows for consumers with no assigned partitions in describe group results.")

    // cleanup
    consumerGroupCommand.close()
    consumer1Mock.stop()
    consumer2Mock.stop()
  }

  @Test
  def testDescribeNonExistingGroupWithNewConsumer() {
    // run one consumer in the group consuming from a single-partition topic
    val executor = new ConsumerGroupExecutor(brokerList, 1, group, topic)

    // note the group to be queried is a different (non-existing) group
    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", "missing.group")
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    val consumerGroupCommand = new KafkaConsumerGroupService(opts)

    TestUtils.waitUntilTrue(() => {
        try {
          val (state, assignments) = consumerGroupCommand.describeGroup()
          println(state == Some("Dead") && assignments == Some(List()))
          state == Some("Dead") && assignments == Some(List())
        } catch {
          case _: GroupCoordinatorNotAvailableException | _: IllegalArgumentException =>
            // Do nothing while the group initializes
            false
          case e: Throwable =>
            e.printStackTrace()
            throw e
        }
      }, "Expected the state to be 'Dead' with no members in the group.")

    consumerGroupCommand.close()
  }

  @Test
  def testDescribeExistingGroupWithNewConsumer() {
    // run one consumer in the group consuming from a single-partition topic
    val executor = new ConsumerGroupExecutor(brokerList, 1, group, topic)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    val consumerGroupCommand = new KafkaConsumerGroupService(opts)

    TestUtils.waitUntilTrue(() => {
        try {
          val (state, assignments) = consumerGroupCommand.describeGroup()
          state == Some("Stable") &&
          assignments.isDefined &&
          assignments.get.count(_.group == group) == 1 &&
          assignments.get.filter(_.group == group).head.consumerId.exists(_.trim != ConsumerGroupCommand.MISSING_COLUMN_VALUE) &&
          assignments.get.filter(_.group == group).head.clientId.exists(_.trim != ConsumerGroupCommand.MISSING_COLUMN_VALUE) &&
          assignments.get.filter(_.group == group).head.host.exists(_.trim != ConsumerGroupCommand.MISSING_COLUMN_VALUE)
        } catch {
          case _: GroupCoordinatorNotAvailableException | _: IllegalArgumentException =>
            // Do nothing while the group initializes
            false
          case e: Throwable =>
            throw e
        }
      }, "Expected a 'Stable' group status, rows and valid values for consumer id / client id / host columns in describe group results.")

    consumerGroupCommand.close()
  }

  @Test
  def testDescribeExistingGroupWithNoMembersWithNewConsumer() {
    // run one consumer in the group consuming from a single-partition topic
    val executor = new ConsumerGroupExecutor(brokerList, 1, group, topic)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    val consumerGroupCommand = new KafkaConsumerGroupService(opts)

    TestUtils.waitUntilTrue(() => {
        try {
          val (state, _) = consumerGroupCommand.describeGroup()
          state == Some("Stable")
        } catch {
          case _: GroupCoordinatorNotAvailableException | _: IllegalArgumentException =>
            // Do nothing while the group initializes
            false
          case e: Throwable =>
            throw e
        }
      }, "Expected the group to initially become stable.")

    // stop the consumer so the group has no active member anymore
    executor.shutdown()

    TestUtils.waitUntilTrue(() => {
        try {
          val (state, assignments) = consumerGroupCommand.describeGroup()
          state == Some("Empty") &&
          assignments.isDefined &&
          assignments.get.count(_.group == group) == 1 &&
          assignments.get.filter(_.group == group).head.consumerId.exists(_.trim == ConsumerGroupCommand.MISSING_COLUMN_VALUE) && // the member should be gone
          assignments.get.filter(_.group == group).head.clientId.exists(_.trim == ConsumerGroupCommand.MISSING_COLUMN_VALUE) &&
          assignments.get.filter(_.group == group).head.host.exists(_.trim == ConsumerGroupCommand.MISSING_COLUMN_VALUE)
        } catch {
          case _: GroupCoordinatorNotAvailableException | _: IllegalArgumentException =>
            // Do nothing while the group initializes
            false
          case e: Throwable =>
            throw e
        } finally {
          consumerGroupCommand.close()
        }
      }, "Expected no active member in describe group results.")
  }

  @Test
  def testDescribeConsumersWithNoAssignedPartitionsWithNewConsumer() {
    // run two consumers in the group consuming from a single-partition topic
    val executor = new ConsumerGroupExecutor(brokerList, 2, group, topic)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    val consumerGroupCommand = new KafkaConsumerGroupService(opts)

    TestUtils.waitUntilTrue(() => {
        try {
          val (state, assignments) = consumerGroupCommand.describeGroup()
          state == Some("Stable") &&
          assignments.isDefined &&
          assignments.get.count(_.group == group) == 2 &&
          assignments.get.count{ x => x.group == group && x.partition.isDefined} == 1 &&
          assignments.get.count{ x => x.group == group && !x.partition.isDefined} == 1
        } catch {
          case _: GroupCoordinatorNotAvailableException | _: IllegalArgumentException =>
            // Do nothing while the group initializes
            false
          case e: Throwable =>
            throw e
        }
      }, "Expected rows for consumers with no assigned partitions in describe group results.")

    consumerGroupCommand.close()
  }

  @Test
  def testDescribeWithMultiPartitionTopicAndMultipleConsumersWithNewConsumer() {
    val topic2 = "foo2"
    AdminUtils.createTopic(zkUtils, topic2, 2, 1)

    // run two consumers in the group consuming from a two-partition topic
    val executor = new ConsumerGroupExecutor(brokerList, 2, group, topic2)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    val consumerGroupCommand = new KafkaConsumerGroupService(opts)

    TestUtils.waitUntilTrue(() => {
        try {
          val (state, assignments) = consumerGroupCommand.describeGroup()
          state == Some("Stable") &&
          assignments.isDefined &&
          assignments.get.count(_.group == group) == 2 &&
          assignments.get.count{ x => x.group == group && x.partition.isDefined} == 2 &&
          assignments.get.count{ x => x.group == group && !x.partition.isDefined} == 0
        } catch {
          case _: GroupCoordinatorNotAvailableException | _: IllegalArgumentException =>
            // Do nothing while the group initializes
            false
          case e: Throwable =>
            throw e
        }
      }, "Expected two rows (one row per consumer) in describe group results.")

    consumerGroupCommand.close()
  }
}


class ConsumerThread(broker: String, id: Int, groupId: String, topic: String) extends Runnable {
  val props = new Properties
  props.put("bootstrap.servers", broker)
  props.put("group.id", groupId)
  props.put("key.deserializer", classOf[StringDeserializer].getName)
  props.put("value.deserializer", classOf[StringDeserializer].getName)
  val consumer = new KafkaConsumer(props)

  def run() {
    try {
      consumer.subscribe(Collections.singleton(topic))
      while (true)
        consumer.poll(Long.MaxValue)
    } catch {
      case e: WakeupException => // OK
    } finally {
      consumer.close()
    }
  }

  def shutdown() {
    consumer.wakeup()
  }
}


class ConsumerGroupExecutor(broker: String, numConsumers: Int, groupId: String, topic: String) {
  val executor: ExecutorService = Executors.newFixedThreadPool(numConsumers)
  var consumers = List[ConsumerThread]()
  for (i <- 1 to numConsumers) {
    val consumer = new ConsumerThread(broker, i, groupId, topic)
    consumers ++= List(consumer)
    executor.submit(consumer);
  }

  Runtime.getRuntime().addShutdownHook(new Thread() {
    override def run() {
      shutdown()
    }
  })

  def shutdown() {
    consumers.foreach(_.shutdown)
    executor.shutdown();
    try {
      executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
    } catch {
      case e: InterruptedException =>
        e.printStackTrace()
    }
  }
}
