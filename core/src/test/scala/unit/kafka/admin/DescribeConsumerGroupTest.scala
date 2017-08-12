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

import org.junit.Assert._
import org.junit.{After, Before, Test}
import kafka.admin.ConsumerGroupCommand.{ConsumerGroupCommandOptions, ConsumerGroupService, KafkaConsumerGroupService, ZkConsumerGroupService}
import kafka.consumer.OldConsumer
import kafka.consumer.Whitelist
import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.mutable.ArrayBuffer

class DescribeConsumerGroupTest extends KafkaServerTestHarness {
  private val topic = "foo"
  private val group = "test.group"

  @deprecated("This field will be removed in a future release", "0.11.0.0")
  private val oldConsumers = new ArrayBuffer[OldConsumer]
  private var consumerGroupService: ConsumerGroupService = _
  private var consumerGroupExecutor: ConsumerGroupExecutor = _

  // configure the servers and clients
  override def generateConfigs = {
    TestUtils.createBrokerConfigs(1, zkConnect, enableControlledShutdown = false).map { props =>
      KafkaConfig.fromProps(props)
    }
  }

  @Before
  override def setUp() {
    super.setUp()
    AdminUtils.createTopic(zkUtils, topic, 1, 1)
  }

  @After
  override def tearDown(): Unit = {
    if (consumerGroupService != null)
      consumerGroupService.close()
    if (consumerGroupExecutor != null)
      consumerGroupExecutor.shutdown()
    oldConsumers.foreach(_.stop())
    super.tearDown()
  }

  @Test
  @deprecated("This test has been deprecated and will be removed in a future release.", "0.11.0.0")
  def testDescribeNonExistingGroup() {
    TestUtils.createOffsetsTopic(zkUtils, servers)
    createOldConsumer()
    val opts = new ConsumerGroupCommandOptions(Array("--zookeeper", zkConnect, "--describe", "--group", "missing.group"))
    consumerGroupService = new ZkConsumerGroupService(opts)
    TestUtils.waitUntilTrue(() => consumerGroupService.describeGroup()._2.isEmpty, "Expected no rows in describe group results.")
  }

  @Test
  @deprecated("This test has been deprecated and will be removed in a future release.", "0.11.0.0")
  def testDescribeExistingGroup() {
    TestUtils.createOffsetsTopic(zkUtils, servers)
    createOldConsumer()
    val opts = new ConsumerGroupCommandOptions(Array("--zookeeper", zkConnect, "--describe", "--group", group))
    consumerGroupService = new ZkConsumerGroupService(opts)
    TestUtils.waitUntilTrue(() => {
      val (_, assignments) = consumerGroupService.describeGroup()
      assignments.isDefined &&
      assignments.get.count(_.group == group) == 1 &&
      assignments.get.filter(_.group == group).head.consumerId.exists(_.trim != ConsumerGroupCommand.MISSING_COLUMN_VALUE)
    }, "Expected rows and a consumer id column in describe group results.")
  }

  @Test
  @deprecated("This test has been deprecated and will be removed in a future release.", "0.11.0.0")
  def testDescribeExistingGroupWithNoMembers() {
    TestUtils.createOffsetsTopic(zkUtils, servers)
    createOldConsumer()
    val opts = new ConsumerGroupCommandOptions(Array("--zookeeper", zkConnect, "--describe", "--group", group))
    consumerGroupService = new ZkConsumerGroupService(opts)

    TestUtils.waitUntilTrue(() => {
      val (_, assignments) = consumerGroupService.describeGroup()
      assignments.isDefined &&
      assignments.get.count(_.group == group) == 1 &&
      assignments.get.filter(_.group == group).head.consumerId.exists(_.trim != ConsumerGroupCommand.MISSING_COLUMN_VALUE)
    }, "Expected rows and a consumer id column in describe group results.")
    oldConsumers.head.stop()

    TestUtils.waitUntilTrue(() => {
      val (_, assignments) = consumerGroupService.describeGroup()
      assignments.isDefined &&
      assignments.get.count(_.group == group) == 1 &&
      assignments.get.filter(_.group == group).head.consumerId.exists(_.trim == ConsumerGroupCommand.MISSING_COLUMN_VALUE) // the member should be gone
    }, "Expected no active member in describe group results.")
  }

  @Test
  @deprecated("This test has been deprecated and will be removed in a future release.", "0.11.0.0")
  def testDescribeConsumersWithNoAssignedPartitions() {
    TestUtils.createOffsetsTopic(zkUtils, servers)
    createOldConsumer()
    createOldConsumer()
    val opts = new ConsumerGroupCommandOptions(Array("--zookeeper", zkConnect, "--describe", "--group", group))
    consumerGroupService = new ZkConsumerGroupService(opts)
    TestUtils.waitUntilTrue(() => {
      val (_, assignments) = consumerGroupService.describeGroup()
      assignments.isDefined &&
      assignments.get.count(_.group == group) == 2 &&
      assignments.get.count { x => x.group == group && x.partition.isDefined } == 1 &&
      assignments.get.count { x => x.group == group && x.partition.isEmpty } == 1
    }, "Expected rows for consumers with no assigned partitions in describe group results.")
  }

  @Test
  def testDescribeNonExistingGroupWithNewConsumer() {
    TestUtils.createOffsetsTopic(zkUtils, servers)
    // run one consumer in the group consuming from a single-partition topic
    consumerGroupExecutor = new ConsumerGroupExecutor(brokerList, 1, group, topic)

    // note the group to be queried is a different (non-existing) group
    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", "missing.group")
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    consumerGroupService = new KafkaConsumerGroupService(opts)

    val (state, assignments) = consumerGroupService.describeGroup()
    assertTrue("Expected the state to be 'Dead' with no members in the group.", state == Some("Dead") && assignments == Some(List()))
  }

  @Test
  def testDescribeExistingGroupWithNewConsumer() {
    TestUtils.createOffsetsTopic(zkUtils, servers)
    // run one consumer in the group consuming from a single-partition topic
    consumerGroupExecutor = new ConsumerGroupExecutor(brokerList, 1, group, topic)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    consumerGroupService = new KafkaConsumerGroupService(opts)

    TestUtils.waitUntilTrue(() => {
        val (state, assignments) = consumerGroupService.describeGroup()
        state == Some("Stable") &&
        assignments.isDefined &&
        assignments.get.count(_.group == group) == 1 &&
        assignments.get.filter(_.group == group).head.consumerId.exists(_.trim != ConsumerGroupCommand.MISSING_COLUMN_VALUE) &&
        assignments.get.filter(_.group == group).head.clientId.exists(_.trim != ConsumerGroupCommand.MISSING_COLUMN_VALUE) &&
        assignments.get.filter(_.group == group).head.host.exists(_.trim != ConsumerGroupCommand.MISSING_COLUMN_VALUE)
    }, "Expected a 'Stable' group status, rows and valid values for consumer id / client id / host columns in describe group results.")
  }

  @Test
  def testDescribeExistingGroupWithNoMembersWithNewConsumer() {
    TestUtils.createOffsetsTopic(zkUtils, servers)
    // run one consumer in the group consuming from a single-partition topic
    consumerGroupExecutor = new ConsumerGroupExecutor(brokerList, 1, group, topic)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    consumerGroupService = new KafkaConsumerGroupService(opts)

    TestUtils.waitUntilTrue(() => {
      val (state, _) = consumerGroupService.describeGroup()
      state == Some("Stable")
    }, "Expected the group to initially become stable.")

    // Group assignments in describeGroup rely on finding committed consumer offsets.
    // Wait for an offset commit before shutting down the group executor.
    TestUtils.waitUntilTrue(() => {
      val (_, assignments) = consumerGroupService.describeGroup()
      assignments.exists(_.exists(_.group == group))
    }, "Expected to find group in assignments after initial offset commit")

    // stop the consumer so the group has no active member anymore
    consumerGroupExecutor.shutdown()

    val (result, succeeded) = TestUtils.computeUntilTrue(consumerGroupService.describeGroup()) { case (state, assignments) =>
      val testGroupAssignments = assignments.toSeq.flatMap(_.filter(_.group == group))
      def assignment = testGroupAssignments.head
      state == Some("Empty") &&
        testGroupAssignments.size == 1 &&
        assignment.consumerId.exists(_.trim == ConsumerGroupCommand.MISSING_COLUMN_VALUE) && // the member should be gone
        assignment.clientId.exists(_.trim == ConsumerGroupCommand.MISSING_COLUMN_VALUE) &&
        assignment.host.exists(_.trim == ConsumerGroupCommand.MISSING_COLUMN_VALUE)
    }
    val (state, assignments) = result
    assertTrue(s"Expected no active member in describe group results, state: $state, assignments: $assignments",
      succeeded)
  }

  @Test
  def testDescribeConsumersWithNoAssignedPartitionsWithNewConsumer() {
    TestUtils.createOffsetsTopic(zkUtils, servers)
    // run two consumers in the group consuming from a single-partition topic
    consumerGroupExecutor = new ConsumerGroupExecutor(brokerList, 2, group, topic)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    consumerGroupService = new KafkaConsumerGroupService(opts)

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = consumerGroupService.describeGroup()
      state == Some("Stable") &&
        assignments.isDefined &&
        assignments.get.count(_.group == group) == 2 &&
        assignments.get.count { x => x.group == group && x.partition.isDefined } == 1 &&
        assignments.get.count { x => x.group == group && x.partition.isEmpty } == 1
    }, "Expected rows for consumers with no assigned partitions in describe group results")
  }

  @Test
  def testDescribeWithMultiPartitionTopicAndMultipleConsumersWithNewConsumer() {
    TestUtils.createOffsetsTopic(zkUtils, servers)
    val topic2 = "foo2"
    AdminUtils.createTopic(zkUtils, topic2, 2, 1)

    // run two consumers in the group consuming from a two-partition topic
    consumerGroupExecutor = new ConsumerGroupExecutor(brokerList, 2, group, topic2)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    consumerGroupService = new KafkaConsumerGroupService(opts)

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = consumerGroupService.describeGroup()
      state == Some("Stable") &&
      assignments.isDefined &&
      assignments.get.count(_.group == group) == 2 &&
      assignments.get.count{ x => x.group == group && x.partition.isDefined} == 2 &&
      assignments.get.count{ x => x.group == group && x.partition.isEmpty} == 0
    }, "Expected two rows (one row per consumer) in describe group results.")
  }

  @Test
  def testDescribeGroupWithNewConsumerWithShortInitializationTimeout() {
    // Let creation of the offsets topic happen during group initialisation to ensure that initialization doesn't
    // complete before the timeout expires

    // run one consumer in the group consuming from a single-partition topic
    consumerGroupExecutor = new ConsumerGroupExecutor(brokerList, 1, group, topic)

    // set the group initialization timeout too low for the group to stabilize
    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", "group", "--timeout", "1")
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    consumerGroupService = new KafkaConsumerGroupService(opts)

    try {
      consumerGroupService.describeGroup()
      fail("The consumer group command should fail due to low initialization timeout")
    } catch {
      case _: TimeoutException => // OK
    }
  }

  @deprecated("This test has been deprecated and will be removed in a future release.", "0.11.1.0")
  private def createOldConsumer(): Unit = {
    val consumerProps = new Properties
    consumerProps.setProperty("group.id", group)
    consumerProps.setProperty("zookeeper.connect", zkConnect)
    oldConsumers += new OldConsumer(Whitelist(topic), consumerProps)
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
      case _: WakeupException => // OK
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
  private val consumers = new ArrayBuffer[ConsumerThread]()
  for (i <- 1 to numConsumers) {
    val consumer = new ConsumerThread(broker, i, groupId, topic)
    consumers += consumer
    executor.submit(consumer)
  }

  def shutdown() {
    consumers.foreach(_.shutdown)
    executor.shutdown()
    try {
      executor.awaitTermination(5000, TimeUnit.MILLISECONDS)
    } catch {
      case e: InterruptedException =>
        e.printStackTrace()
    }
  }
}
