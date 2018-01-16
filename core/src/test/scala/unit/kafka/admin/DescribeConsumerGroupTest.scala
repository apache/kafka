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
import org.junit.Assert._
import org.junit.{After, Before, Test}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class DescribeConsumerGroupTest extends KafkaServerTestHarness {
  private val topic = "foo"
  private val group = "test.group"

  private val describeTypeOffsets = Array(Array(""), Array("--offsets"))
  private val describeTypeMembers = Array(Array("--members"), Array("--members", "--verbose"))
  private val describeTypeState = Array(Array("--state"))
  private val describeTypes = describeTypeOffsets ++ describeTypeMembers ++ describeTypeState

  @deprecated("This field will be removed in a future release", "0.11.0.0")
  private val oldConsumers = new ArrayBuffer[OldConsumer]
  private var consumerGroupService: List[ConsumerGroupService] = List()
  private var consumerGroupExecutor: List[ConsumerGroupExecutor] = List()

  // configure the servers and clients
  override def generateConfigs = {
    TestUtils.createBrokerConfigs(1, zkConnect, enableControlledShutdown = false).map { props =>
      KafkaConfig.fromProps(props)
    }
  }

  @Before
  override def setUp() {
    super.setUp()
    adminZkClient.createTopic(topic, 1, 1)
  }

  @After
  override def tearDown(): Unit = {
    consumerGroupService.foreach(_.close)
    consumerGroupExecutor.foreach(_.shutdown)
    oldConsumers.foreach(_.stop())
    super.tearDown()
  }

  @Test
  @deprecated("This test has been deprecated and will be removed in a future release.", "0.11.0.0")
  def testDescribeNonExistingGroupWithOldConsumer() {
    TestUtils.createOffsetsTopic(zkClient, servers)
    createOldConsumer()
    val service = getConsumerGroupService(Array("--zookeeper", zkConnect, "--describe", "--group", "missing.group"))
    TestUtils.waitUntilTrue(() => service.collectGroupOffsets()._2.isEmpty, "Expected no rows in describe group results.")
  }

  @Test
  @deprecated("This test has been deprecated and will be removed in a future release.", "0.11.0.0")
  def testDescribeExistingGroupWithOldConsumer() {
    TestUtils.createOffsetsTopic(zkClient, servers)
    createOldConsumer()
    val service = getConsumerGroupService(Array("--zookeeper", zkConnect, "--describe", "--group", group))

    TestUtils.waitUntilTrue(() => {
      val (_, assignments) = service.collectGroupOffsets()
      assignments.isDefined &&
      assignments.get.count(_.group == group) == 1 &&
      assignments.get.filter(_.group == group).head.consumerId.exists(_.trim != ConsumerGroupCommand.MISSING_COLUMN_VALUE)
    }, "Expected rows and a consumer id column in describe group results.")
  }

  @Test
  @deprecated("This test has been deprecated and will be removed in a future release.", "0.11.0.0")
  def testDescribeExistingGroupWithNoMembersWithOldConsumer() {
    TestUtils.createOffsetsTopic(zkClient, servers)
    createOldConsumer()
    val service = getConsumerGroupService(Array("--zookeeper", zkConnect, "--describe", "--group", group))

    TestUtils.waitUntilTrue(() => {
      val (_, assignments) = service.collectGroupOffsets()
      assignments.isDefined &&
      assignments.get.count(_.group == group) == 1 &&
      assignments.get.filter(_.group == group).head.consumerId.exists(_.trim != ConsumerGroupCommand.MISSING_COLUMN_VALUE)
    }, "Expected rows and a consumer id column in describe group results.")
    oldConsumers.head.stop()

    TestUtils.waitUntilTrue(() => {
      val (_, assignments) = service.collectGroupOffsets()
      assignments.isDefined &&
      assignments.get.count(_.group == group) == 1 &&
      assignments.get.filter(_.group == group).head.consumerId.exists(_.trim == ConsumerGroupCommand.MISSING_COLUMN_VALUE) // the member should be gone
    }, "Expected no active member in describe group results.")
  }

  @Test
  @deprecated("This test has been deprecated and will be removed in a future release.", "0.11.0.0")
  def testDescribeConsumersWithNoAssignedPartitionsWithOldConsumer() {
    TestUtils.createOffsetsTopic(zkClient, servers)
    createOldConsumer()
    createOldConsumer()
    val service = getConsumerGroupService(Array("--zookeeper", zkConnect, "--describe", "--group", group))

    TestUtils.waitUntilTrue(() => {
      val (_, assignments) = service.collectGroupOffsets()
      assignments.isDefined &&
      assignments.get.count(_.group == group) == 2 &&
      assignments.get.count { x => x.group == group && x.partition.isDefined } == 1 &&
      assignments.get.count { x => x.group == group && x.partition.isEmpty } == 1
    }, "Expected rows for consumers with no assigned partitions in describe group results.")
  }

  @Test
  def testDescribeNonExistingGroup() {
    TestUtils.createOffsetsTopic(zkClient, servers)
    val missingGroup = "missing.group"

    for (describeType <- describeTypes) {
      // note the group to be queried is a different (non-existing) group
      val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", missingGroup) ++ describeType
      val service = getConsumerGroupService(cgcArgs)

      val output = TestUtils.grabConsoleOutput(service.describeGroup())
      assertTrue(s"Expected error was not detected for describe option '${describeType.mkString(" ")}'",
          output.contains(s"Consumer group '$missingGroup' does not exist."))
    }
  }

  @Test(expected = classOf[joptsimple.OptionException])
  def testDescribeWithMultipleSubActions() {
    TestUtils.createOffsetsTopic(zkClient, servers)
    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group, "--members", "--state")
    getConsumerGroupService(cgcArgs)
    fail("Expected an error due to presence of mutually exclusive options")
  }

  @Test
  def testDescribeOffsetsOfNonExistingGroup() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run one consumer in the group consuming from a single-partition topic
    addConsumerGroupExecutor(new ConsumerGroupExecutor(brokerList, 1, group, topic))
    // note the group to be queried is a different (non-existing) group
    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", "missing.group")
    val service = getConsumerGroupService(cgcArgs)

    val (state, assignments) = service.collectGroupOffsets()
    assertTrue(s"Expected the state to be 'Dead', with no members in the group '$group'.",
        state == Some("Dead") && assignments == Some(List()))
  }

  @Test
  def testDescribeMembersOfNonExistingGroup() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run one consumer in the group consuming from a single-partition topic
    addConsumerGroupExecutor(new ConsumerGroupExecutor(brokerList, 1, group, topic))
    // note the group to be queried is a different (non-existing) group
    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", "missing.group")
    val service = getConsumerGroupService(cgcArgs)

    val (state, assignments) = service.collectGroupMembers(false)
    assertTrue(s"Expected the state to be 'Dead', with no members in the group '$group'.",
        state == Some("Dead") && assignments == Some(List()))

    val (state2, assignments2) = service.collectGroupMembers(true)
    assertTrue(s"Expected the state to be 'Dead', with no members in the group '$group' (verbose option).",
        state2 == Some("Dead") && assignments2 == Some(List()))
  }

  @Test
  def testDescribeStateOfNonExistingGroup() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run one consumer in the group consuming from a single-partition topic
    addConsumerGroupExecutor(new ConsumerGroupExecutor(brokerList, 1, group, topic))
    // note the group to be queried is a different (non-existing) group
    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", "missing.group")
    val service = getConsumerGroupService(cgcArgs)

    val state = service.collectGroupState()
    assertTrue(s"Expected the state to be 'Dead', with no members in the group '$group'.",
        state.state == "Dead" && state.numMembers == 0 &&
        state.coordinator != null && servers.map(_.config.brokerId).toList.contains(state.coordinator.id)
    )
  }

  @Test
  def testDescribeExistingGroup() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    for (describeType <- describeTypes) {
      val group = this.group + describeType.mkString("")
      // run one consumer in the group consuming from a single-partition topic
      addConsumerGroupExecutor(new ConsumerGroupExecutor(brokerList, 1, group, topic))
      val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group) ++ describeType
      val service = getConsumerGroupService(cgcArgs)

      TestUtils.waitUntilTrue(() => {
        val (output, error) = TestUtils.grabConsoleOutputAndError(service.describeGroup())
        output.trim.split("\n").length == 2 && error.isEmpty
      }, s"Expected a data row and no error in describe results with describe type ${describeType.mkString(" ")}.")
    }
  }

  @Test
  def testDescribeOffsetsOfExistingGroup() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run one consumer in the group consuming from a single-partition topic
    addConsumerGroupExecutor(new ConsumerGroupExecutor(brokerList, 1, group, topic))
    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupOffsets()
      state == Some("Stable") &&
        assignments.isDefined &&
        assignments.get.count(_.group == group) == 1 &&
        assignments.get.filter(_.group == group).head.consumerId.exists(_.trim != ConsumerGroupCommand.MISSING_COLUMN_VALUE) &&
        assignments.get.filter(_.group == group).head.clientId.exists(_.trim != ConsumerGroupCommand.MISSING_COLUMN_VALUE) &&
        assignments.get.filter(_.group == group).head.host.exists(_.trim != ConsumerGroupCommand.MISSING_COLUMN_VALUE)
    }, s"Expected a 'Stable' group status, rows and valid values for consumer id / client id / host columns in describe results for group $group.")
  }

  @Test
  def testDescribeMembersOfExistingGroup() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run one consumer in the group consuming from a single-partition topic
    addConsumerGroupExecutor(new ConsumerGroupExecutor(brokerList, 1, group, topic))
    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupMembers(false)
      state == Some("Stable") &&
        (assignments match {
          case Some(memberAssignments) =>
            memberAssignments.count(_.group == group) == 1 &&
              memberAssignments.filter(_.group == group).head.consumerId != ConsumerGroupCommand.MISSING_COLUMN_VALUE &&
              memberAssignments.filter(_.group == group).head.clientId != ConsumerGroupCommand.MISSING_COLUMN_VALUE &&
              memberAssignments.filter(_.group == group).head.host != ConsumerGroupCommand.MISSING_COLUMN_VALUE
          case None =>
            false
        })
    }, s"Expected a 'Stable' group status, rows and valid member information for group $group.")

    val (state, assignments) = service.collectGroupMembers(true)
    assignments match {
      case None =>
        fail(s"Expected partition assignments for members of group $group")
      case Some(memberAssignments) =>
        assertTrue(s"Expected a topic partition assigned to the single group member for group $group",
          memberAssignments.size == 1 &&
          memberAssignments.head.assignment.size == 1)
    }
  }

  @Test
  def testDescribeStateOfExistingGroup() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run one consumer in the group consuming from a single-partition topic
    addConsumerGroupExecutor(new ConsumerGroupExecutor(brokerList, 1, group, topic))
    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val state = service.collectGroupState()
      state.state == "Stable" &&
        state.numMembers == 1 &&
        state.assignmentStrategy == "range" &&
        state.coordinator != null &&
        servers.map(_.config.brokerId).toList.contains(state.coordinator.id)
    }, s"Expected a 'Stable' group status, with one member and round robin assignment strategy for group $group.")
  }

  @Test
  def testDescribeStateOfExistingGroupWithRoundRobinAssignor() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run one consumer in the group consuming from a single-partition topic
    addConsumerGroupExecutor(new ConsumerGroupExecutor(brokerList, 1, group, topic, "org.apache.kafka.clients.consumer.RoundRobinAssignor"))
    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val state = service.collectGroupState()
      state.state == "Stable" &&
        state.numMembers == 1 &&
        state.assignmentStrategy == "roundrobin" &&
        state.coordinator != null &&
        servers.map(_.config.brokerId).toList.contains(state.coordinator.id)
    }, s"Expected a 'Stable' group status, with one member and round robin assignment strategy for group $group.")
  }

  @Test
  def testDescribeExistingGroupWithNoMembers() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    for (describeType <- describeTypes) {
      val group = this.group + describeType.mkString("")
      // run one consumer in the group consuming from a single-partition topic
      val executor = addConsumerGroupExecutor(new ConsumerGroupExecutor(brokerList, 1, group, topic))
      val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group) ++ describeType
      val service = getConsumerGroupService(cgcArgs)

      TestUtils.waitUntilTrue(() => {
        val (output, error) = TestUtils.grabConsoleOutputAndError(service.describeGroup())
        output.trim.split("\n").length == 2 && error.isEmpty
      }, s"Expected describe group results with one data row for describe type '${describeType.mkString(" ")}'")

      // stop the consumer so the group has no active member anymore
      executor.shutdown()

      TestUtils.waitUntilTrue(() => {
        TestUtils.grabConsoleError(service.describeGroup()).contains(s"Consumer group '$group' has no active members.")
      }, s"Expected no active member in describe group results with describe type ${describeType.mkString(" ")}")
    }
  }

  @Test
  def testDescribeOffsetsOfExistingGroupWithNoMembers() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run one consumer in the group consuming from a single-partition topic
    val executor = addConsumerGroupExecutor(new ConsumerGroupExecutor(brokerList, 1, group, topic))

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupOffsets()
      state == Some("Stable") && assignments.exists(_.exists(_.group == group))
    }, "Expected the group to initially become stable, and to find group in assignments after initial offset commit.")

    // stop the consumer so the group has no active member anymore
    executor.shutdown()

    val (result, succeeded) = TestUtils.computeUntilTrue(service.collectGroupOffsets()) {
      case (state, assignments) =>
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
  def testDescribeMembersOfExistingGroupWithNoMembers() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run one consumer in the group consuming from a single-partition topic
    val executor = addConsumerGroupExecutor(new ConsumerGroupExecutor(brokerList, 1, group, topic))

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupMembers(false)
      state == Some("Stable") && assignments.exists(_.exists(_.group == group))
    }, "Expected the group to initially become stable, and to find group in assignments after initial offset commit.")

    // stop the consumer so the group has no active member anymore
    executor.shutdown()

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupMembers(false)
      state == Some("Empty") && assignments.isDefined && assignments.get.isEmpty
    }, s"Expected no member in describe group members results for group '$group'")
  }

  @Test
  def testDescribeStateOfExistingGroupWithNoMembers() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run one consumer in the group consuming from a single-partition topic
    val executor = addConsumerGroupExecutor(new ConsumerGroupExecutor(brokerList, 1, group, topic))

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val state = service.collectGroupState()
      state.state == "Stable" &&
        state.numMembers == 1 &&
        state.coordinator != null &&
        servers.map(_.config.brokerId).toList.contains(state.coordinator.id)
    }, s"Expected the group '$group' to initially become stable, and have a single member.")

    // stop the consumer so the group has no active member anymore
    executor.shutdown()

    TestUtils.waitUntilTrue(() => {
      val state = service.collectGroupState()
      state.state == "Empty" && state.numMembers == 0 && state.assignmentStrategy == ""
    }, s"Expected the group '$group' to become empty after the only member leaving.")
  }

  @Test
  def testDescribeWithConsumersWithoutAssignedPartitions() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    for (describeType <- describeTypes) {
      val group = this.group + describeType.mkString("")
      // run one consumer in the group consuming from a single-partition topic
      addConsumerGroupExecutor(new ConsumerGroupExecutor(brokerList, 2, group, topic))
      val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group) ++ describeType
      val service = getConsumerGroupService(cgcArgs)

      TestUtils.waitUntilTrue(() => {
        val (output, error) = TestUtils.grabConsoleOutputAndError(service.describeGroup())
        val expectedNumRows = if (describeTypeMembers.contains(describeType)) 3 else 2
        error.isEmpty && output.trim.split("\n").size == expectedNumRows
      }, s"Expected a single data row in describe group result with describe type '${describeType.mkString(" ")}'")
    }
  }

  @Test
  def testDescribeOffsetsWithConsumersWithoutAssignedPartitions() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run two consumers in the group consuming from a single-partition topic
    addConsumerGroupExecutor(new ConsumerGroupExecutor(brokerList, 2, group, topic))

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupOffsets()
      state == Some("Stable") &&
        assignments.isDefined &&
        assignments.get.count(_.group == group) == 1 &&
        assignments.get.count { x => x.group == group && x.partition.isDefined } == 1
    }, "Expected rows for consumers with no assigned partitions in describe group results")
  }

  @Test
  def testDescribeMembersWithConsumersWithoutAssignedPartitions() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run two consumers in the group consuming from a single-partition topic
    addConsumerGroupExecutor(new ConsumerGroupExecutor(brokerList, 2, group, topic))

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupMembers(false)
      state == Some("Stable") &&
        assignments.isDefined &&
        assignments.get.count(_.group == group) == 2 &&
        assignments.get.count { x => x.group == group && x.numPartitions == 1 } == 1 &&
        assignments.get.count { x => x.group == group && x.numPartitions == 0 } == 1 &&
        assignments.get.count(_.assignment.size > 0) == 0
    }, "Expected rows for consumers with no assigned partitions in describe group results")

    val (state, assignments) = service.collectGroupMembers(true)
    assertTrue("Expected additional columns in verbose vesion of describe members",
        state == Some("Stable") && assignments.get.count(_.assignment.nonEmpty) > 0)
  }

  @Test
  def testDescribeStateWithConsumersWithoutAssignedPartitions() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run two consumers in the group consuming from a single-partition topic
    addConsumerGroupExecutor(new ConsumerGroupExecutor(brokerList, 2, group, topic))

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val state = service.collectGroupState()
      state.state == "Stable" && state.numMembers == 2
    }, "Expected two consumers in describe group results")
  }

  @Test
  def testDescribeWithMultiPartitionTopicAndMultipleConsumers() {
    TestUtils.createOffsetsTopic(zkClient, servers)
    val topic2 = "foo2"
    adminZkClient.createTopic(topic2, 2, 1)

    for (describeType <- describeTypes) {
      val group = this.group + describeType.mkString("")
      // run two consumers in the group consuming from a two-partition topic
      addConsumerGroupExecutor(new ConsumerGroupExecutor(brokerList, 2, group, topic2))
      val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group) ++ describeType
      val service = getConsumerGroupService(cgcArgs)

      TestUtils.waitUntilTrue(() => {
        val (output, error) = TestUtils.grabConsoleOutputAndError(service.describeGroup())
        val expectedNumRows = if (describeTypeState.contains(describeType)) 2 else 3
        error.isEmpty && output.trim.split("\n").size == expectedNumRows
      }, s"Expected a single data row in describe group result with describe type '${describeType.mkString(" ")}'")
    }
  }

  @Test
  def testDescribeOffsetsWithMultiPartitionTopicAndMultipleConsumers() {
    TestUtils.createOffsetsTopic(zkClient, servers)
    val topic2 = "foo2"
    adminZkClient.createTopic(topic2, 2, 1)

    // run two consumers in the group consuming from a two-partition topic
    addConsumerGroupExecutor(new ConsumerGroupExecutor(brokerList, 2, group, topic2))

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupOffsets()
      state == Some("Stable") &&
        assignments.isDefined &&
        assignments.get.count(_.group == group) == 2 &&
        assignments.get.count{ x => x.group == group && x.partition.isDefined} == 2 &&
        assignments.get.count{ x => x.group == group && x.partition.isEmpty} == 0
    }, "Expected two rows (one row per consumer) in describe group results.")
  }

  @Test
  def testDescribeMembersWithMultiPartitionTopicAndMultipleConsumers() {
    TestUtils.createOffsetsTopic(zkClient, servers)
    val topic2 = "foo2"
    adminZkClient.createTopic(topic2, 2, 1)

    // run two consumers in the group consuming from a two-partition topic
    addConsumerGroupExecutor(new ConsumerGroupExecutor(brokerList, 2, group, topic2))

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupMembers(false)
      state == Some("Stable") &&
        assignments.isDefined &&
        assignments.get.count(_.group == group) == 2 &&
        assignments.get.count{ x => x.group == group && x.numPartitions == 1 } == 2 &&
        assignments.get.count{ x => x.group == group && x.numPartitions == 0 } == 0
    }, "Expected two rows (one row per consumer) in describe group members results.")

    val (state, assignments) = service.collectGroupMembers(true)
    assertTrue("Expected additional columns in verbose vesion of describe members",
        state == Some("Stable") && assignments.get.count(_.assignment.isEmpty) == 0)
  }

  @Test
  def testDescribeStateWithMultiPartitionTopicAndMultipleConsumers() {
    TestUtils.createOffsetsTopic(zkClient, servers)
    val topic2 = "foo2"
    adminZkClient.createTopic(topic2, 2, 1)

    // run two consumers in the group consuming from a two-partition topic
    addConsumerGroupExecutor(new ConsumerGroupExecutor(brokerList, 2, group, topic2))

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val state = service.collectGroupState()
      state.state == "Stable" && state.group == group && state.numMembers == 2
    }, "Expected a stable group with two members in describe group state result.")
  }

  @Test
  def testDescribeGroupWithShortInitializationTimeout() {
    // Let creation of the offsets topic happen during group initialization to ensure that initialization doesn't
    // complete before the timeout expires

    val describeType = describeTypes(Random.nextInt(describeTypes.length))
    val group = this.group + describeType.mkString("")
    // run one consumer in the group consuming from a single-partition topic
    addConsumerGroupExecutor(new ConsumerGroupExecutor(brokerList, 1, group, topic))
    // set the group initialization timeout too low for the group to stabilize
    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--timeout", "1", "--group", group) ++ describeType
    val service = getConsumerGroupService(cgcArgs)

    try {
      TestUtils.grabConsoleOutputAndError(service.describeGroup())
      fail(s"The consumer group command should have failed due to low initialization timeout (describe type: ${describeType.mkString(" ")})")
    } catch {
      case _: TimeoutException => // OK
    }
  }

  @Test
  def testDescribeGroupOffsetsWithShortInitializationTimeout() {
    // Let creation of the offsets topic happen during group initialization to ensure that initialization doesn't
    // complete before the timeout expires

    // run one consumer in the group consuming from a single-partition topic
    addConsumerGroupExecutor(new ConsumerGroupExecutor(brokerList, 1, group, topic))

    // set the group initialization timeout too low for the group to stabilize
    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group, "--timeout", "1")
    val service = getConsumerGroupService(cgcArgs)

    try {
      service.collectGroupOffsets()
      fail("The consumer group command should fail due to low initialization timeout")
    } catch {
      case _: TimeoutException => // OK
    }
  }

  @Test
  def testDescribeGroupMembersWithShortInitializationTimeout() {
    // Let creation of the offsets topic happen during group initialization to ensure that initialization doesn't
    // complete before the timeout expires

    // run one consumer in the group consuming from a single-partition topic
    addConsumerGroupExecutor(new ConsumerGroupExecutor(brokerList, 1, group, topic))

    // set the group initialization timeout too low for the group to stabilize
    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group, "--timeout", "1")
    val service = getConsumerGroupService(cgcArgs)

    try {
      service.collectGroupMembers(false)
      fail("The consumer group command should fail due to low initialization timeout")
    } catch {
      case _: TimeoutException => // OK
        try {
          service.collectGroupMembers(true)
          fail("The consumer group command should fail due to low initialization timeout (verbose)")
        } catch {
          case _: TimeoutException => // OK
        }
    }
  }

  @Test
  def testDescribeGroupStateWithShortInitializationTimeout() {
    // Let creation of the offsets topic happen during group initialization to ensure that initialization doesn't
    // complete before the timeout expires

    // run one consumer in the group consuming from a single-partition topic
    addConsumerGroupExecutor(new ConsumerGroupExecutor(brokerList, 1, group, topic))

    // set the group initialization timeout too low for the group to stabilize
    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group, "--timeout", "1")
    val service = getConsumerGroupService(cgcArgs)

    try {
      service.collectGroupState()
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

  def getConsumerGroupService(args: Array[String]): ConsumerGroupService = {
    val opts = new ConsumerGroupCommandOptions(args)
    val service = if (opts.useOldConsumer) new ZkConsumerGroupService(opts) else new KafkaConsumerGroupService(opts)
    consumerGroupService = service :: consumerGroupService
    service
  }

  def addConsumerGroupExecutor(executor: ConsumerGroupExecutor): ConsumerGroupExecutor = {
    consumerGroupExecutor = executor :: consumerGroupExecutor
    executor
  }
}


class ConsumerThread(broker: String, id: Int, groupId: String, topic: String, strategy: String)
    extends Runnable {
  val props = new Properties
  props.put("bootstrap.servers", broker)
  props.put("group.id", groupId)
  props.put("key.deserializer", classOf[StringDeserializer].getName)
  props.put("value.deserializer", classOf[StringDeserializer].getName)
  props.put("partition.assignment.strategy", strategy)
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


class ConsumerGroupExecutor(broker: String, numConsumers: Int, groupId: String, topic: String, strategy: String = "org.apache.kafka.clients.consumer.RangeAssignor") {
  val executor: ExecutorService = Executors.newFixedThreadPool(numConsumers)
  private val consumers = new ArrayBuffer[ConsumerThread]()
  for (i <- 1 to numConsumers) {
    val consumer = new ConsumerThread(broker, i, groupId, topic, strategy)
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
