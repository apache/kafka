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

import joptsimple.OptionException
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, RoundRobinAssignor}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TimeoutException
import org.junit.Assert._
import org.junit.Test

import scala.concurrent.ExecutionException
import scala.util.Random

class DescribeConsumerGroupTest extends ConsumerGroupCommandTest {

  private val describeTypeOffsets = Array(Array(""), Array("--offsets"))
  private val describeTypeMembers = Array(Array("--members"), Array("--members", "--verbose"))
  private val describeTypeState = Array(Array("--state"))
  private val describeTypes = describeTypeOffsets ++ describeTypeMembers ++ describeTypeState

  @Test
  def testDescribeNonExistingGroup() {
    TestUtils.createOffsetsTopic(zkClient, servers)
    val missingGroup = "missing.group"

    for (describeType <- describeTypes) {
      // note the group to be queried is a different (non-existing) group
      val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", missingGroup) ++ describeType
      val service = getConsumerGroupService(cgcArgs)

      val output = TestUtils.grabConsoleOutput(service.describeGroups())
      assertTrue(s"Expected error was not detected for describe option '${describeType.mkString(" ")}'",
          output.contains(s"Consumer group '$missingGroup' does not exist."))
    }
  }

  @Test(expected = classOf[OptionException])
  def testDescribeWithMultipleSubActions() {
    TestUtils.createOffsetsTopic(zkClient, servers)
    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group, "--members", "--state")
    getConsumerGroupService(cgcArgs)
  }

  @Test
  def testDescribeOffsetsOfNonExistingGroup() {
    val group = "missing.group"
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run one consumer in the group consuming from a single-partition topic
    addConsumerGroupExecutor(numConsumers = 1)
    // note the group to be queried is a different (non-existing) group
    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    val (state, assignments) = service.collectGroupOffsets(group)
    assertTrue(s"Expected the state to be 'Dead', with no members in the group '$group'.",
        state.contains("Dead") && assignments.contains(List()))
  }

  @Test
  def testDescribeMembersOfNonExistingGroup() {
    val group = "missing.group"
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run one consumer in the group consuming from a single-partition topic
    addConsumerGroupExecutor(numConsumers = 1)
    // note the group to be queried is a different (non-existing) group
    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    val (state, assignments) = service.collectGroupMembers(group, false)
    assertTrue(s"Expected the state to be 'Dead', with no members in the group '$group'.",
        state.contains("Dead") && assignments.contains(List()))

    val (state2, assignments2) = service.collectGroupMembers(group, true)
    assertTrue(s"Expected the state to be 'Dead', with no members in the group '$group' (verbose option).",
        state2.contains("Dead") && assignments2.contains(List()))
  }

  @Test
  def testDescribeStateOfNonExistingGroup() {
    val group = "missing.group"
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run one consumer in the group consuming from a single-partition topic
    addConsumerGroupExecutor(numConsumers = 1)
    // note the group to be queried is a different (non-existing) group
    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    val state = service.collectGroupState(group)
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
      addConsumerGroupExecutor(numConsumers = 1, group = group)
      val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group) ++ describeType
      val service = getConsumerGroupService(cgcArgs)

      TestUtils.waitUntilTrue(() => {
        val (output, error) = TestUtils.grabConsoleOutputAndError(service.describeGroups())
        output.trim.split("\n").length == 2 && error.isEmpty
      }, s"Expected a data row and no error in describe results with describe type ${describeType.mkString(" ")}.", maxRetries = 3)
    }
  }

  @Test
  def testDescribeExistingGroupsRegex() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    for (describeType <- describeTypes) {

      for (prefix <- List("group1", "group2", "group3")) {
        val group = prefix + describeType.mkString("")
        addConsumerGroupExecutor(numConsumers = 1, group = group)
      }

      // select 2 groups, having "group1" and "group2" prefixes + describe type
      val regex   = """group[1-2]""" + describeType.mkString("")
      val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--regex", regex) ++ describeType
      val service = getConsumerGroupService(cgcArgs)

      TestUtils.waitUntilTrue(() => {
        val (output, error) = TestUtils.grabConsoleOutputAndError(service.describeGroups())
        output.trim.split("\n").length == 5 && error.isEmpty
      }, s"Expected a data row and no error in describe results with describe type ${describeType.mkString(" ")}.", maxRetries = 3)
    }
  }

  @Test
  def testDescribeExistingGroups() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // Create N single-threaded consumer groups from a single-partition topic
    val groups = (for (describeType <- describeTypes) yield {
      val group = this.group + describeType.mkString("")
      addConsumerGroupExecutor(numConsumers = 1, group = group)
      Array("--group", group)
    }).flatten

    val expectedNumLines = describeTypes.length * 2

    for (describeType <- describeTypes) {
      val cgcArgs = Array("--bootstrap-server", brokerList, "--describe") ++ groups ++ describeType
      val service = getConsumerGroupService(cgcArgs)

      TestUtils.waitUntilTrue(() => {
        val (output, error) = TestUtils.grabConsoleOutputAndError(service.describeGroups())
        val numLines = output.trim.split("\n").filterNot(line => line.isEmpty).length
        (numLines == expectedNumLines) && error.isEmpty
      }, s"Expected a data row and no error in describe results with describe type ${describeType.mkString(" ")}.", maxRetries = 3)
    }
  }

  @Test
  def testDescribeAllExistingGroups() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // Create N single-threaded consumer groups from a single-partition topic
    for (describeType <- describeTypes) {
      val group = this.group + describeType.mkString("")
      addConsumerGroupExecutor(numConsumers = 1, group = group)
    }

    val expectedNumLines = describeTypes.length * 2

    for (describeType <- describeTypes) {
      val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--all-groups") ++ describeType
      val service = getConsumerGroupService(cgcArgs)

      TestUtils.waitUntilTrue(() => {
        val (output, error) = TestUtils.grabConsoleOutputAndError(service.describeGroups())
        val numLines = output.trim.split("\n").filterNot(line => line.isEmpty).length
        (numLines == expectedNumLines) && error.isEmpty
      }, s"Expected a data row and no error in describe results with describe type ${describeType.mkString(" ")}.", maxRetries = 3)
    }
  }

  @Test
  def testDescribeOffsetsOfExistingGroup() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run one consumer in the group consuming from a single-partition topic
    addConsumerGroupExecutor(numConsumers = 1)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupOffsets(group)
      state.contains("Stable") &&
        assignments.isDefined &&
        assignments.get.count(_.group == group) == 1 &&
        assignments.get.filter(_.group == group).head.consumerId.exists(_.trim != ConsumerGroupCommand.MISSING_COLUMN_VALUE) &&
        assignments.get.filter(_.group == group).head.clientId.exists(_.trim != ConsumerGroupCommand.MISSING_COLUMN_VALUE) &&
        assignments.get.filter(_.group == group).head.host.exists(_.trim != ConsumerGroupCommand.MISSING_COLUMN_VALUE)
    }, s"Expected a 'Stable' group status, rows and valid values for consumer id / client id / host columns in describe results for group $group.", maxRetries = 3)
  }

  @Test
  def testDescribeMembersOfExistingGroup() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run one consumer in the group consuming from a single-partition topic
    addConsumerGroupExecutor(numConsumers = 1)
    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupMembers(group, false)
      state.contains("Stable") &&
        (assignments match {
          case Some(memberAssignments) =>
            memberAssignments.count(_.group == group) == 1 &&
              memberAssignments.filter(_.group == group).head.consumerId != ConsumerGroupCommand.MISSING_COLUMN_VALUE &&
              memberAssignments.filter(_.group == group).head.clientId != ConsumerGroupCommand.MISSING_COLUMN_VALUE &&
              memberAssignments.filter(_.group == group).head.host != ConsumerGroupCommand.MISSING_COLUMN_VALUE
          case None =>
            false
        })
    }, s"Expected a 'Stable' group status, rows and valid member information for group $group.", maxRetries = 3)

    val (_, assignments) = service.collectGroupMembers(group, true)
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
    addConsumerGroupExecutor(numConsumers = 1)
    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val state = service.collectGroupState(group)
      state.state == "Stable" &&
        state.numMembers == 1 &&
        state.assignmentStrategy == "range" &&
        state.coordinator != null &&
        servers.map(_.config.brokerId).toList.contains(state.coordinator.id)
    }, s"Expected a 'Stable' group status, with one member and round robin assignment strategy for group $group.", maxRetries = 3)
  }

  @Test
  def testDescribeStateOfExistingGroupWithRoundRobinAssignor() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run one consumer in the group consuming from a single-partition topic
    addConsumerGroupExecutor(numConsumers = 1, strategy = classOf[RoundRobinAssignor].getName)
    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val state = service.collectGroupState(group)
      state.state == "Stable" &&
        state.numMembers == 1 &&
        state.assignmentStrategy == "roundrobin" &&
        state.coordinator != null &&
        servers.map(_.config.brokerId).toList.contains(state.coordinator.id)
    }, s"Expected a 'Stable' group status, with one member and round robin assignment strategy for group $group.", maxRetries = 3)
  }

  @Test
  def testDescribeExistingGroupWithNoMembers() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    for (describeType <- describeTypes) {
      val group = this.group + describeType.mkString("")
      // run one consumer in the group consuming from a single-partition topic
      val executor = addConsumerGroupExecutor(numConsumers = 1, group = group)
      val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group) ++ describeType
      val service = getConsumerGroupService(cgcArgs)

      TestUtils.waitUntilTrue(() => {
        val (output, error) = TestUtils.grabConsoleOutputAndError(service.describeGroups())
        output.trim.split("\n").length == 2 && error.isEmpty
      }, s"Expected describe group results with one data row for describe type '${describeType.mkString(" ")}'", maxRetries = 3)

      // stop the consumer so the group has no active member anymore
      executor.shutdown()
      TestUtils.waitUntilTrue(() => {
        TestUtils.grabConsoleError(service.describeGroups()).contains(s"Consumer group '$group' has no active members.")
      }, s"Expected no active member in describe group results with describe type ${describeType.mkString(" ")}", maxRetries = 3)
    }
  }

  @Test
  def testDescribeOffsetsOfExistingGroupWithNoMembers() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run one consumer in the group consuming from a single-partition topic
    val executor = addConsumerGroupExecutor(numConsumers = 1)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupOffsets(group)
      state.contains("Stable") && assignments.exists(_.exists(_.group == group))
    }, "Expected the group to initially become stable, and to find group in assignments after initial offset commit.", maxRetries = 3)

    // stop the consumer so the group has no active member anymore
    executor.shutdown()

    val (result, succeeded) = TestUtils.computeUntilTrue(service.collectGroupOffsets(group)) {
      case (state, assignments) =>
        val testGroupAssignments = assignments.toSeq.flatMap(_.filter(_.group == group))
        def assignment = testGroupAssignments.head
        state.contains("Empty") &&
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
    val executor = addConsumerGroupExecutor(numConsumers = 1)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupMembers(group, false)
      state.contains("Stable") && assignments.exists(_.exists(_.group == group))
    }, "Expected the group to initially become stable, and to find group in assignments after initial offset commit.", maxRetries = 3)

    // stop the consumer so the group has no active member anymore
    executor.shutdown()

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupMembers(group, false)
      state.contains("Empty") && assignments.isDefined && assignments.get.isEmpty
    }, s"Expected no member in describe group members results for group '$group'", maxRetries = 3)
  }

  @Test
  def testDescribeStateOfExistingGroupWithNoMembers() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run one consumer in the group consuming from a single-partition topic
    val executor = addConsumerGroupExecutor(numConsumers = 1)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val state = service.collectGroupState(group)
      state.state == "Stable" &&
        state.numMembers == 1 &&
        state.coordinator != null &&
        servers.map(_.config.brokerId).toList.contains(state.coordinator.id)
    }, s"Expected the group '$group' to initially become stable, and have a single member.", maxRetries = 3)

    // stop the consumer so the group has no active member anymore
    executor.shutdown()

    TestUtils.waitUntilTrue(() => {
      val state = service.collectGroupState(group)
      state.state == "Empty" && state.numMembers == 0 && state.assignmentStrategy == ""
    }, s"Expected the group '$group' to become empty after the only member leaving.", maxRetries = 3)
  }

  @Test
  def testDescribeWithConsumersWithoutAssignedPartitions() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    for (describeType <- describeTypes) {
      val group = this.group + describeType.mkString("")
      // run two consumers in the group consuming from a single-partition topic
      addConsumerGroupExecutor(numConsumers = 2, group = group)
      val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group) ++ describeType
      val service = getConsumerGroupService(cgcArgs)

      TestUtils.waitUntilTrue(() => {
        val (output, error) = TestUtils.grabConsoleOutputAndError(service.describeGroups())
        val expectedNumRows = if (describeTypeMembers.contains(describeType)) 3 else 2
        error.isEmpty && output.trim.split("\n").size == expectedNumRows
      }, s"Expected a single data row in describe group result with describe type '${describeType.mkString(" ")}'", maxRetries = 3)
    }
  }

  @Test
  def testDescribeOffsetsWithConsumersWithoutAssignedPartitions() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run two consumers in the group consuming from a single-partition topic
    addConsumerGroupExecutor(numConsumers = 2)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupOffsets(group)
      state.contains("Stable") &&
        assignments.isDefined &&
        assignments.get.count(_.group == group) == 1 &&
        assignments.get.count { x => x.group == group && x.partition.isDefined } == 1
    }, "Expected rows for consumers with no assigned partitions in describe group results", maxRetries = 3)
  }

  @Test
  def testDescribeMembersWithConsumersWithoutAssignedPartitions() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run two consumers in the group consuming from a single-partition topic
    addConsumerGroupExecutor(numConsumers = 2)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupMembers(group, false)
      state.contains("Stable") &&
        assignments.isDefined &&
        assignments.get.count(_.group == group) == 2 &&
        assignments.get.count { x => x.group == group && x.numPartitions == 1 } == 1 &&
        assignments.get.count { x => x.group == group && x.numPartitions == 0 } == 1 &&
        assignments.get.count(_.assignment.nonEmpty) == 0
    }, "Expected rows for consumers with no assigned partitions in describe group results", maxRetries = 3)

    val (state, assignments) = service.collectGroupMembers(group, true)
    assertTrue("Expected additional columns in verbose version of describe members",
        state.contains("Stable") && assignments.get.count(_.assignment.nonEmpty) > 0)
  }

  @Test
  def testDescribeStateWithConsumersWithoutAssignedPartitions() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run two consumers in the group consuming from a single-partition topic
    addConsumerGroupExecutor(numConsumers = 2)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val state = service.collectGroupState(group)
      state.state == "Stable" && state.numMembers == 2
    }, "Expected two consumers in describe group results", maxRetries = 3)
  }

  @Test
  def testDescribeWithMultiPartitionTopicAndMultipleConsumers() {
    TestUtils.createOffsetsTopic(zkClient, servers)
    val topic2 = "foo2"
    createTopic(topic2, 2, 1)

    for (describeType <- describeTypes) {
      val group = this.group + describeType.mkString("")
      // run two consumers in the group consuming from a two-partition topic
      addConsumerGroupExecutor(2, topic2, group = group)
      val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group) ++ describeType
      val service = getConsumerGroupService(cgcArgs)

      TestUtils.waitUntilTrue(() => {
        val (output, error) = TestUtils.grabConsoleOutputAndError(service.describeGroups())
        val expectedNumRows = if (describeTypeState.contains(describeType)) 2 else 3
        error.isEmpty && output.trim.split("\n").size == expectedNumRows
      }, s"Expected a single data row in describe group result with describe type '${describeType.mkString(" ")}'", maxRetries = 3)
    }
  }

  @Test
  def testDescribeOffsetsWithMultiPartitionTopicAndMultipleConsumers() {
    TestUtils.createOffsetsTopic(zkClient, servers)
    val topic2 = "foo2"
    createTopic(topic2, 2, 1)

    // run two consumers in the group consuming from a two-partition topic
    addConsumerGroupExecutor(numConsumers = 2, topic2)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupOffsets(group)
      state.contains("Stable") &&
        assignments.isDefined &&
        assignments.get.count(_.group == group) == 2 &&
        assignments.get.count{ x => x.group == group && x.partition.isDefined} == 2 &&
        assignments.get.count{ x => x.group == group && x.partition.isEmpty} == 0
    }, "Expected two rows (one row per consumer) in describe group results.", maxRetries = 3)
  }

  @Test
  def testDescribeMembersWithMultiPartitionTopicAndMultipleConsumers() {
    TestUtils.createOffsetsTopic(zkClient, servers)
    val topic2 = "foo2"
    createTopic(topic2, 2, 1)

    // run two consumers in the group consuming from a two-partition topic
    addConsumerGroupExecutor(numConsumers = 2, topic2)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupMembers(group, false)
      state.contains("Stable") &&
        assignments.isDefined &&
        assignments.get.count(_.group == group) == 2 &&
        assignments.get.count{ x => x.group == group && x.numPartitions == 1 } == 2 &&
        assignments.get.count{ x => x.group == group && x.numPartitions == 0 } == 0
    }, "Expected two rows (one row per consumer) in describe group members results.", maxRetries = 3)

    val (state, assignments) = service.collectGroupMembers(group, true)
    assertTrue("Expected additional columns in verbose version of describe members",
        state.contains("Stable") && assignments.get.count(_.assignment.isEmpty) == 0)
  }

  @Test
  def testDescribeStateWithMultiPartitionTopicAndMultipleConsumers() {
    TestUtils.createOffsetsTopic(zkClient, servers)
    val topic2 = "foo2"
    createTopic(topic2, 2, 1)

    // run two consumers in the group consuming from a two-partition topic
    addConsumerGroupExecutor(numConsumers = 2, topic2)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val state = service.collectGroupState(group)
      state.state == "Stable" && state.group == group && state.numMembers == 2
    }, "Expected a stable group with two members in describe group state result.", maxRetries = 3)
  }

  @Test
  def testDescribeSimpleConsumerGroup() {
    // Ensure that the offsets of consumers which don't use group management are still displayed

    TestUtils.createOffsetsTopic(zkClient, servers)
    val topic2 = "foo2"
    createTopic(topic2, 2, 1)
    addSimpleGroupExecutor(Seq(new TopicPartition(topic2, 0), new TopicPartition(topic2, 1)))

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupOffsets(group)
      state.contains("Empty") && assignments.isDefined && assignments.get.count(_.group == group) == 2
    }, "Expected a stable group with two members in describe group state result.", maxRetries = 3)
  }

  @Test
  def testDescribeGroupWithShortInitializationTimeout() {
    // Let creation of the offsets topic happen during group initialization to ensure that initialization doesn't
    // complete before the timeout expires

    val describeType = describeTypes(Random.nextInt(describeTypes.length))
    val group = this.group + describeType.mkString("")
    // run one consumer in the group consuming from a single-partition topic
    addConsumerGroupExecutor(numConsumers = 1)
    // set the group initialization timeout too low for the group to stabilize
    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--timeout", "1", "--group", group) ++ describeType
    val service = getConsumerGroupService(cgcArgs)

    try {
      TestUtils.grabConsoleOutputAndError(service.describeGroups())
      fail(s"The consumer group command should have failed due to low initialization timeout (describe type: ${describeType.mkString(" ")})")
    } catch {
      case e: ExecutionException => assert(e.getCause.isInstanceOf[TimeoutException]) // OK
    }
  }

  @Test
  def testDescribeGroupOffsetsWithShortInitializationTimeout() {
    // Let creation of the offsets topic happen during group initialization to ensure that initialization doesn't
    // complete before the timeout expires

    // run one consumer in the group consuming from a single-partition topic
    addConsumerGroupExecutor(numConsumers = 1)

    // set the group initialization timeout too low for the group to stabilize
    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group, "--timeout", "1")
    val service = getConsumerGroupService(cgcArgs)

    try {
      service.collectGroupOffsets(group)
      fail("The consumer group command should fail due to low initialization timeout")
    } catch {
      case e : ExecutionException => assert(e.getCause.isInstanceOf[TimeoutException]) // OK
    }
  }

  @Test
  def testDescribeGroupMembersWithShortInitializationTimeout() {
    // Let creation of the offsets topic happen during group initialization to ensure that initialization doesn't
    // complete before the timeout expires

    // run one consumer in the group consuming from a single-partition topic
    addConsumerGroupExecutor(numConsumers = 1)

    // set the group initialization timeout too low for the group to stabilize
    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group, "--timeout", "1")
    val service = getConsumerGroupService(cgcArgs)

    try {
      service.collectGroupMembers(group, false)
      fail("The consumer group command should fail due to low initialization timeout")
    } catch {
      case e: ExecutionException => assert(e.getCause.isInstanceOf[TimeoutException])// OK
        try {
          service.collectGroupMembers(group, true)
          fail("The consumer group command should fail due to low initialization timeout (verbose)")
        } catch {
          case e: ExecutionException => assert(e.getCause.isInstanceOf[TimeoutException]) // OK
        }
    }
  }

  @Test
  def testDescribeGroupStateWithShortInitializationTimeout() {
    // Let creation of the offsets topic happen during group initialization to ensure that initialization doesn't
    // complete before the timeout expires

    // run one consumer in the group consuming from a single-partition topic
    addConsumerGroupExecutor(numConsumers = 1)

    // set the group initialization timeout too low for the group to stabilize
    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group, "--timeout", "1")
    val service = getConsumerGroupService(cgcArgs)

    try {
      service.collectGroupState(group)
      fail("The consumer group command should fail due to low initialization timeout")
    } catch {
      case e: ExecutionException => assert(e.getCause.isInstanceOf[TimeoutException]) // OK
    }
  }

  @Test(expected = classOf[joptsimple.OptionException])
  def testDescribeWithUnrecognizedNewConsumerOption() {
    val cgcArgs = Array("--new-consumer", "--bootstrap-server", brokerList, "--describe", "--group", group)
    getConsumerGroupService(cgcArgs)
    fail("Expected an error due to presence of unrecognized --new-consumer option")
  }

  @Test
  def testDescribeNonOffsetCommitGroup() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    val customProps = new Properties
    // create a consumer group that never commits offsets
    customProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    // run one consumer in the group consuming from a single-partition topic
    addConsumerGroupExecutor(numConsumers = 1, customPropsOpt = Some(customProps))

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupOffsets(group)
      state.contains("Stable") &&
        assignments.isDefined &&
        assignments.get.count(_.group == group) == 1 &&
        assignments.get.filter(_.group == group).head.consumerId.exists(_.trim != ConsumerGroupCommand.MISSING_COLUMN_VALUE) &&
        assignments.get.filter(_.group == group).head.clientId.exists(_.trim != ConsumerGroupCommand.MISSING_COLUMN_VALUE) &&
        assignments.get.filter(_.group == group).head.host.exists(_.trim != ConsumerGroupCommand.MISSING_COLUMN_VALUE)
    }, s"Expected a 'Stable' group status, rows and valid values for consumer id / client id / host columns in describe results for non-offset-committing group $group.")
  }

}

