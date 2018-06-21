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

import joptsimple.OptionException
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.RoundRobinAssignor
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TimeoutException
import org.junit.Assert._
import org.junit.Test

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

      val output = TestUtils.grabConsoleOutput(service.describeGroup())
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
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run one consumer in the group consuming from a single-partition topic
    addConsumerGroupExecutor(numConsumers = 1)
    // note the group to be queried is a different (non-existing) group
    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", "missing.group")
    val service = getConsumerGroupService(cgcArgs)

    val (state, assignments) = service.collectGroupOffsets()
    assertTrue(s"Expected the state to be 'Dead', with no members in the group '$group'.",
        state.contains("Dead") && assignments.contains(List()))
  }

  @Test
  def testDescribeMembersOfNonExistingGroup() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run one consumer in the group consuming from a single-partition topic
    addConsumerGroupExecutor(numConsumers = 1)
    // note the group to be queried is a different (non-existing) group
    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", "missing.group")
    val service = getConsumerGroupService(cgcArgs)

    val (state, assignments) = service.collectGroupMembers(false)
    assertTrue(s"Expected the state to be 'Dead', with no members in the group '$group'.",
        state.contains("Dead") && assignments.contains(List()))

    val (state2, assignments2) = service.collectGroupMembers(true)
    assertTrue(s"Expected the state to be 'Dead', with no members in the group '$group' (verbose option).",
        state2.contains("Dead") && assignments2.contains(List()))
  }

  @Test
  def testDescribeStateOfNonExistingGroup() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run one consumer in the group consuming from a single-partition topic
    addConsumerGroupExecutor(numConsumers = 1)
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
      addConsumerGroupExecutor(numConsumers = 1, group = group)
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
    addConsumerGroupExecutor(numConsumers = 1)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupOffsets()
      state.contains("Stable") &&
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
    addConsumerGroupExecutor(numConsumers = 1)
    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupMembers(false)
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
    }, s"Expected a 'Stable' group status, rows and valid member information for group $group.")

    val (_, assignments) = service.collectGroupMembers(true)
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
    addConsumerGroupExecutor(numConsumers = 1, strategy = classOf[RoundRobinAssignor].getName)
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
      val executor = addConsumerGroupExecutor(numConsumers = 1, group = group)
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
    val executor = addConsumerGroupExecutor(numConsumers = 1)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupOffsets()
      state.contains("Stable") && assignments.exists(_.exists(_.group == group))
    }, "Expected the group to initially become stable, and to find group in assignments after initial offset commit.")

    // stop the consumer so the group has no active member anymore
    executor.shutdown()

    val (result, succeeded) = TestUtils.computeUntilTrue(service.collectGroupOffsets()) {
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
      val (state, assignments) = service.collectGroupMembers(false)
      state.contains("Stable") && assignments.exists(_.exists(_.group == group))
    }, "Expected the group to initially become stable, and to find group in assignments after initial offset commit.")

    // stop the consumer so the group has no active member anymore
    executor.shutdown()

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupMembers(false)
      state.contains("Empty") && assignments.isDefined && assignments.get.isEmpty
    }, s"Expected no member in describe group members results for group '$group'")
  }

  @Test
  def testDescribeStateOfExistingGroupWithNoMembers() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run one consumer in the group consuming from a single-partition topic
    val executor = addConsumerGroupExecutor(numConsumers = 1)

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
      // run two consumers in the group consuming from a single-partition topic
      addConsumerGroupExecutor(numConsumers = 2, group = group)
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
    addConsumerGroupExecutor(numConsumers = 2)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupOffsets()
      state.contains("Stable") &&
        assignments.isDefined &&
        assignments.get.count(_.group == group) == 1 &&
        assignments.get.count { x => x.group == group && x.partition.isDefined } == 1
    }, "Expected rows for consumers with no assigned partitions in describe group results")
  }

  @Test
  def testDescribeMembersWithConsumersWithoutAssignedPartitions() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run two consumers in the group consuming from a single-partition topic
    addConsumerGroupExecutor(numConsumers = 2)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupMembers(false)
      state.contains("Stable") &&
        assignments.isDefined &&
        assignments.get.count(_.group == group) == 2 &&
        assignments.get.count { x => x.group == group && x.numPartitions == 1 } == 1 &&
        assignments.get.count { x => x.group == group && x.numPartitions == 0 } == 1 &&
        assignments.get.count(_.assignment.nonEmpty) == 0
    }, "Expected rows for consumers with no assigned partitions in describe group results")

    val (state, assignments) = service.collectGroupMembers(true)
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
      addConsumerGroupExecutor(2, topic2, group = group)
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
    addConsumerGroupExecutor(numConsumers = 2, topic2)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupOffsets()
      state.contains("Stable") &&
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
    addConsumerGroupExecutor(numConsumers = 2, topic2)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupMembers(false)
      state.contains("Stable") &&
        assignments.isDefined &&
        assignments.get.count(_.group == group) == 2 &&
        assignments.get.count{ x => x.group == group && x.numPartitions == 1 } == 2 &&
        assignments.get.count{ x => x.group == group && x.numPartitions == 0 } == 0
    }, "Expected two rows (one row per consumer) in describe group members results.")

    val (state, assignments) = service.collectGroupMembers(true)
    assertTrue("Expected additional columns in verbose version of describe members",
        state.contains("Stable") && assignments.get.count(_.assignment.isEmpty) == 0)
  }

  @Test
  def testDescribeStateWithMultiPartitionTopicAndMultipleConsumers() {
    TestUtils.createOffsetsTopic(zkClient, servers)
    val topic2 = "foo2"
    adminZkClient.createTopic(topic2, 2, 1)

    // run two consumers in the group consuming from a two-partition topic
    addConsumerGroupExecutor(numConsumers = 2, topic2)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val state = service.collectGroupState()
      state.state == "Stable" && state.group == group && state.numMembers == 2
    }, "Expected a stable group with two members in describe group state result.")
  }

  @Test
  def testDescribeSimpleConsumerGroup() {
    // Ensure that the offsets of consumers which don't use group management are still displayed

    TestUtils.createOffsetsTopic(zkClient, servers)
    val topic2 = "foo2"
    adminZkClient.createTopic(topic2, 2, 1)
    addSimpleGroupExecutor(Seq(new TopicPartition(topic2, 0), new TopicPartition(topic2, 1)))

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupOffsets()
      state.contains("Empty") && assignments.isDefined && assignments.get.count(_.group == group) == 2
    }, "Expected a stable group with two members in describe group state result.")
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
    addConsumerGroupExecutor(numConsumers = 1)

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
    addConsumerGroupExecutor(numConsumers = 1)

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
    addConsumerGroupExecutor(numConsumers = 1)

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

  @Test(expected = classOf[joptsimple.OptionException])
  def testDescribeWithUnrecognizedNewConsumerOption() {
    val cgcArgs = Array("--new-consumer", "--bootstrap-server", brokerList, "--describe", "--group", group)
    getConsumerGroupService(cgcArgs)
    fail("Expected an error due to presence of unrecognized --new-consumer option")
  }

}

