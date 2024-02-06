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
import kafka.utils.{Exit, TestInfoUtils, TestUtils}
import org.apache.kafka.clients.consumer.{ConsumerConfig, RoundRobinAssignor}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TimeoutException
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{MethodSource, ValueSource}

import scala.concurrent.ExecutionException
import scala.util.Random

class DescribeConsumerGroupTest extends ConsumerGroupCommandTest {
  private val describeTypeOffsets = Array(Array(""), Array("--offsets"))
  private val describeTypeMembers = Array(Array("--members"), Array("--members", "--verbose"))
  private val describeTypeState = Array(Array("--state"))
  private val describeTypes = describeTypeOffsets ++ describeTypeMembers ++ describeTypeState

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testDescribeNonExistingGroup(quorum: String, groupProtocol: String): Unit = {
    createOffsetsTopic()

    val missingGroup = "missing.group"

    for (describeType <- describeTypes) {
      // note the group to be queried is a different (non-existing) group
      val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--describe", "--group", missingGroup) ++ describeType
      val service = getConsumerGroupService(cgcArgs)

      val output = TestUtils.grabConsoleOutput(service.describeGroups())
      assertTrue(output.contains(s"Consumer group '$missingGroup' does not exist."),
        s"Expected error was not detected for describe option '${describeType.mkString(" ")}'")
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testDescribeWithMultipleSubActions(quorum: String): Unit = {
    var exitStatus: Option[Int] = None
    var exitMessage: Option[String] = None
    Exit.setExitProcedure { (status, err) =>
      exitStatus = Some(status)
      exitMessage = err
      throw new RuntimeException
    }
    val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--describe", "--group", group, "--members", "--state")
    try {
      ConsumerGroupCommand.main(cgcArgs)
    } catch {
      case e: RuntimeException => //expected
    } finally {
      Exit.resetExitProcedure()
    }
    assertEquals(Some(1), exitStatus)
    assertTrue(exitMessage.get.contains("Option [describe] takes at most one of these options"))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testDescribeWithStateValue(quorum: String): Unit = {
    var exitStatus: Option[Int] = None
    var exitMessage: Option[String] = None
    Exit.setExitProcedure { (status, err) =>
      exitStatus = Some(status)
      exitMessage = err
      throw new RuntimeException
    }
    val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--describe", "--all-groups", "--state", "Stable")
    try {
      ConsumerGroupCommand.main(cgcArgs)
    } catch {
      case e: RuntimeException => //expected
    } finally {
      Exit.resetExitProcedure()
    }
    assertEquals(Some(1), exitStatus)
    assertTrue(exitMessage.get.contains("Option [describe] does not take a value for [state]"))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testDescribeOffsetsOfNonExistingGroup(quorum: String, groupProtocol: String): Unit = {
    val group = "missing.group"
    createOffsetsTopic()

    // run one consumer in the group consuming from a single-partition topic
    addConsumerGroupExecutor(numConsumers = 1, groupProtocol = groupProtocol)
    // note the group to be queried is a different (non-existing) group
    val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    val (state, assignments) = service.collectGroupOffsets(group)
    assertTrue(state.contains("Dead") && assignments.contains(List()),
      s"Expected the state to be 'Dead', with no members in the group '$group'.")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testDescribeMembersOfNonExistingGroup(quorum: String, groupProtocol: String): Unit = {
    val group = "missing.group"
    createOffsetsTopic()

    // run one consumer in the group consuming from a single-partition topic
    addConsumerGroupExecutor(numConsumers = 1, groupProtocol = groupProtocol)
    // note the group to be queried is a different (non-existing) group
    val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    val (state, assignments) = service.collectGroupMembers(group, false)
    assertTrue(state.contains("Dead") && assignments.contains(List()),
      s"Expected the state to be 'Dead', with no members in the group '$group'.")

    val (state2, assignments2) = service.collectGroupMembers(group, true)
    assertTrue(state2.contains("Dead") && assignments2.contains(List()),
      s"Expected the state to be 'Dead', with no members in the group '$group' (verbose option).")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testDescribeStateOfNonExistingGroup(quorum: String, groupProtocol: String): Unit = {
    val group = "missing.group"
    createOffsetsTopic()

    // run one consumer in the group consuming from a single-partition topic
    addConsumerGroupExecutor(numConsumers = 1, groupProtocol = groupProtocol)
    // note the group to be queried is a different (non-existing) group
    val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    val state = service.collectGroupState(group)
    assertTrue(state.state == "Dead" && state.numMembers == 0 &&
      state.coordinator != null && brokers.map(_.config.brokerId).toList.contains(state.coordinator.id),
      s"Expected the state to be 'Dead', with no members in the group '$group'."
    )
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testDescribeExistingGroup(quorum: String, groupProtocol: String): Unit = {
    createOffsetsTopic()

    for (describeType <- describeTypes) {
      val group = this.group + describeType.mkString("")
      // run one consumer in the group consuming from a single-partition topic
      addConsumerGroupExecutor(numConsumers = 1, group = group, groupProtocol = groupProtocol)
      val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--describe", "--group", group) ++ describeType
      val service = getConsumerGroupService(cgcArgs)

      TestUtils.waitUntilTrue(() => {
        val (output, error) = TestUtils.grabConsoleOutputAndError(service.describeGroups())
        output.trim.split("\n").length == 2 && error.isEmpty
      }, s"Expected a data row and no error in describe results with describe type ${describeType.mkString(" ")}.")
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testDescribeExistingGroups(quorum: String, groupProtocol: String): Unit = {
    createOffsetsTopic()

    // Create N single-threaded consumer groups from a single-partition topic
    val groups = (for (describeType <- describeTypes) yield {
      val group = this.group + describeType.mkString("")
      addConsumerGroupExecutor(numConsumers = 1, group = group, groupProtocol = groupProtocol)
      Array("--group", group)
    }).flatten

    val expectedNumLines = describeTypes.length * 2

    for (describeType <- describeTypes) {
      val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--describe") ++ groups ++ describeType
      val service = getConsumerGroupService(cgcArgs)

      TestUtils.waitUntilTrue(() => {
        val (output, error) = TestUtils.grabConsoleOutputAndError(service.describeGroups())
        val numLines = output.trim.split("\n").count(line => line.nonEmpty)
        (numLines == expectedNumLines) && error.isEmpty
      }, s"Expected a data row and no error in describe results with describe type ${describeType.mkString(" ")}.")
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testDescribeAllExistingGroups(quorum: String, groupProtocol: String): Unit = {
    createOffsetsTopic()

    // Create N single-threaded consumer groups from a single-partition topic
    for (describeType <- describeTypes) {
      val group = this.group + describeType.mkString("")
      addConsumerGroupExecutor(numConsumers = 1, group = group, groupProtocol = groupProtocol)
    }

    val expectedNumLines = describeTypes.length * 2

    for (describeType <- describeTypes) {
      val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--describe", "--all-groups") ++ describeType
      val service = getConsumerGroupService(cgcArgs)

      TestUtils.waitUntilTrue(() => {
        val (output, error) = TestUtils.grabConsoleOutputAndError(service.describeGroups())
        val numLines = output.trim.split("\n").count(line => line.nonEmpty)
        (numLines == expectedNumLines) && error.isEmpty
      }, s"Expected a data row and no error in describe results with describe type ${describeType.mkString(" ")}.")
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testDescribeOffsetsOfExistingGroup(quorum: String, groupProtocol: String): Unit = {
    createOffsetsTopic()

    // run one consumer in the group consuming from a single-partition topic
    addConsumerGroupExecutor(numConsumers = 1, groupProtocol = groupProtocol)

    val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupOffsets(group)
      state.contains("Stable") &&
        assignments.isDefined &&
        assignments.get.count(_.group == group) == 1 &&
        assignments.get.filter(_.group == group).head.consumerId.exists(_.trim != ConsumerGroupCommand.MISSING_COLUMN_VALUE) &&
        assignments.get.filter(_.group == group).head.clientId.exists(_.trim != ConsumerGroupCommand.MISSING_COLUMN_VALUE) &&
        assignments.get.filter(_.group == group).head.host.exists(_.trim != ConsumerGroupCommand.MISSING_COLUMN_VALUE)
    }, s"Expected a 'Stable' group status, rows and valid values for consumer id / client id / host columns in describe results for group $group.")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testDescribeMembersOfExistingGroup(quorum: String, groupProtocol: String): Unit = {
    createOffsetsTopic()

    // run one consumer in the group consuming from a single-partition topic
    addConsumerGroupExecutor(numConsumers = 1, groupProtocol = groupProtocol)
    val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--describe", "--group", group)
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
    }, s"Expected a 'Stable' group status, rows and valid member information for group $group.")

    val (_, assignments) = service.collectGroupMembers(group, true)
    assignments match {
      case None =>
        fail(s"Expected partition assignments for members of group $group")
      case Some(memberAssignments) =>
        assertTrue(memberAssignments.size == 1 && memberAssignments.head.assignment.size == 1,
          s"Expected a topic partition assigned to the single group member for group $group")
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testDescribeStateOfExistingGroup(quorum: String, groupProtocol: String): Unit = {
    createOffsetsTopic()

    // run one consumer in the group consuming from a single-partition topic
    addConsumerGroupExecutor(
      numConsumers = 1,
      groupProtocol = groupProtocol,
      // This is only effective when new protocol is used.
      remoteAssignor = Some("range")
    )
    val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val state = service.collectGroupState(group)
      state.state == "Stable" &&
        state.numMembers == 1 &&
        state.assignmentStrategy == "range" &&
        state.coordinator != null &&
        brokers.map(_.config.brokerId).toList.contains(state.coordinator.id)
    }, s"Expected a 'Stable' group status, with one member and round robin assignment strategy for group $group.")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testDescribeStateOfExistingGroupWithNonDefaultAssignor(quorum: String, groupProtocol: String): Unit = {
    createOffsetsTopic()

    // run one consumer in the group consuming from a single-partition topic
    val expectedName = if (groupProtocol == "consumer") {
      addConsumerGroupExecutor(numConsumers = 1, remoteAssignor = Some("range"), groupProtocol = groupProtocol)
      "range"
    } else {
      addConsumerGroupExecutor(numConsumers = 1, strategy = classOf[RoundRobinAssignor].getName, groupProtocol = groupProtocol)
      "roundrobin"
    }


    val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val state = service.collectGroupState(group)
      state.state == "Stable" &&
        state.numMembers == 1 &&
        state.assignmentStrategy == expectedName &&
        state.coordinator != null &&
        brokers.map(_.config.brokerId).toList.contains(state.coordinator.id)
    }, s"Expected a 'Stable' group status, with one member and $expectedName assignment strategy for group $group.")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testDescribeExistingGroupWithNoMembers(quorum: String, groupProtocol: String): Unit = {
    createOffsetsTopic()

    for (describeType <- describeTypes) {
      val group = this.group + describeType.mkString("")
      // run one consumer in the group consuming from a single-partition topic
      val executor = addConsumerGroupExecutor(numConsumers = 1, group = group, groupProtocol = groupProtocol)
      val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--describe", "--group", group) ++ describeType
      val service = getConsumerGroupService(cgcArgs)

      TestUtils.waitUntilTrue(() => {
        val (output, error) = TestUtils.grabConsoleOutputAndError(service.describeGroups())
        output.trim.split("\n").length == 2 && error.isEmpty
      }, s"Expected describe group results with one data row for describe type '${describeType.mkString(" ")}'")

      // stop the consumer so the group has no active member anymore
      executor.shutdown()
      TestUtils.waitUntilTrue(() => {
        TestUtils.grabConsoleError(service.describeGroups()).contains(s"Consumer group '$group' has no active members.")
      }, s"Expected no active member in describe group results with describe type ${describeType.mkString(" ")}")
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testDescribeOffsetsOfExistingGroupWithNoMembers(quorum: String, groupProtocol: String): Unit = {
    createOffsetsTopic()

    // run one consumer in the group consuming from a single-partition topic
    val executor = addConsumerGroupExecutor(numConsumers = 1, groupProtocol = groupProtocol, syncCommit = true)

    val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupOffsets(group)
      state.contains("Stable") && assignments.exists(_.exists(assignment => assignment.group == group && assignment.offset.isDefined))
    }, "Expected the group to initially become stable, and to find group in assignments after initial offset commit.")

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
    assertTrue(succeeded, s"Expected no active member in describe group results, state: $state, assignments: $assignments")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testDescribeMembersOfExistingGroupWithNoMembers(quorum: String, groupProtocol: String): Unit = {
    createOffsetsTopic()

    // run one consumer in the group consuming from a single-partition topic
    val executor = addConsumerGroupExecutor(numConsumers = 1, groupProtocol = groupProtocol)

    val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupMembers(group, false)
      state.contains("Stable") && assignments.exists(_.exists(_.group == group))
    }, "Expected the group to initially become stable, and to find group in assignments after initial offset commit.")

    // stop the consumer so the group has no active member anymore
    executor.shutdown()

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupMembers(group, false)
      state.contains("Empty") && assignments.isDefined && assignments.get.isEmpty
    }, s"Expected no member in describe group members results for group '$group'")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testDescribeStateOfExistingGroupWithNoMembers(quorum: String, groupProtocol: String): Unit = {
    createOffsetsTopic()

    // run one consumer in the group consuming from a single-partition topic
    val executor = addConsumerGroupExecutor(numConsumers = 1, groupProtocol = groupProtocol)

    val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val state = service.collectGroupState(group)
      state.state == "Stable" &&
        state.numMembers == 1 &&
        state.coordinator != null &&
        brokers.map(_.config.brokerId).toList.contains(state.coordinator.id)
    }, s"Expected the group '$group' to initially become stable, and have a single member.")

    // stop the consumer so the group has no active member anymore
    executor.shutdown()

    TestUtils.waitUntilTrue(() => {
      val state = service.collectGroupState(group)
      state.state == "Empty" && state.numMembers == 0
    }, s"Expected the group '$group' to become empty after the only member leaving.")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testDescribeWithConsumersWithoutAssignedPartitions(quorum: String, groupProtocol: String): Unit = {
    createOffsetsTopic()

    for (describeType <- describeTypes) {
      val group = this.group + describeType.mkString("")
      // run two consumers in the group consuming from a single-partition topic
      addConsumerGroupExecutor(numConsumers = 2, group = group, groupProtocol = groupProtocol)
      val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--describe", "--group", group) ++ describeType
      val service = getConsumerGroupService(cgcArgs)

      TestUtils.waitUntilTrue(() => {
        val (output, error) = TestUtils.grabConsoleOutputAndError(service.describeGroups())
        val expectedNumRows = if (describeTypeMembers.contains(describeType)) 3 else 2
        error.isEmpty && output.trim.split("\n").size == expectedNumRows
      }, s"Expected a single data row in describe group result with describe type '${describeType.mkString(" ")}'")
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testDescribeOffsetsWithConsumersWithoutAssignedPartitions(quorum: String, groupProtocol: String): Unit = {
    createOffsetsTopic()

    // run two consumers in the group consuming from a single-partition topic
    addConsumerGroupExecutor(numConsumers = 2, groupProtocol = groupProtocol)

    val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupOffsets(group)
      state.contains("Stable") &&
        assignments.isDefined &&
        assignments.get.count(_.group == group) == 1 &&
        assignments.get.count { x => x.group == group && x.partition.isDefined } == 1
    }, "Expected rows for consumers with no assigned partitions in describe group results")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testDescribeMembersWithConsumersWithoutAssignedPartitions(quorum: String, groupProtocol: String): Unit = {
    createOffsetsTopic()

    // run two consumers in the group consuming from a single-partition topic
    addConsumerGroupExecutor(numConsumers = 2, groupProtocol = groupProtocol)

    val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupMembers(group, false)
      state.contains("Stable") &&
        assignments.isDefined &&
        assignments.get.count(_.group == group) == 2 &&
        assignments.get.count { x => x.group == group && x.numPartitions == 1 } == 1 &&
        assignments.get.count { x => x.group == group && x.numPartitions == 0 } == 1 &&
        !assignments.get.exists(_.assignment.nonEmpty)
    }, "Expected rows for consumers with no assigned partitions in describe group results")

    val (state, assignments) = service.collectGroupMembers(group, true)
    assertTrue(state.contains("Stable") && assignments.get.count(_.assignment.nonEmpty) > 0,
      "Expected additional columns in verbose version of describe members")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testDescribeStateWithConsumersWithoutAssignedPartitions(quorum: String, groupProtocol: String): Unit = {
    createOffsetsTopic()

    // run two consumers in the group consuming from a single-partition topic
    addConsumerGroupExecutor(numConsumers = 2, groupProtocol = groupProtocol)

    val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val state = service.collectGroupState(group)
      state.state == "Stable" && state.numMembers == 2
    }, "Expected two consumers in describe group results")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testDescribeWithMultiPartitionTopicAndMultipleConsumers(quorum: String, groupProtocol: String): Unit = {
    createOffsetsTopic()

    val topic2 = "foo2"
    createTopic(topic2, 2, 1)

    for (describeType <- describeTypes) {
      val group = this.group + describeType.mkString("")
      // run two consumers in the group consuming from a two-partition topic
      addConsumerGroupExecutor(2, topic2, group = group, groupProtocol = groupProtocol)
      val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--describe", "--group", group) ++ describeType
      val service = getConsumerGroupService(cgcArgs)

      TestUtils.waitUntilTrue(() => {
        val (output, error) = TestUtils.grabConsoleOutputAndError(service.describeGroups())
        val expectedNumRows = if (describeTypeState.contains(describeType)) 2 else 3
        error.isEmpty && output.trim.split("\n").size == expectedNumRows
      }, s"Expected a single data row in describe group result with describe type '${describeType.mkString(" ")}'")
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testDescribeOffsetsWithMultiPartitionTopicAndMultipleConsumers(quorum: String, groupProtocol: String): Unit = {
    createOffsetsTopic()

    val topic2 = "foo2"
    createTopic(topic2, 2, 1)

    // run two consumers in the group consuming from a two-partition topic
    addConsumerGroupExecutor(numConsumers = 2, topic2, groupProtocol = groupProtocol)

    val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupOffsets(group)
      state.contains("Stable") &&
        assignments.isDefined &&
        assignments.get.count(_.group == group) == 2 &&
        assignments.get.count { x => x.group == group && x.partition.isDefined } == 2 &&
        assignments.get.count { x => x.group == group && x.partition.isEmpty } == 0
    }, "Expected two rows (one row per consumer) in describe group results.")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testDescribeMembersWithMultiPartitionTopicAndMultipleConsumers(quorum: String, groupProtocol: String): Unit = {
    createOffsetsTopic()

    val topic2 = "foo2"
    createTopic(topic2, 2, 1)

    // run two consumers in the group consuming from a two-partition topic
    addConsumerGroupExecutor(numConsumers = 2, topic2, groupProtocol = groupProtocol)

    val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupMembers(group, false)
      state.contains("Stable") &&
        assignments.isDefined &&
        assignments.get.count(_.group == group) == 2 &&
        assignments.get.count { x => x.group == group && x.numPartitions == 1 } == 2 &&
        assignments.get.count { x => x.group == group && x.numPartitions == 0 } == 0
    }, "Expected two rows (one row per consumer) in describe group members results.")

    val (state, assignments) = service.collectGroupMembers(group, true)
    assertTrue(state.contains("Stable") && assignments.get.count(_.assignment.isEmpty) == 0,
      "Expected additional columns in verbose version of describe members")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testDescribeStateWithMultiPartitionTopicAndMultipleConsumers(quorum: String, groupProtocol: String): Unit = {
    createOffsetsTopic()

    val topic2 = "foo2"
    createTopic(topic2, 2, 1)

    // run two consumers in the group consuming from a two-partition topic
    addConsumerGroupExecutor(numConsumers = 2, topic2, groupProtocol = groupProtocol)

    val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val state = service.collectGroupState(group)
      state.state == "Stable" && state.group == group && state.numMembers == 2
    }, "Expected a stable group with two members in describe group state result.")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft", "kraft+kip848"))
  def testDescribeSimpleConsumerGroup(quorum: String): Unit = {
    // Ensure that the offsets of consumers which don't use group management are still displayed

    createOffsetsTopic()
    val topic2 = "foo2"
    createTopic(topic2, 2, 1)
    addSimpleGroupExecutor(Seq(new TopicPartition(topic2, 0), new TopicPartition(topic2, 1)))

    val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.collectGroupOffsets(group)
      state.contains("Empty") && assignments.isDefined && assignments.get.count(_.group == group) == 2
    }, "Expected a stable group with two members in describe group state result.")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testDescribeGroupWithShortInitializationTimeout(quorum: String, groupProtocol: String): Unit = {
    // Let creation of the offsets topic happen during group initialization to ensure that initialization doesn't
    // complete before the timeout expires

    val describeType = describeTypes(Random.nextInt(describeTypes.length))
    val group = this.group + describeType.mkString("")
    // run one consumer in the group consuming from a single-partition topic
    addConsumerGroupExecutor(numConsumers = 1, groupProtocol = groupProtocol)
    // set the group initialization timeout too low for the group to stabilize
    val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--describe", "--timeout", "1", "--group", group) ++ describeType
    val service = getConsumerGroupService(cgcArgs)

    val e = assertThrows(classOf[ExecutionException], () => TestUtils.grabConsoleOutputAndError(service.describeGroups()))
    assertEquals(classOf[TimeoutException], e.getCause.getClass)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testDescribeGroupOffsetsWithShortInitializationTimeout(quorum: String, groupProtocol: String): Unit = {
    // Let creation of the offsets topic happen during group initialization to ensure that initialization doesn't
    // complete before the timeout expires

    // run one consumer in the group consuming from a single-partition topic
    addConsumerGroupExecutor(numConsumers = 1, groupProtocol = groupProtocol)

    // set the group initialization timeout too low for the group to stabilize
    val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--describe", "--group", group, "--timeout", "1")
    val service = getConsumerGroupService(cgcArgs)

    val e = assertThrows(classOf[ExecutionException], () => service.collectGroupOffsets(group))
    assertEquals(classOf[TimeoutException], e.getCause.getClass)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testDescribeGroupMembersWithShortInitializationTimeout(quorum: String, groupProtocol: String): Unit = {
    // Let creation of the offsets topic happen during group initialization to ensure that initialization doesn't
    // complete before the timeout expires

    // run one consumer in the group consuming from a single-partition topic
    addConsumerGroupExecutor(numConsumers = 1, groupProtocol = groupProtocol)

    // set the group initialization timeout too low for the group to stabilize
    val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--describe", "--group", group, "--timeout", "1")
    val service = getConsumerGroupService(cgcArgs)

    var e = assertThrows(classOf[ExecutionException], () => service.collectGroupMembers(group, false))
    assertEquals(classOf[TimeoutException], e.getCause.getClass)
    e = assertThrows(classOf[ExecutionException], () => service.collectGroupMembers(group, true))
    assertEquals(classOf[TimeoutException], e.getCause.getClass)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testDescribeGroupStateWithShortInitializationTimeout(quorum: String, groupProtocol: String): Unit = {
    // Let creation of the offsets topic happen during group initialization to ensure that initialization doesn't
    // complete before the timeout expires

    // run one consumer in the group consuming from a single-partition topic
    addConsumerGroupExecutor(numConsumers = 1, groupProtocol = groupProtocol)

    // set the group initialization timeout too low for the group to stabilize
    val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--describe", "--group", group, "--timeout", "1")
    val service = getConsumerGroupService(cgcArgs)

    val e = assertThrows(classOf[ExecutionException], () => service.collectGroupState(group))
    assertEquals(classOf[TimeoutException], e.getCause.getClass)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testDescribeWithUnrecognizedNewConsumerOption(quorum: String): Unit = {
    val cgcArgs = Array("--new-consumer", "--bootstrap-server", bootstrapServers(), "--describe", "--group", group)
    assertThrows(classOf[joptsimple.OptionException], () => getConsumerGroupService(cgcArgs))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testDescribeNonOffsetCommitGroup(quorum: String, groupProtocol: String): Unit = {
    createOffsetsTopic()

    val customProps = new Properties
    // create a consumer group that never commits offsets
    customProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    // run one consumer in the group consuming from a single-partition topic
    addConsumerGroupExecutor(numConsumers = 1, customPropsOpt = Some(customProps), groupProtocol = groupProtocol)

    val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--describe", "--group", group)
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

