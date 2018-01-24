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

import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TimeoutException
import org.junit.Assert._
import org.junit.Test

class DescribeConsumerGroupTest extends ConsumerGroupCommandTest {

  @Test
  @deprecated("This test has been deprecated and will be removed in a future release.", "0.11.0.0")
  def testDescribeNonExistingGroup() {
    TestUtils.createOffsetsTopic(zkUtils, servers)
    createOldConsumer()
    val cgcArgs = Array("--zookeeper", zkConnect, "--describe", "--group", "missing.group")
    val consumerGroupService = getConsumerGroupService(cgcArgs)
    TestUtils.waitUntilTrue(() => consumerGroupService.describeGroup()._2.isEmpty, "Expected no rows in describe group results.")
  }

  @Test
  def testDescribeSimpleConsumerGroup() {
    // Ensure that the offsets of consumers which don't use group management are still displayed

    TestUtils.createOffsetsTopic(zkUtils, servers)
    val topic2 = "foo2"
    AdminUtils.createTopic(zkUtils, topic2, 2, 1)
    addSimpleGroupExecutor(Seq(new TopicPartition(topic2, 0), new TopicPartition(topic2, 1)))

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val (state, assignments) = service.describeGroup()
      println(assignments.get.map(x => (x.topic, x.partition)))
      state.contains("Empty") && assignments.isDefined && assignments.get.count(_.group == group) == 2
    }, "Expected two partition assignment results in describe group state result.")
  }

  @Test
  @deprecated("This test has been deprecated and will be removed in a future release.", "0.11.0.0")
  def testDescribeExistingGroup() {
    TestUtils.createOffsetsTopic(zkUtils, servers)
    createOldConsumer()
    val consumerGroupService = getConsumerGroupService(Array("--zookeeper", zkConnect, "--describe", "--group", group))
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
    val consumerGroupService = getConsumerGroupService(Array("--zookeeper", zkConnect, "--describe", "--group", group))

    TestUtils.waitUntilTrue(() => {
      val (_, assignments) = consumerGroupService.describeGroup()
      assignments.isDefined &&
      assignments.get.count(_.group == group) == 1 &&
      assignments.get.filter(_.group == group).head.consumerId.exists(_.trim != ConsumerGroupCommand.MISSING_COLUMN_VALUE)
    }, "Expected rows and a consumer id column in describe group results.")
    stopRandomOldConsumer()

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
    val consumerGroupService = getConsumerGroupService(Array("--zookeeper", zkConnect, "--describe", "--group", group))
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
    addConsumerGroupExecutor(numConsumers = 1)

    // note the group to be queried is a different (non-existing) group
    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", "missing.group")
    val consumerGroupService = getConsumerGroupService(cgcArgs)

    val (state, assignments) = consumerGroupService.describeGroup()
    assertTrue("Expected the state to be 'Dead' with no members in the group.", state == Some("Dead") && assignments == Some(List()))
  }

  @Test
  def testDescribeExistingGroupWithNewConsumer() {
    TestUtils.createOffsetsTopic(zkUtils, servers)
    // run one consumer in the group consuming from a single-partition topic
    addConsumerGroupExecutor(numConsumers = 1)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val consumerGroupService = getConsumerGroupService(cgcArgs)

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
    val consumerGroupExecutor = addConsumerGroupExecutor(numConsumers = 1)
    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val consumerGroupService = getConsumerGroupService(cgcArgs)

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
    addConsumerGroupExecutor(numConsumers = 2)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val consumerGroupService = getConsumerGroupService(cgcArgs)

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
    addConsumerGroupExecutor(numConsumers = 2, topic = topic2)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val consumerGroupService = getConsumerGroupService(cgcArgs)

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
    addConsumerGroupExecutor(numConsumers = 1)

    // set the group initialization timeout too low for the group to stabilize
    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", "group", "--timeout", "1")
    val consumerGroupService = getConsumerGroupService(cgcArgs)

    try {
      consumerGroupService.describeGroup()
      fail("The consumer group command should fail due to low initialization timeout")
    } catch {
      case _: TimeoutException => // OK
    }
  }

}
