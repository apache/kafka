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
import org.apache.kafka.common.errors.{GroupIdNotFoundException, GroupNotEmptyException}
import org.apache.kafka.common.protocol.Errors
import org.junit.Assert._
import org.junit.Test

class DeleteConsumerGroupsTest extends ConsumerGroupCommandTest {

  @Test(expected = classOf[OptionException])
  def testDeleteWithTopicOption() {
    TestUtils.createOffsetsTopic(zkClient, servers)
    val cgcArgs = Array("--bootstrap-server", brokerList, "--delete", "--group", group, "--topic")
    getConsumerGroupService(cgcArgs)
  }

  @Test
  def testDeleteCmdNonExistingGroup() {
    TestUtils.createOffsetsTopic(zkClient, servers)
    val missingGroup = "missing.group"

    val cgcArgs = Array("--bootstrap-server", brokerList, "--delete", "--group", missingGroup)
    val service = getConsumerGroupService(cgcArgs)

    val output = TestUtils.grabConsoleOutput(service.deleteGroups())
    assertTrue(s"The expected error (${Errors.GROUP_ID_NOT_FOUND}) was not detected while deleting consumer group",
      output.contains(s"Group '$missingGroup' could not be deleted due to:") && output.contains(Errors.GROUP_ID_NOT_FOUND.message))
  }

  @Test
  def testDeleteNonExistingGroup() {
    TestUtils.createOffsetsTopic(zkClient, servers)
    val missingGroup = "missing.group"

    // note the group to be deleted is a different (non-existing) group
    val cgcArgs = Array("--bootstrap-server", brokerList, "--delete", "--group", missingGroup)
    val service = getConsumerGroupService(cgcArgs)

    val result = service.deleteGroups()
    assertTrue(s"The expected error (${Errors.GROUP_ID_NOT_FOUND}) was not detected while deleting consumer group",
      result.size == 1 && result.keySet.contains(missingGroup) && result(missingGroup).getCause
        .isInstanceOf[GroupIdNotFoundException])
  }

  @Test
  def testDeleteCmdNonEmptyGroup() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run one consumer in the group
    addConsumerGroupExecutor(numConsumers = 1)
    val cgcArgs = Array("--bootstrap-server", brokerList, "--delete", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      service.collectGroupMembers(group, false)._2.get.size == 1
    }, "The group did not initialize as expected.", maxRetries = 3)

    val output = TestUtils.grabConsoleOutput(service.deleteGroups())
    assertTrue(s"The expected error (${Errors.NON_EMPTY_GROUP}) was not detected while deleting consumer group. Output was: (${output})",
      output.contains(s"Group '$group' could not be deleted due to:") && output.contains(Errors.NON_EMPTY_GROUP.message))
  }

  @Test
  def testDeleteNonEmptyGroup() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run one consumer in the group
    addConsumerGroupExecutor(numConsumers = 1)
    val cgcArgs = Array("--bootstrap-server", brokerList, "--delete", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      service.collectGroupMembers(group, false)._2.get.size == 1
    }, "The group did not initialize as expected.", maxRetries = 3)

    val result = service.deleteGroups()
    assertNotNull(s"Group was deleted successfully, but it shouldn't have been. Result was:(${result})", result(group))
    assertTrue(s"The expected error (${Errors.NON_EMPTY_GROUP}) was not detected while deleting consumer group. Result was:(${result})",
      result.size == 1 && result.keySet.contains(group) && result(group).getCause.isInstanceOf[GroupNotEmptyException])
  }

  @Test
  def testDeleteCmdEmptyGroup() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run one consumer in the group
    val executor = addConsumerGroupExecutor(numConsumers = 1)
    val cgcArgs = Array("--bootstrap-server", brokerList, "--delete", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      service.listGroups().contains(group)
    }, "The group did not initialize as expected.", maxRetries = 3)

    executor.shutdown()

    TestUtils.waitUntilTrue(() => {
      service.collectGroupState(group).state == "Empty"
    }, "The group did not become empty as expected.", maxRetries = 3)

    val output = TestUtils.grabConsoleOutput(service.deleteGroups())
    assertTrue(s"The consumer group could not be deleted as expected",
      output.contains(s"Deletion of requested consumer groups ('$group') was successful."))
  }

  @Test
  def testDeleteCmdAllGroups() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // Create 3 groups with 1 consumer per each
    val groups =
      (for (i <- 1 to 3) yield {
        val group = this.group + i
        val executor = addConsumerGroupExecutor(numConsumers = 1, group = group)
        group -> executor
      }).toMap

    val cgcArgs = Array("--bootstrap-server", brokerList, "--delete", "--all-groups")
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      service.listGroups().forall(groupId => groups.keySet.contains(groupId))
    }, "The group did not initialize as expected.", maxRetries = 3)

    // Shutdown consumers to empty out groups
    groups.values.foreach(executor => executor.shutdown())

    TestUtils.waitUntilTrue(() => {
      groups.keySet.forall(groupId => service.collectGroupState(groupId).state == "Empty")
    }, "The group did not become empty as expected.", maxRetries = 3)

    val output = TestUtils.grabConsoleOutput(service.deleteGroups()).trim
    val expectedGroupsForDeletion = groups.keySet
    val deletedGroupsGrepped = output.substring(output.indexOf('(') + 1, output.indexOf(')')).split(',')
      .map(_.replaceAll("'", "").trim).toSet

    assertTrue(s"The consumer group(s) could not be deleted as expected",
      output.matches(s"Deletion of requested consumer groups (.*) was successful.")
        && deletedGroupsGrepped == expectedGroupsForDeletion
    )
  }

  @Test
  def testDeleteEmptyGroup() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run one consumer in the group
    val executor = addConsumerGroupExecutor(numConsumers = 1)
    val cgcArgs = Array("--bootstrap-server", brokerList, "--delete", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      service.listGroups().contains(group)
    }, "The group did not initialize as expected.", maxRetries = 3)

    executor.shutdown()

    TestUtils.waitUntilTrue(() => {
      service.collectGroupState(group).state == "Empty"
    }, "The group did not become empty as expected.", maxRetries = 3)

    val result = service.deleteGroups()
    assertTrue(s"The consumer group could not be deleted as expected",
      result.size == 1 && result.keySet.contains(group) && result(group) == null)
  }

  @Test
  def testDeleteEmptyGroupsRegex() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    val groups = List("group1", "group2", "group3")
    val executors =
      for (group <- groups)
        yield addConsumerGroupExecutor(numConsumers = 1, group = group)

    val regex   = "group[1-2]" // select 2 groups, namely "group1" and "group2"
    val cgcArgs = Array("--bootstrap-server", brokerList, "--delete", "--regex", regex)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      groups.forall(service.listGroups().contains)
    }, "Some groups did not initialize as expected.", maxRetries = 3)

    executors.foreach(_.shutdown())

    TestUtils.waitUntilTrue(() => {
      groups.forall(group => service.collectGroupState(group).state == "Empty")
    }, "All groups did become empty as expected.", maxRetries = 3)

    val groupsExpected = groups.init
    val groupsDeleted = service.deleteGroups()
    assertTrue(s"Some groups could not be deleted as expected",
        groupsDeleted.size == 2 &&
          groupsExpected.forall(groupsDeleted.keySet.contains) &&
          groupsDeleted.values.exists(deletionException => Option(deletionException).isEmpty))
  }

  @Test
  def testDeleteCmdWithMixOfSuccessAndError() {
    TestUtils.createOffsetsTopic(zkClient, servers)
    val missingGroup = "missing.group"

    // run one consumer in the group
    val executor = addConsumerGroupExecutor(numConsumers = 1)
    val cgcArgs = Array("--bootstrap-server", brokerList, "--delete", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      service.listGroups().contains(group)
    }, "The group did not initialize as expected.", maxRetries = 3)

    executor.shutdown()

    TestUtils.waitUntilTrue(() => {
      service.collectGroupState(group).state == "Empty"
    }, "The group did not become empty as expected.", maxRetries = 3)

    val service2 = getConsumerGroupService(cgcArgs ++ Array("--group", missingGroup))
    val output = TestUtils.grabConsoleOutput(service2.deleteGroups())
    assertTrue(s"The consumer group deletion did not work as expected",
      output.contains(s"Group '$missingGroup' could not be deleted due to:") && output.contains(Errors.GROUP_ID_NOT_FOUND.message) &&
        output.contains(s"These consumer groups were deleted successfully: '$group'"))
  }

  @Test
  def testDeleteWithMixOfSuccessAndError() {
    TestUtils.createOffsetsTopic(zkClient, servers)
    val missingGroup = "missing.group"

    // run one consumer in the group
    val executor = addConsumerGroupExecutor(numConsumers = 1)
    val cgcArgs = Array("--bootstrap-server", brokerList, "--delete", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      service.listGroups().contains(group)
    }, "The group did not initialize as expected.", maxRetries = 3)

    executor.shutdown()

    TestUtils.waitUntilTrue(() => {
      service.collectGroupState(group).state == "Empty"
    }, "The group did not become empty as expected.", maxRetries = 3)

    val service2 = getConsumerGroupService(cgcArgs ++ Array("--group", missingGroup))
    val result = service2.deleteGroups()
    assertTrue(s"The consumer group deletion did not work as expected",
      result.size == 2 &&
        result.keySet.contains(group) && result(group) == null &&
        result.keySet.contains(missingGroup) &&
        result(missingGroup).getMessage.contains(Errors.GROUP_ID_NOT_FOUND.message))
  }


  @Test(expected = classOf[OptionException])
  def testDeleteWithUnrecognizedNewConsumerOption() {
    val cgcArgs = Array("--new-consumer", "--bootstrap-server", brokerList, "--delete", "--group", group)
    getConsumerGroupService(cgcArgs)
  }
}
