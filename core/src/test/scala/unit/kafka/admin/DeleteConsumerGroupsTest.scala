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
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class DeleteConsumerGroupsTest extends ConsumerGroupCommandTest {

  @Test
  def testDeleteWithTopicOption(): Unit = {
    TestUtils.createOffsetsTopic(zkClient, servers)
    val cgcArgs = Array("--bootstrap-server", brokerList, "--delete", "--group", group, "--topic")
    assertThrows(classOf[OptionException], () => getConsumerGroupService(cgcArgs))
  }

  @Test
  def testDeleteCmdNonExistingGroup(): Unit = {
    TestUtils.createOffsetsTopic(zkClient, servers)
    val missingGroup = "missing.group"

    val cgcArgs = Array("--bootstrap-server", brokerList, "--delete", "--group", missingGroup)
    val service = getConsumerGroupService(cgcArgs)

    val output = TestUtils.grabConsoleOutput(service.deleteGroups())
    assertTrue(output.contains(s"Group '$missingGroup' could not be deleted due to:") && output.contains(Errors.GROUP_ID_NOT_FOUND.message),
      s"The expected error (${Errors.GROUP_ID_NOT_FOUND}) was not detected while deleting consumer group")
  }

  @Test
  def testDeleteNonExistingGroup(): Unit = {
    TestUtils.createOffsetsTopic(zkClient, servers)
    val missingGroup = "missing.group"

    // note the group to be deleted is a different (non-existing) group
    val cgcArgs = Array("--bootstrap-server", brokerList, "--delete", "--group", missingGroup)
    val service = getConsumerGroupService(cgcArgs)

    val result = service.deleteGroups()
    assertTrue(result.size == 1 && result.keySet.contains(missingGroup) && result(missingGroup).getCause.isInstanceOf[GroupIdNotFoundException],
      s"The expected error (${Errors.GROUP_ID_NOT_FOUND}) was not detected while deleting consumer group")
  }

  @Test
  def testDeleteCmdNonEmptyGroup(): Unit = {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run one consumer in the group
    addConsumerGroupExecutor(numConsumers = 1)
    val cgcArgs = Array("--bootstrap-server", brokerList, "--delete", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      service.collectGroupMembers(group, false)._2.get.size == 1
    }, "The group did not initialize as expected.")

    val output = TestUtils.grabConsoleOutput(service.deleteGroups())
    assertTrue(output.contains(s"Group '$group' could not be deleted due to:") && output.contains(Errors.NON_EMPTY_GROUP.message),
      s"The expected error (${Errors.NON_EMPTY_GROUP}) was not detected while deleting consumer group. Output was: (${output})")
  }

  @Test
  def testDeleteNonEmptyGroup(): Unit = {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run one consumer in the group
    addConsumerGroupExecutor(numConsumers = 1)
    val cgcArgs = Array("--bootstrap-server", brokerList, "--delete", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      service.collectGroupMembers(group, false)._2.get.size == 1
    }, "The group did not initialize as expected.")

    val result = service.deleteGroups()
    assertNotNull(result(group),
      s"Group was deleted successfully, but it shouldn't have been. Result was:(${result})")
    assertTrue(result.size == 1 && result.keySet.contains(group) && result(group).getCause.isInstanceOf[GroupNotEmptyException],
      s"The expected error (${Errors.NON_EMPTY_GROUP}) was not detected while deleting consumer group. Result was:(${result})")
  }

  @Test
  def testDeleteCmdEmptyGroup(): Unit = {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run one consumer in the group
    val executor = addConsumerGroupExecutor(numConsumers = 1)
    val cgcArgs = Array("--bootstrap-server", brokerList, "--delete", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      service.listConsumerGroups().contains(group) && service.collectGroupState(group).state == "Stable"
    }, "The group did not initialize as expected.")

    executor.shutdown()

    TestUtils.waitUntilTrue(() => {
      service.collectGroupState(group).state == "Empty"
    }, "The group did not become empty as expected.")

    val output = TestUtils.grabConsoleOutput(service.deleteGroups())
    assertTrue(output.contains(s"Deletion of requested consumer groups ('$group') was successful."),
      s"The consumer group could not be deleted as expected")
  }

  @Test
  def testDeleteCmdAllGroups(): Unit = {
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
      service.listConsumerGroups().toSet == groups.keySet &&
        groups.keySet.forall(groupId => service.collectGroupState(groupId).state == "Stable")
    }, "The group did not initialize as expected.")

    // Shutdown consumers to empty out groups
    groups.values.foreach(executor => executor.shutdown())

    TestUtils.waitUntilTrue(() => {
      groups.keySet.forall(groupId => service.collectGroupState(groupId).state == "Empty")
    }, "The group did not become empty as expected.")

    val output = TestUtils.grabConsoleOutput(service.deleteGroups()).trim
    val expectedGroupsForDeletion = groups.keySet
    val deletedGroupsGrepped = output.substring(output.indexOf('(') + 1, output.indexOf(')')).split(',')
      .map(_.replaceAll("'", "").trim).toSet

    assertTrue(output.matches(s"Deletion of requested consumer groups (.*) was successful.")
        && deletedGroupsGrepped == expectedGroupsForDeletion, s"The consumer group(s) could not be deleted as expected"
    )
  }

  @Test
  def testDeleteEmptyGroup(): Unit = {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run one consumer in the group
    val executor = addConsumerGroupExecutor(numConsumers = 1)
    val cgcArgs = Array("--bootstrap-server", brokerList, "--delete", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      service.listConsumerGroups().contains(group) && service.collectGroupState(group).state == "Stable"
    }, "The group did not initialize as expected.")

    executor.shutdown()

    TestUtils.waitUntilTrue(() => {
      service.collectGroupState(group).state == "Empty"
    }, "The group did not become empty as expected.")

    val result = service.deleteGroups()
    assertTrue(result.size == 1 && result.keySet.contains(group) && result(group) == null,
      s"The consumer group could not be deleted as expected")
  }

  @Test
  def testDeleteCmdWithMixOfSuccessAndError(): Unit = {
    TestUtils.createOffsetsTopic(zkClient, servers)
    val missingGroup = "missing.group"

    // run one consumer in the group
    val executor = addConsumerGroupExecutor(numConsumers = 1)
    val cgcArgs = Array("--bootstrap-server", brokerList, "--delete", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      service.listConsumerGroups().contains(group) && service.collectGroupState(group).state == "Stable"
    }, "The group did not initialize as expected.")

    executor.shutdown()

    TestUtils.waitUntilTrue(() => {
      service.collectGroupState(group).state == "Empty"
    }, "The group did not become empty as expected.")

    val service2 = getConsumerGroupService(cgcArgs ++ Array("--group", missingGroup))
    val output = TestUtils.grabConsoleOutput(service2.deleteGroups())
    assertTrue(output.contains(s"Group '$missingGroup' could not be deleted due to:") && output.contains(Errors.GROUP_ID_NOT_FOUND.message) &&
        output.contains(s"These consumer groups were deleted successfully: '$group'"), s"The consumer group deletion did not work as expected")
  }

  @Test
  def testDeleteWithMixOfSuccessAndError(): Unit = {
    TestUtils.createOffsetsTopic(zkClient, servers)
    val missingGroup = "missing.group"

    // run one consumer in the group
    val executor = addConsumerGroupExecutor(numConsumers = 1)
    val cgcArgs = Array("--bootstrap-server", brokerList, "--delete", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      service.listConsumerGroups().contains(group) && service.collectGroupState(group).state == "Stable"
    }, "The group did not initialize as expected.")

    executor.shutdown()

    TestUtils.waitUntilTrue(() => {
      service.collectGroupState(group).state == "Empty"
    }, "The group did not become empty as expected.")

    val service2 = getConsumerGroupService(cgcArgs ++ Array("--group", missingGroup))
    val result = service2.deleteGroups()
    assertTrue(result.size == 2 &&
        result.keySet.contains(group) && result(group) == null &&
        result.keySet.contains(missingGroup) &&
        result(missingGroup).getMessage.contains(Errors.GROUP_ID_NOT_FOUND.message),
      s"The consumer group deletion did not work as expected")
  }


  @Test
  def testDeleteWithUnrecognizedNewConsumerOption(): Unit = {
    val cgcArgs = Array("--new-consumer", "--bootstrap-server", brokerList, "--delete", "--group", group)
    assertThrows(classOf[OptionException], () => getConsumerGroupService(cgcArgs))
  }
}
