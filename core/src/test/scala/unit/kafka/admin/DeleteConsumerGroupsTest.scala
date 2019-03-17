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
      result.size == 1 && result.keySet.contains(missingGroup) && result.get(missingGroup).get.getCause
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
      service.collectGroupMembers(false)._2.get.size == 1
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
      service.collectGroupMembers(false)._2.get.size == 1
    }, "The group did not initialize as expected.", maxRetries = 3)

    val result = service.deleteGroups()
    assertNotNull(s"Group was deleted successfully, but it shouldn't have been. Result was:(${result})", result.get(group).get)
    assertTrue(s"The expected error (${Errors.NON_EMPTY_GROUP}) was not detected while deleting consumer group. Result was:(${result})",
      result.size == 1 && result.keySet.contains(group) && result.get(group).get.getCause.isInstanceOf[GroupNotEmptyException])
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
      service.collectGroupState().state == "Empty"
    }, "The group did become empty as expected.", maxRetries = 3)

    val output = TestUtils.grabConsoleOutput(service.deleteGroups())
    assertTrue(s"The consumer group could not be deleted as expected",
      output.contains(s"Deletion of requested consumer groups ('$group') was successful."))
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
      service.collectGroupState().state == "Empty"
    }, "The group did become empty as expected.", maxRetries = 3)

    val result = service.deleteGroups()
    assertTrue(s"The consumer group could not be deleted as expected",
      result.size == 1 && result.keySet.contains(group) && result.get(group).get == null)
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
      service.collectGroupState().state == "Empty"
    }, "The group did become empty as expected.", maxRetries = 3)

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
      service.collectGroupState().state == "Empty"
    }, "The group did become empty as expected.", maxRetries = 3)

    val service2 = getConsumerGroupService(cgcArgs ++ Array("--group", missingGroup))
    val result = service2.deleteGroups()
    assertTrue(s"The consumer group deletion did not work as expected",
      result.size == 2 &&
        result.keySet.contains(group) && result.get(group).get == null &&
        result.keySet.contains(missingGroup) && result.get(missingGroup).get.getMessage.contains(Errors.GROUP_ID_NOT_FOUND.message))
  }


  @Test(expected = classOf[OptionException])
  def testDeleteWithUnrecognizedNewConsumerOption() {
    val cgcArgs = Array("--new-consumer", "--bootstrap-server", brokerList, "--delete", "--group", group)
    getConsumerGroupService(cgcArgs)
  }
}
