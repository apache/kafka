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
package unit.kafka.admin

import kafka.admin.ConsumerGroupCommandTest
import kafka.utils.TestUtils
import org.apache.kafka.common.protocol.Errors
import org.junit.Assert._
import org.junit.Test

class DeleteConsumerGroupTest extends ConsumerGroupCommandTest {

  @Test(expected = classOf[joptsimple.OptionException])
  def testDeleteWithTopicOption() {
    TestUtils.createOffsetsTopic(zkClient, servers)
    val cgcArgs = Array("--bootstrap-server", brokerList, "--delete", "--group", group, "--topic")
    getConsumerGroupService(cgcArgs)
    fail("Expected an error due to presence of mutually exclusive options")
  }

  @Test
  def testDeleteCmdNonExistingGroup() {
    TestUtils.createOffsetsTopic(zkClient, servers)
    val missingGroup = "missing.group"

    val cgcArgs = Array("--bootstrap-server", brokerList, "--delete", "--group", missingGroup)
    val service = getConsumerGroupService(cgcArgs)

    val output = TestUtils.grabConsoleOutput(service.deleteGroups())
    assertTrue(s"The expected error (${Errors.GROUP_ID_NOT_FOUND}) was not detected while deleting consumer group",
        output.contains(s"Group '$missingGroup' could not be deleted due to: ${Errors.GROUP_ID_NOT_FOUND.toString}"))
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
      result.size == 1 && result.keySet.contains(missingGroup) && result.get(missingGroup).contains(Errors.GROUP_ID_NOT_FOUND))
  }

  @Test
  def testDeleteCmdNonEmptyGroup() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run one consumer in the group
    addConsumerGroupExecutor(numConsumers = 1)
    val cgcArgs = Array("--bootstrap-server", brokerList, "--delete", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      service.listGroups().contains(group)
    }, "The group did not initialize as expected.")

    val output = TestUtils.grabConsoleOutput(service.deleteGroups())
    assertTrue(s"The expected error (${Errors.NON_EMPTY_GROUP}) was not detected while deleting consumer group",
      output.contains(s"Group '$group' could not be deleted due to: ${Errors.NON_EMPTY_GROUP}"))
  }

  @Test
  def testDeleteNonEmptyGroup() {
    TestUtils.createOffsetsTopic(zkClient, servers)

    // run one consumer in the group
    addConsumerGroupExecutor(numConsumers = 1)
    val cgcArgs = Array("--bootstrap-server", brokerList, "--delete", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      service.listGroups().contains(group)
    }, "The group did not initialize as expected.")

    val result = service.deleteGroups()
    assertTrue(s"The expected error (${Errors.NON_EMPTY_GROUP}) was not detected while deleting consumer group",
      result.size == 1 && result.keySet.contains(group) && result.get(group).contains(Errors.NON_EMPTY_GROUP))
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
    }, "The group did not initialize as expected.")

    executor.shutdown()

    TestUtils.waitUntilTrue(() => {
      service.collectGroupState().state == "Empty"
    }, "The group did become empty as expected.")

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
    }, "The group did not initialize as expected.")

    executor.shutdown()

    TestUtils.waitUntilTrue(() => {
      service.collectGroupState().state == "Empty"
    }, "The group did become empty as expected.")

    val result = service.deleteGroups()
    assertTrue(s"The consumer group could not be deleted as expected",
      result.size == 1 && result.keySet.contains(group) && result.get(group).contains(Errors.NONE))
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
    }, "The group did not initialize as expected.")

    executor.shutdown()

    TestUtils.waitUntilTrue(() => {
      service.collectGroupState().state == "Empty"
    }, "The group did become empty as expected.")

    val service2 = getConsumerGroupService(cgcArgs ++ Array("--group", missingGroup))
    val output = TestUtils.grabConsoleOutput(service2.deleteGroups())
    assertTrue(s"The consumer group deletion did not work as expected",
      output.contains(s"Group '$missingGroup' could not be deleted due to: ${Errors.GROUP_ID_NOT_FOUND}") &&
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
    }, "The group did not initialize as expected.")

    executor.shutdown()

    TestUtils.waitUntilTrue(() => {
      service.collectGroupState().state == "Empty"
    }, "The group did become empty as expected.")

    val service2 = getConsumerGroupService(cgcArgs ++ Array("--group", missingGroup))
    val result = service2.deleteGroups()
    assertTrue(s"The consumer group deletion did not work as expected",
      result.size == 2 &&
        result.keySet.contains(group) && result.get(group).contains(Errors.NONE) &&
        result.keySet.contains(missingGroup) && result.get(missingGroup).contains(Errors.GROUP_ID_NOT_FOUND))
  }

  @Test
  def testDeleteCmdWithShortInitialization() {
    // run one consumer in the group
    addConsumerGroupExecutor(numConsumers = 1)
    val cgcArgs = Array("--bootstrap-server", brokerList, "--delete", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    val output = TestUtils.grabConsoleOutput(service.deleteGroups())
    assertTrue(s"The consumer group deletion did not work as expected",
      output.contains(s"Group '$group' could not be deleted due to: ${Errors.COORDINATOR_NOT_AVAILABLE}"))
  }

  @Test
  def testDeleteWithShortInitialization() {
    // run one consumer in the group
    addConsumerGroupExecutor(numConsumers = 1)
    val cgcArgs = Array("--bootstrap-server", brokerList, "--delete", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    val result = service.deleteGroups()
    assertTrue(s"The consumer group deletion did not work as expected",
      result.size == 1 &&
        result.keySet.contains(group) && result.get(group).contains(Errors.COORDINATOR_NOT_AVAILABLE))
  }
}
