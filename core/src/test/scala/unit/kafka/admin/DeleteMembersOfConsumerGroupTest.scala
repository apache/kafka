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
import org.junit.Test
import org.junit.Assert._

class DeleteMembersOfConsumerGroupTest extends ConsumerGroupCommandTest {

  @Test
  def testDeleteAllMembersFromGroup(): Unit = {
    TestUtils.createOffsetsTopic(zkClient, servers)
    // run one consumer in the group
    addConsumerGroupExecutor(numConsumers = 1)
    val cgcArgs = Array("--bootstrap-server", brokerList, "--remove-members", "--group", group, "--all-members")
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      service.listConsumerGroups().contains(group)
    }, "The group did not initialize as expected.")

    assertTrue(service.deleteMembersFromConsumerGroup())
  }

  @Test()
  def testDeleteAllMembersFromGroupWithMissingGroup(): Unit = {
    TestUtils.createOffsetsTopic(zkClient, servers)
    val missingGroup = "missing.group"
    val cgcArgs = Array("--bootstrap-server", brokerList, "--remove-members", "--group", missingGroup, "--all-members")
    val service = getConsumerGroupService(cgcArgs)
    assertFalse(service.deleteMembersFromConsumerGroup())
  }

  @Test()
  def testDeleteAllMembersFromGroupWithNoMembers(): Unit = {
    TestUtils.createOffsetsTopic(zkClient, servers)
    val missingMember = "missing.member"

    // run one consumer in the group
    addConsumerGroupExecutor(numConsumers = 1)
    val cgcArgs = Array("--bootstrap-server", brokerList, "--remove-members", "--group", group, "--member", missingMember)
    val service = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      service.listConsumerGroups().contains(group)
    }, "The group did not initialize as expected.")

    assertFalse(service.deleteMembersFromConsumerGroup())
  }
}
