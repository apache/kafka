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
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import kafka.utils.TestUtils
import org.apache.kafka.common.ConsumerGroupState
import org.apache.kafka.clients.admin.ConsumerGroupListing
import java.util.Optional

class ListConsumerGroupTest extends ConsumerGroupCommandTest {

  @Test
  def testListConsumerGroups(): Unit = {
    val simpleGroup = "simple-group"
    addSimpleGroupExecutor(group = simpleGroup)
    addConsumerGroupExecutor(numConsumers = 1)

    val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--list")
    val service = getConsumerGroupService(cgcArgs)

    val expectedGroups = Set(group, simpleGroup)
    var foundGroups = Set.empty[String]
    TestUtils.waitUntilTrue(() => {
      foundGroups = service.listConsumerGroups().toSet
      expectedGroups == foundGroups
    }, s"Expected --list to show groups $expectedGroups, but found $foundGroups.")
  }

  @Test
  def testListWithUnrecognizedNewConsumerOption(): Unit = {
    val cgcArgs = Array("--new-consumer", "--bootstrap-server", bootstrapServers(), "--list")
    assertThrows(classOf[OptionException], () => getConsumerGroupService(cgcArgs))
  }

  @Test
  def testListConsumerGroupsWithStates(): Unit = {
    val simpleGroup = "simple-group"
    addSimpleGroupExecutor(group = simpleGroup)
    addConsumerGroupExecutor(numConsumers = 1)

    val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--list", "--state")
    val service = getConsumerGroupService(cgcArgs)

    val expectedListing = Set(
      new ConsumerGroupListing(simpleGroup, true, Optional.of(ConsumerGroupState.EMPTY)),
      new ConsumerGroupListing(group, false, Optional.of(ConsumerGroupState.STABLE)))

    var foundListing = Set.empty[ConsumerGroupListing]
    TestUtils.waitUntilTrue(() => {
      foundListing = service.listConsumerGroupsWithState(ConsumerGroupState.values.toSet).toSet
      expectedListing == foundListing
    }, s"Expected to show groups $expectedListing, but found $foundListing")

    val expectedListingStable = Set(
      new ConsumerGroupListing(group, false, Optional.of(ConsumerGroupState.STABLE)))

    foundListing = Set.empty[ConsumerGroupListing]
    TestUtils.waitUntilTrue(() => {
      foundListing = service.listConsumerGroupsWithState(Set(ConsumerGroupState.STABLE)).toSet
      expectedListingStable == foundListing
    }, s"Expected to show groups $expectedListingStable, but found $foundListing")
  }

  @Test
  def testConsumerGroupStatesFromString(): Unit = {
    var result = ConsumerGroupCommand.consumerGroupStatesFromString("Stable")
    assertEquals(Set(ConsumerGroupState.STABLE), result)

    result = ConsumerGroupCommand.consumerGroupStatesFromString("Stable, PreparingRebalance")
    assertEquals(Set(ConsumerGroupState.STABLE, ConsumerGroupState.PREPARING_REBALANCE), result)

    result = ConsumerGroupCommand.consumerGroupStatesFromString("Dead,CompletingRebalance,")
    assertEquals(Set(ConsumerGroupState.DEAD, ConsumerGroupState.COMPLETING_REBALANCE), result)

    assertThrows(classOf[IllegalArgumentException], () => ConsumerGroupCommand.consumerGroupStatesFromString("bad, wrong"))

    assertThrows(classOf[IllegalArgumentException], () => ConsumerGroupCommand.consumerGroupStatesFromString("stable"))

    assertThrows(classOf[IllegalArgumentException], () => ConsumerGroupCommand.consumerGroupStatesFromString("  bad, Stable"))

    assertThrows(classOf[IllegalArgumentException], () => ConsumerGroupCommand.consumerGroupStatesFromString("   ,   ,"))
  }

  @Test
  def testListGroupCommand(): Unit = {
    val simpleGroup = "simple-group"
    addSimpleGroupExecutor(group = simpleGroup)
    addConsumerGroupExecutor(numConsumers = 1)
    var out = ""

    var cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--list")
    TestUtils.waitUntilTrue(() => {
      out = TestUtils.grabConsoleOutput(ConsumerGroupCommand.main(cgcArgs))
      !out.contains("STATE") && out.contains(simpleGroup) && out.contains(group)
    }, s"Expected to find $simpleGroup, $group and no header, but found $out")

    cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--list", "--state")
    TestUtils.waitUntilTrue(() => {
      out = TestUtils.grabConsoleOutput(ConsumerGroupCommand.main(cgcArgs))
      out.contains("STATE") && out.contains(simpleGroup) && out.contains(group)
    }, s"Expected to find $simpleGroup, $group and the header, but found $out")

    cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--list", "--state", "Stable")
    TestUtils.waitUntilTrue(() => {
      out = TestUtils.grabConsoleOutput(ConsumerGroupCommand.main(cgcArgs))
      out.contains("STATE") && out.contains(group) && out.contains("Stable")
    }, s"Expected to find $group in state Stable and the header, but found $out")
  }

}
