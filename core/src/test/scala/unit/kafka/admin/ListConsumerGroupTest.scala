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
import kafka.utils.{TestInfoUtils, TestUtils}
import org.apache.kafka.clients.admin.ConsumerGroupListing
import org.apache.kafka.common.{ConsumerGroupState, ConsumerGroupType}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

import java.util.Optional

class ListConsumerGroupTest extends ConsumerGroupCommandTest {

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testListConsumerGroupsWithoutFilters(quorum: String, groupProtocol: String): Unit = {
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

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testListWithUnrecognizedNewConsumerOption(quorum: String, groupProtocol: String): Unit = {
    val cgcArgs = Array("--new-consumer", "--bootstrap-server", bootstrapServers(), "--list")
    assertThrows(classOf[OptionException], () => getConsumerGroupService(cgcArgs))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testListConsumerGroupsWithStates(quorum: String, groupProtocol: String): Unit = {
    val simpleGroup = "simple-group"
    addSimpleGroupExecutor(group = simpleGroup)
    addConsumerGroupExecutor(numConsumers = 1)

    val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--list", "--state")
    val service = getConsumerGroupService(cgcArgs)

    val expectedListing = Set(
      new ConsumerGroupListing(simpleGroup, true)
        .setState(Optional.of(ConsumerGroupState.EMPTY))
        .setType(if (quorum.contains("kip848")) Optional.of(ConsumerGroupType.CLASSIC) else Optional.empty()),
      new ConsumerGroupListing(group, false)
        .setState(Optional.of(ConsumerGroupState.STABLE))
        .setType(if (quorum.contains("kip848")) Optional.of(ConsumerGroupType.CLASSIC) else Optional.empty())
    )

    var foundListing = Set.empty[ConsumerGroupListing]
    TestUtils.waitUntilTrue(() => {
      foundListing = service.listConsumerGroupsWithFilters(ConsumerGroupState.values.toSet, Set.empty).toSet
      expectedListing == foundListing
    }, s"Expected to show groups $expectedListing, but found $foundListing")

    val expectedListingStable = Set.empty[ConsumerGroupListing]

    foundListing = Set.empty[ConsumerGroupListing]
    TestUtils.waitUntilTrue(() => {
      foundListing = service.listConsumerGroupsWithFilters(Set(ConsumerGroupState.PREPARING_REBALANCE), Set.empty).toSet
      expectedListingStable == foundListing
    }, s"Expected to show groups $expectedListingStable, but found $foundListing")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testListConsumerGroupsWithTypes(quorum: String, groupProtocol: String): Unit = {
    val simpleGroup = "simple-group"
    addSimpleGroupExecutor(group = simpleGroup)
    addConsumerGroupExecutor(numConsumers = 1)

    val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--list", "--type")
    val service = getConsumerGroupService(cgcArgs)

    val expectedListingStable = Set.empty[ConsumerGroupListing]

    val expectedListing = Set(
      new ConsumerGroupListing(simpleGroup, true)
        .setState(Optional.of(ConsumerGroupState.EMPTY))
        .setType(if(quorum.contains("kip848")) Optional.of(ConsumerGroupType.CLASSIC) else Optional.empty()),
      new ConsumerGroupListing(group, false)
        .setState(Optional.of(ConsumerGroupState.STABLE))
        .setType(if(quorum.contains("kip848")) Optional.of(ConsumerGroupType.CLASSIC) else Optional.empty())
    )

    var foundListing = Set.empty[ConsumerGroupListing]
    TestUtils.waitUntilTrue(() => {
      foundListing = service.listConsumerGroupsWithFilters(Set.empty, Set.empty).toSet
      expectedListing == foundListing
    }, s"Expected to show groups $expectedListing, but found $foundListing")

    // When group type is mentioned:
    // Old Group Coordinator returns empty listings.
    // New Group Coordinator returns groups according to the filter.
    val expectedListing2 = Set(
      new ConsumerGroupListing(simpleGroup, true)
        .setState(Optional.of(ConsumerGroupState.EMPTY))
        .setType(Optional.of(ConsumerGroupType.CLASSIC)),
      new ConsumerGroupListing(group, false)
        .setState(Optional.of(ConsumerGroupState.STABLE))
        .setType(Optional.of(ConsumerGroupType.CLASSIC))
    )

    foundListing = Set.empty[ConsumerGroupListing]
    TestUtils.waitUntilTrue(() => {
      foundListing = service.listConsumerGroupsWithFilters(Set.empty, Set(ConsumerGroupType.CLASSIC)).toSet
      if (quorum.contains("kip848")) {
        expectedListing2 == foundListing
      } else {
        expectedListingStable == foundListing
      }
    }, s"Expected to show groups $expectedListing2, but found $foundListing")

    // Groups with Consumer type aren't available so empty group listing is returned.
    foundListing = Set.empty[ConsumerGroupListing]
    TestUtils.waitUntilTrue(() => {
      foundListing = service.listConsumerGroupsWithFilters(Set.empty, Set(ConsumerGroupType.CONSUMER)).toSet
      expectedListingStable == foundListing
    }, s"Expected to show groups $expectedListingStable, but found $foundListing")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testConsumerGroupStatesFromString(quorum: String, groupProtocol: String): Unit = {
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

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testConsumerGroupTypesFromString(quorum: String, groupProtocol: String): Unit = {
    var result = ConsumerGroupCommand.consumerGroupTypesFromString("consumer")
    assertEquals(Set(ConsumerGroupType.CONSUMER), result)

    result = ConsumerGroupCommand.consumerGroupTypesFromString("consumer, classic")
    assertEquals(Set(ConsumerGroupType.CONSUMER, ConsumerGroupType.CLASSIC), result)

    assertThrows(classOf[IllegalArgumentException], () => ConsumerGroupCommand.consumerGroupTypesFromString("bad, wrong"))

    assertThrows(classOf[IllegalArgumentException], () => ConsumerGroupCommand.consumerGroupTypesFromString("Consumer"))

    assertThrows(classOf[IllegalArgumentException], () => ConsumerGroupCommand.consumerGroupTypesFromString("  bad, generic"))

    assertThrows(classOf[IllegalArgumentException], () => ConsumerGroupCommand.consumerGroupTypesFromString("   ,   ,"))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testListGroupCommand(quorum: String, groupProtocol: String): Unit = {
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
      out.contains("STATE") && !out.contains("TYPE") && out.contains(simpleGroup) && out.contains(group)
    }, s"Expected to find $simpleGroup, $group and the header, but found $out")

    cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--list", "--type")
    TestUtils.waitUntilTrue(() => {
      out = TestUtils.grabConsoleOutput(ConsumerGroupCommand.main(cgcArgs))
      out.contains("TYPE") && !out.contains("STATE") && out.contains(simpleGroup) && out.contains(group)
    }, s"Expected to find $simpleGroup, $group and the header, but found $out")

    cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--list", "--state", "--type")
    TestUtils.waitUntilTrue(() => {
      out = TestUtils.grabConsoleOutput(ConsumerGroupCommand.main(cgcArgs))
      out.contains("TYPE") && out.contains("STATE") && out.contains(simpleGroup) && out.contains(group)
    }, s"Expected to find $simpleGroup, $group and the header, but found $out")

    cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--list", "--state", "Stable")
    TestUtils.waitUntilTrue(() => {
      out = TestUtils.grabConsoleOutput(ConsumerGroupCommand.main(cgcArgs))
      out.contains("STATE") && out.contains(group) && out.contains("Stable")
    }, s"Expected to find $group in state Stable and the header, but found $out")

    cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--list", "--type", "classic")
    TestUtils.waitUntilTrue(() => {
      out = TestUtils.grabConsoleOutput(ConsumerGroupCommand.main(cgcArgs))
      if (quorum.contains("kip848")) {
        out.contains("TYPE") && out.contains(group) && out.contains("classic")
      } else {
        out.contains("TYPE")
      }
    }, s"Expected to find $group in state Stable and the header, but found $out")
  }
}
