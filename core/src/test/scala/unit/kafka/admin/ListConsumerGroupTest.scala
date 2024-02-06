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
import org.apache.kafka.common.{ConsumerGroupState, GroupType}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, MethodSource}

import java.util.Optional

class ListConsumerGroupTest extends ConsumerGroupCommandTest {

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testListConsumerGroupsWithoutFilters(quorum: String, groupProtocol: String): Unit = {
    val simpleGroup = "simple-group"
    val protocolGroup = "protocol-group"

    addSimpleGroupExecutor(group = simpleGroup)
    addConsumerGroupExecutor(numConsumers = 1)
    addConsumerGroupExecutor(numConsumers = 1, group = protocolGroup, groupProtocol = groupProtocol)

    val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--list")
    val service = getConsumerGroupService(cgcArgs)

    val expectedGroups = Set(protocolGroup, group, simpleGroup)
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
        .setType(Optional.of(GroupType.CLASSIC)),
      new ConsumerGroupListing(group, false)
        .setState(Optional.of(ConsumerGroupState.STABLE))
        .setType(Optional.of(GroupType.CLASSIC))
    )

    var foundListing = Set.empty[ConsumerGroupListing]
    TestUtils.waitUntilTrue(() => {
      foundListing = service.listConsumerGroupsWithFilters(Set.empty, ConsumerGroupState.values.toSet).toSet
      expectedListing == foundListing
    }, s"Expected to show groups $expectedListing, but found $foundListing")

    val expectedListingStable = Set.empty[ConsumerGroupListing]

    foundListing = Set.empty[ConsumerGroupListing]
    TestUtils.waitUntilTrue(() => {
      foundListing = service.listConsumerGroupsWithFilters(Set.empty, Set(ConsumerGroupState.PREPARING_REBALANCE)).toSet
      expectedListingStable == foundListing
    }, s"Expected to show groups $expectedListingStable, but found $foundListing")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersClassicGroupProtocolOnly"))
  def testListConsumerGroupsWithTypesClassicProtocol(quorum: String, groupProtocol: String): Unit = {
    val simpleGroup = "simple-group"
    val protocolGroup = "protocol-group"

    addSimpleGroupExecutor(group = simpleGroup)
    addConsumerGroupExecutor(numConsumers = 1)
    addConsumerGroupExecutor(numConsumers = 1, group = protocolGroup, groupProtocol = groupProtocol)

    val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--list", "--type")
    val service = getConsumerGroupService(cgcArgs)

    val expectedListingStable = Set.empty[ConsumerGroupListing]

    val expectedListing = Set(
      new ConsumerGroupListing(simpleGroup, true)
        .setState(Optional.of(ConsumerGroupState.EMPTY))
        .setType(Optional.of(GroupType.CLASSIC)),
      new ConsumerGroupListing(group, false)
        .setState(Optional.of(ConsumerGroupState.STABLE))
        .setType(Optional.of(GroupType.CLASSIC)),
      new ConsumerGroupListing(protocolGroup, false)
        .setState(Optional.of(ConsumerGroupState.STABLE))
        .setType(Optional.of(GroupType.CLASSIC))
    )

    // No filters explicitly mentioned. Expectation is that all groups are returned.
    var foundListing = Set.empty[ConsumerGroupListing]
    TestUtils.waitUntilTrue(() => {
      foundListing = service.listConsumerGroupsWithFilters(Set.empty, Set.empty).toSet
      expectedListing == foundListing
    }, s"Expected to show groups $expectedListing, but found $foundListing")

    // When group type is mentioned:
    // Old Group Coordinator returns empty listings if the type is not Classic.
    // New Group Coordinator returns groups according to the filter.
    foundListing = Set.empty[ConsumerGroupListing]
    TestUtils.waitUntilTrue(() => {
      foundListing = service.listConsumerGroupsWithFilters(Set(GroupType.CONSUMER), Set.empty).toSet
      expectedListingStable == foundListing
    }, s"Expected to show groups $expectedListing, but found $foundListing")

    foundListing = Set.empty[ConsumerGroupListing]
    TestUtils.waitUntilTrue(() => {
      foundListing = service.listConsumerGroupsWithFilters(Set(GroupType.CLASSIC), Set.empty).toSet
      expectedListing == foundListing
    }, s"Expected to show groups $expectedListing, but found $foundListing")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @CsvSource(Array("kraft+kip848,consumer"))
  def testListConsumerGroupsWithTypesConsumerProtocol(quorum: String, groupProtocol: String): Unit = {
    val simpleGroup = "simple-group"
    val protocolGroup = "protocol-group"

    addSimpleGroupExecutor(group = simpleGroup)
    addConsumerGroupExecutor(numConsumers = 1)
    addConsumerGroupExecutor(numConsumers = 1, group = protocolGroup, groupProtocol = groupProtocol)

    val cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--list", "--type")
    val service = getConsumerGroupService(cgcArgs)

    // No filters explicitly mentioned. Expectation is that all groups are returned.
    var expectedListing = Set(
      new ConsumerGroupListing(simpleGroup, true)
        .setState(Optional.of(ConsumerGroupState.EMPTY))
        .setType(Optional.of(GroupType.CLASSIC)),
      new ConsumerGroupListing(group, false)
        .setState(Optional.of(ConsumerGroupState.STABLE))
        .setType(Optional.of(GroupType.CLASSIC)),
      new ConsumerGroupListing(protocolGroup, false)
        .setState(Optional.of(ConsumerGroupState.STABLE))
        .setType(Optional.of(GroupType.CONSUMER))
    )

    var foundListing = Set.empty[ConsumerGroupListing]
    TestUtils.waitUntilTrue(() => {
      foundListing = service.listConsumerGroupsWithFilters(Set.empty, Set.empty).toSet
      expectedListing.equals(foundListing)
    }, s"Expected to show groups $expectedListing, but found $foundListing")
    System.out.println("found listing" + foundListing);

    // When group type is mentioned:
    // New Group Coordinator returns groups according to the filter.
    expectedListing = Set(
      new ConsumerGroupListing(protocolGroup, false)
        .setState(Optional.of(ConsumerGroupState.STABLE))
        .setType(Optional.of(GroupType.CONSUMER))
    )

    foundListing = Set.empty[ConsumerGroupListing]
    TestUtils.waitUntilTrue(() => {
      foundListing = service.listConsumerGroupsWithFilters(Set(GroupType.CONSUMER), Set.empty).toSet
      expectedListing.equals(foundListing)
    }, s"Expected to show groups $expectedListing, but found $foundListing")
    System.out.println("found listing" + foundListing);
    expectedListing = Set(
      new ConsumerGroupListing(simpleGroup, true)
        .setState(Optional.of(ConsumerGroupState.EMPTY))
        .setType(Optional.of(GroupType.CLASSIC)),
      new ConsumerGroupListing(group, false)
        .setState(Optional.of(ConsumerGroupState.STABLE))
        .setType(Optional.of(GroupType.CLASSIC)),
    )

    foundListing = Set.empty[ConsumerGroupListing]
    TestUtils.waitUntilTrue(() => {
      foundListing = service.listConsumerGroupsWithFilters(Set(GroupType.CLASSIC), Set.empty).toSet
      expectedListing.equals(foundListing)
    }, s"Expected to show groups $expectedListing, but found $foundListing")
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

    result = ConsumerGroupCommand.consumerGroupStatesFromString("stable")
    assertEquals(Set(ConsumerGroupState.STABLE), result)

    result = ConsumerGroupCommand.consumerGroupStatesFromString("stable, assigning")
    assertEquals(Set(ConsumerGroupState.STABLE, ConsumerGroupState.ASSIGNING), result)

    result = ConsumerGroupCommand.consumerGroupStatesFromString("dead,reconciling,")
    assertEquals(Set(ConsumerGroupState.DEAD, ConsumerGroupState.RECONCILING), result)

    assertThrows(classOf[IllegalArgumentException], () => ConsumerGroupCommand.consumerGroupStatesFromString("bad, wrong"))

    assertThrows(classOf[IllegalArgumentException], () => ConsumerGroupCommand.consumerGroupStatesFromString("  bad, Stable"))

    assertThrows(classOf[IllegalArgumentException], () => ConsumerGroupCommand.consumerGroupStatesFromString("   ,   ,"))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testConsumerGroupTypesFromString(quorum: String, groupProtocol: String): Unit = {
    var result = ConsumerGroupCommand.consumerGroupTypesFromString("consumer")
    assertEquals(Set(GroupType.CONSUMER), result)

    result = ConsumerGroupCommand.consumerGroupTypesFromString("consumer, classic")
    assertEquals(Set(GroupType.CONSUMER, GroupType.CLASSIC), result)

    result = ConsumerGroupCommand.consumerGroupTypesFromString("Consumer, Classic")
    assertEquals(Set(GroupType.CONSUMER, GroupType.CLASSIC), result)

    assertThrows(classOf[IllegalArgumentException], () => ConsumerGroupCommand.consumerGroupTypesFromString("bad, wrong"))

    assertThrows(classOf[IllegalArgumentException], () => ConsumerGroupCommand.consumerGroupTypesFromString("  bad, generic"))

    assertThrows(classOf[IllegalArgumentException], () => ConsumerGroupCommand.consumerGroupTypesFromString("   ,   ,"))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersClassicGroupProtocolOnly"))
  def testListGroupCommand(quorum: String, groupProtocol: String): Unit = {
    val simpleGroup = "simple-group"
    val protocolGroup = "protocol-group"

    addSimpleGroupExecutor(group = simpleGroup)
    addConsumerGroupExecutor(numConsumers = 1)
    addConsumerGroupExecutor(numConsumers = 1, group = protocolGroup, groupProtocol = groupProtocol)
    var out = ""

    var cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--list")
    TestUtils.waitUntilTrue(() => {
      out = TestUtils.grabConsoleOutput(ConsumerGroupCommand.main(cgcArgs))
      !out.contains("STATE") && out.contains(simpleGroup) && out.contains(group) && out.contains(protocolGroup)
    }, s"Expected to find $simpleGroup, $group, $protocolGroup and no header, but found $out")

    cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--list", "--state")
    TestUtils.waitUntilTrue(() => {
      out = TestUtils.grabConsoleOutput(ConsumerGroupCommand.main(cgcArgs))
      out.contains("STATE") && !out.contains("TYPE") && out.contains(simpleGroup) && out.contains(group) && out.contains(protocolGroup)
    }, s"Expected to find $simpleGroup, $group, $protocolGroup and the header, but found $out")

    cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--list", "--type")
    TestUtils.waitUntilTrue(() => {
      out = TestUtils.grabConsoleOutput(ConsumerGroupCommand.main(cgcArgs))
      out.contains("TYPE") && !out.contains("STATE") && out.contains(simpleGroup) && out.contains(group) && out.contains(protocolGroup)
    }, s"Expected to find $simpleGroup, $group, $protocolGroup and the header, but found $out")

    cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--list", "--state", "--type")
    TestUtils.waitUntilTrue(() => {
      out = TestUtils.grabConsoleOutput(ConsumerGroupCommand.main(cgcArgs))
      out.contains("TYPE") && out.contains("STATE") && out.contains(simpleGroup) && out.contains(group) && out.contains(protocolGroup)
    }, s"Expected to find $simpleGroup, $group, $protocolGroup and the header, but found $out")

    cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--list", "--state", "Stable")
    TestUtils.waitUntilTrue(() => {
      out = TestUtils.grabConsoleOutput(ConsumerGroupCommand.main(cgcArgs))
      out.contains("STATE") && out.contains(group) && out.contains("Stable") && out.contains(protocolGroup)
    }, s"Expected to find $group, $protocolGroup in state Stable and the header, but found $out")

    cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--list", "--state", "stable")
    TestUtils.waitUntilTrue(() => {
      out = TestUtils.grabConsoleOutput(ConsumerGroupCommand.main(cgcArgs))
      out.contains("STATE") && out.contains(group) && out.contains("Stable") && out.contains(protocolGroup)
    }, s"Expected to find $group, $protocolGroup in state Stable and the header, but found $out")

    cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--list", "--type", "Classic")
    TestUtils.waitUntilTrue(() => {
      out = TestUtils.grabConsoleOutput(ConsumerGroupCommand.main(cgcArgs))
      out.contains("TYPE") && out.contains("Classic") && !out.contains("STATE") && out.contains(simpleGroup) && out.contains(group) && out.contains(protocolGroup)
    }, s"Expected to find $simpleGroup, $group, $protocolGroup and the header, but found $out")

    cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--list", "--type", "classic")
    TestUtils.waitUntilTrue(() => {
      out = TestUtils.grabConsoleOutput(ConsumerGroupCommand.main(cgcArgs))
      out.contains("TYPE") && out.contains("Classic") && !out.contains("STATE") && out.contains(simpleGroup) && out.contains(group) && out.contains(protocolGroup)
    }, s"Expected to find $simpleGroup, $group, $protocolGroup  and the header, but found $out")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @CsvSource(Array("kraft+kip848,consumer"))
  def testListGroupCommandConsumerType(quorum: String, groupProtocol: String): Unit = {
    val simpleGroup = "simple-group"
    val protocolGroup = "protocol-group"

    addSimpleGroupExecutor(group = simpleGroup)
    addConsumerGroupExecutor(numConsumers = 1)
    addConsumerGroupExecutor(numConsumers = 1, group = protocolGroup, groupProtocol = groupProtocol)
    var out = ""

    var cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--list")
    TestUtils.waitUntilTrue(() => {
      out = TestUtils.grabConsoleOutput(ConsumerGroupCommand.main(cgcArgs))
      !out.contains("STATE") && out.contains(simpleGroup) && out.contains(group) && out.contains(protocolGroup)
    }, s"Expected to find $simpleGroup, $group, $protocolGroup and no header, but found $out")
    System.out.println(out);

    cgcArgs = Array("--bootstrap-server", bootstrapServers(), "--list", "--type", "consumer")
    TestUtils.waitUntilTrue(() => {
      out = TestUtils.grabConsoleOutput(ConsumerGroupCommand.main(cgcArgs))
      out.contains("TYPE") && out.contains("Consumer") && !out.contains("STATE") && out.contains(protocolGroup)
    }, s"Expected to find $protocolGroup and the header, but found $out")
    System.out.println(out);
  }
}
