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

package kafka.coordinator

import kafka.server.KafkaConfig
import kafka.utils.{TestUtils, ZkUtils}
import kafka.utils.ZkUtils._

import org.junit.Assert._
import org.I0Itec.zkclient.{IZkDataListener, ZkClient}
import org.apache.zookeeper.data.Stat
import org.easymock.EasyMock
import org.junit.{Before, Test}
import org.scalatest.junit.JUnitSuite

/**
 * Test coordinator group and topic metadata management
 */
class CoordinatorMetadataTest extends JUnitSuite {
  val DefaultNumPartitions = 8
  val DefaultNumReplicas = 2
  var zkUtils: ZkUtils = null
  var coordinatorMetadata: CoordinatorMetadata = null

  @Before
  def setUp() {
    val props = TestUtils.createBrokerConfig(nodeId = 0, zkConnect = "")
    val zkClient = EasyMock.createStrictMock(classOf[ZkClient])
    zkUtils = ZkUtils(zkClient, false)
    coordinatorMetadata = new CoordinatorMetadata(KafkaConfig.fromProps(props).brokerId, zkUtils, null)
  }

  @Test
  def testGetNonexistentGroup() {
    assertNull(coordinatorMetadata.getGroup("group"))
  }

  @Test
  def testGetGroup() {
    val groupId = "group"
    val expected = coordinatorMetadata.addGroup(groupId, "range")
    val actual = coordinatorMetadata.getGroup(groupId)
    assertEquals(expected, actual)
  }

  @Test
  def testAddGroupReturnsPreexistingGroupIfItAlreadyExists() {
    val groupId = "group"
    val group1 = coordinatorMetadata.addGroup(groupId, "range")
    val group2 = coordinatorMetadata.addGroup(groupId, "range")
    assertEquals(group1, group2)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testBindNonexistentGroupToTopics() {
    val groupId = "group"
    val topics = Set("a")
    coordinatorMetadata.bindGroupToTopics(groupId, topics)
  }

  @Test
  def testBindGroupToTopicsNotListenedOn() {
    val groupId = "group"
    val topics = Set("a")
    coordinatorMetadata.addGroup(groupId, "range")

    expectZkClientSubscribeDataChanges(zkUtils, topics)
    EasyMock.replay(zkUtils.zkClient)
    coordinatorMetadata.bindGroupToTopics(groupId, topics)
    assertEquals(Map("a" -> DefaultNumPartitions), coordinatorMetadata.partitionsPerTopic)
  }

  @Test
  def testBindGroupToTopicsAlreadyListenedOn() {
    val group1 = "group1"
    val group2 = "group2"
    val topics = Set("a")
    coordinatorMetadata.addGroup(group1, "range")
    coordinatorMetadata.addGroup(group2, "range")

    expectZkClientSubscribeDataChanges(zkUtils, topics)
    EasyMock.replay(zkUtils.zkClient)
    coordinatorMetadata.bindGroupToTopics(group1, topics)
    coordinatorMetadata.bindGroupToTopics(group2, topics)
    assertEquals(Map("a" -> DefaultNumPartitions), coordinatorMetadata.partitionsPerTopic)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testUnbindNonexistentGroupFromTopics() {
    val groupId = "group"
    val topics = Set("a")
    coordinatorMetadata.unbindGroupFromTopics(groupId, topics)
  }

  @Test
  def testUnbindGroupFromTopicsNotListenedOn() {
    val groupId = "group"
    val topics = Set("a")
    coordinatorMetadata.addGroup(groupId, "range")

    expectZkClientSubscribeDataChanges(zkUtils, topics)
    EasyMock.replay(zkUtils.zkClient)
    coordinatorMetadata.bindGroupToTopics(groupId, topics)
    coordinatorMetadata.unbindGroupFromTopics(groupId, Set("b"))
    assertEquals(Map("a" -> DefaultNumPartitions), coordinatorMetadata.partitionsPerTopic)
  }

  @Test
  def testUnbindGroupFromTopicsListenedOnByOtherGroups() {
    val group1 = "group1"
    val group2 = "group2"
    val topics = Set("a")
    coordinatorMetadata.addGroup(group1, "range")
    coordinatorMetadata.addGroup(group2, "range")

    expectZkClientSubscribeDataChanges(zkUtils, topics)
    EasyMock.replay(zkUtils.zkClient)
    coordinatorMetadata.bindGroupToTopics(group1, topics)
    coordinatorMetadata.bindGroupToTopics(group2, topics)
    coordinatorMetadata.unbindGroupFromTopics(group1, topics)
    assertEquals(Map("a" -> DefaultNumPartitions), coordinatorMetadata.partitionsPerTopic)
  }

  @Test
  def testUnbindGroupFromTopicsListenedOnByNoOtherGroup() {
    val groupId = "group"
    val topics = Set("a")
    coordinatorMetadata.addGroup(groupId, "range")

    expectZkClientSubscribeDataChanges(zkUtils, topics)
    expectZkClientUnsubscribeDataChanges(zkUtils.zkClient, topics)
    EasyMock.replay(zkUtils.zkClient)
    coordinatorMetadata.bindGroupToTopics(groupId, topics)
    coordinatorMetadata.unbindGroupFromTopics(groupId, topics)
    assertEquals(Map.empty[String, Int], coordinatorMetadata.partitionsPerTopic)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testRemoveNonexistentGroup() {
    val groupId = "group"
    val topics = Set("a")
    coordinatorMetadata.removeGroup(groupId, topics)
  }

  @Test
  def testRemoveGroupWithOtherGroupsBoundToItsTopics() {
    val group1 = "group1"
    val group2 = "group2"
    val topics = Set("a")
    coordinatorMetadata.addGroup(group1, "range")
    coordinatorMetadata.addGroup(group2, "range")

    expectZkClientSubscribeDataChanges(zkUtils, topics)
    EasyMock.replay(zkUtils.zkClient)
    coordinatorMetadata.bindGroupToTopics(group1, topics)
    coordinatorMetadata.bindGroupToTopics(group2, topics)
    coordinatorMetadata.removeGroup(group1, topics)
    assertNull(coordinatorMetadata.getGroup(group1))
    assertNotNull(coordinatorMetadata.getGroup(group2))
    assertEquals(Map("a" -> DefaultNumPartitions), coordinatorMetadata.partitionsPerTopic)
  }

  @Test
  def testRemoveGroupWithNoOtherGroupsBoundToItsTopics() {
    val groupId = "group"
    val topics = Set("a")
    coordinatorMetadata.addGroup(groupId, "range")

    expectZkClientSubscribeDataChanges(zkUtils, topics)
    expectZkClientUnsubscribeDataChanges(zkUtils.zkClient, topics)
    EasyMock.replay(zkUtils.zkClient)
    coordinatorMetadata.bindGroupToTopics(groupId, topics)
    coordinatorMetadata.removeGroup(groupId, topics)
    assertNull(coordinatorMetadata.getGroup(groupId))
    assertEquals(Map.empty[String, Int], coordinatorMetadata.partitionsPerTopic)
  }

  private def expectZkClientSubscribeDataChanges(zkUtils: ZkUtils, topics: Set[String]) {
    topics.foreach(topic => expectZkClientSubscribeDataChange(zkUtils.zkClient, topic))
  }

  private def expectZkClientUnsubscribeDataChanges(zkClient: ZkClient, topics: Set[String]) {
    topics.foreach(topic => expectZkClientUnsubscribeDataChange(zkClient, topic))
  }

  private def expectZkClientSubscribeDataChange(zkClient: ZkClient, topic: String) {
    val replicaAssignment =
      (0 until DefaultNumPartitions)
      .map(partition => partition.toString -> (0 until DefaultNumReplicas).toSeq).toMap
    val topicPath = getTopicPath(topic)
    EasyMock.expect(zkClient.readData(topicPath, new Stat()))
      .andReturn(zkUtils.replicaAssignmentZkData(replicaAssignment))
    zkClient.subscribeDataChanges(EasyMock.eq(topicPath), EasyMock.isA(classOf[IZkDataListener]))
  }

  private def expectZkClientUnsubscribeDataChange(zkClient: ZkClient, topic: String) {
    val topicPath = getTopicPath(topic)
    zkClient.unsubscribeDataChanges(EasyMock.eq(topicPath), EasyMock.isA(classOf[IZkDataListener]))
  }
}
