/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.utils

import kafka.api.LeaderAndIsr
import kafka.common.TopicAndPartition
import kafka.controller.LeaderIsrAndControllerEpoch
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.security.JaasUtils
import org.junit.Assert._
import org.junit.{After, Before, Test}

class ZkUtilsTest extends ZooKeeperTestHarness {

  val path = "/path"
  var zkUtils: ZkUtils = _

  @Before
  override def setUp() {
    super.setUp
    zkUtils = ZkUtils(zkConnect, zkSessionTimeout, zkConnectionTimeout, zkAclsEnabled.getOrElse(JaasUtils.isZkSecurityEnabled))
  }

  @After
  override def tearDown() {
    if (zkUtils != null)
     CoreUtils.swallow(zkUtils.close(), this)
    super.tearDown
  }

  val topic = "foo"
  val partition1 = 0
  val replica1 = 1
  val replica2 = 2
  val reassignmentJson =
    s"""
      |{
      |  "version":1,
      |  "partitions": [
      |    { "topic": "$topic", "partition":$partition1, "replicas":[$replica1, $replica2]},
      |    { "topic": "$topic", "partition":$partition1, "replicas":[$replica1, $replica2]}
      |  ]
      |}
    """.stripMargin

  @Test
  def testSuccessfulConditionalDeletePath() {
    // Given an existing path
    zkUtils.createPersistentPath(path)
    val (_, statAfterCreation) = zkUtils.readData(path)

    // Deletion is successful when the version number matches
    assertTrue("Deletion should be successful", zkUtils.conditionalDeletePath(path, statAfterCreation.getVersion))
    val (optionalData, _) = zkUtils.readDataMaybeNull(path)
    assertTrue("Node should be deleted", optionalData.isEmpty)

    // Deletion is successful when the node does not exist too
    assertTrue("Deletion should be successful", zkUtils.conditionalDeletePath(path, 0))
  }

  // Verify behaviour of ZkUtils.createSequentialPersistentPath since PIDManager relies on it
  @Test
  def testPersistentSequentialPath() {
    // Given an existing path
    zkUtils.createPersistentPath(path)

    var result = zkUtils.createSequentialPersistentPath(path + "/sequence_")

    assertEquals("/path/sequence_0000000000", result)

    result = zkUtils.createSequentialPersistentPath(path + "/sequence_")

    assertEquals("/path/sequence_0000000001", result)
  }

  @Test
  def testAbortedConditionalDeletePath() {
    // Given an existing path that gets updated
    zkUtils.createPersistentPath(path)
    val (_, statAfterCreation) = zkUtils.readData(path)
    zkUtils.updatePersistentPath(path, "data")

    // Deletion is aborted when the version number does not match
    assertFalse("Deletion should be aborted", zkUtils.conditionalDeletePath(path, statAfterCreation.getVersion))
    val (optionalData, _) = zkUtils.readDataMaybeNull(path)
    assertTrue("Node should still be there", optionalData.isDefined)
  }

  @Test
  def testClusterIdentifierJsonParsing() {
    val clusterId = "test"
    assertEquals(zkUtils.ClusterId.fromJson(zkUtils.ClusterId.toJson(clusterId)), clusterId)
  }

  @Test
  def testGetAllPartitionsTopicWithoutPartitions() {
    val topic = "testtopic"
    // Create a regular topic and a topic without any partitions
    zkUtils.createPersistentPath(ZkUtils.getTopicPartitionPath(topic, 0))
    zkUtils.createPersistentPath(ZkUtils.getTopicPath("nopartitions"))

    assertEquals(Set(TopicAndPartition(topic, 0)), zkUtils.getAllPartitions())
  }

  @Test
  def testGetLeaderIsrAndEpochForPartition() {
    val topic = "my-topic-test"
    val partition = 0
    val leader = 1
    val leaderEpoch = 1
    val controllerEpoch = 1
    val isr = List(1, 2)
    val topicPath = s"/brokers/topics/$topic/partitions/$partition/state"
    val topicData = Json.legacyEncodeAsString(Map("controller_epoch" -> controllerEpoch, "leader" -> leader,
      "versions" -> 1, "leader_epoch" -> leaderEpoch, "isr" -> isr))
    zkUtils.createPersistentPath(topicPath, topicData)

    val leaderIsrAndControllerEpoch = zkUtils.getLeaderIsrAndEpochForPartition(topic, partition)
    val topicDataLeaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(LeaderAndIsr(leader, leaderEpoch, isr, 0),
      controllerEpoch)
    assertEquals(topicDataLeaderIsrAndControllerEpoch, leaderIsrAndControllerEpoch.get)
    assertEquals(None, zkUtils.getLeaderIsrAndEpochForPartition(topic, partition + 1))
  }

  @Test
  def testGetSequenceIdMethod() {
    val path = "/test/seqid"
    (1 to 10).foreach { seqid =>
      assertEquals(seqid, zkUtils.getSequenceId(path))
    }
  }

  @Test
  def testParsePartitionReassignmentDataWithoutDedup_invalidJson() = {
    val jsonStr = "{invalid json}"

    try {
      ZkUtils.parsePartitionReassignmentData(jsonStr)
      fail("Should have thrown ConfigException");
    } catch {
      case e: ConfigException =>
        assertTrue(e.getMessage.contains("Invalid reassignment config"))
    }
  }

  @Test
  def testParsePartitionReassignmentData() = {
    val result = ZkUtils.parsePartitionReassignmentData(reassignmentJson)

    // Duplicates are removed
    assertEquals(1, result.size)
    assertTrue(result.contains(TopicAndPartition(topic, partition1)))
    assertEquals(Seq(replica1, replica2), result(TopicAndPartition(topic, partition1)))
  }

}
