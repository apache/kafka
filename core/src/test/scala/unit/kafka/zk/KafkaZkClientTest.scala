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
package kafka.zk

import kafka.server.Defaults
import kafka.zookeeper.ZooKeeperClient
import org.apache.kafka.common.TopicPartition
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.{After, Before, Test}

class KafkaZkClientTest extends ZooKeeperTestHarness {

  private var zooKeeperClient: ZooKeeperClient = null
  private var zkClient: KafkaZkClient = null

  private val group = "my-group"
  private val topicPartition = new TopicPartition("topic", 0)

  @Before
  override def setUp() {
    super.setUp()
    zooKeeperClient = new ZooKeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, Defaults.ZkMaxInFlightRequests, null)
    zkClient = new KafkaZkClient(zooKeeperClient, false)
  }

  @After
  override def tearDown() {
    zkClient.close()
    super.tearDown()
  }

  @Test
  def testSetAndGetConsumerOffset() {
    val offset = 123L
    // None if no committed offsets
    assertTrue(zkClient.getConsumerOffset(group, topicPartition).isEmpty)
    // Set and retrieve an offset
    zkClient.setOrCreateConsumerOffset(group, topicPartition, offset)
    assertEquals(offset, zkClient.getConsumerOffset(group, topicPartition).get)
    // Update an existing offset and retrieve it
    zkClient.setOrCreateConsumerOffset(group, topicPartition, offset + 2L)
    assertEquals(offset + 2L, zkClient.getConsumerOffset(group, topicPartition).get)
  }

  @Test
  def testGetConsumerOffsetNoData() {
    zkClient.createRecursive(ConsumerOffset.path(group, topicPartition.topic, topicPartition.partition))
    assertTrue(zkClient.getConsumerOffset(group, topicPartition).isEmpty)
  }

  @Test
  def testDeleteRecursive() {
    zkClient.deleteRecursive("/delete/does-not-exist")

    zkClient.createRecursive("/delete/some/random/path")
    assertTrue(zkClient.pathExists("/delete/some/random/path"))
    zkClient.deleteRecursive("/delete")
    assertFalse(zkClient.pathExists("/delete/some/random/path"))
    assertFalse(zkClient.pathExists("/delete/some/random"))
    assertFalse(zkClient.pathExists("/delete/some"))
    assertFalse(zkClient.pathExists("/delete"))

    intercept[IllegalArgumentException](zkClient.deleteRecursive("delete-invalid-path"))
  }

  @Test
  def testCreateRecursive() {
    zkClient.createRecursive("/create-newrootpath")
    assertTrue(zkClient.pathExists("/create-newrootpath"))

    zkClient.createRecursive("/create/some/random/long/path")
    assertTrue(zkClient.pathExists("/create/some/random/long/path"))
    zkClient.createRecursive("/create/some/random/long/path") // no errors if path already exists

    intercept[IllegalArgumentException](zkClient.createRecursive("create-invalid-path"))
  }

  @Test
  def testGetTopicPartitionCount() {
    val topic = "mytest"

    // test with non-existing topic
    assertTrue(zkClient.getTopicPartitionCount(topic).isEmpty)

    // create a topic path
    zkClient.createRecursive(TopicZNode.path(topic))

    val assignment = Map(
      new TopicPartition(topic, 0) -> Seq(0, 1),
      new TopicPartition(topic, 1) -> Seq(0, 1)
    )
    zkClient.setTopicAssignmentRaw(topic, assignment)

    assertEquals(2, zkClient.getTopicPartitionCount(topic).get)
  }


  @Test
  def testGetDataAndVersion() {
    val path = "/testpath"

    // test with non-existing path
    var dataAndVersion = zkClient.getDataAndVersion(path)
    assertTrue(dataAndVersion._1.isEmpty)
    assertEquals(-1, dataAndVersion._2)

    // create a test path
    zkClient.createRecursive(path)
    zkClient.conditionalUpdatePath(path, "version1", 0)

    // test with existing path
    dataAndVersion = zkClient.getDataAndVersion(path)
    assertEquals("version1", dataAndVersion._1.get)
    assertEquals(1, dataAndVersion._2)

    zkClient.conditionalUpdatePath(path, "version2", 1)
    dataAndVersion = zkClient.getDataAndVersion(path)
    assertEquals("version2", dataAndVersion._1.get)
    assertEquals(2, dataAndVersion._2)
  }

  @Test
  def testConditionalUpdatePath() {
    val path = "/testconditionalpath"

    // test with non-existing path
    var statusAndVersion = zkClient.conditionalUpdatePath(path, "version0", 0)
    assertFalse(statusAndVersion._1)
    assertEquals(-1, statusAndVersion._2)

    // create path
    zkClient.createRecursive(path)

    // test with valid expected version
    statusAndVersion = zkClient.conditionalUpdatePath(path, "version1", 0)
    assertTrue(statusAndVersion._1)
    assertEquals(1, statusAndVersion._2)

    // test with invalid expected version
    statusAndVersion = zkClient.conditionalUpdatePath(path, "version2", 2)
    assertFalse(statusAndVersion._1)
    assertEquals(-1, statusAndVersion._2)
  }

  @Test
  def testSetGetAndDeletePartitionReassignment() {
    zkClient.createRecursive(AdminZNode.path)

    assertEquals(Map.empty, zkClient.getPartitionReassignment)

    val reassignment = Map(
      new TopicPartition("topic_a", 0) -> Seq(0, 1, 3),
      new TopicPartition("topic_a", 1) -> Seq(2, 1, 3),
      new TopicPartition("topic_b", 0) -> Seq(4, 5),
      new TopicPartition("topic_c", 0) -> Seq(5, 3)
    )
    zkClient.setOrCreatePartitionReassignment(reassignment)
    assertEquals(reassignment, zkClient.getPartitionReassignment)

    val updatedReassingment = reassignment - new TopicPartition("topic_b", 0)
    zkClient.setOrCreatePartitionReassignment(updatedReassingment)
    assertEquals(updatedReassingment, zkClient.getPartitionReassignment)

    zkClient.deletePartitionReassignment()
    assertEquals(Map.empty, zkClient.getPartitionReassignment)
  }

}
