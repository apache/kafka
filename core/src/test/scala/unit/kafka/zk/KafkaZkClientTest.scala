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

import kafka.zookeeper.ZooKeeperClient

import org.junit.{After, Before, Test}
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.apache.kafka.common.TopicPartition

class KafkaZkClientTest extends ZooKeeperTestHarness {

  private var zooKeeperClient: ZooKeeperClient = null
  private var zkClient: KafkaZkClient = null

  private val group = "my-group"
  private val topicPartition = new TopicPartition("topic", 0)

  @Before
  override def setUp() {
    super.setUp()
    zooKeeperClient = new ZooKeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null)
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

}