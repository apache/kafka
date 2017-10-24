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
package kafka.controller

import kafka.zk.ZooKeeperTestHarness
import org.junit.{Before, Test}
import org.junit.Assert.{assertEquals, assertTrue}
import org.apache.kafka.common.TopicPartition

class KafkaControllerZkUtilsTest extends ZooKeeperTestHarness {

  private var zookeeperClient: ZookeeperClient = null
  private var newZkUtils: KafkaControllerZkUtils = null

  private val group = "my-group"
  private val topicPartition = new TopicPartition("topic", 0)
  private val offset = 123L

  @Before
  override def setUp() {
    super.setUp()
    zookeeperClient = new ZookeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, null)
    newZkUtils = new KafkaControllerZkUtils(zookeeperClient, false)
  }

  @Test
  def testSetAndGetConsumerOffset() {
    // None if no committed offsets
    assertTrue(newZkUtils.getConsumerOffset(group, topicPartition).isEmpty)
    // Set and retrieve an offset
    newZkUtils.setOrCreateConsumerOffset(group, topicPartition, offset)
    assertEquals(offset, newZkUtils.getConsumerOffset(group, topicPartition).get)
    // Update an existing offset and retrieve it
    newZkUtils.setOrCreateConsumerOffset(group, topicPartition, offset + 2L)
    assertEquals(offset + 2L, newZkUtils.getConsumerOffset(group, topicPartition).get)
  }

}