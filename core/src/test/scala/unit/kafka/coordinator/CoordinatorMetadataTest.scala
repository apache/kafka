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
import kafka.utils.TestUtils

import org.junit.Assert._
import org.junit.{Before, Test}
import org.scalatest.junit.JUnitSuite

/**
 * Test coordinator group and topic metadata management
 */
class CoordinatorMetadataTest extends JUnitSuite {
  val DefaultNumPartitions = 8
  val DefaultNumReplicas = 2
  var coordinatorMetadata: CoordinatorMetadata = null

  @Before
  def setUp() {
    val props = TestUtils.createBrokerConfig(nodeId = 0, zkConnect = "")
    coordinatorMetadata = new CoordinatorMetadata(KafkaConfig.fromProps(props).brokerId)
  }

  @Test
  def testGetNonexistentGroup() {
    assertNull(coordinatorMetadata.getGroup("group"))
  }

  @Test
  def testGetGroup() {
    val groupId = "group"
    val protocolType = "consumer"
    val expected = coordinatorMetadata.addGroup(groupId, protocolType)
    val actual = coordinatorMetadata.getGroup(groupId)
    assertEquals(expected, actual)
  }

  @Test
  def testAddGroupReturnsPreexistingGroupIfItAlreadyExists() {
    val groupId = "group"
    val protocolType = "consumer"
    val group1 = coordinatorMetadata.addGroup(groupId, protocolType)
    val group2 = coordinatorMetadata.addGroup(groupId, protocolType)
    assertEquals(group1, group2)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testRemoveNonexistentGroup() {
    val groupId = "group"
    val topics = Set("a")
    coordinatorMetadata.removeGroup(groupId)
  }

}
