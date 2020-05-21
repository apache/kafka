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
package unit.kafka.controller

import kafka.controller.{KafkaController, ReplicaAssignment}
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import kafka.zk.KafkaZkClient
import org.apache.kafka.server.policy.CreateTopicPolicy
import org.easymock.EasyMock
import org.junit.{Before, Test}
import org.junit.Assert.assertEquals
import org.scalatest.junit.JUnitSuite

class TopicsSatisfyMinRFTest extends JUnitSuite {
  private var createTopicPolicy : Option[CreateTopicPolicy] = null
  private var mockZkClient: KafkaZkClient = null
  @Before
  def setUp(): Unit = {
    val config = TestUtils.createBrokerConfigs(1, "")
      .map(prop => {
        prop.setProperty(KafkaConfig.DefaultReplicationFactorProp,"2")  // Set min RF to be 2
        prop.setProperty(KafkaConfig.CreateTopicPolicyClassNameProp, "kafka.server.LiCreateTopicPolicy")
        prop
      }).map(KafkaConfig.fromProps)
    createTopicPolicy = Option(config.head.getConfiguredInstance(KafkaConfig.CreateTopicPolicyClassNameProp,
      classOf[CreateTopicPolicy]))
    mockZkClient = EasyMock.createMock(classOf[KafkaZkClient])
  }

  // Mock new topic with sufficient RF, should satisfy
  @Test
  def testNewTopicSucceed: Unit = {
    val topicSucceed = "test_topic1"
    assertEquals(true, KafkaController.satisfiesLiCreateTopicPolicy(createTopicPolicy, mockZkClient,
      topicSucceed, Map(0 -> new ReplicaAssignment(Seq(0, 1), Seq.empty[Int], Seq.empty[Int]))))      // RF = 2
  }

  // Mock new topic with insufficient RF, should fail
  @Test
  def testNewTopicFail: Unit = {
    val topicFail = "test_topic1"
    EasyMock.expect(mockZkClient.getTopicPartitions(topicFail)).andReturn(Seq.empty)
    EasyMock.replay(mockZkClient)
    assertEquals(false, KafkaController.satisfiesLiCreateTopicPolicy(createTopicPolicy, mockZkClient,
      topicFail, Map(0 -> new ReplicaAssignment(Seq(1), Seq.empty[Int], Seq.empty[Int]))))            // RF = 1
    EasyMock.verify(mockZkClient)
  }

  // Mock existing topic with insufficient RF, because it's pre-existing, should still satisfy
  @Test
  def testExistingTopicSucceed: Unit = {
    val topicExisting = "test_topic1"
    EasyMock.expect(mockZkClient.getTopicPartitions(topicExisting)).andReturn(Seq("0"))  // Partition number = 1
    EasyMock.replay(mockZkClient)
    assertEquals(true, KafkaController.satisfiesLiCreateTopicPolicy(createTopicPolicy, mockZkClient,
      topicExisting, Map(0 -> new ReplicaAssignment(Seq(1), Seq.empty[Int], Seq.empty[Int]))))        // RF = 1
    EasyMock.verify(mockZkClient)
  }
}
