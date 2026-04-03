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
import kafka.zk.TopicZNode.TopicIdReplicaAssignment
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.server.policy.CreateTopicPolicy
import org.easymock.EasyMock
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Test}

class TopicsSatisfyMinRFTest {
  private var createTopicPolicy : Option[CreateTopicPolicy] = null
  private var mockZkClient: KafkaZkClient = null

  @BeforeEach
  def setUp(): Unit = {
    val config = TestUtils.createBrokerConfigs(1, "")
      .map(prop => {
        prop.setProperty(KafkaConfig.DefaultReplicationFactorProp, "2")  // Set min RF to be 2
        prop.setProperty(KafkaConfig.CreateTopicPolicyClassNameProp, "kafka.server.LiCreateTopicPolicy")
        prop
      }).map(KafkaConfig.fromProps)
    createTopicPolicy = Option(config.head.getConfiguredInstance(KafkaConfig.CreateTopicPolicyClassNameProp,
      classOf[CreateTopicPolicy]))
    mockZkClient = EasyMock.createMock(classOf[KafkaZkClient])
  }

  def buildTopicIdReplicaAssignment(topic: String,
                                    topicId: Option[Uuid],
                                    assignment: Map[Int, ReplicaAssignment]): TopicIdReplicaAssignment = {
    TopicIdReplicaAssignment(
      topic,
      topicId,
      assignment.map{case (partitionId, replicaAssignment) => (new TopicPartition(topic, partitionId), replicaAssignment)}
    )
  }

  // Mock new topic with sufficient RF, should satisfy
  @Test
  def testNewTopicSucceed(): Unit = {
    assertTrue(KafkaController.satisfiesLiCreateTopicPolicy(
      createTopicPolicy, mockZkClient,
      buildTopicIdReplicaAssignment(
        "test_topic1",
        None,
        Map(0 -> ReplicaAssignment(Seq(0, 1)))  // RF = 2
      )
    ))
  }

  // Mock new topic with insufficient RF, should fail
  @Test
  def testNewTopicFail(): Unit = {
    val topicFail = "test_topic1"
    EasyMock.expect(mockZkClient.getTopicPartitions(topicFail)).andReturn(Seq.empty)
    EasyMock.replay(mockZkClient)
    assertFalse(KafkaController.satisfiesLiCreateTopicPolicy(
      createTopicPolicy, mockZkClient,
      buildTopicIdReplicaAssignment(
        "test_topic1",
        None,
        Map(0 -> ReplicaAssignment(Seq(1)))   // RF = 1
      )
    ))
    EasyMock.verify(mockZkClient)
  }

  // Mock existing topic with insufficient RF, because it's pre-existing, should still satisfy
  @Test
  def testExistingTopicSucceed(): Unit = {
    val topicExisting = "test_topic1"
    EasyMock.expect(mockZkClient.getTopicPartitions(topicExisting)).andReturn(Seq("0"))  // Partition number = 1
    EasyMock.replay(mockZkClient)
    assertTrue(KafkaController.satisfiesLiCreateTopicPolicy(
      createTopicPolicy, mockZkClient,
      buildTopicIdReplicaAssignment(
        "test_topic1",
        None,
        Map(0 -> ReplicaAssignment(Seq(1)))   // RF = 1
      )
    ))
    EasyMock.verify(mockZkClient)
  }
}
