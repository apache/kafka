/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package unit.kafka.server

import java.util
import java.util.Properties

import kafka.server.{DeleteTopicsRequestTest, KafkaConfig}
import kafka.utils.TestUtils
import org.apache.kafka.common.errors.PolicyViolationException
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{ApiError, DeleteTopicsRequest}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.server.policy.{ClusterState, TopicManagementPolicy}
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._

class DeleteTopicsRequestTestWithPolicy extends DeleteTopicsRequestTest {

  import DeleteTopicsRequestTestWithPolicy.Policy

  // If required, override properties by mutating the passed Properties object
  protected override def propertyOverrides(properties: Properties): Unit = {
    super.propertyOverrides(properties)
    properties.put(KafkaConfig.TopicManagementPolicyClassNameProp, classOf[Policy].getName)
  }

  @Test
  def testDeleteTopicRequestsViolatePolicy() {
    val timeout = 10000
    // TODO TestUtils.createTopic(zkUtils, "marked-for-deletion", 1, 2, servers)
    // TODO validateValidDeleteTopicRequests(new DeleteTopicsRequest.Builder(Set("marked-for-deletion").asJava, timeout, false).build())

    // Single topic
    TestUtils.createTopic(zkUtils, "policy-protected-topic-1", 1, 1, servers)
    validateErrorDeleteTopicRequests(new DeleteTopicsRequest.Builder(
      Set("policy-protected-topic-1").asJava, timeout, false).build(),
      Map("policy-protected-topic-1" -> new ApiError(Errors.POLICY_VIOLATION, "protected")))
    // Multi topic
    TestUtils.createTopic(zkUtils, "policy-protected-topic-3", 5, 2, servers)
    TestUtils.createTopic(zkUtils, "policy-protected-topic-4", 1, 2, servers)
    validateErrorDeleteTopicRequests(new DeleteTopicsRequest.Builder(
      Set("policy-protected-topic-3", "policy-protected-topic-4").asJava, timeout, false).build(),
      Map("policy-protected-topic-3" -> new ApiError(Errors.POLICY_VIOLATION, "protected"),
        "policy-protected-topic-4" -> new ApiError(Errors.POLICY_VIOLATION, "protected")))
    // Mixed case
    TestUtils.createTopic(zkUtils, "topic-5", 5, 2, servers)
    TestUtils.createTopic(zkUtils, "policy-protected-topic-6", 1, 2, servers)
    validateErrorDeleteTopicRequests(new DeleteTopicsRequest.Builder(
      Set("topic-5", "policy-protected-topic-6").asJava, timeout, false).build(),
      Map("topic-5" -> ApiError.NONE,
        "policy-protected-topic-6" -> new ApiError(Errors.POLICY_VIOLATION, "protected")))

    // test with principal
    TestUtils.createTopic(zkUtils, "not-anonymous-topic", 1, 1, servers)
    validateErrorDeleteTopicRequests(new DeleteTopicsRequest.Builder(Set("not-anonymous-topic").asJava, timeout, false).build(),
      Map("not-anonymous-topic" -> new ApiError(Errors.POLICY_VIOLATION, "anonymous user cannot delete")))
    // TODO test everything wrt the cluster state.
    // TODO test with old version of protocol

    // TODO also need to check the other policy-protected APIs to cover their TopicStateImpls

    // TODO need to test validateOnly
  }

}

object DeleteTopicsRequestTestWithPolicy {
  class Policy extends TopicManagementPolicy {

    var configured = false

    var closed = false

    override def validateCreateTopic(requestMetadata: TopicManagementPolicy.CreateTopicRequest, clusterState: ClusterState): Unit = {
      assertTrue("policy should be configured", configured)
      assertFalse("policy should not be closed", closed)
    }

    override def validateAlterTopic(requestMetadata: TopicManagementPolicy.AlterTopicRequest, clusterState: ClusterState): Unit = {
      fail("this test shouldn't alter topics");
    }

    override def validateDeleteTopic(requestMetadata: TopicManagementPolicy.DeleteTopicRequest, clusterState: ClusterState): Unit = {
      assertTrue("policy should be configured", configured)
      assertFalse("policy should not be closed", closed)
      assertEquals(s"Wrong cluster size ${clusterState.clusterSize}", 3, clusterState.clusterSize)

      if (clusterState.topicState("policy-protected-topic-4") != null) {
        assertEquals(1, clusterState.topicState("policy-protected-topic-4").numPartitions)
        assertEquals(2, clusterState.topicState("policy-protected-topic-4").replicationFactor)
      }

      val topic = requestMetadata.topic
      if (topic.startsWith("policy-protected-topic-")) {
        /* TODO assertEquals("Expected more topics in the cluster", 3, clusterState.topics(false, false))
        assertEquals("Expected more topics in the cluster", 3, clusterState.topics(false, true))
        assertEquals("Expected more topics in the cluster", 3, clusterState.topics(true, false))
        assertEquals("Expected more topics in the cluster", 3, clusterState.topics(true, true))*/
        // TODO assertTrue(clusterState.topicState("marked-for-deletion").markedForDeletion)

        assertFalse(clusterState.topicState("policy-protected-topic-1").internal)
        //assertTrue(clusterState.topicState("__consumer_offsets").internal)
        assertFalse(clusterState.topicState("policy-protected-topic-1").markedForDeletion)

        assertEquals(1, clusterState.topicState("policy-protected-topic-1").numPartitions)
        assertEquals(1, clusterState.topicState("policy-protected-topic-1").replicationFactor)

        throw new PolicyViolationException("protected")
      }
      if (topic == "not-anonymous-topic" && requestMetadata.principal == KafkaPrincipal.ANONYMOUS) {
        throw new PolicyViolationException("anonymous user cannot delete")
      }




    }

    override def validateDeleteRecords(requestMetadata: TopicManagementPolicy.DeleteRecordsRequest, clusterState: ClusterState): Unit = {
      fail("this test shouldn't delete records");
    }

    override def close(): Unit = closed = true

    override def configure(configs: util.Map[String, _]): Unit = {
      configured = true
    }
  }
}