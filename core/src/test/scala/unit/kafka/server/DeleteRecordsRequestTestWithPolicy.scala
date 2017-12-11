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

import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.PolicyViolationException
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{ApiError, DeleteRecordsRequest}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.server.policy.{ClusterState, TopicManagementPolicy}
import org.junit.Assert._
import org.junit.{After, Before, Test}

import scala.collection.JavaConverters._

class DeleteRecordsRequestTestWithPolicy extends DeleteRecordsRequestTest {
  import DeleteRecordsRequestTestWithPolicy.Policy

  val policyProtectedPartition = new TopicPartition("policy-protected-topic", 0)

  val policyProtectedPartition2 = new TopicPartition("policy-protected-topic2", 0)

  val notAnonPartition = new TopicPartition("not-anon", 0)

  // If required, override properties by mutating the passed Properties object
  override def propertyOverrides(properties: Properties): Unit = {
    super.propertyOverrides(properties)
    properties.put(KafkaConfig.TopicManagementPolicyClassNameProp, classOf[Policy].getName)
  }

  @Before
  override def setUp(): Unit = {
    super.setUp()
    TestUtils.createTopic(zkUtils, policyProtectedPartition.topic, Map(0->Seq(1)), servers)
    TestUtils.createTopic(zkUtils, policyProtectedPartition2.topic, Map(0->Seq(1)), servers)
    TestUtils.createTopic(zkUtils, notAnonPartition.topic, Map(0->Seq(1)), servers)
    // create producer
    val producer = TestUtils.createNewProducer(TestUtils.getBrokerListStrFromServers(servers),
      retries = 5, keySerializer = new ByteArraySerializer, valueSerializer = new ByteArraySerializer)
    for (i <- 0 to 10) {
      producer.send(new ProducerRecord(policyProtectedPartition.topic, policyProtectedPartition.partition, new Array[Byte](i), new Array[Byte](i))).get
      producer.send(new ProducerRecord(policyProtectedPartition2.topic, policyProtectedPartition2.partition, new Array[Byte](i), new Array[Byte](i))).get
      producer.send(new ProducerRecord(notAnonPartition.topic, notAnonPartition.partition, new Array[Byte](i), new Array[Byte](i))).get
    }
    producer.close()

    TestUtils.waitUntilTrue(() => {
      val endOffsets = consumer.endOffsets(Set(policyProtectedPartition, policyProtectedPartition2, notAnonPartition).asJava)
      endOffsets.get(policyProtectedPartition) == 11 &&
        endOffsets.get(policyProtectedPartition2) == 11 &&
        endOffsets.get(notAnonPartition) == 11
    }, "Expected to get end offset of 11 for each partition")

  }

  @Test
  def testDeleteRecordsRequestsViolatePolicy(): Unit = {
    deleteRecordsRequestsViolatePolicy(1.toShort)
  }

  @Test
  def testDeleteRecordsRequestsViolatePolicyV0Compat(): Unit = {
    deleteRecordsRequestsViolatePolicy(0.toShort)
  }

  private def deleteRecordsRequestsViolatePolicy(requestVersion: Short) {
    val timeout = 10000
    val expectedError = if (requestVersion >= 1) Errors.POLICY_VIOLATION else Errors.UNKNOWN_SERVER_ERROR
    val expectedMessage = if (requestVersion >= 1) "protected" else null
    // Single topic
    validateErrorDeleteRecordsRequests(new DeleteRecordsRequest.Builder(timeout,
      Map[TopicPartition, java.lang.Long](policyProtectedPartition -> 5L).asJava, false).build(requestVersion),
      Map(policyProtectedPartition -> new ApiError(expectedError, expectedMessage)))
    // Multi topic
    validateErrorDeleteRecordsRequests(new DeleteRecordsRequest.Builder(timeout,
      Map[TopicPartition, java.lang.Long](policyProtectedPartition-> 5L, policyProtectedPartition2 -> 6L).asJava, false).build(requestVersion),
      Map(policyProtectedPartition -> new ApiError(expectedError, expectedMessage),
        policyProtectedPartition2 -> new ApiError(expectedError, expectedMessage)))
    // Mixed case
    validateErrorDeleteRecordsRequests(new DeleteRecordsRequest.Builder(timeout,
      Map[TopicPartition, java.lang.Long](topicPartition -> 5L, policyProtectedPartition -> 5L).asJava, false).build(requestVersion),
      Map(topicPartition -> ApiError.NONE,
        policyProtectedPartition -> new ApiError(expectedError, expectedMessage)))

    // test with principal
    validateErrorDeleteRecordsRequests(new DeleteRecordsRequest.Builder(timeout,
      Map[TopicPartition, java.lang.Long](notAnonPartition->5L).asJava, false).build(requestVersion),
      Map(notAnonPartition -> new ApiError(expectedError, if (expectedError == Errors.POLICY_VIOLATION) "anonymous user cannot delete" else null)))

    // TODO also need to check the other policy-protected APIs to cover their TopicStateImpls

  }
}

object DeleteRecordsRequestTestWithPolicy {
  class Policy extends TopicManagementPolicy {

    var closed = false

    var configured = false

    override def validateCreateTopic(requestMetadata: TopicManagementPolicy.CreateTopicRequest,
                                     clusterState: ClusterState): Unit = {
      assertFalse(closed)
      assertTrue(configured)
    }

    override def validateAlterTopic(requestMetadata: TopicManagementPolicy.AlterTopicRequest,
                                    clusterState: ClusterState): Unit = {
      fail("Should not be called in this test")
    }

    /**
      * Validate the given request to delete a topic
      * and throw a <code>PolicyViolationException</code> with a suitable error
      * message if the request does not satisfy this policy.
      *
      * The given {@code clusterState} can be used to discover the current state of the topic to be deleted.
      *
      * Clients will receive the POLICY_VIOLATION error code along with the exception's message.
      * Note that validation failure only affects the relevant topic,
      * other topics in the request will still be processed.
      *
      * @param requestMetadata the request parameters for the provided topic.
      * @param clusterState    the current state of the cluster
      * @throws PolicyViolationException if the request parameters do not satisfy this policy.
      */
    override def validateDeleteTopic(requestMetadata: TopicManagementPolicy.DeleteTopicRequest,
                                     clusterState: ClusterState): Unit = {
      fail("Should not be called in this test")
    }

    /**
      * Validate the given request to delete records from a topic
      * and throw a <code>PolicyViolationException</code> with a suitable error
      * message if the request does not satisfy this policy.
      *
      * The given {@code clusterState} can be used to discover the current state of the topic to have records deleted.
      *
      * Clients will receive the POLICY_VIOLATION error code along with the exception's message.
      * Note that validation failure only affects the relevant topic,
      * other topics in the request will still be processed.
      *
      * @param request the request parameters for the provided topic.
      * @param clusterState    the current state of the cluster
      * @throws PolicyViolationException if the request parameters do not satisfy this policy.
      */
    override def validateDeleteRecords(request: TopicManagementPolicy.DeleteRecordsRequest, clusterState: ClusterState): Unit = {
      assertFalse(closed)
      assertTrue(configured)

      assertEquals(3, clusterState.clusterSize())

      if (request.topic == "not-anon"
        && request.principal == KafkaPrincipal.ANONYMOUS) {
        throw new PolicyViolationException("anonymous user cannot delete")
      }

      if (request.topic.startsWith("policy-protected-")) {
        throw new PolicyViolationException("protected")
      }
    }

    /**
      * Configure this class with the given key-value pairs
      */
    override def configure(configs: util.Map[String, _]): Unit = {
      configured = true
    }

    override def close(): Unit = {
      closed = true
    }
  }
}