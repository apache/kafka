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

package kafka.server

import java.nio.ByteBuffer

import kafka.client.ClientUtils

import kafka.utils._
import org.apache.kafka.common.protocol.{Errors, ApiKeys, SecurityProtocol}
import org.apache.kafka.common.requests.{CreateTopicResponse, CreateTopicRequest, RequestHeader, ResponseHeader}
import org.junit.Assert._
import org.junit.{Before, Test}

import scala.collection.JavaConverters._

/**
  * TODO: This is a pseudo-temporary test implementation to test CreateTopicRequests while we still do not have an AdminClient.
  * Once the AdminClient is added this should be changed to utilize that instead of this custom/duplicated socket code.
  */
class CreateTopicRequestTest extends BaseAdminRequestTest {

  @Test
  def testValidCreateTopicRequests() {
    // Generated assignments
    validateValidCreateTopicRequests(new CreateTopicRequest(Map("topic1" -> new CreateTopicRequest.TopicDetails(1, 1)).asJava))
    validateValidCreateTopicRequests(new CreateTopicRequest(Map("topic2" -> new CreateTopicRequest.TopicDetails(1, 3)).asJava))
    val config3 = Map("min.insync.replicas" -> "2").asJava
    validateValidCreateTopicRequests(new CreateTopicRequest(Map("topic3" -> new CreateTopicRequest.TopicDetails(5, 2, config3)).asJava))
    // Manual assignments
    val assigments4 = replicaAssignmentToJava(Map(0 -> List(0)))
    validateValidCreateTopicRequests(new CreateTopicRequest(Map("topic4" -> new CreateTopicRequest.TopicDetails(assigments4)).asJava))
    val assigments5 = replicaAssignmentToJava(Map(0 -> List(0, 1), 1 -> List(1, 0), 2 -> List(1, 2)))
    val config5 = Map("min.insync.replicas" -> "2").asJava
    validateValidCreateTopicRequests(new CreateTopicRequest(Map("topic5" -> new CreateTopicRequest.TopicDetails(assigments5, config5)).asJava))
  }

  private def validateValidCreateTopicRequests(request: CreateTopicRequest): Unit = {
    val response = sendCreateTopicRequest(request)

    val error = response.errors.values.asScala.find(_ != Errors.NONE)
    assertTrue(s"There should be no errors, found ${response.errors.asScala}", error.isEmpty)

    request.topics.asScala.foreach { case (topic, details) =>
      TestUtils.waitUntilMetadataIsPropagated(servers, topic, 0)
      val metadata = ClientUtils.fetchTopicMetadata(Set(topic), Seq(broker.getBrokerEndPoint(SecurityProtocol.PLAINTEXT)),
        "validateValidCreateTopicRequests", 2000, 0).topicsMetadata
      val metadataForTopic = metadata.filter(p => p.topic.equals(topic)).head

      val partitions = if (!details.replicasAssignments.isEmpty)
        details.replicasAssignments.size
      else
        details.partitions

      val replication = if (!details.replicasAssignments.isEmpty)
        details.replicasAssignments.asScala.head._2.size
      else
        details.replicationFactor

      assertNotNull("The topic should be created", metadataForTopic)
      assertEquals("The topic should have the correct number of partitions", partitions, metadataForTopic.partitionsMetadata.size)
      assertEquals("The topic should have the correct replication factor", replication, metadataForTopic.partitionsMetadata.head.replicas.size)
    }
  }

  @Test
  def testInvalidCreateTopicRequests() {
    val existingTopic = "existing-topic"
    TestUtils.createTopic(zkUtils, existingTopic, 1, 1, servers)

    // Basic
    validateInvalidCreateTopicRequests(new CreateTopicRequest(Map(existingTopic -> new CreateTopicRequest.TopicDetails(1, 1)).asJava),
      Map(existingTopic -> Errors.TOPIC_ALREADY_EXISTS))
    validateInvalidCreateTopicRequests(new CreateTopicRequest(Map("invalid-partitions" -> new CreateTopicRequest.TopicDetails(-1, 1)).asJava),
      Map("invalid-partitions" -> Errors.INVALID_PARTITIONS))
    validateInvalidCreateTopicRequests(new CreateTopicRequest(Map("invalid-replication" -> new CreateTopicRequest.TopicDetails(1, numBrokers + 1)).asJava),
      Map("invalid-replication" -> Errors.INVALID_REPLICATION_FACTOR))
    val invalidConfig = Map("not.a.property" -> "invalid").asJava
    validateInvalidCreateTopicRequests(new CreateTopicRequest(Map("invalid-config" -> new CreateTopicRequest.TopicDetails(1, 1, invalidConfig)).asJava),
      Map("invalid-config" -> Errors.INVALID_ENTITY_CONFIG))
    val invalidAssignments = replicaAssignmentToJava(Map(0 -> List(0, 1), 1 -> List(0)))
    validateInvalidCreateTopicRequests(new CreateTopicRequest(Map("invalid-assignment" -> new CreateTopicRequest.TopicDetails(invalidAssignments)).asJava),
      Map("invalid-assignment" -> Errors.INVALID_REPLICA_ASSIGNMENT))

    // Partial
    validateInvalidCreateTopicRequests(
      new CreateTopicRequest(Map(
        existingTopic -> new CreateTopicRequest.TopicDetails(1, 1),
        "partial-partitions" -> new CreateTopicRequest.TopicDetails(-1, 1),
        "partial-replication" -> new CreateTopicRequest.TopicDetails(1, numBrokers + 1),
        "partial-none" -> new CreateTopicRequest.TopicDetails(1, 1)).asJava),
      Map(
        existingTopic -> Errors.TOPIC_ALREADY_EXISTS,
        "partial-partitions" -> Errors.INVALID_PARTITIONS,
        "partial-replication" -> Errors.INVALID_REPLICATION_FACTOR,
        "partial-none" -> Errors.NONE
      )
    )
  }

  private def validateInvalidCreateTopicRequests(request: CreateTopicRequest, expectedResponse: Map[String, Errors]): Unit = {
    val response = sendCreateTopicRequest(request)
    val errors = response.errors.asScala
    assertEquals("The response size should match", expectedResponse.size, response.errors.size)

    expectedResponse.foreach { case (topic, expectedError) =>
      assertEquals("The response error should match", expectedResponse(topic), errors(topic))
      // If no error validate topic exists
      if (expectedError == Errors.NONE) {
        TestUtils.waitUntilMetadataIsPropagated(servers, topic, 0)
        validateTopicExists(topic)
      }
    }
  }

  private def validateTopicExists(topic: String): Unit = {
    assertTrue("The topic should be created", topicExists(topic))
  }

  private def replicaAssignmentToJava(assignments: Map[Int, List[Int]]) = {
    assignments.map { case (k, v) => (k:Integer, v.map { i => i:Integer }.asJava) }.asJava
  }

  private def sendCreateTopicRequest(request: CreateTopicRequest): CreateTopicResponse = {
    val correlationId = -1

    val serializedBytes = {
      val header = new RequestHeader(ApiKeys.CREATE_TOPIC.id, 0, "", correlationId)
      val byteBuffer = ByteBuffer.allocate(header.sizeOf() + request.sizeOf)
      header.writeTo(byteBuffer)
      request.writeTo(byteBuffer)
      byteBuffer.array()
    }

    val response = requestAndReceive(serializedBytes)

    val responseBuffer = ByteBuffer.wrap(response)
    val responseHeader = ResponseHeader.parse(responseBuffer)
    CreateTopicResponse.parse(responseBuffer)
  }
}
