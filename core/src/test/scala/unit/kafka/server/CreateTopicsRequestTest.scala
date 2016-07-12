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

import kafka.network.SocketServer
import kafka.utils._
import org.apache.kafka.common.protocol.types.Struct
import org.apache.kafka.common.protocol.{ApiKeys, Errors, ProtoUtils}
import org.apache.kafka.common.requests.{CreateTopicsRequest, CreateTopicsResponse, MetadataRequest, MetadataResponse}
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._


class CreateTopicsRequestTest extends BaseRequestTest {

  @Test
  def testValidCreateTopicsRequests() {
    val timeout = 10000
    // Generated assignments
    validateValidCreateTopicsRequests(new CreateTopicsRequest(Map("topic1" -> new CreateTopicsRequest.TopicDetails(1, 1.toShort)).asJava, timeout))
    validateValidCreateTopicsRequests(new CreateTopicsRequest(Map("topic2" -> new CreateTopicsRequest.TopicDetails(1, 3.toShort)).asJava, timeout))
    val config3 = Map("min.insync.replicas" -> "2").asJava
    validateValidCreateTopicsRequests(new CreateTopicsRequest(Map("topic3" -> new CreateTopicsRequest.TopicDetails(5, 2.toShort, config3)).asJava, timeout))
    // Manual assignments
    val assignments4 = replicaAssignmentToJava(Map(0 -> List(0)))
    validateValidCreateTopicsRequests(new CreateTopicsRequest(Map("topic4" -> new CreateTopicsRequest.TopicDetails(assignments4)).asJava, timeout))
    val assignments5 = replicaAssignmentToJava(Map(0 -> List(0, 1), 1 -> List(1, 0), 2 -> List(1, 2)))
    val config5 = Map("min.insync.replicas" -> "2").asJava
    validateValidCreateTopicsRequests(new CreateTopicsRequest(Map("topic5" -> new CreateTopicsRequest.TopicDetails(assignments5, config5)).asJava, timeout))
    // Mixed
    val assignments8 = replicaAssignmentToJava(Map(0 -> List(0, 1), 1 -> List(1, 0), 2 -> List(1, 2)))
    validateValidCreateTopicsRequests(new CreateTopicsRequest(Map(
      "topic6" -> new CreateTopicsRequest.TopicDetails(1, 1.toShort),
      "topic7" -> new CreateTopicsRequest.TopicDetails(5, 2.toShort),
      "topic8" -> new CreateTopicsRequest.TopicDetails(assignments8)).asJava, timeout)
    )
  }

  private def validateValidCreateTopicsRequests(request: CreateTopicsRequest): Unit = {
    val response = sendCreateTopicRequest(request, 0)

    val error = response.errors.values.asScala.find(_ != Errors.NONE)
    assertTrue(s"There should be no errors, found ${response.errors.asScala}", error.isEmpty)

    request.topics.asScala.foreach { case (topic, details) =>

      def verifyMetadata(socketServer: SocketServer) = {
        val metadata = sendMetadataRequest(new MetadataRequest(List(topic).asJava)).topicMetadata.asScala
        val metadataForTopic = metadata.filter(p => p.topic.equals(topic)).head

        val partitions = if (!details.replicasAssignments.isEmpty)
          details.replicasAssignments.size
        else
          details.numPartitions

        val replication = if (!details.replicasAssignments.isEmpty)
          details.replicasAssignments.asScala.head._2.size
        else
          details.replicationFactor

        assertNotNull("The topic should be created", metadataForTopic)
        assertEquals("The topic should have the correct number of partitions", partitions, metadataForTopic.partitionMetadata.size)
        assertEquals("The topic should have the correct replication factor", replication, metadataForTopic.partitionMetadata.asScala.head.replicas.size)
      }

      // Verify controller broker has the correct metadata
      verifyMetadata(controllerSocketServer)
      // Wait until metadata is propagated and validate non-controller broker has the correct metadata
      TestUtils.waitUntilMetadataIsPropagated(servers, topic, 0)
      verifyMetadata(notControllerSocketServer)
    }
  }

  @Test
  def testErrorCreateTopicsRequests() {
    val timeout = 10000
    val existingTopic = "existing-topic"
    TestUtils.createTopic(zkUtils, existingTopic, 1, 1, servers)

    // Basic
    validateErrorCreateTopicsRequests(new CreateTopicsRequest(Map(existingTopic -> new CreateTopicsRequest.TopicDetails(1, 1.toShort)).asJava, timeout),
      Map(existingTopic -> Errors.TOPIC_ALREADY_EXISTS))
    validateErrorCreateTopicsRequests(new CreateTopicsRequest(Map("error-partitions" -> new CreateTopicsRequest.TopicDetails(-1, 1.toShort)).asJava, timeout),
      Map("error-partitions" -> Errors.INVALID_PARTITIONS))
    validateErrorCreateTopicsRequests(new CreateTopicsRequest(Map("error-replication" -> new CreateTopicsRequest.TopicDetails(1, (numBrokers + 1).toShort)).asJava, timeout),
      Map("error-replication" -> Errors.INVALID_REPLICATION_FACTOR))
    val invalidConfig = Map("not.a.property" -> "error").asJava
    validateErrorCreateTopicsRequests(new CreateTopicsRequest(Map("error-config" -> new CreateTopicsRequest.TopicDetails(1, 1.toShort, invalidConfig)).asJava, timeout),
      Map("error-config" -> Errors.INVALID_CONFIG))
    val invalidAssignments = replicaAssignmentToJava(Map(0 -> List(0, 1), 1 -> List(0)))
    validateErrorCreateTopicsRequests(new CreateTopicsRequest(Map("error-assignment" -> new CreateTopicsRequest.TopicDetails(invalidAssignments)).asJava, timeout),
      Map("error-assignment" -> Errors.INVALID_REPLICA_ASSIGNMENT))

    // Partial
    validateErrorCreateTopicsRequests(
      new CreateTopicsRequest(Map(
        existingTopic -> new CreateTopicsRequest.TopicDetails(1, 1.toShort),
        "partial-partitions" -> new CreateTopicsRequest.TopicDetails(-1, 1.toShort),
        "partial-replication" -> new CreateTopicsRequest.TopicDetails(1, (numBrokers + 1).toShort),
        "partial-assignment" -> new CreateTopicsRequest.TopicDetails(invalidAssignments),
        "partial-none" -> new CreateTopicsRequest.TopicDetails(1, 1.toShort)).asJava, timeout),
      Map(
        existingTopic -> Errors.TOPIC_ALREADY_EXISTS,
        "partial-partitions" -> Errors.INVALID_PARTITIONS,
        "partial-replication" -> Errors.INVALID_REPLICATION_FACTOR,
        "partial-assignment" -> Errors.INVALID_REPLICA_ASSIGNMENT,
        "partial-none" -> Errors.NONE
      )
    )
    validateTopicExists("partial-none")

    // Timeout
    // We don't expect a request to ever complete within 1ms. A timeout of 1 ms allows us to test the purgatory timeout logic.
    validateErrorCreateTopicsRequests(new CreateTopicsRequest(Map("error-timeout" -> new CreateTopicsRequest.TopicDetails(10, 3.toShort)).asJava, 1),
      Map("error-timeout" -> Errors.REQUEST_TIMED_OUT))
    validateErrorCreateTopicsRequests(new CreateTopicsRequest(Map("error-timeout-zero" -> new CreateTopicsRequest.TopicDetails(10, 3.toShort)).asJava, 0),
      Map("error-timeout-zero" -> Errors.REQUEST_TIMED_OUT))
    // Negative timeouts are treated the same as 0
    validateErrorCreateTopicsRequests(new CreateTopicsRequest(Map("error-timeout-negative" -> new CreateTopicsRequest.TopicDetails(10, 3.toShort)).asJava, -1),
      Map("error-timeout-negative" -> Errors.REQUEST_TIMED_OUT))
    // The topics should still get created eventually
    TestUtils.waitUntilMetadataIsPropagated(servers, "error-timeout", 0)
    TestUtils.waitUntilMetadataIsPropagated(servers, "error-timeout-zero", 0)
    TestUtils.waitUntilMetadataIsPropagated(servers, "error-timeout-negative", 0)
    validateTopicExists("error-timeout")
    validateTopicExists("error-timeout-zero")
    validateTopicExists("error-timeout-negative")
  }

  @Test
  def testInvalidCreateTopicsRequests() {
    // Duplicate
    val singleRequest = new CreateTopicsRequest(Map("duplicate-topic" -> new CreateTopicsRequest.TopicDetails(1, 1.toShort)).asJava, 1000)
    val duplicateRequest = duplicateFirstTopic(singleRequest)
    assertFalse("Request doesn't have duplicate topics", duplicateRequest.duplicateTopics().isEmpty)
    validateErrorCreateTopicsRequests(duplicateRequest, Map("duplicate-topic" -> Errors.INVALID_REQUEST))

    // Duplicate Partial
    val doubleRequest = new CreateTopicsRequest(Map(
      "duplicate-topic" -> new CreateTopicsRequest.TopicDetails(1, 1.toShort),
      "other-topic" -> new CreateTopicsRequest.TopicDetails(1, 1.toShort)).asJava, 1000)
    val duplicateDoubleRequest = duplicateFirstTopic(doubleRequest)
    assertFalse("Request doesn't have duplicate topics", duplicateDoubleRequest.duplicateTopics().isEmpty)
    validateErrorCreateTopicsRequests(duplicateDoubleRequest, Map(
      "duplicate-topic" -> Errors.INVALID_REQUEST,
      "other-topic" -> Errors.NONE))

    // Partitions/ReplicationFactor and ReplicaAssignment
    val assignments = replicaAssignmentToJava(Map(0 -> List(0)))
    val assignmentRequest = new CreateTopicsRequest(Map("bad-args-topic" -> new CreateTopicsRequest.TopicDetails(assignments)).asJava, 1000)
    val badArgumentsRequest = addPartitionsAndReplicationFactorToFirstTopic(assignmentRequest)
    validateErrorCreateTopicsRequests(badArgumentsRequest, Map("bad-args-topic" -> Errors.INVALID_REQUEST))
  }

  private def duplicateFirstTopic(request: CreateTopicsRequest) = {
    val struct = request.toStruct
    val topics = struct.getArray("create_topic_requests")
    val firstTopic= topics(0).asInstanceOf[Struct]
    val newTopics = firstTopic :: topics.toList
    struct.set("create_topic_requests", newTopics.toArray)
    new CreateTopicsRequest(struct)
  }

  private def addPartitionsAndReplicationFactorToFirstTopic(request: CreateTopicsRequest) = {
    val struct = request.toStruct
    val topics = struct.getArray("create_topic_requests")
    val firstTopic = topics(0).asInstanceOf[Struct]
    firstTopic.set("num_partitions", 1)
    firstTopic.set("replication_factor", 1.toShort)
    new CreateTopicsRequest(struct)
  }

  private def validateErrorCreateTopicsRequests(request: CreateTopicsRequest, expectedResponse: Map[String, Errors]): Unit = {
    val response = sendCreateTopicRequest(request, 0)
    val errors = response.errors.asScala
    assertEquals("The response size should match", expectedResponse.size, response.errors.size)

    expectedResponse.foreach { case (topic, expectedError) =>
      assertEquals("The response error should match", expectedResponse(topic), errors(topic))
      // If no error validate topic exists
      if (expectedError == Errors.NONE) {
        validateTopicExists(topic)
      }
    }
  }

  @Test
  def testNotController() {
    val request = new CreateTopicsRequest(Map("topic1" -> new CreateTopicsRequest.TopicDetails(1, 1.toShort)).asJava, 1000)
    val response = sendCreateTopicRequest(request, 0, notControllerSocketServer)

    val error = response.errors.asScala.head._2
    assertEquals("Expected controller error when routed incorrectly",  Errors.NOT_CONTROLLER, error)
  }

  private def validateTopicExists(topic: String): Unit = {
    TestUtils.waitUntilMetadataIsPropagated(servers, topic, 0)
    val metadata = sendMetadataRequest(new MetadataRequest(List(topic).asJava)).topicMetadata.asScala
    assertTrue("The topic should be created", metadata.exists(p => p.topic.equals(topic) && p.error() == Errors.NONE))
  }

  private def replicaAssignmentToJava(assignments: Map[Int, List[Int]]) = {
    assignments.map { case (k, v) => (k:Integer, v.map { i => i:Integer }.asJava) }.asJava
  }

  private def sendCreateTopicRequest(request: CreateTopicsRequest, version: Short, socketServer: SocketServer = controllerSocketServer): CreateTopicsResponse = {
    val response = send(request, ApiKeys.CREATE_TOPICS, Some(version), socketServer)
    CreateTopicsResponse.parse(response, version)
  }

  private def sendMetadataRequest(request: MetadataRequest, destination: SocketServer = anySocketServer): MetadataResponse = {
    val version = ProtoUtils.latestVersion(ApiKeys.METADATA.id)
    val response = send(request, ApiKeys.METADATA, destination = destination)
    MetadataResponse.parse(response, version)
  }
}
