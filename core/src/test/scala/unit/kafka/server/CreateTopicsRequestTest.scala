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

import kafka.utils._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.CreateTopicsRequest
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._

class CreateTopicsRequestTest extends AbstractCreateTopicsRequestTest {

  @Test
  def testValidCreateTopicsRequests() {
    val timeout = 10000
    // Generated assignments
    validateValidCreateTopicsRequests(new CreateTopicsRequest.Builder(Map("topic1" -> new CreateTopicsRequest.TopicDetails(1, 1.toShort)).asJava, timeout).build())
    validateValidCreateTopicsRequests(new CreateTopicsRequest.Builder(Map("topic2" -> new CreateTopicsRequest.TopicDetails(1, 3.toShort)).asJava, timeout).build())
    val config3 = Map("min.insync.replicas" -> "2").asJava
    validateValidCreateTopicsRequests(new CreateTopicsRequest.Builder(Map("topic3" -> new CreateTopicsRequest.TopicDetails(5, 2.toShort, config3)).asJava, timeout).build())
    // Manual assignments
    val assignments4 = replicaAssignmentToJava(Map(0 -> List(0)))
    validateValidCreateTopicsRequests(new CreateTopicsRequest.Builder(Map("topic4" -> new CreateTopicsRequest.TopicDetails(assignments4)).asJava, timeout).build())
    val assignments5 = replicaAssignmentToJava(Map(0 -> List(0, 1), 1 -> List(1, 0), 2 -> List(1, 2)))
    val config5 = Map("min.insync.replicas" -> "2").asJava
    validateValidCreateTopicsRequests(new CreateTopicsRequest.Builder(Map("topic5" -> new CreateTopicsRequest.TopicDetails(assignments5, config5)).asJava, timeout).build())
    // Mixed
    val assignments8 = replicaAssignmentToJava(Map(0 -> List(0, 1), 1 -> List(1, 0), 2 -> List(1, 2)))
    validateValidCreateTopicsRequests(new CreateTopicsRequest.Builder(Map(
      "topic6" -> new CreateTopicsRequest.TopicDetails(1, 1.toShort),
      "topic7" -> new CreateTopicsRequest.TopicDetails(5, 2.toShort),
      "topic8" -> new CreateTopicsRequest.TopicDetails(assignments8)).asJava, timeout).build()
    )
    validateValidCreateTopicsRequests(new CreateTopicsRequest.Builder(Map(
      "topic9" -> new CreateTopicsRequest.TopicDetails(1, 1.toShort),
      "topic10" -> new CreateTopicsRequest.TopicDetails(5, 2.toShort),
      "topic11" -> new CreateTopicsRequest.TopicDetails(assignments8)).asJava, timeout, true).build()
    )
  }

  @Test
  def testErrorCreateTopicsRequests() {
    val timeout = 10000
    val existingTopic = "existing-topic"
    TestUtils.createTopic(zkUtils, existingTopic, 1, 1, servers)

    // Basic
    validateErrorCreateTopicsRequests(new CreateTopicsRequest.Builder(Map(existingTopic -> new CreateTopicsRequest.TopicDetails(1, 1.toShort)).asJava, timeout).build(),
      Map(existingTopic -> error(Errors.TOPIC_ALREADY_EXISTS, Some("Topic 'existing-topic' already exists."))))
    validateErrorCreateTopicsRequests(new CreateTopicsRequest.Builder(Map("error-partitions" -> new CreateTopicsRequest.TopicDetails(-1, 1.toShort)).asJava, timeout).build(),
      Map("error-partitions" -> error(Errors.INVALID_PARTITIONS)), checkErrorMessage = false)
    validateErrorCreateTopicsRequests(new CreateTopicsRequest.Builder(Map("error-replication" -> new CreateTopicsRequest.TopicDetails(1, (numBrokers + 1).toShort)).asJava, timeout).build(),
      Map("error-replication" -> error(Errors.INVALID_REPLICATION_FACTOR)), checkErrorMessage = false)
    val invalidConfig = Map("not.a.property" -> "error").asJava
    validateErrorCreateTopicsRequests(new CreateTopicsRequest.Builder(Map("error-config" -> new CreateTopicsRequest.TopicDetails(1, 1.toShort, invalidConfig)).asJava, timeout).build(),
      Map("error-config" -> error(Errors.INVALID_CONFIG)), checkErrorMessage = false)
    val invalidAssignments = replicaAssignmentToJava(Map(0 -> List(0, 1), 1 -> List(0)))
    validateErrorCreateTopicsRequests(new CreateTopicsRequest.Builder(Map("error-assignment" -> new CreateTopicsRequest.TopicDetails(invalidAssignments)).asJava, timeout).build(),
      Map("error-assignment" -> error(Errors.INVALID_REPLICA_ASSIGNMENT)), checkErrorMessage = false)

    // Partial
    validateErrorCreateTopicsRequests(
      new CreateTopicsRequest.Builder(Map(
        existingTopic -> new CreateTopicsRequest.TopicDetails(1, 1.toShort),
        "partial-partitions" -> new CreateTopicsRequest.TopicDetails(-1, 1.toShort),
        "partial-replication" -> new CreateTopicsRequest.TopicDetails(1, (numBrokers + 1).toShort),
        "partial-assignment" -> new CreateTopicsRequest.TopicDetails(invalidAssignments),
        "partial-none" -> new CreateTopicsRequest.TopicDetails(1, 1.toShort)).asJava, timeout).build(),
      Map(
        existingTopic -> error(Errors.TOPIC_ALREADY_EXISTS),
        "partial-partitions" -> error(Errors.INVALID_PARTITIONS),
        "partial-replication" -> error(Errors.INVALID_REPLICATION_FACTOR),
        "partial-assignment" -> error(Errors.INVALID_REPLICA_ASSIGNMENT),
        "partial-none" -> error(Errors.NONE)
      ), checkErrorMessage = false
    )
    validateTopicExists("partial-none")

    // Timeout
    // We don't expect a request to ever complete within 1ms. A timeout of 1 ms allows us to test the purgatory timeout logic.
    validateErrorCreateTopicsRequests(new CreateTopicsRequest.Builder(Map("error-timeout" -> new CreateTopicsRequest.TopicDetails(10, 3.toShort)).asJava, 1).build(),
      Map("error-timeout" -> error(Errors.REQUEST_TIMED_OUT)), checkErrorMessage = false)
    validateErrorCreateTopicsRequests(new CreateTopicsRequest.Builder(Map("error-timeout-zero" -> new CreateTopicsRequest.TopicDetails(10, 3.toShort)).asJava, 0).build(),
      Map("error-timeout-zero" -> error(Errors.REQUEST_TIMED_OUT)), checkErrorMessage = false)
    // Negative timeouts are treated the same as 0
    validateErrorCreateTopicsRequests(new CreateTopicsRequest.Builder(Map("error-timeout-negative" -> new CreateTopicsRequest.TopicDetails(10, 3.toShort)).asJava, -1).build(),
      Map("error-timeout-negative" -> error(Errors.REQUEST_TIMED_OUT)), checkErrorMessage = false)
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
    val singleRequest = new CreateTopicsRequest.Builder(Map("duplicate-topic" ->
        new CreateTopicsRequest.TopicDetails(1, 1.toShort)).asJava, 1000).build()
    validateErrorCreateTopicsRequests(singleRequest, Map("duplicate-topic" -> error(Errors.INVALID_REQUEST,
      Some("""Create topics request from client `client-id` contains multiple entries for the following topics: duplicate-topic"""))),
      requestStruct = Some(toStructWithDuplicateFirstTopic(singleRequest)))

    // Duplicate Partial with validateOnly
    val doubleRequestValidateOnly = new CreateTopicsRequest.Builder(Map(
      "duplicate-topic" -> new CreateTopicsRequest.TopicDetails(1, 1.toShort),
      "other-topic" -> new CreateTopicsRequest.TopicDetails(1, 1.toShort)).asJava, 1000, true).build()
    validateErrorCreateTopicsRequests(doubleRequestValidateOnly, Map(
      "duplicate-topic" -> error(Errors.INVALID_REQUEST),
      "other-topic" -> error(Errors.NONE)), checkErrorMessage = false,
      requestStruct = Some(toStructWithDuplicateFirstTopic(doubleRequestValidateOnly)))

    // Duplicate Partial
    val doubleRequest = new CreateTopicsRequest.Builder(Map(
      "duplicate-topic" -> new CreateTopicsRequest.TopicDetails(1, 1.toShort),
      "other-topic" -> new CreateTopicsRequest.TopicDetails(1, 1.toShort)).asJava, 1000).build()
    validateErrorCreateTopicsRequests(doubleRequest, Map(
      "duplicate-topic" -> error(Errors.INVALID_REQUEST),
      "other-topic" -> error(Errors.NONE)), checkErrorMessage = false,
      requestStruct = Some(toStructWithDuplicateFirstTopic(doubleRequest)))

    // Partitions/ReplicationFactor and ReplicaAssignment
    val assignments = replicaAssignmentToJava(Map(0 -> List(0)))
    val assignmentRequest = new CreateTopicsRequest.Builder(Map("bad-args-topic" ->
        new CreateTopicsRequest.TopicDetails(assignments)).asJava, 1000).build()
    val badArgumentsRequest = addPartitionsAndReplicationFactorToFirstTopic(assignmentRequest)
    validateErrorCreateTopicsRequests(badArgumentsRequest, Map("bad-args-topic" -> error(Errors.INVALID_REQUEST)),
      checkErrorMessage = false)

    // Partitions/ReplicationFactor and ReplicaAssignment with validateOnly
    val assignmentRequestValidateOnly = new CreateTopicsRequest.Builder(Map("bad-args-topic" ->
      new CreateTopicsRequest.TopicDetails(assignments)).asJava, 1000, true).build()
    val badArgumentsRequestValidateOnly = addPartitionsAndReplicationFactorToFirstTopic(assignmentRequestValidateOnly)
    validateErrorCreateTopicsRequests(badArgumentsRequestValidateOnly, Map("bad-args-topic" -> error(Errors.INVALID_REQUEST)),
      checkErrorMessage = false)
  }

  @Test
  def testNotController() {
    val request = new CreateTopicsRequest.Builder(Map("topic1" -> new CreateTopicsRequest.TopicDetails(1, 1.toShort)).asJava, 1000).build()
    val response = sendCreateTopicRequest(request, notControllerSocketServer)

    val error = response.errors.asScala.head._2.error
    assertEquals("Expected controller error when routed incorrectly", Errors.NOT_CONTROLLER, error)
  }

}
