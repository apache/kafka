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
import org.junit.Assert._
import org.junit.Test

class CreateTopicsRequestTest extends AbstractCreateTopicsRequestTest {
  @Test
  def testValidCreateTopicsRequests() {
    // Generated assignments
    validateValidCreateTopicsRequests(topicsReq(Seq(topicReq("topic1"))))
    validateValidCreateTopicsRequests(topicsReq(Seq(topicReq("topic2", replicationFactor = 3))))
    validateValidCreateTopicsRequests(topicsReq(Seq(topicReq("topic3",
      numPartitions = 5, replicationFactor = 2, config = Map("min.insync.replicas" -> "2")))))
    // Manual assignments
    validateValidCreateTopicsRequests(topicsReq(Seq(topicReq("topic4", assignment = Map(0 -> List(0))))))
    validateValidCreateTopicsRequests(topicsReq(Seq(topicReq("topic5",
      assignment = Map(0 -> List(0, 1), 1 -> List(1, 0), 2 -> List(1, 2)),
      config = Map("min.insync.replicas" -> "2")))))
    // Mixed
    validateValidCreateTopicsRequests(topicsReq(Seq(topicReq("topic6"),
      topicReq("topic7", numPartitions = 5, replicationFactor = 2),
      topicReq("topic8", assignment = Map(0 -> List(0, 1), 1 -> List(1, 0), 2 -> List(1, 2))))))
    validateValidCreateTopicsRequests(topicsReq(Seq(topicReq("topic9"),
      topicReq("topic10", numPartitions = 5, replicationFactor = 2),
      topicReq("topic11", assignment = Map(0 -> List(0, 1), 1 -> List(1, 0), 2 -> List(1, 2)))),
      validateOnly = true))
  }

  @Test
  def testErrorCreateTopicsRequests() {
    val existingTopic = "existing-topic"
    createTopic(existingTopic, 1, 1)
    // Basic
    validateErrorCreateTopicsRequests(topicsReq(Seq(topicReq(existingTopic))),
      Map(existingTopic -> error(Errors.TOPIC_ALREADY_EXISTS, Some("Topic 'existing-topic' already exists."))))
    validateErrorCreateTopicsRequests(topicsReq(Seq(topicReq("error-partitions", numPartitions = -1))),
      Map("error-partitions" -> error(Errors.INVALID_PARTITIONS)), checkErrorMessage = false)
    validateErrorCreateTopicsRequests(topicsReq(Seq(topicReq("error-replication",
      replicationFactor = brokerCount + 1))),
      Map("error-replication" -> error(Errors.INVALID_REPLICATION_FACTOR)), checkErrorMessage = false)
    validateErrorCreateTopicsRequests(topicsReq(Seq(topicReq("error-config",
      config=Map("not.a.property" -> "error")))),
      Map("error-config" -> error(Errors.INVALID_CONFIG)), checkErrorMessage = false)
    validateErrorCreateTopicsRequests(topicsReq(Seq(topicReq("error-config-value",
      config=Map("message.format.version" -> "invalid-value")))),
      Map("error-config-value" -> error(Errors.INVALID_CONFIG)), checkErrorMessage = false)
    validateErrorCreateTopicsRequests(topicsReq(Seq(topicReq("error-assignment",
      assignment=Map(0 -> List(0, 1), 1 -> List(0))))),
      Map("error-assignment" -> error(Errors.INVALID_REPLICA_ASSIGNMENT)), checkErrorMessage = false)

    // Partial
    validateErrorCreateTopicsRequests(topicsReq(Seq(
      topicReq(existingTopic),
      topicReq("partial-partitions", numPartitions = -1),
      topicReq("partial-replication", replicationFactor=brokerCount + 1),
      topicReq("partial-assignment", assignment=Map(0 -> List(0, 1), 1 -> List(0))),
      topicReq("partial-none"))),
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
    validateErrorCreateTopicsRequests(topicsReq(Seq(
      topicReq("error-timeout", numPartitions = 10, replicationFactor = 3)), timeout = 1),
      Map("error-timeout" -> error(Errors.REQUEST_TIMED_OUT)), checkErrorMessage = false)
    validateErrorCreateTopicsRequests(topicsReq(Seq(
      topicReq("error-timeout-zero", numPartitions = 10, replicationFactor = 3)), timeout = 0),
      Map("error-timeout-zero" -> error(Errors.REQUEST_TIMED_OUT)), checkErrorMessage = false)
    // Negative timeouts are treated the same as 0
    validateErrorCreateTopicsRequests(topicsReq(Seq(
      topicReq("error-timeout-negative", numPartitions = 10, replicationFactor = 3)), timeout = -1),
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
    // Partitions/ReplicationFactor and ReplicaAssignment
    validateErrorCreateTopicsRequests(topicsReq(Seq(
      topicReq("bad-args-topic", numPartitions = 10, replicationFactor = 3,
        assignment = Map(0 -> List(0))))),
      Map("bad-args-topic" -> error(Errors.INVALID_REQUEST)), checkErrorMessage = false)

    validateErrorCreateTopicsRequests(topicsReq(Seq(
      topicReq("bad-args-topic", numPartitions = 10, replicationFactor = 3,
        assignment = Map(0 -> List(0)))), validateOnly = true),
      Map("bad-args-topic" -> error(Errors.INVALID_REQUEST)), checkErrorMessage = false)
  }

  @Test
  def testNotController() {
    val req = topicsReq(Seq(topicReq("topic1")))
    val response = sendCreateTopicRequest(req, notControllerSocketServer)
    assertEquals(1, response.errorCounts().get(Errors.NOT_CONTROLLER))
  }
}
