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

import org.apache.kafka.common.Uuid
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.CreateTopicsRequestData
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicCollection
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.CreateTopicsRequest
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.jdk.CollectionConverters._

class CreateTopicsRequestTest extends AbstractCreateTopicsRequestTest {

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testValidCreateTopicsRequests(quorum: String): Unit = {
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
    // Defaults
    validateValidCreateTopicsRequests(topicsReq(Seq(
      topicReq("topic12", replicationFactor = -1, numPartitions = -1))))
    validateValidCreateTopicsRequests(topicsReq(Seq(
      topicReq("topic13", replicationFactor = 2, numPartitions = -1))))
    validateValidCreateTopicsRequests(topicsReq(Seq(
      topicReq("topic14", replicationFactor = -1, numPartitions = 2))))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testErrorCreateTopicsRequests(quorum: String): Unit = {
    val existingTopic = "existing-topic"
    createTopic(existingTopic)
    // Basic
    validateErrorCreateTopicsRequests(topicsReq(Seq(topicReq(existingTopic))),
      Map(existingTopic -> error(Errors.TOPIC_ALREADY_EXISTS, Some("Topic 'existing-topic' already exists."))))
    validateErrorCreateTopicsRequests(topicsReq(Seq(topicReq("error-partitions", numPartitions = -2))),
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
      topicReq("partial-partitions", numPartitions = -2),
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
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testInvalidCreateTopicsRequests(quorum: String): Unit = {
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

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testCreateTopicsRequestVersions(quorum: String): Unit = {
    for (version <- ApiKeys.CREATE_TOPICS.oldestVersion to ApiKeys.CREATE_TOPICS.latestVersion) {
      val topic = s"topic_$version"
      val data = new CreateTopicsRequestData()
      data.setTimeoutMs(10000)
      data.setValidateOnly(false)
      data.setTopics(new CreatableTopicCollection(List(
        topicReq(topic, numPartitions = 1, replicationFactor = 1,
          config = Map("min.insync.replicas" -> "2"))
      ).asJava.iterator()))

      val request = new CreateTopicsRequest.Builder(data).build(version.asInstanceOf[Short])
      val response = sendCreateTopicRequest(request, adminSocketServer)

      val topicResponse = response.data.topics.find(topic)
      assertNotNull(topicResponse)
      assertEquals(topic, topicResponse.name)
      assertEquals(Errors.NONE.code, topicResponse.errorCode)
      if (version >= 5) {
        assertEquals(1, topicResponse.numPartitions)
        assertEquals(1, topicResponse.replicationFactor)
        val config = topicResponse.configs().asScala.find(_.name == "min.insync.replicas")
        assertTrue(config.isDefined)
        assertEquals("2", config.get.value)
      } else {
        assertEquals(-1, topicResponse.numPartitions)
        assertEquals(-1, topicResponse.replicationFactor)
        assertTrue(topicResponse.configs.isEmpty)
      }

      if (version >= 7)
        assertNotEquals(Uuid.ZERO_UUID, topicResponse.topicId())
      else
        assertEquals(Uuid.ZERO_UUID, topicResponse.topicId())
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testCreateClusterMetadataTopic(quorum: String): Unit = {
    validateErrorCreateTopicsRequests(
      topicsReq(Seq(topicReq(Topic.CLUSTER_METADATA_TOPIC_NAME))),
      Map(Topic.CLUSTER_METADATA_TOPIC_NAME ->
        error(Errors.INVALID_REQUEST, Some(s"Creation of internal topic ${Topic.CLUSTER_METADATA_TOPIC_NAME} is prohibited.")))
    )
  }
}
