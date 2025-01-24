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

import java.util
import java.util.Properties
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.errors.PolicyViolationException
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.server.config.ServerLogConfigs.CREATE_TOPIC_POLICY_CLASS_NAME_CONFIG
import org.apache.kafka.server.policy.CreateTopicPolicy
import org.apache.kafka.server.policy.CreateTopicPolicy.RequestMetadata
import org.junit.jupiter.api.TestInfo
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.jdk.CollectionConverters._

class CreateTopicsRequestWithPolicyTest extends AbstractCreateTopicsRequestTest {
  import CreateTopicsRequestWithPolicyTest._

  override def brokerPropertyOverrides(properties: Properties): Unit = {
    super.brokerPropertyOverrides(properties)
    properties.put(CREATE_TOPIC_POLICY_CLASS_NAME_CONFIG, classOf[Policy].getName)
  }

  override def kraftControllerConfigs(testInfo: TestInfo): Seq[Properties] = {
    val properties = new Properties()
    properties.put(CREATE_TOPIC_POLICY_CLASS_NAME_CONFIG, classOf[Policy].getName)
    Seq(properties)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testValidCreateTopicsRequests(quorum: String): Unit = {
    validateValidCreateTopicsRequests(topicsReq(Seq(topicReq("topic1",
      numPartitions = 5))))

    validateValidCreateTopicsRequests(topicsReq(Seq(topicReq("topic2",
      numPartitions = 5, replicationFactor = 3)),
      validateOnly = true))

    validateValidCreateTopicsRequests(topicsReq(Seq(topicReq("topic3",
      numPartitions = 11, replicationFactor = 2,
      config = Map(TopicConfig.RETENTION_MS_CONFIG -> 4999.toString))),
      validateOnly = true))

    validateValidCreateTopicsRequests(topicsReq(Seq(topicReq("topic4",
      assignment = Map(0 -> List(1, 0), 1 -> List(0, 1))))))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testErrorCreateTopicsRequests(quorum: String): Unit = {
    val existingTopic = "existing-topic"
    createTopic(existingTopic, 5)

    // Policy violations
    validateErrorCreateTopicsRequests(topicsReq(Seq(topicReq("policy-topic1",
      numPartitions = 4, replicationFactor = 1))),
      Map("policy-topic1" -> error(Errors.POLICY_VIOLATION, Some("Topics should have at least 5 partitions, received 4"))))

    validateErrorCreateTopicsRequests(topicsReq(Seq(topicReq("policy-topic2",
      numPartitions = 4, replicationFactor = 3)), validateOnly = true),
      Map("policy-topic2" -> error(Errors.POLICY_VIOLATION, Some("Topics should have at least 5 partitions, received 4"))))

    validateErrorCreateTopicsRequests(topicsReq(Seq(topicReq("policy-topic3",
      numPartitions = 11, replicationFactor = 2,
      config = Map(TopicConfig.RETENTION_MS_CONFIG -> 5001.toString))), validateOnly = true),
      Map("policy-topic3" -> error(Errors.POLICY_VIOLATION,
        Some("RetentionMs should be less than 5000ms if replicationFactor > 5"))))

    validateErrorCreateTopicsRequests(topicsReq(Seq(topicReq("policy-topic4",
      numPartitions = 11, replicationFactor = 3,
      config = Map(TopicConfig.RETENTION_MS_CONFIG -> 5001.toString))), validateOnly = true),
      Map("policy-topic4" -> error(Errors.POLICY_VIOLATION,
        Some("RetentionMs should be less than 5000ms if replicationFactor > 5"))))

    validateErrorCreateTopicsRequests(topicsReq(Seq(topicReq("policy-topic5",
      assignment = Map(0 -> List(1), 1 -> List(0)),
      config = Map(TopicConfig.RETENTION_MS_CONFIG -> 5001.toString))), validateOnly = true),
      Map("policy-topic5" -> error(Errors.POLICY_VIOLATION,
        Some("Topic partitions should have at least 2 partitions, received 1 for partition 0"))))

    // Check that basic errors still work
    validateErrorCreateTopicsRequests(topicsReq(Seq(topicReq(existingTopic,
      numPartitions = 5, replicationFactor = 1))),
      Map(existingTopic -> error(Errors.TOPIC_ALREADY_EXISTS,
        Some("Topic 'existing-topic' already exists."))))

    var errorMsg = "Unable to replicate the partition 4 time(s): The target replication factor of 4 cannot be reached because only 3 broker(s) are registered."
    validateErrorCreateTopicsRequests(topicsReq(Seq(topicReq("error-replication",
      numPartitions = 10, replicationFactor = brokerCount + 1)), validateOnly = true),
      Map("error-replication" -> error(Errors.INVALID_REPLICATION_FACTOR,
        Some(errorMsg))))

    errorMsg = "Replication factor must be larger than 0, or -1 to use the default value."
    validateErrorCreateTopicsRequests(topicsReq(Seq(topicReq("error-replication2",
      numPartitions = 10, replicationFactor = -2)), validateOnly = true),
      Map("error-replication2" -> error(Errors.INVALID_REPLICATION_FACTOR,
        Some(errorMsg))))

    errorMsg = "Number of partitions was set to an invalid non-positive value."
    validateErrorCreateTopicsRequests(topicsReq(Seq(topicReq("error-partitions",
      numPartitions = -2, replicationFactor = 1)), validateOnly = true),
      Map("error-partitions" -> error(Errors.INVALID_PARTITIONS,
        Some(errorMsg))))
  }

}

object CreateTopicsRequestWithPolicyTest {

  class Policy extends CreateTopicPolicy {

    var configs: Map[String, _] = _
    var closed = false

    def configure(configs: util.Map[String, _]): Unit = {
      this.configs = configs.asScala.toMap
    }

    def validate(requestMetadata: RequestMetadata): Unit = {
      if (Topic.isInternal(requestMetadata.topic())) {
        // Do not verify internal topics
        return
      }
      require(!closed, "Policy should not be closed")
      require(configs.nonEmpty, "configure should have been called with non empty configs")

      import requestMetadata._
      if (numPartitions != null || replicationFactor != null) {
        require(numPartitions != null, s"numPartitions should not be null, but it is $numPartitions")
        require(replicationFactor != null, s"replicationFactor should not be null, but it is $replicationFactor")
        require(replicasAssignments == null, s"replicaAssignments should be null, but it is $replicasAssignments")

        if (numPartitions < 5)
          throw new PolicyViolationException(s"Topics should have at least 5 partitions, received $numPartitions")

        if (numPartitions > 10) {
          if (requestMetadata.configs.asScala.get(TopicConfig.RETENTION_MS_CONFIG).fold(true)(_.toInt > 5000))
            throw new PolicyViolationException("RetentionMs should be less than 5000ms if replicationFactor > 5")
        } else
          require(requestMetadata.configs.isEmpty, s"Topic configs should be empty, but it is ${requestMetadata.configs}")

      } else {
        require(numPartitions == null, s"numPartitions should be null, but it is $numPartitions")
        require(replicationFactor == null, s"replicationFactor should be null, but it is $replicationFactor")
        require(replicasAssignments != null, s"replicaAssignments should not be null, but it is $replicasAssignments")

        replicasAssignments.asScala.toSeq.sortBy { case (tp, _) => tp }.foreach { case (partitionId, assignment) =>
          if (assignment.size < 2)
            throw new PolicyViolationException("Topic partitions should have at least 2 partitions, received " +
              s"${assignment.size} for partition $partitionId")
        }
      }

    }

    def close(): Unit = closed = true

  }
}
