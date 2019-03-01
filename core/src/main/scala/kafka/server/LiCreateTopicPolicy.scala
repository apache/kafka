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

import org.apache.kafka.common.errors.PolicyViolationException
import org.apache.kafka.server.policy.CreateTopicPolicy

class LiCreateTopicPolicy extends CreateTopicPolicy {
  var minRf = 0

  @throws[PolicyViolationException]
  override def validate(requestMetadata: CreateTopicPolicy.RequestMetadata): Unit = {
    val requestTopic = requestMetadata.topic
    val requestRF = requestMetadata.replicationFactor()

    // For scala 2.11.x, a scala object converted from a null java object via asScala is NOT null. This could lead to
    // NPE, thus we check the java object directly. AdminClientIntegrationTest verifies the behavior.
    if (requestMetadata.replicasAssignments() == null && requestRF == null)
      throw new PolicyViolationException(s"Topic [$requestTopic] is missing both replica assignment and " +
        s"replication factor.")

    // In createTopics() in AdminManager, replicationFactor and replicasAssignments are not both set at same time. We
    // follow the same rationale here and prioritize replicasAssignments over replicationFactor
    if (requestMetadata.replicasAssignments() != null) {
      import collection.JavaConverters._
      requestMetadata.replicasAssignments().asScala.foreach { case (p, assignment) =>
          if (assignment.size() < minRf)
            throw new PolicyViolationException(s"Topic [$requestTopic] fails RF requirement. Received RF for " +
              s"[partition-$p]: ${assignment.size()}, min required RF: $minRf.")
      }
    } else if (requestRF < minRf) {
      throw new PolicyViolationException(s"Topic [$requestTopic] fails RF requirement. " +
        s"Received RF: ${requestMetadata.replicationFactor}, min required RF: $minRf.")
    }
  }

  /**
    * Configure this class with the given key-value pairs
    */
  override def configure(configs: java.util.Map[String, _]): Unit = {
    minRf = configs.get(KafkaConfig.DefaultReplicationFactorProp).asInstanceOf[String].toInt
  }

  @throws[Exception]
  override def close(): Unit = {
  }
}
