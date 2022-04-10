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

import org.apache.kafka.common.requests.MetadataRequest
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata
import org.apache.kafka.server.policy.CreateTopicBrokerFilterPolicy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.{assertTrue, assertEquals}

import scala.jdk.CollectionConverters._

class CreateTopicsRequestWithBrokerFilterTest extends AbstractCreateTopicsRequestTest {
  import kafka.server.CreateTopicsRequestWithBrokerFilterTest._

  override def brokerPropertyOverrides(properties: Properties): Unit = {
    super.brokerPropertyOverrides(properties)
    properties.put(KafkaConfig.CreateTopicBrokerFilterPolicyClassNameProp, classOf[EvenBrokersOnlyPolicy].getName)
  }

  @Test
  def testNewTopicPartitionPlacement(): Unit = {
    validateValidCreateTopicsRequests(topicsReq(Seq(topicReq("topic1", numPartitions = 6))))
    validateTopicPartitionsOnEvenBrokers("topic1")
  }

  protected def validateTopicPartitionsOnEvenBrokers(topicName: String): Unit = {
    validateTopicPartitionPlacement(topicName, partitionMeta => {
      partitionMeta.replicaIds.asScala.foreach { replicaId =>
        assertTrue(isEven(replicaId), s"Topic ${topicName} on odd replica: ${replicaId}")
      }
    })
  }

  protected def validateTopicPartitionPlacement(topicName: String, predicate: PartitionMetadata => Unit): Unit = {
    val metadata = sendMetadataRequest(
      new MetadataRequest.Builder(List(topicName).asJava, true).build()).topicMetadata.asScala

    val metadataForTopic = metadata.filter(_.topic == topicName).head

    metadataForTopic.partitionMetadata().asScala.foreach { partitionMeta => {
      predicate(partitionMeta)
    }}
  }
}

// test default case: no custom filter is configured
class CreateTopicsRequestWithoutBrokerFilterTest extends AbstractCreateTopicsRequestTest {
  @Test
  def testNewTopicPartitionPlacement(): Unit = {
    validateValidCreateTopicsRequests(topicsReq(Seq(topicReq("topic1", numPartitions = 6))))
    validateTopicPartitionsOnAllBrokers("topic1")
  }

  protected def validateTopicPartitionsOnAllBrokers(topicName: String): Unit = {
    val metadata = sendMetadataRequest(
      new MetadataRequest.Builder(List(topicName).asJava, true).build()).topicMetadata.asScala

    val metadataForTopic = metadata.filter(_.topic == topicName).head

    val brokersWithReplicas = metadataForTopic.partitionMetadata().asScala.flatMap { partitionMeta => {
      partitionMeta.replicaIds.asScala
    }}.toSet
    val allBrokerIds = 0.until(brokerCount).toSet

    assertEquals(allBrokerIds, brokersWithReplicas,
      s"Partitions for ${topicName} are only on brokers ${brokersWithReplicas} instead of ${allBrokerIds}")
  }
}

object CreateTopicsRequestWithBrokerFilterTest {
  def isEven(num: Int): Boolean = num % 2 == 0

  // a silly implementation for the unit test: only allows partitions to be placed on brokers with even IDs
  class EvenBrokersOnlyPolicy extends CreateTopicBrokerFilterPolicy {
    override def isAllowedToHostPartitions(broker: CreateTopicBrokerFilterPolicy.Broker): Boolean =
      isEven(broker.getBrokerId)

    override def configure(configs: util.Map[String, _]): Unit = {}

    override def close(): Unit = {}
  }
}
