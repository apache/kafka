/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import kafka.api.KAFKA_0_10_2_IV0
import kafka.log.LogConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import java.util.Properties

import scala.annotation.nowarn
import scala.collection.Seq
import scala.jdk.CollectionConverters._

class FetchRequestWithLegacyMessageFormatTest extends BaseFetchRequestTest {

  override def brokerPropertyOverrides(properties: Properties): Unit = {
    super.brokerPropertyOverrides(properties)
    // legacy message formats are only supported with IBP < 3.0
    properties.put(KafkaConfig.InterBrokerProtocolVersionProp, "2.8")
  }

  /**
   * Fetch request v2 (pre KIP-74) respected `maxPartitionBytes` even if no message could be returned
   * due to a message that was larger than `maxPartitionBytes`.
   */
  @nowarn("cat=deprecation")
  @Test
  def testFetchRequestV2WithOversizedMessage(): Unit = {
    initProducer()
    val maxPartitionBytes = 200
    // Fetch v2 down-converts if the message format is >= 0.11 and we want to avoid
    // that as it affects the size of the returned buffer
    val topicConfig = Map(LogConfig.MessageFormatVersionProp -> KAFKA_0_10_2_IV0.version)
    val (topicPartition, leaderId) = createTopics(numTopics = 1, numPartitions = 1, topicConfig).head
    val topicIds = getTopicIds().asJava
    val topicNames = topicIds.asScala.map(_.swap).asJava
    producer.send(new ProducerRecord(topicPartition.topic, topicPartition.partition,
      "key", new String(new Array[Byte](maxPartitionBytes + 1)))).get
    val fetchVersion: Short = 2
    val fetchRequest = FetchRequest.Builder.forConsumer(fetchVersion, Int.MaxValue, 0,
      createPartitionMap(maxPartitionBytes, Seq(topicPartition))).build(fetchVersion)
    val fetchResponse = sendFetchRequest(leaderId, fetchRequest)
    val partitionData = fetchResponse.responseData(topicNames, fetchVersion).get(topicPartition)
    assertEquals(Errors.NONE.code, partitionData.errorCode)

    assertTrue(partitionData.highWatermark > 0)
    assertEquals(maxPartitionBytes, FetchResponse.recordsSize(partitionData))
    assertEquals(0, records(partitionData).map(_.sizeInBytes).sum)
  }

}
