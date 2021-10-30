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

import java.util.Properties

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AddPartitionsToTxnRequest, AddPartitionsToTxnResponse}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Test, TestInfo}

import scala.jdk.CollectionConverters._

class AddPartitionsToTxnRequestServerTest extends BaseRequestTest {
  private val topic1 = "topic1"
  val numPartitions = 1

  override def brokerPropertyOverrides(properties: Properties): Unit =
    properties.put(KafkaConfig.AutoCreateTopicsEnableProp, false.toString)

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    createTopic(topic1, numPartitions, servers.size, new Properties())
  }

  @Test
  def shouldReceiveOperationNotAttemptedWhenOtherPartitionHasError(): Unit = {
    // The basic idea is that we have one unknown topic and one created topic. We should get the 'UNKNOWN_TOPIC_OR_PARTITION'
    // error for the unknown topic and the 'OPERATION_NOT_ATTEMPTED' error for the known and authorized topic.
    val nonExistentTopic = new TopicPartition("unknownTopic", 0)
    val createdTopicPartition = new TopicPartition(topic1, 0)

    val transactionalId = "foobar"
    val producerId = 1000L
    val producerEpoch: Short = 0

    val request = new AddPartitionsToTxnRequest.Builder(
      transactionalId,
      producerId,
      producerEpoch,
      List(createdTopicPartition, nonExistentTopic).asJava)
      .build()

    val leaderId = servers.head.config.brokerId
    val response = connectAndReceive[AddPartitionsToTxnResponse](request, brokerSocketServer(leaderId))

    assertEquals(2, response.errors.size)

    assertTrue(response.errors.containsKey(createdTopicPartition))
    assertEquals(Errors.OPERATION_NOT_ATTEMPTED, response.errors.get(createdTopicPartition))

    assertTrue(response.errors.containsKey(nonExistentTopic))
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, response.errors.get(nonExistentTopic))
  }
}
