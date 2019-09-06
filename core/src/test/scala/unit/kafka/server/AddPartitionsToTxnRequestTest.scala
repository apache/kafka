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
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{AddPartitionsToTxnRequest, AddPartitionsToTxnResponse}
import org.junit.{Before, Test}
import org.junit.Assert._

import scala.collection.JavaConverters._

class AddPartitionsToTxnRequestTest extends BaseRequestTest {
  private val topic1 = "foobartopic"
  val numPartitions = 3

  override def brokerPropertyOverrides(properties: Properties): Unit =
    properties.put(KafkaConfig.AutoCreateTopicsEnableProp, false.toString)

  @Before
  override def setUp(): Unit = {
    super.setUp()
    createTopic(topic1, numPartitions, servers.size, new Properties())
  }

  @Test
  def shouldReceiveOperationNotAttemptedWhenOtherPartitionHasError(): Unit = {
    // The basic idea is that we have one unknown topic and one created topic. We should get the 'UNKNOWN_TOPIC_OR_PARTITION'
    // error for the unknown topic and the 'OPERATION_NOT_ATTEMPTED' error for the known and authorized topic.
    val nonExistentTopic = new TopicPartition("unknownTopic", 0)
    val createdTopicPartition = new TopicPartition(topic1, 0)

    val request = createRequest(List(createdTopicPartition, nonExistentTopic))
    val leaderId = servers.head.config.brokerId
    val response = sendAddPartitionsRequest(leaderId, request)

    assertEquals(2, response.errors.size)

    assertTrue(response.errors.containsKey(createdTopicPartition))
    assertEquals(Errors.OPERATION_NOT_ATTEMPTED, response.errors.get(createdTopicPartition))

    assertTrue(response.errors.containsKey(nonExistentTopic))
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, response.errors.get(nonExistentTopic))
  }

  private def sendAddPartitionsRequest(leaderId: Int, request: AddPartitionsToTxnRequest) : AddPartitionsToTxnResponse = {
    val response = connectAndSend(request, ApiKeys.ADD_PARTITIONS_TO_TXN, destination = brokerSocketServer(leaderId))
    AddPartitionsToTxnResponse.parse(response, request.version)
  }

  private def createRequest(partitions: List[TopicPartition]): AddPartitionsToTxnRequest = {
    val transactionalId = "foobar"
    val producerId = 1000L
    val producerEpoch: Short = 0
    val builder = new AddPartitionsToTxnRequest.Builder(transactionalId, producerId, producerEpoch, partitions.asJava)
    builder.build()
  }
}
