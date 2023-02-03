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

import kafka.utils.TestInfoUtils

import java.util.{Collections, Properties}
import java.util.stream.{Stream => JStream}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTransaction
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTransactionCollection
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTopicCollection
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{AddPartitionsToTxnRequest, AddPartitionsToTxnResponse}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, TestInfo}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class AddPartitionsToTxnRequestServerTest extends BaseRequestTest {
  private val topic1 = "topic1"
  val numPartitions = 1

  override def brokerPropertyOverrides(properties: Properties): Unit =
    properties.put(KafkaConfig.AutoCreateTopicsEnableProp, false.toString)

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    createTopic(topic1, numPartitions, brokers.size, new Properties())
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @MethodSource(value = Array("parameters"))
  def shouldReceiveOperationNotAttemptedWhenOtherPartitionHasError(quorum: String, version: Short): Unit = {
    // The basic idea is that we have one unknown topic and one created topic. We should get the 'UNKNOWN_TOPIC_OR_PARTITION'
    // error for the unknown topic and the 'OPERATION_NOT_ATTEMPTED' error for the known and authorized topic.
    val nonExistentTopic = new TopicPartition("unknownTopic", 0)
    val createdTopicPartition = new TopicPartition(topic1, 0)

    val transactionalId = "foobar"
    val producerId = 1000L
    val producerEpoch: Short = 0

    val request =
      if (version < 4) {
        new AddPartitionsToTxnRequest.Builder(
          transactionalId,
          producerId,
          producerEpoch,
          List(createdTopicPartition, nonExistentTopic).asJava)
          .build()
      } else {
        val topics = new AddPartitionsToTxnTopicCollection()
        topics.add(new AddPartitionsToTxnTopic()
          .setName(createdTopicPartition.topic())
          .setPartitions(Collections.singletonList(createdTopicPartition.partition())))
        topics.add(new AddPartitionsToTxnTopic()
          .setName(nonExistentTopic.topic())
          .setPartitions(Collections.singletonList(nonExistentTopic.partition())))

        val transactions = new AddPartitionsToTxnTransactionCollection()
        transactions.add(new AddPartitionsToTxnTransaction()
          .setTransactionalId(transactionalId)
          .setProducerId(producerId)
          .setProducerEpoch(producerEpoch)
          .setTopics(topics))
        new AddPartitionsToTxnRequest.Builder(transactions, false).build()
      }

    val leaderId = brokers.head.config.brokerId
    val response = connectAndReceive[AddPartitionsToTxnResponse](request, brokerSocketServer(leaderId))
    
    val errors = if (version < 4) response.errors else response.errorsPerTransaction(transactionalId)
    
    assertEquals(2, errors.size)

    assertTrue(errors.containsKey(createdTopicPartition))
    assertEquals(Errors.OPERATION_NOT_ATTEMPTED, errors.get(createdTopicPartition))

    assertTrue(errors.containsKey(nonExistentTopic))
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, errors.get(nonExistentTopic))
  }
}

object AddPartitionsToTxnRequestServerTest {
   def parameters: JStream[Arguments] = {
    val arguments = mutable.ListBuffer[Arguments]()
    ApiKeys.ADD_PARTITIONS_TO_TXN.allVersions().forEach( version =>
      Array("kraft", "zk").foreach( quorum =>
        arguments += Arguments.of(quorum, version)
      )
    )
    arguments.asJava.stream()
  }
}
