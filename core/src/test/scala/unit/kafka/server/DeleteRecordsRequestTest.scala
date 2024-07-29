/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.DeleteRecordsRequestData
import org.apache.kafka.common.message.DeleteRecordsRequestData.{DeleteRecordsPartition, DeleteRecordsTopic}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{DeleteRecordsRequest, DeleteRecordsResponse}
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util.Collections
import java.util.concurrent.TimeUnit
import scala.collection.Seq

class DeleteRecordsRequestTest extends BaseRequestTest {
  private val TIMEOUT_MS = 1000
  private val MESSAGES_PRODUCED_PER_PARTITION = 10

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testDeleteRecordsHappyCase(quorum: String): Unit = {
    val (topicPartition: TopicPartition, leaderId: Int) = createTopicAndSendRecords

    // Create the DeleteRecord request requesting deletion of offset which is not present
    val offsetToDelete = Math.max(MESSAGES_PRODUCED_PER_PARTITION - 8, 0)
    val request: DeleteRecordsRequest = createDeleteRecordsRequestForTopicPartition(topicPartition, offsetToDelete)

    // call the API
    val response = sendDeleteRecordsRequest(request, leaderId)
    val partitionResult = response.data.topics.find(topicPartition.topic).partitions.find(topicPartition.partition)

    // Validate the expected error code in the response
    assertEquals(Errors.NONE.code(), partitionResult.errorCode(),
      s"Unexpected error code received: ${Errors.forCode(partitionResult.errorCode).name()}")

    // Validate the expected lowWaterMark in the response
    assertEquals(offsetToDelete, partitionResult.lowWatermark(),
      s"Unexpected lowWatermark received: ${partitionResult.lowWatermark}")

    // Validate that the records have actually deleted
    validateLogStartOffsetForTopic(topicPartition, offsetToDelete)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testErrorWhenDeletingRecordsWithInvalidOffset(quorum: String): Unit = {
    val (topicPartition: TopicPartition, leaderId: Int) = createTopicAndSendRecords

    // Create the DeleteRecord request requesting deletion of offset which is not present
    val offsetToDelete = MESSAGES_PRODUCED_PER_PARTITION + 5
    val request: DeleteRecordsRequest = createDeleteRecordsRequestForTopicPartition(topicPartition, offsetToDelete)

    // call the API
    val response = sendDeleteRecordsRequest(request, leaderId)
    val partitionResult = response.data.topics.find(topicPartition.topic).partitions.find(topicPartition.partition)

    // Validate the expected error code in the response
    assertEquals(Errors.OFFSET_OUT_OF_RANGE.code(), partitionResult.errorCode(),
      s"Unexpected error code received: ${Errors.forCode(partitionResult.errorCode()).name()}")

    // Validate the expected value for low watermark
    assertEquals(DeleteRecordsResponse.INVALID_LOW_WATERMARK, partitionResult.lowWatermark())

    // After error, the offset of the topic should have been the original i.e. delete record should not have deleted
    // records.
    validateLogStartOffsetForTopic(topicPartition, 0)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testErrorWhenDeletingRecordsWithInvalidTopic(quorum: String): Unit = {
    val invalidTopicPartition = new TopicPartition("invalid-topic", 0)
    // Create the DeleteRecord request requesting deletion of offset which is not present
    val offsetToDelete = 1
    val request: DeleteRecordsRequest = createDeleteRecordsRequestForTopicPartition(invalidTopicPartition, offsetToDelete)

    // call the API
    val response = sendDeleteRecordsRequest(request)
    val partitionResult = response.data.topics.find(invalidTopicPartition.topic).partitions.find(invalidTopicPartition.partition)

    // Validate the expected error code in the response
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), partitionResult.errorCode(),
      s"Unexpected error code received: ${Errors.forCode(partitionResult.errorCode()).name()}")

    // Validate the expected value for low watermark
    assertEquals(DeleteRecordsResponse.INVALID_LOW_WATERMARK, partitionResult.lowWatermark())
  }

  private def createTopicAndSendRecords = {
    // Single topic
    val topic1 = "topic-1"
    val topicPartition = new TopicPartition(topic1, 0)
    val partitionToLeader = createTopic(topic1)
    assertTrue(partitionToLeader.contains(topicPartition.partition), "Topic creation did not succeed.")
    // Write records
    produceData(Seq(topicPartition), MESSAGES_PRODUCED_PER_PARTITION)
    (topicPartition, partitionToLeader(topicPartition.partition))
  }

  private def createDeleteRecordsRequestForTopicPartition(topicPartition: TopicPartition, offsetToDelete: Int) = {
    val requestData = new DeleteRecordsRequestData()
      .setTopics(Collections.singletonList(new DeleteRecordsTopic()
        .setName(topicPartition.topic())
        .setPartitions(Collections.singletonList(new DeleteRecordsPartition()
          .setOffset(offsetToDelete)
          .setPartitionIndex(topicPartition.partition())))))
      .setTimeoutMs(TIMEOUT_MS)
    val request = new DeleteRecordsRequest.Builder(requestData).build()
    request
  }

  private def sendDeleteRecordsRequest(request: DeleteRecordsRequest): DeleteRecordsResponse = {
    connectAndReceive[DeleteRecordsResponse](request, destination = anySocketServer)
  }

  private def sendDeleteRecordsRequest(request: DeleteRecordsRequest, leaderId: Int): DeleteRecordsResponse = {
    connectAndReceive[DeleteRecordsResponse](request, destination = brokerSocketServer(leaderId))
  }

  private def produceData(topicPartitions: Iterable[TopicPartition], numMessagesPerPartition: Int): Seq[RecordMetadata] = {
    val producer = createProducer(keySerializer = new StringSerializer, valueSerializer = new StringSerializer)
    val records = for {
      tp <- topicPartitions.toSeq
      messageIndex <- 0 until numMessagesPerPartition
    } yield {
      val suffix = s"$tp-$messageIndex"
      new ProducerRecord(tp.topic, tp.partition, s"key $suffix", s"value $suffix")
    }

    val sendfutureList = records.map(producer.send)

    // ensure that records are flushed to server
    producer.flush()

    val recordMetadataList = sendfutureList.map(_.get(10, TimeUnit.SECONDS))
    recordMetadataList
      .foreach(recordMetadata => assertTrue(recordMetadata.offset >= 0, s"Invalid offset $recordMetadata"))

    recordMetadataList
  }

  private def validateLogStartOffsetForTopic(topicPartition: TopicPartition, expectedStartOffset: Long): Unit = {
    val logForTopicPartition = brokers.flatMap(_.replicaManager.logManager.getLog(topicPartition)).headOption
    // logManager should exist for the provided partition
    assertTrue(logForTopicPartition.isDefined)
    // assert that log start offset is equal to the expectedStartOffset after DeleteRecords has been called.
    assertEquals(expectedStartOffset, logForTopicPartition.get.logStartOffset)
  }
}
