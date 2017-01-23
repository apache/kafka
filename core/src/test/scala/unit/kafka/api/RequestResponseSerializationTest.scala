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

package kafka.api

import kafka.common.{OffsetAndMetadata, OffsetMetadataAndError}
import kafka.common._
import kafka.message.{ByteBufferMessageSet, Message}
import kafka.common.TopicAndPartition
import kafka.utils.TestUtils
import TestUtils.createBroker
import java.nio.ByteBuffer

import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{Errors, SecurityProtocol}
import org.apache.kafka.common.utils.Time
import org.junit._
import org.scalatest.junit.JUnitSuite
import org.junit.Assert._


object SerializationTestUtils {
  private val topic1 = "test1"
  private val topic2 = "test2"
  private val partitionDataFetchResponse0 = new FetchResponsePartitionData(messages = new ByteBufferMessageSet(new Message("first message".getBytes)))
  private val partitionDataFetchResponse1 = new FetchResponsePartitionData(messages = new ByteBufferMessageSet(new Message("second message".getBytes)))
  private val partitionDataFetchResponse2 = new FetchResponsePartitionData(messages = new ByteBufferMessageSet(new Message("third message".getBytes)))
  private val partitionDataFetchResponse3 = new FetchResponsePartitionData(messages = new ByteBufferMessageSet(new Message("fourth message".getBytes)))
  private val partitionDataFetchResponseMap = Map((0, partitionDataFetchResponse0), (1, partitionDataFetchResponse1), (2, partitionDataFetchResponse2), (3, partitionDataFetchResponse3))

  private val topicDataFetchResponse = {
    val groupedData = Array(topic1, topic2).flatMap(topic =>
      partitionDataFetchResponseMap.map(partitionAndData =>
        (TopicAndPartition(topic, partitionAndData._1), partitionAndData._2)))
    collection.immutable.Map(groupedData:_*)
  }

  private val partitionDataMessage0 = new ByteBufferMessageSet(new Message("first message".getBytes))
  private val partitionDataMessage1 = new ByteBufferMessageSet(new Message("second message".getBytes))
  private val partitionDataMessage2 = new ByteBufferMessageSet(new Message("third message".getBytes))
  private val partitionDataMessage3 = new ByteBufferMessageSet(new Message("fourth message".getBytes))
  private val partitionDataProducerRequestArray = Array(partitionDataMessage0, partitionDataMessage1, partitionDataMessage2, partitionDataMessage3)

  val topicDataProducerRequest = {
    val groupedData = Array(topic1, topic2).flatMap(topic =>
      partitionDataProducerRequestArray.zipWithIndex.map
      {
        case(partitionDataMessage, partition) =>
          (TopicAndPartition(topic, partition), partitionDataMessage)
      })
    collection.mutable.Map(groupedData:_*)
  }

  private val requestInfos = collection.immutable.Map(
    TopicAndPartition(topic1, 0) -> PartitionFetchInfo(1000, 100),
    TopicAndPartition(topic1, 1) -> PartitionFetchInfo(2000, 100),
    TopicAndPartition(topic1, 2) -> PartitionFetchInfo(3000, 100),
    TopicAndPartition(topic1, 3) -> PartitionFetchInfo(4000, 100),
    TopicAndPartition(topic2, 0) -> PartitionFetchInfo(1000, 100),
    TopicAndPartition(topic2, 1) -> PartitionFetchInfo(2000, 100),
    TopicAndPartition(topic2, 2) -> PartitionFetchInfo(3000, 100),
    TopicAndPartition(topic2, 3) -> PartitionFetchInfo(4000, 100)
  )

  private val brokers = List(createBroker(0, "localhost", 1011), createBroker(0, "localhost", 1012),
    createBroker(0, "localhost", 1013))

  def createTestProducerRequest: ProducerRequest = {
    new ProducerRequest(1, "client 1", 0, 1000, topicDataProducerRequest)
  }

  def createTestProducerResponse: ProducerResponse =
    ProducerResponse(1, Map(
      TopicAndPartition(topic1, 0) -> ProducerResponseStatus(0.toShort, 10001),
      TopicAndPartition(topic2, 0) -> ProducerResponseStatus(0.toShort, 20001)
    ), ProducerRequest.CurrentVersion, 100)

  def createTestFetchRequest: FetchRequest = new FetchRequest(requestInfo = requestInfos.toVector)

  def createTestFetchResponse: FetchResponse = FetchResponse(1, topicDataFetchResponse.toVector)

  def createTestOffsetRequest = new OffsetRequest(
      collection.immutable.Map(TopicAndPartition(topic1, 1) -> PartitionOffsetRequestInfo(1000, 200)),
      replicaId = 0
  )

  def createTestOffsetResponse: OffsetResponse = {
    new OffsetResponse(0, collection.immutable.Map(
      TopicAndPartition(topic1, 1) -> PartitionOffsetsResponse(Errors.NONE.code, Seq(1000l, 2000l, 3000l, 4000l)))
    )
  }

  def createTestOffsetCommitRequestV2: OffsetCommitRequest = {
    new OffsetCommitRequest(
      groupId = "group 1",
      retentionMs = Time.SYSTEM.milliseconds,
      requestInfo=collection.immutable.Map(
      TopicAndPartition(topic1, 0) -> OffsetAndMetadata(42L, "some metadata"),
      TopicAndPartition(topic1, 1) -> OffsetAndMetadata(100L, OffsetMetadata.NoMetadata)
    ))
  }

  def createTestOffsetCommitRequestV1: OffsetCommitRequest = {
    new OffsetCommitRequest(
      versionId = 1,
      groupId = "group 1",
      requestInfo = collection.immutable.Map(
      TopicAndPartition(topic1, 0) -> OffsetAndMetadata(42L, "some metadata", Time.SYSTEM.milliseconds),
      TopicAndPartition(topic1, 1) -> OffsetAndMetadata(100L, OffsetMetadata.NoMetadata, Time.SYSTEM.milliseconds)
    ))
  }

  def createTestOffsetCommitRequestV0: OffsetCommitRequest = {
    new OffsetCommitRequest(
      versionId = 0,
      groupId = "group 1",
      requestInfo = collection.immutable.Map(
        TopicAndPartition(topic1, 0) -> OffsetAndMetadata(42L, "some metadata"),
        TopicAndPartition(topic1, 1) -> OffsetAndMetadata(100L, OffsetMetadata.NoMetadata)
      ))
  }

  def createTestOffsetCommitResponse: OffsetCommitResponse = {
    new OffsetCommitResponse(collection.immutable.Map(TopicAndPartition(topic1, 0) -> Errors.NONE.code,
                                 TopicAndPartition(topic1, 1) -> Errors.NONE.code))
  }

  def createTestOffsetFetchRequest: OffsetFetchRequest = {
    new OffsetFetchRequest("group 1", Seq(
      TopicAndPartition(topic1, 0),
      TopicAndPartition(topic1, 1)
    ))
  }

  def createTestOffsetFetchResponse: OffsetFetchResponse = {
    new OffsetFetchResponse(collection.immutable.Map(
      TopicAndPartition(topic1, 0) -> OffsetMetadataAndError(42L, "some metadata", Errors.NONE.code),
      TopicAndPartition(topic1, 1) -> OffsetMetadataAndError(100L, OffsetMetadata.NoMetadata, Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
    ), errorCode = Errors.NONE.code)
  }

  def createConsumerMetadataRequest: GroupCoordinatorRequest = GroupCoordinatorRequest("group 1", clientId = "client 1")

  def createConsumerMetadataResponse: GroupCoordinatorResponse = {
    GroupCoordinatorResponse(Some(
      brokers.head.getBrokerEndPoint(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))), Errors.NONE.code, 0)
  }
}

class RequestResponseSerializationTest extends JUnitSuite {
  private val producerRequest = SerializationTestUtils.createTestProducerRequest
  private val producerResponse = SerializationTestUtils.createTestProducerResponse
  private val fetchRequest = SerializationTestUtils.createTestFetchRequest
  private val offsetRequest = SerializationTestUtils.createTestOffsetRequest
  private val offsetResponse = SerializationTestUtils.createTestOffsetResponse
  private val offsetCommitRequestV0 = SerializationTestUtils.createTestOffsetCommitRequestV0
  private val offsetCommitRequestV1 = SerializationTestUtils.createTestOffsetCommitRequestV1
  private val offsetCommitRequestV2 = SerializationTestUtils.createTestOffsetCommitRequestV2
  private val offsetCommitResponse = SerializationTestUtils.createTestOffsetCommitResponse
  private val offsetFetchRequest = SerializationTestUtils.createTestOffsetFetchRequest
  private val offsetFetchResponse = SerializationTestUtils.createTestOffsetFetchResponse
  private val consumerMetadataRequest = SerializationTestUtils.createConsumerMetadataRequest
  private val consumerMetadataResponse = SerializationTestUtils.createConsumerMetadataResponse
  private val consumerMetadataResponseNoCoordinator = GroupCoordinatorResponse(None, ErrorMapping.ConsumerCoordinatorNotAvailableCode, 0)

  @Test
  def testSerializationAndDeserialization() {

    val requestsAndResponses =
      collection.immutable.Seq(producerRequest, producerResponse,
                               fetchRequest, offsetRequest, offsetResponse,
                               offsetCommitRequestV0, offsetCommitRequestV1, offsetCommitRequestV2, offsetCommitResponse,
                               offsetFetchRequest, offsetFetchResponse,
                               consumerMetadataRequest, consumerMetadataResponse,
                               consumerMetadataResponseNoCoordinator)

    requestsAndResponses.foreach { original =>
      val buffer = ByteBuffer.allocate(original.sizeInBytes)
      original.writeTo(buffer)
      buffer.rewind()
      val deserializer = original.getClass.getDeclaredMethod("readFrom", classOf[ByteBuffer])
      val deserialized = deserializer.invoke(null, buffer)
      assertFalse("All serialized bytes in " + original.getClass.getSimpleName + " should have been consumed",
                  buffer.hasRemaining)
      assertEquals("The original and deserialized for " + original.getClass.getSimpleName + " should be the same.", original, deserialized)
    }
  }

  @Test
  def testProduceResponseVersion() {
    val oldClientResponse = ProducerResponse(1, Map(
      TopicAndPartition("t1", 0) -> ProducerResponseStatus(0.toShort, 10001),
      TopicAndPartition("t2", 0) -> ProducerResponseStatus(0.toShort, 20001)
    ))

    val newClientResponse = ProducerResponse(1, Map(
      TopicAndPartition("t1", 0) -> ProducerResponseStatus(0.toShort, 10001),
      TopicAndPartition("t2", 0) -> ProducerResponseStatus(0.toShort, 20001)
    ), 1, 100)

    // new response should have 4 bytes more than the old response since delayTime is an INT32
    assertEquals(oldClientResponse.sizeInBytes + 4, newClientResponse.sizeInBytes)

    val buffer = ByteBuffer.allocate(newClientResponse.sizeInBytes)
    newClientResponse.writeTo(buffer)
    buffer.rewind()
    assertEquals(ProducerResponse.readFrom(buffer).throttleTime, 100)
  }

  @Test
  def testFetchResponseVersion() {
    val oldClientResponse = FetchResponse(1, Map(
      TopicAndPartition("t1", 0) -> new FetchResponsePartitionData(messages = new ByteBufferMessageSet(new Message("first message".getBytes)))
    ).toVector, 0)

    val newClientResponse = FetchResponse(1, Map(
      TopicAndPartition("t1", 0) -> new FetchResponsePartitionData(messages = new ByteBufferMessageSet(new Message("first message".getBytes)))
    ).toVector, 1, 100)

    // new response should have 4 bytes more than the old response since delayTime is an INT32
    assertEquals(oldClientResponse.sizeInBytes + 4, newClientResponse.sizeInBytes)
  }

}
