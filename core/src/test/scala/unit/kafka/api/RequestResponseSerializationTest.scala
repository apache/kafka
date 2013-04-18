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

import org.junit._
import org.scalatest.junit.JUnitSuite
import junit.framework.Assert._
import java.nio.ByteBuffer
import kafka.message.{Message, ByteBufferMessageSet}
import kafka.cluster.Broker
import collection.mutable._
import kafka.common.{TopicAndPartition, ErrorMapping, OffsetMetadataAndError}
import kafka.controller.LeaderIsrAndControllerEpoch


object SerializationTestUtils{
  private val topic1 = "test1"
  private val topic2 = "test2"
  private val leader1 = 0
  private val isr1 = List(0, 1, 2)
  private val leader2 = 0
  private val isr2 = List(0, 2, 3)
  private val partitionDataFetchResponse0 = new FetchResponsePartitionData(new ByteBufferMessageSet(new Message("first message".getBytes)))
  private val partitionDataFetchResponse1 = new FetchResponsePartitionData(new ByteBufferMessageSet(new Message("second message".getBytes)))
  private val partitionDataFetchResponse2 = new FetchResponsePartitionData(new ByteBufferMessageSet(new Message("third message".getBytes)))
  private val partitionDataFetchResponse3 = new FetchResponsePartitionData(new ByteBufferMessageSet(new Message("fourth message".getBytes)))
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

  private val topicDataProducerRequest = {
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

  private val brokers = List(new Broker(0, "localhost", 1011), new Broker(1, "localhost", 1012), new Broker(2, "localhost", 1013))
  private val partitionMetaData0 = new PartitionMetadata(0, Some(brokers.head), replicas = brokers, isr = brokers, errorCode = 0)
  private val partitionMetaData1 = new PartitionMetadata(1, Some(brokers.head), replicas = brokers, isr = brokers.tail, errorCode = 1)
  private val partitionMetaData2 = new PartitionMetadata(2, Some(brokers.head), replicas = brokers, isr = brokers, errorCode = 2)
  private val partitionMetaData3 = new PartitionMetadata(3, Some(brokers.head), replicas = brokers, isr = brokers.tail.tail, errorCode = 3)
  private val partitionMetaDataSeq = Seq(partitionMetaData0, partitionMetaData1, partitionMetaData2, partitionMetaData3)
  private val topicmetaData1 = new TopicMetadata(topic1, partitionMetaDataSeq)
  private val topicmetaData2 = new TopicMetadata(topic2, partitionMetaDataSeq)

  def createTestLeaderAndIsrRequest() : LeaderAndIsrRequest = {
    val leaderAndIsr1 = new LeaderIsrAndControllerEpoch(new LeaderAndIsr(leader1, 1, isr1, 1), 1)
    val leaderAndIsr2 = new LeaderIsrAndControllerEpoch(new LeaderAndIsr(leader2, 1, isr2, 2), 1)
    val map = Map(((topic1, 0), PartitionStateInfo(leaderAndIsr1, 3)),
                  ((topic2, 0), PartitionStateInfo(leaderAndIsr2, 3)))
    new LeaderAndIsrRequest(map.toMap, collection.immutable.Set[Broker](), 0, 1, 0)
  }

  def createTestLeaderAndIsrResponse() : LeaderAndIsrResponse = {
    val responseMap = Map(((topic1, 0), ErrorMapping.NoError),
                          ((topic2, 0), ErrorMapping.NoError))
    new LeaderAndIsrResponse(1, responseMap)
  }

  def createTestStopReplicaRequest() : StopReplicaRequest = {
    new StopReplicaRequest(controllerId = 0, controllerEpoch = 1, correlationId = 0, deletePartitions = true,
                           partitions = collection.immutable.Set((topic1, 0), (topic2, 0)))
  }

  def createTestStopReplicaResponse() : StopReplicaResponse = {
    val responseMap = Map(((topic1, 0), ErrorMapping.NoError),
                          ((topic2, 0), ErrorMapping.NoError))
    new StopReplicaResponse(0, responseMap.toMap)
  }

  def createTestProducerRequest: ProducerRequest = {
    new ProducerRequest(1, "client 1", 0, 1000, topicDataProducerRequest)
  }

  def createTestProducerResponse: ProducerResponse =
    ProducerResponse(1, Map(
      TopicAndPartition(topic1, 0) -> ProducerResponseStatus(0.toShort, 10001),
      TopicAndPartition(topic2, 0) -> ProducerResponseStatus(0.toShort, 20001)
    ))

  def createTestFetchRequest: FetchRequest = {
    new FetchRequest(requestInfo = requestInfos)
  }

  def createTestFetchResponse: FetchResponse = {
    FetchResponse(1, topicDataFetchResponse)
  }

  def createTestOffsetRequest = new OffsetRequest(
      collection.immutable.Map(TopicAndPartition(topic1, 1) -> PartitionOffsetRequestInfo(1000, 200)),
      replicaId = 0
  )

  def createTestOffsetResponse: OffsetResponse = {
    new OffsetResponse(0, collection.immutable.Map(
      TopicAndPartition(topic1, 1) -> PartitionOffsetsResponse(ErrorMapping.NoError, Seq(1000l, 2000l, 3000l, 4000l)))
    )
  }

  def createTestTopicMetadataRequest: TopicMetadataRequest = {
    new TopicMetadataRequest(1, 1, "client 1", Seq(topic1, topic2))
  }

  def createTestTopicMetadataResponse: TopicMetadataResponse = {
    new TopicMetadataResponse(Seq(topicmetaData1, topicmetaData2), 1)
  }

  def createTestOffsetCommitRequest: OffsetCommitRequest = {
    new OffsetCommitRequest("group 1", collection.immutable.Map(
      TopicAndPartition(topic1, 0) -> OffsetMetadataAndError(offset=42L, metadata="some metadata"),
      TopicAndPartition(topic1, 1) -> OffsetMetadataAndError(offset=100L, metadata=OffsetMetadataAndError.NoMetadata)
    ))
  }

  def createTestOffsetCommitResponse: OffsetCommitResponse = {
    new OffsetCommitResponse(collection.immutable.Map(
      TopicAndPartition(topic1, 0) -> ErrorMapping.NoError,
      TopicAndPartition(topic1, 1) -> ErrorMapping.UnknownTopicOrPartitionCode
    ))
  }

  def createTestOffsetFetchRequest: OffsetFetchRequest = {
    new OffsetFetchRequest("group 1", Seq(
      TopicAndPartition(topic1, 0),
      TopicAndPartition(topic1, 1)
    ))
  }

  def createTestOffsetFetchResponse: OffsetFetchResponse = {
    new OffsetFetchResponse(collection.immutable.Map(
      TopicAndPartition(topic1, 0) -> OffsetMetadataAndError(42L, "some metadata", ErrorMapping.NoError),
      TopicAndPartition(topic1, 1) -> OffsetMetadataAndError(100L, OffsetMetadataAndError.NoMetadata,
        ErrorMapping.UnknownTopicOrPartitionCode)
    ))
  }

}

class RequestResponseSerializationTest extends JUnitSuite {
  private val leaderAndIsrRequest = SerializationTestUtils.createTestLeaderAndIsrRequest
  private val leaderAndIsrResponse = SerializationTestUtils.createTestLeaderAndIsrResponse
  private val stopReplicaRequest = SerializationTestUtils.createTestStopReplicaRequest
  private val stopReplicaResponse = SerializationTestUtils.createTestStopReplicaResponse
  private val producerRequest = SerializationTestUtils.createTestProducerRequest
  private val producerResponse = SerializationTestUtils.createTestProducerResponse
  private val fetchRequest = SerializationTestUtils.createTestFetchRequest
  private val offsetRequest = SerializationTestUtils.createTestOffsetRequest
  private val offsetResponse = SerializationTestUtils.createTestOffsetResponse
  private val topicMetadataRequest = SerializationTestUtils.createTestTopicMetadataRequest
  private val topicMetadataResponse = SerializationTestUtils.createTestTopicMetadataResponse
  private val offsetCommitRequest = SerializationTestUtils.createTestOffsetCommitRequest
  private val offsetCommitResponse = SerializationTestUtils.createTestOffsetCommitResponse
  private val offsetFetchRequest = SerializationTestUtils.createTestOffsetFetchRequest
  private val offsetFetchResponse = SerializationTestUtils.createTestOffsetFetchResponse


  @Test
  def testSerializationAndDeserialization() {
    var buffer: ByteBuffer = ByteBuffer.allocate(leaderAndIsrRequest.sizeInBytes())
    leaderAndIsrRequest.writeTo(buffer)
    buffer.rewind()
    val deserializedLeaderAndIsrRequest = LeaderAndIsrRequest.readFrom(buffer)
    assertEquals("The original and deserialzed leaderAndISRRequest should be the same", leaderAndIsrRequest,
                 deserializedLeaderAndIsrRequest)

    buffer = ByteBuffer.allocate(leaderAndIsrResponse.sizeInBytes())
    leaderAndIsrResponse.writeTo(buffer)
    buffer.rewind()
    val deserializedLeaderAndIsrResponse = LeaderAndIsrResponse.readFrom(buffer)
    assertEquals("The original and deserialzed leaderAndISRResponse should be the same", leaderAndIsrResponse,
                 deserializedLeaderAndIsrResponse)

    buffer = ByteBuffer.allocate(stopReplicaRequest.sizeInBytes())
    stopReplicaRequest.writeTo(buffer)
    buffer.rewind()
    val deserializedStopReplicaRequest = StopReplicaRequest.readFrom(buffer)
    assertEquals("The original and deserialzed stopReplicaRequest should be the same", stopReplicaRequest,
                 deserializedStopReplicaRequest)

    buffer = ByteBuffer.allocate(stopReplicaResponse.sizeInBytes())
    stopReplicaResponse.writeTo(buffer)
    buffer.rewind()
    val deserializedStopReplicaResponse = StopReplicaResponse.readFrom(buffer)
    assertEquals("The original and deserialzed stopReplicaResponse should be the same", stopReplicaResponse,
                 deserializedStopReplicaResponse)

    buffer = ByteBuffer.allocate(producerRequest.sizeInBytes)
    producerRequest.writeTo(buffer)
    buffer.rewind()
    val deserializedProducerRequest = ProducerRequest.readFrom(buffer)
    assertEquals("The original and deserialzed producerRequest should be the same", producerRequest,
                 deserializedProducerRequest)

    buffer = ByteBuffer.allocate(producerResponse.sizeInBytes)
    producerResponse.writeTo(buffer)
    buffer.rewind()
    val deserializedProducerResponse = ProducerResponse.readFrom(buffer)
    assertEquals("The original and deserialzed producerResponse should be the same: [%s], [%s]".format(producerResponse, deserializedProducerResponse), producerResponse,
                 deserializedProducerResponse)

    buffer = ByteBuffer.allocate(fetchRequest.sizeInBytes)
    fetchRequest.writeTo(buffer)
    buffer.rewind()
    val deserializedFetchRequest = FetchRequest.readFrom(buffer)
    assertEquals("The original and deserialzed fetchRequest should be the same", fetchRequest,
                 deserializedFetchRequest)

    buffer = ByteBuffer.allocate(offsetRequest.sizeInBytes)
    offsetRequest.writeTo(buffer)
    buffer.rewind()
    val deserializedOffsetRequest = OffsetRequest.readFrom(buffer)
    assertEquals("The original and deserialzed offsetRequest should be the same", offsetRequest,
                 deserializedOffsetRequest)

    buffer = ByteBuffer.allocate(offsetResponse.sizeInBytes)
    offsetResponse.writeTo(buffer)
    buffer.rewind()
    val deserializedOffsetResponse = OffsetResponse.readFrom(buffer)
    assertEquals("The original and deserialzed offsetResponse should be the same", offsetResponse,
                 deserializedOffsetResponse)

    buffer = ByteBuffer.allocate(topicMetadataRequest.sizeInBytes())
    topicMetadataRequest.writeTo(buffer)
    buffer.rewind()
    val deserializedTopicMetadataRequest = TopicMetadataRequest.readFrom(buffer)
    assertEquals("The original and deserialzed topicMetadataRequest should be the same", topicMetadataRequest,
                 deserializedTopicMetadataRequest)

    buffer = ByteBuffer.allocate(topicMetadataResponse.sizeInBytes)
    topicMetadataResponse.writeTo(buffer)
    buffer.rewind()
    val deserializedTopicMetadataResponse = TopicMetadataResponse.readFrom(buffer)
    assertEquals("The original and deserialzed topicMetadataResponse should be the same", topicMetadataResponse,
                 deserializedTopicMetadataResponse)

    buffer = ByteBuffer.allocate(offsetCommitRequest.sizeInBytes)
    offsetCommitRequest.writeTo(buffer)
    buffer.rewind()
    val deserializedOffsetCommitRequest = OffsetCommitRequest.readFrom(buffer)
    assertEquals("The original and deserialzed offsetCommitRequest should be the same", offsetCommitRequest, 
      deserializedOffsetCommitRequest)

    buffer = ByteBuffer.allocate(offsetCommitResponse.sizeInBytes)
    offsetCommitResponse.writeTo(buffer)
    buffer.rewind()
    val deserializedOffsetCommitResponse = OffsetCommitResponse.readFrom(buffer)
    assertEquals("The original and deserialzed offsetCommitResponse should be the same", offsetCommitResponse, 
      deserializedOffsetCommitResponse)

    buffer = ByteBuffer.allocate(offsetFetchRequest.sizeInBytes)
    offsetFetchRequest.writeTo(buffer)
    buffer.rewind()
    val deserializedOffsetFetchRequest = OffsetFetchRequest.readFrom(buffer)
    assertEquals("The original and deserialzed offsetFetchRequest should be the same", offsetFetchRequest, 
      deserializedOffsetFetchRequest)

    buffer = ByteBuffer.allocate(offsetFetchResponse.sizeInBytes)
    offsetFetchResponse.writeTo(buffer)
    buffer.rewind()
    val deserializedOffsetFetchResponse = OffsetFetchResponse.readFrom(buffer)
    assertEquals("The original and deserialzed offsetFetchResponse should be the same", offsetFetchResponse, 
      deserializedOffsetFetchResponse)

  }
}
