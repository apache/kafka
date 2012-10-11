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
import kafka.common.{TopicAndPartition, ErrorMapping}


object SerializationTestUtils{
  private val topic1 = "test1"
  private val topic2 = "test2"
  private val leader1 = 0
  private val isr1 = List(0, 1, 2)
  private val leader2 = 0
  private val isr2 = List(0, 2, 3)
  private val partitionDataFetchResponse0 = new FetchResponsePartitionData(0, new ByteBufferMessageSet(new Message("first message".getBytes)))
  private val partitionDataFetchResponse1 = new FetchResponsePartitionData(1, new ByteBufferMessageSet(new Message("second message".getBytes)))
  private val partitionDataFetchResponse2 = new FetchResponsePartitionData(2, new ByteBufferMessageSet(new Message("third message".getBytes)))
  private val partitionDataFetchResponse3 = new FetchResponsePartitionData(3, new ByteBufferMessageSet(new Message("fourth message".getBytes)))
  private val partitionDataFetchResponseArray = Array(partitionDataFetchResponse0, partitionDataFetchResponse1, partitionDataFetchResponse2, partitionDataFetchResponse3)

  private val topicDataFetchResponse = {
    val groupedData = Array(topic1, topic2).flatMap(topic =>
      partitionDataFetchResponseArray.map(partitionData =>
        (TopicAndPartition(topic, partitionData.partition), partitionData)))
    collection.immutable.Map(groupedData:_*)
  }

  private val partitionDataProducerRequest0 = new ProducerRequestPartitionData(0, new ByteBufferMessageSet(new Message("first message".getBytes)))
  private val partitionDataProducerRequest1 = new ProducerRequestPartitionData(1, new ByteBufferMessageSet(new Message("second message".getBytes)))
  private val partitionDataProducerRequest2 = new ProducerRequestPartitionData(2, new ByteBufferMessageSet(new Message("third message".getBytes)))
  private val partitionDataProducerRequest3 = new ProducerRequestPartitionData(3, new ByteBufferMessageSet(new Message("fourth message".getBytes)))
  private val partitionDataProducerRequestArray = Array(partitionDataProducerRequest0, partitionDataProducerRequest1, partitionDataProducerRequest2, partitionDataProducerRequest3)

  private val topicDataProducerRequest = {
    val groupedData = Array(topic1, topic2).flatMap(topic =>
      partitionDataProducerRequestArray.map(partitionData =>
        (TopicAndPartition(topic, partitionData.partition), partitionData)))
    collection.immutable.Map(groupedData:_*)
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

  private val partitionMetaData0 = new PartitionMetadata(0, Some(new Broker(0, "creator", "localhost", 1011)), collection.immutable.Seq.empty)
  private val partitionMetaData1 = new PartitionMetadata(1, Some(new Broker(0, "creator", "localhost", 1011)), collection.immutable.Seq.empty)
  private val partitionMetaData2 = new PartitionMetadata(2, Some(new Broker(0, "creator", "localhost", 1011)), collection.immutable.Seq.empty)
  private val partitionMetaData3 = new PartitionMetadata(3, Some(new Broker(0, "creator", "localhost", 1011)), collection.immutable.Seq.empty)
  private val partitionMetaDataSeq = Seq(partitionMetaData0, partitionMetaData1, partitionMetaData2, partitionMetaData3)
  private val topicmetaData1 = new TopicMetadata(topic1, partitionMetaDataSeq)
  private val topicmetaData2 = new TopicMetadata(topic2, partitionMetaDataSeq)

  def createTestLeaderAndISRRequest() : LeaderAndIsrRequest = {
    val leaderAndISR1 = new LeaderAndIsr(leader1, 1, isr1, 1)
    val leaderAndISR2 = new LeaderAndIsr(leader2, 1, isr2, 2)
    val map = Map(((topic1, 0), PartitionStateInfo(leaderAndISR1, 3)),
                  ((topic2, 0), PartitionStateInfo(leaderAndISR2, 3)))
    new LeaderAndIsrRequest(map)
  }

  def createTestLeaderAndISRResponse() : LeaderAndISRResponse = {
    val responseMap = Map(((topic1, 0), ErrorMapping.NoError),
                          ((topic2, 0), ErrorMapping.NoError))
    new LeaderAndISRResponse(1, responseMap)
  }

  def createTestStopReplicaRequest() : StopReplicaRequest = {
    new StopReplicaRequest(collection.immutable.Set((topic1, 0), (topic2, 0)))
  }

  def createTestStopReplicaResponse() : StopReplicaResponse = {
    val responseMap = Map(((topic1, 0), ErrorMapping.NoError),
                          ((topic2, 0), ErrorMapping.NoError))
    new StopReplicaResponse(1, responseMap)
  }

  def createTestProducerRequest: ProducerRequest = {
    new ProducerRequest(1, "client 1", 0, 1000, topicDataProducerRequest)
  }

  def createTestProducerResponse: ProducerResponse =
    ProducerResponse(1, 1, Map(
      TopicAndPartition(topic1, 0) -> ProducerResponseStatus(0.toShort, 10001),
      TopicAndPartition(topic2, 0) -> ProducerResponseStatus(0.toShort, 20001)
    ))

  def createTestFetchRequest: FetchRequest = {
    new FetchRequest(requestInfo = requestInfos)
  }

  def createTestFetchResponse: FetchResponse = {
    FetchResponse(1, 1, topicDataFetchResponse)
  }

  def createTestOffsetRequest = new OffsetRequest(
      collection.immutable.Map(TopicAndPartition(topic1, 1) -> PartitionOffsetRequestInfo(1000, 200)),
      replicaId = 0
  )

  def createTestOffsetResponse: OffsetResponse = {
    new OffsetResponse(OffsetRequest.CurrentVersion, collection.immutable.Map(
      TopicAndPartition(topic1, 1) -> PartitionOffsetsResponse(ErrorMapping.NoError, Seq(1000l, 2000l, 3000l, 4000l)))
    )
  }

  def createTestTopicMetadataRequest: TopicMetadataRequest = {
    new TopicMetadataRequest(1, "client 1", Seq(topic1, topic2))
  }

  def createTestTopicMetadataResponse: TopicMetadataResponse = {
    new TopicMetadataResponse(1, Seq(topicmetaData1, topicmetaData2))
  }
}

class RequestResponseSerializationTest extends JUnitSuite {
  private val leaderAndISRRequest = SerializationTestUtils.createTestLeaderAndISRRequest
  private val leaderAndISRResponse = SerializationTestUtils.createTestLeaderAndISRResponse
  private val stopReplicaRequest = SerializationTestUtils.createTestStopReplicaRequest
  private val stopReplicaResponse = SerializationTestUtils.createTestStopReplicaResponse
  private val producerRequest = SerializationTestUtils.createTestProducerRequest
  private val producerResponse = SerializationTestUtils.createTestProducerResponse
  private val fetchRequest = SerializationTestUtils.createTestFetchRequest
  private val offsetRequest = SerializationTestUtils.createTestOffsetRequest
  private val offsetResponse = SerializationTestUtils.createTestOffsetResponse
  private val topicMetadataRequest = SerializationTestUtils.createTestTopicMetadataRequest
  private val topicMetadataResponse = SerializationTestUtils.createTestTopicMetadataResponse


  @Test
  def testSerializationAndDeserialization() {
    var buffer: ByteBuffer = ByteBuffer.allocate(leaderAndISRRequest.sizeInBytes())
    leaderAndISRRequest.writeTo(buffer)
    buffer.rewind()
    val deserializedLeaderAndISRRequest = LeaderAndIsrRequest.readFrom(buffer)
    assertEquals("The original and deserialzed leaderAndISRRequest should be the same", leaderAndISRRequest,
                 deserializedLeaderAndISRRequest)

    buffer = ByteBuffer.allocate(leaderAndISRResponse.sizeInBytes())
    leaderAndISRResponse.writeTo(buffer)
    buffer.rewind()
    val deserializedLeaderAndISRResponse = LeaderAndISRResponse.readFrom(buffer)
    assertEquals("The original and deserialzed leaderAndISRResponse should be the same", leaderAndISRResponse,
                 deserializedLeaderAndISRResponse)

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
  }
}
