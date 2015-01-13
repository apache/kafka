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
import kafka.common.{OffsetAndMetadata, ErrorMapping, OffsetMetadataAndError}
import kafka.utils.SystemTime
import org.apache.kafka.common.requests._
import org.apache.kafka.common.protocol.ApiKeys
import scala.Some
import kafka.controller.LeaderIsrAndControllerEpoch
import kafka.common.TopicAndPartition
import org.apache.kafka.common.TopicPartition


object SerializationTestUtils {
  private val topic1 = "test1"
  private val topic2 = "test2"
  private val leader1 = 0
  private val isr1 = List(0, 1, 2)
  private val leader2 = 0
  private val isr2 = List(0, 2, 3)
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
    val map = Map(((topic1, 0), PartitionStateInfo(leaderAndIsr1, isr1.toSet)),
                  ((topic2, 0), PartitionStateInfo(leaderAndIsr2, isr2.toSet)))
    new LeaderAndIsrRequest(map.toMap, collection.immutable.Set[Broker](), 0, 1, 0, "")
  }

  def createTestLeaderAndIsrResponse() : LeaderAndIsrResponse = {
    val responseMap = Map(((topic1, 0), ErrorMapping.NoError),
                          ((topic2, 0), ErrorMapping.NoError))
    new LeaderAndIsrResponse(1, responseMap)
  }

  def createTestStopReplicaRequest() : StopReplicaRequest = {
    new StopReplicaRequest(controllerId = 0, controllerEpoch = 1, correlationId = 0, deletePartitions = true,
                           partitions = collection.immutable.Set(TopicAndPartition(topic1, 0),TopicAndPartition(topic2, 0)))
  }

  def createTestStopReplicaResponse() : StopReplicaResponse = {
    val responseMap = Map((TopicAndPartition(topic1, 0), ErrorMapping.NoError),
                          (TopicAndPartition(topic2, 0), ErrorMapping.NoError))
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
    new TopicMetadataResponse(brokers, Seq(topicmetaData1, topicmetaData2), 1)
  }

  def createTestOffsetCommitRequestV1: OffsetCommitRequest = {
    new OffsetCommitRequest("group 1", collection.immutable.Map(
      TopicAndPartition(topic1, 0) -> OffsetAndMetadata(offset=42L, metadata="some metadata", timestamp=SystemTime.milliseconds),
      TopicAndPartition(topic1, 1) -> OffsetAndMetadata(offset=100L, metadata=OffsetAndMetadata.NoMetadata, timestamp=SystemTime.milliseconds)
    ))
  }

  def createTestOffsetCommitRequestV0: OffsetCommitRequest = {
    new OffsetCommitRequest(
      versionId = 0,
      groupId = "group 1",
      requestInfo = collection.immutable.Map(
        TopicAndPartition(topic1, 0) -> OffsetAndMetadata(offset=42L, metadata="some metadata"),
        TopicAndPartition(topic1, 1) -> OffsetAndMetadata(offset=100L, metadata=OffsetAndMetadata.NoMetadata)
      ))
  }

  def createTestOffsetCommitResponse: OffsetCommitResponse = {
    new OffsetCommitResponse(collection.immutable.Map(TopicAndPartition(topic1, 0) -> ErrorMapping.NoError,
                                 TopicAndPartition(topic1, 1) -> ErrorMapping.NoError))
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
      TopicAndPartition(topic1, 1) -> OffsetMetadataAndError(100L, OffsetAndMetadata.NoMetadata, ErrorMapping.UnknownTopicOrPartitionCode)
    ))
  }

  def createConsumerMetadataRequest: ConsumerMetadataRequest = {
    ConsumerMetadataRequest("group 1", clientId = "client 1")
  }

  def createConsumerMetadataResponse: ConsumerMetadataResponse = {
    ConsumerMetadataResponse(Some(brokers.head), ErrorMapping.NoError)
  }

  def createHeartbeatRequestAndHeader: HeartbeatRequestAndHeader = {
    val body = new HeartbeatRequest("group1", 1, "consumer1")
    HeartbeatRequestAndHeader(0.asInstanceOf[Short], 1, "", body)
  }

  def createHeartbeatResponseAndHeader: HeartbeatResponseAndHeader = {
    val body = new HeartbeatResponse(0.asInstanceOf[Short])
    HeartbeatResponseAndHeader(1, body)
  }

  def createJoinGroupRequestAndHeader: JoinGroupRequestAndHeader = {
    import scala.collection.JavaConversions._
    val body = new JoinGroupRequest("group1", 30000, List("topic1"), "consumer1", "strategy1");
    JoinGroupRequestAndHeader(0.asInstanceOf[Short], 1, "", body)
  }

  def createJoinGroupResponseAndHeader: JoinGroupResponseAndHeader = {
    import scala.collection.JavaConversions._
    val body = new JoinGroupResponse(0.asInstanceOf[Short], 1, "consumer1", List(new TopicPartition("test11", 1)))
    JoinGroupResponseAndHeader(1, body)
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
  private val offsetCommitRequestV0 = SerializationTestUtils.createTestOffsetCommitRequestV0
  private val offsetCommitRequestV1 = SerializationTestUtils.createTestOffsetCommitRequestV1
  private val offsetCommitResponse = SerializationTestUtils.createTestOffsetCommitResponse
  private val offsetFetchRequest = SerializationTestUtils.createTestOffsetFetchRequest
  private val offsetFetchResponse = SerializationTestUtils.createTestOffsetFetchResponse
  private val consumerMetadataRequest = SerializationTestUtils.createConsumerMetadataRequest
  private val consumerMetadataResponse = SerializationTestUtils.createConsumerMetadataResponse
  private val consumerMetadataResponseNoCoordinator = ConsumerMetadataResponse(None, ErrorMapping.ConsumerCoordinatorNotAvailableCode)
  private val heartbeatRequest = SerializationTestUtils.createHeartbeatRequestAndHeader
  private val heartbeatResponse = SerializationTestUtils.createHeartbeatResponseAndHeader
  private val joinGroupRequest = SerializationTestUtils.createJoinGroupRequestAndHeader
  private val joinGroupResponse = SerializationTestUtils.createJoinGroupResponseAndHeader

  @Test
  def testSerializationAndDeserialization() {

    val requestsAndResponses =
      collection.immutable.Seq(leaderAndIsrRequest, leaderAndIsrResponse, stopReplicaRequest,
                               stopReplicaResponse, producerRequest, producerResponse,
                               fetchRequest, offsetRequest, offsetResponse, topicMetadataRequest,
                               topicMetadataResponse, offsetCommitRequestV0, offsetCommitRequestV1,
                               offsetCommitResponse, offsetFetchRequest, offsetFetchResponse,
                               consumerMetadataRequest, consumerMetadataResponse,
                               consumerMetadataResponseNoCoordinator, heartbeatRequest,
                               heartbeatResponse, joinGroupRequest, joinGroupResponse)

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
}
