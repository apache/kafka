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

package unit.kafka.server


import java.net.InetAddress
import java.util

import kafka.api.{ApiVersion, KAFKA_0_10_2_IV0, KAFKA_0_11_0_IV0}
import kafka.controller.KafkaController
import kafka.coordinator.group.GroupCoordinator
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.network.RequestChannel
import kafka.network.RequestChannel.Session
import kafka.security.auth.Authorizer
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server._
import kafka.utils.{MockTime, TestUtils, ZkUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{ApiKeys, Errors, SecurityProtocol}
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests.WriteTxnMarkersRequest.TxnMarkerEntry
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.Utils
import org.easymock.{Capture, EasyMock, IAnswer}
import org.junit.{Assert, Before, Test}

import scala.collection.JavaConverters._
import scala.collection.Map


class KafkaApisTest {

  private val requestChannel = EasyMock.createNiceMock(classOf[RequestChannel])
  private val replicaManager = EasyMock.createNiceMock(classOf[ReplicaManager])
  private val groupCoordinator = EasyMock.createNiceMock(classOf[GroupCoordinator])
  private val adminManager = EasyMock.createNiceMock(classOf[AdminManager])
  private val txnCoordinator = EasyMock.createNiceMock(classOf[TransactionCoordinator])
  private val controller = EasyMock.createNiceMock(classOf[KafkaController])
  private val zkUtils = EasyMock.createNiceMock(classOf[ZkUtils])
  private val metadataCache = EasyMock.createNiceMock(classOf[MetadataCache])
  private val metrics = new Metrics()
  private val brokerId = 1
  private val authorizer: Option[Authorizer] = None
  private val clientQuotaManager = EasyMock.createNiceMock(classOf[ClientQuotaManager])
  private val clientRequestQuotaManager = EasyMock.createNiceMock(classOf[ClientRequestQuotaManager])
  private val replicaQuotaManager = EasyMock.createNiceMock(classOf[ReplicationQuotaManager])
  private val quotas = QuotaManagers(clientQuotaManager, clientQuotaManager, clientRequestQuotaManager, replicaQuotaManager, replicaQuotaManager)
  private val brokerTopicStats = new BrokerTopicStats
  private val clusterId = "clusterId"
  private val time = new MockTime



  def createKafkaApis(interBrokerProtocolVersion: ApiVersion): KafkaApis = {
    val properties = TestUtils.createBrokerConfig(brokerId, "zk")
    properties.put(KafkaConfig.InterBrokerProtocolVersionProp, interBrokerProtocolVersion.toString)
    properties.put(KafkaConfig.LogMessageFormatVersionProp, interBrokerProtocolVersion.toString)
    new KafkaApis(requestChannel,
      replicaManager,
      adminManager,
      groupCoordinator,
      txnCoordinator,
      controller,
      zkUtils,
      brokerId,
      new KafkaConfig(properties),
      metadataCache,
      metrics,
      authorizer,
      quotas,
      brokerTopicStats,
      clusterId,
      time
    )
  }

  @Test(expected = classOf[UnsupportedVersionException])
  def shouldThrowUnsupportedVersionExceptionOnHandleAddOffsetToTxnRequestWhenInterBrokerProtocolNotSupported(): Unit = {
    createKafkaApis(KAFKA_0_10_2_IV0).handleAddOffsetsToTxnRequest(null)
  }

  @Test(expected = classOf[UnsupportedVersionException])
  def shouldThrowUnsupportedVersionExceptionOnHandleAddPartitionsToTxnRequestWhenInterBrokerProtocolNotSupported(): Unit = {
    createKafkaApis(KAFKA_0_10_2_IV0).handleAddPartitionToTxnRequest(null)
  }

  @Test(expected = classOf[UnsupportedVersionException])
  def shouldThrowUnsupportedVersionExceptionOnHandleTxnOffsetCommitRequestWhenInterBrokerProtocolNotSupported(): Unit = {
    createKafkaApis(KAFKA_0_10_2_IV0).handleAddPartitionToTxnRequest(null)
  }

  @Test(expected = classOf[UnsupportedVersionException])
  def shouldThrowUnsupportedVersionExceptionOnHandleEndTxnRequestWhenInterBrokerProtocolNotSupported(): Unit = {
    createKafkaApis(KAFKA_0_10_2_IV0).handleEndTxnRequest(null)
  }

  @Test(expected = classOf[UnsupportedVersionException])
  def shouldThrowUnsupportedVersionExceptionOnHandleWriteTxnMarkersRequestWhenInterBrokerProtocolNotSupported(): Unit = {
    createKafkaApis(KAFKA_0_10_2_IV0).handleWriteTxnMarkersRequest(null)
  }

  @Test
  def shouldRespondWithUnsupportedForMessageFormatOnHandleWriteTxnMarkersWhenMagicLowerThanRequired(): Unit = {
    val topicPartition = new TopicPartition("t", 0)
    val (writeTxnMarkersRequest: WriteTxnMarkersRequest, request: RequestChannel.Request) = createWriteTxnMarkersRequest(Utils.mkList(topicPartition))
    val expectedErrors = Map(topicPartition -> Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT).asJava
    val capturedResponse: Capture[RequestChannel.Response] = EasyMock.newCapture()

    EasyMock.expect(replicaManager.getMagic(topicPartition))
      .andReturn(Some(RecordBatch.MAGIC_VALUE_V1))
    EasyMock.expect(requestChannel.sendResponse(EasyMock.capture(capturedResponse)))
    EasyMock.replay(replicaManager, replicaQuotaManager, requestChannel)

    createKafkaApis(KAFKA_0_11_0_IV0).handleWriteTxnMarkersRequest(request)

    val send = capturedResponse.getValue.responseSend.get
    val channel = new ByteBufferChannel(send.size())
    send.writeTo(channel)
    channel.close()

    // read the size
    channel.buffer.getInt()

    val responseHeader = ResponseHeader.parse(channel.buffer)
    val struct = ApiKeys.WRITE_TXN_MARKERS.responseSchema(writeTxnMarkersRequest.version()).read(channel.buffer)

    val markersResponse = new WriteTxnMarkersResponse(struct)
    Assert.assertEquals(expectedErrors, markersResponse.errors(1))
  }

  @Test
  def shouldRespondWithUnsupportedMessageFormatForBadPartitionAndNoErrorsForGoodPartition(): Unit = {
    val tp1 = new TopicPartition("t", 0)
    val tp2 = new TopicPartition("t1", 0)
    val (writeTxnMarkersRequest: WriteTxnMarkersRequest, request: RequestChannel.Request) = createWriteTxnMarkersRequest(Utils.mkList(tp1, tp2))
    val expectedErrors = Map(tp1 -> Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT, tp2 -> Errors.NONE).asJava

    val capturedResponse: Capture[RequestChannel.Response] = EasyMock.newCapture()
    val responseCallback: Capture[Map[TopicPartition, PartitionResponse] => Unit]  = EasyMock.newCapture()

    EasyMock.expect(replicaManager.getMagic(tp1))
      .andReturn(Some(RecordBatch.MAGIC_VALUE_V1))
    EasyMock.expect(replicaManager.getMagic(tp2))
      .andReturn(Some(RecordBatch.MAGIC_VALUE_V2))


    EasyMock.expect(replicaManager.appendRecords(EasyMock.anyLong(),
      EasyMock.anyShort(),
      EasyMock.eq(true),
      EasyMock.eq(false),
      EasyMock.anyObject(),
      EasyMock.capture(responseCallback),
      EasyMock.anyObject())).andAnswer(new IAnswer[Unit] {
      override def answer(): Unit = {
        responseCallback.getValue.apply(Map(tp2 -> new PartitionResponse(Errors.NONE)))
      }
    })

    EasyMock.expect(requestChannel.sendResponse(EasyMock.capture(capturedResponse)))
    EasyMock.replay(replicaManager, replicaQuotaManager, requestChannel)


    createKafkaApis(KAFKA_0_11_0_IV0).handleWriteTxnMarkersRequest(request)

    val send = capturedResponse.getValue.responseSend.get
    val channel = new ByteBufferChannel(send.size())
    send.writeTo(channel)
    channel.close()

    // read the size
    channel.buffer.getInt()

    val responseHeader = ResponseHeader.parse(channel.buffer)
    val struct = ApiKeys.WRITE_TXN_MARKERS.responseSchema(writeTxnMarkersRequest.version()).read(channel.buffer)

    val markersResponse = new WriteTxnMarkersResponse(struct)
    Assert.assertEquals(expectedErrors, markersResponse.errors(1))
    EasyMock.verify(replicaManager)
  }

  @Test
  def shouldAppendToLogOnWriteTxnMarkersWhenCorrectMagicVersion(): Unit = {
    val topicPartition = new TopicPartition("t", 0)
    val (writeTxnMarkersRequest: WriteTxnMarkersRequest, request: RequestChannel.Request) = createWriteTxnMarkersRequest(Utils.mkList(topicPartition))
    EasyMock.expect(replicaManager.getMagic(topicPartition))
      .andReturn(Some(RecordBatch.MAGIC_VALUE_V2))

    EasyMock.expect(replicaManager.appendRecords(EasyMock.anyLong(),
      EasyMock.anyShort(),
      EasyMock.eq(true),
      EasyMock.eq(false),
      EasyMock.anyObject(),
      EasyMock.anyObject(),
      EasyMock.anyObject()))

    EasyMock.replay(replicaManager)

    createKafkaApis(KAFKA_0_11_0_IV0).handleWriteTxnMarkersRequest(request)
    EasyMock.verify(replicaManager)
  }

  private def createWriteTxnMarkersRequest(partitions: util.List[TopicPartition]) = {
    val writeTxnMarkersRequest = new WriteTxnMarkersRequest.Builder(Utils.mkList(
    new TxnMarkerEntry(1, 1.toShort, 0, TransactionResult.COMMIT, partitions))).build()
    val header = new RequestHeader(ApiKeys.WRITE_TXN_MARKERS.id, writeTxnMarkersRequest.version(), "", 0)
    val byteBuffer = writeTxnMarkersRequest.serialize(header)

    val request = RequestChannel.Request(1, "1",
    Session(KafkaPrincipal.ANONYMOUS,
    InetAddress.getLocalHost),
    byteBuffer, 0,
    new ListenerName(""),
    SecurityProtocol.PLAINTEXT)
    (writeTxnMarkersRequest, request)
  }


}
