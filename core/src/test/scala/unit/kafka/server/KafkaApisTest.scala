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

import java.lang.{Long => JLong}
import java.net.InetAddress
import java.util

import kafka.api.{ApiVersion, KAFKA_0_10_2_IV0}
import kafka.cluster.Replica
import kafka.controller.KafkaController
import kafka.coordinator.group.GroupCoordinator
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.log.{Log, TimestampOffset}
import kafka.network.RequestChannel
import kafka.security.auth.Authorizer
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server._
import kafka.utils.{MockTime, TestUtils, ZkUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests.WriteTxnMarkersRequest.TxnMarkerEntry
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.utils.Utils
import org.easymock.{Capture, EasyMock, IAnswer}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

import scala.collection.JavaConverters._
import scala.collection.Map

class KafkaApisTest {

  private val requestChannel = EasyMock.createNiceMock(classOf[RequestChannel])
  private val requestChannelMetrics = EasyMock.createNiceMock(classOf[RequestChannel.Metrics])
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

  def createKafkaApis(interBrokerProtocolVersion: ApiVersion = ApiVersion.latestVersion): KafkaApis = {
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
    val (writeTxnMarkersRequest, request) = createWriteTxnMarkersRequest(Utils.mkList(topicPartition))
    val expectedErrors = Map(topicPartition -> Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT).asJava
    val capturedResponse: Capture[RequestChannel.Response] = EasyMock.newCapture()

    EasyMock.expect(replicaManager.getMagic(topicPartition))
      .andReturn(Some(RecordBatch.MAGIC_VALUE_V1))
    EasyMock.expect(requestChannel.sendResponse(EasyMock.capture(capturedResponse)))
    EasyMock.replay(replicaManager, replicaQuotaManager, requestChannel)

    createKafkaApis().handleWriteTxnMarkersRequest(request)

    val markersResponse = readResponse(ApiKeys.WRITE_TXN_MARKERS, writeTxnMarkersRequest, capturedResponse)
      .asInstanceOf[WriteTxnMarkersResponse]
    assertEquals(expectedErrors, markersResponse.errors(1))
  }

  @Test
  def shouldRespondWithUnknownTopicWhenPartitionIsNotHosted(): Unit = {
    val topicPartition = new TopicPartition("t", 0)
    val (writeTxnMarkersRequest, request) = createWriteTxnMarkersRequest(Utils.mkList(topicPartition))
    val expectedErrors = Map(topicPartition -> Errors.UNKNOWN_TOPIC_OR_PARTITION).asJava
    val capturedResponse: Capture[RequestChannel.Response] = EasyMock.newCapture()

    EasyMock.expect(replicaManager.getMagic(topicPartition))
      .andReturn(None)
    EasyMock.expect(requestChannel.sendResponse(EasyMock.capture(capturedResponse)))
    EasyMock.replay(replicaManager, replicaQuotaManager, requestChannel)

    createKafkaApis().handleWriteTxnMarkersRequest(request)

    val markersResponse = readResponse(ApiKeys.WRITE_TXN_MARKERS, writeTxnMarkersRequest, capturedResponse)
      .asInstanceOf[WriteTxnMarkersResponse]
    assertEquals(expectedErrors, markersResponse.errors(1))
  }

  @Test
  def shouldRespondWithUnsupportedMessageFormatForBadPartitionAndNoErrorsForGoodPartition(): Unit = {
    val tp1 = new TopicPartition("t", 0)
    val tp2 = new TopicPartition("t1", 0)
    val (writeTxnMarkersRequest, request) = createWriteTxnMarkersRequest(Utils.mkList(tp1, tp2))
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

    createKafkaApis().handleWriteTxnMarkersRequest(request)

    val markersResponse = readResponse(ApiKeys.WRITE_TXN_MARKERS, writeTxnMarkersRequest, capturedResponse)
      .asInstanceOf[WriteTxnMarkersResponse]
    assertEquals(expectedErrors, markersResponse.errors(1))
    EasyMock.verify(replicaManager)
  }

  @Test
  def shouldRespondWithUnknownTopicOrPartitionForBadPartitionAndNoErrorsForGoodPartition(): Unit = {
    val tp1 = new TopicPartition("t", 0)
    val tp2 = new TopicPartition("t1", 0)
    val (writeTxnMarkersRequest, request) = createWriteTxnMarkersRequest(Utils.mkList(tp1, tp2))
    val expectedErrors = Map(tp1 -> Errors.UNKNOWN_TOPIC_OR_PARTITION, tp2 -> Errors.NONE).asJava

    val capturedResponse: Capture[RequestChannel.Response] = EasyMock.newCapture()
    val responseCallback: Capture[Map[TopicPartition, PartitionResponse] => Unit]  = EasyMock.newCapture()

    EasyMock.expect(replicaManager.getMagic(tp1))
      .andReturn(None)
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

    createKafkaApis().handleWriteTxnMarkersRequest(request)

    val markersResponse = readResponse(ApiKeys.WRITE_TXN_MARKERS, writeTxnMarkersRequest, capturedResponse)
      .asInstanceOf[WriteTxnMarkersResponse]
    assertEquals(expectedErrors, markersResponse.errors(1))
    EasyMock.verify(replicaManager)
  }

  @Test
  def shouldAppendToLogOnWriteTxnMarkersWhenCorrectMagicVersion(): Unit = {
    val topicPartition = new TopicPartition("t", 0)
    val request = createWriteTxnMarkersRequest(Utils.mkList(topicPartition))._2
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

    createKafkaApis().handleWriteTxnMarkersRequest(request)
    EasyMock.verify(replicaManager)
  }

  @Test
  def testReadUncommittedConsumerListOffsetLimitedAtHighWatermark(): Unit = {
    testConsumerListOffsetLimit(IsolationLevel.READ_UNCOMMITTED)
  }

  @Test
  def testReadCommittedConsumerListOffsetLimitedAtLastStableOffset(): Unit = {
    testConsumerListOffsetLimit(IsolationLevel.READ_COMMITTED)
  }

  private def testConsumerListOffsetLimit(isolationLevel: IsolationLevel): Unit = {
    val tp = new TopicPartition("foo", 0)
    val timestamp: JLong = time.milliseconds()
    val limitOffset = 15L

    val capturedResponse = EasyMock.newCapture[RequestChannel.Response]()
    val capturedThrottleCallback = EasyMock.newCapture[Int => Unit]()
    val replica = EasyMock.mock(classOf[Replica])
    val log = EasyMock.mock(classOf[Log])
    EasyMock.expect(replicaManager.getLeaderReplicaIfLocal(tp)).andReturn(replica)
    if (isolationLevel == IsolationLevel.READ_UNCOMMITTED)
      EasyMock.expect(replica.highWatermark).andReturn(LogOffsetMetadata(messageOffset = limitOffset))
    else
      EasyMock.expect(replica.lastStableOffset).andReturn(LogOffsetMetadata(messageOffset = limitOffset))
    EasyMock.expect(replicaManager.getLog(tp)).andReturn(Some(log))
    EasyMock.expect(log.fetchOffsetsByTimestamp(timestamp)).andReturn(Some(TimestampOffset(timestamp = timestamp, offset = limitOffset)))
    expectThrottleCallbackAndInvoke(capturedThrottleCallback)
    EasyMock.expect(requestChannel.sendResponse(EasyMock.capture(capturedResponse)))
    EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, replica, log)

    val builder = ListOffsetRequest.Builder.forConsumer(true, isolationLevel)
      .setTargetTimes(Map(tp -> timestamp).asJava)
    val (listOffsetRequest, request) = buildRequest(builder)
    createKafkaApis().handleListOffsetRequest(request)

    val response = readResponse(ApiKeys.LIST_OFFSETS, listOffsetRequest, capturedResponse).asInstanceOf[ListOffsetResponse]
    assertTrue(response.responseData.containsKey(tp))

    val partitionData = response.responseData.get(tp)
    assertEquals(Errors.NONE, partitionData.error)
    assertEquals(ListOffsetResponse.UNKNOWN_OFFSET, partitionData.offset)
    assertEquals(ListOffsetResponse.UNKNOWN_TIMESTAMP, partitionData.timestamp)
  }

  @Test
  def testReadUncommittedConsumerListOffsetEarliestOffsetEqualsHighWatermark(): Unit = {
    testConsumerListOffsetEarliestOffsetEqualsLimit(IsolationLevel.READ_UNCOMMITTED)
  }

  @Test
  def testReadCommittedConsumerListOffsetEarliestOffsetEqualsLastStableOffset(): Unit = {
    testConsumerListOffsetEarliestOffsetEqualsLimit(IsolationLevel.READ_COMMITTED)
  }

  private def testConsumerListOffsetEarliestOffsetEqualsLimit(isolationLevel: IsolationLevel): Unit = {
    val tp = new TopicPartition("foo", 0)
    val limitOffset = 15L

    val capturedResponse = EasyMock.newCapture[RequestChannel.Response]()
    val capturedThrottleCallback = EasyMock.newCapture[Int => Unit]()
    val replica = EasyMock.mock(classOf[Replica])
    val log = EasyMock.mock(classOf[Log])
    EasyMock.expect(replicaManager.getLeaderReplicaIfLocal(tp)).andReturn(replica)
    if (isolationLevel == IsolationLevel.READ_UNCOMMITTED)
      EasyMock.expect(replica.highWatermark).andReturn(LogOffsetMetadata(messageOffset = limitOffset))
    else
      EasyMock.expect(replica.lastStableOffset).andReturn(LogOffsetMetadata(messageOffset = limitOffset))
    EasyMock.expect(replicaManager.getLog(tp)).andReturn(Some(log))
    EasyMock.expect(log.fetchOffsetsByTimestamp(ListOffsetRequest.EARLIEST_TIMESTAMP))
      .andReturn(Some(TimestampOffset(timestamp = ListOffsetResponse.UNKNOWN_TIMESTAMP, offset = limitOffset)))
    expectThrottleCallbackAndInvoke(capturedThrottleCallback)
    EasyMock.expect(requestChannel.sendResponse(EasyMock.capture(capturedResponse)))
    EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, replica, log)

    val builder = ListOffsetRequest.Builder.forConsumer(true, isolationLevel)
      .setTargetTimes(Map(tp -> (ListOffsetRequest.EARLIEST_TIMESTAMP: JLong)).asJava)
    val (listOffsetRequest, request) = buildRequest(builder)
    createKafkaApis().handleListOffsetRequest(request)

    val response = readResponse(ApiKeys.LIST_OFFSETS, listOffsetRequest, capturedResponse).asInstanceOf[ListOffsetResponse]
    assertTrue(response.responseData.containsKey(tp))

    val partitionData = response.responseData.get(tp)
    assertEquals(Errors.NONE, partitionData.error)
    assertEquals(limitOffset, partitionData.offset)
    assertEquals(ListOffsetResponse.UNKNOWN_TIMESTAMP, partitionData.timestamp)
  }

  @Test
  def testReadUncommittedConsumerListOffsetLatest(): Unit = {
    testConsumerListOffsetLatest(IsolationLevel.READ_UNCOMMITTED)
  }

  @Test
  def testReadCommittedConsumerListOffsetLatest(): Unit = {
    testConsumerListOffsetLatest(IsolationLevel.READ_COMMITTED)
  }

  private def testConsumerListOffsetLatest(isolationLevel: IsolationLevel): Unit = {
    val tp = new TopicPartition("foo", 0)
    val latestOffset = 15L

    val capturedResponse = EasyMock.newCapture[RequestChannel.Response]()
    val capturedThrottleCallback = EasyMock.newCapture[Int => Unit]()
    val replica = EasyMock.mock(classOf[Replica])
    val log = EasyMock.mock(classOf[Log])
    EasyMock.expect(replicaManager.getLeaderReplicaIfLocal(tp)).andReturn(replica)
    if (isolationLevel == IsolationLevel.READ_UNCOMMITTED)
      EasyMock.expect(replica.highWatermark).andReturn(LogOffsetMetadata(messageOffset = latestOffset))
    else
      EasyMock.expect(replica.lastStableOffset).andReturn(LogOffsetMetadata(messageOffset = latestOffset))
    expectThrottleCallbackAndInvoke(capturedThrottleCallback)
    EasyMock.expect(requestChannel.sendResponse(EasyMock.capture(capturedResponse)))
    EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, replica, log)

    val builder = ListOffsetRequest.Builder.forConsumer(true, isolationLevel)
      .setTargetTimes(Map(tp -> (ListOffsetRequest.LATEST_TIMESTAMP: JLong)).asJava)
    val (listOffsetRequest, request) = buildRequest(builder)
    createKafkaApis().handleListOffsetRequest(request)

    val response = readResponse(ApiKeys.LIST_OFFSETS, listOffsetRequest, capturedResponse).asInstanceOf[ListOffsetResponse]
    assertTrue(response.responseData.containsKey(tp))

    val partitionData = response.responseData.get(tp)
    assertEquals(Errors.NONE, partitionData.error)
    assertEquals(latestOffset, partitionData.offset)
    assertEquals(ListOffsetResponse.UNKNOWN_TIMESTAMP, partitionData.timestamp)
  }

  private def createWriteTxnMarkersRequest(partitions: util.List[TopicPartition]) = {
    val requestBuilder = new WriteTxnMarkersRequest.Builder(Utils.mkList(
      new TxnMarkerEntry(1, 1.toShort, 0, TransactionResult.COMMIT, partitions)))
    buildRequest(requestBuilder)
  }

  private def buildRequest[T <: AbstractRequest](builder: AbstractRequest.Builder[T]): (T, RequestChannel.Request) = {
    val request = builder.build()
    val buffer = request.serialize(new RequestHeader(builder.apiKey, request.version, "", 0))

    // read the header from the buffer first so that the body can be read next from the Request constructor
    val header = RequestHeader.parse(buffer)
    val context = new RequestContext(header, "1", InetAddress.getLocalHost, KafkaPrincipal.ANONYMOUS,
      new ListenerName(""), SecurityProtocol.PLAINTEXT)
    (request, new RequestChannel.Request(processor = 1, context = context, startTimeNanos =  0,
      MemoryPool.NONE, buffer, requestChannelMetrics))
  }

  private def readResponse(api: ApiKeys, request: AbstractRequest, capturedResponse: Capture[RequestChannel.Response]): AbstractResponse = {
    val send = capturedResponse.getValue.responseSend.get
    val channel = new ByteBufferChannel(send.size)
    send.writeTo(channel)
    channel.close()
    channel.buffer.getInt() // read the size
    ResponseHeader.parse(channel.buffer)
    val struct = api.responseSchema(request.version).read(channel.buffer)
    AbstractResponse.parseResponse(api, struct)
  }

  private def expectThrottleCallbackAndInvoke(capturedThrottleCallback: Capture[Int => Unit]): Unit = {
    EasyMock.expect(clientRequestQuotaManager.maybeRecordAndThrottle(
      EasyMock.anyObject[RequestChannel.Request](),
      EasyMock.capture(capturedThrottleCallback)))
      .andAnswer(new IAnswer[Unit] {
        override def answer(): Unit = {
          val callback = capturedThrottleCallback.getValue
          callback(0)
        }
      })
  }

}
