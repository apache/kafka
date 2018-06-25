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

import java.lang.{Long => JLong}
import java.net.InetAddress
import java.util
import java.util.Collections

import kafka.api.{ApiVersion, KAFKA_0_10_2_IV0}
import kafka.cluster.Replica
import kafka.controller.KafkaController
import kafka.coordinator.group.GroupCoordinator
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.log.{Log, TimestampOffset}
import kafka.network.RequestChannel
import kafka.network.RequestChannel.SendResponse
import kafka.security.auth.Authorizer
import kafka.server.QuotaFactory.QuotaManagers
import kafka.utils.{MockTime, TestUtils}
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests.UpdateMetadataRequest.{Broker, EndPoint}
import org.apache.kafka.common.requests.WriteTxnMarkersRequest.TxnMarkerEntry
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.utils.Utils
import org.easymock.{Capture, EasyMock, IAnswer}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.{After, Test}

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
  private val zkClient = EasyMock.createNiceMock(classOf[KafkaZkClient])
  private val metrics = new Metrics()
  private val brokerId = 1
  private val metadataCache = new MetadataCache(brokerId)
  private val authorizer: Option[Authorizer] = None
  private val clientQuotaManager = EasyMock.createNiceMock(classOf[ClientQuotaManager])
  private val clientRequestQuotaManager = EasyMock.createNiceMock(classOf[ClientRequestQuotaManager])
  private val replicaQuotaManager = EasyMock.createNiceMock(classOf[ReplicationQuotaManager])
  private val quotas = QuotaManagers(clientQuotaManager, clientQuotaManager, clientRequestQuotaManager,
    replicaQuotaManager, replicaQuotaManager, replicaQuotaManager, None)
  private val fetchManager = EasyMock.createNiceMock(classOf[FetchManager])
  private val brokerTopicStats = new BrokerTopicStats
  private val clusterId = "clusterId"
  private val time = new MockTime

  @After
  def tearDown() {
    quotas.shutdown()
    metrics.close()
  }

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
      zkClient,
      brokerId,
      new KafkaConfig(properties),
      metadataCache,
      metrics,
      authorizer,
      quotas,
      fetchManager,
      brokerTopicStats,
      clusterId,
      time,
      null
    )
  }

  @Test
  def testOffsetCommitWithInvalidPartition(): Unit = {
    val topic = "topic"
    setupBasicMetadataCache(topic, numPartitions = 1)

    def checkInvalidPartition(invalidPartitionId: Int): Unit = {
      EasyMock.reset(replicaManager, clientRequestQuotaManager, requestChannel)

      val invalidTopicPartition = new TopicPartition(topic, invalidPartitionId)
      val partitionOffsetCommitData = new OffsetCommitRequest.PartitionData(15L, "")
      val (offsetCommitRequest, request) = buildRequest(new OffsetCommitRequest.Builder("groupId",
        Map(invalidTopicPartition -> partitionOffsetCommitData).asJava))

      val capturedResponse = expectNoThrottling()
      EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel)
      createKafkaApis().handleOffsetCommitRequest(request)

      val response = readResponse(ApiKeys.OFFSET_COMMIT, offsetCommitRequest, capturedResponse)
        .asInstanceOf[OffsetCommitResponse]
      assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, response.responseData().get(invalidTopicPartition))
    }

    checkInvalidPartition(-1)
    checkInvalidPartition(1) // topic has only one partition
  }

  @Test
  def testTxnOffsetCommitWithInvalidPartition(): Unit = {
    val topic = "topic"
    setupBasicMetadataCache(topic, numPartitions = 1)

    def checkInvalidPartition(invalidPartitionId: Int): Unit = {
      EasyMock.reset(replicaManager, clientRequestQuotaManager, requestChannel)

      val invalidTopicPartition = new TopicPartition(topic, invalidPartitionId)
      val partitionOffsetCommitData = new TxnOffsetCommitRequest.CommittedOffset(15L, "")
      val (offsetCommitRequest, request) = buildRequest(new TxnOffsetCommitRequest.Builder("txnlId", "groupId",
        15L, 0.toShort, Map(invalidTopicPartition -> partitionOffsetCommitData).asJava))

      val capturedResponse = expectNoThrottling()
      EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel)
      createKafkaApis().handleTxnOffsetCommitRequest(request)

      val response = readResponse(ApiKeys.TXN_OFFSET_COMMIT, offsetCommitRequest, capturedResponse)
        .asInstanceOf[TxnOffsetCommitResponse]
      assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, response.errors().get(invalidTopicPartition))
    }

    checkInvalidPartition(-1)
    checkInvalidPartition(1) // topic has only one partition
  }

  @Test
  def testAddPartitionsToTxnWithInvalidPartition(): Unit = {
    val topic = "topic"
    setupBasicMetadataCache(topic, numPartitions = 1)

    def checkInvalidPartition(invalidPartitionId: Int): Unit = {
      EasyMock.reset(replicaManager, clientRequestQuotaManager, requestChannel)

      val invalidTopicPartition = new TopicPartition(topic, invalidPartitionId)

      val (addPartitionsToTxnRequest, request) = buildRequest(new AddPartitionsToTxnRequest.Builder(
        "txnlId", 15L, 0.toShort, List(invalidTopicPartition).asJava))

      val capturedResponse = expectNoThrottling()
      EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel)
      createKafkaApis().handleAddPartitionToTxnRequest(request)

      val response = readResponse(ApiKeys.ADD_PARTITIONS_TO_TXN, addPartitionsToTxnRequest, capturedResponse)
        .asInstanceOf[AddPartitionsToTxnResponse]
      assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, response.errors().get(invalidTopicPartition))
    }

    checkInvalidPartition(-1)
    checkInvalidPartition(1) // topic has only one partition
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
      EasyMock.anyObject(),
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
      EasyMock.anyObject(),
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

    val replica = EasyMock.mock(classOf[Replica])
    val log = EasyMock.mock(classOf[Log])
    EasyMock.expect(replicaManager.getLeaderReplicaIfLocal(tp)).andReturn(replica)
    if (isolationLevel == IsolationLevel.READ_UNCOMMITTED)
      EasyMock.expect(replica.highWatermark).andReturn(LogOffsetMetadata(messageOffset = limitOffset))
    else
      EasyMock.expect(replica.lastStableOffset).andReturn(LogOffsetMetadata(messageOffset = limitOffset))
    EasyMock.expect(replicaManager.getLog(tp)).andReturn(Some(log))
    EasyMock.expect(log.fetchOffsetsByTimestamp(timestamp)).andReturn(Some(TimestampOffset(timestamp = timestamp, offset = limitOffset)))
    val capturedResponse = expectNoThrottling()
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
    val capturedResponse = expectNoThrottling()
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

  /**
   * Verifies that the metadata response is correct if the broker listeners are inconsistent (i.e. one broker has
   * more listeners than another) and the request is sent on the listener that exists in both brokers.
   */
  @Test
  def testMetadataRequestOnSharedListenerWithInconsistentListenersAcrossBrokers(): Unit = {
    val (plaintextListener, _) = updateMetadataCacheWithInconsistentListeners()
    val response = sendMetadataRequestWithInconsistentListeners(plaintextListener)
    assertEquals(Set(0, 1), response.brokers.asScala.map(_.id).toSet)
  }

  /*
   * Verifies that the metadata response is correct if the broker listeners are inconsistent (i.e. one broker has
   * more listeners than another) and the request is sent on the listener that exists in one broker.
   */
  @Test
  def testMetadataRequestOnDistinctListenerWithInconsistentListenersAcrossBrokers(): Unit = {
    val (_, anotherListener) = updateMetadataCacheWithInconsistentListeners()
    val response = sendMetadataRequestWithInconsistentListeners(anotherListener)
    assertEquals(Set(0), response.brokers.asScala.map(_.id).toSet)
  }

  /**
   * Return pair of listener names in the metadataCache: PLAINTEXT and LISTENER2 respectively.
   */
  private def updateMetadataCacheWithInconsistentListeners(): (ListenerName, ListenerName) = {
    val plaintextListener = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val anotherListener = new ListenerName("LISTENER2")
    val brokers = Set(
      new Broker(0, Seq(new EndPoint("broker0", 9092, SecurityProtocol.PLAINTEXT, plaintextListener),
        new EndPoint("broker0", 9093, SecurityProtocol.PLAINTEXT, anotherListener)).asJava, "rack"),
      new Broker(1, Seq(new EndPoint("broker1", 9092, SecurityProtocol.PLAINTEXT, plaintextListener)).asJava,
        "rack")
    )
    val updateMetadataRequest = new UpdateMetadataRequest.Builder(ApiKeys.UPDATE_METADATA.latestVersion, 0,
      0, Map.empty[TopicPartition, UpdateMetadataRequest.PartitionState].asJava, brokers.asJava).build()
    metadataCache.updateCache(correlationId = 0, updateMetadataRequest)
    (plaintextListener, anotherListener)
  }

  private def sendMetadataRequestWithInconsistentListeners(requestListener: ListenerName): MetadataResponse = {
    val capturedResponse = expectNoThrottling()
    EasyMock.replay(clientRequestQuotaManager, requestChannel)

    val (metadataRequest, requestChannelRequest) = buildRequest(MetadataRequest.Builder.allTopics, requestListener)
    createKafkaApis().handleTopicMetadataRequest(requestChannelRequest)

    readResponse(ApiKeys.METADATA, metadataRequest, capturedResponse).asInstanceOf[MetadataResponse]
  }

  private def testConsumerListOffsetLatest(isolationLevel: IsolationLevel): Unit = {
    val tp = new TopicPartition("foo", 0)
    val latestOffset = 15L

    val replica = EasyMock.mock(classOf[Replica])
    val log = EasyMock.mock(classOf[Log])
    EasyMock.expect(replicaManager.getLeaderReplicaIfLocal(tp)).andReturn(replica)
    if (isolationLevel == IsolationLevel.READ_UNCOMMITTED)
      EasyMock.expect(replica.highWatermark).andReturn(LogOffsetMetadata(messageOffset = latestOffset))
    else
      EasyMock.expect(replica.lastStableOffset).andReturn(LogOffsetMetadata(messageOffset = latestOffset))

    val capturedResponse = expectNoThrottling()

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

  private def buildRequest[T <: AbstractRequest](builder: AbstractRequest.Builder[T],
      listenerName: ListenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)): (T, RequestChannel.Request) = {

    val request = builder.build()
    val buffer = request.serialize(new RequestHeader(builder.apiKey, request.version, "", 0))

    // read the header from the buffer first so that the body can be read next from the Request constructor
    val header = RequestHeader.parse(buffer)
    val context = new RequestContext(header, "1", InetAddress.getLocalHost, KafkaPrincipal.ANONYMOUS,
      listenerName, SecurityProtocol.PLAINTEXT)
    (request, new RequestChannel.Request(processor = 1, context = context, startTimeNanos =  0, MemoryPool.NONE, buffer,
      requestChannelMetrics))
  }

  private def readResponse(api: ApiKeys, request: AbstractRequest, capturedResponse: Capture[RequestChannel.Response]): AbstractResponse = {
    val response = capturedResponse.getValue
    assertTrue(s"Unexpected response type: ${response.getClass}", response.isInstanceOf[SendResponse])
    val sendResponse = response.asInstanceOf[SendResponse]
    val send = sendResponse.responseSend
    val channel = new ByteBufferChannel(send.size)
    send.writeTo(channel)
    channel.close()
    channel.buffer.getInt() // read the size
    ResponseHeader.parse(channel.buffer)
    val struct = api.responseSchema(request.version).read(channel.buffer)
    AbstractResponse.parseResponse(api, struct)
  }

  private def expectNoThrottling(): Capture[RequestChannel.Response] = {
    EasyMock.expect(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(EasyMock.anyObject[RequestChannel.Request]()))
      .andReturn(0)
    EasyMock.expect(clientRequestQuotaManager.throttle(EasyMock.anyObject[RequestChannel.Request](), EasyMock.eq(0),
      EasyMock.anyObject[RequestChannel.Response => Unit]()))

    val capturedResponse = EasyMock.newCapture[RequestChannel.Response]()
    EasyMock.expect(requestChannel.sendResponse(EasyMock.capture(capturedResponse)))
    capturedResponse
  }

  private def setupBasicMetadataCache(topic: String, numPartitions: Int): Unit = {
    val replicas = List(0.asInstanceOf[Integer]).asJava
    val partitionState = new UpdateMetadataRequest.PartitionState(1, 0, 1, replicas, 0, replicas, Collections.emptyList())
    val plaintextListener = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val broker = new Broker(0, Seq(new EndPoint("broker0", 9092, SecurityProtocol.PLAINTEXT, plaintextListener)).asJava, "rack")
    val partitions = (0 until numPartitions).map(new TopicPartition(topic, _) -> partitionState).toMap
    val updateMetadataRequest = new UpdateMetadataRequest.Builder(ApiKeys.UPDATE_METADATA.latestVersion, 0,
      0, partitions.asJava, Set(broker).asJava).build()
    metadataCache.updateCache(correlationId = 0, updateMetadataRequest)
  }
}
