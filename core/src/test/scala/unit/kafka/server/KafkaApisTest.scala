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

import java.net.InetAddress
import java.nio.charset.StandardCharsets
import java.util
import java.util.Arrays.asList
import java.util.Random
import java.util.{Collections, Optional}

import kafka.api.{ApiVersion, KAFKA_0_10_2_IV0, KAFKA_2_2_IV1}
import kafka.cluster.Partition
import kafka.controller.KafkaController
import kafka.coordinator.group.{GroupCoordinator, GroupSummary, MemberSummary}
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.network.RequestChannel
import kafka.network.RequestChannel.SendResponse
import kafka.server.QuotaFactory.QuotaManagers
import kafka.utils.{MockTime, TestUtils}
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.{IsolationLevel, TopicPartition}
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocol
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity
import org.apache.kafka.common.message.OffsetDeleteRequestData.{OffsetDeleteRequestPartition, OffsetDeleteRequestTopic, OffsetDeleteRequestTopicCollection}
import org.apache.kafka.common.message.UpdateMetadataRequestData.{UpdateMetadataBroker, UpdateMetadataEndpoint, UpdateMetadataPartitionState}
import org.apache.kafka.common.message._
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record._
import org.apache.kafka.common.replica.ClientMetadata
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests.WriteTxnMarkersRequest.TxnMarkerEntry
import org.apache.kafka.common.requests.{FetchMetadata => JFetchMetadata, _}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.server.authorizer.Authorizer
import org.easymock.EasyMock._
import org.easymock.{Capture, EasyMock, IAnswer}
import org.junit.Assert.{assertArrayEquals, assertEquals, assertNull, assertTrue}
import org.junit.{After, Test}

import scala.collection.JavaConverters._
import scala.collection.{Map, Seq}

class KafkaApisTest {

  private val requestChannel: RequestChannel = EasyMock.createNiceMock(classOf[RequestChannel])
  private val requestChannelMetrics: RequestChannel.Metrics = EasyMock.createNiceMock(classOf[RequestChannel.Metrics])
  private val replicaManager: ReplicaManager = EasyMock.createNiceMock(classOf[ReplicaManager])
  private val groupCoordinator: GroupCoordinator = EasyMock.createNiceMock(classOf[GroupCoordinator])
  private val adminManager: AdminManager = EasyMock.createNiceMock(classOf[AdminManager])
  private val txnCoordinator: TransactionCoordinator = EasyMock.createNiceMock(classOf[TransactionCoordinator])
  private val controller: KafkaController = EasyMock.createNiceMock(classOf[KafkaController])
  private val zkClient: KafkaZkClient = EasyMock.createNiceMock(classOf[KafkaZkClient])
  private val metrics = new Metrics()
  private val brokerId = 1
  private val metadataCache = new MetadataCache(brokerId)
  private val authorizer: Option[Authorizer] = None
  private val clientQuotaManager: ClientQuotaManager = EasyMock.createNiceMock(classOf[ClientQuotaManager])
  private val clientRequestQuotaManager: ClientRequestQuotaManager = EasyMock.createNiceMock(classOf[ClientRequestQuotaManager])
  private val replicaQuotaManager: ReplicationQuotaManager = EasyMock.createNiceMock(classOf[ReplicationQuotaManager])
  private val quotas = QuotaManagers(clientQuotaManager, clientQuotaManager, clientRequestQuotaManager,
    replicaQuotaManager, replicaQuotaManager, replicaQuotaManager, None)
  private val fetchManager: FetchManager = EasyMock.createNiceMock(classOf[FetchManager])
  private val brokerTopicStats = new BrokerTopicStats
  private val clusterId = "clusterId"
  private val time = new MockTime

  @After
  def tearDown(): Unit = {
    quotas.shutdown()
    TestUtils.clearYammerMetrics()
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

      val (offsetCommitRequest, request) = buildRequest(new OffsetCommitRequest.Builder(
        new OffsetCommitRequestData()
          .setGroupId("groupId")
          .setTopics(Collections.singletonList(
            new OffsetCommitRequestData.OffsetCommitRequestTopic()
              .setName(topic)
              .setPartitions(Collections.singletonList(
                new OffsetCommitRequestData.OffsetCommitRequestPartition()
                  .setPartitionIndex(invalidPartitionId)
                  .setCommittedOffset(15)
                  .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                  .setCommittedMetadata(""))
              )
          ))
      ))

      val capturedResponse = expectNoThrottling()
      EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel)
      createKafkaApis().handleOffsetCommitRequest(request)

      val response = readResponse(ApiKeys.OFFSET_COMMIT, offsetCommitRequest, capturedResponse)
        .asInstanceOf[OffsetCommitResponse]
      assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION,
        Errors.forCode(response.data().topics().get(0).partitions().get(0).errorCode()))
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
      val partitionOffsetCommitData = new TxnOffsetCommitRequest.CommittedOffset(15L, "", Optional.empty())
      val (offsetCommitRequest, request) = buildRequest(new TxnOffsetCommitRequest.Builder(
        new TxnOffsetCommitRequestData()
          .setTransactionalId("txnlId")
          .setGroupId("groupId")
          .setProducerId(15L)
          .setProducerEpoch(0.toShort)
          .setTopics(TxnOffsetCommitRequest.getTopics(
            Map(invalidTopicPartition -> partitionOffsetCommitData).asJava))
      ))

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
    val (writeTxnMarkersRequest, request) = createWriteTxnMarkersRequest(asList(topicPartition))
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
    val (writeTxnMarkersRequest, request) = createWriteTxnMarkersRequest(asList(topicPartition))
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
    val (writeTxnMarkersRequest, request) = createWriteTxnMarkersRequest(asList(tp1, tp2))
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
    val (writeTxnMarkersRequest, request) = createWriteTxnMarkersRequest(asList(tp1, tp2))
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
    val request = createWriteTxnMarkersRequest(asList(topicPartition))._2
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
  def testLeaderReplicaIfLocalRaisesFencedLeaderEpoch(): Unit = {
    testListOffsetFailedGetLeaderReplica(Errors.FENCED_LEADER_EPOCH)
  }

  @Test
  def testLeaderReplicaIfLocalRaisesUnknownLeaderEpoch(): Unit = {
    testListOffsetFailedGetLeaderReplica(Errors.UNKNOWN_LEADER_EPOCH)
  }

  @Test
  def testLeaderReplicaIfLocalRaisesNotLeaderForPartition(): Unit = {
    testListOffsetFailedGetLeaderReplica(Errors.NOT_LEADER_FOR_PARTITION)
  }

  @Test
  def testLeaderReplicaIfLocalRaisesUnknownTopicOrPartition(): Unit = {
    testListOffsetFailedGetLeaderReplica(Errors.UNKNOWN_TOPIC_OR_PARTITION)
  }

  @Test
  def testDescribeGroups(): Unit = {
    val groupId = "groupId"
    val random = new Random()
    val metadata = new Array[Byte](10)
    random.nextBytes(metadata)
    val assignment = new Array[Byte](10)
    random.nextBytes(assignment)

    val memberSummary = MemberSummary("memberid", Some("instanceid"), "clientid", "clienthost", metadata, assignment)
    val groupSummary = GroupSummary("Stable", "consumer", "roundrobin", List(memberSummary))

    EasyMock.reset(groupCoordinator, replicaManager, clientRequestQuotaManager, requestChannel)

    val (describeGroupsRequest, request) = buildRequest(new DescribeGroupsRequest.Builder(
      new DescribeGroupsRequestData().setGroups(List(groupId).asJava)
    ))

    val capturedResponse = expectNoThrottling()
    EasyMock.expect(groupCoordinator.handleDescribeGroup(EasyMock.eq(groupId)))
      .andReturn((Errors.NONE, groupSummary))
    EasyMock.replay(groupCoordinator, replicaManager, clientRequestQuotaManager, requestChannel)

    createKafkaApis().handleDescribeGroupRequest(request)

    val response = readResponse(ApiKeys.DESCRIBE_GROUPS, describeGroupsRequest, capturedResponse)
      .asInstanceOf[DescribeGroupsResponse]

    val group = response.data().groups().get(0)
    assertEquals(Errors.NONE, Errors.forCode(group.errorCode()))
    assertEquals(groupId, group.groupId())
    assertEquals(groupSummary.state, group.groupState())
    assertEquals(groupSummary.protocolType, group.protocolType())
    assertEquals(groupSummary.protocol, group.protocolData())
    assertEquals(groupSummary.members.size, group.members().size())

    val member = group.members().get(0)
    assertEquals(memberSummary.memberId, member.memberId())
    assertEquals(memberSummary.groupInstanceId.orNull, member.groupInstanceId())
    assertEquals(memberSummary.clientId, member.clientId())
    assertEquals(memberSummary.clientHost, member.clientHost())
    assertArrayEquals(memberSummary.metadata, member.memberMetadata())
    assertArrayEquals(memberSummary.assignment, member.memberAssignment())
  }

  @Test
  def testOffsetDeleteWithInvalidPartition(): Unit = {
    val group = "groupId"
    val topic = "topic"
    setupBasicMetadataCache(topic, numPartitions = 1)

    def checkInvalidPartition(invalidPartitionId: Int): Unit = {
      EasyMock.reset(groupCoordinator, replicaManager, clientRequestQuotaManager, requestChannel)

      val topics = new OffsetDeleteRequestTopicCollection()
      topics.add(new OffsetDeleteRequestTopic()
        .setName(topic)
        .setPartitions(Collections.singletonList(
          new OffsetDeleteRequestPartition().setPartitionIndex(invalidPartitionId))))
      val (offsetDeleteRequest, request) = buildRequest(new OffsetDeleteRequest.Builder(
        new OffsetDeleteRequestData()
          .setGroupId(group)
          .setTopics(topics)
      ))

      val capturedResponse = expectNoThrottling()
      EasyMock.expect(groupCoordinator.handleDeleteOffsets(EasyMock.eq(group), EasyMock.eq(Seq.empty)))
        .andReturn((Errors.NONE, Map.empty))
      EasyMock.replay(groupCoordinator, replicaManager, clientRequestQuotaManager, requestChannel)

      createKafkaApis().handleOffsetDeleteRequest(request)

      val response = readResponse(ApiKeys.OFFSET_DELETE, offsetDeleteRequest, capturedResponse)
        .asInstanceOf[OffsetDeleteResponse]

      assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION,
        Errors.forCode(response.data.topics.find(topic).partitions.find(invalidPartitionId).errorCode()))
    }

    checkInvalidPartition(-1)
    checkInvalidPartition(1) // topic has only one partition
  }

  @Test
  def testOffsetDeleteWithInvalidGroup(): Unit = {
    val group = "groupId"

    EasyMock.reset(groupCoordinator, replicaManager, clientRequestQuotaManager, requestChannel)

    val (offsetDeleteRequest, request) = buildRequest(new OffsetDeleteRequest.Builder(
      new OffsetDeleteRequestData()
        .setGroupId(group)
    ))

    val capturedResponse = expectNoThrottling()
    EasyMock.expect(groupCoordinator.handleDeleteOffsets(EasyMock.eq(group), EasyMock.eq(Seq.empty)))
      .andReturn((Errors.GROUP_ID_NOT_FOUND, Map.empty))
    EasyMock.replay(groupCoordinator, replicaManager, clientRequestQuotaManager, requestChannel)

    createKafkaApis().handleOffsetDeleteRequest(request)

    val response = readResponse(ApiKeys.OFFSET_DELETE, offsetDeleteRequest, capturedResponse)
      .asInstanceOf[OffsetDeleteResponse]

    assertEquals(Errors.GROUP_ID_NOT_FOUND, Errors.forCode(response.data.errorCode()))
  }

  private def testListOffsetFailedGetLeaderReplica(error: Errors): Unit = {
    val tp = new TopicPartition("foo", 0)
    val isolationLevel = IsolationLevel.READ_UNCOMMITTED
    val currentLeaderEpoch = Optional.of[Integer](15)

    EasyMock.expect(replicaManager.fetchOffsetForTimestamp(
      EasyMock.eq(tp),
      EasyMock.eq(ListOffsetRequest.EARLIEST_TIMESTAMP),
      EasyMock.eq(Some(isolationLevel)),
      EasyMock.eq(currentLeaderEpoch),
      fetchOnlyFromLeader = EasyMock.eq(true))
    ).andThrow(error.exception)

    val capturedResponse = expectNoThrottling()
    EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel)

    val targetTimes = Map(tp -> new ListOffsetRequest.PartitionData(ListOffsetRequest.EARLIEST_TIMESTAMP,
      currentLeaderEpoch))
    val builder = ListOffsetRequest.Builder.forConsumer(true, isolationLevel)
      .setTargetTimes(targetTimes.asJava)
    val (listOffsetRequest, request) = buildRequest(builder)
    createKafkaApis().handleListOffsetRequest(request)

    val response = readResponse(ApiKeys.LIST_OFFSETS, listOffsetRequest, capturedResponse)
      .asInstanceOf[ListOffsetResponse]
    assertTrue(response.responseData.containsKey(tp))

    val partitionData = response.responseData.get(tp)
    assertEquals(error, partitionData.error)
    assertEquals(ListOffsetResponse.UNKNOWN_OFFSET, partitionData.offset)
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
   * Verifies that sending a fetch request with version 9 works correctly when
   * ReplicaManager.getLogConfig returns None.
   */
  @Test
  def testFetchRequestV9WithNoLogConfig(): Unit = {
    val tp = new TopicPartition("foo", 0)
    setupBasicMetadataCache(tp.topic, numPartitions = 1)
    val hw = 3
    val timestamp = 1000

    expect(replicaManager.getLogConfig(EasyMock.eq(tp))).andReturn(None)

    replicaManager.fetchMessages(anyLong, anyInt, anyInt, anyInt, anyBoolean,
      anyObject[Seq[(TopicPartition, FetchRequest.PartitionData)]], anyObject[ReplicaQuota],
      anyObject[Seq[(TopicPartition, FetchPartitionData)] => Unit](), anyObject[IsolationLevel],
      anyObject[Option[ClientMetadata]])
    expectLastCall[Unit].andAnswer(new IAnswer[Unit] {
      def answer: Unit = {
        val callback = getCurrentArguments.apply(7).asInstanceOf[(Seq[(TopicPartition, FetchPartitionData)] => Unit)]
        val records = MemoryRecords.withRecords(CompressionType.NONE,
          new SimpleRecord(timestamp, "foo".getBytes(StandardCharsets.UTF_8)))
        callback(Seq(tp -> FetchPartitionData(Errors.NONE, hw, 0, records,
          None, None, Option.empty, isReassignmentFetch = false, 0)))
      }
    })

    val fetchData = Map(tp -> new FetchRequest.PartitionData(0, 0, 1000,
      Optional.empty())).asJava
    val fetchMetadata = new JFetchMetadata(0, 0)
    val fetchContext = new FullFetchContext(time, new FetchSessionCache(1000, 100),
      fetchMetadata, fetchData, false)
    expect(fetchManager.newContext(anyObject[JFetchMetadata],
      anyObject[util.Map[TopicPartition, FetchRequest.PartitionData]],
      anyObject[util.List[TopicPartition]],
      anyBoolean)).andReturn(fetchContext)

    val capturedResponse = expectNoThrottling()
    EasyMock.expect(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      anyObject[RequestChannel.Request](), anyDouble, anyLong)).andReturn(0)

    EasyMock.replay(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, fetchManager)

    val builder = new FetchRequest.Builder(9, 9, -1, 100, 0, fetchData)
    val (fetchRequest, request) = buildRequest(builder)
    createKafkaApis().handleFetchRequest(request)

    val response = readResponse(ApiKeys.FETCH, fetchRequest, capturedResponse)
      .asInstanceOf[FetchResponse[BaseRecords]]
    assertTrue(response.responseData.containsKey(tp))

    val partitionData = response.responseData.get(tp)
    assertEquals(Errors.NONE, partitionData.error)
    assertEquals(hw, partitionData.highWatermark)
    assertEquals(-1, partitionData.lastStableOffset)
    assertEquals(0, partitionData.logStartOffset)
    assertEquals(timestamp,
      partitionData.records.asInstanceOf[MemoryRecords].batches.iterator.next.maxTimestamp)
    assertNull(partitionData.abortedTransactions)
  }

  @Test
  def testJoinGroupProtocolsOrder(): Unit = {
    val protocols = List(
      new JoinGroupRequestProtocol().setName("first").setMetadata("first".getBytes()),
      new JoinGroupRequestProtocol().setName("second").setMetadata("second".getBytes())
    )

    EasyMock.expect(groupCoordinator.handleJoinGroup(
      anyString,
      anyString,
      anyObject(classOf[Option[String]]),
      anyBoolean,
      anyString,
      anyString,
      anyInt,
      anyInt,
      anyString,
      EasyMock.eq(protocols.map(protocol => (protocol.name, protocol.metadata))),
      anyObject()
    ))

    createKafkaApis().handleJoinGroupRequest(
      buildRequest(
        new JoinGroupRequest.Builder(
          new JoinGroupRequestData()
            .setGroupId("test")
            .setMemberId("test")
            .setProtocolType("consumer")
            .setProtocols(new JoinGroupRequestData.JoinGroupRequestProtocolCollection(protocols.iterator.asJava))
        )
      )._2)

    EasyMock.replay(groupCoordinator)
  }

  @Test
  def rejectJoinGroupRequestWhenStaticMembershipNotSupported(): Unit = {
    val capturedResponse = expectNoThrottling()
    EasyMock.replay(clientRequestQuotaManager, requestChannel)

    val (joinGroupRequest, requestChannelRequest) = buildRequest(new JoinGroupRequest.Builder(
      new JoinGroupRequestData()
        .setGroupId("test")
        .setMemberId("test")
        .setGroupInstanceId("instanceId")
        .setProtocolType("consumer")
        .setProtocols(new JoinGroupRequestData.JoinGroupRequestProtocolCollection)
    ))
    createKafkaApis(KAFKA_2_2_IV1).handleJoinGroupRequest(requestChannelRequest)

    val response = readResponse(ApiKeys.JOIN_GROUP, joinGroupRequest, capturedResponse).asInstanceOf[JoinGroupResponse]
    assertEquals(Errors.UNSUPPORTED_VERSION, response.error())
    EasyMock.replay(groupCoordinator)
  }

  @Test
  def rejectSyncGroupRequestWhenStaticMembershipNotSupported(): Unit = {
    val capturedResponse = expectNoThrottling()
    EasyMock.replay(clientRequestQuotaManager, requestChannel)

    val (syncGroupRequest, requestChannelRequest) = buildRequest(new SyncGroupRequest.Builder(
      new SyncGroupRequestData()
        .setGroupId("test")
        .setMemberId("test")
        .setGroupInstanceId("instanceId")
        .setGenerationId(1)
    ))
    createKafkaApis(KAFKA_2_2_IV1).handleSyncGroupRequest(requestChannelRequest)

    val response = readResponse(ApiKeys.SYNC_GROUP, syncGroupRequest, capturedResponse).asInstanceOf[SyncGroupResponse]
    assertEquals(Errors.UNSUPPORTED_VERSION, response.error())
    EasyMock.replay(groupCoordinator)
  }

  @Test
  def rejectHeartbeatRequestWhenStaticMembershipNotSupported(): Unit = {
    val capturedResponse = expectNoThrottling()
    EasyMock.replay(clientRequestQuotaManager, requestChannel)

    val (heartbeatRequest, requestChannelRequest) = buildRequest(new HeartbeatRequest.Builder(
      new HeartbeatRequestData()
        .setGroupId("test")
        .setMemberId("test")
        .setGroupInstanceId("instanceId")
        .setGenerationId(1)
    ))
    createKafkaApis(KAFKA_2_2_IV1).handleHeartbeatRequest(requestChannelRequest)

    val response = readResponse(ApiKeys.HEARTBEAT, heartbeatRequest, capturedResponse).asInstanceOf[HeartbeatResponse]
    assertEquals(Errors.UNSUPPORTED_VERSION, response.error())
    EasyMock.replay(groupCoordinator)
  }

  @Test
  def rejectOffsetCommitRequestWhenStaticMembershipNotSupported(): Unit = {
    val capturedResponse = expectNoThrottling()
    EasyMock.replay(clientRequestQuotaManager, requestChannel)

    val (offsetCommitRequest, requestChannelRequest) = buildRequest(new OffsetCommitRequest.Builder(
      new OffsetCommitRequestData()
        .setGroupId("test")
        .setMemberId("test")
        .setGroupInstanceId("instanceId")
        .setGenerationId(100)
        .setTopics(Collections.singletonList(
          new OffsetCommitRequestData.OffsetCommitRequestTopic()
            .setName("test")
            .setPartitions(Collections.singletonList(
              new OffsetCommitRequestData.OffsetCommitRequestPartition()
                .setPartitionIndex(0)
                .setCommittedOffset(100)
                .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                .setCommittedMetadata("")
            ))
        ))
    ))
    createKafkaApis(KAFKA_2_2_IV1).handleOffsetCommitRequest(requestChannelRequest)

    val expectedTopicErrors = Collections.singletonList(
      new OffsetCommitResponseData.OffsetCommitResponseTopic()
        .setName("test")
        .setPartitions(Collections.singletonList(
          new OffsetCommitResponseData.OffsetCommitResponsePartition()
            .setPartitionIndex(0)
            .setErrorCode(Errors.UNSUPPORTED_VERSION.code())
        ))
    )
    val response = readResponse(ApiKeys.OFFSET_COMMIT, offsetCommitRequest, capturedResponse).asInstanceOf[OffsetCommitResponse]
    assertEquals(expectedTopicErrors, response.data.topics())
    EasyMock.replay(groupCoordinator)
  }

  @Test
  def testMultipleLeaveGroup(): Unit = {
    val groupId = "groupId"

    val leaveMemberList = List(
      new MemberIdentity()
        .setMemberId("member-1")
        .setGroupInstanceId("instance-1"),
      new MemberIdentity()
        .setMemberId("member-2")
        .setGroupInstanceId("instance-2")
    )

    EasyMock.expect(groupCoordinator.handleLeaveGroup(
      EasyMock.eq(groupId),
      EasyMock.eq(leaveMemberList),
      anyObject()
    ))

    val (_, leaveRequest) = buildRequest(
      new LeaveGroupRequest.Builder(
        groupId,
        leaveMemberList.asJava)
    )

    createKafkaApis().handleLeaveGroupRequest(leaveRequest)

    EasyMock.replay(groupCoordinator)
  }

  @Test
  def testSingleLeaveGroup(): Unit = {
    val groupId = "groupId"
    val memberId = "member"

    val singleLeaveMember = List(
      new MemberIdentity()
        .setMemberId(memberId)
    )

    EasyMock.expect(groupCoordinator.handleLeaveGroup(
      EasyMock.eq(groupId),
      EasyMock.eq(singleLeaveMember),
      anyObject()
    ))

    val (_, leaveRequest) = buildRequest(
      new LeaveGroupRequest.Builder(
        groupId,
        singleLeaveMember.asJava)
    )

    createKafkaApis().handleLeaveGroupRequest(leaveRequest)

    EasyMock.replay(groupCoordinator)
  }

  @Test
  def testReassignmentAndReplicationBytesOutRateWhenReassigning(): Unit = {
    assertReassignmentAndReplicationBytesOutPerSec(true)
  }

  @Test
  def testReassignmentAndReplicationBytesOutRateWhenNotReassigning(): Unit = {
    assertReassignmentAndReplicationBytesOutPerSec(false)
  }

  private def assertReassignmentAndReplicationBytesOutPerSec(isReassigning: Boolean) {
    val leaderEpoch = 0
    val tp0 = new TopicPartition("tp", 0)

    val fetchData = Collections.singletonMap(tp0, new FetchRequest.PartitionData(0,0, Int.MaxValue, Optional.of(leaderEpoch)))
    val (_, fetchFromFollower) = buildRequest(new FetchRequest.Builder(
      ApiKeys.FETCH.oldestVersion(), ApiKeys.FETCH.latestVersion(), 1, 1000, 0, fetchData))

    setupBasicMetadataCache(tp0.topic, numPartitions = 1)
    val hw = 3

    val records = MemoryRecords.withRecords(CompressionType.NONE,
      new SimpleRecord(1000, "foo".getBytes(StandardCharsets.UTF_8)))
    replicaManager.fetchMessages(anyLong, anyInt, anyInt, anyInt, anyBoolean,
      anyObject[Seq[(TopicPartition, FetchRequest.PartitionData)]], anyObject[ReplicaQuota],
      anyObject[Seq[(TopicPartition, FetchPartitionData)] => Unit](), anyObject[IsolationLevel],
      anyObject[Option[ClientMetadata]])
    expectLastCall[Unit].andAnswer(new IAnswer[Unit] {
      def answer: Unit = {
        val callback = getCurrentArguments.apply(7).asInstanceOf[Seq[(TopicPartition, FetchPartitionData)] => Unit]
        callback(Seq(tp0 -> FetchPartitionData(Errors.NONE, hw, 0, records, None, None, Option.empty, isReassignmentFetch = isReassigning, 0)))
      }
    })

    val fetchMetadata = new JFetchMetadata(0, 0)
    val fetchContext = new FullFetchContext(time, new FetchSessionCache(1000, 100),
      fetchMetadata, fetchData, true)
    expect(fetchManager.newContext(anyObject[JFetchMetadata],
      anyObject[util.Map[TopicPartition, FetchRequest.PartitionData]],
      anyObject[util.List[TopicPartition]],
      anyBoolean)).andReturn(fetchContext)

    expect(replicaQuotaManager.record(anyLong()))
    expect(replicaManager.getLogConfig(EasyMock.eq(tp0))).andReturn(None)

    val partition: Partition = createNiceMock(classOf[Partition])
    expect(replicaManager.isAddingReplica(anyObject(), anyInt())).andReturn(isReassigning)

    replay(replicaManager, fetchManager, clientQuotaManager, requestChannel, replicaQuotaManager, partition)

    createKafkaApis().handle(fetchFromFollower)

    if (isReassigning)
      assertEquals(records.sizeInBytes(), brokerTopicStats.allTopicsStats.reassignmentBytesOutPerSec.get.count())
    else
      assertEquals(0, brokerTopicStats.allTopicsStats.reassignmentBytesOutPerSec.get.count())
    assertEquals(records.sizeInBytes(), brokerTopicStats.allTopicsStats.replicationBytesOutRate.get.count())

  }

  /**
   * Return pair of listener names in the metadataCache: PLAINTEXT and LISTENER2 respectively.
   */
  private def updateMetadataCacheWithInconsistentListeners(): (ListenerName, ListenerName) = {
    val plaintextListener = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val anotherListener = new ListenerName("LISTENER2")
    val brokers = Seq(
      new UpdateMetadataBroker()
        .setId(0)
        .setRack("rack")
        .setEndpoints(Seq(
          new UpdateMetadataEndpoint()
            .setHost("broker0")
            .setPort(9092)
            .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
            .setListener(plaintextListener.value),
          new UpdateMetadataEndpoint()
            .setHost("broker0")
            .setPort(9093)
            .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
            .setListener(anotherListener.value)
        ).asJava),
      new UpdateMetadataBroker()
        .setId(1)
        .setRack("rack")
        .setEndpoints(Seq(
          new UpdateMetadataEndpoint()
            .setHost("broker1")
            .setPort(9092)
            .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
            .setListener(plaintextListener.value)).asJava)
    )
    val updateMetadataRequest = new UpdateMetadataRequest.Builder(ApiKeys.UPDATE_METADATA.latestVersion, 0,
      0, 0, Seq.empty[UpdateMetadataPartitionState].asJava, brokers.asJava).build()
    metadataCache.updateMetadata(correlationId = 0, updateMetadataRequest)
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
    val currentLeaderEpoch = Optional.empty[Integer]()

    EasyMock.expect(replicaManager.fetchOffsetForTimestamp(
      EasyMock.eq(tp),
      EasyMock.eq(ListOffsetRequest.LATEST_TIMESTAMP),
      EasyMock.eq(Some(isolationLevel)),
      EasyMock.eq(currentLeaderEpoch),
      fetchOnlyFromLeader = EasyMock.eq(true))
    ).andReturn(Some(new TimestampAndOffset(ListOffsetResponse.UNKNOWN_TIMESTAMP, latestOffset, currentLeaderEpoch)))

    val capturedResponse = expectNoThrottling()
    EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel)

    val targetTimes = Map(tp -> new ListOffsetRequest.PartitionData(ListOffsetRequest.LATEST_TIMESTAMP,
      currentLeaderEpoch))
    val builder = ListOffsetRequest.Builder.forConsumer(true, isolationLevel)
      .setTargetTimes(targetTimes.asJava)
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
    val requestBuilder = new WriteTxnMarkersRequest.Builder(asList(
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
    (request, new RequestChannel.Request(processor = 1, context = context, startTimeNanos = 0, MemoryPool.NONE, buffer,
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
    ResponseHeader.parse(channel.buffer, api.responseHeaderVersion(request.version()))
    val struct = api.responseSchema(request.version).read(channel.buffer)
    AbstractResponse.parseResponse(api, struct, request.version)
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

    def createPartitionState(partition: Int) = new UpdateMetadataPartitionState()
      .setTopicName(topic)
      .setPartitionIndex(partition)
      .setControllerEpoch(1)
      .setLeader(0)
      .setLeaderEpoch(1)
      .setReplicas(replicas)
      .setZkVersion(0)
      .setReplicas(replicas)

    val plaintextListener = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val broker = new UpdateMetadataBroker()
      .setId(0)
      .setRack("rack")
      .setEndpoints(Seq(new UpdateMetadataEndpoint()
        .setHost("broker0")
        .setPort(9092)
        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
        .setListener(plaintextListener.value)).asJava)
    val partitionStates = (0 until numPartitions).map(createPartitionState)
    val updateMetadataRequest = new UpdateMetadataRequest.Builder(ApiKeys.UPDATE_METADATA.latestVersion, 0,
      0, 0, partitionStates.asJava, Seq(broker).asJava).build()
    metadataCache.updateMetadata(correlationId = 0, updateMetadataRequest)
  }
}
