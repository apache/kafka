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
import java.util.{Collections, Optional, Random}
import java.util.concurrent.TimeUnit

import kafka.api.LeaderAndIsr
import kafka.api.{ApiVersion, KAFKA_0_10_2_IV0, KAFKA_2_2_IV1}
import kafka.cluster.Partition
import kafka.controller.KafkaController
import kafka.coordinator.group.GroupOverview
import kafka.coordinator.group.GroupCoordinatorConcurrencyTest.JoinGroupCallback
import kafka.coordinator.group.GroupCoordinatorConcurrencyTest.SyncGroupCallback
import kafka.coordinator.group.JoinGroupResult
import kafka.coordinator.group.SyncGroupResult
import kafka.coordinator.group.{GroupCoordinator, GroupSummary, MemberSummary}
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.log.AppendOrigin
import kafka.network.RequestChannel
import kafka.network.RequestChannel.SendResponse
import kafka.server.QuotaFactory.QuotaManagers
import kafka.utils.{MockTime, TestUtils}
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.{IsolationLevel, Node, TopicPartition}
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.AlterableConfig
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocol
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity
import org.apache.kafka.common.message.OffsetDeleteRequestData.{OffsetDeleteRequestPartition, OffsetDeleteRequestTopic, OffsetDeleteRequestTopicCollection}
import org.apache.kafka.common.message.StopReplicaRequestData.{StopReplicaPartitionState, StopReplicaTopicState}
import org.apache.kafka.common.message.UpdateMetadataRequestData.{UpdateMetadataBroker, UpdateMetadataEndpoint, UpdateMetadataPartitionState}
import org.apache.kafka.common.message._
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ClientInformation
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record._
import org.apache.kafka.common.replica.ClientMetadata
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests.WriteTxnMarkersRequest.TxnMarkerEntry
import org.apache.kafka.common.requests.{FetchMetadata => JFetchMetadata, _}
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.server.authorizer.Action
import org.apache.kafka.server.authorizer.AuthorizationResult
import org.apache.kafka.server.authorizer.Authorizer
import org.easymock.EasyMock._
import org.easymock.{Capture, EasyMock, IAnswer, IArgumentMatcher}
import org.junit.Assert.{assertArrayEquals, assertEquals, assertNull, assertTrue}
import org.junit.{After, Test}

import scala.jdk.CollectionConverters._
import scala.collection.{Map, Seq, mutable}

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
  private val clientQuotaManager: ClientQuotaManager = EasyMock.createNiceMock(classOf[ClientQuotaManager])
  private val clientRequestQuotaManager: ClientRequestQuotaManager = EasyMock.createNiceMock(classOf[ClientRequestQuotaManager])
  private val replicaQuotaManager: ReplicationQuotaManager = EasyMock.createNiceMock(classOf[ReplicationQuotaManager])
  private val quotas = QuotaManagers(clientQuotaManager, clientQuotaManager, clientRequestQuotaManager,
    replicaQuotaManager, replicaQuotaManager, replicaQuotaManager, None)
  private val fetchManager: FetchManager = EasyMock.createNiceMock(classOf[FetchManager])
  private val brokerTopicStats = new BrokerTopicStats
  private val clusterId = "clusterId"
  private val time = new MockTime
  private val clientId = ""

  @After
  def tearDown(): Unit = {
    quotas.shutdown()
    TestUtils.clearYammerMetrics()
    metrics.close()
  }

  def createKafkaApis(interBrokerProtocolVersion: ApiVersion = ApiVersion.latestVersion,
                      authorizer: Option[Authorizer] = None): KafkaApis = {
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
  def testAuthorize(): Unit = {
    val authorizer: Authorizer = EasyMock.niceMock(classOf[Authorizer])

    val operation = AclOperation.WRITE
    val resourceType = ResourceType.TOPIC
    val resourceName = "topic-1"
    val requestHeader = new RequestHeader(ApiKeys.PRODUCE, ApiKeys.PRODUCE.latestVersion,
      clientId, 0)
    val requestContext = new RequestContext(requestHeader, "1", InetAddress.getLocalHost,
      KafkaPrincipal.ANONYMOUS, ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
      SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY)

    val expectedActions = Seq(
      new Action(operation, new ResourcePattern(resourceType, resourceName, PatternType.LITERAL),
        1, true, true)
    )

    EasyMock.expect(authorizer.authorize(requestContext, expectedActions.asJava))
      .andReturn(Seq(AuthorizationResult.ALLOWED).asJava)
      .once()

    EasyMock.replay(authorizer)

    val result = createKafkaApis(authorizer = Some(authorizer)).authorize(
      requestContext, operation, resourceType, resourceName)

    verify(authorizer)

    assertEquals(true, result)
  }

  @Test
  def testFilterByAuthorized(): Unit = {
    val authorizer: Authorizer = EasyMock.niceMock(classOf[Authorizer])

    val operation = AclOperation.WRITE
    val resourceType = ResourceType.TOPIC
    val resourceName1 = "topic-1"
    val resourceName2 = "topic-2"
    val resourceName3 = "topic-3"
    val requestHeader = new RequestHeader(ApiKeys.PRODUCE, ApiKeys.PRODUCE.latestVersion,
      clientId, 0)
    val requestContext = new RequestContext(requestHeader, "1", InetAddress.getLocalHost,
      KafkaPrincipal.ANONYMOUS, ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
      SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY)

    val expectedActions = Seq(
      new Action(operation, new ResourcePattern(resourceType, resourceName1, PatternType.LITERAL),
        2, true, true),
      new Action(operation, new ResourcePattern(resourceType, resourceName2, PatternType.LITERAL),
        1, true, true),
      new Action(operation, new ResourcePattern(resourceType, resourceName3, PatternType.LITERAL),
        1, true, true),
    )

    EasyMock.expect(authorizer.authorize(
      EasyMock.eq(requestContext), matchSameElements(expectedActions.asJava)
    )).andAnswer { () =>
      val actions = EasyMock.getCurrentArguments.apply(1).asInstanceOf[util.List[Action]].asScala
      actions.map { action =>
        if (Set(resourceName1, resourceName3).contains(action.resourcePattern.name))
          AuthorizationResult.ALLOWED
        else
          AuthorizationResult.DENIED
      }.asJava
    }.once()

    EasyMock.replay(authorizer)

    val result = createKafkaApis(authorizer = Some(authorizer)).filterByAuthorized(
      requestContext,
      operation,
      resourceType,
      // Duplicate resource names should not trigger multiple calls to authorize
      Seq(resourceName1, resourceName2, resourceName1, resourceName3)
    )(identity)

    verify(authorizer)

    assertEquals(Set(resourceName1, resourceName3), result)
  }

  /**
   * Returns true if the elements in both lists are the same irrespective of ordering.
   */
  private def matchSameElements[T](list: util.List[T]): util.List[T] = {
    EasyMock.reportMatcher(new IArgumentMatcher {
      def matches(argument: Any): Boolean = argument match {
        case s: util.List[_] => s.asScala.toSet == list.asScala.toSet
        case _ => false
      }
      def appendTo(buffer: StringBuffer): Unit = buffer.append(s"list($list)")
    })
    null
  }

  @Test
  def testDescribeConfigsWithAuthorizer(): Unit = {
    val authorizer: Authorizer = EasyMock.niceMock(classOf[Authorizer])

    val operation = AclOperation.DESCRIBE_CONFIGS
    val resourceType = ResourceType.TOPIC
    val resourceName = "topic-1"
    val requestHeader = new RequestHeader(ApiKeys.DESCRIBE_CONFIGS, ApiKeys.DESCRIBE_CONFIGS.latestVersion,
      clientId, 0)

    val expectedActions = Seq(
      new Action(operation, new ResourcePattern(resourceType, resourceName, PatternType.LITERAL),
        1, true, true)
    )

    // Verify that authorize is only called once
    EasyMock.expect(authorizer.authorize(anyObject[RequestContext], EasyMock.eq(expectedActions.asJava)))
      .andReturn(Seq(AuthorizationResult.ALLOWED).asJava)
      .once()

    expectNoThrottling()

    val configResource = new ConfigResource(ConfigResource.Type.TOPIC, resourceName)
    val config = new DescribeConfigsResponse.Config(ApiError.NONE, Collections.emptyList[DescribeConfigsResponse.ConfigEntry])
    EasyMock.expect(adminManager.describeConfigs(anyObject(), EasyMock.eq(true), EasyMock.eq(false)))
        .andReturn(Map(configResource -> config))

    EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, authorizer,
      adminManager)

    val resourceToConfigNames = Map[ConfigResource, util.Collection[String]](
      configResource -> Collections.emptyList[String])
    val request = buildRequest(new DescribeConfigsRequest(requestHeader.apiVersion,
      resourceToConfigNames.asJava, true))
    createKafkaApis(authorizer = Some(authorizer)).handleDescribeConfigsRequest(request)

    verify(authorizer, adminManager)
  }

  @Test
  def testAlterConfigsWithAuthorizer(): Unit = {
    val authorizer: Authorizer = EasyMock.niceMock(classOf[Authorizer])

    val operation = AclOperation.ALTER_CONFIGS
    val resourceType = ResourceType.TOPIC
    val resourceName = "topic-1"
    val requestHeader = new RequestHeader(ApiKeys.ALTER_CONFIGS, ApiKeys.ALTER_CONFIGS.latestVersion,
      clientId, 0)

    val expectedActions = Seq(
      new Action(operation, new ResourcePattern(resourceType, resourceName, PatternType.LITERAL),
        1, true, true)
    )

    // Verify that authorize is only called once
    EasyMock.expect(authorizer.authorize(anyObject[RequestContext], EasyMock.eq(expectedActions.asJava)))
      .andReturn(Seq(AuthorizationResult.ALLOWED).asJava)
      .once()

    expectNoThrottling()

    val configResource = new ConfigResource(ConfigResource.Type.TOPIC, resourceName)
    EasyMock.expect(adminManager.alterConfigs(anyObject(), EasyMock.eq(false)))
      .andReturn(Map(configResource -> ApiError.NONE))

    EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, authorizer,
      adminManager)

    val configs = Map(
      configResource -> new AlterConfigsRequest.Config(
        Seq(new AlterConfigsRequest.ConfigEntry("foo", "bar")).asJava))
    val request = buildRequest(new AlterConfigsRequest.Builder(configs.asJava, false)
      .build(requestHeader.apiVersion))
    createKafkaApis(authorizer = Some(authorizer)).handleAlterConfigsRequest(request)

    verify(authorizer, adminManager)
  }

  @Test
  def testIncrementalAlterConfigsWithAuthorizer(): Unit = {
    val authorizer: Authorizer = EasyMock.niceMock(classOf[Authorizer])

    val operation = AclOperation.ALTER_CONFIGS
    val resourceType = ResourceType.TOPIC
    val resourceName = "topic-1"
    val requestHeader = new RequestHeader(ApiKeys.INCREMENTAL_ALTER_CONFIGS,
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.latestVersion, clientId, 0)

    val expectedActions = Seq(
      new Action(operation, new ResourcePattern(resourceType, resourceName, PatternType.LITERAL),
        1, true, true)
    )

    // Verify that authorize is only called once
    EasyMock.expect(authorizer.authorize(anyObject[RequestContext], EasyMock.eq(expectedActions.asJava)))
      .andReturn(Seq(AuthorizationResult.ALLOWED).asJava)
      .once()

    expectNoThrottling()

    val configResource = new ConfigResource(ConfigResource.Type.TOPIC, resourceName)
    EasyMock.expect(adminManager.incrementalAlterConfigs(anyObject(), EasyMock.eq(false)))
      .andReturn(Map(configResource -> ApiError.NONE))

    EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, authorizer,
      adminManager)

    val requestData = new IncrementalAlterConfigsRequestData()
    val alterResource = new IncrementalAlterConfigsRequestData.AlterConfigsResource()
      .setResourceName(configResource.name)
      .setResourceType(configResource.`type`.id)
    alterResource.configs.add(new AlterableConfig()
      .setName("foo")
      .setValue("bar"))
    requestData.resources.add(alterResource)

    val request = buildRequest(new IncrementalAlterConfigsRequest.Builder(requestData)
      .build(requestHeader.apiVersion))
    createKafkaApis(authorizer = Some(authorizer)).handleIncrementalAlterConfigsRequest(request)

    verify(authorizer, adminManager)
  }

  @Test
  def testOffsetCommitWithInvalidPartition(): Unit = {
    val topic = "topic"
    setupBasicMetadataCache(topic, numPartitions = 1)

    def checkInvalidPartition(invalidPartitionId: Int): Unit = {
      EasyMock.reset(replicaManager, clientRequestQuotaManager, requestChannel)

      val offsetCommitRequest = new OffsetCommitRequest.Builder(
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
          ))).build()

      val request = buildRequest(offsetCommitRequest)
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
      val offsetCommitRequest = new TxnOffsetCommitRequest.Builder(
        "txnId",
        "groupId",
        15L,
        0.toShort,
        Map(invalidTopicPartition -> partitionOffsetCommitData).asJava,
        false
      ).build()
      val request = buildRequest(offsetCommitRequest)

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
  def shouldReplaceCoordinatorNotAvailableWithLoadInProcessInTxnOffsetCommitWithOlderClient(): Unit = {
    val topic = "topic"
    setupBasicMetadataCache(topic, numPartitions = 2)

    EasyMock.reset(replicaManager, clientRequestQuotaManager, requestChannel, groupCoordinator)

    val topicPartition = new TopicPartition(topic, 1)
    val capturedResponse: Capture[RequestChannel.Response] = EasyMock.newCapture()
    val responseCallback: Capture[Map[TopicPartition, Errors] => Unit] = EasyMock.newCapture()

    val partitionOffsetCommitData = new TxnOffsetCommitRequest.CommittedOffset(15L, "", Optional.empty())
    val groupId = "groupId"

    val offsetCommitRequest = new TxnOffsetCommitRequest.Builder(
      "txnId",
      groupId,
      15L,
      0.toShort,
      Map(topicPartition -> partitionOffsetCommitData).asJava,
      false
    ).build(1)
    val request = buildRequest(offsetCommitRequest)

    EasyMock.expect(groupCoordinator.handleTxnCommitOffsets(
      EasyMock.eq(groupId),
      EasyMock.eq(15L),
      EasyMock.eq(0),
      EasyMock.anyString(),
      EasyMock.eq(Option.empty),
      EasyMock.anyInt(),
      EasyMock.anyObject(),
      EasyMock.capture(responseCallback)
    )).andAnswer(
      () => responseCallback.getValue.apply(Map(topicPartition -> Errors.COORDINATOR_LOAD_IN_PROGRESS)))

    EasyMock.expect(requestChannel.sendResponse(EasyMock.capture(capturedResponse)))

    EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, groupCoordinator)

    createKafkaApis().handleTxnOffsetCommitRequest(request)

    val response = readResponse(ApiKeys.TXN_OFFSET_COMMIT, offsetCommitRequest, capturedResponse)
      .asInstanceOf[TxnOffsetCommitResponse]
    assertEquals(Errors.COORDINATOR_NOT_AVAILABLE, response.errors().get(topicPartition))
  }

  @Test
  def testAddPartitionsToTxnWithInvalidPartition(): Unit = {
    val topic = "topic"
    setupBasicMetadataCache(topic, numPartitions = 1)

    def checkInvalidPartition(invalidPartitionId: Int): Unit = {
      EasyMock.reset(replicaManager, clientRequestQuotaManager, requestChannel)

      val invalidTopicPartition = new TopicPartition(topic, invalidPartitionId)
      val addPartitionsToTxnRequest = new AddPartitionsToTxnRequest.Builder(
        "txnlId", 15L, 0.toShort, List(invalidTopicPartition).asJava
      ).build()
      val request = buildRequest(addPartitionsToTxnRequest)

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
    val responseCallback: Capture[Map[TopicPartition, PartitionResponse] => Unit] = EasyMock.newCapture()

    EasyMock.expect(replicaManager.getMagic(tp1))
      .andReturn(Some(RecordBatch.MAGIC_VALUE_V1))
    EasyMock.expect(replicaManager.getMagic(tp2))
      .andReturn(Some(RecordBatch.MAGIC_VALUE_V2))

    EasyMock.expect(replicaManager.appendRecords(EasyMock.anyLong(),
      EasyMock.anyShort(),
      EasyMock.eq(true),
      EasyMock.eq(AppendOrigin.Coordinator),
      EasyMock.anyObject(),
      EasyMock.capture(responseCallback),
      EasyMock.anyObject(),
      EasyMock.anyObject())
    ).andAnswer(() => responseCallback.getValue.apply(Map(tp2 -> new PartitionResponse(Errors.NONE))))

    EasyMock.expect(requestChannel.sendResponse(EasyMock.capture(capturedResponse)))
    EasyMock.replay(replicaManager, replicaQuotaManager, requestChannel)

    createKafkaApis().handleWriteTxnMarkersRequest(request)

    val markersResponse = readResponse(ApiKeys.WRITE_TXN_MARKERS, writeTxnMarkersRequest, capturedResponse)
      .asInstanceOf[WriteTxnMarkersResponse]
    assertEquals(expectedErrors, markersResponse.errors(1))
    EasyMock.verify(replicaManager)
  }

  @Test
  def shouldResignCoordinatorsIfStopReplicaReceivedWithDeleteFlagAndLeaderEpoch(): Unit = {
    shouldResignCoordinatorsIfStopReplicaReceivedWithDeleteFlag(
      LeaderAndIsr.initialLeaderEpoch + 2, true)
  }

  @Test
  def shouldResignCoordinatorsIfStopReplicaReceivedWithDeleteFlagAndDeleteSentinel(): Unit = {
    shouldResignCoordinatorsIfStopReplicaReceivedWithDeleteFlag(
      LeaderAndIsr.EpochDuringDelete, true)
  }

  @Test
  def shouldResignCoordinatorsIfStopReplicaReceivedWithDeleteFlagAndNoEpochSentinel(): Unit = {
    shouldResignCoordinatorsIfStopReplicaReceivedWithDeleteFlag(
      LeaderAndIsr.NoEpoch, true)
  }

  @Test
  def shouldNotResignCoordinatorsIfStopReplicaReceivedWithoutDeleteFlag(): Unit = {
    shouldResignCoordinatorsIfStopReplicaReceivedWithDeleteFlag(
      LeaderAndIsr.initialLeaderEpoch + 2, false)
  }

  def shouldResignCoordinatorsIfStopReplicaReceivedWithDeleteFlag(leaderEpoch: Int,
                                                                  deletePartition: Boolean): Unit = {
    val controllerId = 0
    val controllerEpoch = 5
    val brokerEpoch = 230498320L

    val fooPartition = new TopicPartition("foo", 0)
    val groupMetadataPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)
    val txnStatePartition = new TopicPartition(Topic.TRANSACTION_STATE_TOPIC_NAME, 0)

    val topicStates = Seq(
      new StopReplicaTopicState()
        .setTopicName(groupMetadataPartition.topic())
        .setPartitionStates(Seq(new StopReplicaPartitionState()
          .setPartitionIndex(groupMetadataPartition.partition())
          .setLeaderEpoch(leaderEpoch)
          .setDeletePartition(deletePartition)).asJava),
      new StopReplicaTopicState()
        .setTopicName(txnStatePartition.topic())
        .setPartitionStates(Seq(new StopReplicaPartitionState()
          .setPartitionIndex(txnStatePartition.partition())
          .setLeaderEpoch(leaderEpoch)
          .setDeletePartition(deletePartition)).asJava),
      new StopReplicaTopicState()
        .setTopicName(fooPartition.topic())
        .setPartitionStates(Seq(new StopReplicaPartitionState()
          .setPartitionIndex(fooPartition.partition())
          .setLeaderEpoch(leaderEpoch)
          .setDeletePartition(deletePartition)).asJava)
    ).asJava

    val stopReplicaRequest = new StopReplicaRequest.Builder(
      ApiKeys.STOP_REPLICA.latestVersion,
      controllerId,
      controllerEpoch,
      brokerEpoch,
      false,
      topicStates
    ).build()
    val request = buildRequest(stopReplicaRequest)

    EasyMock.expect(replicaManager.stopReplicas(
      EasyMock.eq(request.context.correlationId),
      EasyMock.eq(controllerId),
      EasyMock.eq(controllerEpoch),
      EasyMock.eq(brokerEpoch),
      EasyMock.eq(stopReplicaRequest.partitionStates().asScala)
    )).andReturn(
      (mutable.Map(
        groupMetadataPartition -> Errors.NONE,
        txnStatePartition -> Errors.NONE,
        fooPartition -> Errors.NONE
      ), Errors.NONE)
    )
    EasyMock.expect(controller.brokerEpoch).andStubReturn(brokerEpoch)

    if (deletePartition) {
      if (leaderEpoch >= 0) {
        txnCoordinator.onResignation(txnStatePartition.partition, Some(leaderEpoch))
      } else {
        txnCoordinator.onResignation(txnStatePartition.partition, None)
      }
      EasyMock.expectLastCall()
    }

    if (deletePartition) {
      groupCoordinator.onResignation(groupMetadataPartition.partition)
      EasyMock.expectLastCall()
    }

    EasyMock.replay(controller, replicaManager, txnCoordinator, groupCoordinator)

    createKafkaApis().handleStopReplicaRequest(request)

    EasyMock.verify(txnCoordinator, groupCoordinator)
  }

  @Test
  def shouldRespondWithUnknownTopicOrPartitionForBadPartitionAndNoErrorsForGoodPartition(): Unit = {
    val tp1 = new TopicPartition("t", 0)
    val tp2 = new TopicPartition("t1", 0)
    val (writeTxnMarkersRequest, request) = createWriteTxnMarkersRequest(asList(tp1, tp2))
    val expectedErrors = Map(tp1 -> Errors.UNKNOWN_TOPIC_OR_PARTITION, tp2 -> Errors.NONE).asJava

    val capturedResponse: Capture[RequestChannel.Response] = EasyMock.newCapture()
    val responseCallback: Capture[Map[TopicPartition, PartitionResponse] => Unit] = EasyMock.newCapture()

    EasyMock.expect(replicaManager.getMagic(tp1))
      .andReturn(None)
    EasyMock.expect(replicaManager.getMagic(tp2))
      .andReturn(Some(RecordBatch.MAGIC_VALUE_V2))

    EasyMock.expect(replicaManager.appendRecords(EasyMock.anyLong(),
      EasyMock.anyShort(),
      EasyMock.eq(true),
      EasyMock.eq(AppendOrigin.Coordinator),
      EasyMock.anyObject(),
      EasyMock.capture(responseCallback),
      EasyMock.anyObject(),
      EasyMock.anyObject())
    ).andAnswer(() => responseCallback.getValue.apply(Map(tp2 -> new PartitionResponse(Errors.NONE))))

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
      EasyMock.eq(AppendOrigin.Coordinator),
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

    val describeGroupsRequest = new DescribeGroupsRequest.Builder(
      new DescribeGroupsRequestData().setGroups(List(groupId).asJava)
    ).build()
    val request = buildRequest(describeGroupsRequest)

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
  def testOffsetDelete(): Unit = {
    val group = "groupId"
    setupBasicMetadataCache("topic-1", numPartitions = 2)
    setupBasicMetadataCache("topic-2", numPartitions = 2)

    EasyMock.reset(groupCoordinator, replicaManager, clientRequestQuotaManager, requestChannel)

    val topics = new OffsetDeleteRequestTopicCollection()
    topics.add(new OffsetDeleteRequestTopic()
      .setName("topic-1")
      .setPartitions(Seq(
        new OffsetDeleteRequestPartition().setPartitionIndex(0),
        new OffsetDeleteRequestPartition().setPartitionIndex(1)).asJava))
    topics.add(new OffsetDeleteRequestTopic()
      .setName("topic-2")
      .setPartitions(Seq(
        new OffsetDeleteRequestPartition().setPartitionIndex(0),
        new OffsetDeleteRequestPartition().setPartitionIndex(1)).asJava))

    val offsetDeleteRequest = new OffsetDeleteRequest.Builder(
      new OffsetDeleteRequestData()
        .setGroupId(group)
        .setTopics(topics)
    ).build()
    val request = buildRequest(offsetDeleteRequest)

    val capturedResponse = expectNoThrottling()
    EasyMock.expect(groupCoordinator.handleDeleteOffsets(
      EasyMock.eq(group),
      EasyMock.eq(Seq(
        new TopicPartition("topic-1", 0),
        new TopicPartition("topic-1", 1),
        new TopicPartition("topic-2", 0),
        new TopicPartition("topic-2", 1)
      ))
    )).andReturn((Errors.NONE, Map(
      new TopicPartition("topic-1", 0) -> Errors.NONE,
      new TopicPartition("topic-1", 1) -> Errors.NONE,
      new TopicPartition("topic-2", 0) -> Errors.NONE,
      new TopicPartition("topic-2", 1) -> Errors.NONE,
    )))

    EasyMock.replay(groupCoordinator, replicaManager, clientRequestQuotaManager, requestChannel)

    createKafkaApis().handleOffsetDeleteRequest(request)

    val response = readResponse(ApiKeys.OFFSET_DELETE, offsetDeleteRequest, capturedResponse)
      .asInstanceOf[OffsetDeleteResponse]

    def errorForPartition(topic: String, partition: Int): Errors = {
      Errors.forCode(response.data.topics.find(topic).partitions.find(partition).errorCode())
    }

    assertEquals(2, response.data.topics.size)
    assertEquals(Errors.NONE, errorForPartition("topic-1", 0))
    assertEquals(Errors.NONE, errorForPartition("topic-1", 1))
    assertEquals(Errors.NONE, errorForPartition("topic-2", 0))
    assertEquals(Errors.NONE, errorForPartition("topic-2", 1))
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
      val offsetDeleteRequest = new OffsetDeleteRequest.Builder(
        new OffsetDeleteRequestData()
          .setGroupId(group)
          .setTopics(topics)
      ).build()
      val request = buildRequest(offsetDeleteRequest)

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

    val offsetDeleteRequest = new OffsetDeleteRequest.Builder(
      new OffsetDeleteRequestData()
        .setGroupId(group)
    ).build()
    val request = buildRequest(offsetDeleteRequest)

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
    val listOffsetRequest = ListOffsetRequest.Builder.forConsumer(true, isolationLevel)
      .setTargetTimes(targetTimes.asJava).build()
    val request = buildRequest(listOffsetRequest)
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
        val callback = getCurrentArguments.apply(7)
          .asInstanceOf[Seq[(TopicPartition, FetchPartitionData)] => Unit]
        val records = MemoryRecords.withRecords(CompressionType.NONE,
          new SimpleRecord(timestamp, "foo".getBytes(StandardCharsets.UTF_8)))
        callback(Seq(tp -> FetchPartitionData(Errors.NONE, hw, 0, records,
          None, None, Option.empty, isReassignmentFetch = false)))
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

    val fetchRequest = new FetchRequest.Builder(9, 9, -1, 100, 0, fetchData)
      .build()
    val request = buildRequest(fetchRequest)
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
      ("first", "first".getBytes()),
      ("second", "second".getBytes())
    )

    val groupId = "group"
    val memberId = "member1"
    val protocolType = "consumer"
    val rebalanceTimeoutMs = 10
    val sessionTimeoutMs = 5
    val capturedProtocols = EasyMock.newCapture[List[(String, Array[Byte])]]()

    EasyMock.expect(groupCoordinator.handleJoinGroup(
      EasyMock.eq(groupId),
      EasyMock.eq(memberId),
      EasyMock.eq(None),
      EasyMock.eq(true),
      EasyMock.eq(clientId),
      EasyMock.eq(InetAddress.getLocalHost.toString),
      EasyMock.eq(rebalanceTimeoutMs),
      EasyMock.eq(sessionTimeoutMs),
      EasyMock.eq(protocolType),
      EasyMock.capture(capturedProtocols),
      anyObject()
    ))

    EasyMock.replay(groupCoordinator)

    createKafkaApis().handleJoinGroupRequest(
      buildRequest(
        new JoinGroupRequest.Builder(
          new JoinGroupRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId)
            .setProtocolType(protocolType)
            .setRebalanceTimeoutMs(rebalanceTimeoutMs)
            .setSessionTimeoutMs(sessionTimeoutMs)
            .setProtocols(new JoinGroupRequestData.JoinGroupRequestProtocolCollection(
              protocols.map { case (name, protocol) => new JoinGroupRequestProtocol()
                .setName(name).setMetadata(protocol)
              }.iterator.asJava))
        ).build()
      ))

    EasyMock.verify(groupCoordinator)

    val capturedProtocolsList = capturedProtocols.getValue
    assertEquals(protocols.size, capturedProtocolsList.size)
    protocols.zip(capturedProtocolsList).foreach { case ((expectedName, expectedBytes), (name, bytes)) =>
      assertEquals(expectedName, name)
      assertArrayEquals(expectedBytes, bytes)
    }
  }

  @Test
  def testJoinGroupWhenAnErrorOccurs(): Unit = {
    for (version <- ApiKeys.JOIN_GROUP.oldestVersion to ApiKeys.JOIN_GROUP.latestVersion) {
      testJoinGroupWhenAnErrorOccurs(version.asInstanceOf[Short])
    }
  }

  def testJoinGroupWhenAnErrorOccurs(version: Short): Unit = {
    EasyMock.reset(groupCoordinator, clientRequestQuotaManager, requestChannel)

    val capturedResponse = expectNoThrottling()

    val groupId = "group"
    val memberId = "member1"
    val protocolType = "consumer"
    val rebalanceTimeoutMs = 10
    val sessionTimeoutMs = 5

    val capturedCallback = EasyMock.newCapture[JoinGroupCallback]()

    EasyMock.expect(groupCoordinator.handleJoinGroup(
      EasyMock.eq(groupId),
      EasyMock.eq(memberId),
      EasyMock.eq(None),
      EasyMock.eq(if (version >= 4) true else false),
      EasyMock.eq(clientId),
      EasyMock.eq(InetAddress.getLocalHost.toString),
      EasyMock.eq(if (version >= 1) rebalanceTimeoutMs else sessionTimeoutMs),
      EasyMock.eq(sessionTimeoutMs),
      EasyMock.eq(protocolType),
      EasyMock.eq(List.empty),
      EasyMock.capture(capturedCallback)
    ))

    val joinGroupRequest = new JoinGroupRequest.Builder(
      new JoinGroupRequestData()
        .setGroupId(groupId)
        .setMemberId(memberId)
        .setProtocolType(protocolType)
        .setRebalanceTimeoutMs(rebalanceTimeoutMs)
        .setSessionTimeoutMs(sessionTimeoutMs)
    ).build(version)

    val requestChannelRequest = buildRequest(joinGroupRequest)

    EasyMock.replay(groupCoordinator, clientRequestQuotaManager, requestChannel)

    createKafkaApis().handleJoinGroupRequest(requestChannelRequest)

    EasyMock.verify(groupCoordinator)

    capturedCallback.getValue.apply(JoinGroupResult(memberId, Errors.INCONSISTENT_GROUP_PROTOCOL))

    val response = readResponse(ApiKeys.JOIN_GROUP, joinGroupRequest, capturedResponse)
      .asInstanceOf[JoinGroupResponse]

    assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL, response.error)
    assertEquals(0, response.data.members.size)
    assertEquals(memberId, response.data.memberId)
    assertEquals(GroupCoordinator.NoGeneration, response.data.generationId)
    assertEquals(GroupCoordinator.NoLeader, response.data.leader)
    assertNull(response.data.protocolType)

    if (version >= 7) {
      assertNull(response.data.protocolName)
    } else {
      assertEquals(GroupCoordinator.NoProtocol, response.data.protocolName)
    }

    EasyMock.verify(clientRequestQuotaManager, requestChannel)
  }

  @Test
  def testJoinGroupProtocolType(): Unit = {
    for (version <- ApiKeys.JOIN_GROUP.oldestVersion to ApiKeys.JOIN_GROUP.latestVersion) {
      testJoinGroupProtocolType(version.asInstanceOf[Short])
    }
  }

  def testJoinGroupProtocolType(version: Short): Unit = {
    EasyMock.reset(groupCoordinator, clientRequestQuotaManager, requestChannel)

    val capturedResponse = expectNoThrottling()

    val groupId = "group"
    val memberId = "member1"
    val protocolType = "consumer"
    val protocolName = "range"
    val rebalanceTimeoutMs = 10
    val sessionTimeoutMs = 5

    val capturedCallback = EasyMock.newCapture[JoinGroupCallback]()

    EasyMock.expect(groupCoordinator.handleJoinGroup(
      EasyMock.eq(groupId),
      EasyMock.eq(memberId),
      EasyMock.eq(None),
      EasyMock.eq(if (version >= 4) true else false),
      EasyMock.eq(clientId),
      EasyMock.eq(InetAddress.getLocalHost.toString),
      EasyMock.eq(if (version >= 1) rebalanceTimeoutMs else sessionTimeoutMs),
      EasyMock.eq(sessionTimeoutMs),
      EasyMock.eq(protocolType),
      EasyMock.eq(List.empty),
      EasyMock.capture(capturedCallback)
    ))

    val joinGroupRequest = new JoinGroupRequest.Builder(
      new JoinGroupRequestData()
        .setGroupId(groupId)
        .setMemberId(memberId)
        .setProtocolType(protocolType)
        .setRebalanceTimeoutMs(rebalanceTimeoutMs)
        .setSessionTimeoutMs(sessionTimeoutMs)
    ).build(version)

    val requestChannelRequest = buildRequest(joinGroupRequest)

    EasyMock.replay(groupCoordinator, clientRequestQuotaManager, requestChannel)

    createKafkaApis().handleJoinGroupRequest(requestChannelRequest)

    EasyMock.verify(groupCoordinator)

    capturedCallback.getValue.apply(JoinGroupResult(
      members = List.empty,
      memberId = memberId,
      generationId = 0,
      protocolType = Some(protocolType),
      protocolName = Some(protocolName),
      leaderId = memberId,
      error = Errors.NONE
    ))

    val response = readResponse(ApiKeys.JOIN_GROUP, joinGroupRequest, capturedResponse)
      .asInstanceOf[JoinGroupResponse]

    assertEquals(Errors.NONE, response.error)
    assertEquals(0, response.data.members.size)
    assertEquals(memberId, response.data.memberId)
    assertEquals(0, response.data.generationId)
    assertEquals(memberId, response.data.leader)
    assertEquals(protocolName, response.data.protocolName)

    if (version >= 7) {
      assertEquals(protocolType, response.data.protocolType)
    } else {
      assertNull(response.data.protocolType)
    }

    EasyMock.verify(clientRequestQuotaManager, requestChannel)
  }

  @Test
  def testSyncGroupProtocolTypeAndName(): Unit = {
    for (version <- ApiKeys.SYNC_GROUP.oldestVersion to ApiKeys.SYNC_GROUP.latestVersion) {
      testSyncGroupProtocolTypeAndName(version.asInstanceOf[Short])
    }
  }

  def testSyncGroupProtocolTypeAndName(version: Short): Unit = {
    EasyMock.reset(groupCoordinator, clientRequestQuotaManager, requestChannel)

    val capturedResponse = expectNoThrottling()

    val groupId = "group"
    val memberId = "member1"
    val protocolType = "consumer"
    val protocolName = "range"

    val capturedCallback = EasyMock.newCapture[SyncGroupCallback]()

    EasyMock.expect(groupCoordinator.handleSyncGroup(
      EasyMock.eq(groupId),
      EasyMock.eq(0),
      EasyMock.eq(memberId),
      EasyMock.eq(if (version >= 5) Some(protocolType) else None),
      EasyMock.eq(if (version >= 5) Some(protocolName) else None),
      EasyMock.eq(None),
      EasyMock.eq(Map.empty),
      EasyMock.capture(capturedCallback)
    ))

    val syncGroupRequest = new SyncGroupRequest.Builder(
      new SyncGroupRequestData()
        .setGroupId(groupId)
        .setGenerationId(0)
        .setMemberId(memberId)
        .setProtocolType(protocolType)
        .setProtocolName(protocolName)
    ).build(version)

    val requestChannelRequest = buildRequest(syncGroupRequest)

    EasyMock.replay(groupCoordinator, clientRequestQuotaManager, requestChannel)

    createKafkaApis().handleSyncGroupRequest(requestChannelRequest)

    EasyMock.verify(groupCoordinator)

    capturedCallback.getValue.apply(SyncGroupResult(
      protocolType = Some(protocolType),
      protocolName = Some(protocolName),
      memberAssignment = Array.empty,
      error = Errors.NONE
    ))

    val response = readResponse(ApiKeys.SYNC_GROUP, syncGroupRequest, capturedResponse)
      .asInstanceOf[SyncGroupResponse]

    assertEquals(Errors.NONE, response.error)
    assertArrayEquals(Array.empty[Byte], response.data.assignment)

    if (version >= 5) {
      assertEquals(protocolType, response.data.protocolType)
    } else {
      assertNull(response.data.protocolType)
    }

    EasyMock.verify(clientRequestQuotaManager, requestChannel)
  }

  @Test
  def testSyncGroupProtocolTypeAndNameAreMandatorySinceV5(): Unit = {
    for (version <- ApiKeys.SYNC_GROUP.oldestVersion to ApiKeys.SYNC_GROUP.latestVersion) {
      testSyncGroupProtocolTypeAndNameAreMandatorySinceV5(version.asInstanceOf[Short])
    }
  }

  def testSyncGroupProtocolTypeAndNameAreMandatorySinceV5(version: Short): Unit = {
    EasyMock.reset(groupCoordinator, clientRequestQuotaManager, requestChannel)

    val capturedResponse = expectNoThrottling()

    val groupId = "group"
    val memberId = "member1"
    val protocolType = "consumer"
    val protocolName = "range"

    val capturedCallback = EasyMock.newCapture[SyncGroupCallback]()

    if (version < 5) {
      EasyMock.expect(groupCoordinator.handleSyncGroup(
        EasyMock.eq(groupId),
        EasyMock.eq(0),
        EasyMock.eq(memberId),
        EasyMock.eq(None),
        EasyMock.eq(None),
        EasyMock.eq(None),
        EasyMock.eq(Map.empty),
        EasyMock.capture(capturedCallback)
      ))
    }

    val syncGroupRequest = new SyncGroupRequest.Builder(
      new SyncGroupRequestData()
        .setGroupId(groupId)
        .setGenerationId(0)
        .setMemberId(memberId)
    ).build(version)

    val requestChannelRequest = buildRequest(syncGroupRequest)

    EasyMock.replay(groupCoordinator, clientRequestQuotaManager, requestChannel)

    createKafkaApis().handleSyncGroupRequest(requestChannelRequest)

    EasyMock.verify(groupCoordinator)

    if (version < 5) {
      capturedCallback.getValue.apply(SyncGroupResult(
        protocolType = Some(protocolType),
        protocolName = Some(protocolName),
        memberAssignment = Array.empty,
        error = Errors.NONE
      ))
    }

    val response = readResponse(ApiKeys.SYNC_GROUP, syncGroupRequest, capturedResponse)
      .asInstanceOf[SyncGroupResponse]

    if (version < 5) {
      assertEquals(Errors.NONE, response.error)
    } else {
      assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL, response.error)
    }

    EasyMock.verify(clientRequestQuotaManager, requestChannel)
  }

  @Test
  def rejectJoinGroupRequestWhenStaticMembershipNotSupported(): Unit = {
    val capturedResponse = expectNoThrottling()
    EasyMock.replay(clientRequestQuotaManager, requestChannel)

    val joinGroupRequest = new JoinGroupRequest.Builder(
      new JoinGroupRequestData()
        .setGroupId("test")
        .setMemberId("test")
        .setGroupInstanceId("instanceId")
        .setProtocolType("consumer")
        .setProtocols(new JoinGroupRequestData.JoinGroupRequestProtocolCollection)
    ).build()

    val requestChannelRequest = buildRequest(joinGroupRequest)
    createKafkaApis(KAFKA_2_2_IV1).handleJoinGroupRequest(requestChannelRequest)

    val response = readResponse(ApiKeys.JOIN_GROUP, joinGroupRequest, capturedResponse).asInstanceOf[JoinGroupResponse]
    assertEquals(Errors.UNSUPPORTED_VERSION, response.error())
    EasyMock.replay(groupCoordinator)
  }

  @Test
  def rejectSyncGroupRequestWhenStaticMembershipNotSupported(): Unit = {
    val capturedResponse = expectNoThrottling()
    EasyMock.replay(clientRequestQuotaManager, requestChannel)

    val syncGroupRequest = new SyncGroupRequest.Builder(
      new SyncGroupRequestData()
        .setGroupId("test")
        .setMemberId("test")
        .setGroupInstanceId("instanceId")
        .setGenerationId(1)
    ).build()

    val requestChannelRequest = buildRequest(syncGroupRequest)
    createKafkaApis(KAFKA_2_2_IV1).handleSyncGroupRequest(requestChannelRequest)

    val response = readResponse(ApiKeys.SYNC_GROUP, syncGroupRequest, capturedResponse).asInstanceOf[SyncGroupResponse]
    assertEquals(Errors.UNSUPPORTED_VERSION, response.error)
    EasyMock.replay(groupCoordinator)
  }

  @Test
  def rejectHeartbeatRequestWhenStaticMembershipNotSupported(): Unit = {
    val capturedResponse = expectNoThrottling()
    EasyMock.replay(clientRequestQuotaManager, requestChannel)

    val heartbeatRequest = new HeartbeatRequest.Builder(
      new HeartbeatRequestData()
        .setGroupId("test")
        .setMemberId("test")
        .setGroupInstanceId("instanceId")
        .setGenerationId(1)
    ).build()
    val requestChannelRequest = buildRequest(heartbeatRequest)
    createKafkaApis(KAFKA_2_2_IV1).handleHeartbeatRequest(requestChannelRequest)

    val response = readResponse(ApiKeys.HEARTBEAT, heartbeatRequest, capturedResponse).asInstanceOf[HeartbeatResponse]
    assertEquals(Errors.UNSUPPORTED_VERSION, response.error())
    EasyMock.replay(groupCoordinator)
  }

  @Test
  def rejectOffsetCommitRequestWhenStaticMembershipNotSupported(): Unit = {
    val capturedResponse = expectNoThrottling()
    EasyMock.replay(clientRequestQuotaManager, requestChannel)

    val offsetCommitRequest = new OffsetCommitRequest.Builder(
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
    ).build()

    val requestChannelRequest = buildRequest(offsetCommitRequest)
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

    val leaveRequest = buildRequest(
      new LeaveGroupRequest.Builder(
        groupId,
        leaveMemberList.asJava
      ).build()
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

    val leaveRequest = buildRequest(
      new LeaveGroupRequest.Builder(
        groupId,
        singleLeaveMember.asJava
      ).build()
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

  private def assertReassignmentAndReplicationBytesOutPerSec(isReassigning: Boolean): Unit = {
    val leaderEpoch = 0
    val tp0 = new TopicPartition("tp", 0)

    val fetchData = Collections.singletonMap(tp0, new FetchRequest.PartitionData(0, 0, Int.MaxValue, Optional.of(leaderEpoch)))
    val fetchFromFollower = buildRequest(new FetchRequest.Builder(
      ApiKeys.FETCH.oldestVersion(), ApiKeys.FETCH.latestVersion(), 1, 1000, 0, fetchData
    ).build())

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
        callback(Seq(tp0 -> FetchPartitionData(Errors.NONE, hw, 0, records, None, None, Option.empty, isReassignmentFetch = isReassigning)))
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

  @Test
  def rejectInitProducerIdWhenIdButNotEpochProvided(): Unit = {
    val capturedResponse = expectNoThrottling()
    EasyMock.replay(clientRequestQuotaManager, requestChannel)

    val initProducerIdRequest = new InitProducerIdRequest.Builder(
      new InitProducerIdRequestData()
        .setTransactionalId("known")
        .setTransactionTimeoutMs(TimeUnit.MINUTES.toMillis(15).toInt)
        .setProducerId(10)
        .setProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH)
    ).build()

    val requestChannelRequest = buildRequest(initProducerIdRequest)
    createKafkaApis(KAFKA_2_2_IV1).handleInitProducerIdRequest(requestChannelRequest)

    val response = readResponse(ApiKeys.INIT_PRODUCER_ID, initProducerIdRequest, capturedResponse)
      .asInstanceOf[InitProducerIdResponse]
    assertEquals(Errors.INVALID_REQUEST, response.error)
  }

  @Test
  def rejectInitProducerIdWhenEpochButNotIdProvided(): Unit = {
    val capturedResponse = expectNoThrottling()
    EasyMock.replay(clientRequestQuotaManager, requestChannel)

    val initProducerIdRequest = new InitProducerIdRequest.Builder(
      new InitProducerIdRequestData()
        .setTransactionalId("known")
        .setTransactionTimeoutMs(TimeUnit.MINUTES.toMillis(15).toInt)
        .setProducerId(RecordBatch.NO_PRODUCER_ID)
        .setProducerEpoch(2)
    ).build()
    val requestChannelRequest = buildRequest(initProducerIdRequest)
    createKafkaApis(KAFKA_2_2_IV1).handleInitProducerIdRequest(requestChannelRequest)

    val response = readResponse(ApiKeys.INIT_PRODUCER_ID, initProducerIdRequest, capturedResponse).asInstanceOf[InitProducerIdResponse]
    assertEquals(Errors.INVALID_REQUEST, response.error)
  }

  @Test
  def testUpdateMetadataRequestWithCurrentBrokerEpoch(): Unit = {
    val currentBrokerEpoch = 1239875L
    testUpdateMetadataRequest(currentBrokerEpoch, currentBrokerEpoch, Errors.NONE)
  }

  @Test
  def testUpdateMetadataRequestWithNewerBrokerEpochIsValid(): Unit = {
    val currentBrokerEpoch = 1239875L
    testUpdateMetadataRequest(currentBrokerEpoch, currentBrokerEpoch + 1, Errors.NONE)
  }

  @Test
  def testUpdateMetadataRequestWithStaleBrokerEpochIsRejected(): Unit = {
    val currentBrokerEpoch = 1239875L
    testUpdateMetadataRequest(currentBrokerEpoch, currentBrokerEpoch - 1, Errors.STALE_BROKER_EPOCH)
  }

  def testUpdateMetadataRequest(currentBrokerEpoch: Long, brokerEpochInRequest: Long, expectedError: Errors): Unit = {
    val updateMetadataRequest = createBasicMetadataRequest("topicA", 1, brokerEpochInRequest)
    val request = buildRequest(updateMetadataRequest)

    val capturedResponse: Capture[RequestChannel.Response] = EasyMock.newCapture()

    EasyMock.expect(controller.brokerEpoch).andStubReturn(currentBrokerEpoch)
    EasyMock.expect(replicaManager.maybeUpdateMetadataCache(
      EasyMock.eq(request.context.correlationId),
      EasyMock.anyObject()
    )).andStubReturn(
      Seq()
    )

    EasyMock.expect(requestChannel.sendResponse(EasyMock.capture(capturedResponse)))
    EasyMock.replay(replicaManager, controller, requestChannel)

    createKafkaApis().handleUpdateMetadataRequest(request)
    val updateMetadataResponse = readResponse(ApiKeys.UPDATE_METADATA, updateMetadataRequest, capturedResponse)
      .asInstanceOf[UpdateMetadataResponse]
    assertEquals(expectedError, updateMetadataResponse.error())
    EasyMock.verify(replicaManager)
  }

  @Test
  def testLeaderAndIsrRequestWithCurrentBrokerEpoch(): Unit = {
    val currentBrokerEpoch = 1239875L
    testLeaderAndIsrRequest(currentBrokerEpoch, currentBrokerEpoch, Errors.NONE)
  }

  @Test
  def testLeaderAndIsrRequestWithNewerBrokerEpochIsValid(): Unit = {
    val currentBrokerEpoch = 1239875L
    testLeaderAndIsrRequest(currentBrokerEpoch, currentBrokerEpoch + 1, Errors.NONE)
  }

  @Test
  def testLeaderAndIsrRequestWithStaleBrokerEpochIsRejected(): Unit = {
    val currentBrokerEpoch = 1239875L
    testLeaderAndIsrRequest(currentBrokerEpoch, currentBrokerEpoch - 1, Errors.STALE_BROKER_EPOCH)
  }

  def testLeaderAndIsrRequest(currentBrokerEpoch: Long, brokerEpochInRequest: Long, expectedError: Errors): Unit = {
    val controllerId = 2
    val controllerEpoch = 6
    val capturedResponse: Capture[RequestChannel.Response] = EasyMock.newCapture()
    val partitionStates = Seq(
      new LeaderAndIsrRequestData.LeaderAndIsrPartitionState()
        .setTopicName("topicW")
        .setPartitionIndex(1)
        .setControllerEpoch(1)
        .setLeader(0)
        .setLeaderEpoch(1)
        .setIsr(asList(0, 1))
        .setZkVersion(2)
        .setReplicas(asList(0, 1, 2))
        .setIsNew(false)
    ).asJava
    val leaderAndIsrRequest = new LeaderAndIsrRequest.Builder(
      ApiKeys.LEADER_AND_ISR.latestVersion,
      controllerId,
      controllerEpoch,
      brokerEpochInRequest,
      partitionStates,
      asList(new Node(0, "host0", 9090), new Node(1, "host1", 9091))
    ).build()
    val request = buildRequest(leaderAndIsrRequest)
    val response = new LeaderAndIsrResponse(new LeaderAndIsrResponseData()
      .setErrorCode(Errors.NONE.code)
      .setPartitionErrors(asList()))

    EasyMock.expect(controller.brokerEpoch).andStubReturn(currentBrokerEpoch)
    EasyMock.expect(replicaManager.becomeLeaderOrFollower(
      EasyMock.eq(request.context.correlationId),
      EasyMock.anyObject(),
      EasyMock.anyObject()
    )).andStubReturn(
      response
    )

    EasyMock.expect(requestChannel.sendResponse(EasyMock.capture(capturedResponse)))
    EasyMock.replay(replicaManager, controller, requestChannel)

    createKafkaApis().handleLeaderAndIsrRequest(request)
    val leaderAndIsrResponse = readResponse(ApiKeys.LEADER_AND_ISR, leaderAndIsrRequest, capturedResponse)
      .asInstanceOf[LeaderAndIsrResponse]
    assertEquals(expectedError, leaderAndIsrResponse.error())
    EasyMock.verify(replicaManager)
  }

  @Test
  def testStopReplicaRequestWithCurrentBrokerEpoch(): Unit = {
    val currentBrokerEpoch = 1239875L
    testStopReplicaRequest(currentBrokerEpoch, currentBrokerEpoch, Errors.NONE)
  }

  @Test
  def testStopReplicaRequestWithNewerBrokerEpochIsValid(): Unit = {
    val currentBrokerEpoch = 1239875L
    testStopReplicaRequest(currentBrokerEpoch, currentBrokerEpoch + 1, Errors.NONE)
  }

  @Test
  def testStopReplicaRequestWithStaleBrokerEpochIsRejected(): Unit = {
    val currentBrokerEpoch = 1239875L
    testStopReplicaRequest(currentBrokerEpoch, currentBrokerEpoch - 1, Errors.STALE_BROKER_EPOCH)
  }

  def testStopReplicaRequest(currentBrokerEpoch: Long, brokerEpochInRequest: Long, expectedError: Errors): Unit = {
    val controllerId = 0
    val controllerEpoch = 5
    val capturedResponse: Capture[RequestChannel.Response] = EasyMock.newCapture()
    val fooPartition = new TopicPartition("foo", 0)
    val topicStates = Seq(
      new StopReplicaTopicState()
        .setTopicName(fooPartition.topic())
        .setPartitionStates(Seq(new StopReplicaPartitionState()
          .setPartitionIndex(fooPartition.partition())
          .setLeaderEpoch(1)
          .setDeletePartition(false)).asJava)
    ).asJava
    val stopReplicaRequest = new StopReplicaRequest.Builder(
      ApiKeys.STOP_REPLICA.latestVersion,
      controllerId,
      controllerEpoch,
      brokerEpochInRequest,
      false,
      topicStates
    ).build()
    val request = buildRequest(stopReplicaRequest)

    EasyMock.expect(controller.brokerEpoch).andStubReturn(currentBrokerEpoch)
    EasyMock.expect(replicaManager.stopReplicas(
      EasyMock.eq(request.context.correlationId),
      EasyMock.eq(controllerId),
      EasyMock.eq(controllerEpoch),
      EasyMock.eq(brokerEpochInRequest),
      EasyMock.eq(stopReplicaRequest.partitionStates().asScala)
    )).andStubReturn(
      (mutable.Map(
        fooPartition -> Errors.NONE
      ), Errors.NONE)
    )
    EasyMock.expect(requestChannel.sendResponse(EasyMock.capture(capturedResponse)))

    EasyMock.replay(controller, replicaManager, requestChannel)

    createKafkaApis().handleStopReplicaRequest(request)
    val stopReplicaResponse = readResponse(ApiKeys.STOP_REPLICA, stopReplicaRequest, capturedResponse)
      .asInstanceOf[StopReplicaResponse]
    assertEquals(expectedError, stopReplicaResponse.error())
    EasyMock.verify(replicaManager)
  }

  @Test
  def testListGroupsRequest(): Unit = {
    val overviews = List(
      GroupOverview("group1", "protocol1", "Stable"),
      GroupOverview("group2", "qwerty", "Empty")
    )
    val response = listGroupRequest(None, overviews)
    assertEquals(2, response.data.groups.size)
    assertEquals("Stable", response.data.groups.get(0).groupState)
    assertEquals("Empty", response.data.groups.get(1).groupState)
  }

  @Test
  def testListGroupsRequestWithState(): Unit = {
    val overviews = List(
      GroupOverview("group1", "protocol1", "Stable")
    )
    val response = listGroupRequest(Some("Stable"), overviews)
    assertEquals(1, response.data.groups.size)
    assertEquals("Stable", response.data.groups.get(0).groupState)
  }

  private def listGroupRequest(state: Option[String], overviews: List[GroupOverview]): ListGroupsResponse = {
    EasyMock.reset(groupCoordinator, clientRequestQuotaManager, requestChannel)

    val data = new ListGroupsRequestData()
    if (state.isDefined)
      data.setStatesFilter(Collections.singletonList(state.get))
    val listGroupsRequest = new ListGroupsRequest.Builder(data).build()
    val requestChannelRequest = buildRequest(listGroupsRequest)

    val capturedResponse = expectNoThrottling()
    val expectedStates: Set[String] = if (state.isDefined) Set(state.get) else Set()
    EasyMock.expect(groupCoordinator.handleListGroups(expectedStates))
      .andReturn((Errors.NONE, overviews))
    EasyMock.replay(groupCoordinator, clientRequestQuotaManager, requestChannel)

    createKafkaApis().handleListGroupsRequest(requestChannelRequest)

    val response = readResponse(ApiKeys.LIST_GROUPS, listGroupsRequest, capturedResponse).asInstanceOf[ListGroupsResponse]
    assertEquals(Errors.NONE.code, response.data.errorCode)
    response
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

    val metadataRequest = MetadataRequest.Builder.allTopics.build()
    val requestChannelRequest = buildRequest(metadataRequest, requestListener)
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
    val listOffsetRequest = ListOffsetRequest.Builder.forConsumer(true, isolationLevel)
      .setTargetTimes(targetTimes.asJava).build()
    val request = buildRequest(listOffsetRequest)
    createKafkaApis().handleListOffsetRequest(request)

    val response = readResponse(ApiKeys.LIST_OFFSETS, listOffsetRequest, capturedResponse).asInstanceOf[ListOffsetResponse]
    assertTrue(response.responseData.containsKey(tp))

    val partitionData = response.responseData.get(tp)
    assertEquals(Errors.NONE, partitionData.error)
    assertEquals(latestOffset, partitionData.offset)
    assertEquals(ListOffsetResponse.UNKNOWN_TIMESTAMP, partitionData.timestamp)
  }

  private def createWriteTxnMarkersRequest(partitions: util.List[TopicPartition]) = {
    val writeTxnMarkersRequest = new WriteTxnMarkersRequest.Builder(asList(
      new TxnMarkerEntry(1, 1.toShort, 0, TransactionResult.COMMIT, partitions))
    ).build()
    (writeTxnMarkersRequest, buildRequest(writeTxnMarkersRequest))
  }

  private def buildRequest[T <: AbstractRequest](request: AbstractRequest,
                                                 listenerName: ListenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)): RequestChannel.Request = {

    val buffer = request.serialize(new RequestHeader(request.api, request.version, clientId, 0))

    // read the header from the buffer first so that the body can be read next from the Request constructor
    val header = RequestHeader.parse(buffer)
    val context = new RequestContext(header, "1", InetAddress.getLocalHost, KafkaPrincipal.ANONYMOUS,
      listenerName, SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY)
    new RequestChannel.Request(processor = 1, context = context, startTimeNanos = 0, MemoryPool.NONE, buffer,
      requestChannelMetrics)
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
    ResponseHeader.parse(channel.buffer, api.responseHeaderVersion(request.version))
    val struct = api.responseSchema(request.version).read(channel.buffer)
    AbstractResponse.parseResponse(api, struct, request.version)
  }

  private def expectNoThrottling(): Capture[RequestChannel.Response] = {
    EasyMock.expect(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(EasyMock.anyObject[RequestChannel.Request](),
      EasyMock.anyObject[Long])).andReturn(0)
    EasyMock.expect(clientRequestQuotaManager.throttle(EasyMock.anyObject[RequestChannel.Request](), EasyMock.eq(0),
      EasyMock.anyObject[RequestChannel.Response => Unit]()))

    val capturedResponse = EasyMock.newCapture[RequestChannel.Response]()
    EasyMock.expect(requestChannel.sendResponse(EasyMock.capture(capturedResponse)))
    capturedResponse
  }

  private def createBasicMetadataRequest(topic: String, numPartitions: Int, brokerEpoch: Long): UpdateMetadataRequest = {
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
    new UpdateMetadataRequest.Builder(ApiKeys.UPDATE_METADATA.latestVersion, 0,
      0, brokerEpoch, partitionStates.asJava, Seq(broker).asJava).build()
  }

  private def setupBasicMetadataCache(topic: String, numPartitions: Int): Unit = {
    val updateMetadataRequest = createBasicMetadataRequest(topic, numPartitions, 0)
    metadataCache.updateMetadata(correlationId = 0, updateMetadataRequest)
  }
}
