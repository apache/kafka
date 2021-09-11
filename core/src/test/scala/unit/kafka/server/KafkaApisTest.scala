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
import java.util.concurrent.TimeUnit
import java.util.{Collections, Optional, Properties, Random}
import kafka.api.{ApiVersion, KAFKA_0_10_2_IV0, KAFKA_2_2_IV1, LeaderAndIsr}
import kafka.cluster.{Broker, Partition}
import kafka.controller.{ControllerContext, KafkaController}
import kafka.coordinator.group.GroupCoordinatorConcurrencyTest.{JoinGroupCallback, SyncGroupCallback}
import kafka.coordinator.group._
import kafka.coordinator.transaction.{InitProducerIdResult, TransactionCoordinator}
import kafka.log.AppendOrigin
import kafka.network.RequestChannel
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.metadata.{ConfigRepository, KRaftMetadataCache, MockConfigRepository}
import kafka.utils.{MockTime, TestUtils}
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.clients.admin.{AlterConfigOp, ConfigEntry}
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.internals.{KafkaFutureImpl, Topic}
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.message.CreateTopicsRequestData.{CreatableTopic, CreatableTopicCollection}
import org.apache.kafka.common.message.DescribeConfigsResponseData.DescribeConfigsResult
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocol
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity
import org.apache.kafka.common.message.ListOffsetsRequestData.{ListOffsetsPartition, ListOffsetsTopic}
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic
import org.apache.kafka.common.message.OffsetDeleteRequestData.{OffsetDeleteRequestPartition, OffsetDeleteRequestTopic, OffsetDeleteRequestTopicCollection}
import org.apache.kafka.common.message.StopReplicaRequestData.{StopReplicaPartitionState, StopReplicaTopicState}
import org.apache.kafka.common.message.UpdateMetadataRequestData.{UpdateMetadataBroker, UpdateMetadataEndpoint, UpdateMetadataPartitionState}
import org.apache.kafka.common.message._
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.quota.{ClientQuotaAlteration, ClientQuotaEntity}
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record._
import org.apache.kafka.common.replica.ClientMetadata
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType
import org.apache.kafka.common.requests.MetadataResponse.TopicMetadata
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests.WriteTxnMarkersRequest.TxnMarkerEntry
import org.apache.kafka.common.requests.{FetchMetadata => JFetchMetadata, _}
import org.apache.kafka.common.resource.{PatternType, Resource, ResourcePattern, ResourceType}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, KafkaPrincipalSerde, SecurityProtocol}
import org.apache.kafka.common.utils.{ProducerIdAndEpoch, SecurityUtils, Utils}
import org.apache.kafka.common.{IsolationLevel, Node, TopicPartition, Uuid}
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.easymock.EasyMock._
import org.easymock.{Capture, EasyMock, IAnswer}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.{ArgumentMatchers, Mockito}

import scala.collection.{Map, Seq, mutable}
import scala.jdk.CollectionConverters._
import java.util.Arrays

class KafkaApisTest {

  private val requestChannel: RequestChannel = EasyMock.createNiceMock(classOf[RequestChannel])
  private val requestChannelMetrics: RequestChannel.Metrics = EasyMock.createNiceMock(classOf[RequestChannel.Metrics])
  private val replicaManager: ReplicaManager = EasyMock.createNiceMock(classOf[ReplicaManager])
  private val groupCoordinator: GroupCoordinator = EasyMock.createNiceMock(classOf[GroupCoordinator])
  private val adminManager: ZkAdminManager = EasyMock.createNiceMock(classOf[ZkAdminManager])
  private val txnCoordinator: TransactionCoordinator = EasyMock.createNiceMock(classOf[TransactionCoordinator])
  private val controller: KafkaController = EasyMock.createNiceMock(classOf[KafkaController])
  private val forwardingManager: ForwardingManager = EasyMock.createNiceMock(classOf[ForwardingManager])
  private val autoTopicCreationManager: AutoTopicCreationManager = EasyMock.createNiceMock(classOf[AutoTopicCreationManager])

  private val kafkaPrincipalSerde = new KafkaPrincipalSerde {
    override def serialize(principal: KafkaPrincipal): Array[Byte] = Utils.utf8(principal.toString)
    override def deserialize(bytes: Array[Byte]): KafkaPrincipal = SecurityUtils.parseKafkaPrincipal(Utils.utf8(bytes))
  }
  private val zkClient: KafkaZkClient = EasyMock.createNiceMock(classOf[KafkaZkClient])
  private val metrics = new Metrics()
  private val brokerId = 1
  private var metadataCache: MetadataCache = MetadataCache.zkMetadataCache(brokerId)
  private val clientQuotaManager: ClientQuotaManager = EasyMock.createNiceMock(classOf[ClientQuotaManager])
  private val clientRequestQuotaManager: ClientRequestQuotaManager = EasyMock.createNiceMock(classOf[ClientRequestQuotaManager])
  private val clientControllerQuotaManager: ControllerMutationQuotaManager = EasyMock.createNiceMock(classOf[ControllerMutationQuotaManager])
  private val replicaQuotaManager: ReplicationQuotaManager = EasyMock.createNiceMock(classOf[ReplicationQuotaManager])
  private val quotas = QuotaManagers(clientQuotaManager, clientQuotaManager, clientRequestQuotaManager,
    clientControllerQuotaManager, replicaQuotaManager, replicaQuotaManager, replicaQuotaManager, None)
  private val fetchManager: FetchManager = EasyMock.createNiceMock(classOf[FetchManager])
  private val brokerTopicStats = new BrokerTopicStats
  private val clusterId = "clusterId"
  private val time = new MockTime
  private val clientId = ""

  @AfterEach
  def tearDown(): Unit = {
    quotas.shutdown()
    TestUtils.clearYammerMetrics()
    metrics.close()
  }

  def createKafkaApis(interBrokerProtocolVersion: ApiVersion = ApiVersion.latestVersion,
                      authorizer: Option[Authorizer] = None,
                      enableForwarding: Boolean = false,
                      configRepository: ConfigRepository = new MockConfigRepository(),
                      raftSupport: Boolean = false,
                      overrideProperties: Map[String, String] = Map.empty): KafkaApis = {

    val properties = if (raftSupport) {
      val properties = TestUtils.createBrokerConfig(brokerId, "")
      properties.put(KafkaConfig.NodeIdProp, brokerId.toString)
      properties.put(KafkaConfig.ProcessRolesProp, "broker")
      val voterId = (brokerId + 1)
      properties.put(KafkaConfig.QuorumVotersProp, s"$voterId@localhost:9093")
      properties
    } else {
      TestUtils.createBrokerConfig(brokerId, "zk")
    }
    overrideProperties.foreach( p => properties.put(p._1, p._2))
    TestUtils.setIbpAndMessageFormatVersions(properties, interBrokerProtocolVersion)
    val config = new KafkaConfig(properties)

    val forwardingManagerOpt = if (enableForwarding)
      Some(this.forwardingManager)
    else
      None

    val metadataSupport = if (raftSupport) {
      // it will be up to the test to replace the default ZkMetadataCache implementation
      // with a KRaftMetadataCache instance
      metadataCache match {
        case cache: KRaftMetadataCache => RaftSupport(forwardingManager, cache)
        case _ => throw new IllegalStateException("Test must set an instance of KRaftMetadataCache")
      }
    } else {
      metadataCache match {
        case zkMetadataCache: ZkMetadataCache =>
          ZkSupport(adminManager, controller, zkClient, forwardingManagerOpt, zkMetadataCache)
        case _ => throw new IllegalStateException("Test must set an instance of ZkMetadataCache")
      }
    }

    val listenerType = if (raftSupport) ListenerType.BROKER else ListenerType.ZK_BROKER
    val enabledApis = if (enableForwarding) {
      ApiKeys.apisForListener(listenerType).asScala ++ Set(ApiKeys.ENVELOPE)
    } else {
      ApiKeys.apisForListener(listenerType).asScala.toSet
    }
    val apiVersionManager = new SimpleApiVersionManager(listenerType, enabledApis)

    new KafkaApis(requestChannel,
      metadataSupport,
      replicaManager,
      groupCoordinator,
      txnCoordinator,
      autoTopicCreationManager,
      brokerId,
      config,
      configRepository,
      metadataCache,
      metrics,
      authorizer,
      quotas,
      fetchManager,
      brokerTopicStats,
      clusterId,
      time,
      null,
      apiVersionManager)
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

    val configRepository: ConfigRepository = EasyMock.strictMock(classOf[ConfigRepository])
    val topicConfigs = new Properties()
    val propName = "min.insync.replicas"
    val propValue = "3"
    topicConfigs.put(propName, propValue)
    EasyMock.expect(configRepository.topicConfig(resourceName)).andReturn(topicConfigs)

    metadataCache =
      EasyMock.partialMockBuilder(classOf[ZkMetadataCache])
        .withConstructor(classOf[Int])
        .withArgs(Int.box(brokerId))  // Need to box it for Scala 2.12 and before
        .addMockedMethod("contains", classOf[String])
        .createMock()

    expect(metadataCache.contains(resourceName)).andReturn(true)

    val describeConfigsRequest = new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData()
      .setIncludeSynonyms(true)
      .setResources(List(new DescribeConfigsRequestData.DescribeConfigsResource()
        .setResourceName(resourceName)
        .setResourceType(ConfigResource.Type.TOPIC.id)).asJava))
      .build(requestHeader.apiVersion)
    val request = buildRequest(describeConfigsRequest,
      requestHeader = Option(requestHeader))
    val capturedResponse = expectNoThrottling(request)

    EasyMock.replay(metadataCache, replicaManager, clientRequestQuotaManager, requestChannel,
      authorizer, configRepository, adminManager)
    createKafkaApis(authorizer = Some(authorizer), configRepository = configRepository)
      .handleDescribeConfigsRequest(request)

    verify(authorizer, replicaManager)

    val response = capturedResponse.getValue.asInstanceOf[DescribeConfigsResponse]
    val results = response.data().results()
    assertEquals(1, results.size())
    val describeConfigsResult: DescribeConfigsResult = results.get(0)
    assertEquals(ConfigResource.Type.TOPIC.id, describeConfigsResult.resourceType())
    assertEquals(resourceName, describeConfigsResult.resourceName())
    val configs = describeConfigsResult.configs().asScala.filter(_.name() == propName)
    assertEquals(1, configs.length)
    val describeConfigsResponseData = configs.head
    assertEquals(propName, describeConfigsResponseData.name())
    assertEquals(propValue, describeConfigsResponseData.value())
  }

  @Test
  def testEnvelopeRequestHandlingAsController(): Unit = {
    testEnvelopeRequestWithAlterConfig(
      alterConfigHandler = () => ApiError.NONE,
      expectedError = Errors.NONE
    )
  }

  @Test
  def testEnvelopeRequestWithAlterConfigUnhandledError(): Unit = {
    testEnvelopeRequestWithAlterConfig(
      alterConfigHandler = () => throw new IllegalStateException(),
      expectedError = Errors.UNKNOWN_SERVER_ERROR
    )
  }

  private def testEnvelopeRequestWithAlterConfig(
    alterConfigHandler: () => ApiError,
    expectedError: Errors
  ): Unit = {
    val authorizer: Authorizer = EasyMock.niceMock(classOf[Authorizer])

    authorizeResource(authorizer, AclOperation.CLUSTER_ACTION, ResourceType.CLUSTER, Resource.CLUSTER_NAME, AuthorizationResult.ALLOWED)

    val operation = AclOperation.ALTER_CONFIGS
    val resourceName = "topic-1"
    val requestHeader = new RequestHeader(ApiKeys.ALTER_CONFIGS, ApiKeys.ALTER_CONFIGS.latestVersion,
      clientId, 0)

    EasyMock.expect(controller.isActive).andReturn(true)

    authorizeResource(authorizer, operation, ResourceType.TOPIC, resourceName, AuthorizationResult.ALLOWED)

    val configResource = new ConfigResource(ConfigResource.Type.TOPIC, resourceName)
    EasyMock.expect(adminManager.alterConfigs(anyObject(), EasyMock.eq(false)))
      .andAnswer(() => {
        Map(configResource -> alterConfigHandler.apply())
      })

    val configs = Map(
      configResource -> new AlterConfigsRequest.Config(
        Seq(new AlterConfigsRequest.ConfigEntry("foo", "bar")).asJava))
    val alterConfigsRequest = new AlterConfigsRequest.Builder(configs.asJava, false).build(requestHeader.apiVersion)

    val request = TestUtils.buildRequestWithEnvelope(
      alterConfigsRequest, kafkaPrincipalSerde, requestChannelMetrics, time.nanoseconds())

    val capturedResponse = EasyMock.newCapture[AbstractResponse]()
    val capturedRequest = EasyMock.newCapture[RequestChannel.Request]()

    EasyMock.expect(requestChannel.sendResponse(
      EasyMock.capture(capturedRequest),
      EasyMock.capture(capturedResponse),
      EasyMock.anyObject()
    ))

    EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, authorizer,
      adminManager, controller)
    createKafkaApis(authorizer = Some(authorizer), enableForwarding = true).handle(request, RequestLocal.withThreadConfinedCaching)

    assertEquals(Some(request), capturedRequest.getValue.envelope)
    val innerResponse = capturedResponse.getValue.asInstanceOf[AlterConfigsResponse]
    val responseMap = innerResponse.data.responses().asScala.map { resourceResponse =>
      resourceResponse.resourceName() -> Errors.forCode(resourceResponse.errorCode)
    }.toMap

    assertEquals(Map(resourceName -> expectedError), responseMap)

    verify(authorizer, controller, adminManager)
  }

  @Test
  def testInvalidEnvelopeRequestWithNonForwardableAPI(): Unit = {
    val requestHeader = new RequestHeader(ApiKeys.LEAVE_GROUP, ApiKeys.LEAVE_GROUP.latestVersion,
      clientId, 0)
    val leaveGroupRequest = new LeaveGroupRequest.Builder("group",
      Collections.singletonList(new MemberIdentity())).build(requestHeader.apiVersion)

    EasyMock.expect(controller.isActive).andReturn(true)

    val request = TestUtils.buildRequestWithEnvelope(
      leaveGroupRequest, kafkaPrincipalSerde, requestChannelMetrics, time.nanoseconds())

    val capturedResponse = expectNoThrottling(request)

    EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, controller)
    createKafkaApis(enableForwarding = true).handle(request, RequestLocal.withThreadConfinedCaching)

    val response = capturedResponse.getValue.asInstanceOf[EnvelopeResponse]
    assertEquals(Errors.INVALID_REQUEST, response.error())
  }

  @Test
  def testEnvelopeRequestWithNotFromPrivilegedListener(): Unit = {
    testInvalidEnvelopeRequest(Errors.NONE, fromPrivilegedListener = false,
      shouldCloseConnection = true)
  }

  @Test
  def testEnvelopeRequestNotAuthorized(): Unit = {
    testInvalidEnvelopeRequest(Errors.CLUSTER_AUTHORIZATION_FAILED,
      performAuthorize = true, authorizeResult = AuthorizationResult.DENIED)
  }

  @Test
  def testEnvelopeRequestNotControllerHandling(): Unit = {
    testInvalidEnvelopeRequest(Errors.NOT_CONTROLLER, performAuthorize = true, isActiveController = false)
  }

  private def testInvalidEnvelopeRequest(expectedError: Errors,
                                         fromPrivilegedListener: Boolean = true,
                                         shouldCloseConnection: Boolean = false,
                                         performAuthorize: Boolean = false,
                                         authorizeResult: AuthorizationResult = AuthorizationResult.ALLOWED,
                                         isActiveController: Boolean = true): Unit = {
    val authorizer: Authorizer = EasyMock.niceMock(classOf[Authorizer])

    if (performAuthorize) {
      authorizeResource(authorizer, AclOperation.CLUSTER_ACTION, ResourceType.CLUSTER, Resource.CLUSTER_NAME, authorizeResult)
    }

    val resourceName = "topic-1"
    val requestHeader = new RequestHeader(ApiKeys.ALTER_CONFIGS, ApiKeys.ALTER_CONFIGS.latestVersion,
      clientId, 0)

    EasyMock.expect(controller.isActive).andReturn(isActiveController)

    val configResource = new ConfigResource(ConfigResource.Type.TOPIC, resourceName)

    val configs = Map(
      configResource -> new AlterConfigsRequest.Config(
        Seq(new AlterConfigsRequest.ConfigEntry("foo", "bar")).asJava))
    val alterConfigsRequest = new AlterConfigsRequest.Builder(configs.asJava, false)
      .build(requestHeader.apiVersion)

    val request = TestUtils.buildRequestWithEnvelope(
      alterConfigsRequest, kafkaPrincipalSerde, requestChannelMetrics, time.nanoseconds(), fromPrivilegedListener)

    val capturedResponse = EasyMock.newCapture[AbstractResponse]()
    if (shouldCloseConnection) {
      EasyMock.expect(requestChannel.closeConnection(
        EasyMock.eq(request),
        EasyMock.eq(java.util.Collections.emptyMap())
      ))
    } else {
      EasyMock.expect(requestChannel.sendResponse(
        EasyMock.eq(request),
        EasyMock.capture(capturedResponse),
        EasyMock.eq(None)
      ))
    }

    EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, authorizer,
      adminManager, controller)
    createKafkaApis(authorizer = Some(authorizer), enableForwarding = true).handle(request, RequestLocal.withThreadConfinedCaching)

    if (!shouldCloseConnection) {
      val response = capturedResponse.getValue.asInstanceOf[EnvelopeResponse]
      assertEquals(expectedError, response.error)
    }

    verify(authorizer, adminManager, requestChannel)
  }

  @Test
  def testAlterConfigsWithAuthorizer(): Unit = {
    val authorizer: Authorizer = EasyMock.niceMock(classOf[Authorizer])

    val authorizedTopic = "authorized-topic"
    val unauthorizedTopic = "unauthorized-topic"
    val (authorizedResource, unauthorizedResource) =
      createConfigsWithAuthorization(authorizer, authorizedTopic, unauthorizedTopic)

    val configs = Map(
      authorizedResource -> new AlterConfigsRequest.Config(
        Seq(new AlterConfigsRequest.ConfigEntry("foo", "bar")).asJava),
      unauthorizedResource -> new AlterConfigsRequest.Config(
        Seq(new AlterConfigsRequest.ConfigEntry("foo-1", "bar-1")).asJava)
    )

    val topicHeader = new RequestHeader(ApiKeys.ALTER_CONFIGS, ApiKeys.ALTER_CONFIGS.latestVersion,
      clientId, 0)

    val alterConfigsRequest = new AlterConfigsRequest.Builder(configs.asJava, false)
      .build(topicHeader.apiVersion)
    val request = buildRequest(alterConfigsRequest)

    EasyMock.expect(controller.isActive).andReturn(false)

    val capturedResponse = expectNoThrottling(request)

    EasyMock.expect(adminManager.alterConfigs(anyObject(), EasyMock.eq(false)))
      .andReturn(Map(authorizedResource -> ApiError.NONE))

    EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, authorizer,
      adminManager, controller)

    createKafkaApis(authorizer = Some(authorizer)).handleAlterConfigsRequest(request)

    verifyAlterConfigResult(capturedResponse, Map(authorizedTopic -> Errors.NONE,
        unauthorizedTopic -> Errors.TOPIC_AUTHORIZATION_FAILED))

    verify(authorizer, adminManager)
  }

  @Test
  def testAlterConfigsWithForwarding(): Unit = {
    val requestBuilder = new AlterConfigsRequest.Builder(Collections.emptyMap(), false)
    testForwardableApi(ApiKeys.ALTER_CONFIGS, requestBuilder)
  }

  @Test
  def testDescribeQuorumNotAllowedForZkClusters(): Unit = {
    val requestData = DescribeQuorumRequest.singletonRequest(KafkaRaftServer.MetadataPartition)
    val requestBuilder = new DescribeQuorumRequest.Builder(requestData)
    val request = buildRequest(requestBuilder.build())

    val capturedResponse = expectNoThrottling(request)
    EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, adminManager, controller)
    createKafkaApis(enableForwarding = true).handle(request, RequestLocal.withThreadConfinedCaching)

    val response = capturedResponse.getValue.asInstanceOf[DescribeQuorumResponse]
    assertEquals(Errors.UNKNOWN_SERVER_ERROR, Errors.forCode(response.data.errorCode))
  }

  @Test
  def testDescribeQuorumForwardedForKRaftClusters(): Unit = {
    val requestData = DescribeQuorumRequest.singletonRequest(KafkaRaftServer.MetadataPartition)
    val requestBuilder = new DescribeQuorumRequest.Builder(requestData)
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)

    testForwardableApi(
      createKafkaApis(raftSupport = true),
      ApiKeys.DESCRIBE_QUORUM,
      requestBuilder
    )
  }

  private def testForwardableApi(apiKey: ApiKeys, requestBuilder: AbstractRequest.Builder[_ <: AbstractRequest]): Unit = {
    testForwardableApi(
      createKafkaApis(enableForwarding = true),
      apiKey,
      requestBuilder
    )
  }

  private def testForwardableApi(
    kafkaApis: KafkaApis,
    apiKey: ApiKeys,
    requestBuilder: AbstractRequest.Builder[_ <: AbstractRequest]
  ): Unit = {
    val topicHeader = new RequestHeader(apiKey, apiKey.latestVersion,
      clientId, 0)

    val request = buildRequest(requestBuilder.build(topicHeader.apiVersion))

    if (kafkaApis.metadataSupport.isInstanceOf[ZkSupport]) {
      // The controller check only makes sense for ZK clusters. For KRaft,
      // controller requests are handled on a separate listener, so there
      // is no choice but to forward them.
      EasyMock.expect(controller.isActive).andReturn(false)
    }

    expectNoThrottling(request)

    EasyMock.expect(forwardingManager.forwardRequest(
      EasyMock.eq(request),
      anyObject[Option[AbstractResponse] => Unit]()
    )).once()

    EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, controller, forwardingManager)

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    EasyMock.verify(controller, forwardingManager)
  }

  private def authorizeResource(authorizer: Authorizer,
                                operation: AclOperation,
                                resourceType: ResourceType,
                                resourceName: String,
                                result: AuthorizationResult,
                                logIfAllowed: Boolean = true,
                                logIfDenied: Boolean = true): Unit = {
    val expectedAuthorizedAction = if (operation == AclOperation.CLUSTER_ACTION)
      new Action(operation,
        new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL),
        1, logIfAllowed, logIfDenied)
    else
      new Action(operation,
        new ResourcePattern(resourceType, resourceName, PatternType.LITERAL),
        1, logIfAllowed, logIfDenied)

    EasyMock.expect(authorizer.authorize(anyObject[RequestContext], EasyMock.eq(Seq(expectedAuthorizedAction).asJava)))
      .andReturn(Seq(result).asJava)
      .once()
  }

  private def verifyAlterConfigResult(capturedResponse: Capture[AbstractResponse],
                                      expectedResults: Map[String, Errors]): Unit = {
    val response = capturedResponse.getValue.asInstanceOf[AlterConfigsResponse]
    val responseMap = response.data.responses().asScala.map { resourceResponse =>
      resourceResponse.resourceName() -> Errors.forCode(resourceResponse.errorCode)
    }.toMap

    assertEquals(expectedResults, responseMap)
  }

  private def createConfigsWithAuthorization(authorizer: Authorizer,
                                             authorizedTopic: String,
                                             unauthorizedTopic: String): (ConfigResource, ConfigResource) = {
    val authorizedResource = new ConfigResource(ConfigResource.Type.TOPIC, authorizedTopic)

    val unauthorizedResource = new ConfigResource(ConfigResource.Type.TOPIC, unauthorizedTopic)

    createTopicAuthorization(authorizer, AclOperation.ALTER_CONFIGS, authorizedTopic, unauthorizedTopic)
    (authorizedResource, unauthorizedResource)
  }

  @Test
  def testIncrementalAlterConfigsWithAuthorizer(): Unit = {
    val authorizer: Authorizer = EasyMock.niceMock(classOf[Authorizer])

    val authorizedTopic = "authorized-topic"
    val unauthorizedTopic = "unauthorized-topic"
    val (authorizedResource, unauthorizedResource) =
      createConfigsWithAuthorization(authorizer, authorizedTopic, unauthorizedTopic)

    val requestHeader = new RequestHeader(ApiKeys.INCREMENTAL_ALTER_CONFIGS, ApiKeys.INCREMENTAL_ALTER_CONFIGS.latestVersion, clientId, 0)

    val incrementalAlterConfigsRequest = getIncrementalAlterConfigRequestBuilder(Seq(authorizedResource, unauthorizedResource))
      .build(requestHeader.apiVersion)
    val request = buildRequest(incrementalAlterConfigsRequest,
      fromPrivilegedListener = true, requestHeader = Option(requestHeader))

    EasyMock.expect(controller.isActive).andReturn(true)

    val capturedResponse = expectNoThrottling(request)

    EasyMock.expect(adminManager.incrementalAlterConfigs(anyObject(), EasyMock.eq(false)))
      .andReturn(Map(authorizedResource -> ApiError.NONE))

    EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, authorizer,
      adminManager, controller)
    createKafkaApis(authorizer = Some(authorizer)).handleIncrementalAlterConfigsRequest(request)

    verifyIncrementalAlterConfigResult(capturedResponse, Map(
      authorizedTopic -> Errors.NONE,
      unauthorizedTopic -> Errors.TOPIC_AUTHORIZATION_FAILED
    ))

    verify(authorizer, adminManager)
  }

  @Test
  def testIncrementalAlterConfigsWithForwarding(): Unit = {
    val requestBuilder = new IncrementalAlterConfigsRequest.Builder(
      new IncrementalAlterConfigsRequestData())
    testForwardableApi(ApiKeys.INCREMENTAL_ALTER_CONFIGS, requestBuilder)
  }

  private def getIncrementalAlterConfigRequestBuilder(configResources: Seq[ConfigResource]): IncrementalAlterConfigsRequest.Builder = {
    val resourceMap = configResources.map(configResource => {
      configResource -> Set(
        new AlterConfigOp(new ConfigEntry("foo", "bar"),
        OpType.forId(configResource.`type`.id))).asJavaCollection
    }).toMap.asJava

    new IncrementalAlterConfigsRequest.Builder(resourceMap, false)
  }

  private def verifyIncrementalAlterConfigResult(capturedResponse: Capture[AbstractResponse],
                                                 expectedResults: Map[String, Errors]): Unit = {
    val response = capturedResponse.getValue.asInstanceOf[IncrementalAlterConfigsResponse]
    val responseMap = response.data.responses().asScala.map { resourceResponse =>
      resourceResponse.resourceName() -> Errors.forCode(resourceResponse.errorCode)
    }.toMap
    assertEquals(expectedResults, responseMap)
  }

  @Test
  def testAlterClientQuotasWithAuthorizer(): Unit = {
    val authorizer: Authorizer = EasyMock.niceMock(classOf[Authorizer])

    authorizeResource(authorizer, AclOperation.ALTER_CONFIGS, ResourceType.CLUSTER,
      Resource.CLUSTER_NAME, AuthorizationResult.DENIED)

    val quotaEntity = new ClientQuotaEntity(Collections.singletonMap(ClientQuotaEntity.USER, "user"))
    val quotas = Seq(new ClientQuotaAlteration(quotaEntity, Seq.empty.asJavaCollection))

    val requestHeader = new RequestHeader(ApiKeys.ALTER_CLIENT_QUOTAS, ApiKeys.ALTER_CLIENT_QUOTAS.latestVersion, clientId, 0)

    val alterClientQuotasRequest = new AlterClientQuotasRequest.Builder(quotas.asJavaCollection, false)
      .build(requestHeader.apiVersion)
    val request = buildRequest(alterClientQuotasRequest,
      fromPrivilegedListener = true, requestHeader = Option(requestHeader))

    EasyMock.expect(controller.isActive).andReturn(true)

    val capturedResponse = expectNoThrottling(request)

    EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, authorizer,
      adminManager, controller)
    createKafkaApis(authorizer = Some(authorizer)).handleAlterClientQuotasRequest(request)

    verifyAlterClientQuotaResult(capturedResponse, Map(quotaEntity -> Errors.CLUSTER_AUTHORIZATION_FAILED))

    verify(authorizer, adminManager)
  }

  @Test
  def testAlterClientQuotasWithForwarding(): Unit = {
    val requestBuilder = new AlterClientQuotasRequest.Builder(List.empty.asJava, false)
    testForwardableApi(ApiKeys.ALTER_CLIENT_QUOTAS, requestBuilder)
  }

  private def verifyAlterClientQuotaResult(capturedResponse: Capture[AbstractResponse],
                                           expected: Map[ClientQuotaEntity, Errors]): Unit = {
    val response = capturedResponse.getValue.asInstanceOf[AlterClientQuotasResponse]
    val futures = expected.keys.map(quotaEntity => quotaEntity -> new KafkaFutureImpl[Void]()).toMap
    response.complete(futures.asJava)
    futures.foreach {
      case (entity, future) =>
        future.whenComplete((_, thrown) =>
          assertEquals(thrown, expected(entity).exception())
        ).isDone
    }
  }

  @Test
  def testCreateTopicsWithAuthorizer(): Unit = {
    val authorizer: Authorizer = EasyMock.niceMock(classOf[Authorizer])

    val authorizedTopic = "authorized-topic"
    val unauthorizedTopic = "unauthorized-topic"

    authorizeResource(authorizer, AclOperation.CREATE, ResourceType.CLUSTER,
      Resource.CLUSTER_NAME, AuthorizationResult.DENIED, logIfDenied = false)

    createCombinedTopicAuthorization(authorizer, AclOperation.CREATE,
      authorizedTopic, unauthorizedTopic)

    createCombinedTopicAuthorization(authorizer, AclOperation.DESCRIBE_CONFIGS,
      authorizedTopic, unauthorizedTopic, logIfDenied = false)

    val requestHeader = new RequestHeader(ApiKeys.CREATE_TOPICS, ApiKeys.CREATE_TOPICS.latestVersion, clientId, 0)

    EasyMock.expect(controller.isActive).andReturn(true)

    val topics = new CreateTopicsRequestData.CreatableTopicCollection(2)
    val topicToCreate = new CreateTopicsRequestData.CreatableTopic()
      .setName(authorizedTopic)
    topics.add(topicToCreate)

    val topicToFilter = new CreateTopicsRequestData.CreatableTopic()
      .setName(unauthorizedTopic)
    topics.add(topicToFilter)

    val timeout = 10
    val createTopicsRequest = new CreateTopicsRequest.Builder(
      new CreateTopicsRequestData()
        .setTimeoutMs(timeout)
        .setValidateOnly(false)
        .setTopics(topics))
      .build(requestHeader.apiVersion)
    val request = buildRequest(createTopicsRequest,
      fromPrivilegedListener = true, requestHeader = Option(requestHeader))

    val capturedResponse = expectNoThrottling(request)

    EasyMock.expect(clientControllerQuotaManager.newQuotaFor(
      EasyMock.eq(request), EasyMock.eq(6))).andReturn(UnboundedControllerMutationQuota)

    val capturedCallback = EasyMock.newCapture[Map[String, ApiError] => Unit]()

    EasyMock.expect(adminManager.createTopics(
      EasyMock.eq(timeout),
      EasyMock.eq(false),
      EasyMock.eq(Map(authorizedTopic -> topicToCreate)),
      anyObject(),
      EasyMock.eq(UnboundedControllerMutationQuota),
      EasyMock.capture(capturedCallback)))

    EasyMock.replay(replicaManager, clientRequestQuotaManager, clientControllerQuotaManager,
      requestChannel, authorizer, adminManager, controller)

    createKafkaApis(authorizer = Some(authorizer)).handleCreateTopicsRequest(request)

    capturedCallback.getValue.apply(Map(authorizedTopic -> ApiError.NONE))

    verifyCreateTopicsResult(createTopicsRequest,
      capturedResponse, Map(authorizedTopic -> Errors.NONE,
        unauthorizedTopic -> Errors.TOPIC_AUTHORIZATION_FAILED))

    verify(authorizer, adminManager, clientControllerQuotaManager)
  }

  @Test
  def testCreateTopicsWithForwarding(): Unit = {
    val requestBuilder = new CreateTopicsRequest.Builder(
      new CreateTopicsRequestData().setTopics(
        new CreatableTopicCollection(Collections.singleton(
          new CreatableTopic().setName("topic").setNumPartitions(1).
            setReplicationFactor(1.toShort)).iterator())))
    testForwardableApi(ApiKeys.CREATE_TOPICS, requestBuilder)
  }

  private def createTopicAuthorization(authorizer: Authorizer,
                                       operation: AclOperation,
                                       authorizedTopic: String,
                                       unauthorizedTopic: String,
                                       logIfAllowed: Boolean = true,
                                       logIfDenied: Boolean = true): Unit = {
    authorizeResource(authorizer, operation, ResourceType.TOPIC,
      authorizedTopic, AuthorizationResult.ALLOWED, logIfAllowed, logIfDenied)
    authorizeResource(authorizer, operation, ResourceType.TOPIC,
      unauthorizedTopic, AuthorizationResult.DENIED, logIfAllowed, logIfDenied)
  }

  private def createCombinedTopicAuthorization(authorizer: Authorizer,
                                               operation: AclOperation,
                                               authorizedTopic: String,
                                               unauthorizedTopic: String,
                                               logIfAllowed: Boolean = true,
                                               logIfDenied: Boolean = true): Unit = {
    val expectedAuthorizedActions = Seq(
      new Action(operation,
        new ResourcePattern(ResourceType.TOPIC, authorizedTopic, PatternType.LITERAL),
        1, logIfAllowed, logIfDenied),
      new Action(operation,
        new ResourcePattern(ResourceType.TOPIC, unauthorizedTopic, PatternType.LITERAL),
        1, logIfAllowed, logIfDenied))

    EasyMock.expect(authorizer.authorize(
      anyObject[RequestContext], AuthHelperTest.matchSameElements(expectedAuthorizedActions.asJava)
    )).andAnswer { () =>
      val actions = EasyMock.getCurrentArguments.apply(1).asInstanceOf[util.List[Action]].asScala
      actions.map { action =>
        if (action.resourcePattern().name().equals(authorizedTopic))
          AuthorizationResult.ALLOWED
        else
          AuthorizationResult.DENIED
      }.asJava
    }.once()
  }

  private def verifyCreateTopicsResult(createTopicsRequest: CreateTopicsRequest,
                                       capturedResponse: Capture[AbstractResponse],
                                       expectedResults: Map[String, Errors]): Unit = {
    val response = capturedResponse.getValue.asInstanceOf[CreateTopicsResponse]
    val responseMap = response.data.topics().asScala.map { topicResponse =>
      topicResponse.name() -> Errors.forCode(topicResponse.errorCode)
    }.toMap

    assertEquals(expectedResults, responseMap)
  }

  @Test
  def testCreateAclWithForwarding(): Unit = {
    val requestBuilder = new CreateAclsRequest.Builder(new CreateAclsRequestData())
    testForwardableApi(ApiKeys.CREATE_ACLS, requestBuilder)
  }

  @Test
  def testDeleteAclWithForwarding(): Unit = {
    val requestBuilder = new DeleteAclsRequest.Builder(new DeleteAclsRequestData())
    testForwardableApi(ApiKeys.DELETE_ACLS, requestBuilder)
  }

  @Test
  def testCreateDelegationTokenWithForwarding(): Unit = {
    val requestBuilder = new CreateDelegationTokenRequest.Builder(new CreateDelegationTokenRequestData())
    testForwardableApi(ApiKeys.CREATE_DELEGATION_TOKEN, requestBuilder)
  }

  @Test
  def testRenewDelegationTokenWithForwarding(): Unit = {
    val requestBuilder = new RenewDelegationTokenRequest.Builder(new RenewDelegationTokenRequestData())
    testForwardableApi(ApiKeys.RENEW_DELEGATION_TOKEN, requestBuilder)
  }

  @Test
  def testExpireDelegationTokenWithForwarding(): Unit = {
    val requestBuilder = new ExpireDelegationTokenRequest.Builder(new ExpireDelegationTokenRequestData())
    testForwardableApi(ApiKeys.EXPIRE_DELEGATION_TOKEN, requestBuilder)
  }

  @Test
  def testAlterPartitionReassignmentsWithForwarding(): Unit = {
    val requestBuilder = new AlterPartitionReassignmentsRequest.Builder(new AlterPartitionReassignmentsRequestData())
    testForwardableApi(ApiKeys.ALTER_PARTITION_REASSIGNMENTS, requestBuilder)
  }

  @Test
  def testCreatePartitionsWithForwarding(): Unit = {
    val requestBuilder = new CreatePartitionsRequest.Builder(new CreatePartitionsRequestData())
    testForwardableApi(ApiKeys.CREATE_PARTITIONS, requestBuilder)
  }

  @Test
  def testDeleteTopicsWithForwarding(): Unit = {
    val requestBuilder = new DeleteTopicsRequest.Builder(new DeleteTopicsRequestData())
    testForwardableApi(ApiKeys.DELETE_TOPICS, requestBuilder)
  }

  @Test
  def testUpdateFeaturesWithForwarding(): Unit = {
    val requestBuilder = new UpdateFeaturesRequest.Builder(new UpdateFeaturesRequestData())
    testForwardableApi(ApiKeys.UPDATE_FEATURES, requestBuilder)
  }

  @Test
  def testAlterScramWithForwarding(): Unit = {
    val requestBuilder = new AlterUserScramCredentialsRequest.Builder(new AlterUserScramCredentialsRequestData())
    testForwardableApi(ApiKeys.ALTER_USER_SCRAM_CREDENTIALS, requestBuilder)
  }

  @Test
  def testFindCoordinatorAutoTopicCreationForOffsetTopic(): Unit = {
    testFindCoordinatorWithTopicCreation(CoordinatorType.GROUP)
  }

  @Test
  def testFindCoordinatorAutoTopicCreationForTxnTopic(): Unit = {
    testFindCoordinatorWithTopicCreation(CoordinatorType.TRANSACTION)
  }

  @Test
  def testFindCoordinatorNotEnoughBrokersForOffsetTopic(): Unit = {
    testFindCoordinatorWithTopicCreation(CoordinatorType.GROUP, hasEnoughLiveBrokers = false)
  }

  @Test
  def testFindCoordinatorNotEnoughBrokersForTxnTopic(): Unit = {
    testFindCoordinatorWithTopicCreation(CoordinatorType.TRANSACTION, hasEnoughLiveBrokers = false)
  }

  @Test
  def testOldFindCoordinatorAutoTopicCreationForOffsetTopic(): Unit = {
    testFindCoordinatorWithTopicCreation(CoordinatorType.GROUP, version = 3)
  }

  @Test
  def testOldFindCoordinatorAutoTopicCreationForTxnTopic(): Unit = {
    testFindCoordinatorWithTopicCreation(CoordinatorType.TRANSACTION, version = 3)
  }

  @Test
  def testOldFindCoordinatorNotEnoughBrokersForOffsetTopic(): Unit = {
    testFindCoordinatorWithTopicCreation(CoordinatorType.GROUP, hasEnoughLiveBrokers = false, version = 3)
  }

  @Test
  def testOldFindCoordinatorNotEnoughBrokersForTxnTopic(): Unit = {
    testFindCoordinatorWithTopicCreation(CoordinatorType.TRANSACTION, hasEnoughLiveBrokers = false, version = 3)
  }

  private def testFindCoordinatorWithTopicCreation(coordinatorType: CoordinatorType,
                                                   hasEnoughLiveBrokers: Boolean = true,
                                                   version: Short = ApiKeys.FIND_COORDINATOR.latestVersion): Unit = {
    val authorizer: Authorizer = EasyMock.niceMock(classOf[Authorizer])

    val requestHeader = new RequestHeader(ApiKeys.FIND_COORDINATOR, version, clientId, 0)

    val numBrokersNeeded = 3

    setupBrokerMetadata(hasEnoughLiveBrokers, numBrokersNeeded)

    val requestTimeout = 10
    val topicConfigOverride = mutable.Map.empty[String, String]
    topicConfigOverride.put(KafkaConfig.RequestTimeoutMsProp, requestTimeout.toString)

    val groupId = "group"
    val topicName =
      coordinatorType match {
        case CoordinatorType.GROUP =>
          topicConfigOverride.put(KafkaConfig.OffsetsTopicPartitionsProp, numBrokersNeeded.toString)
          topicConfigOverride.put(KafkaConfig.OffsetsTopicReplicationFactorProp, numBrokersNeeded.toString)
          EasyMock.expect(groupCoordinator.offsetsTopicConfigs).andReturn(new Properties)
          authorizeResource(authorizer, AclOperation.DESCRIBE, ResourceType.GROUP,
            groupId, AuthorizationResult.ALLOWED)
          Topic.GROUP_METADATA_TOPIC_NAME
        case CoordinatorType.TRANSACTION =>
          topicConfigOverride.put(KafkaConfig.TransactionsTopicPartitionsProp, numBrokersNeeded.toString)
          topicConfigOverride.put(KafkaConfig.TransactionsTopicReplicationFactorProp, numBrokersNeeded.toString)
          EasyMock.expect(txnCoordinator.transactionTopicConfigs).andReturn(new Properties)
          authorizeResource(authorizer, AclOperation.DESCRIBE, ResourceType.TRANSACTIONAL_ID,
            groupId, AuthorizationResult.ALLOWED)
          Topic.TRANSACTION_STATE_TOPIC_NAME
        case _ =>
          throw new IllegalStateException(s"Unknown coordinator type $coordinatorType")
      }

    val findCoordinatorRequestBuilder = if (version >= 4) {
      new FindCoordinatorRequest.Builder(
        new FindCoordinatorRequestData()
          .setKeyType(coordinatorType.id())
          .setCoordinatorKeys(Arrays.asList(groupId)))
    } else {
      new FindCoordinatorRequest.Builder(
        new FindCoordinatorRequestData()
          .setKeyType(coordinatorType.id())
          .setKey(groupId))
    }
    val request = buildRequest(findCoordinatorRequestBuilder.build(requestHeader.apiVersion))

    val capturedResponse = expectNoThrottling(request)

    val capturedRequest = verifyTopicCreation(topicName, true, true, request)

    EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, authorizer,
      autoTopicCreationManager, forwardingManager, controller, clientControllerQuotaManager, groupCoordinator, txnCoordinator)

    createKafkaApis(authorizer = Some(authorizer),
      overrideProperties = topicConfigOverride).handleFindCoordinatorRequest(request)

    val response = capturedResponse.getValue.asInstanceOf[FindCoordinatorResponse]
    if (version >= 4) {
      assertEquals(Errors.COORDINATOR_NOT_AVAILABLE.code, response.data.coordinators.get(0).errorCode)
      assertEquals(groupId, response.data.coordinators.get(0).key)
    } else {
      assertEquals(Errors.COORDINATOR_NOT_AVAILABLE.code, response.data.errorCode)
    }

    assertTrue(capturedRequest.getValue.isEmpty)

    verify(authorizer, autoTopicCreationManager)
  }

  @Test
  def testMetadataAutoTopicCreationForOffsetTopic(): Unit = {
    testMetadataAutoTopicCreation(Topic.GROUP_METADATA_TOPIC_NAME, enableAutoTopicCreation = true,
      expectedError = Errors.UNKNOWN_TOPIC_OR_PARTITION)
  }

  @Test
  def testMetadataAutoTopicCreationForTxnTopic(): Unit = {
    testMetadataAutoTopicCreation(Topic.TRANSACTION_STATE_TOPIC_NAME, enableAutoTopicCreation = true,
      expectedError = Errors.UNKNOWN_TOPIC_OR_PARTITION)
  }

  @Test
  def testMetadataAutoTopicCreationForNonInternalTopic(): Unit = {
    testMetadataAutoTopicCreation("topic", enableAutoTopicCreation = true,
      expectedError = Errors.UNKNOWN_TOPIC_OR_PARTITION)
  }

  @Test
  def testMetadataAutoTopicCreationDisabledForOffsetTopic(): Unit = {
    testMetadataAutoTopicCreation(Topic.GROUP_METADATA_TOPIC_NAME, enableAutoTopicCreation = false,
      expectedError = Errors.UNKNOWN_TOPIC_OR_PARTITION)
  }

  @Test
  def testMetadataAutoTopicCreationDisabledForTxnTopic(): Unit = {
    testMetadataAutoTopicCreation(Topic.TRANSACTION_STATE_TOPIC_NAME, enableAutoTopicCreation = false,
      expectedError = Errors.UNKNOWN_TOPIC_OR_PARTITION)
  }

  @Test
  def testMetadataAutoTopicCreationDisabledForNonInternalTopic(): Unit = {
    testMetadataAutoTopicCreation("topic", enableAutoTopicCreation = false,
      expectedError = Errors.UNKNOWN_TOPIC_OR_PARTITION)
  }

  @Test
  def testMetadataAutoCreationDisabledForNonInternal(): Unit = {
    testMetadataAutoTopicCreation("topic", enableAutoTopicCreation = true,
      expectedError = Errors.UNKNOWN_TOPIC_OR_PARTITION)
  }

  private def testMetadataAutoTopicCreation(topicName: String,
                                            enableAutoTopicCreation: Boolean,
                                            expectedError: Errors): Unit = {
    val authorizer: Authorizer = EasyMock.niceMock(classOf[Authorizer])

    val requestHeader = new RequestHeader(ApiKeys.METADATA, ApiKeys.METADATA.latestVersion,
      clientId, 0)

    val numBrokersNeeded = 3
    addTopicToMetadataCache("some-topic", 1, 3)

    authorizeResource(authorizer, AclOperation.DESCRIBE, ResourceType.TOPIC,
      topicName, AuthorizationResult.ALLOWED)

    if (enableAutoTopicCreation)
      authorizeResource(authorizer, AclOperation.CREATE, ResourceType.CLUSTER,
        Resource.CLUSTER_NAME, AuthorizationResult.ALLOWED, logIfDenied = false)

    val topicConfigOverride = mutable.Map.empty[String, String]
    val isInternal =
      topicName match {
        case Topic.GROUP_METADATA_TOPIC_NAME =>
          topicConfigOverride.put(KafkaConfig.OffsetsTopicPartitionsProp, numBrokersNeeded.toString)
          topicConfigOverride.put(KafkaConfig.OffsetsTopicReplicationFactorProp, numBrokersNeeded.toString)
          EasyMock.expect(groupCoordinator.offsetsTopicConfigs).andReturn(new Properties)
          true

        case Topic.TRANSACTION_STATE_TOPIC_NAME =>
          topicConfigOverride.put(KafkaConfig.TransactionsTopicPartitionsProp, numBrokersNeeded.toString)
          topicConfigOverride.put(KafkaConfig.TransactionsTopicReplicationFactorProp, numBrokersNeeded.toString)
          EasyMock.expect(txnCoordinator.transactionTopicConfigs).andReturn(new Properties)
          true
        case _ =>
          topicConfigOverride.put(KafkaConfig.NumPartitionsProp, numBrokersNeeded.toString)
          topicConfigOverride.put(KafkaConfig.DefaultReplicationFactorProp, numBrokersNeeded.toString)
          false
      }

    val metadataRequest = new MetadataRequest.Builder(
      List(topicName).asJava, enableAutoTopicCreation
    ).build(requestHeader.apiVersion)
    val request = buildRequest(metadataRequest)

    val capturedResponse = expectNoThrottling(request)

    val capturedRequest = verifyTopicCreation(topicName, enableAutoTopicCreation, isInternal, request)

    EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, authorizer,
      autoTopicCreationManager, forwardingManager, clientControllerQuotaManager, groupCoordinator, txnCoordinator)

    createKafkaApis(authorizer = Some(authorizer), enableForwarding = enableAutoTopicCreation,
      overrideProperties = topicConfigOverride).handleTopicMetadataRequest(request)

    val response = capturedResponse.getValue.asInstanceOf[MetadataResponse]
    val expectedMetadataResponse = util.Collections.singletonList(new TopicMetadata(
      expectedError,
      topicName,
      isInternal,
      util.Collections.emptyList()
    ))

    assertEquals(expectedMetadataResponse, response.topicMetadata())

    if (enableAutoTopicCreation) {
      assertTrue(capturedRequest.getValue.isDefined)
      assertEquals(request.context, capturedRequest.getValue.get)
    }

    verify(authorizer, autoTopicCreationManager)
  }

  private def verifyTopicCreation(topicName: String,
                                  enableAutoTopicCreation: Boolean,
                                  isInternal: Boolean,
                                  request: RequestChannel.Request): Capture[Option[RequestContext]] = {
    val capturedRequest = EasyMock.newCapture[Option[RequestContext]]()
    if (enableAutoTopicCreation) {
      EasyMock.expect(clientControllerQuotaManager.newPermissiveQuotaFor(EasyMock.eq(request)))
        .andReturn(UnboundedControllerMutationQuota)

      EasyMock.expect(autoTopicCreationManager.createTopics(
        EasyMock.eq(Set(topicName)),
        EasyMock.eq(UnboundedControllerMutationQuota),
        EasyMock.capture(capturedRequest))).andReturn(
        Seq(new MetadataResponseTopic()
        .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
        .setIsInternal(isInternal)
        .setName(topicName))
      ).once()
    }
    capturedRequest
  }

  private def setupBrokerMetadata(hasEnoughLiveBrokers: Boolean, numBrokersNeeded: Int): Unit = {
    addTopicToMetadataCache("some-topic", 1,
      if (hasEnoughLiveBrokers)
        numBrokersNeeded
      else
        numBrokersNeeded - 1)
  }

  @Test
  def testInvalidMetadataRequestReturnsError(): Unit = {
    // Construct invalid MetadataRequestTopics. We will try each one separately and ensure the error is thrown.
    val topics = List(new MetadataRequestData.MetadataRequestTopic().setName(null).setTopicId(Uuid.randomUuid()),
      new MetadataRequestData.MetadataRequestTopic().setName(null),
      new MetadataRequestData.MetadataRequestTopic().setTopicId(Uuid.randomUuid()),
      new MetadataRequestData.MetadataRequestTopic().setName("topic1").setTopicId(Uuid.randomUuid()))

    EasyMock.replay(replicaManager, clientRequestQuotaManager,
      autoTopicCreationManager, forwardingManager, clientControllerQuotaManager, groupCoordinator, txnCoordinator)

    // if version is 10 or 11, the invalid topic metadata should return an error
    val invalidVersions = Set(10, 11)
    invalidVersions.foreach( version =>
      topics.foreach(topic => {
        val metadataRequestData = new MetadataRequestData().setTopics(Collections.singletonList(topic))
        val request = buildRequest(new MetadataRequest(metadataRequestData, version.toShort))
        val kafkaApis = createKafkaApis()

        val capturedResponse = EasyMock.newCapture[AbstractResponse]()
        EasyMock.expect(requestChannel.sendResponse(
          EasyMock.eq(request),
          EasyMock.capture(capturedResponse),
          EasyMock.anyObject()
        ))

        EasyMock.replay(requestChannel)
        kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

        val response = capturedResponse.getValue.asInstanceOf[MetadataResponse]
        assertEquals(1, response.topicMetadata.size)
        assertEquals(1, response.errorCounts.get(Errors.INVALID_REQUEST))
        response.data.topics.forEach(topic => assertNotEquals(null, topic.name))
        reset(requestChannel)
      })
    )
  }

  @Test
  def testOffsetCommitWithInvalidPartition(): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 1)

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
      val capturedResponse = expectNoThrottling(request)
      EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel)
      createKafkaApis().handleOffsetCommitRequest(request, RequestLocal.withThreadConfinedCaching)

      val response = capturedResponse.getValue.asInstanceOf[OffsetCommitResponse]
      assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION,
        Errors.forCode(response.data.topics().get(0).partitions().get(0).errorCode))
    }

    checkInvalidPartition(-1)
    checkInvalidPartition(1) // topic has only one partition
  }

  @Test
  def testTxnOffsetCommitWithInvalidPartition(): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 1)

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
      ).build()
      val request = buildRequest(offsetCommitRequest)

      val capturedResponse = expectNoThrottling(request)
      EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel)
      createKafkaApis().handleTxnOffsetCommitRequest(request, RequestLocal.withThreadConfinedCaching)

      val response = capturedResponse.getValue.asInstanceOf[TxnOffsetCommitResponse]
      assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, response.errors().get(invalidTopicPartition))
    }

    checkInvalidPartition(-1)
    checkInvalidPartition(1) // topic has only one partition
  }

  @Test
  def shouldReplaceCoordinatorNotAvailableWithLoadInProcessInTxnOffsetCommitWithOlderClient(): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 2)

    for (version <- ApiKeys.TXN_OFFSET_COMMIT.oldestVersion to ApiKeys.TXN_OFFSET_COMMIT.latestVersion) {
      EasyMock.reset(replicaManager, clientRequestQuotaManager, requestChannel, groupCoordinator)

      val topicPartition = new TopicPartition(topic, 1)
      val capturedResponse: Capture[AbstractResponse] = EasyMock.newCapture()
      val responseCallback: Capture[Map[TopicPartition, Errors] => Unit] = EasyMock.newCapture()

      val partitionOffsetCommitData = new TxnOffsetCommitRequest.CommittedOffset(15L, "", Optional.empty())
      val groupId = "groupId"

      val producerId = 15L
      val epoch = 0.toShort

      val offsetCommitRequest = new TxnOffsetCommitRequest.Builder(
        "txnId",
        groupId,
        producerId,
        epoch,
        Map(topicPartition -> partitionOffsetCommitData).asJava,
      ).build(version.toShort)
      val request = buildRequest(offsetCommitRequest)

      val requestLocal = RequestLocal.withThreadConfinedCaching
      EasyMock.expect(groupCoordinator.handleTxnCommitOffsets(
        EasyMock.eq(groupId),
        EasyMock.eq(producerId),
        EasyMock.eq(epoch),
        EasyMock.anyString(),
        EasyMock.eq(Option.empty),
        EasyMock.anyInt(),
        EasyMock.anyObject(),
        EasyMock.capture(responseCallback),
        EasyMock.eq(requestLocal)
      )).andAnswer(
        () => responseCallback.getValue.apply(Map(topicPartition -> Errors.COORDINATOR_LOAD_IN_PROGRESS)))

      EasyMock.expect(requestChannel.sendResponse(
        EasyMock.eq(request),
        EasyMock.capture(capturedResponse),
        EasyMock.eq(None)
      ))

      EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, groupCoordinator)

      createKafkaApis().handleTxnOffsetCommitRequest(request, requestLocal)

      val response = capturedResponse.getValue.asInstanceOf[TxnOffsetCommitResponse]

      if (version < 2) {
        assertEquals(Errors.COORDINATOR_NOT_AVAILABLE, response.errors().get(topicPartition))
      } else {
        assertEquals(Errors.COORDINATOR_LOAD_IN_PROGRESS, response.errors().get(topicPartition))
      }
    }
  }

  @Test
  def shouldReplaceProducerFencedWithInvalidProducerEpochInInitProducerIdWithOlderClient(): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 2)

    for (version <- ApiKeys.INIT_PRODUCER_ID.oldestVersion to ApiKeys.INIT_PRODUCER_ID.latestVersion) {

      EasyMock.reset(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      val capturedResponse: Capture[AbstractResponse] = EasyMock.newCapture()
      val responseCallback: Capture[InitProducerIdResult => Unit] = EasyMock.newCapture()

      val transactionalId = "txnId"
      val producerId = if (version < 3)
        RecordBatch.NO_PRODUCER_ID
      else
        15

      val epoch = if (version < 3)
        RecordBatch.NO_PRODUCER_EPOCH
      else
        0.toShort

      val txnTimeoutMs = TimeUnit.MINUTES.toMillis(15).toInt

      val initProducerIdRequest = new InitProducerIdRequest.Builder(
        new InitProducerIdRequestData()
          .setTransactionalId(transactionalId)
          .setTransactionTimeoutMs(txnTimeoutMs)
          .setProducerId(producerId)
          .setProducerEpoch(epoch)
      ).build(version.toShort)

      val request = buildRequest(initProducerIdRequest)

      val expectedProducerIdAndEpoch = if (version < 3)
        Option.empty
      else
        Option(new ProducerIdAndEpoch(producerId, epoch))

      val requestLocal = RequestLocal.withThreadConfinedCaching
      EasyMock.expect(txnCoordinator.handleInitProducerId(
        EasyMock.eq(transactionalId),
        EasyMock.eq(txnTimeoutMs),
        EasyMock.eq(expectedProducerIdAndEpoch),
        EasyMock.capture(responseCallback),
        EasyMock.eq(requestLocal)
      )).andAnswer(
        () => responseCallback.getValue.apply(InitProducerIdResult(producerId, epoch, Errors.PRODUCER_FENCED)))

      EasyMock.expect(requestChannel.sendResponse(
        EasyMock.eq(request),
        EasyMock.capture(capturedResponse),
        EasyMock.eq(None)
      ))

      EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      createKafkaApis().handleInitProducerIdRequest(request, requestLocal)

      val response = capturedResponse.getValue.asInstanceOf[InitProducerIdResponse]

      if (version < 4) {
        assertEquals(Errors.INVALID_PRODUCER_EPOCH.code, response.data.errorCode)
      } else {
        assertEquals(Errors.PRODUCER_FENCED.code, response.data.errorCode)
      }
    }
  }

  @Test
  def shouldReplaceProducerFencedWithInvalidProducerEpochInAddOffsetToTxnWithOlderClient(): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 2)

    for (version <- ApiKeys.ADD_OFFSETS_TO_TXN.oldestVersion to ApiKeys.ADD_OFFSETS_TO_TXN.latestVersion) {

      EasyMock.reset(replicaManager, clientRequestQuotaManager, requestChannel, groupCoordinator, txnCoordinator)

      val capturedResponse: Capture[AbstractResponse] = EasyMock.newCapture()
      val responseCallback: Capture[Errors => Unit] = EasyMock.newCapture()

      val groupId = "groupId"
      val transactionalId = "txnId"
      val producerId = 15L
      val epoch = 0.toShort

      val addOffsetsToTxnRequest = new AddOffsetsToTxnRequest.Builder(
        new AddOffsetsToTxnRequestData()
          .setGroupId(groupId)
          .setTransactionalId(transactionalId)
          .setProducerId(producerId)
          .setProducerEpoch(epoch)
      ).build(version.toShort)
      val request = buildRequest(addOffsetsToTxnRequest)

      val partition = 1
      EasyMock.expect(groupCoordinator.partitionFor(
        EasyMock.eq(groupId)
      )).andReturn(partition)

      val requestLocal = RequestLocal.withThreadConfinedCaching
      EasyMock.expect(txnCoordinator.handleAddPartitionsToTransaction(
        EasyMock.eq(transactionalId),
        EasyMock.eq(producerId),
        EasyMock.eq(epoch),
        EasyMock.eq(Set(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, partition))),
        EasyMock.capture(responseCallback),
        EasyMock.eq(requestLocal)
      )).andAnswer(
        () => responseCallback.getValue.apply(Errors.PRODUCER_FENCED))

      EasyMock.expect(requestChannel.sendResponse(
        EasyMock.eq(request),
        EasyMock.capture(capturedResponse),
        EasyMock.eq(None)
      ))

      EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator, groupCoordinator)

      createKafkaApis().handleAddOffsetsToTxnRequest(request, requestLocal)

      val response = capturedResponse.getValue.asInstanceOf[AddOffsetsToTxnResponse]

      if (version < 2) {
        assertEquals(Errors.INVALID_PRODUCER_EPOCH.code, response.data.errorCode)
      } else {
        assertEquals(Errors.PRODUCER_FENCED.code, response.data.errorCode)
      }
    }
  }

  @Test
  def shouldReplaceProducerFencedWithInvalidProducerEpochInAddPartitionToTxnWithOlderClient(): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 2)

    for (version <- ApiKeys.ADD_PARTITIONS_TO_TXN.oldestVersion to ApiKeys.ADD_PARTITIONS_TO_TXN.latestVersion) {

      EasyMock.reset(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      val capturedResponse: Capture[AbstractResponse] = EasyMock.newCapture()
      val responseCallback: Capture[Errors => Unit] = EasyMock.newCapture()

      val transactionalId = "txnId"
      val producerId = 15L
      val epoch = 0.toShort

      val partition = 1
      val topicPartition = new TopicPartition(topic, partition)

      val addPartitionsToTxnRequest = new AddPartitionsToTxnRequest.Builder(
        transactionalId,
        producerId,
        epoch,
        Collections.singletonList(topicPartition)
      ).build(version.toShort)
      val request = buildRequest(addPartitionsToTxnRequest)

      val requestLocal = RequestLocal.withThreadConfinedCaching
      EasyMock.expect(txnCoordinator.handleAddPartitionsToTransaction(
        EasyMock.eq(transactionalId),
        EasyMock.eq(producerId),
        EasyMock.eq(epoch),
        EasyMock.eq(Set(topicPartition)),
        EasyMock.capture(responseCallback),
        EasyMock.eq(requestLocal)
      )).andAnswer(
        () => responseCallback.getValue.apply(Errors.PRODUCER_FENCED))

      EasyMock.expect(requestChannel.sendResponse(
        EasyMock.eq(request),
        EasyMock.capture(capturedResponse),
        EasyMock.eq(None)
      ))

      EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      createKafkaApis().handleAddPartitionToTxnRequest(request, requestLocal)

      val response = capturedResponse.getValue.asInstanceOf[AddPartitionsToTxnResponse]

      if (version < 2) {
        assertEquals(Collections.singletonMap(topicPartition, Errors.INVALID_PRODUCER_EPOCH), response.errors())
      } else {
        assertEquals(Collections.singletonMap(topicPartition, Errors.PRODUCER_FENCED), response.errors())
      }
    }
  }

  @Test
  def shouldReplaceProducerFencedWithInvalidProducerEpochInEndTxnWithOlderClient(): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 2)

    for (version <- ApiKeys.END_TXN.oldestVersion to ApiKeys.END_TXN.latestVersion) {
      EasyMock.reset(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      val capturedResponse: Capture[AbstractResponse] = EasyMock.newCapture()
      val responseCallback: Capture[Errors => Unit]  = EasyMock.newCapture()

      val transactionalId = "txnId"
      val producerId = 15L
      val epoch = 0.toShort

      val endTxnRequest = new EndTxnRequest.Builder(
        new EndTxnRequestData()
          .setTransactionalId(transactionalId)
          .setProducerId(producerId)
          .setProducerEpoch(epoch)
          .setCommitted(true)
      ).build(version.toShort)
      val request = buildRequest(endTxnRequest)

      val requestLocal = RequestLocal.withThreadConfinedCaching
      EasyMock.expect(txnCoordinator.handleEndTransaction(
        EasyMock.eq(transactionalId),
        EasyMock.eq(producerId),
        EasyMock.eq(epoch),
        EasyMock.eq(TransactionResult.COMMIT),
        EasyMock.capture(responseCallback),
        EasyMock.eq(requestLocal)
      )).andAnswer(
        () => responseCallback.getValue.apply(Errors.PRODUCER_FENCED))

      EasyMock.expect(requestChannel.sendResponse(
        EasyMock.eq(request),
        EasyMock.capture(capturedResponse),
        EasyMock.eq(None)
      ))

      EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)
      createKafkaApis().handleEndTxnRequest(request, requestLocal)

      val response = capturedResponse.getValue.asInstanceOf[EndTxnResponse]

      if (version < 2) {
        assertEquals(Errors.INVALID_PRODUCER_EPOCH.code, response.data.errorCode)
      } else {
        assertEquals(Errors.PRODUCER_FENCED.code, response.data.errorCode)
      }
    }
  }

  @Test
  def shouldReplaceProducerFencedWithInvalidProducerEpochInProduceResponse(): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 2)

    for (version <- ApiKeys.PRODUCE.oldestVersion to ApiKeys.PRODUCE.latestVersion) {

      EasyMock.reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      val responseCallback: Capture[Map[TopicPartition, PartitionResponse] => Unit] = EasyMock.newCapture()

      val tp = new TopicPartition("topic", 0)

      val produceRequest = ProduceRequest.forCurrentMagic(new ProduceRequestData()
        .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
          Collections.singletonList(new ProduceRequestData.TopicProduceData()
            .setName(tp.topic).setPartitionData(Collections.singletonList(
            new ProduceRequestData.PartitionProduceData()
              .setIndex(tp.partition)
              .setRecords(MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord("test".getBytes))))))
            .iterator))
        .setAcks(1.toShort)
        .setTimeoutMs(5000))
        .build(version.toShort)
      val request = buildRequest(produceRequest)

      EasyMock.expect(replicaManager.appendRecords(EasyMock.anyLong(),
        EasyMock.anyShort(),
        EasyMock.eq(false),
        EasyMock.eq(AppendOrigin.Client),
        EasyMock.anyObject(),
        EasyMock.capture(responseCallback),
        EasyMock.anyObject(),
        EasyMock.anyObject(),
        EasyMock.anyObject())
      ).andAnswer(() => responseCallback.getValue.apply(Map(tp -> new PartitionResponse(Errors.INVALID_PRODUCER_EPOCH))))

      val capturedResponse = expectNoThrottling(request)
      EasyMock.expect(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
        anyObject[RequestChannel.Request](), anyDouble, anyLong)).andReturn(0)

      EasyMock.replay(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      createKafkaApis().handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

      val response = capturedResponse.getValue.asInstanceOf[ProduceResponse]

      assertEquals(1, response.data.responses.size)
      val topicProduceResponse = response.data.responses.asScala.head
      assertEquals(1, topicProduceResponse.partitionResponses.size)   
      val partitionProduceResponse = topicProduceResponse.partitionResponses.asScala.head
      assertEquals(Errors.INVALID_PRODUCER_EPOCH, Errors.forCode(partitionProduceResponse.errorCode))
    }
  }

  @Test
  def testAddPartitionsToTxnWithInvalidPartition(): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 1)

    def checkInvalidPartition(invalidPartitionId: Int): Unit = {
      EasyMock.reset(replicaManager, clientRequestQuotaManager, requestChannel)

      val invalidTopicPartition = new TopicPartition(topic, invalidPartitionId)
      val addPartitionsToTxnRequest = new AddPartitionsToTxnRequest.Builder(
        "txnlId", 15L, 0.toShort, List(invalidTopicPartition).asJava
      ).build()
      val request = buildRequest(addPartitionsToTxnRequest)

      val capturedResponse = expectNoThrottling(request)
      EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel)
      createKafkaApis().handleAddPartitionToTxnRequest(request, RequestLocal.withThreadConfinedCaching)

      val response = capturedResponse.getValue.asInstanceOf[AddPartitionsToTxnResponse]
      assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, response.errors().get(invalidTopicPartition))
    }

    checkInvalidPartition(-1)
    checkInvalidPartition(1) // topic has only one partition
  }

  @Test
  def shouldThrowUnsupportedVersionExceptionOnHandleAddOffsetToTxnRequestWhenInterBrokerProtocolNotSupported(): Unit = {
    assertThrows(classOf[UnsupportedVersionException],
      () => createKafkaApis(KAFKA_0_10_2_IV0).handleAddOffsetsToTxnRequest(null, RequestLocal.withThreadConfinedCaching))
  }

  @Test
  def shouldThrowUnsupportedVersionExceptionOnHandleAddPartitionsToTxnRequestWhenInterBrokerProtocolNotSupported(): Unit = {
    assertThrows(classOf[UnsupportedVersionException],
      () => createKafkaApis(KAFKA_0_10_2_IV0).handleAddPartitionToTxnRequest(null, RequestLocal.withThreadConfinedCaching))
  }

  @Test
  def shouldThrowUnsupportedVersionExceptionOnHandleTxnOffsetCommitRequestWhenInterBrokerProtocolNotSupported(): Unit = {
    assertThrows(classOf[UnsupportedVersionException],
      () => createKafkaApis(KAFKA_0_10_2_IV0).handleAddPartitionToTxnRequest(null, RequestLocal.withThreadConfinedCaching))
  }

  @Test
  def shouldThrowUnsupportedVersionExceptionOnHandleEndTxnRequestWhenInterBrokerProtocolNotSupported(): Unit = {
    assertThrows(classOf[UnsupportedVersionException],
      () => createKafkaApis(KAFKA_0_10_2_IV0).handleEndTxnRequest(null, RequestLocal.withThreadConfinedCaching))
  }

  @Test
  def shouldThrowUnsupportedVersionExceptionOnHandleWriteTxnMarkersRequestWhenInterBrokerProtocolNotSupported(): Unit = {
    assertThrows(classOf[UnsupportedVersionException],
      () => createKafkaApis(KAFKA_0_10_2_IV0).handleWriteTxnMarkersRequest(null, RequestLocal.withThreadConfinedCaching))
  }

  @Test
  def shouldRespondWithUnsupportedForMessageFormatOnHandleWriteTxnMarkersWhenMagicLowerThanRequired(): Unit = {
    val topicPartition = new TopicPartition("t", 0)
    val (writeTxnMarkersRequest, request) = createWriteTxnMarkersRequest(asList(topicPartition))
    val expectedErrors = Map(topicPartition -> Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT).asJava
    val capturedResponse: Capture[AbstractResponse] = EasyMock.newCapture()

    EasyMock.expect(replicaManager.getMagic(topicPartition))
      .andReturn(Some(RecordBatch.MAGIC_VALUE_V1))
    EasyMock.expect(requestChannel.sendResponse(
      EasyMock.eq(request),
      EasyMock.capture(capturedResponse),
      EasyMock.eq(None)
    ))
    EasyMock.replay(replicaManager, replicaQuotaManager, requestChannel)

    createKafkaApis().handleWriteTxnMarkersRequest(request, RequestLocal.withThreadConfinedCaching)

    val markersResponse = capturedResponse.getValue.asInstanceOf[WriteTxnMarkersResponse]
    assertEquals(expectedErrors, markersResponse.errorsByProducerId.get(1L))
  }

  @Test
  def shouldRespondWithUnknownTopicWhenPartitionIsNotHosted(): Unit = {
    val topicPartition = new TopicPartition("t", 0)
    val (writeTxnMarkersRequest, request) = createWriteTxnMarkersRequest(asList(topicPartition))
    val expectedErrors = Map(topicPartition -> Errors.UNKNOWN_TOPIC_OR_PARTITION).asJava
    val capturedResponse: Capture[AbstractResponse] = EasyMock.newCapture()

    EasyMock.expect(replicaManager.getMagic(topicPartition))
      .andReturn(None)
    EasyMock.expect(requestChannel.sendResponse(
      EasyMock.eq(request),
      EasyMock.capture(capturedResponse),
      EasyMock.eq(None)
    ))
    EasyMock.replay(replicaManager, replicaQuotaManager, requestChannel)

    createKafkaApis().handleWriteTxnMarkersRequest(request, RequestLocal.withThreadConfinedCaching)

    val markersResponse = capturedResponse.getValue.asInstanceOf[WriteTxnMarkersResponse]
    assertEquals(expectedErrors, markersResponse.errorsByProducerId.get(1L))
  }

  @Test
  def shouldRespondWithUnsupportedMessageFormatForBadPartitionAndNoErrorsForGoodPartition(): Unit = {
    val tp1 = new TopicPartition("t", 0)
    val tp2 = new TopicPartition("t1", 0)
    val (writeTxnMarkersRequest, request) = createWriteTxnMarkersRequest(asList(tp1, tp2))
    val expectedErrors = Map(tp1 -> Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT, tp2 -> Errors.NONE).asJava

    val capturedResponse: Capture[AbstractResponse] = EasyMock.newCapture()
    val responseCallback: Capture[Map[TopicPartition, PartitionResponse] => Unit] = EasyMock.newCapture()

    EasyMock.expect(replicaManager.getMagic(tp1))
      .andReturn(Some(RecordBatch.MAGIC_VALUE_V1))
    EasyMock.expect(replicaManager.getMagic(tp2))
      .andReturn(Some(RecordBatch.MAGIC_VALUE_V2))

    val requestLocal = RequestLocal.withThreadConfinedCaching
    EasyMock.expect(replicaManager.appendRecords(EasyMock.anyLong(),
      EasyMock.anyShort(),
      EasyMock.eq(true),
      EasyMock.eq(AppendOrigin.Coordinator),
      EasyMock.anyObject(),
      EasyMock.capture(responseCallback),
      EasyMock.anyObject(),
      EasyMock.anyObject(),
      EasyMock.eq(requestLocal))
    ).andAnswer(() => responseCallback.getValue.apply(Map(tp2 -> new PartitionResponse(Errors.NONE))))

    EasyMock.expect(requestChannel.sendResponse(
      EasyMock.eq(request),
      EasyMock.capture(capturedResponse),
      EasyMock.eq(None)
    ))
    EasyMock.replay(replicaManager, replicaQuotaManager, requestChannel)

    createKafkaApis().handleWriteTxnMarkersRequest(request, requestLocal)

    val markersResponse = capturedResponse.getValue.asInstanceOf[WriteTxnMarkersResponse]
    assertEquals(expectedErrors, markersResponse.errorsByProducerId.get(1L))
    EasyMock.verify(replicaManager)
  }

  @Test
  def shouldResignCoordinatorsIfStopReplicaReceivedWithDeleteFlagAndLeaderEpoch(): Unit = {
    shouldResignCoordinatorsIfStopReplicaReceivedWithDeleteFlag(
      LeaderAndIsr.initialLeaderEpoch + 2, deletePartition = true)
  }

  @Test
  def shouldResignCoordinatorsIfStopReplicaReceivedWithDeleteFlagAndDeleteSentinel(): Unit = {
    shouldResignCoordinatorsIfStopReplicaReceivedWithDeleteFlag(
      LeaderAndIsr.EpochDuringDelete, deletePartition = true)
  }

  @Test
  def shouldResignCoordinatorsIfStopReplicaReceivedWithDeleteFlagAndNoEpochSentinel(): Unit = {
    shouldResignCoordinatorsIfStopReplicaReceivedWithDeleteFlag(
      LeaderAndIsr.NoEpoch, deletePartition = true)
  }

  @Test
  def shouldNotResignCoordinatorsIfStopReplicaReceivedWithoutDeleteFlag(): Unit = {
    shouldResignCoordinatorsIfStopReplicaReceivedWithDeleteFlag(
      LeaderAndIsr.initialLeaderEpoch + 2, deletePartition = false)
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
        .setTopicName(groupMetadataPartition.topic)
        .setPartitionStates(Seq(new StopReplicaPartitionState()
          .setPartitionIndex(groupMetadataPartition.partition)
          .setLeaderEpoch(leaderEpoch)
          .setDeletePartition(deletePartition)).asJava),
      new StopReplicaTopicState()
        .setTopicName(txnStatePartition.topic)
        .setPartitionStates(Seq(new StopReplicaPartitionState()
          .setPartitionIndex(txnStatePartition.partition)
          .setLeaderEpoch(leaderEpoch)
          .setDeletePartition(deletePartition)).asJava),
      new StopReplicaTopicState()
        .setTopicName(fooPartition.topic)
        .setPartitionStates(Seq(new StopReplicaPartitionState()
          .setPartitionIndex(fooPartition.partition)
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
      if (leaderEpoch >= 0) {
        groupCoordinator.onResignation(groupMetadataPartition.partition, Some(leaderEpoch))
      } else {
        groupCoordinator.onResignation(groupMetadataPartition.partition, None)
      }
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

    val capturedResponse: Capture[AbstractResponse] = EasyMock.newCapture()
    val responseCallback: Capture[Map[TopicPartition, PartitionResponse] => Unit] = EasyMock.newCapture()

    EasyMock.expect(replicaManager.getMagic(tp1))
      .andReturn(None)
    EasyMock.expect(replicaManager.getMagic(tp2))
      .andReturn(Some(RecordBatch.MAGIC_VALUE_V2))

    val requestLocal = RequestLocal.withThreadConfinedCaching
    EasyMock.expect(replicaManager.appendRecords(EasyMock.anyLong(),
      EasyMock.anyShort(),
      EasyMock.eq(true),
      EasyMock.eq(AppendOrigin.Coordinator),
      EasyMock.anyObject(),
      EasyMock.capture(responseCallback),
      EasyMock.anyObject(),
      EasyMock.anyObject(),
      EasyMock.eq(requestLocal))
    ).andAnswer(() => responseCallback.getValue.apply(Map(tp2 -> new PartitionResponse(Errors.NONE))))

    EasyMock.expect(requestChannel.sendResponse(
      EasyMock.eq(request),
      EasyMock.capture(capturedResponse),
      EasyMock.eq(None)
    ))
    EasyMock.replay(replicaManager, replicaQuotaManager, requestChannel)

    createKafkaApis().handleWriteTxnMarkersRequest(request, requestLocal)

    val markersResponse = capturedResponse.getValue.asInstanceOf[WriteTxnMarkersResponse]
    assertEquals(expectedErrors, markersResponse.errorsByProducerId.get(1L))
    EasyMock.verify(replicaManager)
  }

  @Test
  def shouldAppendToLogOnWriteTxnMarkersWhenCorrectMagicVersion(): Unit = {
    val topicPartition = new TopicPartition("t", 0)
    val request = createWriteTxnMarkersRequest(asList(topicPartition))._2
    EasyMock.expect(replicaManager.getMagic(topicPartition))
      .andReturn(Some(RecordBatch.MAGIC_VALUE_V2))

    val requestLocal = RequestLocal.withThreadConfinedCaching
    EasyMock.expect(replicaManager.appendRecords(EasyMock.anyLong(),
      EasyMock.anyShort(),
      EasyMock.eq(true),
      EasyMock.eq(AppendOrigin.Coordinator),
      EasyMock.anyObject(),
      EasyMock.anyObject(),
      EasyMock.anyObject(),
      EasyMock.anyObject(),
      EasyMock.eq(requestLocal)))

    EasyMock.replay(replicaManager)

    createKafkaApis().handleWriteTxnMarkersRequest(request, requestLocal)
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
  def testLeaderReplicaIfLocalRaisesNotLeaderOrFollower(): Unit = {
    testListOffsetFailedGetLeaderReplica(Errors.NOT_LEADER_OR_FOLLOWER)
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

    val capturedResponse = expectNoThrottling(request)
    EasyMock.expect(groupCoordinator.handleDescribeGroup(EasyMock.eq(groupId)))
      .andReturn((Errors.NONE, groupSummary))
    EasyMock.replay(groupCoordinator, replicaManager, clientRequestQuotaManager, requestChannel)

    createKafkaApis().handleDescribeGroupRequest(request)

    val response = capturedResponse.getValue.asInstanceOf[DescribeGroupsResponse]

    val group = response.data.groups().get(0)
    assertEquals(Errors.NONE, Errors.forCode(group.errorCode))
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
    addTopicToMetadataCache("topic-1", numPartitions = 2)
    addTopicToMetadataCache("topic-2", numPartitions = 2)

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

    val requestLocal = RequestLocal.withThreadConfinedCaching
    val capturedResponse = expectNoThrottling(request)
    EasyMock.expect(groupCoordinator.handleDeleteOffsets(
      EasyMock.eq(group),
      EasyMock.eq(Seq(
        new TopicPartition("topic-1", 0),
        new TopicPartition("topic-1", 1),
        new TopicPartition("topic-2", 0),
        new TopicPartition("topic-2", 1)
      )),
      EasyMock.eq(requestLocal)
    )).andReturn((Errors.NONE, Map(
      new TopicPartition("topic-1", 0) -> Errors.NONE,
      new TopicPartition("topic-1", 1) -> Errors.NONE,
      new TopicPartition("topic-2", 0) -> Errors.NONE,
      new TopicPartition("topic-2", 1) -> Errors.NONE,
    )))

    EasyMock.replay(groupCoordinator, replicaManager, clientRequestQuotaManager, requestChannel)

    createKafkaApis().handleOffsetDeleteRequest(request, requestLocal)

    val response = capturedResponse.getValue.asInstanceOf[OffsetDeleteResponse]

    def errorForPartition(topic: String, partition: Int): Errors = {
      Errors.forCode(response.data.topics.find(topic).partitions.find(partition).errorCode)
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
    addTopicToMetadataCache(topic, numPartitions = 1)

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
      val capturedResponse = expectNoThrottling(request)

      val requestLocal = RequestLocal.withThreadConfinedCaching
      EasyMock.expect(groupCoordinator.handleDeleteOffsets(EasyMock.eq(group), EasyMock.eq(Seq.empty),
        EasyMock.eq(requestLocal))).andReturn((Errors.NONE, Map.empty))
      EasyMock.replay(groupCoordinator, replicaManager, clientRequestQuotaManager, requestChannel)

      createKafkaApis().handleOffsetDeleteRequest(request, requestLocal)

      val response = capturedResponse.getValue.asInstanceOf[OffsetDeleteResponse]

      assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION,
        Errors.forCode(response.data.topics.find(topic).partitions.find(invalidPartitionId).errorCode))
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

    val capturedResponse = expectNoThrottling(request)
    val requestLocal = RequestLocal.withThreadConfinedCaching
    EasyMock.expect(groupCoordinator.handleDeleteOffsets(EasyMock.eq(group), EasyMock.eq(Seq.empty),
      EasyMock.eq(requestLocal))).andReturn((Errors.GROUP_ID_NOT_FOUND, Map.empty))
    EasyMock.replay(groupCoordinator, replicaManager, clientRequestQuotaManager, requestChannel)

    createKafkaApis().handleOffsetDeleteRequest(request, requestLocal)

    val response = capturedResponse.getValue.asInstanceOf[OffsetDeleteResponse]

    assertEquals(Errors.GROUP_ID_NOT_FOUND, Errors.forCode(response.data.errorCode))
  }

  private def testListOffsetFailedGetLeaderReplica(error: Errors): Unit = {
    val tp = new TopicPartition("foo", 0)
    val isolationLevel = IsolationLevel.READ_UNCOMMITTED
    val currentLeaderEpoch = Optional.of[Integer](15)

    EasyMock.expect(replicaManager.fetchOffsetForTimestamp(
      EasyMock.eq(tp),
      EasyMock.eq(ListOffsetsRequest.EARLIEST_TIMESTAMP),
      EasyMock.eq(Some(isolationLevel)),
      EasyMock.eq(currentLeaderEpoch),
      fetchOnlyFromLeader = EasyMock.eq(true))
    ).andThrow(error.exception)

    val targetTimes = List(new ListOffsetsTopic()
      .setName(tp.topic)
      .setPartitions(List(new ListOffsetsPartition()
        .setPartitionIndex(tp.partition)
        .setTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP)
        .setCurrentLeaderEpoch(currentLeaderEpoch.get)).asJava)).asJava
    val listOffsetRequest = ListOffsetsRequest.Builder.forConsumer(true, isolationLevel, false)
      .setTargetTimes(targetTimes).build()
    val request = buildRequest(listOffsetRequest)
    val capturedResponse = expectNoThrottling(request)

    EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel)
    createKafkaApis().handleListOffsetRequest(request)

    val response = capturedResponse.getValue.asInstanceOf[ListOffsetsResponse]
    val partitionDataOptional = response.topics.asScala.find(_.name == tp.topic).get
      .partitions.asScala.find(_.partitionIndex == tp.partition)
    assertTrue(partitionDataOptional.isDefined)

    val partitionData = partitionDataOptional.get
    assertEquals(error.code, partitionData.errorCode)
    assertEquals(ListOffsetsResponse.UNKNOWN_OFFSET, partitionData.offset)
    assertEquals(ListOffsetsResponse.UNKNOWN_TIMESTAMP, partitionData.timestamp)
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

  /**
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
   * Metadata request to fetch all topics should not result in the followings:
   * 1) Auto topic creation
   * 2) UNKNOWN_TOPIC_OR_PARTITION
   *
   * This case is testing the case that a topic is being deleted from MetadataCache right after
   * authorization but before checking in MetadataCache.
   */
  @Test
  def getAllTopicMetadataShouldNotCreateTopicOrReturnUnknownTopicPartition(): Unit = {
    // Setup: authorizer authorizes 2 topics, but one got deleted in metadata cache
    metadataCache =
      EasyMock.partialMockBuilder(classOf[ZkMetadataCache])
        .withConstructor(classOf[Int])
        .withArgs(Int.box(brokerId))  // Need to box it for Scala 2.12 and before
        .addMockedMethod("getAllTopics")
        .addMockedMethod("getTopicMetadata")
        .createMock()

    // 2 topics returned for authorization in during handle
    val topicsReturnedFromMetadataCacheForAuthorization = Set("remaining-topic", "later-deleted-topic")
    expect(metadataCache.getAllTopics()).andReturn(topicsReturnedFromMetadataCacheForAuthorization).once()
    // 1 topic is deleted from metadata right at the time between authorization and the next getTopicMetadata() call
    expect(metadataCache.getTopicMetadata(
      EasyMock.eq(topicsReturnedFromMetadataCacheForAuthorization),
      anyObject[ListenerName],
      anyBoolean,
      anyBoolean
    )).andStubReturn(Seq(
      new MetadataResponseTopic()
        .setErrorCode(Errors.NONE.code)
        .setName("remaining-topic")
        .setIsInternal(false)
    ))

    EasyMock.replay(metadataCache)

    var createTopicIsCalled: Boolean = false;
    // Specific mock on zkClient for this use case
    // Expect it's never called to do auto topic creation
    expect(zkClient.setOrCreateEntityConfigs(
      EasyMock.eq(ConfigType.Topic),
      EasyMock.anyString,
      EasyMock.anyObject[Properties]
    )).andStubAnswer(() => {
      createTopicIsCalled = true
    })
    // No need to use
    expect(zkClient.getAllBrokersInCluster)
      .andStubReturn(Seq(new Broker(
        brokerId, "localhost", 9902,
        ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT), SecurityProtocol.PLAINTEXT
      )))

    EasyMock.replay(zkClient)

    val (requestListener, _) = updateMetadataCacheWithInconsistentListeners()
    val response = sendMetadataRequestWithInconsistentListeners(requestListener)

    assertFalse(createTopicIsCalled)
    val responseTopics = response.topicMetadata().asScala.map { metadata => metadata.topic() }
    assertEquals(List("remaining-topic"), responseTopics)
    assertTrue(response.topicsByError(Errors.UNKNOWN_TOPIC_OR_PARTITION).isEmpty)
  }

  @Test
  def testUnauthorizedTopicMetadataRequest(): Unit = {
    // 1. Set up broker information
    val plaintextListener = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val broker = new UpdateMetadataBroker()
      .setId(0)
      .setRack("rack")
      .setEndpoints(Seq(
        new UpdateMetadataEndpoint()
          .setHost("broker0")
          .setPort(9092)
          .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
          .setListener(plaintextListener.value)
      ).asJava)

    // 2. Set up authorizer
    val authorizer: Authorizer = EasyMock.niceMock(classOf[Authorizer])
    val unauthorizedTopic = "unauthorized-topic"
    val authorizedTopic = "authorized-topic"

    val expectedActions = Seq(
      new Action(AclOperation.DESCRIBE, new ResourcePattern(ResourceType.TOPIC, unauthorizedTopic, PatternType.LITERAL), 1, true, true),
      new Action(AclOperation.DESCRIBE, new ResourcePattern(ResourceType.TOPIC, authorizedTopic, PatternType.LITERAL), 1, true, true)
    )

    // Here we need to use AuthHelperTest.matchSameElements instead of EasyMock.eq since the order of the request is unknown
    EasyMock.expect(authorizer.authorize(anyObject[RequestContext], AuthHelperTest.matchSameElements(expectedActions.asJava)))
      .andAnswer { () =>
      val actions = EasyMock.getCurrentArguments.apply(1).asInstanceOf[util.List[Action]].asScala
      actions.map { action =>
        if (action.resourcePattern().name().equals(authorizedTopic))
          AuthorizationResult.ALLOWED
        else
          AuthorizationResult.DENIED
      }.asJava
    }.times(2)

    // 3. Set up MetadataCache
    val authorizedTopicId = Uuid.randomUuid()
    val unauthorizedTopicId = Uuid.randomUuid();

    val topicIds = new util.HashMap[String, Uuid]()
    topicIds.put(authorizedTopic, authorizedTopicId)
    topicIds.put(unauthorizedTopic, unauthorizedTopicId)

    def createDummyPartitionStates(topic: String) = {
      new UpdateMetadataPartitionState()
        .setTopicName(topic)
        .setPartitionIndex(0)
        .setControllerEpoch(0)
        .setLeader(0)
        .setLeaderEpoch(0)
        .setReplicas(Collections.singletonList(0))
        .setZkVersion(0)
        .setIsr(Collections.singletonList(0))
    }

    // Send UpdateMetadataReq to update MetadataCache
    val partitionStates = Seq(unauthorizedTopic, authorizedTopic).map(createDummyPartitionStates)

    val updateMetadataRequest = new UpdateMetadataRequest.Builder(ApiKeys.UPDATE_METADATA.latestVersion, 0,
      0, 0, partitionStates.asJava, Seq(broker).asJava, topicIds).build()
    metadataCache.asInstanceOf[ZkMetadataCache].updateMetadata(correlationId = 0, updateMetadataRequest)

    // 4. Send TopicMetadataReq using topicId
    val metadataReqByTopicId = new MetadataRequest.Builder(util.Arrays.asList(authorizedTopicId, unauthorizedTopicId)).build()
    val repByTopicId = buildRequest(metadataReqByTopicId, plaintextListener)
    val capturedMetadataByTopicIdResp = expectNoThrottling(repByTopicId)
    EasyMock.replay(clientRequestQuotaManager, requestChannel, authorizer)

    createKafkaApis(authorizer = Some(authorizer)).handleTopicMetadataRequest(repByTopicId)
    val metadataByTopicIdResp = capturedMetadataByTopicIdResp.getValue.asInstanceOf[MetadataResponse]

    val metadataByTopicId = metadataByTopicIdResp.data().topics().asScala.groupBy(_.topicId()).map(kv => (kv._1, kv._2.head))

    metadataByTopicId.foreach{ case (topicId, metadataResponseTopic) =>
      if (topicId == unauthorizedTopicId) {
        // Return an TOPIC_AUTHORIZATION_FAILED on unauthorized error regardless of leaking the existence of topic id
        assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code(), metadataResponseTopic.errorCode())
        // Do not return topic information on unauthorized error
        assertNull(metadataResponseTopic.name())
      } else {
        assertEquals(Errors.NONE.code(), metadataResponseTopic.errorCode())
        assertEquals(authorizedTopic, metadataResponseTopic.name())
      }
    }

    // 4. Send TopicMetadataReq using topic name
    EasyMock.reset(clientRequestQuotaManager, requestChannel)
    val metadataReqByTopicName = new MetadataRequest.Builder(util.Arrays.asList(authorizedTopic, unauthorizedTopic), false).build()
    val repByTopicName = buildRequest(metadataReqByTopicName, plaintextListener)
    val capturedMetadataByTopicNameResp = expectNoThrottling(repByTopicName)
    EasyMock.replay(clientRequestQuotaManager, requestChannel)

    createKafkaApis(authorizer = Some(authorizer)).handleTopicMetadataRequest(repByTopicName)
    val metadataByTopicNameResp = capturedMetadataByTopicNameResp.getValue.asInstanceOf[MetadataResponse]

    val metadataByTopicName = metadataByTopicNameResp.data().topics().asScala.groupBy(_.name()).map(kv => (kv._1, kv._2.head))

    metadataByTopicName.foreach{ case (topicName, metadataResponseTopic) =>
      if (topicName == unauthorizedTopic) {
        assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code(), metadataResponseTopic.errorCode())
        // Do not return topic Id on unauthorized error
        assertEquals(Uuid.ZERO_UUID, metadataResponseTopic.topicId())
      } else {
        assertEquals(Errors.NONE.code(), metadataResponseTopic.errorCode())
        assertEquals(authorizedTopicId, metadataResponseTopic.topicId())
      }
    }
  }

  /**
   * Verifies that sending a fetch request with version 9 works correctly when
   * ReplicaManager.getLogConfig returns None.
   */
  @Test
  def testFetchRequestV9WithNoLogConfig(): Unit = {
    val tp = new TopicPartition("foo", 0)
    addTopicToMetadataCache(tp.topic, numPartitions = 1)
    val hw = 3
    val timestamp = 1000

    expect(replicaManager.getLogConfig(EasyMock.eq(tp))).andReturn(None)

    replicaManager.fetchMessages(anyLong, anyInt, anyInt, anyInt, anyBoolean,
      anyObject[Seq[(TopicPartition, FetchRequest.PartitionData)]], anyObject[util.Map[String, Uuid]](), anyObject[ReplicaQuota],
      anyObject[Seq[(TopicPartition, FetchPartitionData)] => Unit](), anyObject[IsolationLevel],
      anyObject[Option[ClientMetadata]])
    expectLastCall[Unit].andAnswer(new IAnswer[Unit] {
      def answer: Unit = {
        val callback = getCurrentArguments.apply(8)
          .asInstanceOf[Seq[(TopicPartition, FetchPartitionData)] => Unit]
        val records = MemoryRecords.withRecords(CompressionType.NONE,
          new SimpleRecord(timestamp, "foo".getBytes(StandardCharsets.UTF_8)))
        callback(Seq(tp -> FetchPartitionData(Errors.NONE, hw, 0, records,
          None, None, None, Option.empty, isReassignmentFetch = false)))
      }
    })

    val fetchData = Map(tp -> new FetchRequest.PartitionData(0, 0, 1000,
      Optional.empty())).asJava
    val fetchMetadata = new JFetchMetadata(0, 0)
    val fetchContext = new FullFetchContext(time, new FetchSessionCache(1000, 100),
      fetchMetadata, fetchData, false, metadataCache.topicNamesToIds(), false)
    expect(fetchManager.newContext(
      anyObject[Short],
      anyObject[JFetchMetadata],
      anyObject[Boolean],
      anyObject[util.Map[TopicPartition, FetchRequest.PartitionData]],
      anyObject[util.List[TopicPartition]],
      anyObject[util.Map[String, Uuid]])).andReturn(fetchContext)

    EasyMock.expect(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      anyObject[RequestChannel.Request](), anyDouble, anyLong)).andReturn(0)

    val fetchRequest = new FetchRequest.Builder(9, 9, -1, 100, 0, fetchData,
      metadataCache.topicNamesToIds())
      .build()
    val request = buildRequest(fetchRequest)
    val capturedResponse = expectNoThrottling(request)

    EasyMock.replay(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, fetchManager)
    createKafkaApis().handleFetchRequest(request)

    val response = capturedResponse.getValue.asInstanceOf[FetchResponse]
    val responseData = response.responseData(metadataCache.topicIdsToNames(), 9)
    assertTrue(responseData.containsKey(tp))

    val partitionData = responseData.get(tp)
    assertEquals(Errors.NONE.code, partitionData.errorCode)
    assertEquals(hw, partitionData.highWatermark)
    assertEquals(-1, partitionData.lastStableOffset)
    assertEquals(0, partitionData.logStartOffset)
    assertEquals(timestamp, FetchResponse.recordsOrFail(partitionData).batches.iterator.next.maxTimestamp)
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
      anyObject(),
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
      ),
      RequestLocal.withThreadConfinedCaching)

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
    EasyMock.reset(groupCoordinator, clientRequestQuotaManager, requestChannel, replicaManager)

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
      EasyMock.capture(capturedCallback),
      EasyMock.anyObject()
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
    val capturedResponse = expectNoThrottling(requestChannelRequest)

    EasyMock.replay(groupCoordinator, clientRequestQuotaManager, requestChannel, replicaManager)
    createKafkaApis().handleJoinGroupRequest(requestChannelRequest, RequestLocal.withThreadConfinedCaching)

    EasyMock.verify(groupCoordinator)

    capturedCallback.getValue.apply(JoinGroupResult(memberId, Errors.INCONSISTENT_GROUP_PROTOCOL))

    val response = capturedResponse.getValue.asInstanceOf[JoinGroupResponse]

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
    EasyMock.reset(groupCoordinator, clientRequestQuotaManager, requestChannel, replicaManager)

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
      EasyMock.capture(capturedCallback),
      EasyMock.anyObject()
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
    val capturedResponse = expectNoThrottling(requestChannelRequest)

    EasyMock.replay(groupCoordinator, clientRequestQuotaManager, requestChannel, replicaManager)
    createKafkaApis().handleJoinGroupRequest(requestChannelRequest, RequestLocal.withThreadConfinedCaching)

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

    val response = capturedResponse.getValue.asInstanceOf[JoinGroupResponse]

    assertEquals(Errors.NONE, response.error)
    assertEquals(0, response.data.members.size)
    assertEquals(memberId, response.data.memberId)
    assertEquals(0, response.data.generationId)
    assertEquals(memberId, response.data.leader)
    assertEquals(protocolName, response.data.protocolName)
    assertEquals(protocolType, response.data.protocolType)

    EasyMock.verify(clientRequestQuotaManager, requestChannel)
  }

  @Test
  def testSyncGroupProtocolTypeAndName(): Unit = {
    for (version <- ApiKeys.SYNC_GROUP.oldestVersion to ApiKeys.SYNC_GROUP.latestVersion) {
      testSyncGroupProtocolTypeAndName(version.asInstanceOf[Short])
    }
  }

  def testSyncGroupProtocolTypeAndName(version: Short): Unit = {
    EasyMock.reset(groupCoordinator, clientRequestQuotaManager, requestChannel, replicaManager)

    val groupId = "group"
    val memberId = "member1"
    val protocolType = "consumer"
    val protocolName = "range"

    val capturedCallback = EasyMock.newCapture[SyncGroupCallback]()

    val requestLocal = RequestLocal.withThreadConfinedCaching
    EasyMock.expect(groupCoordinator.handleSyncGroup(
      EasyMock.eq(groupId),
      EasyMock.eq(0),
      EasyMock.eq(memberId),
      EasyMock.eq(if (version >= 5) Some(protocolType) else None),
      EasyMock.eq(if (version >= 5) Some(protocolName) else None),
      EasyMock.eq(None),
      EasyMock.eq(Map.empty),
      EasyMock.capture(capturedCallback),
      EasyMock.eq(requestLocal)
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
    val capturedResponse = expectNoThrottling(requestChannelRequest)

    EasyMock.replay(groupCoordinator, clientRequestQuotaManager, requestChannel, replicaManager)
    createKafkaApis().handleSyncGroupRequest(requestChannelRequest, requestLocal)

    EasyMock.verify(groupCoordinator)

    capturedCallback.getValue.apply(SyncGroupResult(
      protocolType = Some(protocolType),
      protocolName = Some(protocolName),
      memberAssignment = Array.empty,
      error = Errors.NONE
    ))

    val response = capturedResponse.getValue.asInstanceOf[SyncGroupResponse]

    assertEquals(Errors.NONE, response.error)
    assertArrayEquals(Array.empty[Byte], response.data.assignment)
    assertEquals(protocolType, response.data.protocolType)

    EasyMock.verify(clientRequestQuotaManager, requestChannel)
  }

  @Test
  def testSyncGroupProtocolTypeAndNameAreMandatorySinceV5(): Unit = {
    for (version <- ApiKeys.SYNC_GROUP.oldestVersion to ApiKeys.SYNC_GROUP.latestVersion) {
      testSyncGroupProtocolTypeAndNameAreMandatorySinceV5(version.asInstanceOf[Short])
    }
  }

  def testSyncGroupProtocolTypeAndNameAreMandatorySinceV5(version: Short): Unit = {
    EasyMock.reset(groupCoordinator, clientRequestQuotaManager, requestChannel, replicaManager)

    val groupId = "group"
    val memberId = "member1"
    val protocolType = "consumer"
    val protocolName = "range"

    val capturedCallback = EasyMock.newCapture[SyncGroupCallback]()

    val requestLocal = RequestLocal.withThreadConfinedCaching
    if (version < 5) {
      EasyMock.expect(groupCoordinator.handleSyncGroup(
        EasyMock.eq(groupId),
        EasyMock.eq(0),
        EasyMock.eq(memberId),
        EasyMock.eq(None),
        EasyMock.eq(None),
        EasyMock.eq(None),
        EasyMock.eq(Map.empty),
        EasyMock.capture(capturedCallback),
        EasyMock.eq(requestLocal)
      ))
    }

    val syncGroupRequest = new SyncGroupRequest.Builder(
      new SyncGroupRequestData()
        .setGroupId(groupId)
        .setGenerationId(0)
        .setMemberId(memberId)
    ).build(version)

    val requestChannelRequest = buildRequest(syncGroupRequest)
    val capturedResponse = expectNoThrottling(requestChannelRequest)

    EasyMock.replay(groupCoordinator, clientRequestQuotaManager, requestChannel, replicaManager)
    createKafkaApis().handleSyncGroupRequest(requestChannelRequest, requestLocal)

    EasyMock.verify(groupCoordinator)

    if (version < 5) {
      capturedCallback.getValue.apply(SyncGroupResult(
        protocolType = Some(protocolType),
        protocolName = Some(protocolName),
        memberAssignment = Array.empty,
        error = Errors.NONE
      ))
    }

    val response = capturedResponse.getValue.asInstanceOf[SyncGroupResponse]

    if (version < 5) {
      assertEquals(Errors.NONE, response.error)
    } else {
      assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL, response.error)
    }

    EasyMock.verify(clientRequestQuotaManager, requestChannel)
  }

  @Test
  def rejectJoinGroupRequestWhenStaticMembershipNotSupported(): Unit = {
    val joinGroupRequest = new JoinGroupRequest.Builder(
      new JoinGroupRequestData()
        .setGroupId("test")
        .setMemberId("test")
        .setGroupInstanceId("instanceId")
        .setProtocolType("consumer")
        .setProtocols(new JoinGroupRequestData.JoinGroupRequestProtocolCollection)
    ).build()

    val requestChannelRequest = buildRequest(joinGroupRequest)
    val capturedResponse = expectNoThrottling(requestChannelRequest)

    EasyMock.replay(clientRequestQuotaManager, requestChannel)
    createKafkaApis(KAFKA_2_2_IV1).handleJoinGroupRequest(requestChannelRequest, RequestLocal.withThreadConfinedCaching)

    val response = capturedResponse.getValue.asInstanceOf[JoinGroupResponse]
    assertEquals(Errors.UNSUPPORTED_VERSION, response.error())
    EasyMock.replay(groupCoordinator)
  }

  @Test
  def rejectSyncGroupRequestWhenStaticMembershipNotSupported(): Unit = {
    val syncGroupRequest = new SyncGroupRequest.Builder(
      new SyncGroupRequestData()
        .setGroupId("test")
        .setMemberId("test")
        .setGroupInstanceId("instanceId")
        .setGenerationId(1)
    ).build()

    val requestChannelRequest = buildRequest(syncGroupRequest)
    val capturedResponse = expectNoThrottling(requestChannelRequest)

    EasyMock.replay(clientRequestQuotaManager, requestChannel)
    createKafkaApis(KAFKA_2_2_IV1).handleSyncGroupRequest(requestChannelRequest, RequestLocal.withThreadConfinedCaching)

    val response = capturedResponse.getValue.asInstanceOf[SyncGroupResponse]
    assertEquals(Errors.UNSUPPORTED_VERSION, response.error)
    EasyMock.replay(groupCoordinator)
  }

  @Test
  def rejectHeartbeatRequestWhenStaticMembershipNotSupported(): Unit = {
    val heartbeatRequest = new HeartbeatRequest.Builder(
      new HeartbeatRequestData()
        .setGroupId("test")
        .setMemberId("test")
        .setGroupInstanceId("instanceId")
        .setGenerationId(1)
    ).build()
    val requestChannelRequest = buildRequest(heartbeatRequest)
    val capturedResponse = expectNoThrottling(requestChannelRequest)

    EasyMock.replay(clientRequestQuotaManager, requestChannel)
    createKafkaApis(KAFKA_2_2_IV1).handleHeartbeatRequest(requestChannelRequest)

    val response = capturedResponse.getValue.asInstanceOf[HeartbeatResponse]
    assertEquals(Errors.UNSUPPORTED_VERSION, response.error())
    EasyMock.replay(groupCoordinator)
  }

  @Test
  def rejectOffsetCommitRequestWhenStaticMembershipNotSupported(): Unit = {
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
    val capturedResponse = expectNoThrottling(requestChannelRequest)

    EasyMock.replay(clientRequestQuotaManager, requestChannel)
    createKafkaApis(KAFKA_2_2_IV1).handleOffsetCommitRequest(requestChannelRequest, RequestLocal.withThreadConfinedCaching)

    val expectedTopicErrors = Collections.singletonList(
      new OffsetCommitResponseData.OffsetCommitResponseTopic()
        .setName("test")
        .setPartitions(Collections.singletonList(
          new OffsetCommitResponseData.OffsetCommitResponsePartition()
            .setPartitionIndex(0)
            .setErrorCode(Errors.UNSUPPORTED_VERSION.code)
        ))
    )
    val response = capturedResponse.getValue.asInstanceOf[OffsetCommitResponse]
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

    setupBasicMetadataCache(tp0.topic, numPartitions = 1, 1, Uuid.randomUuid())
    val hw = 3

    val fetchData = Collections.singletonMap(tp0, new FetchRequest.PartitionData(0, 0, Int.MaxValue, Optional.of(leaderEpoch)))
    val fetchFromFollower = buildRequest(new FetchRequest.Builder(
      ApiKeys.FETCH.oldestVersion(), ApiKeys.FETCH.latestVersion(), 1, 1000, 0, fetchData,
        metadataCache.topicNamesToIds()).build())

    val records = MemoryRecords.withRecords(CompressionType.NONE,
      new SimpleRecord(1000, "foo".getBytes(StandardCharsets.UTF_8)))
    replicaManager.fetchMessages(anyLong, anyInt, anyInt, anyInt, anyBoolean,
      anyObject[Seq[(TopicPartition, FetchRequest.PartitionData)]], anyObject[util.Map[String, Uuid]](), anyObject[ReplicaQuota],
      anyObject[Seq[(TopicPartition, FetchPartitionData)] => Unit](), anyObject[IsolationLevel],
      anyObject[Option[ClientMetadata]])
    expectLastCall[Unit].andAnswer(new IAnswer[Unit] {
      def answer: Unit = {
        val callback = getCurrentArguments.apply(8).asInstanceOf[Seq[(TopicPartition, FetchPartitionData)] => Unit]
        callback(Seq(tp0 -> FetchPartitionData(Errors.NONE, hw, 0, records,
          None, None, None, Option.empty, isReassignmentFetch = isReassigning)))
      }
    })

    val fetchMetadata = new JFetchMetadata(0, 0)
    val fetchContext = new FullFetchContext(time, new FetchSessionCache(1000, 100),
      fetchMetadata, fetchData, true, metadataCache.topicNamesToIds(), true)
    expect(fetchManager.newContext(
      anyObject[Short],
      anyObject[JFetchMetadata],
      anyObject[Boolean],
      anyObject[util.Map[TopicPartition, FetchRequest.PartitionData]],
      anyObject[util.List[TopicPartition]],
      anyObject[util.Map[String, Uuid]])).andReturn(fetchContext)

    expect(replicaQuotaManager.record(anyLong()))
    expect(replicaManager.getLogConfig(EasyMock.eq(tp0))).andReturn(None)

    val partition: Partition = createNiceMock(classOf[Partition])
    expect(replicaManager.isAddingReplica(anyObject(), anyInt())).andReturn(isReassigning)

    replay(replicaManager, fetchManager, clientQuotaManager, requestChannel, replicaQuotaManager, partition)

    createKafkaApis().handle(fetchFromFollower, RequestLocal.withThreadConfinedCaching)

    if (isReassigning)
      assertEquals(records.sizeInBytes(), brokerTopicStats.allTopicsStats.reassignmentBytesOutPerSec.get.count())
    else
      assertEquals(0, brokerTopicStats.allTopicsStats.reassignmentBytesOutPerSec.get.count())
    assertEquals(records.sizeInBytes(), brokerTopicStats.allTopicsStats.replicationBytesOutRate.get.count())

  }

  @Test
  def rejectInitProducerIdWhenIdButNotEpochProvided(): Unit = {
    val initProducerIdRequest = new InitProducerIdRequest.Builder(
      new InitProducerIdRequestData()
        .setTransactionalId("known")
        .setTransactionTimeoutMs(TimeUnit.MINUTES.toMillis(15).toInt)
        .setProducerId(10)
        .setProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH)
    ).build()

    val requestChannelRequest = buildRequest(initProducerIdRequest)
    val capturedResponse = expectNoThrottling(requestChannelRequest)

    EasyMock.replay(clientRequestQuotaManager, requestChannel)
    createKafkaApis(KAFKA_2_2_IV1).handleInitProducerIdRequest(requestChannelRequest, RequestLocal.withThreadConfinedCaching)

    val response = capturedResponse.getValue.asInstanceOf[InitProducerIdResponse]
    assertEquals(Errors.INVALID_REQUEST, response.error)
  }

  @Test
  def rejectInitProducerIdWhenEpochButNotIdProvided(): Unit = {
    val initProducerIdRequest = new InitProducerIdRequest.Builder(
      new InitProducerIdRequestData()
        .setTransactionalId("known")
        .setTransactionTimeoutMs(TimeUnit.MINUTES.toMillis(15).toInt)
        .setProducerId(RecordBatch.NO_PRODUCER_ID)
        .setProducerEpoch(2)
    ).build()
    val requestChannelRequest = buildRequest(initProducerIdRequest)
    val capturedResponse = expectNoThrottling(requestChannelRequest)

    EasyMock.replay(clientRequestQuotaManager, requestChannel)
    createKafkaApis(KAFKA_2_2_IV1).handleInitProducerIdRequest(requestChannelRequest, RequestLocal.withThreadConfinedCaching)

    val response = capturedResponse.getValue.asInstanceOf[InitProducerIdResponse]
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
    val updateMetadataRequest = createBasicMetadataRequest("topicA", 1, brokerEpochInRequest, 1)
    val request = buildRequest(updateMetadataRequest)

    val capturedResponse: Capture[AbstractResponse] = EasyMock.newCapture()

    EasyMock.expect(controller.brokerEpoch).andStubReturn(currentBrokerEpoch)
    EasyMock.expect(replicaManager.maybeUpdateMetadataCache(
      EasyMock.eq(request.context.correlationId),
      EasyMock.anyObject()
    )).andStubReturn(
      Seq()
    )

    EasyMock.expect(requestChannel.sendResponse(
      EasyMock.eq(request),
      EasyMock.capture(capturedResponse),
      EasyMock.eq(None)
    ))
    EasyMock.replay(replicaManager, controller, requestChannel)

    createKafkaApis().handleUpdateMetadataRequest(request, RequestLocal.withThreadConfinedCaching)
    val updateMetadataResponse = capturedResponse.getValue.asInstanceOf[UpdateMetadataResponse]
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
    val capturedResponse: Capture[AbstractResponse] = EasyMock.newCapture()
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
      Collections.singletonMap("topicW", Uuid.randomUuid()),
      asList(new Node(0, "host0", 9090), new Node(1, "host1", 9091))
    ).build()
    val request = buildRequest(leaderAndIsrRequest)
    val response = new LeaderAndIsrResponse(new LeaderAndIsrResponseData()
      .setErrorCode(Errors.NONE.code)
      .setPartitionErrors(asList()), leaderAndIsrRequest.version())

    EasyMock.expect(controller.brokerEpoch).andStubReturn(currentBrokerEpoch)
    EasyMock.expect(replicaManager.becomeLeaderOrFollower(
      EasyMock.eq(request.context.correlationId),
      EasyMock.anyObject(),
      EasyMock.anyObject()
    )).andStubReturn(
      response
    )

    EasyMock.expect(requestChannel.sendResponse(
      EasyMock.eq(request),
      EasyMock.capture(capturedResponse),
      EasyMock.eq(None)
    ))
    EasyMock.replay(replicaManager, controller, requestChannel)

    createKafkaApis().handleLeaderAndIsrRequest(request)
    val leaderAndIsrResponse = capturedResponse.getValue.asInstanceOf[LeaderAndIsrResponse]
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
    val capturedResponse: Capture[AbstractResponse] = EasyMock.newCapture()
    val fooPartition = new TopicPartition("foo", 0)
    val topicStates = Seq(
      new StopReplicaTopicState()
        .setTopicName(fooPartition.topic)
        .setPartitionStates(Seq(new StopReplicaPartitionState()
          .setPartitionIndex(fooPartition.partition)
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
      EasyMock.eq(stopReplicaRequest.partitionStates().asScala)
    )).andStubReturn(
      (mutable.Map(
        fooPartition -> Errors.NONE
      ), Errors.NONE)
    )
    EasyMock.expect(requestChannel.sendResponse(
      EasyMock.eq(request),
      EasyMock.capture(capturedResponse),
      EasyMock.eq(None)
    ))

    EasyMock.replay(controller, replicaManager, requestChannel)

    createKafkaApis().handleStopReplicaRequest(request)
    val stopReplicaResponse = capturedResponse.getValue.asInstanceOf[StopReplicaResponse]
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

    val capturedResponse = expectNoThrottling(requestChannelRequest)
    val expectedStates: Set[String] = if (state.isDefined) Set(state.get) else Set()
    EasyMock.expect(groupCoordinator.handleListGroups(expectedStates))
      .andReturn((Errors.NONE, overviews))
    EasyMock.replay(groupCoordinator, clientRequestQuotaManager, requestChannel)

    createKafkaApis().handleListGroupsRequest(requestChannelRequest)

    val response = capturedResponse.getValue.asInstanceOf[ListGroupsResponse]
    assertEquals(Errors.NONE.code, response.data.errorCode)
    response
  }

  @Test
  def testDescribeClusterRequest(): Unit = {
    val plaintextListener = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val brokers = Seq(
      new UpdateMetadataBroker()
        .setId(0)
        .setRack("rack")
        .setEndpoints(Seq(
          new UpdateMetadataEndpoint()
            .setHost("broker0")
            .setPort(9092)
            .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
            .setListener(plaintextListener.value)
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
      0, 0, Seq.empty[UpdateMetadataPartitionState].asJava, brokers.asJava, Collections.emptyMap()).build()
    MetadataCacheTest.updateCache(metadataCache, updateMetadataRequest)

    val describeClusterRequest = new DescribeClusterRequest.Builder(new DescribeClusterRequestData()
      .setIncludeClusterAuthorizedOperations(true)).build()

    val request = buildRequest(describeClusterRequest, plaintextListener)
    val capturedResponse = expectNoThrottling(request)

    EasyMock.replay(clientRequestQuotaManager, requestChannel)
    createKafkaApis().handleDescribeCluster(request)

    val describeClusterResponse = capturedResponse.getValue.asInstanceOf[DescribeClusterResponse]

    assertEquals(metadataCache.getControllerId.get, describeClusterResponse.data.controllerId)
    assertEquals(clusterId, describeClusterResponse.data.clusterId)
    assertEquals(8096, describeClusterResponse.data.clusterAuthorizedOperations)
    assertEquals(metadataCache.getAliveBrokerNodes(plaintextListener).toSet,
      describeClusterResponse.nodes.asScala.values.toSet)
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
      0, 0, Seq.empty[UpdateMetadataPartitionState].asJava, brokers.asJava, Collections.emptyMap()).build()
    MetadataCacheTest.updateCache(metadataCache, updateMetadataRequest)
    (plaintextListener, anotherListener)
  }

  private def sendMetadataRequestWithInconsistentListeners(requestListener: ListenerName): MetadataResponse = {
    val metadataRequest = MetadataRequest.Builder.allTopics.build()
    val requestChannelRequest = buildRequest(metadataRequest, requestListener)
    val capturedResponse = expectNoThrottling(requestChannelRequest)
    EasyMock.replay(clientRequestQuotaManager, requestChannel)

    createKafkaApis().handleTopicMetadataRequest(requestChannelRequest)

    capturedResponse.getValue.asInstanceOf[MetadataResponse]
  }

  private def testConsumerListOffsetLatest(isolationLevel: IsolationLevel): Unit = {
    val tp = new TopicPartition("foo", 0)
    val latestOffset = 15L
    val currentLeaderEpoch = Optional.empty[Integer]()

    EasyMock.expect(replicaManager.fetchOffsetForTimestamp(
      EasyMock.eq(tp),
      EasyMock.eq(ListOffsetsRequest.LATEST_TIMESTAMP),
      EasyMock.eq(Some(isolationLevel)),
      EasyMock.eq(currentLeaderEpoch),
      fetchOnlyFromLeader = EasyMock.eq(true))
    ).andReturn(Some(new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, latestOffset, currentLeaderEpoch)))

    val targetTimes = List(new ListOffsetsTopic()
      .setName(tp.topic)
      .setPartitions(List(new ListOffsetsPartition()
        .setPartitionIndex(tp.partition)
        .setTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP)).asJava)).asJava
    val listOffsetRequest = ListOffsetsRequest.Builder.forConsumer(true, isolationLevel, false)
      .setTargetTimes(targetTimes).build()
    val request = buildRequest(listOffsetRequest)
    val capturedResponse = expectNoThrottling(request)

    EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel)
    createKafkaApis().handleListOffsetRequest(request)

    val response = capturedResponse.getValue.asInstanceOf[ListOffsetsResponse]
    val partitionDataOptional = response.topics.asScala.find(_.name == tp.topic).get
      .partitions.asScala.find(_.partitionIndex == tp.partition)
    assertTrue(partitionDataOptional.isDefined)

    val partitionData = partitionDataOptional.get
    assertEquals(Errors.NONE.code, partitionData.errorCode)
    assertEquals(latestOffset, partitionData.offset)
    assertEquals(ListOffsetsResponse.UNKNOWN_TIMESTAMP, partitionData.timestamp)
  }

  private def createWriteTxnMarkersRequest(partitions: util.List[TopicPartition]) = {
    val writeTxnMarkersRequest = new WriteTxnMarkersRequest.Builder(ApiKeys.WRITE_TXN_MARKERS.latestVersion(),
      asList(new TxnMarkerEntry(1, 1.toShort, 0, TransactionResult.COMMIT, partitions))).build()
    (writeTxnMarkersRequest, buildRequest(writeTxnMarkersRequest))
  }

  private def buildRequest(request: AbstractRequest,
                           listenerName: ListenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
                           fromPrivilegedListener: Boolean = false,
                           requestHeader: Option[RequestHeader] = None): RequestChannel.Request = {
    val buffer = request.serializeWithHeader(
      requestHeader.getOrElse(new RequestHeader(request.apiKey, request.version, clientId, 0)))

    // read the header from the buffer first so that the body can be read next from the Request constructor
    val header = RequestHeader.parse(buffer)
    val context = new RequestContext(header, "1", InetAddress.getLocalHost, KafkaPrincipal.ANONYMOUS,
      listenerName, SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY, fromPrivilegedListener,
      Optional.of(kafkaPrincipalSerde))
    new RequestChannel.Request(processor = 1, context = context, startTimeNanos = 0, MemoryPool.NONE, buffer,
      requestChannelMetrics, envelope = None)
  }

  private def expectNoThrottling(request: RequestChannel.Request): Capture[AbstractResponse] = {
    EasyMock.expect(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(EasyMock.anyObject[RequestChannel.Request](),
      EasyMock.anyObject[Long])).andReturn(0)

    EasyMock.expect(clientRequestQuotaManager.throttle(
      EasyMock.eq(request),
      EasyMock.anyObject[ThrottleCallback](),
      EasyMock.eq(0)))

    val capturedResponse = EasyMock.newCapture[AbstractResponse]()
    EasyMock.expect(requestChannel.sendResponse(
      EasyMock.eq(request),
      EasyMock.capture(capturedResponse),
      EasyMock.anyObject()
    ))

    capturedResponse
  }

  private def createBasicMetadataRequest(topic: String,
                                         numPartitions: Int,
                                         brokerEpoch: Long,
                                         numBrokers: Int,
                                         topicId: Uuid = Uuid.ZERO_UUID): UpdateMetadataRequest = {
    val replicas = List(0.asInstanceOf[Integer]).asJava

    def createPartitionState(partition: Int) = new UpdateMetadataPartitionState()
      .setTopicName(topic)
      .setPartitionIndex(partition)
      .setControllerEpoch(1)
      .setLeader(0)
      .setLeaderEpoch(1)
      .setReplicas(replicas)
      .setZkVersion(0)
      .setIsr(replicas)

    val plaintextListener = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val partitionStates = (0 until numPartitions).map(createPartitionState)
    val liveBrokers = (0 until numBrokers).map(
      brokerId => createMetadataBroker(brokerId, plaintextListener))
    new UpdateMetadataRequest.Builder(ApiKeys.UPDATE_METADATA.latestVersion, 0,
      0, brokerEpoch, partitionStates.asJava, liveBrokers.asJava, Collections.singletonMap(topic, topicId)).build()
  }

  private def setupBasicMetadataCache(topic: String, numPartitions: Int, numBrokers: Int, topicId: Uuid): Unit = {
    val updateMetadataRequest = createBasicMetadataRequest(topic, numPartitions, 0, numBrokers, topicId)
    MetadataCacheTest.updateCache(metadataCache, updateMetadataRequest)
  }

  private def addTopicToMetadataCache(topic: String, numPartitions: Int, numBrokers: Int = 1, topicId: Uuid = Uuid.ZERO_UUID): Unit = {
    val updateMetadataRequest = createBasicMetadataRequest(topic, numPartitions, 0, numBrokers)
    MetadataCacheTest.updateCache(metadataCache, updateMetadataRequest)
  }

  private def createMetadataBroker(brokerId: Int,
                                   listener: ListenerName): UpdateMetadataBroker = {
    new UpdateMetadataBroker()
      .setId(brokerId)
      .setRack("rack")
      .setEndpoints(Seq(new UpdateMetadataEndpoint()
        .setHost("broker" + brokerId)
        .setPort(9092)
        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
        .setListener(listener.value)).asJava)
  }

  @Test
  def testAlterReplicaLogDirs(): Unit = {
    val data = new AlterReplicaLogDirsRequestData()
    val dir = new AlterReplicaLogDirsRequestData.AlterReplicaLogDir()
      .setPath("/foo")
    dir.topics().add(new AlterReplicaLogDirsRequestData.AlterReplicaLogDirTopic().setName("t0").setPartitions(asList(0, 1, 2)))
    data.dirs().add(dir)
    val alterReplicaLogDirsRequest = new AlterReplicaLogDirsRequest.Builder(
      data
    ).build()
    val request = buildRequest(alterReplicaLogDirsRequest)

    EasyMock.reset(replicaManager, clientRequestQuotaManager, requestChannel)

    val capturedResponse = expectNoThrottling(request)
    val t0p0 = new TopicPartition("t0", 0)
    val t0p1 = new TopicPartition("t0", 1)
    val t0p2 = new TopicPartition("t0", 2)
    val partitionResults = Map(
      t0p0 -> Errors.NONE,
      t0p1 -> Errors.LOG_DIR_NOT_FOUND,
      t0p2 -> Errors.INVALID_TOPIC_EXCEPTION)
    EasyMock.expect(replicaManager.alterReplicaLogDirs(EasyMock.eq(Map(
      t0p0 -> "/foo",
      t0p1 -> "/foo",
      t0p2 -> "/foo"))))
    .andReturn(partitionResults)
    EasyMock.replay(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel)

    createKafkaApis().handleAlterReplicaLogDirsRequest(request)

    val response = capturedResponse.getValue.asInstanceOf[AlterReplicaLogDirsResponse]
    assertEquals(partitionResults, response.data.results.asScala.flatMap { tr =>
      tr.partitions().asScala.map { pr =>
        new TopicPartition(tr.topicName, pr.partitionIndex) -> Errors.forCode(pr.errorCode)
      }
    }.toMap)
    assertEquals(Map(Errors.NONE -> 1,
      Errors.LOG_DIR_NOT_FOUND -> 1,
      Errors.INVALID_TOPIC_EXCEPTION -> 1).asJava, response.errorCounts)
  }

  @Test
  def testSizeOfThrottledPartitions(): Unit = {
    val topicNames = new util.HashMap[Uuid, String]
    val topicIds = new util.HashMap[String, Uuid]()
    def fetchResponse(data: Map[TopicPartition, String]): FetchResponse = {
      val responseData = new util.LinkedHashMap[TopicPartition, FetchResponseData.PartitionData](
        data.map { case (tp, raw) =>
          tp -> new FetchResponseData.PartitionData()
            .setPartitionIndex(tp.partition)
            .setHighWatermark(105)
            .setLastStableOffset(105)
            .setLogStartOffset(0)
            .setRecords(MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord(100, raw.getBytes(StandardCharsets.UTF_8))))
      }.toMap.asJava)

      data.foreach{case (tp, _) =>
        val id = Uuid.randomUuid()
        topicIds.put(tp.topic(), id)
        topicNames.put(id, tp.topic())
      }
      FetchResponse.of(Errors.NONE, 100, 100, responseData, topicIds)
    }

    val throttledPartition = new TopicPartition("throttledData", 0)
    val throttledData = Map(throttledPartition -> "throttledData")
    val expectedSize = FetchResponse.sizeOf(FetchResponseData.HIGHEST_SUPPORTED_VERSION,
      fetchResponse(throttledData).responseData(topicNames, FetchResponseData.HIGHEST_SUPPORTED_VERSION).entrySet.iterator, topicIds)

    val response = fetchResponse(throttledData ++ Map(new TopicPartition("nonThrottledData", 0) -> "nonThrottledData"))

    val quota = Mockito.mock(classOf[ReplicationQuotaManager])
    Mockito.when(quota.isThrottled(ArgumentMatchers.any(classOf[TopicPartition])))
      .thenAnswer(invocation => throttledPartition == invocation.getArgument(0).asInstanceOf[TopicPartition])

    assertEquals(expectedSize, KafkaApis.sizeOfThrottledPartitions(FetchResponseData.HIGHEST_SUPPORTED_VERSION, response, quota, topicIds))
  }

  @Test
  def testDescribeProducers(): Unit = {
    val tp1 = new TopicPartition("foo", 0)
    val tp2 = new TopicPartition("bar", 3)
    val tp3 = new TopicPartition("baz", 1)
    val tp4 = new TopicPartition("invalid;topic", 1)

    val authorizer: Authorizer = EasyMock.niceMock(classOf[Authorizer])
    val data = new DescribeProducersRequestData().setTopics(List(
      new DescribeProducersRequestData.TopicRequest()
        .setName(tp1.topic)
        .setPartitionIndexes(List(Int.box(tp1.partition)).asJava),
      new DescribeProducersRequestData.TopicRequest()
        .setName(tp2.topic)
        .setPartitionIndexes(List(Int.box(tp2.partition)).asJava),
      new DescribeProducersRequestData.TopicRequest()
        .setName(tp3.topic)
        .setPartitionIndexes(List(Int.box(tp3.partition)).asJava),
      new DescribeProducersRequestData.TopicRequest()
        .setName(tp4.topic)
        .setPartitionIndexes(List(Int.box(tp4.partition)).asJava)
    ).asJava)

    def buildExpectedActions(topic: String): util.List[Action] = {
      val pattern = new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL)
      val action = new Action(AclOperation.READ, pattern, 1, true, true)
      Collections.singletonList(action)
    }

    // Topic `foo` is authorized and present in the metadata
    addTopicToMetadataCache(tp1.topic, 4) // We will only access the first topic
    EasyMock.expect(authorizer.authorize(anyObject[RequestContext], EasyMock.eq(buildExpectedActions(tp1.topic))))
      .andReturn(Seq(AuthorizationResult.ALLOWED).asJava)
      .once()

    // Topic `bar` is not authorized
    EasyMock.expect(authorizer.authorize(anyObject[RequestContext], EasyMock.eq(buildExpectedActions(tp2.topic))))
      .andReturn(Seq(AuthorizationResult.DENIED).asJava)
      .once()

    // Topic `baz` is authorized, but not present in the metadata
    EasyMock.expect(authorizer.authorize(anyObject[RequestContext], EasyMock.eq(buildExpectedActions(tp3.topic))))
      .andReturn(Seq(AuthorizationResult.ALLOWED).asJava)
      .once()

    EasyMock.expect(replicaManager.activeProducerState(tp1))
      .andReturn(new DescribeProducersResponseData.PartitionResponse()
        .setErrorCode(Errors.NONE.code)
        .setPartitionIndex(tp1.partition)
        .setActiveProducers(List(
          new DescribeProducersResponseData.ProducerState()
            .setProducerId(12345L)
            .setProducerEpoch(15)
            .setLastSequence(100)
            .setLastTimestamp(time.milliseconds())
            .setCurrentTxnStartOffset(-1)
            .setCoordinatorEpoch(200)
        ).asJava))

    val describeProducersRequest = new DescribeProducersRequest.Builder(data).build()
    val request = buildRequest(describeProducersRequest)
    val capturedResponse = expectNoThrottling(request)

    EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator, authorizer)
    createKafkaApis(authorizer = Some(authorizer)).handleDescribeProducersRequest(request)

    val response = capturedResponse.getValue.asInstanceOf[DescribeProducersResponse]
    assertEquals(Set("foo", "bar", "baz", "invalid;topic"), response.data.topics.asScala.map(_.name).toSet)

    def assertPartitionError(
      topicPartition: TopicPartition,
      error: Errors
    ): DescribeProducersResponseData.PartitionResponse = {
      val topicData = response.data.topics.asScala.find(_.name == topicPartition.topic).get
      val partitionData = topicData.partitions.asScala.find(_.partitionIndex == topicPartition.partition).get
      assertEquals(error, Errors.forCode(partitionData.errorCode))
      partitionData
    }

    val fooPartition = assertPartitionError(tp1, Errors.NONE)
    assertEquals(Errors.NONE, Errors.forCode(fooPartition.errorCode))
    assertEquals(1, fooPartition.activeProducers.size)
    val fooProducer = fooPartition.activeProducers.get(0)
    assertEquals(12345L, fooProducer.producerId)
    assertEquals(15, fooProducer.producerEpoch)
    assertEquals(100, fooProducer.lastSequence)
    assertEquals(time.milliseconds(), fooProducer.lastTimestamp)
    assertEquals(-1, fooProducer.currentTxnStartOffset)
    assertEquals(200, fooProducer.coordinatorEpoch)

    assertPartitionError(tp2, Errors.TOPIC_AUTHORIZATION_FAILED)
    assertPartitionError(tp3, Errors.UNKNOWN_TOPIC_OR_PARTITION)
    assertPartitionError(tp4, Errors.INVALID_TOPIC_EXCEPTION)
  }

  @Test
  def testDescribeTransactions(): Unit = {
    val authorizer: Authorizer = EasyMock.niceMock(classOf[Authorizer])
    val data = new DescribeTransactionsRequestData()
      .setTransactionalIds(List("foo", "bar").asJava)
    val describeTransactionsRequest = new DescribeTransactionsRequest.Builder(data).build()
    val request = buildRequest(describeTransactionsRequest)
    val capturedResponse = expectNoThrottling(request)

    def buildExpectedActions(transactionalId: String): util.List[Action] = {
      val pattern = new ResourcePattern(ResourceType.TRANSACTIONAL_ID, transactionalId, PatternType.LITERAL)
      val action = new Action(AclOperation.DESCRIBE, pattern, 1, true, true)
      Collections.singletonList(action)
    }

    EasyMock.expect(txnCoordinator.handleDescribeTransactions("foo"))
      .andReturn(new DescribeTransactionsResponseData.TransactionState()
        .setErrorCode(Errors.NONE.code)
        .setTransactionalId("foo")
        .setProducerId(12345L)
        .setProducerEpoch(15)
        .setTransactionStartTimeMs(time.milliseconds())
        .setTransactionState("CompleteCommit")
        .setTransactionTimeoutMs(10000))

    EasyMock.expect(authorizer.authorize(anyObject[RequestContext], EasyMock.eq(buildExpectedActions("foo"))))
      .andReturn(Seq(AuthorizationResult.ALLOWED).asJava)
      .once()

    EasyMock.expect(authorizer.authorize(anyObject[RequestContext], EasyMock.eq(buildExpectedActions("bar"))))
      .andReturn(Seq(AuthorizationResult.DENIED).asJava)
      .once()

    EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator, authorizer)
    createKafkaApis(authorizer = Some(authorizer)).handleDescribeTransactionsRequest(request)

    val response = capturedResponse.getValue.asInstanceOf[DescribeTransactionsResponse]
    assertEquals(2, response.data.transactionStates.size)

    val fooState = response.data.transactionStates.asScala.find(_.transactionalId == "foo").get
    assertEquals(Errors.NONE.code, fooState.errorCode)
    assertEquals(12345L, fooState.producerId)
    assertEquals(15, fooState.producerEpoch)
    assertEquals(time.milliseconds(), fooState.transactionStartTimeMs)
    assertEquals("CompleteCommit", fooState.transactionState)
    assertEquals(10000, fooState.transactionTimeoutMs)
    assertEquals(List.empty, fooState.topics.asScala.toList)

    val barState = response.data.transactionStates.asScala.find(_.transactionalId == "bar").get
    assertEquals(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code, barState.errorCode)
  }

  @Test
  def testDescribeTransactionsFiltersUnauthorizedTopics(): Unit = {
    val authorizer: Authorizer = EasyMock.niceMock(classOf[Authorizer])
    val transactionalId = "foo"
    val data = new DescribeTransactionsRequestData()
      .setTransactionalIds(List(transactionalId).asJava)
    val describeTransactionsRequest = new DescribeTransactionsRequest.Builder(data).build()
    val request = buildRequest(describeTransactionsRequest)
    val capturedResponse = expectNoThrottling(request)

    def expectDescribe(
      resourceType: ResourceType,
      transactionalId: String,
      result: AuthorizationResult
    ): Unit = {
      val pattern = new ResourcePattern(resourceType, transactionalId, PatternType.LITERAL)
      val action = new Action(AclOperation.DESCRIBE, pattern, 1, true, true)
      val actions = Collections.singletonList(action)

      EasyMock.expect(authorizer.authorize(anyObject[RequestContext], EasyMock.eq(actions)))
        .andReturn(Seq(result).asJava)
        .once()
    }

    // Principal is authorized to one of the two topics. The second topic should be
    // filtered from the result.
    expectDescribe(ResourceType.TRANSACTIONAL_ID, transactionalId, AuthorizationResult.ALLOWED)
    expectDescribe(ResourceType.TOPIC, "foo", AuthorizationResult.ALLOWED)
    expectDescribe(ResourceType.TOPIC, "bar", AuthorizationResult.DENIED)

    def mkTopicData(
      topic: String,
      partitions: Seq[Int]
    ): DescribeTransactionsResponseData.TopicData = {
      new DescribeTransactionsResponseData.TopicData()
        .setTopic(topic)
        .setPartitions(partitions.map(Int.box).asJava)
    }

    val describeTransactionsResponse = new DescribeTransactionsResponseData.TransactionState()
      .setErrorCode(Errors.NONE.code)
      .setTransactionalId(transactionalId)
      .setProducerId(12345L)
      .setProducerEpoch(15)
      .setTransactionStartTimeMs(time.milliseconds())
      .setTransactionState("Ongoing")
      .setTransactionTimeoutMs(10000)

    describeTransactionsResponse.topics.add(mkTopicData(topic = "foo", Seq(1, 2)))
    describeTransactionsResponse.topics.add(mkTopicData(topic = "bar", Seq(3, 4)))

    EasyMock.expect(txnCoordinator.handleDescribeTransactions("foo"))
      .andReturn(describeTransactionsResponse)

    EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator, authorizer)
    createKafkaApis(authorizer = Some(authorizer)).handleDescribeTransactionsRequest(request)

    val response = capturedResponse.getValue.asInstanceOf[DescribeTransactionsResponse]
    assertEquals(1, response.data.transactionStates.size)

    val fooState = response.data.transactionStates.asScala.find(_.transactionalId == "foo").get
    assertEquals(Errors.NONE.code, fooState.errorCode)
    assertEquals(12345L, fooState.producerId)
    assertEquals(15, fooState.producerEpoch)
    assertEquals(time.milliseconds(), fooState.transactionStartTimeMs)
    assertEquals("Ongoing", fooState.transactionState)
    assertEquals(10000, fooState.transactionTimeoutMs)
    assertEquals(List(mkTopicData(topic = "foo", Seq(1, 2))), fooState.topics.asScala.toList)
  }

  @Test
  def testListTransactionsErrorResponse(): Unit = {
    val data = new ListTransactionsRequestData()
    val listTransactionsRequest = new ListTransactionsRequest.Builder(data).build()
    val request = buildRequest(listTransactionsRequest)
    val capturedResponse = expectNoThrottling(request)

    EasyMock.expect(txnCoordinator.handleListTransactions(Set.empty[Long], Set.empty[String]))
      .andReturn(new ListTransactionsResponseData()
        .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code))

    EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)
    createKafkaApis().handleListTransactionsRequest(request)

    val response = capturedResponse.getValue.asInstanceOf[ListTransactionsResponse]
    assertEquals(0, response.data.transactionStates.size)
    assertEquals(Errors.COORDINATOR_LOAD_IN_PROGRESS, Errors.forCode(response.data.errorCode))
  }

  @Test
  def testListTransactionsAuthorization(): Unit = {
    val authorizer: Authorizer = EasyMock.niceMock(classOf[Authorizer])
    val data = new ListTransactionsRequestData()
    val listTransactionsRequest = new ListTransactionsRequest.Builder(data).build()
    val request = buildRequest(listTransactionsRequest)
    val capturedResponse = expectNoThrottling(request)

    val transactionStates = new util.ArrayList[ListTransactionsResponseData.TransactionState]()
    transactionStates.add(new ListTransactionsResponseData.TransactionState()
      .setTransactionalId("foo")
      .setProducerId(12345L)
      .setTransactionState("Ongoing"))
    transactionStates.add(new ListTransactionsResponseData.TransactionState()
      .setTransactionalId("bar")
      .setProducerId(98765)
      .setTransactionState("PrepareAbort"))

    EasyMock.expect(txnCoordinator.handleListTransactions(Set.empty[Long], Set.empty[String]))
      .andReturn(new ListTransactionsResponseData()
        .setErrorCode(Errors.NONE.code)
        .setTransactionStates(transactionStates))

    def buildExpectedActions(transactionalId: String): util.List[Action] = {
      val pattern = new ResourcePattern(ResourceType.TRANSACTIONAL_ID, transactionalId, PatternType.LITERAL)
      val action = new Action(AclOperation.DESCRIBE, pattern, 1, true, true)
      Collections.singletonList(action)
    }

    EasyMock.expect(authorizer.authorize(anyObject[RequestContext], EasyMock.eq(buildExpectedActions("foo"))))
      .andReturn(Seq(AuthorizationResult.ALLOWED).asJava)
      .once()

    EasyMock.expect(authorizer.authorize(anyObject[RequestContext], EasyMock.eq(buildExpectedActions("bar"))))
      .andReturn(Seq(AuthorizationResult.DENIED).asJava)
      .once()

    EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator, authorizer)
    createKafkaApis(authorizer = Some(authorizer)).handleListTransactionsRequest(request)

    val response = capturedResponse.getValue.asInstanceOf[ListTransactionsResponse]
    assertEquals(1, response.data.transactionStates.size())
    val transactionState = response.data.transactionStates.get(0)
    assertEquals("foo", transactionState.transactionalId)
    assertEquals(12345L, transactionState.producerId)
    assertEquals("Ongoing", transactionState.transactionState)
  }

  @Test
  def testDeleteTopicsByIdAuthorization(): Unit = {
    val authorizer: Authorizer = EasyMock.niceMock(classOf[Authorizer])
    val controllerContext: ControllerContext = EasyMock.mock(classOf[ControllerContext])

    EasyMock.expect(clientControllerQuotaManager.newQuotaFor(
      EasyMock.anyObject(classOf[RequestChannel.Request]),
      EasyMock.anyShort()
    )).andReturn(UnboundedControllerMutationQuota)
    EasyMock.expect(controller.isActive).andReturn(true)
    EasyMock.expect(controller.controllerContext).andStubReturn(controllerContext)

    // Try to delete three topics:
    // 1. One without describe permission
    // 2. One without delete permission
    // 3. One which is authorized, but doesn't exist

    expectTopicAuthorization(authorizer, AclOperation.DESCRIBE, Map(
      "foo" -> AuthorizationResult.DENIED,
      "bar" -> AuthorizationResult.ALLOWED
    ))

    expectTopicAuthorization(authorizer, AclOperation.DELETE, Map(
      "foo" -> AuthorizationResult.DENIED,
      "bar" -> AuthorizationResult.DENIED
    ))

    val topicIdsMap = Map(
      Uuid.randomUuid() -> Some("foo"),
      Uuid.randomUuid() -> Some("bar"),
      Uuid.randomUuid() -> None
    )

    topicIdsMap.foreach { case (topicId, topicNameOpt) =>
      EasyMock.expect(controllerContext.topicName(topicId)).andReturn(topicNameOpt)
    }

    val topicDatas = topicIdsMap.keys.map { topicId =>
      new DeleteTopicsRequestData.DeleteTopicState().setTopicId(topicId)
    }.toList
    val deleteRequest = new DeleteTopicsRequest.Builder(new DeleteTopicsRequestData()
      .setTopics(topicDatas.asJava))
      .build(ApiKeys.DELETE_TOPICS.latestVersion)

    val request = buildRequest(deleteRequest)
    val capturedResponse = expectNoThrottling(request)

    EasyMock.replay(replicaManager, clientRequestQuotaManager, clientControllerQuotaManager,
      requestChannel, txnCoordinator, controller, controllerContext, authorizer)
    createKafkaApis(authorizer = Some(authorizer)).handleDeleteTopicsRequest(request)

    val deleteResponse = capturedResponse.getValue.asInstanceOf[DeleteTopicsResponse]

    topicIdsMap.foreach { case (topicId, nameOpt) =>
      val response = deleteResponse.data.responses.asScala.find(_.topicId == topicId).get
      nameOpt match {
        case Some("foo") =>
          assertNull(response.name)
          assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED, Errors.forCode(response.errorCode))
        case Some("bar") =>
          assertEquals("bar", response.name)
          assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED, Errors.forCode(response.errorCode))
        case None =>
          assertNull(response.name)
          assertEquals(Errors.UNKNOWN_TOPIC_ID, Errors.forCode(response.errorCode))
        case _ =>
          fail("Unexpected topic id/name mapping")
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testDeleteTopicsByNameAuthorization(usePrimitiveTopicNameArray: Boolean): Unit = {
    val authorizer: Authorizer = EasyMock.niceMock(classOf[Authorizer])

    EasyMock.expect(clientControllerQuotaManager.newQuotaFor(
      EasyMock.anyObject(classOf[RequestChannel.Request]),
      EasyMock.anyShort()
    )).andReturn(UnboundedControllerMutationQuota)
    EasyMock.expect(controller.isActive).andReturn(true)

    // Try to delete three topics:
    // 1. One without describe permission
    // 2. One without delete permission
    // 3. One which is authorized, but doesn't exist

    expectTopicAuthorization(authorizer, AclOperation.DESCRIBE, Map(
      "foo" -> AuthorizationResult.DENIED,
      "bar" -> AuthorizationResult.ALLOWED,
      "baz" -> AuthorizationResult.ALLOWED
    ))

    expectTopicAuthorization(authorizer, AclOperation.DELETE, Map(
      "foo" -> AuthorizationResult.DENIED,
      "bar" -> AuthorizationResult.DENIED,
      "baz" -> AuthorizationResult.ALLOWED
    ))

    val deleteRequest = if (usePrimitiveTopicNameArray) {
      new DeleteTopicsRequest.Builder(new DeleteTopicsRequestData()
        .setTopicNames(List("foo", "bar", "baz").asJava))
        .build(5.toShort)
    } else {
      val topicDatas = List(
        new DeleteTopicsRequestData.DeleteTopicState().setName("foo"),
        new DeleteTopicsRequestData.DeleteTopicState().setName("bar"),
        new DeleteTopicsRequestData.DeleteTopicState().setName("baz")
      )
      new DeleteTopicsRequest.Builder(new DeleteTopicsRequestData()
        .setTopics(topicDatas.asJava))
        .build(ApiKeys.DELETE_TOPICS.latestVersion)
    }

    val request = buildRequest(deleteRequest)
    val capturedResponse = expectNoThrottling(request)

    EasyMock.replay(replicaManager, clientRequestQuotaManager, clientControllerQuotaManager,
      requestChannel, txnCoordinator, controller, authorizer)
    createKafkaApis(authorizer = Some(authorizer)).handleDeleteTopicsRequest(request)

    val deleteResponse = capturedResponse.getValue.asInstanceOf[DeleteTopicsResponse]

    def lookupErrorCode(topic: String): Option[Errors] = {
      Option(deleteResponse.data.responses().find(topic))
        .map(result => Errors.forCode(result.errorCode))
    }

    assertEquals(Some(Errors.TOPIC_AUTHORIZATION_FAILED), lookupErrorCode("foo"))
    assertEquals(Some(Errors.TOPIC_AUTHORIZATION_FAILED), lookupErrorCode("bar"))
    assertEquals(Some(Errors.UNKNOWN_TOPIC_OR_PARTITION), lookupErrorCode("baz"))
  }

  def expectTopicAuthorization(
    authorizer: Authorizer,
    aclOperation: AclOperation,
    topicResults: Map[String, AuthorizationResult]
  ): Unit = {
    val expectedActions = topicResults.keys.map { topic =>
      val pattern = new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL)
      topic -> new Action(aclOperation, pattern, 1, true, true)
    }.toMap

    val actionsCapture: Capture[util.List[Action]] = EasyMock.newCapture()
    EasyMock.expect(authorizer.authorize(anyObject[RequestContext], EasyMock.capture(actionsCapture)))
      .andAnswer(() => {
        actionsCapture.getValue.asScala.map { action =>
          val topic = action.resourcePattern.name
          assertEquals(expectedActions(topic), action)
          topicResults(topic)
        }.asJava
      })
      .once()
  }

  private def createMockRequest(): RequestChannel.Request = {
    val request: RequestChannel.Request = EasyMock.createNiceMock(classOf[RequestChannel.Request])
    val requestHeader: RequestHeader = EasyMock.createNiceMock(classOf[RequestHeader])
    expect(request.header).andReturn(requestHeader).anyTimes()
    expect(requestHeader.apiKey()).andReturn(ApiKeys.values().head).anyTimes()
    EasyMock.replay(request, requestHeader)
    request
  }

  private def verifyShouldNeverHandle(handler: RequestChannel.Request => Unit): Unit = {
    val request = createMockRequest()
    val e = assertThrows(classOf[UnsupportedVersionException], () => handler(request))
    assertEquals(KafkaApis.shouldNeverReceive(request).getMessage, e.getMessage)
  }

  private def verifyShouldAlwaysForward(handler: RequestChannel.Request => Unit): Unit = {
    val request = createMockRequest()
    val e = assertThrows(classOf[UnsupportedVersionException], () => handler(request))
    assertEquals(KafkaApis.shouldAlwaysForward(request).getMessage, e.getMessage)
  }

  @Test
  def testRaftShouldNeverHandleLeaderAndIsrRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldNeverHandle(createKafkaApis(raftSupport = true).handleLeaderAndIsrRequest)
  }

  @Test
  def testRaftShouldNeverHandleStopReplicaRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldNeverHandle(createKafkaApis(raftSupport = true).handleStopReplicaRequest)
  }

  @Test
  def testRaftShouldNeverHandleUpdateMetadataRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldNeverHandle(createKafkaApis(raftSupport = true).handleUpdateMetadataRequest(_, RequestLocal.withThreadConfinedCaching))
  }

  @Test
  def testRaftShouldNeverHandleControlledShutdownRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldNeverHandle(createKafkaApis(raftSupport = true).handleControlledShutdownRequest)
  }

  @Test
  def testRaftShouldNeverHandleAlterIsrRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldNeverHandle(createKafkaApis(raftSupport = true).handleAlterIsrRequest)
  }

  @Test
  def testRaftShouldNeverHandleEnvelope(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldNeverHandle(createKafkaApis(raftSupport = true).handleEnvelope(_, RequestLocal.withThreadConfinedCaching))
  }

  @Test
  def testRaftShouldAlwaysForwardCreateTopicsRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldAlwaysForward(createKafkaApis(raftSupport = true).handleCreateTopicsRequest)
  }

  @Test
  def testRaftShouldAlwaysForwardCreatePartitionsRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldAlwaysForward(createKafkaApis(raftSupport = true).handleCreatePartitionsRequest)
  }

  @Test
  def testRaftShouldAlwaysForwardDeleteTopicsRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldAlwaysForward(createKafkaApis(raftSupport = true).handleDeleteTopicsRequest)
  }

  @Test
  def testRaftShouldAlwaysForwardCreateAcls(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldAlwaysForward(createKafkaApis(raftSupport = true).handleCreateAcls)
  }

  @Test
  def testRaftShouldAlwaysForwardDeleteAcls(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldAlwaysForward(createKafkaApis(raftSupport = true).handleDeleteAcls)
  }

  @Test
  def testRaftShouldAlwaysForwardAlterPartitionReassignmentsRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldAlwaysForward(createKafkaApis(raftSupport = true).handleAlterPartitionReassignmentsRequest)
  }

  @Test
  def testRaftShouldAlwaysForwardCreateTokenRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldAlwaysForward(createKafkaApis(raftSupport = true).handleCreateTokenRequest)
  }

  @Test
  def testRaftShouldAlwaysForwardRenewTokenRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldAlwaysForward(createKafkaApis(raftSupport = true).handleRenewTokenRequest)
  }

  @Test
  def testRaftShouldAlwaysForwardExpireTokenRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldAlwaysForward(createKafkaApis(raftSupport = true).handleExpireTokenRequest)
  }

  @Test
  def testRaftShouldAlwaysForwardAlterClientQuotasRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldAlwaysForward(createKafkaApis(raftSupport = true).handleAlterClientQuotasRequest)
  }

  @Test
  def testRaftShouldAlwaysForwardAlterUserScramCredentialsRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldAlwaysForward(createKafkaApis(raftSupport = true).handleAlterUserScramCredentialsRequest)
  }

  @Test
  def testRaftShouldAlwaysForwardUpdateFeatures(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldAlwaysForward(createKafkaApis(raftSupport = true).handleUpdateFeatures)
  }

  @Test
  def testRaftShouldAlwaysForwardListPartitionReassignments(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldAlwaysForward(createKafkaApis(raftSupport = true).handleListPartitionReassignmentsRequest)
  }
}
