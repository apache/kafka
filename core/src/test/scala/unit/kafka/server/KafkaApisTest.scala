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
import kafka.controller.KafkaController
import kafka.coordinator.group.GroupCoordinatorConcurrencyTest.{JoinGroupCallback, SyncGroupCallback}
import kafka.coordinator.group._
import kafka.coordinator.transaction.{InitProducerIdResult, TransactionCoordinator}
import kafka.log.AppendOrigin
import kafka.network.RequestChannel
import kafka.network.RequestChannel.{CloseConnectionResponse, SendResponse}
import kafka.server.QuotaFactory.QuotaManagers
import kafka.utils.{MockTime, TestUtils}
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.NodeApiVersions
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.clients.admin.{AlterConfigOp, ConfigEntry}
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.internals.{KafkaFutureImpl, Topic}
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.CreateTopicsRequestData.{CreatableTopic, CreatableTopicCollection}
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
import org.mockito.{ArgumentMatchers, Mockito}

import scala.annotation.nowarn
import scala.collection.{Map, Seq, mutable}
import scala.jdk.CollectionConverters._

class KafkaApisTest {

  private val requestChannel: RequestChannel = EasyMock.createNiceMock(classOf[RequestChannel])
  private val requestChannelMetrics: RequestChannel.Metrics = EasyMock.createNiceMock(classOf[RequestChannel.Metrics])
  private val replicaManager: ReplicaManager = EasyMock.createNiceMock(classOf[ReplicaManager])
  private val groupCoordinator: GroupCoordinator = EasyMock.createNiceMock(classOf[GroupCoordinator])
  private val adminManager: ZkAdminManager = EasyMock.createNiceMock(classOf[ZkAdminManager])
  private val txnCoordinator: TransactionCoordinator = EasyMock.createNiceMock(classOf[TransactionCoordinator])
  private val controller: KafkaController = EasyMock.createNiceMock(classOf[KafkaController])
  private val forwardingManager: ForwardingManager = EasyMock.createNiceMock(classOf[ForwardingManager])
  private val hostAddress: Array[Byte] = InetAddress.getByName("192.168.1.1").getAddress
  private val kafkaPrincipalSerde = new KafkaPrincipalSerde {
    override def serialize(principal: KafkaPrincipal): Array[Byte] = Utils.utf8(principal.toString)
    override def deserialize(bytes: Array[Byte]): KafkaPrincipal = SecurityUtils.parseKafkaPrincipal(Utils.utf8(bytes))
  }
  private val zkClient: KafkaZkClient = EasyMock.createNiceMock(classOf[KafkaZkClient])
  private val metrics = new Metrics()
  private val brokerId = 1
  private var metadataCache: MetadataCache = new MetadataCache(brokerId)
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
                      enableForwarding: Boolean = false): KafkaApis = {
    val brokerFeatures = BrokerFeatures.createDefault()
    val cache = new FinalizedFeatureCache(brokerFeatures)
    val properties = TestUtils.createBrokerConfig(brokerId, "zk")
    properties.put(KafkaConfig.InterBrokerProtocolVersionProp, interBrokerProtocolVersion.toString)
    properties.put(KafkaConfig.LogMessageFormatVersionProp, interBrokerProtocolVersion.toString)

    val forwardingManagerOpt = if (enableForwarding)
      Some(this.forwardingManager)
    else
      None

    new KafkaApis(requestChannel,
      replicaManager,
      adminManager,
      groupCoordinator,
      txnCoordinator,
      controller,
      forwardingManagerOpt,
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
      null,
      brokerFeatures,
      cache)
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
    EasyMock.expect(adminManager.describeConfigs(anyObject(), EasyMock.eq(true), EasyMock.eq(false)))
        .andReturn(
          List(new DescribeConfigsResponseData.DescribeConfigsResult()
            .setResourceName(configResource.name)
            .setResourceType(configResource.`type`.id)
            .setErrorCode(Errors.NONE.code)
            .setConfigs(Collections.emptyList())))

    EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, authorizer,
      adminManager)

    val request = buildRequest(new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData()
      .setIncludeSynonyms(true)
      .setResources(List(new DescribeConfigsRequestData.DescribeConfigsResource()
        .setResourceName("topic-1")
        .setResourceType(ConfigResource.Type.TOPIC.id)).asJava))
      .build(requestHeader.apiVersion),
      requestHeader = Option(requestHeader))
    createKafkaApis(authorizer = Some(authorizer)).handleDescribeConfigsRequest(request)

    verify(authorizer, adminManager)
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

    val capturedResponse = expectNoThrottling()

    val configResource = new ConfigResource(ConfigResource.Type.TOPIC, resourceName)
    EasyMock.expect(adminManager.alterConfigs(anyObject(), EasyMock.eq(false)))
      .andAnswer(() => {
        Map(configResource -> alterConfigHandler.apply())
      })

    EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, authorizer,
      adminManager, controller)

    val configs = Map(
      configResource -> new AlterConfigsRequest.Config(
        Seq(new AlterConfigsRequest.ConfigEntry("foo", "bar")).asJava))
    val alterConfigsRequest = new AlterConfigsRequest.Builder(configs.asJava, false).build(requestHeader.apiVersion)

    val request = buildRequestWithEnvelope(alterConfigsRequest, fromPrivilegedListener = true)

    createKafkaApis(authorizer = Some(authorizer), enableForwarding = true).handle(request)

    val envelopeRequest = request.body[EnvelopeRequest]
    val response = readResponse(envelopeRequest, capturedResponse)
      .asInstanceOf[EnvelopeResponse]

    assertEquals(Errors.NONE, response.error)

    val innerResponse = AbstractResponse.parseResponse(
      response.responseData(),
      requestHeader
    ).asInstanceOf[AlterConfigsResponse]

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
    val serializedRequestData = RequestTestUtils.serializeRequestWithHeader(requestHeader, leaveGroupRequest)

    resetToStrict(requestChannel)

    EasyMock.expect(controller.isActive).andReturn(true)

    EasyMock.expect(requestChannel.updateErrorMetrics(ApiKeys.ENVELOPE, Map(Errors.INVALID_REQUEST -> 1)))
    val capturedResponse = expectNoThrottling()

    EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, controller)

    val envelopeHeader = new RequestHeader(ApiKeys.ENVELOPE, ApiKeys.ENVELOPE.latestVersion,
      clientId, 0)

    val envelopeRequest = new EnvelopeRequest.Builder(serializedRequestData, new Array[Byte](0), hostAddress)
      .build(envelopeHeader.apiVersion)
    val request = buildRequestWithEnvelope(leaveGroupRequest, fromPrivilegedListener = true)

    createKafkaApis(enableForwarding = true).handle(request)

    val response = readResponse(envelopeRequest, capturedResponse)
      .asInstanceOf[EnvelopeResponse]
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

    val capturedResponse = expectNoThrottling()

    val configResource = new ConfigResource(ConfigResource.Type.TOPIC, resourceName)

    EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, authorizer,
      adminManager, controller)

    val configs = Map(
      configResource -> new AlterConfigsRequest.Config(
        Seq(new AlterConfigsRequest.ConfigEntry("foo", "bar")).asJava))
    val alterConfigsRequest = new AlterConfigsRequest.Builder(configs.asJava, false)
      .build(requestHeader.apiVersion)

    val request = buildRequestWithEnvelope(alterConfigsRequest,
      fromPrivilegedListener = fromPrivilegedListener)
    createKafkaApis(authorizer = Some(authorizer), enableForwarding = true).handle(request)

    if (shouldCloseConnection) {
      assertTrue(capturedResponse.getValue.isInstanceOf[CloseConnectionResponse])
    } else {
      val envelopeRequest = request.body[EnvelopeRequest]
      val response = readResponse(envelopeRequest, capturedResponse)
        .asInstanceOf[EnvelopeResponse]

      assertEquals(expectedError, response.error())

      verify(authorizer, adminManager)
    }
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

    val capturedResponse = expectNoThrottling()

    EasyMock.expect(adminManager.alterConfigs(anyObject(), EasyMock.eq(false)))
      .andReturn(Map(authorizedResource -> ApiError.NONE))

    EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, authorizer,
      adminManager, controller)

    createKafkaApis(authorizer = Some(authorizer)).handleAlterConfigsRequest(request)

    verifyAlterConfigResult(alterConfigsRequest,
      capturedResponse, Map(authorizedTopic -> Errors.NONE,
        unauthorizedTopic -> Errors.TOPIC_AUTHORIZATION_FAILED))

    verify(authorizer, adminManager)
  }

  @Test
  def testAlterConfigsWithForwarding(): Unit = {
    val requestBuilder = new AlterConfigsRequest.Builder(Collections.emptyMap(), false)
    testForwardableAPI(ApiKeys.ALTER_CONFIGS, requestBuilder)
  }

  private def testForwardableAPI(apiKey: ApiKeys, requestBuilder: AbstractRequest.Builder[_ <: AbstractRequest]): Unit = {
    val topicHeader = new RequestHeader(apiKey, apiKey.latestVersion,
      clientId, 0)

    val request = buildRequest(requestBuilder.build(topicHeader.apiVersion))

    EasyMock.expect(controller.isActive).andReturn(false)

    expectNoThrottling()

    EasyMock.expect(forwardingManager.forwardRequest(
      EasyMock.eq(request),
      anyObject[Option[AbstractResponse] => Unit]()
    )).once()

    EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, controller, forwardingManager)

    createKafkaApis(enableForwarding = true).handle(request)

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

  private def verifyAlterConfigResult(alterConfigsRequest: AlterConfigsRequest,
                                      capturedResponse: Capture[RequestChannel.Response],
                                      expectedResults: Map[String, Errors]): Unit = {
    val response = readResponse(alterConfigsRequest, capturedResponse)
      .asInstanceOf[AlterConfigsResponse]
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

    val capturedResponse = expectNoThrottling()

    EasyMock.expect(adminManager.incrementalAlterConfigs(anyObject(), EasyMock.eq(false)))
      .andReturn(Map(authorizedResource -> ApiError.NONE))

    EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, authorizer,
      adminManager, controller)

    createKafkaApis(authorizer = Some(authorizer)).handleIncrementalAlterConfigsRequest(request)

    verifyIncrementalAlterConfigResult(incrementalAlterConfigsRequest,
      capturedResponse, Map(authorizedTopic -> Errors.NONE,
        unauthorizedTopic -> Errors.TOPIC_AUTHORIZATION_FAILED))

    verify(authorizer, adminManager)
  }

  @Test
  def testIncrementalAlterConfigsWithForwarding(): Unit = {
    val requestBuilder = new IncrementalAlterConfigsRequest.Builder(
      new IncrementalAlterConfigsRequestData())
    testForwardableAPI(ApiKeys.INCREMENTAL_ALTER_CONFIGS, requestBuilder)
  }

  private def getIncrementalAlterConfigRequestBuilder(configResources: Seq[ConfigResource]): IncrementalAlterConfigsRequest.Builder = {
    val resourceMap = configResources.map(configResource => {
      configResource -> Set(
        new AlterConfigOp(new ConfigEntry("foo", "bar"),
        OpType.forId(configResource.`type`.id))).asJavaCollection
    }).toMap.asJava

    new IncrementalAlterConfigsRequest.Builder(resourceMap, false)
  }

  private def verifyIncrementalAlterConfigResult(incrementalAlterConfigsRequest: IncrementalAlterConfigsRequest,
                                                 capturedResponse: Capture[RequestChannel.Response],
                                                 expectedResults: Map[String, Errors]): Unit = {
    val response = readResponse(incrementalAlterConfigsRequest, capturedResponse)
      .asInstanceOf[IncrementalAlterConfigsResponse]
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

    val capturedResponse = expectNoThrottling()

    EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, authorizer,
      adminManager, controller)

    createKafkaApis(authorizer = Some(authorizer)).handleAlterClientQuotasRequest(request)

    verifyAlterClientQuotaResult(alterClientQuotasRequest,
      capturedResponse, Map(quotaEntity -> Errors.CLUSTER_AUTHORIZATION_FAILED))

    verify(authorizer, adminManager)
  }

  @Test
  def testAlterClientQuotasWithForwarding(): Unit = {
    val requestBuilder = new AlterClientQuotasRequest.Builder(List.empty.asJava, false)
    testForwardableAPI(ApiKeys.ALTER_CLIENT_QUOTAS, requestBuilder)
  }

  private def verifyAlterClientQuotaResult(alterClientQuotasRequest: AlterClientQuotasRequest,
                                           capturedResponse: Capture[RequestChannel.Response],
                                           expected: Map[ClientQuotaEntity, Errors]): Unit = {
    val response = readResponse(alterClientQuotasRequest, capturedResponse)
      .asInstanceOf[AlterClientQuotasResponse]
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
  def testHandleApiVersionsWithControllerApiVersions(): Unit = {
    val authorizer: Authorizer = EasyMock.niceMock(classOf[Authorizer])

    val requestHeader = new RequestHeader(ApiKeys.API_VERSIONS, ApiKeys.API_VERSIONS.latestVersion, clientId, 0)

    val permittedVersion: Short = 0
    EasyMock.expect(forwardingManager.controllerApiVersions).andReturn(
      Some(NodeApiVersions.create(ApiKeys.ALTER_CONFIGS.id, permittedVersion, permittedVersion)))

    val capturedResponse = expectNoThrottling()

    val apiVersionsRequest = new ApiVersionsRequest.Builder()
      .build(requestHeader.apiVersion)
    val request = buildRequest(apiVersionsRequest,
      fromPrivilegedListener = true, requestHeader = Option(requestHeader))

    EasyMock.replay(replicaManager, clientRequestQuotaManager, forwardingManager,
      requestChannel, authorizer, adminManager, controller)

    createKafkaApis(authorizer = Some(authorizer), enableForwarding = true).handleApiVersionsRequest(request)

    val expectedVersions = new ApiVersionsResponseData.ApiVersion()
      .setApiKey(ApiKeys.ALTER_CONFIGS.id)
      .setMaxVersion(permittedVersion)
      .setMinVersion(permittedVersion)

    val response = readResponse(apiVersionsRequest, capturedResponse)
      .asInstanceOf[ApiVersionsResponse]
    assertEquals(Errors.NONE, Errors.forCode(response.data().errorCode()))

    val alterConfigVersions = response.data().apiKeys().find(ApiKeys.ALTER_CONFIGS.id)
    assertEquals(expectedVersions, alterConfigVersions)

    verify(authorizer, adminManager, forwardingManager)
  }

  @Test
  def testGetUnsupportedVersionsWhenControllerApiVersionsNotAvailable(): Unit = {
    val authorizer: Authorizer = EasyMock.niceMock(classOf[Authorizer])

    val requestHeader = new RequestHeader(ApiKeys.API_VERSIONS, ApiKeys.API_VERSIONS.latestVersion, clientId, 0)

    EasyMock.expect(forwardingManager.controllerApiVersions).andReturn(None)

    val capturedResponse = expectNoThrottling()

    val apiVersionsRequest = new ApiVersionsRequest.Builder()
      .build(requestHeader.apiVersion)
    val request = buildRequest(apiVersionsRequest,
      fromPrivilegedListener = true, requestHeader = Option(requestHeader))

    EasyMock.replay(replicaManager, clientRequestQuotaManager, forwardingManager,
      requestChannel, authorizer, adminManager, controller)

    createKafkaApis(authorizer = Some(authorizer), enableForwarding = true).handleApiVersionsRequest(request)

    val response = readResponse(apiVersionsRequest, capturedResponse)
      .asInstanceOf[ApiVersionsResponse]
    assertEquals(Errors.NONE, Errors.forCode(response.data().errorCode()))

    val expectedVersions = ApiVersionsResponse.toApiVersion(ApiKeys.ALTER_CONFIGS)

    val alterConfigVersions = response.data().apiKeys().find(ApiKeys.ALTER_CONFIGS.id)
    assertEquals(expectedVersions, alterConfigVersions)

    verify(authorizer, adminManager, forwardingManager)
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

    val capturedResponse = expectNoThrottling()

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
    testForwardableAPI(ApiKeys.CREATE_TOPICS, requestBuilder)
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
                                       capturedResponse: Capture[RequestChannel.Response],
                                       expectedResults: Map[String, Errors]): Unit = {
    val response = readResponse(createTopicsRequest, capturedResponse)
      .asInstanceOf[CreateTopicsResponse]
    val responseMap = response.data.topics().asScala.map { topicResponse =>
      topicResponse.name() -> Errors.forCode(topicResponse.errorCode)
    }.toMap

    assertEquals(expectedResults, responseMap)
  }

  @Test
  def testCreateAclWithForwarding(): Unit = {
    val requestBuilder = new CreateAclsRequest.Builder(new CreateAclsRequestData())
    testForwardableAPI(ApiKeys.CREATE_ACLS, requestBuilder)
  }

  @Test
  def testDeleteAclWithForwarding(): Unit = {
    val requestBuilder = new DeleteAclsRequest.Builder(new DeleteAclsRequestData())
    testForwardableAPI(ApiKeys.DELETE_ACLS, requestBuilder)
  }

  @Test
  def testCreateDelegationTokenWithForwarding(): Unit = {
    val requestBuilder = new CreateDelegationTokenRequest.Builder(new CreateDelegationTokenRequestData())
    testForwardableAPI(ApiKeys.CREATE_DELEGATION_TOKEN, requestBuilder)
  }

  @Test
  def testRenewDelegationTokenWithForwarding(): Unit = {
    val requestBuilder = new RenewDelegationTokenRequest.Builder(new RenewDelegationTokenRequestData())
    testForwardableAPI(ApiKeys.RENEW_DELEGATION_TOKEN, requestBuilder)
  }

  @Test
  def testExpireDelegationTokenWithForwarding(): Unit = {
    val requestBuilder = new ExpireDelegationTokenRequest.Builder(new ExpireDelegationTokenRequestData())
    testForwardableAPI(ApiKeys.EXPIRE_DELEGATION_TOKEN, requestBuilder)
  }

  @Test
  def testAlterPartitionReassignmentsWithForwarding(): Unit = {
    val requestBuilder = new AlterPartitionReassignmentsRequest.Builder(new AlterPartitionReassignmentsRequestData())
    testForwardableAPI(ApiKeys.ALTER_PARTITION_REASSIGNMENTS, requestBuilder)
  }

  @Test
  def testCreatePartitionsWithForwarding(): Unit = {
    val requestBuilder = new CreatePartitionsRequest.Builder(new CreatePartitionsRequestData())
    testForwardableAPI(ApiKeys.CREATE_PARTITIONS, requestBuilder)
  }

  @Test
  def testDeleteTopicsWithForwarding(): Unit = {
    val requestBuilder = new DeleteTopicsRequest.Builder(new DeleteTopicsRequestData())
    testForwardableAPI(ApiKeys.DELETE_TOPICS, requestBuilder)
  }

  @Test
  def testUpdateFeaturesWithForwarding(): Unit = {
    val requestBuilder = new UpdateFeaturesRequest.Builder(new UpdateFeaturesRequestData())
    testForwardableAPI(ApiKeys.UPDATE_FEATURES, requestBuilder)
  }

  @Test
  def testAlterScramWithForwarding(): Unit = {
    val requestBuilder = new AlterUserScramCredentialsRequest.Builder(new AlterUserScramCredentialsRequestData())
    testForwardableAPI(ApiKeys.ALTER_USER_SCRAM_CREDENTIALS, requestBuilder)
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
      val capturedResponse = expectNoThrottling()
      EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel)
      createKafkaApis().handleOffsetCommitRequest(request)

      val response = readResponse(offsetCommitRequest, capturedResponse)
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
        false
      ).build()
      val request = buildRequest(offsetCommitRequest)

      val capturedResponse = expectNoThrottling()
      EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel)
      createKafkaApis().handleTxnOffsetCommitRequest(request)

      val response = readResponse(offsetCommitRequest, capturedResponse)
        .asInstanceOf[TxnOffsetCommitResponse]
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
      val capturedResponse: Capture[RequestChannel.Response] = EasyMock.newCapture()
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
        false
      ).build(version.toShort)
      val request = buildRequest(offsetCommitRequest)

      EasyMock.expect(groupCoordinator.handleTxnCommitOffsets(
        EasyMock.eq(groupId),
        EasyMock.eq(producerId),
        EasyMock.eq(epoch),
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

      val response = readResponse(offsetCommitRequest, capturedResponse)
        .asInstanceOf[TxnOffsetCommitResponse]

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

      val capturedResponse: Capture[RequestChannel.Response] = EasyMock.newCapture()
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

      EasyMock.expect(txnCoordinator.handleInitProducerId(
        EasyMock.eq(transactionalId),
        EasyMock.eq(txnTimeoutMs),
        EasyMock.eq(expectedProducerIdAndEpoch),
        EasyMock.capture(responseCallback)
      )).andAnswer(
        () => responseCallback.getValue.apply(InitProducerIdResult(producerId, epoch, Errors.PRODUCER_FENCED)))

      EasyMock.expect(requestChannel.sendResponse(EasyMock.capture(capturedResponse)))

      EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      createKafkaApis().handleInitProducerIdRequest(request)

      val response = readResponse(initProducerIdRequest, capturedResponse)
        .asInstanceOf[InitProducerIdResponse]

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

      val capturedResponse: Capture[RequestChannel.Response] = EasyMock.newCapture()
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

      EasyMock.expect(txnCoordinator.handleAddPartitionsToTransaction(
        EasyMock.eq(transactionalId),
        EasyMock.eq(producerId),
        EasyMock.eq(epoch),
        EasyMock.eq(Set(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, partition))),
        EasyMock.capture(responseCallback)
      )).andAnswer(
        () => responseCallback.getValue.apply(Errors.PRODUCER_FENCED))

      EasyMock.expect(requestChannel.sendResponse(EasyMock.capture(capturedResponse)))

      EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator, groupCoordinator)

      createKafkaApis().handleAddOffsetsToTxnRequest(request)

      val response = readResponse(addOffsetsToTxnRequest, capturedResponse)
        .asInstanceOf[AddOffsetsToTxnResponse]

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

      val capturedResponse: Capture[RequestChannel.Response] = EasyMock.newCapture()
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

      EasyMock.expect(txnCoordinator.handleAddPartitionsToTransaction(
        EasyMock.eq(transactionalId),
        EasyMock.eq(producerId),
        EasyMock.eq(epoch),
        EasyMock.eq(Set(topicPartition)),

        EasyMock.capture(responseCallback)
      )).andAnswer(
        () => responseCallback.getValue.apply(Errors.PRODUCER_FENCED))

      EasyMock.expect(requestChannel.sendResponse(EasyMock.capture(capturedResponse)))

      EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      createKafkaApis().handleAddPartitionToTxnRequest(request)

      val response = readResponse(addPartitionsToTxnRequest, capturedResponse)
        .asInstanceOf[AddPartitionsToTxnResponse]

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

      val capturedResponse: Capture[RequestChannel.Response] = EasyMock.newCapture()
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

      EasyMock.expect(txnCoordinator.handleEndTransaction(
        EasyMock.eq(transactionalId),
        EasyMock.eq(producerId),
        EasyMock.eq(epoch),
        EasyMock.eq(TransactionResult.COMMIT),
        EasyMock.capture(responseCallback)
      )).andAnswer(
        () => responseCallback.getValue.apply(Errors.PRODUCER_FENCED))

      EasyMock.expect(requestChannel.sendResponse(EasyMock.capture(capturedResponse)))

      EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      createKafkaApis().handleEndTxnRequest(request)

      val response = readResponse(endTxnRequest, capturedResponse)
        .asInstanceOf[EndTxnResponse]

      if (version < 2) {
        assertEquals(Errors.INVALID_PRODUCER_EPOCH.code, response.data.errorCode)
      } else {
        assertEquals(Errors.PRODUCER_FENCED.code, response.data.errorCode)
      }
    }
  }

  @nowarn("cat=deprecation")
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
            .setName(tp.topic()).setPartitionData(Collections.singletonList(
            new ProduceRequestData.PartitionProduceData()
              .setIndex(tp.partition())
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
        EasyMock.anyObject())
      ).andAnswer(() => responseCallback.getValue.apply(Map(tp -> new PartitionResponse(Errors.INVALID_PRODUCER_EPOCH))))

      val capturedResponse = expectNoThrottling()
      EasyMock.expect(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
        anyObject[RequestChannel.Request](), anyDouble, anyLong)).andReturn(0)

      EasyMock.replay(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      createKafkaApis().handleProduceRequest(request)

      val response = readResponse(produceRequest, capturedResponse)
        .asInstanceOf[ProduceResponse]

      assertEquals(1, response.responses().size())
      for (partitionResponse <- response.responses().asScala) {
        assertEquals(Errors.INVALID_PRODUCER_EPOCH, partitionResponse._2.error)
      }
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

      val capturedResponse = expectNoThrottling()
      EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel)
      createKafkaApis().handleAddPartitionToTxnRequest(request)

      val response = readResponse(addPartitionsToTxnRequest, capturedResponse)
        .asInstanceOf[AddPartitionsToTxnResponse]
      assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, response.errors().get(invalidTopicPartition))
    }

    checkInvalidPartition(-1)
    checkInvalidPartition(1) // topic has only one partition
  }

  @Test
  def shouldThrowUnsupportedVersionExceptionOnHandleAddOffsetToTxnRequestWhenInterBrokerProtocolNotSupported(): Unit = {
    assertThrows(classOf[UnsupportedVersionException], () => createKafkaApis(KAFKA_0_10_2_IV0).handleAddOffsetsToTxnRequest(null))
  }

  @Test
  def shouldThrowUnsupportedVersionExceptionOnHandleAddPartitionsToTxnRequestWhenInterBrokerProtocolNotSupported(): Unit = {
    assertThrows(classOf[UnsupportedVersionException], () => createKafkaApis(KAFKA_0_10_2_IV0).handleAddPartitionToTxnRequest(null))
  }

  @Test
  def shouldThrowUnsupportedVersionExceptionOnHandleTxnOffsetCommitRequestWhenInterBrokerProtocolNotSupported(): Unit = {
    assertThrows(classOf[UnsupportedVersionException], () => createKafkaApis(KAFKA_0_10_2_IV0).handleAddPartitionToTxnRequest(null))
  }

  @Test
  def shouldThrowUnsupportedVersionExceptionOnHandleEndTxnRequestWhenInterBrokerProtocolNotSupported(): Unit = {
    assertThrows(classOf[UnsupportedVersionException], () => createKafkaApis(KAFKA_0_10_2_IV0).handleEndTxnRequest(null))
  }

  @Test
  def shouldThrowUnsupportedVersionExceptionOnHandleWriteTxnMarkersRequestWhenInterBrokerProtocolNotSupported(): Unit = {
    assertThrows(classOf[UnsupportedVersionException], () => createKafkaApis(KAFKA_0_10_2_IV0).handleWriteTxnMarkersRequest(null))
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

    val markersResponse = readResponse(writeTxnMarkersRequest, capturedResponse)
      .asInstanceOf[WriteTxnMarkersResponse]
    assertEquals(expectedErrors, markersResponse.errorsByProducerId.get(1L))
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

    val markersResponse = readResponse(writeTxnMarkersRequest, capturedResponse)
      .asInstanceOf[WriteTxnMarkersResponse]
    assertEquals(expectedErrors, markersResponse.errorsByProducerId.get(1L))
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

    val markersResponse = readResponse(writeTxnMarkersRequest, capturedResponse)
      .asInstanceOf[WriteTxnMarkersResponse]
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

    val markersResponse = readResponse(writeTxnMarkersRequest, capturedResponse)
      .asInstanceOf[WriteTxnMarkersResponse]
    assertEquals(expectedErrors, markersResponse.errorsByProducerId.get(1L))
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

    val capturedResponse = expectNoThrottling()
    EasyMock.expect(groupCoordinator.handleDescribeGroup(EasyMock.eq(groupId)))
      .andReturn((Errors.NONE, groupSummary))
    EasyMock.replay(groupCoordinator, replicaManager, clientRequestQuotaManager, requestChannel)

    createKafkaApis().handleDescribeGroupRequest(request)

    val response = readResponse(describeGroupsRequest, capturedResponse)
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

    val response = readResponse(offsetDeleteRequest, capturedResponse)
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

      val capturedResponse = expectNoThrottling()
      EasyMock.expect(groupCoordinator.handleDeleteOffsets(EasyMock.eq(group), EasyMock.eq(Seq.empty)))
        .andReturn((Errors.NONE, Map.empty))
      EasyMock.replay(groupCoordinator, replicaManager, clientRequestQuotaManager, requestChannel)

      createKafkaApis().handleOffsetDeleteRequest(request)

      val response = readResponse(offsetDeleteRequest, capturedResponse)
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

    val response = readResponse(offsetDeleteRequest, capturedResponse)
      .asInstanceOf[OffsetDeleteResponse]

    assertEquals(Errors.GROUP_ID_NOT_FOUND, Errors.forCode(response.data.errorCode()))
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

    val capturedResponse = expectNoThrottling()
    EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel)

    val targetTimes = List(new ListOffsetsTopic()
      .setName(tp.topic)
      .setPartitions(List(new ListOffsetsPartition()
        .setPartitionIndex(tp.partition)
        .setTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP)
        .setCurrentLeaderEpoch(currentLeaderEpoch.get)).asJava)).asJava
    val listOffsetRequest = ListOffsetsRequest.Builder.forConsumer(true, isolationLevel)
      .setTargetTimes(targetTimes).build()
    val request = buildRequest(listOffsetRequest)
    createKafkaApis().handleListOffsetRequest(request)

    val response = readResponse(listOffsetRequest, capturedResponse)
      .asInstanceOf[ListOffsetsResponse]
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
      EasyMock.partialMockBuilder(classOf[MetadataCache])
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
    assertEquals(List("remaining-topic"), response.topicMetadata().asScala.map { metadata => metadata.topic() })
    assertTrue(response.topicsByError(Errors.UNKNOWN_TOPIC_OR_PARTITION).isEmpty)
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
          None, None, None, Option.empty, isReassignmentFetch = false)))
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

    val response = readResponse(fetchRequest, capturedResponse)
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

    val response = readResponse(joinGroupRequest, capturedResponse)
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

    val response = readResponse(joinGroupRequest, capturedResponse)
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

    val response = readResponse(syncGroupRequest, capturedResponse)
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

    val response = readResponse(syncGroupRequest, capturedResponse)
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

    val response = readResponse(joinGroupRequest, capturedResponse).asInstanceOf[JoinGroupResponse]
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

    val response = readResponse(syncGroupRequest, capturedResponse).asInstanceOf[SyncGroupResponse]
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

    val response = readResponse(heartbeatRequest, capturedResponse).asInstanceOf[HeartbeatResponse]
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
    val response = readResponse(offsetCommitRequest, capturedResponse).asInstanceOf[OffsetCommitResponse]
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

    addTopicToMetadataCache(tp0.topic, numPartitions = 1)
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
        callback(Seq(tp0 -> FetchPartitionData(Errors.NONE, hw, 0, records,
          None, None, None, Option.empty, isReassignmentFetch = isReassigning)))
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

    val response = readResponse(initProducerIdRequest, capturedResponse)
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

    val response = readResponse(initProducerIdRequest, capturedResponse).asInstanceOf[InitProducerIdResponse]
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
    val updateMetadataResponse = readResponse(updateMetadataRequest, capturedResponse)
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

    EasyMock.expect(requestChannel.sendResponse(EasyMock.capture(capturedResponse)))
    EasyMock.replay(replicaManager, controller, requestChannel)

    createKafkaApis().handleLeaderAndIsrRequest(request)
    val leaderAndIsrResponse = readResponse(leaderAndIsrRequest, capturedResponse)
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
    val stopReplicaResponse = readResponse(stopReplicaRequest, capturedResponse)
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

    val response = readResponse(listGroupsRequest, capturedResponse).asInstanceOf[ListGroupsResponse]
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
    metadataCache.updateMetadata(correlationId = 0, updateMetadataRequest)

    val capturedResponse = expectNoThrottling()
    EasyMock.replay(clientRequestQuotaManager, requestChannel)

    val describeClusterRequest = new DescribeClusterRequest.Builder(new DescribeClusterRequestData()
      .setIncludeClusterAuthorizedOperations(true)).build()

    val request = buildRequest(describeClusterRequest, plaintextListener)
    createKafkaApis().handleDescribeCluster(request)

    val describeClusterResponse = readResponse(describeClusterRequest, capturedResponse)
      .asInstanceOf[DescribeClusterResponse]

    assertEquals(metadataCache.getControllerId.get, describeClusterResponse.data.controllerId)
    assertEquals(clusterId, describeClusterResponse.data.clusterId)
    assertEquals(8096, describeClusterResponse.data.clusterAuthorizedOperations)
    assertEquals(metadataCache.getAliveBrokers.map(_.node(plaintextListener)).toSet,
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
    metadataCache.updateMetadata(correlationId = 0, updateMetadataRequest)
    (plaintextListener, anotherListener)
  }

  private def sendMetadataRequestWithInconsistentListeners(requestListener: ListenerName): MetadataResponse = {
    val capturedResponse = expectNoThrottling()
    EasyMock.replay(clientRequestQuotaManager, requestChannel)

    val metadataRequest = MetadataRequest.Builder.allTopics.build()
    val requestChannelRequest = buildRequest(metadataRequest, requestListener)
    createKafkaApis().handleTopicMetadataRequest(requestChannelRequest)

    readResponse(metadataRequest, capturedResponse).asInstanceOf[MetadataResponse]
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

    val capturedResponse = expectNoThrottling()
    EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel)

    val targetTimes = List(new ListOffsetsTopic()
      .setName(tp.topic)
      .setPartitions(List(new ListOffsetsPartition()
        .setPartitionIndex(tp.partition)
        .setTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP)).asJava)).asJava
    val listOffsetRequest = ListOffsetsRequest.Builder.forConsumer(true, isolationLevel)
      .setTargetTimes(targetTimes).build()
    val request = buildRequest(listOffsetRequest)
    createKafkaApis().handleListOffsetRequest(request)

    val response = readResponse(listOffsetRequest, capturedResponse).asInstanceOf[ListOffsetsResponse]
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

  private def buildRequestWithEnvelope(
    request: AbstractRequest,
    fromPrivilegedListener: Boolean,
    principalSerde: KafkaPrincipalSerde = kafkaPrincipalSerde
  ): RequestChannel.Request = {
    val listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)

    val requestHeader = new RequestHeader(request.apiKey, request.version, clientId, 0)
    val requestBuffer = RequestTestUtils.serializeRequestWithHeader(requestHeader, request)

    val envelopeHeader = new RequestHeader(ApiKeys.ENVELOPE, ApiKeys.ENVELOPE.latestVersion(), clientId, 0)
    val envelopeBuffer = RequestTestUtils.serializeRequestWithHeader(envelopeHeader, new EnvelopeRequest.Builder(
      requestBuffer,
      principalSerde.serialize(KafkaPrincipal.ANONYMOUS),
      InetAddress.getLocalHost.getAddress
    ).build())
    val envelopeContext = new RequestContext(envelopeHeader, "1", InetAddress.getLocalHost,
      KafkaPrincipal.ANONYMOUS, listenerName, SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY,
      fromPrivilegedListener, Optional.of(principalSerde))

    RequestHeader.parse(envelopeBuffer)
    new RequestChannel.Request(
      processor = 1,
      context = envelopeContext,
      startTimeNanos = time.nanoseconds(),
      memoryPool = MemoryPool.NONE,
      buffer = envelopeBuffer,
      metrics = requestChannelMetrics
    )
  }

  private def buildRequest(request: AbstractRequest,
                           listenerName: ListenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
                           fromPrivilegedListener: Boolean = false,
                           requestHeader: Option[RequestHeader] = None): RequestChannel.Request = {
    val buffer = RequestTestUtils.serializeRequestWithHeader(requestHeader.getOrElse(
      new RequestHeader(request.apiKey, request.version, clientId, 0)), request)

    // read the header from the buffer first so that the body can be read next from the Request constructor
    val header = RequestHeader.parse(buffer)
    val context = new RequestContext(header, "1", InetAddress.getLocalHost, KafkaPrincipal.ANONYMOUS,
      listenerName, SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY, fromPrivilegedListener,
      Optional.of(kafkaPrincipalSerde))
    new RequestChannel.Request(processor = 1, context = context, startTimeNanos = 0, MemoryPool.NONE, buffer,
      requestChannelMetrics, envelope = None)
  }

  private def readResponse(request: AbstractRequest, capturedResponse: Capture[RequestChannel.Response]) = {
    val api = request.apiKey
    val response = capturedResponse.getValue
    assertTrue(response.isInstanceOf[SendResponse], s"Unexpected response type: ${response.getClass}")
    val sendResponse = response.asInstanceOf[SendResponse]
    val send = sendResponse.responseSend
    val channel = new ByteBufferChannel(send.size)
    send.writeTo(channel)
    channel.close()
    channel.buffer.getInt() // read the size
    ResponseHeader.parse(channel.buffer, api.responseHeaderVersion(request.version))
    AbstractResponse.parseResponse(api, channel.buffer, request.version)
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
      0, brokerEpoch, partitionStates.asJava, Seq(broker).asJava, Collections.emptyMap()).build()
  }

  private def addTopicToMetadataCache(topic: String, numPartitions: Int): Unit = {
    val updateMetadataRequest = createBasicMetadataRequest(topic, numPartitions, 0)
    metadataCache.updateMetadata(correlationId = 0, updateMetadataRequest)
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

    val capturedResponse = expectNoThrottling()
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

    val response = readResponse(alterReplicaLogDirsRequest, capturedResponse)
      .asInstanceOf[AlterReplicaLogDirsResponse]
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
    def fetchResponse(data: Map[TopicPartition, String]): FetchResponse[Records] = {
      val responseData = new util.LinkedHashMap[TopicPartition, FetchResponse.PartitionData[Records]](
        data.map { case (tp, raw) =>
          tp -> new FetchResponse.PartitionData(Errors.NONE,
            105, 105, 0, Optional.empty(), Collections.emptyList(), Optional.empty(),
            MemoryRecords.withRecords(CompressionType.NONE,
              new SimpleRecord(100, raw.getBytes(StandardCharsets.UTF_8))).asInstanceOf[Records])
      }.toMap.asJava)
      new FetchResponse(Errors.NONE, responseData, 100, 100)
    }

    val throttledPartition = new TopicPartition("throttledData", 0)
    val throttledData = Map(throttledPartition -> "throttledData")
    val expectedSize = FetchResponse.sizeOf(FetchResponseData.HIGHEST_SUPPORTED_VERSION,
      fetchResponse(throttledData).responseData.entrySet.iterator)

    val response = fetchResponse(throttledData ++ Map(new TopicPartition("nonThrottledData", 0) -> "nonThrottledData"))

    val quota = Mockito.mock(classOf[ReplicationQuotaManager])
    Mockito.when(quota.isThrottled(ArgumentMatchers.any(classOf[TopicPartition])))
      .thenAnswer(invocation => throttledPartition == invocation.getArgument(0).asInstanceOf[TopicPartition])

    assertEquals(expectedSize, KafkaApis.sizeOfThrottledPartitions(FetchResponseData.HIGHEST_SUPPORTED_VERSION, response, quota))
  }

  @Test
  def testDescribeProducers(): Unit = {
    val tp1 = new TopicPartition("foo", 0)
    val tp2 = new TopicPartition("bar", 3)
    val tp3 = new TopicPartition("baz", 1)

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
        .setPartitionIndexes(List(Int.box(tp3.partition)).asJava)

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
    val capturedResponse = expectNoThrottling()

    EasyMock.replay(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator, authorizer)
    createKafkaApis(authorizer = Some(authorizer)).handleDescribeProducersRequest(request)

    val response = readResponse(describeProducersRequest, capturedResponse)
      .asInstanceOf[DescribeProducersResponse]
    assertEquals(3, response.data.topics.size())
    assertEquals(Set("foo", "bar", "baz"), response.data.topics.asScala.map(_.name).toSet)

    val fooTopic = response.data.topics.asScala.find(_.name == tp1.topic).get
    val fooPartition = fooTopic.partitions.asScala.find(_.partitionIndex == tp1.partition).get
    assertEquals(Errors.NONE, Errors.forCode(fooPartition.errorCode))
    assertEquals(1, fooPartition.activeProducers.size)
    val fooProducer = fooPartition.activeProducers.get(0)
    assertEquals(12345L, fooProducer.producerId)
    assertEquals(15, fooProducer.producerEpoch)
    assertEquals(100, fooProducer.lastSequence)
    assertEquals(time.milliseconds(), fooProducer.lastTimestamp)
    assertEquals(-1, fooProducer.currentTxnStartOffset)
    assertEquals(200, fooProducer.coordinatorEpoch)

    val barTopic = response.data.topics.asScala.find(_.name == tp2.topic).get
    val barPartition = barTopic.partitions.asScala.find(_.partitionIndex == tp2.partition).get
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED, Errors.forCode(barPartition.errorCode))

    val bazTopic = response.data.topics.asScala.find(_.name == tp3.topic).get
    val bazPartition = bazTopic.partitions.asScala.find(_.partitionIndex == tp3.partition).get
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.forCode(bazPartition.errorCode))

  }

}
