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
import kafka.cluster.Broker
import kafka.controller.{ControllerContext, KafkaController}
import kafka.coordinator.group.GroupCoordinatorConcurrencyTest.{JoinGroupCallback, SyncGroupCallback}
import kafka.coordinator.group._
import kafka.coordinator.transaction.{InitProducerIdResult, TransactionCoordinator}
import kafka.log.AppendOrigin
import kafka.network.RequestChannel
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.metadata.{ConfigRepository, KRaftMetadataCache, MockConfigRepository, ZkMetadataCache}
import kafka.utils.{Log4jController, MockTime, TestUtils}
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.clients.admin.{AlterConfigOp, ConfigEntry}
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.internals.{KafkaFutureImpl, Topic}
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.config.ConfigResource.Type.{BROKER, BROKER_LOGGER}
import org.apache.kafka.common.message.AlterConfigsRequestData.{AlterConfigsResourceCollection => LAlterConfigsResourceCollection}
import org.apache.kafka.common.message.AlterConfigsRequestData.{AlterConfigsResource => LAlterConfigsResource}
import org.apache.kafka.common.message.AlterConfigsRequestData.{AlterableConfigCollection => LAlterableConfigCollection}
import org.apache.kafka.common.message.AlterConfigsRequestData.{AlterableConfig => LAlterableConfig}
import org.apache.kafka.common.message.AlterConfigsResponseData.{AlterConfigsResourceResponse => LAlterConfigsResourceResponse}
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.message.CreateTopicsRequestData.{CreatableTopic, CreatableTopicCollection}
import org.apache.kafka.common.message.DescribeConfigsResponseData.DescribeConfigsResult
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.{AlterConfigsResource => IAlterConfigsResource}
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.{AlterConfigsResourceCollection => IAlterConfigsResourceCollection}
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.{AlterableConfig => IAlterableConfig}
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.{AlterableConfigCollection => IAlterableConfigCollection}
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData.{AlterConfigsResourceResponse => IAlterConfigsResourceResponse}
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
import org.apache.kafka.common.{ElectionType, IsolationLevel, Node, TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.ArgumentMatchers.{any, anyBoolean, anyDouble, anyInt, anyLong, anyShort, anyString, argThat, isNotNull}
import org.mockito.Mockito.{mock, reset, times, verify, when}
import org.mockito.{ArgumentCaptor, ArgumentMatchers, Mockito}

import scala.collection.{Map, Seq, mutable}
import scala.jdk.CollectionConverters._

import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic

class KafkaApisTest {
  private val requestChannel: RequestChannel = mock(classOf[RequestChannel])
  private val requestChannelMetrics: RequestChannel.Metrics = mock(classOf[RequestChannel.Metrics])
  private val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
  private val groupCoordinator: GroupCoordinator = mock(classOf[GroupCoordinator])
  private val adminManager: ZkAdminManager = mock(classOf[ZkAdminManager])
  private val txnCoordinator: TransactionCoordinator = mock(classOf[TransactionCoordinator])
  private val controller: KafkaController = mock(classOf[KafkaController])
  private val forwardingManager: ForwardingManager = mock(classOf[ForwardingManager])
  private val autoTopicCreationManager: AutoTopicCreationManager = mock(classOf[AutoTopicCreationManager])

  private val kafkaPrincipalSerde = new KafkaPrincipalSerde {
    override def serialize(principal: KafkaPrincipal): Array[Byte] = Utils.utf8(principal.toString)
    override def deserialize(bytes: Array[Byte]): KafkaPrincipal = SecurityUtils.parseKafkaPrincipal(Utils.utf8(bytes))
  }
  private val zkClient: KafkaZkClient = mock(classOf[KafkaZkClient])
  private val metrics = new Metrics()
  private val brokerId = 1
  private var metadataCache: MetadataCache = MetadataCache.zkMetadataCache(brokerId)
  private val clientQuotaManager: ClientQuotaManager = mock(classOf[ClientQuotaManager])
  private val clientRequestQuotaManager: ClientRequestQuotaManager = mock(classOf[ClientRequestQuotaManager])
  private val clientControllerQuotaManager: ControllerMutationQuotaManager = mock(classOf[ControllerMutationQuotaManager])
  private val replicaQuotaManager: ReplicationQuotaManager = mock(classOf[ReplicationQuotaManager])
  private val quotas = QuotaManagers(clientQuotaManager, clientQuotaManager, clientRequestQuotaManager,
    clientControllerQuotaManager, replicaQuotaManager, replicaQuotaManager, replicaQuotaManager, None)
  private val fetchManager: FetchManager = mock(classOf[FetchManager])
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
      val voterId = brokerId + 1
      properties.put(KafkaConfig.QuorumVotersProp, s"$voterId@localhost:9093")
      properties.put(KafkaConfig.ControllerListenerNamesProp, "SSL")
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

    new KafkaApis(
      metadataSupport = metadataSupport,
      requestChannel = requestChannel,
      replicaManager = replicaManager,
      groupCoordinator = groupCoordinator,
      txnCoordinator = txnCoordinator,
      autoTopicCreationManager = autoTopicCreationManager,
      brokerId = brokerId,
      config = config,
      configRepository = configRepository,
      metadataCache = metadataCache,
      metrics = metrics,
      authorizer = authorizer,
      quotas = quotas,
      fetchManager = fetchManager,
      brokerTopicStats = brokerTopicStats,
      clusterId = clusterId,
      time = time,
      tokenManager = null,
      apiVersionManager = apiVersionManager)
  }

  @Test
  def testDescribeConfigsWithAuthorizer(): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])

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
    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(expectedActions.asJava)))
      .thenReturn(Seq(AuthorizationResult.ALLOWED).asJava)

    val configRepository: ConfigRepository = mock(classOf[ConfigRepository])
    val topicConfigs = new Properties()
    val propName = "min.insync.replicas"
    val propValue = "3"
    topicConfigs.put(propName, propValue)
    when(configRepository.topicConfig(resourceName)).thenReturn(topicConfigs)

    metadataCache = mock(classOf[ZkMetadataCache])
    when(metadataCache.contains(resourceName)).thenReturn(true)

    val describeConfigsRequest = new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData()
      .setIncludeSynonyms(true)
      .setResources(List(new DescribeConfigsRequestData.DescribeConfigsResource()
        .setResourceName(resourceName)
        .setResourceType(ConfigResource.Type.TOPIC.id)).asJava))
      .build(requestHeader.apiVersion)
    val request = buildRequest(describeConfigsRequest,
      requestHeader = Option(requestHeader))
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    createKafkaApis(authorizer = Some(authorizer), configRepository = configRepository)
      .handleDescribeConfigsRequest(request)

    verify(authorizer).authorize(any(), ArgumentMatchers.eq(expectedActions.asJava))
    val capturedResponse = verifyNoThrottling(request)
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
    val authorizer: Authorizer = mock(classOf[Authorizer])

    authorizeResource(authorizer, AclOperation.CLUSTER_ACTION, ResourceType.CLUSTER, Resource.CLUSTER_NAME, AuthorizationResult.ALLOWED)

    val operation = AclOperation.ALTER_CONFIGS
    val resourceName = "topic-1"
    val requestHeader = new RequestHeader(ApiKeys.ALTER_CONFIGS, ApiKeys.ALTER_CONFIGS.latestVersion,
      clientId, 0)

    when(controller.isActive).thenReturn(true)

    authorizeResource(authorizer, operation, ResourceType.TOPIC, resourceName, AuthorizationResult.ALLOWED)

    val configResource = new ConfigResource(ConfigResource.Type.TOPIC, resourceName)
    when(adminManager.alterConfigs(any(), ArgumentMatchers.eq(false)))
      .thenAnswer(_ => {
        Map(configResource -> alterConfigHandler.apply())
      })

    val configs = Map(
      configResource -> new AlterConfigsRequest.Config(
        Seq(new AlterConfigsRequest.ConfigEntry("foo", "bar")).asJava))
    val alterConfigsRequest = new AlterConfigsRequest.Builder(configs.asJava, false).build(requestHeader.apiVersion)

    val request = TestUtils.buildRequestWithEnvelope(
      alterConfigsRequest, kafkaPrincipalSerde, requestChannelMetrics, time.nanoseconds())

    val capturedResponse: ArgumentCaptor[AlterConfigsResponse] = ArgumentCaptor.forClass(classOf[AlterConfigsResponse])
    val capturedRequest: ArgumentCaptor[RequestChannel.Request] = ArgumentCaptor.forClass(classOf[RequestChannel.Request])

    createKafkaApis(authorizer = Some(authorizer), enableForwarding = true).handle(request, RequestLocal.withThreadConfinedCaching)

    verify(requestChannel).sendResponse(
      capturedRequest.capture(),
      capturedResponse.capture(),
      any()
    )
    assertEquals(Some(request), capturedRequest.getValue.envelope)
    val innerResponse = capturedResponse.getValue
    val responseMap = innerResponse.data.responses().asScala.map { resourceResponse =>
      resourceResponse.resourceName() -> Errors.forCode(resourceResponse.errorCode)
    }.toMap

    assertEquals(Map(resourceName -> expectedError), responseMap)

    verify(controller).isActive
    verify(adminManager).alterConfigs(any(), ArgumentMatchers.eq(false))
  }

  @Test
  def testInvalidEnvelopeRequestWithNonForwardableAPI(): Unit = {
    val requestHeader = new RequestHeader(ApiKeys.LEAVE_GROUP, ApiKeys.LEAVE_GROUP.latestVersion,
      clientId, 0)
    val leaveGroupRequest = new LeaveGroupRequest.Builder("group",
      Collections.singletonList(new MemberIdentity())).build(requestHeader.apiVersion)

    when(controller.isActive).thenReturn(true)

    val request = TestUtils.buildRequestWithEnvelope(
      leaveGroupRequest, kafkaPrincipalSerde, requestChannelMetrics, time.nanoseconds())
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    createKafkaApis(enableForwarding = true).handle(request, RequestLocal.withThreadConfinedCaching)

    val capturedResponse = verifyNoThrottling(request)
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
    val authorizer: Authorizer = mock(classOf[Authorizer])

    if (performAuthorize) {
      authorizeResource(authorizer, AclOperation.CLUSTER_ACTION, ResourceType.CLUSTER, Resource.CLUSTER_NAME, authorizeResult)
    }

    val resourceName = "topic-1"
    val requestHeader = new RequestHeader(ApiKeys.ALTER_CONFIGS, ApiKeys.ALTER_CONFIGS.latestVersion,
      clientId, 0)

    when(controller.isActive).thenReturn(isActiveController)

    val configResource = new ConfigResource(ConfigResource.Type.TOPIC, resourceName)

    val configs = Map(
      configResource -> new AlterConfigsRequest.Config(
        Seq(new AlterConfigsRequest.ConfigEntry("foo", "bar")).asJava))
    val alterConfigsRequest = new AlterConfigsRequest.Builder(configs.asJava, false)
      .build(requestHeader.apiVersion)

    val request = TestUtils.buildRequestWithEnvelope(
      alterConfigsRequest, kafkaPrincipalSerde, requestChannelMetrics, time.nanoseconds(), fromPrivilegedListener)

    val capturedResponse: ArgumentCaptor[AbstractResponse] = ArgumentCaptor.forClass(classOf[AbstractResponse])
    createKafkaApis(authorizer = Some(authorizer), enableForwarding = true).handle(request, RequestLocal.withThreadConfinedCaching)

    if (shouldCloseConnection) {
      verify(requestChannel).closeConnection(
        ArgumentMatchers.eq(request),
        ArgumentMatchers.eq(java.util.Collections.emptyMap())
      )
    } else {
      verify(requestChannel).sendResponse(
        ArgumentMatchers.eq(request),
        capturedResponse.capture(),
        ArgumentMatchers.eq(None))
      val response = capturedResponse.getValue.asInstanceOf[EnvelopeResponse]
      assertEquals(expectedError, response.error)
    }
    if (performAuthorize) {
      verify(authorizer).authorize(any(), any())
    }
  }

  @Test
  def testAlterConfigsWithAuthorizer(): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])

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

    when(controller.isActive).thenReturn(false)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    when(adminManager.alterConfigs(any(), ArgumentMatchers.eq(false)))
      .thenReturn(Map(authorizedResource -> ApiError.NONE))

    createKafkaApis(authorizer = Some(authorizer)).handleAlterConfigsRequest(request)

    val capturedResponse = verifyNoThrottling(request)
    verifyAlterConfigResult(capturedResponse, Map(authorizedTopic -> Errors.NONE,
        unauthorizedTopic -> Errors.TOPIC_AUTHORIZATION_FAILED))
    verify(authorizer, times(2)).authorize(any(), any())
    verify(adminManager).alterConfigs(any(), anyBoolean())
  }

  @Test
  def testElectLeadersForwarding(): Unit = {
    val requestBuilder = new ElectLeadersRequest.Builder(ElectionType.PREFERRED, null, 30000)
    testKraftForwarding(ApiKeys.ELECT_LEADERS, requestBuilder)
  }

  @Test
  def testDescribeQuorumNotAllowedForZkClusters(): Unit = {
    val requestData = DescribeQuorumRequest.singletonRequest(KafkaRaftServer.MetadataPartition)
    val requestBuilder = new DescribeQuorumRequest.Builder(requestData)
    val request = buildRequest(requestBuilder.build())

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    createKafkaApis(enableForwarding = true).handle(request, RequestLocal.withThreadConfinedCaching)

    val capturedResponse = verifyNoThrottling(request)
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

  private def testKraftForwarding(
    apiKey: ApiKeys,
    requestBuilder: AbstractRequest.Builder[_ <: AbstractRequest]
  ): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    testForwardableApi(
      createKafkaApis(enableForwarding = true, raftSupport = true),
      apiKey,
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

    val apiRequest = requestBuilder.build(topicHeader.apiVersion)
    val request = buildRequest(apiRequest)

    if (kafkaApis.metadataSupport.isInstanceOf[ZkSupport]) {
      // The controller check only makes sense for ZK clusters. For KRaft,
      // controller requests are handled on a separate listener, so there
      // is no choice but to forward them.
      when(controller.isActive).thenReturn(false)
    }

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    val forwardCallback: ArgumentCaptor[Option[AbstractResponse] => Unit] = ArgumentCaptor.forClass(classOf[Option[AbstractResponse] => Unit])

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)
    verify(forwardingManager).forwardRequest(
      ArgumentMatchers.eq(request),
      forwardCallback.capture()
    )
    assertNotNull(request.buffer, "The buffer was unexpectedly deallocated after " +
      s"`handle` returned (is $apiKey marked as forwardable in `ApiKeys`?)")

    val expectedResponse = apiRequest.getErrorResponse(Errors.NOT_CONTROLLER.exception)
    forwardCallback.getValue.apply(Some(expectedResponse))

    val capturedResponse = verifyNoThrottling(request)
    assertEquals(expectedResponse, capturedResponse.getValue)

    if (kafkaApis.metadataSupport.isInstanceOf[ZkSupport]) {
      verify(controller).isActive
    }
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

    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(Seq(expectedAuthorizedAction).asJava)))
      .thenReturn(Seq(result).asJava)
  }

  private def verifyAlterConfigResult(capturedResponse: ArgumentCaptor[AbstractResponse],
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
    val authorizer: Authorizer = mock(classOf[Authorizer])

    val authorizedTopic = "authorized-topic"
    val unauthorizedTopic = "unauthorized-topic"
    val (authorizedResource, unauthorizedResource) =
      createConfigsWithAuthorization(authorizer, authorizedTopic, unauthorizedTopic)

    val requestHeader = new RequestHeader(ApiKeys.INCREMENTAL_ALTER_CONFIGS, ApiKeys.INCREMENTAL_ALTER_CONFIGS.latestVersion, clientId, 0)

    val incrementalAlterConfigsRequest = getIncrementalAlterConfigRequestBuilder(Seq(authorizedResource, unauthorizedResource))
      .build(requestHeader.apiVersion)
    val request = buildRequest(incrementalAlterConfigsRequest,
      fromPrivilegedListener = true, requestHeader = Option(requestHeader))

    when(controller.isActive).thenReturn(true)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    when(adminManager.incrementalAlterConfigs(any(), ArgumentMatchers.eq(false)))
      .thenReturn(Map(authorizedResource -> ApiError.NONE))

    createKafkaApis(authorizer = Some(authorizer)).handleIncrementalAlterConfigsRequest(request)

    val capturedResponse = verifyNoThrottling(request)
    verifyIncrementalAlterConfigResult(capturedResponse, Map(
      authorizedTopic -> Errors.NONE,
      unauthorizedTopic -> Errors.TOPIC_AUTHORIZATION_FAILED
    ))

    verify(authorizer, times(2)).authorize(any(), any())
    verify(adminManager).incrementalAlterConfigs(any(), anyBoolean())
  }

  private def getIncrementalAlterConfigRequestBuilder(configResources: Seq[ConfigResource]): IncrementalAlterConfigsRequest.Builder = {
    val resourceMap = configResources.map(configResource => {
      configResource -> Set(
        new AlterConfigOp(new ConfigEntry("foo", "bar"),
        OpType.forId(configResource.`type`.id))).asJavaCollection
    }).toMap.asJava

    new IncrementalAlterConfigsRequest.Builder(resourceMap, false)
  }

  private def verifyIncrementalAlterConfigResult(capturedResponse: ArgumentCaptor[AbstractResponse],
                                                 expectedResults: Map[String, Errors]): Unit = {
    val response = capturedResponse.getValue.asInstanceOf[IncrementalAlterConfigsResponse]
    val responseMap = response.data.responses().asScala.map { resourceResponse =>
      resourceResponse.resourceName() -> Errors.forCode(resourceResponse.errorCode)
    }.toMap
    assertEquals(expectedResults, responseMap)
  }

  @Test
  def testAlterClientQuotasWithAuthorizer(): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])

    authorizeResource(authorizer, AclOperation.ALTER_CONFIGS, ResourceType.CLUSTER,
      Resource.CLUSTER_NAME, AuthorizationResult.DENIED)

    val quotaEntity = new ClientQuotaEntity(Collections.singletonMap(ClientQuotaEntity.USER, "user"))
    val quotas = Seq(new ClientQuotaAlteration(quotaEntity, Seq.empty.asJavaCollection))

    val requestHeader = new RequestHeader(ApiKeys.ALTER_CLIENT_QUOTAS, ApiKeys.ALTER_CLIENT_QUOTAS.latestVersion, clientId, 0)

    val alterClientQuotasRequest = new AlterClientQuotasRequest.Builder(quotas.asJavaCollection, false)
      .build(requestHeader.apiVersion)
    val request = buildRequest(alterClientQuotasRequest,
      fromPrivilegedListener = true, requestHeader = Option(requestHeader))

    when(controller.isActive).thenReturn(true)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      anyLong)).thenReturn(0)

    createKafkaApis(authorizer = Some(authorizer)).handleAlterClientQuotasRequest(request)

    val capturedResponse = verifyNoThrottling(request)
    verifyAlterClientQuotaResult(capturedResponse, Map(quotaEntity -> Errors.CLUSTER_AUTHORIZATION_FAILED))

    verify(authorizer).authorize(any(), any())
    verify(clientRequestQuotaManager).maybeRecordAndGetThrottleTimeMs(any(), anyLong)
  }

  @Test
  def testAlterClientQuotasWithForwarding(): Unit = {
    val requestBuilder = new AlterClientQuotasRequest.Builder(List.empty.asJava, false)
    testForwardableApi(ApiKeys.ALTER_CLIENT_QUOTAS, requestBuilder)
  }

  private def verifyAlterClientQuotaResult(capturedResponse: ArgumentCaptor[AbstractResponse],
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
    val authorizer: Authorizer = mock(classOf[Authorizer])

    val authorizedTopic = "authorized-topic"
    val unauthorizedTopic = "unauthorized-topic"

    authorizeResource(authorizer, AclOperation.CREATE, ResourceType.CLUSTER,
      Resource.CLUSTER_NAME, AuthorizationResult.DENIED, logIfDenied = false)

    createCombinedTopicAuthorization(authorizer, AclOperation.CREATE,
      authorizedTopic, unauthorizedTopic)

    createCombinedTopicAuthorization(authorizer, AclOperation.DESCRIBE_CONFIGS,
      authorizedTopic, unauthorizedTopic, logIfDenied = false)

    val requestHeader = new RequestHeader(ApiKeys.CREATE_TOPICS, ApiKeys.CREATE_TOPICS.latestVersion, clientId, 0)

    when(controller.isActive).thenReturn(true)

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

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    when(clientControllerQuotaManager.newQuotaFor(
      ArgumentMatchers.eq(request), ArgumentMatchers.eq(6))).thenReturn(UnboundedControllerMutationQuota)

    createKafkaApis(authorizer = Some(authorizer)).handleCreateTopicsRequest(request)

    val capturedCallback: ArgumentCaptor[Map[String, ApiError] => Unit] = ArgumentCaptor.forClass(classOf[Map[String, ApiError] => Unit])

    verify(adminManager).createTopics(
      ArgumentMatchers.eq(timeout),
      ArgumentMatchers.eq(false),
      ArgumentMatchers.eq(Map(authorizedTopic -> topicToCreate)),
      any(),
      ArgumentMatchers.eq(UnboundedControllerMutationQuota),
      capturedCallback.capture())
    capturedCallback.getValue.apply(Map(authorizedTopic -> ApiError.NONE))

    val capturedResponse = verifyNoThrottling(request)
    verifyCreateTopicsResult(capturedResponse, Map(authorizedTopic -> Errors.NONE,
        unauthorizedTopic -> Errors.TOPIC_AUTHORIZATION_FAILED))
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

  @Test
  def testCreatePartitionsAuthorization(): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])
    val kafkaApis = createKafkaApis(authorizer = Some(authorizer))

    val timeoutMs = 35000
    val requestData = new CreatePartitionsRequestData()
      .setTimeoutMs(timeoutMs)
      .setValidateOnly(false)
    val fooCreatePartitionsData = new CreatePartitionsTopic().setName("foo").setAssignments(null).setCount(2)
    val barCreatePartitionsData = new CreatePartitionsTopic().setName("bar").setAssignments(null).setCount(10)
    requestData.topics().add(fooCreatePartitionsData)
    requestData.topics().add(barCreatePartitionsData)

    val fooResource = new ResourcePattern(ResourceType.TOPIC, "foo", PatternType.LITERAL)
    val fooAction = new Action(AclOperation.ALTER, fooResource, 1, true, true)

    val barResource = new ResourcePattern(ResourceType.TOPIC, "bar", PatternType.LITERAL)
    val barAction = new Action(AclOperation.ALTER, barResource, 1, true, true)

    when(authorizer.authorize(
      any[RequestContext](),
      any[util.List[Action]]()
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument[util.List[Action]](1).asScala
      val results = actions.map { action =>
        if (action == fooAction) AuthorizationResult.ALLOWED
        else if (action == barAction) AuthorizationResult.DENIED
        else throw new AssertionError(s"Unexpected action $action")
      }
      new util.ArrayList[AuthorizationResult](results.asJava)
    }

    val request = buildRequest(new CreatePartitionsRequest.Builder(requestData).build())

    when(controller.isActive).thenReturn(true)
    when(controller.isTopicQueuedForDeletion("foo")).thenReturn(false)
    when(clientControllerQuotaManager.newQuotaFor(
      ArgumentMatchers.eq(request), ArgumentMatchers.anyShort())
    ).thenReturn(UnboundedControllerMutationQuota)
    when(adminManager.createPartitions(
      timeoutMs = ArgumentMatchers.eq(timeoutMs),
      newPartitions = ArgumentMatchers.eq(Seq(fooCreatePartitionsData)),
      validateOnly = ArgumentMatchers.eq(false),
      controllerMutationQuota = ArgumentMatchers.eq(UnboundedControllerMutationQuota),
      callback = ArgumentMatchers.any[Map[String, ApiError] => Unit]()
    )).thenAnswer { invocation =>
      val callback = invocation.getArgument[Map[String, ApiError] => Unit](4)
      callback.apply(Map("foo" -> ApiError.NONE))
    }

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    val capturedResponse = verifyNoThrottling(request)
    val response = capturedResponse.getValue.asInstanceOf[CreatePartitionsResponse]
    val results = response.data.results.asScala
    assertEquals(Some(Errors.NONE), results.find(_.name == "foo").map(result => Errors.forCode(result.errorCode)))
    assertEquals(Some(Errors.TOPIC_AUTHORIZATION_FAILED), results.find(_.name == "bar").map(result => Errors.forCode(result.errorCode)))
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

    when(authorizer.authorize(
      any[RequestContext], argThat((t: java.util.List[Action]) => t != null && t.containsAll(expectedAuthorizedActions.asJava))
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument(1).asInstanceOf[util.List[Action]]
      actions.asScala.map { action =>
        if (action.resourcePattern().name().equals(authorizedTopic))
          AuthorizationResult.ALLOWED
        else
          AuthorizationResult.DENIED
      }.asJava
    }
  }

  private def verifyCreateTopicsResult(capturedResponse: ArgumentCaptor[AbstractResponse],
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
    val authorizer: Authorizer = mock(classOf[Authorizer])

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
          when(groupCoordinator.offsetsTopicConfigs).thenReturn(new Properties)
          authorizeResource(authorizer, AclOperation.DESCRIBE, ResourceType.GROUP,
            groupId, AuthorizationResult.ALLOWED)
          Topic.GROUP_METADATA_TOPIC_NAME
        case CoordinatorType.TRANSACTION =>
          topicConfigOverride.put(KafkaConfig.TransactionsTopicPartitionsProp, numBrokersNeeded.toString)
          topicConfigOverride.put(KafkaConfig.TransactionsTopicReplicationFactorProp, numBrokersNeeded.toString)
          when(txnCoordinator.transactionTopicConfigs).thenReturn(new Properties)
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
          .setCoordinatorKeys(asList(groupId)))
    } else {
      new FindCoordinatorRequest.Builder(
        new FindCoordinatorRequestData()
          .setKeyType(coordinatorType.id())
          .setKey(groupId))
    }
    val request = buildRequest(findCoordinatorRequestBuilder.build(requestHeader.apiVersion))
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    val capturedRequest = verifyTopicCreation(topicName, true, true, request)

    createKafkaApis(authorizer = Some(authorizer),
      overrideProperties = topicConfigOverride).handleFindCoordinatorRequest(request)

    val capturedResponse = verifyNoThrottling(request)
    val response = capturedResponse.getValue.asInstanceOf[FindCoordinatorResponse]
    if (version >= 4) {
      assertEquals(Errors.COORDINATOR_NOT_AVAILABLE.code, response.data.coordinators.get(0).errorCode)
      assertEquals(groupId, response.data.coordinators.get(0).key)
    } else {
      assertEquals(Errors.COORDINATOR_NOT_AVAILABLE.code, response.data.errorCode)
    }
    assertTrue(capturedRequest.getValue.isEmpty)
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
    val authorizer: Authorizer = mock(classOf[Authorizer])

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
          when(groupCoordinator.offsetsTopicConfigs).thenReturn(new Properties)
          true

        case Topic.TRANSACTION_STATE_TOPIC_NAME =>
          topicConfigOverride.put(KafkaConfig.TransactionsTopicPartitionsProp, numBrokersNeeded.toString)
          topicConfigOverride.put(KafkaConfig.TransactionsTopicReplicationFactorProp, numBrokersNeeded.toString)
          when(txnCoordinator.transactionTopicConfigs).thenReturn(new Properties)
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

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    val capturedRequest = verifyTopicCreation(topicName, enableAutoTopicCreation, isInternal, request)

    createKafkaApis(authorizer = Some(authorizer), enableForwarding = enableAutoTopicCreation,
      overrideProperties = topicConfigOverride).handleTopicMetadataRequest(request)

    val capturedResponse = verifyNoThrottling(request)
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
  }

  private def verifyTopicCreation(topicName: String,
                                  enableAutoTopicCreation: Boolean,
                                  isInternal: Boolean,
                                  request: RequestChannel.Request): ArgumentCaptor[Option[RequestContext]] = {
    val capturedRequest: ArgumentCaptor[Option[RequestContext]] = ArgumentCaptor.forClass(classOf[Option[RequestContext]])
    if (enableAutoTopicCreation) {
      when(clientControllerQuotaManager.newPermissiveQuotaFor(ArgumentMatchers.eq(request)))
        .thenReturn(UnboundedControllerMutationQuota)

      when(autoTopicCreationManager.createTopics(
        ArgumentMatchers.eq(Set(topicName)),
        ArgumentMatchers.eq(UnboundedControllerMutationQuota),
        capturedRequest.capture())).thenReturn(
        Seq(new MetadataResponseTopic()
        .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
        .setIsInternal(isInternal)
        .setName(topicName))
      )
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

    // if version is 10 or 11, the invalid topic metadata should return an error
    val invalidVersions = Set(10, 11)
    invalidVersions.foreach( version =>
      topics.foreach(topic => {
        val metadataRequestData = new MetadataRequestData().setTopics(Collections.singletonList(topic))
        val request = buildRequest(new MetadataRequest(metadataRequestData, version.toShort))
        val kafkaApis = createKafkaApis()

        val capturedResponse: ArgumentCaptor[AbstractResponse] = ArgumentCaptor.forClass(classOf[AbstractResponse])
        kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)
        verify(requestChannel).sendResponse(
          ArgumentMatchers.eq(request),
          capturedResponse.capture(),
          any()
        )
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
      reset(replicaManager, clientRequestQuotaManager, requestChannel)

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
      when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
        any[Long])).thenReturn(0)
      createKafkaApis().handleOffsetCommitRequest(request, RequestLocal.withThreadConfinedCaching)

      val capturedResponse = verifyNoThrottling(request)
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
      reset(replicaManager, clientRequestQuotaManager, requestChannel)

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
      when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
        any[Long])).thenReturn(0)
      createKafkaApis().handleTxnOffsetCommitRequest(request, RequestLocal.withThreadConfinedCaching)

      val capturedResponse = verifyNoThrottling(request)
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
      reset(replicaManager, clientRequestQuotaManager, requestChannel, groupCoordinator)

      val topicPartition = new TopicPartition(topic, 1)
      val capturedResponse: ArgumentCaptor[TxnOffsetCommitResponse] = ArgumentCaptor.forClass(classOf[TxnOffsetCommitResponse])
      val responseCallback: ArgumentCaptor[Map[TopicPartition, Errors] => Unit] = ArgumentCaptor.forClass(classOf[Map[TopicPartition, Errors] => Unit])

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
      when(groupCoordinator.handleTxnCommitOffsets(
        ArgumentMatchers.eq(groupId),
        ArgumentMatchers.eq(producerId),
        ArgumentMatchers.eq(epoch),
        anyString,
        ArgumentMatchers.eq(Option.empty),
        anyInt,
        any(),
        responseCallback.capture(),
        ArgumentMatchers.eq(requestLocal)
      )).thenAnswer(_ => responseCallback.getValue.apply(Map(topicPartition -> Errors.COORDINATOR_LOAD_IN_PROGRESS)))

      createKafkaApis().handleTxnOffsetCommitRequest(request, requestLocal)

      verify(requestChannel).sendResponse(
        ArgumentMatchers.eq(request),
        capturedResponse.capture(),
        ArgumentMatchers.eq(None)
      )
      val response = capturedResponse.getValue

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

      reset(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      val capturedResponse: ArgumentCaptor[InitProducerIdResponse] = ArgumentCaptor.forClass(classOf[InitProducerIdResponse])
      val responseCallback: ArgumentCaptor[InitProducerIdResult => Unit] = ArgumentCaptor.forClass(classOf[InitProducerIdResult => Unit])

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
      when(txnCoordinator.handleInitProducerId(
        ArgumentMatchers.eq(transactionalId),
        ArgumentMatchers.eq(txnTimeoutMs),
        ArgumentMatchers.eq(expectedProducerIdAndEpoch),
        responseCallback.capture(),
        ArgumentMatchers.eq(requestLocal)
      )).thenAnswer(_ => responseCallback.getValue.apply(InitProducerIdResult(producerId, epoch, Errors.PRODUCER_FENCED)))

      createKafkaApis().handleInitProducerIdRequest(request, requestLocal)

      verify(requestChannel).sendResponse(
        ArgumentMatchers.eq(request),
        capturedResponse.capture(),
        ArgumentMatchers.eq(None)
      )
      val response = capturedResponse.getValue

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

      reset(replicaManager, clientRequestQuotaManager, requestChannel, groupCoordinator, txnCoordinator)

      val capturedResponse: ArgumentCaptor[AddOffsetsToTxnResponse] = ArgumentCaptor.forClass(classOf[AddOffsetsToTxnResponse])
      val responseCallback: ArgumentCaptor[Errors => Unit] = ArgumentCaptor.forClass(classOf[Errors => Unit])

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
      when(groupCoordinator.partitionFor(
        ArgumentMatchers.eq(groupId)
      )).thenReturn(partition)

      val requestLocal = RequestLocal.withThreadConfinedCaching
      when(txnCoordinator.handleAddPartitionsToTransaction(
        ArgumentMatchers.eq(transactionalId),
        ArgumentMatchers.eq(producerId),
        ArgumentMatchers.eq(epoch),
        ArgumentMatchers.eq(Set(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, partition))),
        responseCallback.capture(),
        ArgumentMatchers.eq(requestLocal)
      )).thenAnswer(_ => responseCallback.getValue.apply(Errors.PRODUCER_FENCED))

      createKafkaApis().handleAddOffsetsToTxnRequest(request, requestLocal)

      verify(requestChannel).sendResponse(
        ArgumentMatchers.eq(request),
        capturedResponse.capture(),
        ArgumentMatchers.eq(None)
      )
      val response = capturedResponse.getValue

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

      reset(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      val capturedResponse: ArgumentCaptor[AddPartitionsToTxnResponse] = ArgumentCaptor.forClass(classOf[AddPartitionsToTxnResponse])
      val responseCallback: ArgumentCaptor[Errors => Unit] = ArgumentCaptor.forClass(classOf[Errors => Unit])

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
      when(txnCoordinator.handleAddPartitionsToTransaction(
        ArgumentMatchers.eq(transactionalId),
        ArgumentMatchers.eq(producerId),
        ArgumentMatchers.eq(epoch),
        ArgumentMatchers.eq(Set(topicPartition)),
        responseCallback.capture(),
        ArgumentMatchers.eq(requestLocal)
      )).thenAnswer(_ => responseCallback.getValue.apply(Errors.PRODUCER_FENCED))

      createKafkaApis().handleAddPartitionToTxnRequest(request, requestLocal)

      verify(requestChannel).sendResponse(
        ArgumentMatchers.eq(request),
        capturedResponse.capture(),
        ArgumentMatchers.eq(None)
      )
      val response = capturedResponse.getValue

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
      reset(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      val capturedResponse: ArgumentCaptor[EndTxnResponse] = ArgumentCaptor.forClass(classOf[EndTxnResponse])
      val responseCallback: ArgumentCaptor[Errors => Unit] = ArgumentCaptor.forClass(classOf[Errors => Unit])

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
      when(txnCoordinator.handleEndTransaction(
        ArgumentMatchers.eq(transactionalId),
        ArgumentMatchers.eq(producerId),
        ArgumentMatchers.eq(epoch),
        ArgumentMatchers.eq(TransactionResult.COMMIT),
        responseCallback.capture(),
        ArgumentMatchers.eq(requestLocal)
      )).thenAnswer(_ => responseCallback.getValue.apply(Errors.PRODUCER_FENCED))

      createKafkaApis().handleEndTxnRequest(request, requestLocal)

      verify(requestChannel).sendResponse(
        ArgumentMatchers.eq(request),
        capturedResponse.capture(),
        ArgumentMatchers.eq(None)
      )
      val response = capturedResponse.getValue

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

      reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      val responseCallback: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] = ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])

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

      when(replicaManager.appendRecords(anyLong,
        anyShort,
        ArgumentMatchers.eq(false),
        ArgumentMatchers.eq(AppendOrigin.Client),
        any(),
        responseCallback.capture(),
        any(),
        any(),
        any())
      ).thenAnswer(_ => responseCallback.getValue.apply(Map(tp -> new PartitionResponse(Errors.INVALID_PRODUCER_EPOCH))))

      when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
        any[Long])).thenReturn(0)
      when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
        any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

      createKafkaApis().handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

      val capturedResponse = verifyNoThrottling(request)
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
      reset(replicaManager, clientRequestQuotaManager, requestChannel)

      val invalidTopicPartition = new TopicPartition(topic, invalidPartitionId)
      val addPartitionsToTxnRequest = new AddPartitionsToTxnRequest.Builder(
        "txnlId", 15L, 0.toShort, List(invalidTopicPartition).asJava
      ).build()
      val request = buildRequest(addPartitionsToTxnRequest)

      when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
        any[Long])).thenReturn(0)
      createKafkaApis().handleAddPartitionToTxnRequest(request, RequestLocal.withThreadConfinedCaching)

      val capturedResponse = verifyNoThrottling(request)
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
    val (_, request) = createWriteTxnMarkersRequest(asList(topicPartition))
    val expectedErrors = Map(topicPartition -> Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT).asJava
    val capturedResponse: ArgumentCaptor[WriteTxnMarkersResponse] = ArgumentCaptor.forClass(classOf[WriteTxnMarkersResponse])

    when(replicaManager.getMagic(topicPartition))
      .thenReturn(Some(RecordBatch.MAGIC_VALUE_V1))

    createKafkaApis().handleWriteTxnMarkersRequest(request, RequestLocal.withThreadConfinedCaching)

    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None)
    )
    val markersResponse = capturedResponse.getValue
    assertEquals(expectedErrors, markersResponse.errorsByProducerId.get(1L))
  }

  @Test
  def shouldRespondWithUnknownTopicWhenPartitionIsNotHosted(): Unit = {
    val topicPartition = new TopicPartition("t", 0)
    val (_, request) = createWriteTxnMarkersRequest(asList(topicPartition))
    val expectedErrors = Map(topicPartition -> Errors.UNKNOWN_TOPIC_OR_PARTITION).asJava
    val capturedResponse: ArgumentCaptor[WriteTxnMarkersResponse] = ArgumentCaptor.forClass(classOf[WriteTxnMarkersResponse])

    when(replicaManager.getMagic(topicPartition))
      .thenReturn(None)
    createKafkaApis().handleWriteTxnMarkersRequest(request, RequestLocal.withThreadConfinedCaching)

    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None)
    )
    val markersResponse = capturedResponse.getValue
    assertEquals(expectedErrors, markersResponse.errorsByProducerId.get(1L))
  }

  @Test
  def shouldRespondWithUnsupportedMessageFormatForBadPartitionAndNoErrorsForGoodPartition(): Unit = {
    val tp1 = new TopicPartition("t", 0)
    val tp2 = new TopicPartition("t1", 0)
    val (_, request) = createWriteTxnMarkersRequest(asList(tp1, tp2))
    val expectedErrors = Map(tp1 -> Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT, tp2 -> Errors.NONE).asJava

    val capturedResponse: ArgumentCaptor[WriteTxnMarkersResponse] = ArgumentCaptor.forClass(classOf[WriteTxnMarkersResponse])
    val responseCallback: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] = ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])

    when(replicaManager.getMagic(tp1))
      .thenReturn(Some(RecordBatch.MAGIC_VALUE_V1))
    when(replicaManager.getMagic(tp2))
      .thenReturn(Some(RecordBatch.MAGIC_VALUE_V2))

    val requestLocal = RequestLocal.withThreadConfinedCaching
    when(replicaManager.appendRecords(anyLong,
      anyShort,
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(AppendOrigin.Coordinator),
      any(),
      responseCallback.capture(),
      any(),
      any(),
      ArgumentMatchers.eq(requestLocal))
    ).thenAnswer(_ => responseCallback.getValue.apply(Map(tp2 -> new PartitionResponse(Errors.NONE))))

    createKafkaApis().handleWriteTxnMarkersRequest(request, requestLocal)

    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None)
    )
    val markersResponse = capturedResponse.getValue
    assertEquals(expectedErrors, markersResponse.errorsByProducerId.get(1L))
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

    when(replicaManager.stopReplicas(
      ArgumentMatchers.eq(request.context.correlationId),
      ArgumentMatchers.eq(controllerId),
      ArgumentMatchers.eq(controllerEpoch),
      ArgumentMatchers.eq(stopReplicaRequest.partitionStates().asScala)
    )).thenReturn(
      (mutable.Map(
        groupMetadataPartition -> Errors.NONE,
        txnStatePartition -> Errors.NONE,
        fooPartition -> Errors.NONE
      ), Errors.NONE)
    )
    when(controller.brokerEpoch).thenReturn(brokerEpoch)

    createKafkaApis().handleStopReplicaRequest(request)

    if (deletePartition) {
      if (leaderEpoch >= 0) {
        verify(txnCoordinator).onResignation(txnStatePartition.partition, Some(leaderEpoch))
        verify(groupCoordinator).onResignation(groupMetadataPartition.partition, Some(leaderEpoch))
      } else {
        verify(txnCoordinator).onResignation(txnStatePartition.partition, None)
        verify(groupCoordinator).onResignation(groupMetadataPartition.partition, None)
      }
    }
  }

  @Test
  def shouldRespondWithUnknownTopicOrPartitionForBadPartitionAndNoErrorsForGoodPartition(): Unit = {
    val tp1 = new TopicPartition("t", 0)
    val tp2 = new TopicPartition("t1", 0)
    val (_, request) = createWriteTxnMarkersRequest(asList(tp1, tp2))
    val expectedErrors = Map(tp1 -> Errors.UNKNOWN_TOPIC_OR_PARTITION, tp2 -> Errors.NONE).asJava

    val capturedResponse: ArgumentCaptor[WriteTxnMarkersResponse] = ArgumentCaptor.forClass(classOf[WriteTxnMarkersResponse])
    val responseCallback: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] = ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])

    when(replicaManager.getMagic(tp1))
      .thenReturn(None)
    when(replicaManager.getMagic(tp2))
      .thenReturn(Some(RecordBatch.MAGIC_VALUE_V2))

    val requestLocal = RequestLocal.withThreadConfinedCaching
    when(replicaManager.appendRecords(anyLong,
      anyShort,
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(AppendOrigin.Coordinator),
      any(),
      responseCallback.capture(),
      any(),
      any(),
      ArgumentMatchers.eq(requestLocal))
    ).thenAnswer(_ => responseCallback.getValue.apply(Map(tp2 -> new PartitionResponse(Errors.NONE))))

    createKafkaApis().handleWriteTxnMarkersRequest(request, requestLocal)
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None)
    )

    val markersResponse = capturedResponse.getValue
    assertEquals(expectedErrors, markersResponse.errorsByProducerId.get(1L))
  }

  @Test
  def shouldAppendToLogOnWriteTxnMarkersWhenCorrectMagicVersion(): Unit = {
    val topicPartition = new TopicPartition("t", 0)
    val request = createWriteTxnMarkersRequest(asList(topicPartition))._2
    when(replicaManager.getMagic(topicPartition))
      .thenReturn(Some(RecordBatch.MAGIC_VALUE_V2))

    val requestLocal = RequestLocal.withThreadConfinedCaching

    createKafkaApis().handleWriteTxnMarkersRequest(request, requestLocal)
    verify(replicaManager).appendRecords(anyLong,
      anyShort,
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(AppendOrigin.Coordinator),
      any(),
      any(),
      any(),
      any(),
      ArgumentMatchers.eq(requestLocal))
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

    reset(groupCoordinator, replicaManager, clientRequestQuotaManager, requestChannel)

    val describeGroupsRequest = new DescribeGroupsRequest.Builder(
      new DescribeGroupsRequestData().setGroups(List(groupId).asJava)
    ).build()
    val request = buildRequest(describeGroupsRequest)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    when(groupCoordinator.handleDescribeGroup(ArgumentMatchers.eq(groupId)))
      .thenReturn((Errors.NONE, groupSummary))

    createKafkaApis().handleDescribeGroupRequest(request)

    val capturedResponse = verifyNoThrottling(request)
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

    reset(groupCoordinator, replicaManager, clientRequestQuotaManager, requestChannel)

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
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    when(groupCoordinator.handleDeleteOffsets(
      ArgumentMatchers.eq(group),
      ArgumentMatchers.eq(Seq(
        new TopicPartition("topic-1", 0),
        new TopicPartition("topic-1", 1),
        new TopicPartition("topic-2", 0),
        new TopicPartition("topic-2", 1)
      )),
      ArgumentMatchers.eq(requestLocal)
    )).thenReturn((Errors.NONE, Map(
      new TopicPartition("topic-1", 0) -> Errors.NONE,
      new TopicPartition("topic-1", 1) -> Errors.NONE,
      new TopicPartition("topic-2", 0) -> Errors.NONE,
      new TopicPartition("topic-2", 1) -> Errors.NONE,
    )))

    createKafkaApis().handleOffsetDeleteRequest(request, requestLocal)

    val capturedResponse = verifyNoThrottling(request)
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
      reset(groupCoordinator, replicaManager, clientRequestQuotaManager, requestChannel)

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
      when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
        any[Long])).thenReturn(0)

      val requestLocal = RequestLocal.withThreadConfinedCaching
      when(groupCoordinator.handleDeleteOffsets(ArgumentMatchers.eq(group), ArgumentMatchers.eq(Seq.empty),
        ArgumentMatchers.eq(requestLocal))).thenReturn((Errors.NONE, Map.empty[TopicPartition, Errors]))

      createKafkaApis().handleOffsetDeleteRequest(request, requestLocal)

      val capturedResponse = verifyNoThrottling(request)
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

    reset(groupCoordinator, replicaManager, clientRequestQuotaManager, requestChannel)

    val offsetDeleteRequest = new OffsetDeleteRequest.Builder(
      new OffsetDeleteRequestData()
        .setGroupId(group)
    ).build()
    val request = buildRequest(offsetDeleteRequest)

    val requestLocal = RequestLocal.withThreadConfinedCaching
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    when(groupCoordinator.handleDeleteOffsets(ArgumentMatchers.eq(group), ArgumentMatchers.eq(Seq.empty),
      ArgumentMatchers.eq(requestLocal))).thenReturn((Errors.GROUP_ID_NOT_FOUND, Map.empty[TopicPartition, Errors]))

    createKafkaApis().handleOffsetDeleteRequest(request, requestLocal)

    val capturedResponse = verifyNoThrottling(request)
    val response = capturedResponse.getValue.asInstanceOf[OffsetDeleteResponse]

    assertEquals(Errors.GROUP_ID_NOT_FOUND, Errors.forCode(response.data.errorCode))
  }

  private def testListOffsetFailedGetLeaderReplica(error: Errors): Unit = {
    val tp = new TopicPartition("foo", 0)
    val isolationLevel = IsolationLevel.READ_UNCOMMITTED
    val currentLeaderEpoch = Optional.of[Integer](15)

    when(replicaManager.fetchOffsetForTimestamp(
      ArgumentMatchers.eq(tp),
      ArgumentMatchers.eq(ListOffsetsRequest.EARLIEST_TIMESTAMP),
      ArgumentMatchers.eq(Some(isolationLevel)),
      ArgumentMatchers.eq(currentLeaderEpoch),
      fetchOnlyFromLeader = ArgumentMatchers.eq(true))
    ).thenThrow(error.exception)

    val targetTimes = List(new ListOffsetsTopic()
      .setName(tp.topic)
      .setPartitions(List(new ListOffsetsPartition()
        .setPartitionIndex(tp.partition)
        .setTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP)
        .setCurrentLeaderEpoch(currentLeaderEpoch.get)).asJava)).asJava
    val listOffsetRequest = ListOffsetsRequest.Builder.forConsumer(true, isolationLevel, false)
      .setTargetTimes(targetTimes).build()
    val request = buildRequest(listOffsetRequest)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    createKafkaApis().handleListOffsetRequest(request)

    val capturedResponse = verifyNoThrottling(request)
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
  def testGetAllTopicMetadataShouldNotCreateTopicOrReturnUnknownTopicPartition(): Unit = {
    // Setup: authorizer authorizes 2 topics, but one got deleted in metadata cache
    metadataCache = mock(classOf[ZkMetadataCache])
    when(metadataCache.getAliveBrokerNodes(any())).thenReturn(List(new Node(brokerId,"localhost", 0)))
    when(metadataCache.getControllerId).thenReturn(None)

    // 2 topics returned for authorization in during handle
    val topicsReturnedFromMetadataCacheForAuthorization = Set("remaining-topic", "later-deleted-topic")
    when(metadataCache.getAllTopics()).thenReturn(topicsReturnedFromMetadataCacheForAuthorization)
    // 1 topic is deleted from metadata right at the time between authorization and the next getTopicMetadata() call
    when(metadataCache.getTopicMetadata(
      ArgumentMatchers.eq(topicsReturnedFromMetadataCacheForAuthorization),
      any[ListenerName],
      anyBoolean,
      anyBoolean
    )).thenReturn(Seq(
      new MetadataResponseTopic()
        .setErrorCode(Errors.NONE.code)
        .setName("remaining-topic")
        .setIsInternal(false)
    ))


    var createTopicIsCalled: Boolean = false
    // Specific mock on zkClient for this use case
    // Expect it's never called to do auto topic creation
    when(zkClient.setOrCreateEntityConfigs(
      ArgumentMatchers.eq(ConfigType.Topic),
      anyString,
      any[Properties]
    )).thenAnswer(_ => {
      createTopicIsCalled = true
    })
    // No need to use
    when(zkClient.getAllBrokersInCluster)
      .thenReturn(Seq(new Broker(
        brokerId, "localhost", 9902,
        ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT), SecurityProtocol.PLAINTEXT
      )))


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
    val authorizer: Authorizer = mock(classOf[Authorizer])
    val unauthorizedTopic = "unauthorized-topic"
    val authorizedTopic = "authorized-topic"

    val expectedActions = Seq(
      new Action(AclOperation.DESCRIBE, new ResourcePattern(ResourceType.TOPIC, unauthorizedTopic, PatternType.LITERAL), 1, true, true),
      new Action(AclOperation.DESCRIBE, new ResourcePattern(ResourceType.TOPIC, authorizedTopic, PatternType.LITERAL), 1, true, true)
    )

    // Here we need to use AuthHelperTest.matchSameElements instead of EasyMock.eq since the order of the request is unknown
    when(authorizer.authorize(any[RequestContext], argThat((t: java.util.List[Action]) => t.containsAll(expectedActions.asJava))))
      .thenAnswer { invocation =>
      val actions = invocation.getArgument(1).asInstanceOf[util.List[Action]].asScala
      actions.map { action =>
        if (action.resourcePattern().name().equals(authorizedTopic))
          AuthorizationResult.ALLOWED
        else
          AuthorizationResult.DENIED
      }.asJava
    }

    // 3. Set up MetadataCache
    val authorizedTopicId = Uuid.randomUuid()
    val unauthorizedTopicId = Uuid.randomUuid()

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
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    createKafkaApis(authorizer = Some(authorizer)).handleTopicMetadataRequest(repByTopicId)
    val capturedMetadataByTopicIdResp = verifyNoThrottling(repByTopicId)
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
    reset(clientRequestQuotaManager, requestChannel)
    val metadataReqByTopicName = new MetadataRequest.Builder(util.Arrays.asList(authorizedTopic, unauthorizedTopic), false).build()
    val repByTopicName = buildRequest(metadataReqByTopicName, plaintextListener)

    createKafkaApis(authorizer = Some(authorizer)).handleTopicMetadataRequest(repByTopicName)
    val capturedMetadataByTopicNameResp = verifyNoThrottling(repByTopicName)
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
    val tidp = new TopicIdPartition(Uuid.ZERO_UUID, new TopicPartition("foo", 0))
    val tp = tidp.topicPartition
    addTopicToMetadataCache(tp.topic, numPartitions = 1)
    val hw = 3
    val timestamp = 1000

    when(replicaManager.getLogConfig(ArgumentMatchers.eq(tp))).thenReturn(None)

    when(replicaManager.fetchMessages(anyLong, anyInt, anyInt, anyInt, anyBoolean,
      any[Seq[(TopicIdPartition, FetchRequest.PartitionData)]], any[ReplicaQuota],
      any[Seq[(TopicIdPartition, FetchPartitionData)] => Unit](), any[IsolationLevel],
      any[Option[ClientMetadata]])
    ).thenAnswer(invocation => {
      val callback = invocation.getArgument(7).asInstanceOf[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]
      val records = MemoryRecords.withRecords(CompressionType.NONE,
        new SimpleRecord(timestamp, "foo".getBytes(StandardCharsets.UTF_8)))
      callback(Seq(tidp -> FetchPartitionData(Errors.NONE, hw, 0, records,
        None, None, None, Option.empty, isReassignmentFetch = false)))
    })

    val fetchData = Map(tidp -> new FetchRequest.PartitionData(Uuid.ZERO_UUID, 0, 0, 1000,
      Optional.empty())).asJava
    val fetchDataBuilder = Map(tp -> new FetchRequest.PartitionData(Uuid.ZERO_UUID, 0, 0, 1000,
      Optional.empty())).asJava
    val fetchMetadata = new JFetchMetadata(0, 0)
    val fetchContext = new FullFetchContext(time, new FetchSessionCache(1000, 100),
      fetchMetadata, fetchData, false, false)
    when(fetchManager.newContext(
      any[Short],
      any[JFetchMetadata],
      any[Boolean],
      any[util.Map[TopicIdPartition, FetchRequest.PartitionData]],
      any[util.List[TopicIdPartition]],
      any[util.Map[Uuid, String]])).thenReturn(fetchContext)

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val fetchRequest = new FetchRequest.Builder(9, 9, -1, 100, 0, fetchDataBuilder)
      .build()
    val request = buildRequest(fetchRequest)

    createKafkaApis().handleFetchRequest(request)

    val capturedResponse = verifyNoThrottling(request)
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

  /**
   * Verifies that partitions with unknown topic ID errors are added to the erroneous set and there is not an attempt to fetch them.
   */
  @ParameterizedTest
  @ValueSource(ints = Array(-1, 0))
  def testFetchRequestErroneousPartitions(replicaId: Int): Unit = {
    val foo = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0))
    val unresolvedFoo = new TopicIdPartition(foo.topicId, new TopicPartition(null, foo.partition))

    addTopicToMetadataCache(foo.topic, 1, topicId = foo.topicId)

    // We will never return a logConfig when the topic name is null. This is ok since we won't have any records to convert.
    when(replicaManager.getLogConfig(ArgumentMatchers.eq(unresolvedFoo.topicPartition))).thenReturn(None)

    // Simulate unknown topic ID in the context
    val fetchData = Map(new TopicIdPartition(foo.topicId, new TopicPartition(null, foo.partition)) ->
      new FetchRequest.PartitionData(foo.topicId, 0, 0, 1000, Optional.empty())).asJava
    val fetchDataBuilder = Map(foo.topicPartition -> new FetchRequest.PartitionData(foo.topicId, 0, 0, 1000,
      Optional.empty())).asJava
    val fetchMetadata = new JFetchMetadata(0, 0)
    val fetchContext = new FullFetchContext(time, new FetchSessionCache(1000, 100),
      fetchMetadata, fetchData, true, replicaId >= 0)
    // We expect to have the resolved partition, but we will simulate an unknown one with the fetchContext we return.
    when(fetchManager.newContext(
      ApiKeys.FETCH.latestVersion,
      fetchMetadata,
      replicaId >= 0,
      Collections.singletonMap(foo, new FetchRequest.PartitionData(foo.topicId, 0, 0, 1000, Optional.empty())),
      Collections.emptyList[TopicIdPartition],
      metadataCache.topicIdsToNames())
    ).thenReturn(fetchContext)

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    // If replicaId is -1 we will build a consumer request. Any non-negative replicaId will build a follower request.
    val fetchRequest = new FetchRequest.Builder(ApiKeys.FETCH.latestVersion, ApiKeys.FETCH.latestVersion,
      replicaId, 100, 0, fetchDataBuilder).metadata(fetchMetadata).build()
    val request = buildRequest(fetchRequest)

    createKafkaApis().handleFetchRequest(request)

    val capturedResponse = verifyNoThrottling(request)
    val response = capturedResponse.getValue.asInstanceOf[FetchResponse]
    val responseData = response.responseData(metadataCache.topicIdsToNames(), ApiKeys.FETCH.latestVersion)
    assertTrue(responseData.containsKey(foo.topicPartition))

    val partitionData = responseData.get(foo.topicPartition)
    assertEquals(Errors.UNKNOWN_TOPIC_ID.code, partitionData.errorCode)
    assertEquals(-1, partitionData.highWatermark)
    assertEquals(-1, partitionData.lastStableOffset)
    assertEquals(-1, partitionData.logStartOffset)
    assertEquals(MemoryRecords.EMPTY, FetchResponse.recordsOrFail(partitionData))
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
    val capturedProtocols: ArgumentCaptor[List[(String, Array[Byte])]] = ArgumentCaptor.forClass(classOf[List[(String, Array[Byte])]])

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

    verify(groupCoordinator).handleJoinGroup(
      ArgumentMatchers.eq(groupId),
      ArgumentMatchers.eq(memberId),
      ArgumentMatchers.eq(None),
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(clientId),
      ArgumentMatchers.eq(InetAddress.getLocalHost.toString),
      ArgumentMatchers.eq(rebalanceTimeoutMs),
      ArgumentMatchers.eq(sessionTimeoutMs),
      ArgumentMatchers.eq(protocolType),
      capturedProtocols.capture(),
      any(),
      any(),
      any()
    )
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
    reset(groupCoordinator, clientRequestQuotaManager, requestChannel, replicaManager)

    val groupId = "group"
    val memberId = "member1"
    val protocolType = "consumer"
    val rebalanceTimeoutMs = 10
    val sessionTimeoutMs = 5

    val capturedCallback: ArgumentCaptor[JoinGroupCallback] = ArgumentCaptor.forClass(classOf[JoinGroupCallback])

    val joinGroupRequest = new JoinGroupRequest.Builder(
      new JoinGroupRequestData()
        .setGroupId(groupId)
        .setMemberId(memberId)
        .setProtocolType(protocolType)
        .setRebalanceTimeoutMs(rebalanceTimeoutMs)
        .setSessionTimeoutMs(sessionTimeoutMs)
    ).build(version)

    val requestChannelRequest = buildRequest(joinGroupRequest)

    createKafkaApis().handleJoinGroupRequest(requestChannelRequest, RequestLocal.withThreadConfinedCaching)

    verify(groupCoordinator).handleJoinGroup(
      ArgumentMatchers.eq(groupId),
      ArgumentMatchers.eq(memberId),
      ArgumentMatchers.eq(None),
      ArgumentMatchers.eq(if (version >= 4) true else false),
      ArgumentMatchers.eq(if (version >= 9) true else false),
      ArgumentMatchers.eq(clientId),
      ArgumentMatchers.eq(InetAddress.getLocalHost.toString),
      ArgumentMatchers.eq(if (version >= 1) rebalanceTimeoutMs else sessionTimeoutMs),
      ArgumentMatchers.eq(sessionTimeoutMs),
      ArgumentMatchers.eq(protocolType),
      ArgumentMatchers.eq(List.empty),
      capturedCallback.capture(),
      any(),
      any()
    )
    capturedCallback.getValue.apply(JoinGroupResult(memberId, Errors.INCONSISTENT_GROUP_PROTOCOL))

    val capturedResponse = verifyNoThrottling(requestChannelRequest)
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
  }

  @Test
  def testJoinGroupProtocolType(): Unit = {
    for (version <- ApiKeys.JOIN_GROUP.oldestVersion to ApiKeys.JOIN_GROUP.latestVersion) {
      testJoinGroupProtocolType(version.asInstanceOf[Short])
    }
  }

  def testJoinGroupProtocolType(version: Short): Unit = {
    reset(groupCoordinator, clientRequestQuotaManager, requestChannel, replicaManager)

    val groupId = "group"
    val memberId = "member1"
    val protocolType = "consumer"
    val protocolName = "range"
    val rebalanceTimeoutMs = 10
    val sessionTimeoutMs = 5

    val capturedCallback: ArgumentCaptor[JoinGroupCallback] = ArgumentCaptor.forClass(classOf[JoinGroupCallback])

    val joinGroupRequest = new JoinGroupRequest.Builder(
      new JoinGroupRequestData()
        .setGroupId(groupId)
        .setMemberId(memberId)
        .setProtocolType(protocolType)
        .setRebalanceTimeoutMs(rebalanceTimeoutMs)
        .setSessionTimeoutMs(sessionTimeoutMs)
    ).build(version)

    val requestChannelRequest = buildRequest(joinGroupRequest)

    createKafkaApis().handleJoinGroupRequest(requestChannelRequest, RequestLocal.withThreadConfinedCaching)

    verify(groupCoordinator).handleJoinGroup(
      ArgumentMatchers.eq(groupId),
      ArgumentMatchers.eq(memberId),
      ArgumentMatchers.eq(None),
      ArgumentMatchers.eq(if (version >= 4) true else false),
      ArgumentMatchers.eq(if (version >= 9) true else false),
      ArgumentMatchers.eq(clientId),
      ArgumentMatchers.eq(InetAddress.getLocalHost.toString),
      ArgumentMatchers.eq(if (version >= 1) rebalanceTimeoutMs else sessionTimeoutMs),
      ArgumentMatchers.eq(sessionTimeoutMs),
      ArgumentMatchers.eq(protocolType),
      ArgumentMatchers.eq(List.empty),
      capturedCallback.capture(),
      any(),
      any()
    )
    capturedCallback.getValue.apply(JoinGroupResult(
      members = List.empty,
      memberId = memberId,
      generationId = 0,
      protocolType = Some(protocolType),
      protocolName = Some(protocolName),
      leaderId = memberId,
      skipAssignment = true,
      error = Errors.NONE
    ))
    val capturedResponse = verifyNoThrottling(requestChannelRequest)
    val response = capturedResponse.getValue.asInstanceOf[JoinGroupResponse]

    assertEquals(Errors.NONE, response.error)
    assertEquals(0, response.data.members.size)
    assertEquals(memberId, response.data.memberId)
    assertEquals(0, response.data.generationId)
    assertEquals(memberId, response.data.leader)
    assertEquals(protocolName, response.data.protocolName)
    assertEquals(protocolType, response.data.protocolType)
    assertTrue(response.data.skipAssignment)
  }

  @Test
  def testSyncGroupProtocolTypeAndName(): Unit = {
    for (version <- ApiKeys.SYNC_GROUP.oldestVersion to ApiKeys.SYNC_GROUP.latestVersion) {
      testSyncGroupProtocolTypeAndName(version.asInstanceOf[Short])
    }
  }

  def testSyncGroupProtocolTypeAndName(version: Short): Unit = {
    reset(groupCoordinator, clientRequestQuotaManager, requestChannel, replicaManager)

    val groupId = "group"
    val memberId = "member1"
    val protocolType = "consumer"
    val protocolName = "range"

    val capturedCallback: ArgumentCaptor[SyncGroupCallback] = ArgumentCaptor.forClass(classOf[SyncGroupCallback])

    val requestLocal = RequestLocal.withThreadConfinedCaching
    val syncGroupRequest = new SyncGroupRequest.Builder(
      new SyncGroupRequestData()
        .setGroupId(groupId)
        .setGenerationId(0)
        .setMemberId(memberId)
        .setProtocolType(protocolType)
        .setProtocolName(protocolName)
    ).build(version)

    val requestChannelRequest = buildRequest(syncGroupRequest)

    createKafkaApis().handleSyncGroupRequest(requestChannelRequest, requestLocal)

    verify(groupCoordinator).handleSyncGroup(
      ArgumentMatchers.eq(groupId),
      ArgumentMatchers.eq(0),
      ArgumentMatchers.eq(memberId),
      ArgumentMatchers.eq(if (version >= 5) Some(protocolType) else None),
      ArgumentMatchers.eq(if (version >= 5) Some(protocolName) else None),
      ArgumentMatchers.eq(None),
      ArgumentMatchers.eq(Map.empty),
      capturedCallback.capture(),
      ArgumentMatchers.eq(requestLocal)
    )
    capturedCallback.getValue.apply(SyncGroupResult(
      protocolType = Some(protocolType),
      protocolName = Some(protocolName),
      memberAssignment = Array.empty,
      error = Errors.NONE
    ))

    val capturedResponse = verifyNoThrottling(requestChannelRequest)
    val response = capturedResponse.getValue.asInstanceOf[SyncGroupResponse]

    assertEquals(Errors.NONE, response.error)
    assertArrayEquals(Array.empty[Byte], response.data.assignment)
    assertEquals(protocolType, response.data.protocolType)
  }

  @Test
  def testSyncGroupProtocolTypeAndNameAreMandatorySinceV5(): Unit = {
    for (version <- ApiKeys.SYNC_GROUP.oldestVersion to ApiKeys.SYNC_GROUP.latestVersion) {
      testSyncGroupProtocolTypeAndNameAreMandatorySinceV5(version.asInstanceOf[Short])
    }
  }

  def testSyncGroupProtocolTypeAndNameAreMandatorySinceV5(version: Short): Unit = {
    reset(groupCoordinator, clientRequestQuotaManager, requestChannel, replicaManager)

    val groupId = "group"
    val memberId = "member1"
    val protocolType = "consumer"
    val protocolName = "range"

    val capturedCallback: ArgumentCaptor[SyncGroupCallback] = ArgumentCaptor.forClass(classOf[SyncGroupCallback])

    val requestLocal = RequestLocal.withThreadConfinedCaching

    val syncGroupRequest = new SyncGroupRequest.Builder(
      new SyncGroupRequestData()
        .setGroupId(groupId)
        .setGenerationId(0)
        .setMemberId(memberId)
    ).build(version)

    val requestChannelRequest = buildRequest(syncGroupRequest)

    createKafkaApis().handleSyncGroupRequest(requestChannelRequest, requestLocal)

    if (version < 5) {
      verify(groupCoordinator).handleSyncGroup(
        ArgumentMatchers.eq(groupId),
        ArgumentMatchers.eq(0),
        ArgumentMatchers.eq(memberId),
        ArgumentMatchers.eq(None),
        ArgumentMatchers.eq(None),
        ArgumentMatchers.eq(None),
        ArgumentMatchers.eq(Map.empty),
        capturedCallback.capture(),
        ArgumentMatchers.eq(requestLocal))
      capturedCallback.getValue.apply(SyncGroupResult(
        protocolType = Some(protocolType),
        protocolName = Some(protocolName),
        memberAssignment = Array.empty,
        error = Errors.NONE
      ))
    }

    val capturedResponse = verifyNoThrottling(requestChannelRequest)
    val response = capturedResponse.getValue.asInstanceOf[SyncGroupResponse]

    if (version < 5) {
      assertEquals(Errors.NONE, response.error)
    } else {
      assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL, response.error)
    }
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

    createKafkaApis(KAFKA_2_2_IV1).handleJoinGroupRequest(requestChannelRequest, RequestLocal.withThreadConfinedCaching)

    val capturedResponse = verifyNoThrottling(requestChannelRequest)
    val response = capturedResponse.getValue.asInstanceOf[JoinGroupResponse]
    assertEquals(Errors.UNSUPPORTED_VERSION, response.error())
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

    createKafkaApis(KAFKA_2_2_IV1).handleSyncGroupRequest(requestChannelRequest, RequestLocal.withThreadConfinedCaching)

    val capturedResponse = verifyNoThrottling(requestChannelRequest)
    val response = capturedResponse.getValue.asInstanceOf[SyncGroupResponse]
    assertEquals(Errors.UNSUPPORTED_VERSION, response.error)
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

    createKafkaApis(KAFKA_2_2_IV1).handleHeartbeatRequest(requestChannelRequest)

    val capturedResponse = verifyNoThrottling(requestChannelRequest)
    val response = capturedResponse.getValue.asInstanceOf[HeartbeatResponse]
    assertEquals(Errors.UNSUPPORTED_VERSION, response.error())
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
    val capturedResponse = verifyNoThrottling(requestChannelRequest)
    val response = capturedResponse.getValue.asInstanceOf[OffsetCommitResponse]
    assertEquals(expectedTopicErrors, response.data.topics())
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

    val leaveRequest = buildRequest(
      new LeaveGroupRequest.Builder(
        groupId,
        leaveMemberList.asJava
      ).build()
    )

    createKafkaApis().handleLeaveGroupRequest(leaveRequest)
    verify(groupCoordinator).handleLeaveGroup(
      ArgumentMatchers.eq(groupId),
      ArgumentMatchers.eq(leaveMemberList),
      any()
    )
  }

  @Test
  def testSingleLeaveGroup(): Unit = {
    val groupId = "groupId"
    val memberId = "member"

    val singleLeaveMember = List(
      new MemberIdentity()
        .setMemberId(memberId)
    )

    val leaveRequest = buildRequest(
      new LeaveGroupRequest.Builder(
        groupId,
        singleLeaveMember.asJava
      ).build()
    )

    createKafkaApis().handleLeaveGroupRequest(leaveRequest)
    verify(groupCoordinator).handleLeaveGroup(
      ArgumentMatchers.eq(groupId),
      ArgumentMatchers.eq(singleLeaveMember),
      any()
    )
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
    val topicId = Uuid.randomUuid()
    val tidp0 = new TopicIdPartition(topicId, tp0)

    setupBasicMetadataCache(tp0.topic, numPartitions = 1, 1, topicId)
    val hw = 3

    val fetchDataBuilder = Collections.singletonMap(tp0, new FetchRequest.PartitionData(Uuid.ZERO_UUID, 0, 0, Int.MaxValue, Optional.of(leaderEpoch)))
    val fetchData = Collections.singletonMap(tidp0, new FetchRequest.PartitionData(Uuid.ZERO_UUID, 0, 0, Int.MaxValue, Optional.of(leaderEpoch)))
    val fetchFromFollower = buildRequest(new FetchRequest.Builder(
      ApiKeys.FETCH.oldestVersion(), ApiKeys.FETCH.latestVersion(), 1, 1000, 0, fetchDataBuilder).build())

    val records = MemoryRecords.withRecords(CompressionType.NONE,
      new SimpleRecord(1000, "foo".getBytes(StandardCharsets.UTF_8)))
    when(replicaManager.fetchMessages(anyLong, anyInt, anyInt, anyInt, anyBoolean,
      any[Seq[(TopicIdPartition, FetchRequest.PartitionData)]], any[ReplicaQuota],
      any[Seq[(TopicIdPartition, FetchPartitionData)] => Unit](), any[IsolationLevel],
      any[Option[ClientMetadata]])
    ).thenAnswer(invocation => {
      val callback = invocation.getArgument(7).asInstanceOf[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]
      callback(Seq(tidp0 -> FetchPartitionData(Errors.NONE, hw, 0, records,
        None, None, None, Option.empty, isReassignmentFetch = isReassigning)))
    })

    val fetchMetadata = new JFetchMetadata(0, 0)
    val fetchContext = new FullFetchContext(time, new FetchSessionCache(1000, 100),
      fetchMetadata, fetchData, true, true)
    when(fetchManager.newContext(
      any[Short],
      any[JFetchMetadata],
      any[Boolean],
      any[util.Map[TopicIdPartition, FetchRequest.PartitionData]],
      any[util.List[TopicIdPartition]],
      any[util.Map[Uuid, String]])).thenReturn(fetchContext)

    when(replicaManager.getLogConfig(ArgumentMatchers.eq(tp0))).thenReturn(None)
    when(replicaManager.isAddingReplica(any(), anyInt)).thenReturn(isReassigning)

    createKafkaApis().handle(fetchFromFollower, RequestLocal.withThreadConfinedCaching)
    verify(replicaQuotaManager).record(anyLong)

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

    createKafkaApis(KAFKA_2_2_IV1).handleInitProducerIdRequest(requestChannelRequest, RequestLocal.withThreadConfinedCaching)

    val capturedResponse = verifyNoThrottling(requestChannelRequest)
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

    createKafkaApis(KAFKA_2_2_IV1).handleInitProducerIdRequest(requestChannelRequest, RequestLocal.withThreadConfinedCaching)

    val capturedResponse = verifyNoThrottling(requestChannelRequest)
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

    val capturedResponse: ArgumentCaptor[UpdateMetadataResponse] = ArgumentCaptor.forClass(classOf[UpdateMetadataResponse])

    when(controller.brokerEpoch).thenReturn(currentBrokerEpoch)
    when(replicaManager.maybeUpdateMetadataCache(
      ArgumentMatchers.eq(request.context.correlationId),
      any()
    )).thenReturn(
      Seq()
    )

    createKafkaApis().handleUpdateMetadataRequest(request, RequestLocal.withThreadConfinedCaching)
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None)
    )
    val updateMetadataResponse = capturedResponse.getValue
    assertEquals(expectedError, updateMetadataResponse.error())
    if (expectedError == Errors.NONE) {
      verify(replicaManager).maybeUpdateMetadataCache(
        ArgumentMatchers.eq(request.context.correlationId),
        any()
      )
    }
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
    val capturedResponse: ArgumentCaptor[LeaderAndIsrResponse] = ArgumentCaptor.forClass(classOf[LeaderAndIsrResponse])
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

    when(controller.brokerEpoch).thenReturn(currentBrokerEpoch)
    when(replicaManager.becomeLeaderOrFollower(
      ArgumentMatchers.eq(request.context.correlationId),
      any(),
      any()
    )).thenReturn(
      response
    )

    createKafkaApis().handleLeaderAndIsrRequest(request)
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None)
    )
    val leaderAndIsrResponse = capturedResponse.getValue
    assertEquals(expectedError, leaderAndIsrResponse.error())
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
    val capturedResponse: ArgumentCaptor[StopReplicaResponse] = ArgumentCaptor.forClass(classOf[StopReplicaResponse])
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

    when(controller.brokerEpoch).thenReturn(currentBrokerEpoch)
    when(replicaManager.stopReplicas(
      ArgumentMatchers.eq(request.context.correlationId),
      ArgumentMatchers.eq(controllerId),
      ArgumentMatchers.eq(controllerEpoch),
      ArgumentMatchers.eq(stopReplicaRequest.partitionStates().asScala)
    )).thenReturn(
      (mutable.Map(
        fooPartition -> Errors.NONE
      ), Errors.NONE)
    )

    createKafkaApis().handleStopReplicaRequest(request)
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None)
    )
    val stopReplicaResponse = capturedResponse.getValue
    assertEquals(expectedError, stopReplicaResponse.error())
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
    reset(groupCoordinator, clientRequestQuotaManager, requestChannel)

    val data = new ListGroupsRequestData()
    if (state.isDefined)
      data.setStatesFilter(Collections.singletonList(state.get))
    val listGroupsRequest = new ListGroupsRequest.Builder(data).build()
    val requestChannelRequest = buildRequest(listGroupsRequest)

    val expectedStates: Set[String] = if (state.isDefined) Set(state.get) else Set()
    when(groupCoordinator.handleListGroups(expectedStates))
      .thenReturn((Errors.NONE, overviews))

    createKafkaApis().handleListGroupsRequest(requestChannelRequest)

    val capturedResponse = verifyNoThrottling(requestChannelRequest)
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

    createKafkaApis().handleDescribeCluster(request)

    val capturedResponse = verifyNoThrottling(request)
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

    createKafkaApis().handleTopicMetadataRequest(requestChannelRequest)

    val capturedResponse = verifyNoThrottling(requestChannelRequest)
    capturedResponse.getValue.asInstanceOf[MetadataResponse]
  }

  private def testConsumerListOffsetLatest(isolationLevel: IsolationLevel): Unit = {
    val tp = new TopicPartition("foo", 0)
    val latestOffset = 15L
    val currentLeaderEpoch = Optional.empty[Integer]()

    when(replicaManager.fetchOffsetForTimestamp(
      ArgumentMatchers.eq(tp),
      ArgumentMatchers.eq(ListOffsetsRequest.LATEST_TIMESTAMP),
      ArgumentMatchers.eq(Some(isolationLevel)),
      ArgumentMatchers.eq(currentLeaderEpoch),
      fetchOnlyFromLeader = ArgumentMatchers.eq(true))
    ).thenReturn(Some(new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, latestOffset, currentLeaderEpoch)))

    val targetTimes = List(new ListOffsetsTopic()
      .setName(tp.topic)
      .setPartitions(List(new ListOffsetsPartition()
        .setPartitionIndex(tp.partition)
        .setTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP)).asJava)).asJava
    val listOffsetRequest = ListOffsetsRequest.Builder.forConsumer(true, isolationLevel, false)
      .setTargetTimes(targetTimes).build()
    val request = buildRequest(listOffsetRequest)

    createKafkaApis().handleListOffsetRequest(request)

    val capturedResponse = verifyNoThrottling(request)
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

  private def verifyNoThrottling(request: RequestChannel.Request): ArgumentCaptor[AbstractResponse] = {
    val capturedResponse: ArgumentCaptor[AbstractResponse] = ArgumentCaptor.forClass(classOf[AbstractResponse])
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      any()
    )
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
    val updateMetadataRequest = createBasicMetadataRequest(topic, numPartitions, 0, numBrokers, topicId)
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

    reset(replicaManager, clientRequestQuotaManager, requestChannel)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    val t0p0 = new TopicPartition("t0", 0)
    val t0p1 = new TopicPartition("t0", 1)
    val t0p2 = new TopicPartition("t0", 2)
    val partitionResults = Map(
      t0p0 -> Errors.NONE,
      t0p1 -> Errors.LOG_DIR_NOT_FOUND,
      t0p2 -> Errors.INVALID_TOPIC_EXCEPTION)
    when(replicaManager.alterReplicaLogDirs(ArgumentMatchers.eq(Map(
      t0p0 -> "/foo",
      t0p1 -> "/foo",
      t0p2 -> "/foo"))))
    .thenReturn(partitionResults)

    createKafkaApis().handleAlterReplicaLogDirsRequest(request)

    val capturedResponse = verifyNoThrottling(request)
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
    def fetchResponse(data: Map[TopicIdPartition, String]): FetchResponse = {
      val responseData = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData](
        data.map { case (tp, raw) =>
          tp -> new FetchResponseData.PartitionData()
            .setPartitionIndex(tp.topicPartition.partition)
            .setHighWatermark(105)
            .setLastStableOffset(105)
            .setLogStartOffset(0)
            .setRecords(MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord(100, raw.getBytes(StandardCharsets.UTF_8))))
      }.toMap.asJava)

      data.foreach{case (tp, _) =>
        topicIds.put(tp.topicPartition.topic, tp.topicId)
        topicNames.put(tp.topicId, tp.topicPartition.topic)
      }
      FetchResponse.of(Errors.NONE, 100, 100, responseData)
    }

    val throttledPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("throttledData", 0))
    val throttledData = Map(throttledPartition -> "throttledData")
    val expectedSize = FetchResponse.sizeOf(FetchResponseData.HIGHEST_SUPPORTED_VERSION,
      fetchResponse(throttledData).responseData(topicNames, FetchResponseData.HIGHEST_SUPPORTED_VERSION).entrySet.asScala.map( entry =>
      (new TopicIdPartition(Uuid.ZERO_UUID, entry.getKey), entry.getValue)).toMap.asJava.entrySet.iterator)

    val response = fetchResponse(throttledData ++ Map(new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("nonThrottledData", 0)) -> "nonThrottledData"))

    val quota = Mockito.mock(classOf[ReplicationQuotaManager])
    Mockito.when(quota.isThrottled(ArgumentMatchers.any(classOf[TopicPartition])))
      .thenAnswer(invocation => throttledPartition.topicPartition == invocation.getArgument(0).asInstanceOf[TopicPartition])

    assertEquals(expectedSize, KafkaApis.sizeOfThrottledPartitions(FetchResponseData.HIGHEST_SUPPORTED_VERSION, response, quota))
  }

  @Test
  def testDescribeProducers(): Unit = {
    val tp1 = new TopicPartition("foo", 0)
    val tp2 = new TopicPartition("bar", 3)
    val tp3 = new TopicPartition("baz", 1)
    val tp4 = new TopicPartition("invalid;topic", 1)

    val authorizer: Authorizer = mock(classOf[Authorizer])
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
    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(buildExpectedActions(tp1.topic))))
      .thenReturn(Seq(AuthorizationResult.ALLOWED).asJava)

    // Topic `bar` is not authorized
    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(buildExpectedActions(tp2.topic))))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava)

    // Topic `baz` is authorized, but not present in the metadata
    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(buildExpectedActions(tp3.topic))))
      .thenReturn(Seq(AuthorizationResult.ALLOWED).asJava)

    when(replicaManager.activeProducerState(tp1))
      .thenReturn(new DescribeProducersResponseData.PartitionResponse()
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
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    createKafkaApis(authorizer = Some(authorizer)).handleDescribeProducersRequest(request)

    val capturedResponse = verifyNoThrottling(request)
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
    val authorizer: Authorizer = mock(classOf[Authorizer])
    val data = new DescribeTransactionsRequestData()
      .setTransactionalIds(List("foo", "bar").asJava)
    val describeTransactionsRequest = new DescribeTransactionsRequest.Builder(data).build()
    val request = buildRequest(describeTransactionsRequest)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    def buildExpectedActions(transactionalId: String): util.List[Action] = {
      val pattern = new ResourcePattern(ResourceType.TRANSACTIONAL_ID, transactionalId, PatternType.LITERAL)
      val action = new Action(AclOperation.DESCRIBE, pattern, 1, true, true)
      Collections.singletonList(action)
    }

    when(txnCoordinator.handleDescribeTransactions("foo"))
      .thenReturn(new DescribeTransactionsResponseData.TransactionState()
        .setErrorCode(Errors.NONE.code)
        .setTransactionalId("foo")
        .setProducerId(12345L)
        .setProducerEpoch(15)
        .setTransactionStartTimeMs(time.milliseconds())
        .setTransactionState("CompleteCommit")
        .setTransactionTimeoutMs(10000))

    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(buildExpectedActions("foo"))))
      .thenReturn(Seq(AuthorizationResult.ALLOWED).asJava)

    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(buildExpectedActions("bar"))))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava)

    createKafkaApis(authorizer = Some(authorizer)).handleDescribeTransactionsRequest(request)

    val capturedResponse = verifyNoThrottling(request)
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
    val authorizer: Authorizer = mock(classOf[Authorizer])
    val transactionalId = "foo"
    val data = new DescribeTransactionsRequestData()
      .setTransactionalIds(List(transactionalId).asJava)
    val describeTransactionsRequest = new DescribeTransactionsRequest.Builder(data).build()
    val request = buildRequest(describeTransactionsRequest)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    def expectDescribe(
      resourceType: ResourceType,
      transactionalId: String,
      result: AuthorizationResult
    ): Unit = {
      val pattern = new ResourcePattern(resourceType, transactionalId, PatternType.LITERAL)
      val action = new Action(AclOperation.DESCRIBE, pattern, 1, true, true)
      val actions = Collections.singletonList(action)

      when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(actions)))
        .thenReturn(Seq(result).asJava)
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

    when(txnCoordinator.handleDescribeTransactions("foo"))
      .thenReturn(describeTransactionsResponse)

    createKafkaApis(authorizer = Some(authorizer)).handleDescribeTransactionsRequest(request)

    val capturedResponse = verifyNoThrottling(request)
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
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    when(txnCoordinator.handleListTransactions(Set.empty[Long], Set.empty[String]))
      .thenReturn(new ListTransactionsResponseData()
        .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code))

    createKafkaApis().handleListTransactionsRequest(request)

    val capturedResponse = verifyNoThrottling(request)
    val response = capturedResponse.getValue.asInstanceOf[ListTransactionsResponse]
    assertEquals(0, response.data.transactionStates.size)
    assertEquals(Errors.COORDINATOR_LOAD_IN_PROGRESS, Errors.forCode(response.data.errorCode))
  }

  @Test
  def testListTransactionsAuthorization(): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])
    val data = new ListTransactionsRequestData()
    val listTransactionsRequest = new ListTransactionsRequest.Builder(data).build()
    val request = buildRequest(listTransactionsRequest)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    val transactionStates = new util.ArrayList[ListTransactionsResponseData.TransactionState]()
    transactionStates.add(new ListTransactionsResponseData.TransactionState()
      .setTransactionalId("foo")
      .setProducerId(12345L)
      .setTransactionState("Ongoing"))
    transactionStates.add(new ListTransactionsResponseData.TransactionState()
      .setTransactionalId("bar")
      .setProducerId(98765)
      .setTransactionState("PrepareAbort"))

    when(txnCoordinator.handleListTransactions(Set.empty[Long], Set.empty[String]))
      .thenReturn(new ListTransactionsResponseData()
        .setErrorCode(Errors.NONE.code)
        .setTransactionStates(transactionStates))

    def buildExpectedActions(transactionalId: String): util.List[Action] = {
      val pattern = new ResourcePattern(ResourceType.TRANSACTIONAL_ID, transactionalId, PatternType.LITERAL)
      val action = new Action(AclOperation.DESCRIBE, pattern, 1, true, true)
      Collections.singletonList(action)
    }

    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(buildExpectedActions("foo"))))
      .thenReturn(Seq(AuthorizationResult.ALLOWED).asJava)

    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(buildExpectedActions("bar"))))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava)

    createKafkaApis(authorizer = Some(authorizer)).handleListTransactionsRequest(request)

    val capturedResponse = verifyNoThrottling(request)
    val response = capturedResponse.getValue.asInstanceOf[ListTransactionsResponse]
    assertEquals(1, response.data.transactionStates.size())
    val transactionState = response.data.transactionStates.get(0)
    assertEquals("foo", transactionState.transactionalId)
    assertEquals(12345L, transactionState.producerId)
    assertEquals("Ongoing", transactionState.transactionState)
  }

  @Test
  def testDeleteTopicsByIdAuthorization(): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])
    val controllerContext: ControllerContext = mock(classOf[ControllerContext])

    when(clientControllerQuotaManager.newQuotaFor(
      any[RequestChannel.Request],
      anyShort
    )).thenReturn(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(true)
    when(controller.controllerContext).thenReturn(controllerContext)

    val topicResults = Map(
      AclOperation.DESCRIBE -> Map(
        "foo" -> AuthorizationResult.DENIED,
        "bar" -> AuthorizationResult.ALLOWED
      ),
      AclOperation.DELETE -> Map(
        "foo" -> AuthorizationResult.DENIED,
        "bar" -> AuthorizationResult.DENIED
      )
    )
    when(authorizer.authorize(any[RequestContext], isNotNull[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1).asInstanceOf[util.List[Action]]
      actions.asScala.map { action =>
        val topic = action.resourcePattern.name
        val ops = action.operation()
        topicResults(ops)(topic)
      }.asJava
    })

    // Try to delete three topics:
    // 1. One without describe permission
    // 2. One without delete permission
    // 3. One which is authorized, but doesn't exist
    val topicIdsMap = Map(
      Uuid.randomUuid() -> Some("foo"),
      Uuid.randomUuid() -> Some("bar"),
      Uuid.randomUuid() -> None
    )

    topicIdsMap.foreach { case (topicId, topicNameOpt) =>
      when(controllerContext.topicName(topicId)).thenReturn(topicNameOpt)
    }

    val topicDatas = topicIdsMap.keys.map { topicId =>
      new DeleteTopicsRequestData.DeleteTopicState().setTopicId(topicId)
    }.toList
    val deleteRequest = new DeleteTopicsRequest.Builder(new DeleteTopicsRequestData()
      .setTopics(topicDatas.asJava))
      .build(ApiKeys.DELETE_TOPICS.latestVersion)

    val request = buildRequest(deleteRequest)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    createKafkaApis(authorizer = Some(authorizer)).handleDeleteTopicsRequest(request)
    verify(authorizer, times(2)).authorize(any(), any())

    val capturedResponse = verifyNoThrottling(request)
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
    val authorizer: Authorizer = mock(classOf[Authorizer])

    when(clientControllerQuotaManager.newQuotaFor(
      any[RequestChannel.Request],
      anyShort
    )).thenReturn(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(true)

    // Try to delete three topics:
    // 1. One without describe permission
    // 2. One without delete permission
    // 3. One which is authorized, but doesn't exist

    val topicResults = Map(
      AclOperation.DESCRIBE -> Map(
        "foo" -> AuthorizationResult.DENIED,
        "bar" -> AuthorizationResult.ALLOWED,
        "baz" -> AuthorizationResult.ALLOWED
      ),
      AclOperation.DELETE -> Map(
        "foo" -> AuthorizationResult.DENIED,
        "bar" -> AuthorizationResult.DENIED,
        "baz" -> AuthorizationResult.ALLOWED
      )
    )
    when(authorizer.authorize(any[RequestContext], isNotNull[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1).asInstanceOf[util.List[Action]]
      actions.asScala.map { action =>
        val topic = action.resourcePattern.name
        val ops = action.operation()
        topicResults(ops)(topic)
      }.asJava
    })

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
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    createKafkaApis(authorizer = Some(authorizer)).handleDeleteTopicsRequest(request)
    verify(authorizer, times(2)).authorize(any(), any())

    val capturedResponse = verifyNoThrottling(request)
    val deleteResponse = capturedResponse.getValue.asInstanceOf[DeleteTopicsResponse]

    def lookupErrorCode(topic: String): Option[Errors] = {
      Option(deleteResponse.data.responses().find(topic))
        .map(result => Errors.forCode(result.errorCode))
    }

    assertEquals(Some(Errors.TOPIC_AUTHORIZATION_FAILED), lookupErrorCode("foo"))
    assertEquals(Some(Errors.TOPIC_AUTHORIZATION_FAILED), lookupErrorCode("bar"))
    assertEquals(Some(Errors.UNKNOWN_TOPIC_OR_PARTITION), lookupErrorCode("baz"))
  }

  private def createMockRequest(): RequestChannel.Request = {
    val request: RequestChannel.Request = mock(classOf[RequestChannel.Request])
    val requestHeader: RequestHeader = mock(classOf[RequestHeader])
    when(request.header).thenReturn(requestHeader)
    when(requestHeader.apiKey()).thenReturn(ApiKeys.values().head)
    request
  }

  private def verifyShouldNeverHandleErrorMessage(handler: RequestChannel.Request => Unit): Unit = {
    val request = createMockRequest()
    val e = assertThrows(classOf[UnsupportedVersionException], () => handler(request))
    assertEquals(KafkaApis.shouldNeverReceive(request).getMessage, e.getMessage)
  }

  private def verifyShouldAlwaysForwardErrorMessage(handler: RequestChannel.Request => Unit): Unit = {
    val request = createMockRequest()
    val e = assertThrows(classOf[UnsupportedVersionException], () => handler(request))
    assertEquals(KafkaApis.shouldAlwaysForward(request).getMessage, e.getMessage)
  }

  @Test
  def testRaftShouldNeverHandleLeaderAndIsrRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldNeverHandleErrorMessage(createKafkaApis(raftSupport = true).handleLeaderAndIsrRequest)
  }

  @Test
  def testRaftShouldNeverHandleStopReplicaRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldNeverHandleErrorMessage(createKafkaApis(raftSupport = true).handleStopReplicaRequest)
  }

  @Test
  def testRaftShouldNeverHandleUpdateMetadataRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldNeverHandleErrorMessage(createKafkaApis(raftSupport = true).handleUpdateMetadataRequest(_, RequestLocal.withThreadConfinedCaching))
  }

  @Test
  def testRaftShouldNeverHandleControlledShutdownRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldNeverHandleErrorMessage(createKafkaApis(raftSupport = true).handleControlledShutdownRequest)
  }

  @Test
  def testRaftShouldNeverHandleAlterPartitionRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldNeverHandleErrorMessage(createKafkaApis(raftSupport = true).handleAlterPartitionRequest)
  }

  @Test
  def testRaftShouldNeverHandleEnvelope(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldNeverHandleErrorMessage(createKafkaApis(raftSupport = true).handleEnvelope(_, RequestLocal.withThreadConfinedCaching))
  }

  @Test
  def testRaftShouldAlwaysForwardCreateTopicsRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldAlwaysForwardErrorMessage(createKafkaApis(raftSupport = true).handleCreateTopicsRequest)
  }

  @Test
  def testRaftShouldAlwaysForwardCreatePartitionsRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldAlwaysForwardErrorMessage(createKafkaApis(raftSupport = true).handleCreatePartitionsRequest)
  }

  @Test
  def testRaftShouldAlwaysForwardDeleteTopicsRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldAlwaysForwardErrorMessage(createKafkaApis(raftSupport = true).handleDeleteTopicsRequest)
  }

  @Test
  def testRaftShouldAlwaysForwardCreateAcls(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldAlwaysForwardErrorMessage(createKafkaApis(raftSupport = true).handleCreateAcls)
  }

  @Test
  def testRaftShouldAlwaysForwardDeleteAcls(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldAlwaysForwardErrorMessage(createKafkaApis(raftSupport = true).handleDeleteAcls)
  }

  @Test
  def testEmptyLegacyAlterConfigsRequestWithKRaft(): Unit = {
    val request = buildRequest(new AlterConfigsRequest(new AlterConfigsRequestData(), 1.toShort))
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    createKafkaApis(raftSupport = true).handleAlterConfigsRequest(request)
    val capturedResponse = verifyNoThrottling(request)
    assertEquals(new AlterConfigsResponseData(),
      capturedResponse.getValue.asInstanceOf[AlterConfigsResponse].data())
  }

  @Test
  def testInvalidLegacyAlterConfigsRequestWithKRaft(): Unit = {
    val request = buildRequest(new AlterConfigsRequest(new AlterConfigsRequestData().
      setValidateOnly(true).
      setResources(new LAlterConfigsResourceCollection(asList(
        new LAlterConfigsResource().
          setResourceName(brokerId.toString).
          setResourceType(BROKER.id()).
          setConfigs(new LAlterableConfigCollection(asList(new LAlterableConfig().
            setName("foo").
            setValue(null)).iterator()))).iterator())), 1.toShort))
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    createKafkaApis(raftSupport = true).handleAlterConfigsRequest(request)
    val capturedResponse = verifyNoThrottling(request)
    assertEquals(new AlterConfigsResponseData().setResponses(asList(
      new LAlterConfigsResourceResponse().
        setErrorCode(Errors.INVALID_REQUEST.code()).
        setErrorMessage("Null value not supported for : foo").
        setResourceName(brokerId.toString).
        setResourceType(BROKER.id()))),
      capturedResponse.getValue.asInstanceOf[AlterConfigsResponse].data())
  }

  @Test
  def testRaftShouldAlwaysForwardAlterPartitionReassignmentsRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldAlwaysForwardErrorMessage(createKafkaApis(raftSupport = true).handleAlterPartitionReassignmentsRequest)
  }

  @Test
  def testEmptyIncrementalAlterConfigsRequestWithKRaft(): Unit = {
    val request = buildRequest(new IncrementalAlterConfigsRequest(new IncrementalAlterConfigsRequestData(), 1.toShort))
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    createKafkaApis(raftSupport = true).handleIncrementalAlterConfigsRequest(request)
    val capturedResponse = verifyNoThrottling(request)
    assertEquals(new IncrementalAlterConfigsResponseData(),
      capturedResponse.getValue.asInstanceOf[IncrementalAlterConfigsResponse].data())
  }

  @Test
  def testLog4jIncrementalAlterConfigsRequestWithKRaft(): Unit = {
    val request = buildRequest(new IncrementalAlterConfigsRequest(new IncrementalAlterConfigsRequestData().
      setValidateOnly(true).
      setResources(new IAlterConfigsResourceCollection(asList(new IAlterConfigsResource().
        setResourceName(brokerId.toString).
        setResourceType(BROKER_LOGGER.id()).
        setConfigs(new IAlterableConfigCollection(asList(new IAlterableConfig().
          setName(Log4jController.ROOT_LOGGER).
          setValue("TRACE")).iterator()))).iterator())),
        1.toShort))
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    createKafkaApis(raftSupport = true).handleIncrementalAlterConfigsRequest(request)
    val capturedResponse = verifyNoThrottling(request)
    assertEquals(new IncrementalAlterConfigsResponseData().setResponses(asList(
      new IAlterConfigsResourceResponse().
        setErrorCode(0.toShort).
        setErrorMessage(null).
        setResourceName(brokerId.toString).
        setResourceType(BROKER_LOGGER.id()))),
      capturedResponse.getValue.asInstanceOf[IncrementalAlterConfigsResponse].data())
  }

  @Test
  def testRaftShouldAlwaysForwardCreateTokenRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldAlwaysForwardErrorMessage(createKafkaApis(raftSupport = true).handleCreateTokenRequest)
  }

  @Test
  def testRaftShouldAlwaysForwardRenewTokenRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldAlwaysForwardErrorMessage(createKafkaApis(raftSupport = true).handleRenewTokenRequest)
  }

  @Test
  def testRaftShouldAlwaysForwardExpireTokenRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldAlwaysForwardErrorMessage(createKafkaApis(raftSupport = true).handleExpireTokenRequest)
  }

  @Test
  def testRaftShouldAlwaysForwardAlterClientQuotasRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldAlwaysForwardErrorMessage(createKafkaApis(raftSupport = true).handleAlterClientQuotasRequest)
  }

  @Test
  def testRaftShouldAlwaysForwardAlterUserScramCredentialsRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldAlwaysForwardErrorMessage(createKafkaApis(raftSupport = true).handleAlterUserScramCredentialsRequest)
  }

  @Test
  def testRaftShouldAlwaysForwardUpdateFeatures(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldAlwaysForwardErrorMessage(createKafkaApis(raftSupport = true).handleUpdateFeatures)
  }

  @Test
  def testRaftShouldAlwaysForwardElectLeaders(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldAlwaysForwardErrorMessage(createKafkaApis(raftSupport = true).handleElectLeaders)
  }

  @Test
  def testRaftShouldAlwaysForwardListPartitionReassignments(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    verifyShouldAlwaysForwardErrorMessage(createKafkaApis(raftSupport = true).handleListPartitionReassignmentsRequest)
  }
}
