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

import kafka.api.LeaderAndIsr
import kafka.cluster.{Broker, Partition}
import kafka.controller.{ControllerContext, KafkaController}
import kafka.coordinator.transaction.{InitProducerIdResult, TransactionCoordinator}
import kafka.log.UnifiedLog
import kafka.network.{RequestChannel, RequestMetrics}
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.metadata.{ConfigRepository, KRaftMetadataCache, MockConfigRepository, ZkMetadataCache}
import kafka.utils.{CoreUtils, Log4jController, Logging, TestUtils}
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.clients.admin.{AlterConfigOp, ConfigEntry}
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.ConfigResource.Type.{BROKER, BROKER_LOGGER}
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.internals.{KafkaFutureImpl, Topic}
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.{AddPartitionsToTxnTopic, AddPartitionsToTxnTopicCollection, AddPartitionsToTxnTransaction, AddPartitionsToTxnTransactionCollection}
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnResult
import org.apache.kafka.common.message.AlterConfigsRequestData.{AlterConfigsResource => LAlterConfigsResource, AlterConfigsResourceCollection => LAlterConfigsResourceCollection, AlterableConfig => LAlterableConfig, AlterableConfigCollection => LAlterableConfigCollection}
import org.apache.kafka.common.message.AlterConfigsResponseData.{AlterConfigsResourceResponse => LAlterConfigsResourceResponse}
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData.DescribedGroup
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic
import org.apache.kafka.common.message.CreateTopicsRequestData.{CreatableTopic, CreatableTopicCollection}
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult
import org.apache.kafka.common.message.DescribeConfigsResponseData.DescribeConfigsResult
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.{AlterConfigsResource => IAlterConfigsResource, AlterConfigsResourceCollection => IAlterConfigsResourceCollection, AlterableConfig => IAlterableConfig, AlterableConfigCollection => IAlterableConfigCollection}
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData.{AlterConfigsResourceResponse => IAlterConfigsResourceResponse}
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity
import org.apache.kafka.common.message.ListClientMetricsResourcesResponseData.ClientMetricsResource
import org.apache.kafka.common.message.ListOffsetsRequestData.{ListOffsetsPartition, ListOffsetsTopic}
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic
import org.apache.kafka.common.message.OffsetDeleteRequestData.{OffsetDeleteRequestPartition, OffsetDeleteRequestTopic, OffsetDeleteRequestTopicCollection}
import org.apache.kafka.common.message.OffsetDeleteResponseData.{OffsetDeleteResponsePartition, OffsetDeleteResponsePartitionCollection, OffsetDeleteResponseTopic, OffsetDeleteResponseTopicCollection}
import org.apache.kafka.common.message.StopReplicaRequestData.{StopReplicaPartitionState, StopReplicaTopicState}
import org.apache.kafka.common.message.UpdateMetadataRequestData.{UpdateMetadataBroker, UpdateMetadataEndpoint, UpdateMetadataPartitionState}
import org.apache.kafka.common.message._
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.{ApiKeys, Errors, MessageUtil}
import org.apache.kafka.common.quota.{ClientQuotaAlteration, ClientQuotaEntity}
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType
import org.apache.kafka.common.requests.MetadataResponse.TopicMetadata
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests.WriteTxnMarkersRequest.TxnMarkerEntry
import org.apache.kafka.common.requests.{FetchMetadata => JFetchMetadata, _}
import org.apache.kafka.common.resource.{PatternType, Resource, ResourcePattern, ResourceType}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, KafkaPrincipalSerde, SecurityProtocol}
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource
import org.apache.kafka.common.utils.{ProducerIdAndEpoch, SecurityUtils, Utils}
import org.apache.kafka.common._
import org.apache.kafka.coordinator.group.GroupCoordinator
import org.apache.kafka.server.ClientMetricsManager
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.MetadataVersion.{IBP_0_10_2_IV0, IBP_2_2_IV1}
import org.apache.kafka.server.common.{Features, MetadataVersion}
import org.apache.kafka.server.config.{ConfigType, Defaults}
import org.apache.kafka.server.metrics.ClientMetricsTestUtils
import org.apache.kafka.server.util.{FutureUtils, MockTime}
import org.apache.kafka.storage.internals.log.{AppendOrigin, FetchParams, FetchPartitionData, LogConfig}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, EnumSource, ValueSource}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.{ArgumentCaptor, ArgumentMatchers, Mockito}

import java.net.InetAddress
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util
import java.util.Arrays.asList
import java.util.concurrent.{CompletableFuture, TimeUnit}
import java.util.{Collections, Comparator, Optional, OptionalInt, OptionalLong, Properties}
import scala.collection.{Map, Seq, mutable}
import scala.jdk.CollectionConverters._

class KafkaApisTest extends Logging {
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
  // KRaft tests should override this with a KRaftMetadataCache
  private var metadataCache: MetadataCache = MetadataCache.zkMetadataCache(brokerId, MetadataVersion.latestTesting())
  private val brokerEpochManager: ZkBrokerEpochManager = new ZkBrokerEpochManager(metadataCache, controller, None)
  private val clientQuotaManager: ClientQuotaManager = mock(classOf[ClientQuotaManager])
  private val clientRequestQuotaManager: ClientRequestQuotaManager = mock(classOf[ClientRequestQuotaManager])
  private val clientControllerQuotaManager: ControllerMutationQuotaManager = mock(classOf[ControllerMutationQuotaManager])
  private val replicaQuotaManager: ReplicationQuotaManager = mock(classOf[ReplicationQuotaManager])
  private val quotas = QuotaManagers(clientQuotaManager, clientQuotaManager, clientRequestQuotaManager,
    clientControllerQuotaManager, replicaQuotaManager, replicaQuotaManager, replicaQuotaManager, None)
  private val fetchManager: FetchManager = mock(classOf[FetchManager])
  private val clientMetricsManager: ClientMetricsManager = mock(classOf[ClientMetricsManager])
  private val brokerTopicStats = new BrokerTopicStats
  private val clusterId = "clusterId"
  private val time = new MockTime
  private val clientId = ""
  private var kafkaApis: KafkaApis = _

  @AfterEach
  def tearDown(): Unit = {
    CoreUtils.swallow(quotas.shutdown(), this)
    if (kafkaApis != null)
      CoreUtils.swallow(kafkaApis.close(), this)
    TestUtils.clearYammerMetrics()
    metrics.close()
  }

  def createKafkaApis(interBrokerProtocolVersion: MetadataVersion = MetadataVersion.latestTesting,
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
          ZkSupport(adminManager, controller, zkClient, forwardingManagerOpt, zkMetadataCache, brokerEpochManager)
        case _ => throw new IllegalStateException("Test must set an instance of ZkMetadataCache")
      }
    }

    val listenerType = if (raftSupport) ListenerType.BROKER else ListenerType.ZK_BROKER
    val enabledApis = if (enableForwarding) {
      ApiKeys.apisForListener(listenerType).asScala ++ Set(ApiKeys.ENVELOPE)
    } else {
      ApiKeys.apisForListener(listenerType).asScala.toSet
    }
    val apiVersionManager = new SimpleApiVersionManager(
      listenerType,
      enabledApis,
      BrokerFeatures.defaultSupportedFeatures(true),
      true,
      false,
      () => new Features(MetadataVersion.latestTesting(), Collections.emptyMap[String, java.lang.Short], 0, raftSupport))

    val clientMetricsManagerOpt = if (raftSupport) Some(clientMetricsManager) else None

    new KafkaApis(
      requestChannel = requestChannel,
      metadataSupport = metadataSupport,
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
      apiVersionManager = apiVersionManager,
      clientMetricsManager = clientMetricsManagerOpt)
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
    kafkaApis = createKafkaApis(authorizer = Some(authorizer), configRepository = configRepository)
    kafkaApis.handleDescribeConfigsRequest(request)

    verify(authorizer).authorize(any(), ArgumentMatchers.eq(expectedActions.asJava))
    val response = verifyNoThrottling[DescribeConfigsResponse](request)
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

    val startTimeNanos = time.nanoseconds()
    val queueDurationNanos = 5 * 1000 * 1000
    val request = TestUtils.buildEnvelopeRequest(
      alterConfigsRequest, kafkaPrincipalSerde, requestChannelMetrics, startTimeNanos, startTimeNanos + queueDurationNanos)

    val capturedResponse: ArgumentCaptor[AlterConfigsResponse] = ArgumentCaptor.forClass(classOf[AlterConfigsResponse])
    val capturedRequest: ArgumentCaptor[RequestChannel.Request] = ArgumentCaptor.forClass(classOf[RequestChannel.Request])
    kafkaApis = createKafkaApis(authorizer = Some(authorizer), enableForwarding = true)
    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    verify(requestChannel).sendResponse(
      capturedRequest.capture(),
      capturedResponse.capture(),
      any()
    )
    assertEquals(Some(request), capturedRequest.getValue.envelope)
    // the dequeue time of forwarded request should equals to envelop request
    assertEquals(request.requestDequeueTimeNanos, capturedRequest.getValue.requestDequeueTimeNanos)
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

    val request = TestUtils.buildEnvelopeRequest(
      leaveGroupRequest, kafkaPrincipalSerde, requestChannelMetrics, time.nanoseconds())
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    kafkaApis = createKafkaApis(enableForwarding = true)
    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[EnvelopeResponse](request)
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

    val request = TestUtils.buildEnvelopeRequest(
      alterConfigsRequest, kafkaPrincipalSerde, requestChannelMetrics, time.nanoseconds(), fromPrivilegedListener = fromPrivilegedListener)

    val capturedResponse: ArgumentCaptor[AbstractResponse] = ArgumentCaptor.forClass(classOf[AbstractResponse])
    kafkaApis = createKafkaApis(authorizer = Some(authorizer), enableForwarding = true)
    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

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
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleAlterConfigsRequest(request)

    val response = verifyNoThrottling[AlterConfigsResponse](request)
    verifyAlterConfigResult(response, Map(authorizedTopic -> Errors.NONE,
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
  def testAlterConfigsClientMetrics(): Unit = {
    val subscriptionName = "client_metric_subscription_1"
    val authorizedResource = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, subscriptionName)

    val authorizer: Authorizer = mock(classOf[Authorizer])
    authorizeResource(authorizer, AclOperation.ALTER_CONFIGS, ResourceType.CLUSTER,
      Resource.CLUSTER_NAME, AuthorizationResult.ALLOWED)

    val props = ClientMetricsTestUtils.defaultProperties
    val configEntries = new util.ArrayList[AlterConfigsRequest.ConfigEntry]()
    props.forEach((x, y) =>
      configEntries.add(new AlterConfigsRequest.ConfigEntry(x.asInstanceOf[String], y.asInstanceOf[String])))

    val configs = Map(authorizedResource -> new AlterConfigsRequest.Config(configEntries))

    val requestHeader = new RequestHeader(ApiKeys.ALTER_CONFIGS, ApiKeys.ALTER_CONFIGS.latestVersion, clientId, 0)
    val request = buildRequest(
      new AlterConfigsRequest.Builder(configs.asJava, false).build(requestHeader.apiVersion))

    when(controller.isActive).thenReturn(false)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    when(adminManager.alterConfigs(any(), ArgumentMatchers.eq(false)))
      .thenReturn(Map(authorizedResource -> ApiError.NONE))
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleAlterConfigsRequest(request)
    val response = verifyNoThrottling[AlterConfigsResponse](request)
    verifyAlterConfigResult(response, Map(subscriptionName -> Errors.NONE))
    verify(authorizer, times(1)).authorize(any(), any())
    verify(adminManager).alterConfigs(any(), anyBoolean())
  }

  @Test
  def testIncrementalClientMetricAlterConfigs(): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])

    val subscriptionName = "client_metric_subscription_1"
    val resource = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, subscriptionName)

    authorizeResource(authorizer, AclOperation.ALTER_CONFIGS, ResourceType.CLUSTER,
      Resource.CLUSTER_NAME, AuthorizationResult.ALLOWED)

    val requestHeader = new RequestHeader(ApiKeys.INCREMENTAL_ALTER_CONFIGS,
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.latestVersion, clientId, 0)

    val incrementalAlterConfigsRequest = getIncrementalClientMetricsAlterConfigRequestBuilder(
      Seq(resource)).build(requestHeader.apiVersion)
    val request = buildRequest(incrementalAlterConfigsRequest,
      fromPrivilegedListener = true, requestHeader = Option(requestHeader))

    when(controller.isActive).thenReturn(true)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    when(adminManager.incrementalAlterConfigs(any(), ArgumentMatchers.eq(false)))
      .thenReturn(Map(resource -> ApiError.NONE))
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleIncrementalAlterConfigsRequest(request)
    val response = verifyNoThrottling[IncrementalAlterConfigsResponse](request)
    verifyIncrementalAlterConfigResult(response, Map(subscriptionName -> Errors.NONE ))
    verify(authorizer, times(1)).authorize(any(), any())
    verify(adminManager).incrementalAlterConfigs(any(), anyBoolean())
  }

  private def getIncrementalClientMetricsAlterConfigRequestBuilder(configResources: Seq[ConfigResource]): IncrementalAlterConfigsRequest.Builder = {
    val resourceMap = configResources.map(configResource => {
      val entryToBeModified = new ConfigEntry("metrics", "foo.bar")
      configResource -> Set(new AlterConfigOp(entryToBeModified, OpType.SET)).asJavaCollection
    }).toMap.asJava
    new IncrementalAlterConfigsRequest.Builder(resourceMap, false)
  }

  @Test
  def testDescribeConfigsClientMetrics(): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])
    val operation = AclOperation.DESCRIBE_CONFIGS
    val resourceType = ResourceType.CLUSTER
    val subscriptionName = "client_metric_subscription_1"
    val requestHeader =
      new RequestHeader(ApiKeys.DESCRIBE_CONFIGS, ApiKeys.DESCRIBE_CONFIGS.latestVersion, clientId, 0)
    val expectedActions = Seq(
      new Action(operation, new ResourcePattern(resourceType, Resource.CLUSTER_NAME, PatternType.LITERAL),
        1, true, true)
    )

    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(expectedActions.asJava)))
      .thenReturn(Seq(AuthorizationResult.ALLOWED).asJava)

    val resource = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, subscriptionName)
    val configRepository: ConfigRepository = mock(classOf[ConfigRepository])
    val cmConfigs = ClientMetricsTestUtils.defaultProperties
    when(configRepository.config(resource)).thenReturn(cmConfigs)

    metadataCache = mock(classOf[ZkMetadataCache])
    when(metadataCache.contains(subscriptionName)).thenReturn(true)

    val describeConfigsRequest = new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData()
      .setIncludeSynonyms(true)
      .setResources(List(new DescribeConfigsRequestData.DescribeConfigsResource()
        .setResourceName(subscriptionName)
        .setResourceType(ConfigResource.Type.CLIENT_METRICS.id)).asJava))
      .build(requestHeader.apiVersion)
    val request = buildRequest(describeConfigsRequest,
      requestHeader = Option(requestHeader))
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer), configRepository = configRepository)
    kafkaApis.handleDescribeConfigsRequest(request)

    val response = verifyNoThrottling[DescribeConfigsResponse](request)
    // Verify that authorize is only called once
    verify(authorizer, times(1)).authorize(any(), any())
    val results = response.data().results()
    assertEquals(1, results.size())
    val describeConfigsResult: DescribeConfigsResult = results.get(0)

    assertEquals(ConfigResource.Type.CLIENT_METRICS.id, describeConfigsResult.resourceType())
    assertEquals(subscriptionName, describeConfigsResult.resourceName())
    val configs = describeConfigsResult.configs()
    assertEquals(cmConfigs.size(), configs.size())
  }

  @Test
  def testDescribeQuorumNotAllowedForZkClusters(): Unit = {
    val requestData = DescribeQuorumRequest.singletonRequest(KafkaRaftServer.MetadataPartition)
    val requestBuilder = new DescribeQuorumRequest.Builder(requestData)
    val request = buildRequest(requestBuilder.build())

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    kafkaApis = createKafkaApis(enableForwarding = true)
    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[DescribeQuorumResponse](request)
    assertEquals(Errors.UNKNOWN_SERVER_ERROR, Errors.forCode(response.data.errorCode))
  }

  @Test
  def testDescribeQuorumForwardedForKRaftClusters(): Unit = {
    val requestData = DescribeQuorumRequest.singletonRequest(KafkaRaftServer.MetadataPartition)
    val requestBuilder = new DescribeQuorumRequest.Builder(requestData)
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    kafkaApis = createKafkaApis(raftSupport = true)
    testForwardableApi(kafkaApis = kafkaApis,
      ApiKeys.DESCRIBE_QUORUM,
      requestBuilder
    )
  }

  private def testKraftForwarding(
    apiKey: ApiKeys,
    requestBuilder: AbstractRequest.Builder[_ <: AbstractRequest]
  ): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    kafkaApis = createKafkaApis(enableForwarding = true, raftSupport = true)
    testForwardableApi(kafkaApis = kafkaApis,
      apiKey,
      requestBuilder
    )
  }

  private def testForwardableApi(apiKey: ApiKeys, requestBuilder: AbstractRequest.Builder[_ <: AbstractRequest]): Unit = {
    kafkaApis = createKafkaApis(enableForwarding = true)
    testForwardableApi(kafkaApis = kafkaApis,
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

    val capturedResponse = verifyNoThrottling[AbstractResponse](request)
    assertEquals(expectedResponse.data, capturedResponse.data)

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

  private def verifyAlterConfigResult(response: AlterConfigsResponse,
                                      expectedResults: Map[String, Errors]): Unit = {
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
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleIncrementalAlterConfigsRequest(request)

    val capturedResponse = verifyNoThrottling[IncrementalAlterConfigsResponse](request)
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

  private def verifyIncrementalAlterConfigResult(response: IncrementalAlterConfigsResponse,
                                                 expectedResults: Map[String, Errors]): Unit = {
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
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleAlterClientQuotasRequest(request)

    val capturedResponse = verifyNoThrottling[AlterClientQuotasResponse](request)
    verifyAlterClientQuotaResult(capturedResponse, Map(quotaEntity -> Errors.CLUSTER_AUTHORIZATION_FAILED))

    verify(authorizer).authorize(any(), any())
    verify(clientRequestQuotaManager).maybeRecordAndGetThrottleTimeMs(any(), anyLong)
  }

  @Test
  def testAlterClientQuotasWithForwarding(): Unit = {
    val requestBuilder = new AlterClientQuotasRequest.Builder(List.empty.asJava, false)
    testForwardableApi(ApiKeys.ALTER_CLIENT_QUOTAS, requestBuilder)
  }

  private def verifyAlterClientQuotaResult(response: AlterClientQuotasResponse,
                                           expected: Map[ClientQuotaEntity, Errors]): Unit = {
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
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleCreateTopicsRequest(request)

    val capturedCallback: ArgumentCaptor[Map[String, ApiError] => Unit] = ArgumentCaptor.forClass(classOf[Map[String, ApiError] => Unit])

    verify(adminManager).createTopics(
      ArgumentMatchers.eq(timeout),
      ArgumentMatchers.eq(false),
      ArgumentMatchers.eq(Map(authorizedTopic -> topicToCreate)),
      any(),
      ArgumentMatchers.eq(UnboundedControllerMutationQuota),
      capturedCallback.capture())
    capturedCallback.getValue.apply(Map(authorizedTopic -> ApiError.NONE))

    val capturedResponse = verifyNoThrottling[CreateTopicsResponse](request)
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

  @ParameterizedTest
  @CsvSource(value = Array("0,1500", "1500,0", "3000,1000"))
  def testKRaftControllerThrottleTimeEnforced(
    controllerThrottleTimeMs: Int,
    requestThrottleTimeMs: Int
  ): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)

    val topicToCreate = new CreatableTopic()
      .setName("topic")
      .setNumPartitions(1)
      .setReplicationFactor(1.toShort)

    val requestData = new CreateTopicsRequestData()
    requestData.topics().add(topicToCreate)

    val requestBuilder = new CreateTopicsRequest.Builder(requestData).build()
    val request = buildRequest(requestBuilder)

    kafkaApis = createKafkaApis(enableForwarding = true, raftSupport = true)
    val forwardCallback: ArgumentCaptor[Option[AbstractResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Option[AbstractResponse] => Unit])

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(request, time.milliseconds()))
      .thenReturn(requestThrottleTimeMs)

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    verify(forwardingManager).forwardRequest(
      ArgumentMatchers.eq(request),
      forwardCallback.capture()
    )

    val responseData = new CreateTopicsResponseData()
      .setThrottleTimeMs(controllerThrottleTimeMs)
    responseData.topics().add(new CreatableTopicResult()
      .setErrorCode(Errors.THROTTLING_QUOTA_EXCEEDED.code))

    forwardCallback.getValue.apply(Some(new CreateTopicsResponse(responseData)))

    val expectedThrottleTimeMs = math.max(controllerThrottleTimeMs, requestThrottleTimeMs)

    verify(clientRequestQuotaManager).throttle(
      ArgumentMatchers.eq(request),
      any[ThrottleCallback](),
      ArgumentMatchers.eq(expectedThrottleTimeMs)
    )

    assertEquals(expectedThrottleTimeMs, responseData.throttleTimeMs)
  }

  @Test
  def testCreatePartitionsAuthorization(): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))

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

    val response = verifyNoThrottling[CreatePartitionsResponse](request)
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

  private def verifyCreateTopicsResult(response: CreateTopicsResponse,
                                       expectedResults: Map[String, Errors]): Unit = {
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
  def testUpdateFeaturesWithForwarding(): Unit = {
    val requestBuilder = new UpdateFeaturesRequest.Builder(new UpdateFeaturesRequestData())
    testForwardableApi(ApiKeys.UPDATE_FEATURES, requestBuilder)
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
          when(groupCoordinator.groupMetadataTopicConfigs).thenReturn(new Properties)
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
    kafkaApis = createKafkaApis(authorizer = Some(authorizer),
      overrideProperties = topicConfigOverride)
    kafkaApis.handleFindCoordinatorRequest(request)

    val response = verifyNoThrottling[FindCoordinatorResponse](request)
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
          when(groupCoordinator.groupMetadataTopicConfigs).thenReturn(new Properties)
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
    kafkaApis = createKafkaApis(authorizer = Some(authorizer), enableForwarding = enableAutoTopicCreation,
      overrideProperties = topicConfigOverride)
    kafkaApis.handleTopicMetadataRequest(request)

    val response = verifyNoThrottling[MetadataResponse](request)
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
        try {
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
        } finally {
          kafkaApis.close()
        }
      })
    )
  }

  @Test
  def testHandleOffsetCommitRequest(): Unit = {
    addTopicToMetadataCache("foo", numPartitions = 1)

    val offsetCommitRequest = new OffsetCommitRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setTopics(List(
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName("foo")
          .setPartitions(List(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(10)).asJava)).asJava)

    val requestChannelRequest = buildRequest(new OffsetCommitRequest.Builder(offsetCommitRequest).build())

    val future = new CompletableFuture[OffsetCommitResponseData]()
    when(groupCoordinator.commitOffsets(
      requestChannelRequest.context,
      offsetCommitRequest,
      RequestLocal.NoCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(
      requestChannelRequest,
      RequestLocal.NoCaching
    )

    // This is the response returned by the group coordinator.
    val offsetCommitResponse = new OffsetCommitResponseData()
      .setTopics(List(
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setName("foo")
          .setPartitions(List(
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code)).asJava)).asJava)

    future.complete(offsetCommitResponse)
    val response = verifyNoThrottling[OffsetCommitResponse](requestChannelRequest)
    assertEquals(offsetCommitResponse, response.data)
  }

  @Test
  def testHandleOffsetCommitRequestFutureFailed(): Unit = {
    addTopicToMetadataCache("foo", numPartitions = 1)

    val offsetCommitRequest = new OffsetCommitRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setTopics(List(
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName("foo")
          .setPartitions(List(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(10)).asJava)).asJava)

    val requestChannelRequest = buildRequest(new OffsetCommitRequest.Builder(offsetCommitRequest).build())

    val future = new CompletableFuture[OffsetCommitResponseData]()
    when(groupCoordinator.commitOffsets(
      requestChannelRequest.context,
      offsetCommitRequest,
      RequestLocal.NoCaching.bufferSupplier
    )).thenReturn(future)
    
    kafkaApis = createKafkaApis()
    kafkaApis.handle(
      requestChannelRequest,
      RequestLocal.NoCaching
    )

    val expectedOffsetCommitResponse = new OffsetCommitResponseData()
      .setTopics(List(
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setName("foo")
          .setPartitions(List(
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NOT_COORDINATOR.code)).asJava)).asJava)

    future.completeExceptionally(Errors.NOT_COORDINATOR.exception)
    val response = verifyNoThrottling[OffsetCommitResponse](requestChannelRequest)
    assertEquals(expectedOffsetCommitResponse, response.data)
  }

  @Test
  def testHandleOffsetCommitRequestTopicsAndPartitionsValidation(): Unit = {
    addTopicToMetadataCache("foo", numPartitions = 2)
    addTopicToMetadataCache("bar", numPartitions = 2)

    val offsetCommitRequest = new OffsetCommitRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setTopics(List(
        // foo exists but only has 2 partitions.
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName("foo")
          .setPartitions(List(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(10),
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(20),
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(2)
              .setCommittedOffset(30)).asJava),
        // bar exists.
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName("bar")
          .setPartitions(List(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(40),
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(50)).asJava),
        // zar does not exist.
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName("zar")
          .setPartitions(List(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(60),
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(70)).asJava)).asJava)

    val requestChannelRequest = buildRequest(new OffsetCommitRequest.Builder(offsetCommitRequest).build())

    // This is the request expected by the group coordinator.
    val expectedOffsetCommitRequest = new OffsetCommitRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setTopics(List(
        // foo exists but only has 2 partitions.
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName("foo")
          .setPartitions(List(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(10),
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(20)).asJava),
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName("bar")
          .setPartitions(List(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(40),
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(50)).asJava)).asJava)

    val future = new CompletableFuture[OffsetCommitResponseData]()
    when(groupCoordinator.commitOffsets(
      requestChannelRequest.context,
      expectedOffsetCommitRequest,
      RequestLocal.NoCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(
      requestChannelRequest,
      RequestLocal.NoCaching
    )

    // This is the response returned by the group coordinator.
    val offsetCommitResponse = new OffsetCommitResponseData()
      .setTopics(List(
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setName("foo")
          .setPartitions(List(
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)).asJava),
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setName("bar")
          .setPartitions(List(
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)).asJava)).asJava)

    val expectedOffsetCommitResponse = new OffsetCommitResponseData()
      .setTopics(List(
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setName("foo")
          .setPartitions(List(
            // foo-2 is first because partitions failing the validation
            // are put in the response first.
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(2)
              .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code),
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)).asJava),
        // zar is before bar because topics failing the validation are
        // put in the response first.
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setName("zar")
          .setPartitions(List(
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code),
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)).asJava),
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setName("bar")
          .setPartitions(List(
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)).asJava)).asJava)

    future.complete(offsetCommitResponse)
    val response = verifyNoThrottling[OffsetCommitResponse](requestChannelRequest)
    assertEquals(expectedOffsetCommitResponse, response.data)
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
      val kafkaApis = createKafkaApis()
      try {
        kafkaApis.handleOffsetCommitRequest(request, RequestLocal.withThreadConfinedCaching)

        val response = verifyNoThrottling[OffsetCommitResponse](request)
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION,
          Errors.forCode(response.data.topics().get(0).partitions().get(0).errorCode))
      } finally {
        kafkaApis.close()
      }
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
      val kafkaApis = createKafkaApis()
      try {
        kafkaApis.handleTxnOffsetCommitRequest(request, RequestLocal.withThreadConfinedCaching)

        val response = verifyNoThrottling[TxnOffsetCommitResponse](request)
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, response.errors().get(invalidTopicPartition))
      } finally {
        kafkaApis.close()
      }
    }

    checkInvalidPartition(-1)
    checkInvalidPartition(1) // topic has only one partition
  }

  @Test
  def testHandleTxnOffsetCommitRequest(): Unit = {
    addTopicToMetadataCache("foo", numPartitions = 1)

    val txnOffsetCommitRequest = new TxnOffsetCommitRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setGenerationId(10)
      .setProducerId(20)
      .setProducerEpoch(30)
      .setGroupInstanceId("instance-id")
      .setTransactionalId("transactional-id")
      .setTopics(List(
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName("foo")
          .setPartitions(List(
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(10)).asJava)).asJava)

    val requestChannelRequest = buildRequest(new TxnOffsetCommitRequest.Builder(txnOffsetCommitRequest).build())

    val future = new CompletableFuture[TxnOffsetCommitResponseData]()
    when(txnCoordinator.partitionFor(txnOffsetCommitRequest.transactionalId)).thenReturn(0)
    when(groupCoordinator.commitTransactionalOffsets(
      requestChannelRequest.context,
      txnOffsetCommitRequest,
      RequestLocal.NoCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(
      requestChannelRequest,
      RequestLocal.NoCaching
    )

    // This is the response returned by the group coordinator.
    val txnOffsetCommitResponse = new TxnOffsetCommitResponseData()
      .setTopics(List(
        new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
          .setName("foo")
          .setPartitions(List(
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code)).asJava)).asJava)

    future.complete(txnOffsetCommitResponse)
    val response = verifyNoThrottling[TxnOffsetCommitResponse](requestChannelRequest)
    assertEquals(txnOffsetCommitResponse, response.data)
  }

  @Test
  def testHandleTxnOffsetCommitRequestFutureFailed(): Unit = {
    addTopicToMetadataCache("foo", numPartitions = 1)

    val txnOffsetCommitRequest = new TxnOffsetCommitRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setTopics(List(
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName("foo")
          .setPartitions(List(
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(10)).asJava)).asJava)

    val requestChannelRequest = buildRequest(new TxnOffsetCommitRequest.Builder(txnOffsetCommitRequest).build())

    val future = new CompletableFuture[TxnOffsetCommitResponseData]()
    when(txnCoordinator.partitionFor(txnOffsetCommitRequest.transactionalId)).thenReturn(0)
    when(groupCoordinator.commitTransactionalOffsets(
      requestChannelRequest.context,
      txnOffsetCommitRequest,
      RequestLocal.NoCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(
      requestChannelRequest,
      RequestLocal.NoCaching
    )

    val expectedTxnOffsetCommitResponse = new TxnOffsetCommitResponseData()
      .setTopics(List(
        new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
          .setName("foo")
          .setPartitions(List(
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NOT_COORDINATOR.code)).asJava)).asJava)

    future.completeExceptionally(Errors.NOT_COORDINATOR.exception)
    val response = verifyNoThrottling[TxnOffsetCommitResponse](requestChannelRequest)
    assertEquals(expectedTxnOffsetCommitResponse, response.data)
  }

  @Test
  def testHandleTxnOffsetCommitRequestTopicsAndPartitionsValidation(): Unit = {
    addTopicToMetadataCache("foo", numPartitions = 2)
    addTopicToMetadataCache("bar", numPartitions = 2)

    val txnOffsetCommitRequest = new TxnOffsetCommitRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setTopics(List(
        // foo exists but only has 2 partitions.
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName("foo")
          .setPartitions(List(
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(10),
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(20),
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(2)
              .setCommittedOffset(30)).asJava),
        // bar exists.
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName("bar")
          .setPartitions(List(
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(40),
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(50)).asJava),
        // zar does not exist.
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName("zar")
          .setPartitions(List(
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(60),
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(70)).asJava)).asJava)

    val requestChannelRequest = buildRequest(new TxnOffsetCommitRequest.Builder(txnOffsetCommitRequest).build())

    // This is the request expected by the group coordinator.
    val expectedTxnOffsetCommitRequest = new TxnOffsetCommitRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setTopics(List(
        // foo exists but only has 2 partitions.
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName("foo")
          .setPartitions(List(
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(10),
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(20)).asJava),
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName("bar")
          .setPartitions(List(
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(40),
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(50)).asJava)).asJava)

    val future = new CompletableFuture[TxnOffsetCommitResponseData]()
    when(txnCoordinator.partitionFor(expectedTxnOffsetCommitRequest.transactionalId)).thenReturn(0)
    when(groupCoordinator.commitTransactionalOffsets(
      requestChannelRequest.context,
      expectedTxnOffsetCommitRequest,
      RequestLocal.NoCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(
      requestChannelRequest,
      RequestLocal.NoCaching
    )

    // This is the response returned by the group coordinator.
    val txnOffsetCommitResponse = new TxnOffsetCommitResponseData()
      .setTopics(List(
        new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
          .setName("foo")
          .setPartitions(List(
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)).asJava),
        new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
          .setName("bar")
          .setPartitions(List(
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)).asJava)).asJava)

    val expectedTxnOffsetCommitResponse = new TxnOffsetCommitResponseData()
      .setTopics(List(
        new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
          .setName("foo")
          .setPartitions(List(
            // foo-2 is first because partitions failing the validation
            // are put in the response first.
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(2)
              .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code),
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)).asJava),
        // zar is before bar because topics failing the validation are
        // put in the response first.
        new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
          .setName("zar")
          .setPartitions(List(
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code),
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)).asJava),
        new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
          .setName("bar")
          .setPartitions(List(
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)).asJava)).asJava)

    future.complete(txnOffsetCommitResponse)
    val response = verifyNoThrottling[TxnOffsetCommitResponse](requestChannelRequest)
    assertEquals(expectedTxnOffsetCommitResponse, response.data)
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.TXN_OFFSET_COMMIT)
  def shouldReplaceCoordinatorNotAvailableWithLoadInProcessInTxnOffsetCommitWithOlderClient(version: Short): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 2)

    val topicPartition = new TopicPartition(topic, 1)
    val capturedResponse: ArgumentCaptor[TxnOffsetCommitResponse] = ArgumentCaptor.forClass(classOf[TxnOffsetCommitResponse])

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
    ).build(version)
    val request = buildRequest(offsetCommitRequest)

    val requestLocal = RequestLocal.withThreadConfinedCaching
    val future = new CompletableFuture[TxnOffsetCommitResponseData]()
    when(txnCoordinator.partitionFor(offsetCommitRequest.data.transactionalId)).thenReturn(0)
    when(groupCoordinator.commitTransactionalOffsets(
      request.context,
      offsetCommitRequest.data,
      requestLocal.bufferSupplier
    )).thenReturn(future)

    future.complete(new TxnOffsetCommitResponseData()
      .setTopics(List(
        new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
          .setName(topicPartition.topic)
          .setPartitions(List(
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(topicPartition.partition)
              .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code)
          ).asJava)
      ).asJava))
    kafkaApis = createKafkaApis()
    kafkaApis.handleTxnOffsetCommitRequest(request, requestLocal)

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
      val kafkaApis = createKafkaApis()
      try {
        kafkaApis.handleInitProducerIdRequest(request, requestLocal)

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
      } finally {
        kafkaApis.close()
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
      val kafkaApis = createKafkaApis()
      try {
        kafkaApis.handleAddOffsetsToTxnRequest(request, requestLocal)

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
      } finally {
        kafkaApis.close()
      }
    }
  }

  @Test
  def shouldReplaceProducerFencedWithInvalidProducerEpochInAddPartitionToTxnWithOlderClient(): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 2)

    for (version <- ApiKeys.ADD_PARTITIONS_TO_TXN.oldestVersion to 3) {

      reset(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      val capturedResponse: ArgumentCaptor[AddPartitionsToTxnResponse] = ArgumentCaptor.forClass(classOf[AddPartitionsToTxnResponse])
      val responseCallback: ArgumentCaptor[Errors => Unit] = ArgumentCaptor.forClass(classOf[Errors => Unit])

      val transactionalId = "txnId"
      val producerId = 15L
      val epoch = 0.toShort

      val partition = 1
      val topicPartition = new TopicPartition(topic, partition)

      val addPartitionsToTxnRequest = AddPartitionsToTxnRequest.Builder.forClient(
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
      val kafkaApis = createKafkaApis()
      try {
        kafkaApis.handleAddPartitionsToTxnRequest(request, requestLocal)

        verify(requestChannel).sendResponse(
          ArgumentMatchers.eq(request),
          capturedResponse.capture(),
          ArgumentMatchers.eq(None)
        )
        val response = capturedResponse.getValue

        if (version < 2) {
          assertEquals(Collections.singletonMap(topicPartition, Errors.INVALID_PRODUCER_EPOCH), response.errors().get(AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID))
        } else {
          assertEquals(Collections.singletonMap(topicPartition, Errors.PRODUCER_FENCED), response.errors().get(AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID))
        }
      } finally {
        kafkaApis.close()
      }
    }
  }

  @Test
  def testBatchedAddPartitionsToTxnRequest(): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 2)

    val responseCallback: ArgumentCaptor[Errors => Unit] = ArgumentCaptor.forClass(classOf[Errors => Unit])
    val verifyPartitionsCallback: ArgumentCaptor[AddPartitionsToTxnResult => Unit] = ArgumentCaptor.forClass(classOf[AddPartitionsToTxnResult => Unit])

    val transactionalId1 = "txnId1"
    val transactionalId2 = "txnId2"
    val producerId = 15L
    val epoch = 0.toShort
    
    val tp0 = new TopicPartition(topic, 0)
    val tp1 = new TopicPartition(topic, 1)

    val addPartitionsToTxnRequest = AddPartitionsToTxnRequest.Builder.forBroker(
      new AddPartitionsToTxnTransactionCollection(
        List(new AddPartitionsToTxnTransaction()
          .setTransactionalId(transactionalId1)
          .setProducerId(producerId)
          .setProducerEpoch(epoch)
          .setVerifyOnly(false)
          .setTopics(new AddPartitionsToTxnTopicCollection(
            Collections.singletonList(new AddPartitionsToTxnTopic()
              .setName(tp0.topic)
              .setPartitions(Collections.singletonList(tp0.partition))
            ).iterator())
          ), new AddPartitionsToTxnTransaction()
          .setTransactionalId(transactionalId2)
          .setProducerId(producerId)
          .setProducerEpoch(epoch)
          .setVerifyOnly(true)
          .setTopics(new AddPartitionsToTxnTopicCollection(
            Collections.singletonList(new AddPartitionsToTxnTopic()
              .setName(tp1.topic)
              .setPartitions(Collections.singletonList(tp1.partition))
            ).iterator())
          )
        ).asJava.iterator()
      )
    ).build(4.toShort)
    val request = buildRequest(addPartitionsToTxnRequest)

    val requestLocal = RequestLocal.withThreadConfinedCaching
    when(txnCoordinator.handleAddPartitionsToTransaction(
      ArgumentMatchers.eq(transactionalId1),
      ArgumentMatchers.eq(producerId),
      ArgumentMatchers.eq(epoch),
      ArgumentMatchers.eq(Set(tp0)),
      responseCallback.capture(),
      ArgumentMatchers.eq(requestLocal)
    )).thenAnswer(_ => responseCallback.getValue.apply(Errors.NONE))

    when(txnCoordinator.handleVerifyPartitionsInTransaction(
      ArgumentMatchers.eq(transactionalId2),
      ArgumentMatchers.eq(producerId),
      ArgumentMatchers.eq(epoch),
      ArgumentMatchers.eq(Set(tp1)),
      verifyPartitionsCallback.capture(),
    )).thenAnswer(_ => verifyPartitionsCallback.getValue.apply(AddPartitionsToTxnResponse.resultForTransaction(transactionalId2, Map(tp1 -> Errors.PRODUCER_FENCED).asJava)))
    kafkaApis = createKafkaApis()
    kafkaApis.handleAddPartitionsToTxnRequest(request, requestLocal)

    val response = verifyNoThrottling[AddPartitionsToTxnResponse](request)
    
    val expectedErrors = Map(
      transactionalId1 -> Collections.singletonMap(tp0, Errors.NONE),
      transactionalId2 -> Collections.singletonMap(tp1, Errors.PRODUCER_FENCED)
    ).asJava
    
    assertEquals(expectedErrors, response.errors())
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.ADD_PARTITIONS_TO_TXN)
  def testHandleAddPartitionsToTxnAuthorizationFailedAndMetrics(version: Short): Unit = {
    val requestMetrics = new RequestChannel.Metrics(Seq(ApiKeys.ADD_PARTITIONS_TO_TXN))
    try {
      val topic = "topic"

      val transactionalId = "txnId1"
      val producerId = 15L
      val epoch = 0.toShort

      val tp = new TopicPartition(topic, 0)

      val addPartitionsToTxnRequest =
        if (version < 4)
          AddPartitionsToTxnRequest.Builder.forClient(
            transactionalId,
            producerId,
            epoch,
            Collections.singletonList(tp)).build(version)
        else
          AddPartitionsToTxnRequest.Builder.forBroker(
            new AddPartitionsToTxnTransactionCollection(
              List(new AddPartitionsToTxnTransaction()
                .setTransactionalId(transactionalId)
                .setProducerId(producerId)
                .setProducerEpoch(epoch)
                .setVerifyOnly(true)
                .setTopics(new AddPartitionsToTxnTopicCollection(
                  Collections.singletonList(new AddPartitionsToTxnTopic()
                    .setName(tp.topic)
                    .setPartitions(Collections.singletonList(tp.partition))
                  ).iterator()))
              ).asJava.iterator())).build(version)

      val requestChannelRequest = buildRequest(addPartitionsToTxnRequest, requestMetrics = requestMetrics)

      val authorizer: Authorizer = mock(classOf[Authorizer])
      when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
        .thenReturn(Seq(AuthorizationResult.DENIED).asJava)
      kafkaApis = createKafkaApis(authorizer = Some(authorizer))
      kafkaApis.handle(
        requestChannelRequest,
        RequestLocal.NoCaching
      )

      val response = verifyNoThrottlingAndUpdateMetrics[AddPartitionsToTxnResponse](requestChannelRequest)
      val error = if (version < 4)
        response.errors().get(AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID).get(tp)
      else
        Errors.forCode(response.data().errorCode)

      val expectedError = if (version < 4) Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED else Errors.CLUSTER_AUTHORIZATION_FAILED
      assertEquals(expectedError, error)

      val metricName = if (version < 4) ApiKeys.ADD_PARTITIONS_TO_TXN.name else RequestMetrics.verifyPartitionsInTxnMetricName
      assertEquals(8, TestUtils.metersCount(metricName))
    } finally {
      requestMetrics.close()
    }
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.ADD_PARTITIONS_TO_TXN)
  def testAddPartitionsToTxnOperationNotAttempted(version: Short): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 1)

    val transactionalId = "txnId1"
    val producerId = 15L
    val epoch = 0.toShort

    val tp0 = new TopicPartition(topic, 0)
    val tp1 = new TopicPartition(topic, 1)

    val addPartitionsToTxnRequest = if (version < 4)
      AddPartitionsToTxnRequest.Builder.forClient(
        transactionalId,
        producerId,
        epoch,
        List(tp0, tp1).asJava).build(version)
    else
      AddPartitionsToTxnRequest.Builder.forBroker(
        new AddPartitionsToTxnTransactionCollection(
          List(new AddPartitionsToTxnTransaction()
            .setTransactionalId(transactionalId)
            .setProducerId(producerId)
            .setProducerEpoch(epoch)
            .setVerifyOnly(true)
            .setTopics(new AddPartitionsToTxnTopicCollection(
              Collections.singletonList(new AddPartitionsToTxnTopic()
                .setName(tp0.topic)
                .setPartitions(List[Integer](tp0.partition, tp1.partition()).asJava)
              ).iterator()))
          ).asJava.iterator())).build(version)

    val requestChannelRequest = buildRequest(addPartitionsToTxnRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleAddPartitionsToTxnRequest(
      requestChannelRequest,
      RequestLocal.NoCaching
    )

    val response = verifyNoThrottling[AddPartitionsToTxnResponse](requestChannelRequest)
    
    def checkErrorForTp(tp: TopicPartition, expectedError: Errors): Unit = {
      val error = if (version < 4)
        response.errors().get(AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID).get(tp)
      else
        response.errors().get(transactionalId).get(tp)

      assertEquals(expectedError, error)
    }

    checkErrorForTp(tp0, Errors.OPERATION_NOT_ATTEMPTED)
    checkErrorForTp(tp1, Errors.UNKNOWN_TOPIC_OR_PARTITION)
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
      val kafkaApis = createKafkaApis()
      try {
        kafkaApis.handleEndTxnRequest(request, requestLocal)

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
      } finally {
        kafkaApis.close()
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

      when(replicaManager.handleProduceAppend(anyLong,
        anyShort,
        ArgumentMatchers.eq(false),
        any(),
        any(),
        responseCallback.capture(),
        any(),
        any(),
        any()
      )).thenAnswer(_ => responseCallback.getValue.apply(Map(tp -> new PartitionResponse(Errors.INVALID_PRODUCER_EPOCH))))

      when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
        any[Long])).thenReturn(0)
      when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
        any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
      val kafkaApis = createKafkaApis()
      try {
        kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

        val response = verifyNoThrottling[ProduceResponse](request)

        assertEquals(1, response.data.responses.size)
        val topicProduceResponse = response.data.responses.asScala.head
        assertEquals(1, topicProduceResponse.partitionResponses.size)
        val partitionProduceResponse = topicProduceResponse.partitionResponses.asScala.head
        assertEquals(Errors.INVALID_PRODUCER_EPOCH, Errors.forCode(partitionProduceResponse.errorCode))
      } finally {
        kafkaApis.close()
      }
    }
  }

  @Test
  def testProduceResponseContainsNewLeaderOnNotLeaderOrFollower(): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 2, numBrokers = 3)

    for (version <- 10 to ApiKeys.PRODUCE.latestVersion) {

      reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      val responseCallback: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] = ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])

      val tp = new TopicPartition(topic, 0)
      val partition = mock(classOf[Partition])
      val newLeaderId = 2
      val newLeaderEpoch = 5

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

      when(replicaManager.handleProduceAppend(anyLong,
        anyShort,
        ArgumentMatchers.eq(false),
        any(),
        any(),
        responseCallback.capture(),
        any(),
        any(),
        any())
      ).thenAnswer(_ => responseCallback.getValue.apply(Map(tp -> new PartitionResponse(Errors.NOT_LEADER_OR_FOLLOWER))))

      when(replicaManager.getPartitionOrError(tp)).thenAnswer(_ => Right(partition))
      when(partition.leaderReplicaIdOpt).thenAnswer(_ => Some(newLeaderId))
      when(partition.getLeaderEpoch).thenAnswer(_ => newLeaderEpoch)

      when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
        any[Long])).thenReturn(0)
      when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
        any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
      kafkaApis = createKafkaApis()
      kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

      val response = verifyNoThrottling[ProduceResponse](request)

      assertEquals(1, response.data.responses.size)
      val topicProduceResponse = response.data.responses.asScala.head
      assertEquals(1, topicProduceResponse.partitionResponses.size)
      val partitionProduceResponse = topicProduceResponse.partitionResponses.asScala.head
      assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, Errors.forCode(partitionProduceResponse.errorCode))
      assertEquals(newLeaderId, partitionProduceResponse.currentLeader.leaderId())
      assertEquals(newLeaderEpoch, partitionProduceResponse.currentLeader.leaderEpoch())
      assertEquals(1, response.data.nodeEndpoints.size)
      val node = response.data.nodeEndpoints.asScala.head
      assertEquals(2, node.nodeId)
      assertEquals("broker2", node.host)
    }
  }

  @Test
  def testProduceResponseReplicaManagerLookupErrorOnNotLeaderOrFollower(): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 2, numBrokers = 3)

    for (version <- 10 to ApiKeys.PRODUCE.latestVersion) {

      reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      val responseCallback: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] = ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])

      val tp = new TopicPartition(topic, 0)

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

      when(replicaManager.handleProduceAppend(anyLong,
        anyShort,
        ArgumentMatchers.eq(false),
        any(),
        any(),
        responseCallback.capture(),
        any(),
        any(),
        any())
      ).thenAnswer(_ => responseCallback.getValue.apply(Map(tp -> new PartitionResponse(Errors.NOT_LEADER_OR_FOLLOWER))))

      when(replicaManager.getPartitionOrError(tp)).thenAnswer(_ => Left(Errors.UNKNOWN_TOPIC_OR_PARTITION))

      when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
        any[Long])).thenReturn(0)
      when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
        any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
      kafkaApis = createKafkaApis()
      kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

      val response = verifyNoThrottling[ProduceResponse](request)

      assertEquals(1, response.data.responses.size)
      val topicProduceResponse = response.data.responses.asScala.head
      assertEquals(1, topicProduceResponse.partitionResponses.size)
      val partitionProduceResponse = topicProduceResponse.partitionResponses.asScala.head
      assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, Errors.forCode(partitionProduceResponse.errorCode))
      // LeaderId and epoch should be the same values inserted into the metadata cache
      assertEquals(0, partitionProduceResponse.currentLeader.leaderId())
      assertEquals(1, partitionProduceResponse.currentLeader.leaderEpoch())
      assertEquals(1, response.data.nodeEndpoints.size)
      val node = response.data.nodeEndpoints.asScala.head
      assertEquals(0, node.nodeId)
      assertEquals("broker0", node.host)
    }
  }

  @Test
  def testProduceResponseMetadataLookupErrorOnNotLeaderOrFollower(): Unit = {
    val topic = "topic"
    metadataCache = mock(classOf[ZkMetadataCache])

    for (version <- 10 to ApiKeys.PRODUCE.latestVersion) {

      reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      val responseCallback: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] = ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])

      val tp = new TopicPartition(topic, 0)

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

      when(replicaManager.handleProduceAppend(anyLong,
        anyShort,
        ArgumentMatchers.eq(false),
        any(),
        any(),
        responseCallback.capture(),
        any(),
        any(),
        any())
      ).thenAnswer(_ => responseCallback.getValue.apply(Map(tp -> new PartitionResponse(Errors.NOT_LEADER_OR_FOLLOWER))))

      when(replicaManager.getPartitionOrError(tp)).thenAnswer(_ => Left(Errors.UNKNOWN_TOPIC_OR_PARTITION))

      when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
        any[Long])).thenReturn(0)
      when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
        any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
      when(metadataCache.contains(tp)).thenAnswer(_ => true)
      when(metadataCache.getPartitionInfo(tp.topic(), tp.partition())).thenAnswer(_ => Option.empty)
      when(metadataCache.getAliveBrokerNode(any(), any())).thenReturn(Option.empty)
      kafkaApis = createKafkaApis()
      kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

      val response = verifyNoThrottling[ProduceResponse](request)

      assertEquals(1, response.data.responses.size)
      val topicProduceResponse = response.data.responses.asScala.head
      assertEquals(1, topicProduceResponse.partitionResponses.size)
      val partitionProduceResponse = topicProduceResponse.partitionResponses.asScala.head
      assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, Errors.forCode(partitionProduceResponse.errorCode))
      assertEquals(-1, partitionProduceResponse.currentLeader.leaderId())
      assertEquals(-1, partitionProduceResponse.currentLeader.leaderEpoch())
      assertEquals(0, response.data.nodeEndpoints.size)
    }
  }

  @Test
  def testTransactionalParametersSetCorrectly(): Unit = {
    val topic = "topic"
    val transactionalId = "txn1"

    addTopicToMetadataCache(topic, numPartitions = 2)

    for (version <- 3 to ApiKeys.PRODUCE.latestVersion) {

      reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      val tp = new TopicPartition("topic", 0)

      val produceRequest = ProduceRequest.forCurrentMagic(new ProduceRequestData()
        .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
          Collections.singletonList(new ProduceRequestData.TopicProduceData()
            .setName(tp.topic).setPartitionData(Collections.singletonList(
            new ProduceRequestData.PartitionProduceData()
              .setIndex(tp.partition)
              .setRecords(MemoryRecords.withTransactionalRecords(CompressionType.NONE, 0, 0, 0, new SimpleRecord("test".getBytes))))))
            .iterator))
        .setAcks(1.toShort)
        .setTransactionalId(transactionalId)
        .setTimeoutMs(5000))
        .build(version.toShort)
      val request = buildRequest(produceRequest)

      val kafkaApis = createKafkaApis()
      try {
        kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

        verify(replicaManager).handleProduceAppend(anyLong,
          anyShort,
          ArgumentMatchers.eq(false),
          ArgumentMatchers.eq(transactionalId),
          any(),
          any(),
          any(),
          any(),
          any())
      } finally {
        kafkaApis.close()
      }
    }
  }

  @Test
  def testAddPartitionsToTxnWithInvalidPartition(): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 1)

    def checkInvalidPartition(invalidPartitionId: Int): Unit = {
      reset(replicaManager, clientRequestQuotaManager, requestChannel)

      val invalidTopicPartition = new TopicPartition(topic, invalidPartitionId)
      val addPartitionsToTxnRequest = AddPartitionsToTxnRequest.Builder.forClient(
        "txnlId", 15L, 0.toShort, List(invalidTopicPartition).asJava
      ).build()
      val request = buildRequest(addPartitionsToTxnRequest)

      when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
        any[Long])).thenReturn(0)
      val kafkaApis = createKafkaApis()
      try {
        kafkaApis.handleAddPartitionsToTxnRequest(request, RequestLocal.withThreadConfinedCaching)

        val response = verifyNoThrottling[AddPartitionsToTxnResponse](request)
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, response.errors().get(AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID).get(invalidTopicPartition))
      } finally {
        kafkaApis.close()
      }
    }

    checkInvalidPartition(-1)
    checkInvalidPartition(1) // topic has only one partition
  }

  @Test
  def shouldThrowUnsupportedVersionExceptionOnHandleAddOffsetToTxnRequestWhenInterBrokerProtocolNotSupported(): Unit = {
    kafkaApis = createKafkaApis(IBP_0_10_2_IV0)
    assertThrows(classOf[UnsupportedVersionException],
      () => kafkaApis.handleAddOffsetsToTxnRequest(null, RequestLocal.withThreadConfinedCaching))
  }

  @Test
  def shouldThrowUnsupportedVersionExceptionOnHandleAddPartitionsToTxnRequestWhenInterBrokerProtocolNotSupported(): Unit = {
    kafkaApis = createKafkaApis(IBP_0_10_2_IV0)
    assertThrows(classOf[UnsupportedVersionException],
      () => kafkaApis.handleAddPartitionsToTxnRequest(null, RequestLocal.withThreadConfinedCaching))
  }

  @Test
  def shouldThrowUnsupportedVersionExceptionOnHandleTxnOffsetCommitRequestWhenInterBrokerProtocolNotSupported(): Unit = {
    kafkaApis = createKafkaApis(IBP_0_10_2_IV0)
    assertThrows(classOf[UnsupportedVersionException],
      () => kafkaApis.handleAddPartitionsToTxnRequest(null, RequestLocal.withThreadConfinedCaching))
  }

  @Test
  def shouldThrowUnsupportedVersionExceptionOnHandleEndTxnRequestWhenInterBrokerProtocolNotSupported(): Unit = {
    kafkaApis = createKafkaApis(IBP_0_10_2_IV0)
    assertThrows(classOf[UnsupportedVersionException],
      () => kafkaApis.handleEndTxnRequest(null, RequestLocal.withThreadConfinedCaching))
  }

  @Test
  def shouldThrowUnsupportedVersionExceptionOnHandleWriteTxnMarkersRequestWhenInterBrokerProtocolNotSupported(): Unit = {
    kafkaApis = createKafkaApis(IBP_0_10_2_IV0)
    assertThrows(classOf[UnsupportedVersionException],
      () => kafkaApis.handleWriteTxnMarkersRequest(null, RequestLocal.withThreadConfinedCaching))
  }

  @Test
  def shouldRespondWithUnsupportedForMessageFormatOnHandleWriteTxnMarkersWhenMagicLowerThanRequired(): Unit = {
    val topicPartition = new TopicPartition("t", 0)
    val (_, request) = createWriteTxnMarkersRequest(asList(topicPartition))
    val expectedErrors = Map(topicPartition -> Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT).asJava
    val capturedResponse: ArgumentCaptor[WriteTxnMarkersResponse] = ArgumentCaptor.forClass(classOf[WriteTxnMarkersResponse])

    when(replicaManager.getMagic(topicPartition))
      .thenReturn(Some(RecordBatch.MAGIC_VALUE_V1))
    kafkaApis = createKafkaApis()
    kafkaApis.handleWriteTxnMarkersRequest(request, RequestLocal.withThreadConfinedCaching)

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
    kafkaApis = createKafkaApis()
    kafkaApis.handleWriteTxnMarkersRequest(request, RequestLocal.withThreadConfinedCaching)

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
      ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      any(),
      responseCallback.capture(),
      any(),
      any(),
      ArgumentMatchers.eq(requestLocal),
      any(),
      any()
    )).thenAnswer(_ => responseCallback.getValue.apply(Map(tp2 -> new PartitionResponse(Errors.NONE))))
    kafkaApis = createKafkaApis()
    kafkaApis.handleWriteTxnMarkersRequest(request, requestLocal)

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
      LeaderAndIsr.InitialLeaderEpoch + 2, deletePartition = true)
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
      LeaderAndIsr.InitialLeaderEpoch + 2, deletePartition = false)
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
    kafkaApis = createKafkaApis()
    kafkaApis.handleStopReplicaRequest(request)

    if (deletePartition) {
      if (leaderEpoch >= 0) {
        verify(txnCoordinator).onResignation(txnStatePartition.partition, Some(leaderEpoch))
        verify(groupCoordinator).onResignation(groupMetadataPartition.partition, OptionalInt.of(leaderEpoch))
      } else {
        verify(txnCoordinator).onResignation(txnStatePartition.partition, None)
        verify(groupCoordinator).onResignation(groupMetadataPartition.partition, OptionalInt.empty)
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
      ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      any(),
      responseCallback.capture(),
      any(),
      any(),
      ArgumentMatchers.eq(requestLocal),
      any(),
      any()
    )).thenAnswer(_ => responseCallback.getValue.apply(Map(tp2 -> new PartitionResponse(Errors.NONE))))
    kafkaApis = createKafkaApis()
    kafkaApis.handleWriteTxnMarkersRequest(request, requestLocal)
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
    kafkaApis = createKafkaApis()
    kafkaApis.handleWriteTxnMarkersRequest(request, requestLocal)
    verify(replicaManager).appendRecords(anyLong,
      anyShort,
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      any(),
      any(),
      any(),
      any(),
      ArgumentMatchers.eq(requestLocal),
      any(),
      any())
  }

  @Test
  def testHandleWriteTxnMarkersRequestWithNewGroupCoordinator(): Unit = {
    val offset0 = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)
    val offset1 = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 1)
    val foo0 = new TopicPartition("foo", 0)
    val foo1 = new TopicPartition("foo", 1)

    val allPartitions = List(
      offset0,
      offset1,
      foo0,
      foo1
    )

    val writeTxnMarkersRequest = new WriteTxnMarkersRequest.Builder(
      ApiKeys.WRITE_TXN_MARKERS.latestVersion(),
      List(
        new TxnMarkerEntry(
          1L,
          1.toShort,
          0,
          TransactionResult.COMMIT,
          List(offset0, foo0).asJava
        ),
        new TxnMarkerEntry(
          2L,
          1.toShort,
          0,
          TransactionResult.ABORT,
          List(offset1, foo1).asJava
        )
      ).asJava
    ).build()

    val requestChannelRequest = buildRequest(writeTxnMarkersRequest)

    allPartitions.foreach { tp =>
      when(replicaManager.getMagic(tp))
        .thenReturn(Some(RecordBatch.MAGIC_VALUE_V2))
    }

    when(groupCoordinator.completeTransaction(
      ArgumentMatchers.eq(offset0),
      ArgumentMatchers.eq(1L),
      ArgumentMatchers.eq(1.toShort),
      ArgumentMatchers.eq(0),
      ArgumentMatchers.eq(TransactionResult.COMMIT),
      ArgumentMatchers.eq(Duration.ofMillis(Defaults.REQUEST_TIMEOUT_MS))
    )).thenReturn(CompletableFuture.completedFuture[Void](null))

    when(groupCoordinator.completeTransaction(
      ArgumentMatchers.eq(offset1),
      ArgumentMatchers.eq(2L),
      ArgumentMatchers.eq(1.toShort),
      ArgumentMatchers.eq(0),
      ArgumentMatchers.eq(TransactionResult.ABORT),
      ArgumentMatchers.eq(Duration.ofMillis(Defaults.REQUEST_TIMEOUT_MS))
    )).thenReturn(CompletableFuture.completedFuture[Void](null))

    val entriesPerPartition: ArgumentCaptor[Map[TopicPartition, MemoryRecords]] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, MemoryRecords]])
    val responseCallback: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])

    when(replicaManager.appendRecords(
      ArgumentMatchers.eq(Defaults.REQUEST_TIMEOUT_MS.toLong),
      ArgumentMatchers.eq(-1),
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      entriesPerPartition.capture(),
      responseCallback.capture(),
      any(),
      any(),
      ArgumentMatchers.eq(RequestLocal.NoCaching),
      any(),
      any()
    )).thenAnswer { _ =>
      responseCallback.getValue.apply(
        entriesPerPartition.getValue.keySet.map { tp =>
          tp -> new PartitionResponse(Errors.NONE)
        }.toMap
      )
    }
    kafkaApis = createKafkaApis(overrideProperties = Map(
      KafkaConfig.NewGroupCoordinatorEnableProp -> "true"
    ))
    kafkaApis.handleWriteTxnMarkersRequest(requestChannelRequest, RequestLocal.NoCaching)

    val expectedResponse = new WriteTxnMarkersResponseData()
      .setMarkers(List(
        new WriteTxnMarkersResponseData.WritableTxnMarkerResult()
          .setProducerId(1L)
          .setTopics(List(
            new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
              .setName(Topic.GROUP_METADATA_TOPIC_NAME)
              .setPartitions(List(
                new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                  .setPartitionIndex(0)
                  .setErrorCode(Errors.NONE.code)
              ).asJava),
            new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
              .setName("foo")
              .setPartitions(List(
                new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                  .setPartitionIndex(0)
                  .setErrorCode(Errors.NONE.code)
              ).asJava)
          ).asJava),
        new WriteTxnMarkersResponseData.WritableTxnMarkerResult()
          .setProducerId(2L)
          .setTopics(List(
            new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
              .setName(Topic.GROUP_METADATA_TOPIC_NAME)
              .setPartitions(List(
                new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                  .setPartitionIndex(1)
                  .setErrorCode(Errors.NONE.code)
              ).asJava),
            new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
              .setName("foo")
              .setPartitions(List(
                new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                  .setPartitionIndex(1)
                  .setErrorCode(Errors.NONE.code)
              ).asJava)
          ).asJava)
      ).asJava)

    val response = verifyNoThrottling[WriteTxnMarkersResponse](requestChannelRequest)
    assertEquals(normalize(expectedResponse), normalize(response.data))
  }

  @ParameterizedTest
  @EnumSource(value = classOf[Errors], names = Array(
    "COORDINATOR_NOT_AVAILABLE",
    "COORDINATOR_LOAD_IN_PROGRESS",
    "NOT_COORDINATOR",
    "REQUEST_TIMED_OUT"
  ))
  def testHandleWriteTxnMarkersRequestWithNewGroupCoordinatorErrorTranslation(error: Errors): Unit = {
    val offset0 = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)

    val writeTxnMarkersRequest = new WriteTxnMarkersRequest.Builder(
      ApiKeys.WRITE_TXN_MARKERS.latestVersion(),
      List(
        new TxnMarkerEntry(
          1L,
          1.toShort,
          0,
          TransactionResult.COMMIT,
          List(offset0).asJava
        )
      ).asJava
    ).build()

    val requestChannelRequest = buildRequest(writeTxnMarkersRequest)

    when(replicaManager.getMagic(offset0))
      .thenReturn(Some(RecordBatch.MAGIC_VALUE_V2))

    when(groupCoordinator.completeTransaction(
      ArgumentMatchers.eq(offset0),
      ArgumentMatchers.eq(1L),
      ArgumentMatchers.eq(1.toShort),
      ArgumentMatchers.eq(0),
      ArgumentMatchers.eq(TransactionResult.COMMIT),
      ArgumentMatchers.eq(Duration.ofMillis(Defaults.REQUEST_TIMEOUT_MS))
    )).thenReturn(FutureUtils.failedFuture[Void](error.exception()))
    kafkaApis = createKafkaApis(overrideProperties = Map(
      KafkaConfig.NewGroupCoordinatorEnableProp -> "true"
    ))
    kafkaApis.handleWriteTxnMarkersRequest(requestChannelRequest, RequestLocal.NoCaching)

    val expectedError = error match {
      case Errors.COORDINATOR_NOT_AVAILABLE | Errors.COORDINATOR_LOAD_IN_PROGRESS | Errors.NOT_COORDINATOR =>
        Errors.NOT_LEADER_OR_FOLLOWER
      case error =>
        error
    }

    val expectedResponse = new WriteTxnMarkersResponseData()
      .setMarkers(List(
        new WriteTxnMarkersResponseData.WritableTxnMarkerResult()
          .setProducerId(1L)
          .setTopics(List(
            new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
              .setName(Topic.GROUP_METADATA_TOPIC_NAME)
              .setPartitions(List(
                new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                  .setPartitionIndex(0)
                  .setErrorCode(expectedError.code)
              ).asJava)
          ).asJava)
      ).asJava)

    val response = verifyNoThrottling[WriteTxnMarkersResponse](requestChannelRequest)
    assertEquals(normalize(expectedResponse), normalize(response.data))
  }

  private def normalize(
    response: WriteTxnMarkersResponseData
  ): WriteTxnMarkersResponseData = {
    val copy = response.duplicate()
    copy.markers.sort(
      Comparator.comparingLong[WriteTxnMarkersResponseData.WritableTxnMarkerResult](_.producerId)
    )
    copy.markers.forEach { marker =>
      marker.topics.sort((t1, t2) => t1.name.compareTo(t2.name))
      marker.topics.forEach { topic =>
        topic.partitions.sort(
          Comparator.comparingInt[WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult](_.partitionIndex)
        )
      }
    }
    copy
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
  def testHandleDeleteGroups(): Unit = {
    val deleteGroupsRequest = new DeleteGroupsRequestData().setGroupsNames(List(
      "group-1",
      "group-2",
      "group-3"
    ).asJava)

    val requestChannelRequest = buildRequest(new DeleteGroupsRequest.Builder(deleteGroupsRequest).build())

    val future = new CompletableFuture[DeleteGroupsResponseData.DeletableGroupResultCollection]()
    when(groupCoordinator.deleteGroups(
      requestChannelRequest.context,
      List("group-1", "group-2", "group-3").asJava,
      RequestLocal.NoCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleDeleteGroupsRequest(
      requestChannelRequest,
      RequestLocal.NoCaching
    )

    val results = new DeleteGroupsResponseData.DeletableGroupResultCollection(List(
      new DeleteGroupsResponseData.DeletableGroupResult()
        .setGroupId("group-1")
        .setErrorCode(Errors.NONE.code),
      new DeleteGroupsResponseData.DeletableGroupResult()
        .setGroupId("group-2")
        .setErrorCode(Errors.NOT_CONTROLLER.code),
      new DeleteGroupsResponseData.DeletableGroupResult()
        .setGroupId("group-3")
        .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code),
    ).iterator.asJava)

    future.complete(results)

    val expectedDeleteGroupsResponse = new DeleteGroupsResponseData()
      .setResults(results)

    val response = verifyNoThrottling[DeleteGroupsResponse](requestChannelRequest)
    assertEquals(expectedDeleteGroupsResponse, response.data)
  }

  @Test
  def testHandleDeleteGroupsFutureFailed(): Unit = {
    val deleteGroupsRequest = new DeleteGroupsRequestData().setGroupsNames(List(
      "group-1",
      "group-2",
      "group-3"
    ).asJava)

    val requestChannelRequest = buildRequest(new DeleteGroupsRequest.Builder(deleteGroupsRequest).build())

    val future = new CompletableFuture[DeleteGroupsResponseData.DeletableGroupResultCollection]()
    when(groupCoordinator.deleteGroups(
      requestChannelRequest.context,
      List("group-1", "group-2", "group-3").asJava,
      RequestLocal.NoCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleDeleteGroupsRequest(
      requestChannelRequest,
      RequestLocal.NoCaching
    )

    future.completeExceptionally(Errors.NOT_CONTROLLER.exception)

    val expectedDeleteGroupsResponse = new DeleteGroupsResponseData()
      .setResults(new DeleteGroupsResponseData.DeletableGroupResultCollection(List(
        new DeleteGroupsResponseData.DeletableGroupResult()
          .setGroupId("group-1")
          .setErrorCode(Errors.NOT_CONTROLLER.code),
        new DeleteGroupsResponseData.DeletableGroupResult()
          .setGroupId("group-2")
          .setErrorCode(Errors.NOT_CONTROLLER.code),
        new DeleteGroupsResponseData.DeletableGroupResult()
          .setGroupId("group-3")
          .setErrorCode(Errors.NOT_CONTROLLER.code),
      ).iterator.asJava))

    val response = verifyNoThrottling[DeleteGroupsResponse](requestChannelRequest)
    assertEquals(expectedDeleteGroupsResponse, response.data)
  }

  @Test
  def testHandleDeleteGroupsAuthenticationFailed(): Unit = {
    val deleteGroupsRequest = new DeleteGroupsRequestData().setGroupsNames(List(
      "group-1",
      "group-2",
      "group-3"
    ).asJava)

    val requestChannelRequest = buildRequest(new DeleteGroupsRequest.Builder(deleteGroupsRequest).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])

    val acls = Map(
      "group-1" -> AuthorizationResult.DENIED,
      "group-2" -> AuthorizationResult.ALLOWED,
      "group-3" -> AuthorizationResult.ALLOWED
    )

    when(authorizer.authorize(
      any[RequestContext],
      any[util.List[Action]]
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      actions.asScala.map { action =>
        acls.getOrElse(action.resourcePattern.name, AuthorizationResult.DENIED)
      }.asJava
    }

    val future = new CompletableFuture[DeleteGroupsResponseData.DeletableGroupResultCollection]()
    when(groupCoordinator.deleteGroups(
      requestChannelRequest.context,
      List("group-2", "group-3").asJava,
      RequestLocal.NoCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleDeleteGroupsRequest(
      requestChannelRequest,
      RequestLocal.NoCaching
    )

    future.complete(new DeleteGroupsResponseData.DeletableGroupResultCollection(List(
      new DeleteGroupsResponseData.DeletableGroupResult()
        .setGroupId("group-2")
        .setErrorCode(Errors.NONE.code),
      new DeleteGroupsResponseData.DeletableGroupResult()
        .setGroupId("group-3")
        .setErrorCode(Errors.NONE.code)
    ).iterator.asJava))

    val expectedDeleteGroupsResponse = new DeleteGroupsResponseData()
      .setResults(new DeleteGroupsResponseData.DeletableGroupResultCollection(List(
        new DeleteGroupsResponseData.DeletableGroupResult()
          .setGroupId("group-2")
          .setErrorCode(Errors.NONE.code),
        new DeleteGroupsResponseData.DeletableGroupResult()
          .setGroupId("group-3")
          .setErrorCode(Errors.NONE.code),
        new DeleteGroupsResponseData.DeletableGroupResult()
          .setGroupId("group-1")
          .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code)).iterator.asJava))

    val response = verifyNoThrottling[DeleteGroupsResponse](requestChannelRequest)
    assertEquals(expectedDeleteGroupsResponse, response.data)
  }

  @Test
  def testHandleDescribeGroups(): Unit = {
    val describeGroupsRequest = new DescribeGroupsRequestData().setGroups(List(
      "group-1",
      "group-2",
      "group-3"
    ).asJava)

    val requestChannelRequest = buildRequest(new DescribeGroupsRequest.Builder(describeGroupsRequest).build())

    val future = new CompletableFuture[util.List[DescribeGroupsResponseData.DescribedGroup]]()
    when(groupCoordinator.describeGroups(
      requestChannelRequest.context,
      describeGroupsRequest.groups
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleDescribeGroupsRequest(requestChannelRequest)

    val groupResults = List(
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-1")
        .setProtocolType("consumer")
        .setProtocolData("range")
        .setGroupState("Stable")
        .setMembers(List(
          new DescribeGroupsResponseData.DescribedGroupMember()
            .setMemberId("member-1")).asJava),
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-2")
        .setErrorCode(Errors.NOT_COORDINATOR.code),
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-3")
        .setErrorCode(Errors.REQUEST_TIMED_OUT.code)
    ).asJava

    future.complete(groupResults)

    val expectedDescribeGroupsResponse = new DescribeGroupsResponseData().setGroups(groupResults)
    val response = verifyNoThrottling[DescribeGroupsResponse](requestChannelRequest)
    assertEquals(expectedDescribeGroupsResponse, response.data)
  }

  @Test
  def testHandleDescribeGroupsFutureFailed(): Unit = {
    val describeGroupsRequest = new DescribeGroupsRequestData().setGroups(List(
      "group-1",
      "group-2",
      "group-3"
    ).asJava)

    val requestChannelRequest = buildRequest(new DescribeGroupsRequest.Builder(describeGroupsRequest).build())

    val future = new CompletableFuture[util.List[DescribeGroupsResponseData.DescribedGroup]]()
    when(groupCoordinator.describeGroups(
      requestChannelRequest.context,
      describeGroupsRequest.groups
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleDescribeGroupsRequest(requestChannelRequest)

    val expectedDescribeGroupsResponse = new DescribeGroupsResponseData().setGroups(List(
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-1")
        .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code),
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-2")
        .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code),
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-3")
        .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code)
    ).asJava)

    future.completeExceptionally(Errors.UNKNOWN_SERVER_ERROR.exception)

    val response = verifyNoThrottling[DescribeGroupsResponse](requestChannelRequest)
    assertEquals(expectedDescribeGroupsResponse, response.data)
  }

  @Test
  def testHandleDescribeGroupsAuthenticationFailed(): Unit = {
    val describeGroupsRequest = new DescribeGroupsRequestData().setGroups(List(
      "group-1",
      "group-2",
      "group-3"
    ).asJava)

    val requestChannelRequest = buildRequest(new DescribeGroupsRequest.Builder(describeGroupsRequest).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])

    val acls = Map(
      "group-1" -> AuthorizationResult.DENIED,
      "group-2" -> AuthorizationResult.ALLOWED,
      "group-3" -> AuthorizationResult.DENIED
    )

    when(authorizer.authorize(
      any[RequestContext],
      any[util.List[Action]]
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      actions.asScala.map { action =>
        acls.getOrElse(action.resourcePattern.name, AuthorizationResult.DENIED)
      }.asJava
    }

    val future = new CompletableFuture[util.List[DescribeGroupsResponseData.DescribedGroup]]()
    when(groupCoordinator.describeGroups(
      requestChannelRequest.context,
      List("group-2").asJava
    )).thenReturn(future)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleDescribeGroupsRequest(requestChannelRequest)

    future.complete(List(
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-2")
        .setErrorCode(Errors.NOT_COORDINATOR.code)
    ).asJava)

    val expectedDescribeGroupsResponse = new DescribeGroupsResponseData().setGroups(List(
      // group-1 and group-3 are first because unauthorized are put first into the response.
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-1")
        .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code),
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-3")
        .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code),
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-2")
        .setErrorCode(Errors.NOT_COORDINATOR.code)
    ).asJava)

    val response = verifyNoThrottling[DescribeGroupsResponse](requestChannelRequest)
    assertEquals(expectedDescribeGroupsResponse, response.data)
  }

  @Test
  def testOffsetDelete(): Unit = {
    val group = "groupId"
    addTopicToMetadataCache("topic-1", numPartitions = 2)
    addTopicToMetadataCache("topic-2", numPartitions = 2)

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
    val future = new CompletableFuture[OffsetDeleteResponseData]()
    when(groupCoordinator.deleteOffsets(
      request.context,
      offsetDeleteRequest.data,
      requestLocal.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleOffsetDeleteRequest(request, requestLocal)

    val offsetDeleteResponseData = new OffsetDeleteResponseData()
      .setTopics(new OffsetDeleteResponseData.OffsetDeleteResponseTopicCollection(List(
        new OffsetDeleteResponseData.OffsetDeleteResponseTopic()
          .setName("topic-1")
          .setPartitions(new OffsetDeleteResponseData.OffsetDeleteResponsePartitionCollection(List(
            new OffsetDeleteResponseData.OffsetDeleteResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetDeleteResponseData.OffsetDeleteResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)
          ).asJava.iterator)),
        new OffsetDeleteResponseData.OffsetDeleteResponseTopic()
          .setName("topic-2")
          .setPartitions(new OffsetDeleteResponseData.OffsetDeleteResponsePartitionCollection(List(
            new OffsetDeleteResponseData.OffsetDeleteResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetDeleteResponseData.OffsetDeleteResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)
          ).asJava.iterator))
      ).asJava.iterator()))

    future.complete(offsetDeleteResponseData)

    val response = verifyNoThrottling[OffsetDeleteResponse](request)
    assertEquals(offsetDeleteResponseData, response.data)
  }

  @Test
  def testOffsetDeleteTopicsAndPartitionsValidation(): Unit = {
    val group = "groupId"
    addTopicToMetadataCache("foo", numPartitions = 2)
    addTopicToMetadataCache("bar", numPartitions = 2)

    val offsetDeleteRequest = new OffsetDeleteRequestData()
      .setGroupId(group)
      .setTopics(new OffsetDeleteRequestTopicCollection(List(
        // foo exists but has only 2 partitions.
        new OffsetDeleteRequestTopic()
          .setName("foo")
          .setPartitions(List(
            new OffsetDeleteRequestPartition().setPartitionIndex(0),
            new OffsetDeleteRequestPartition().setPartitionIndex(1),
            new OffsetDeleteRequestPartition().setPartitionIndex(2)
          ).asJava),
        // bar exists.
        new OffsetDeleteRequestTopic()
          .setName("bar")
          .setPartitions(List(
            new OffsetDeleteRequestPartition().setPartitionIndex(0),
            new OffsetDeleteRequestPartition().setPartitionIndex(1)
          ).asJava),
        // zar does not exist.
        new OffsetDeleteRequestTopic()
          .setName("zar")
          .setPartitions(List(
            new OffsetDeleteRequestPartition().setPartitionIndex(0),
            new OffsetDeleteRequestPartition().setPartitionIndex(1)
          ).asJava),
      ).asJava.iterator))

    val requestChannelRequest = buildRequest(new OffsetDeleteRequest.Builder(offsetDeleteRequest).build())

    // This is the request expected by the group coordinator. It contains
    // only existing topic-partitions.
    val expectedOffsetDeleteRequest = new OffsetDeleteRequestData()
      .setGroupId(group)
      .setTopics(new OffsetDeleteRequestTopicCollection(List(
        new OffsetDeleteRequestTopic()
          .setName("foo")
          .setPartitions(List(
            new OffsetDeleteRequestPartition().setPartitionIndex(0),
            new OffsetDeleteRequestPartition().setPartitionIndex(1)
          ).asJava),
        new OffsetDeleteRequestTopic()
          .setName("bar")
          .setPartitions(List(
            new OffsetDeleteRequestPartition().setPartitionIndex(0),
            new OffsetDeleteRequestPartition().setPartitionIndex(1)
          ).asJava)
      ).asJava.iterator))

    val future = new CompletableFuture[OffsetDeleteResponseData]()
    when(groupCoordinator.deleteOffsets(
      requestChannelRequest.context,
      expectedOffsetDeleteRequest,
      RequestLocal.NoCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(
      requestChannelRequest,
      RequestLocal.NoCaching
    )

    // This is the response returned by the group coordinator.
    val offsetDeleteResponse = new OffsetDeleteResponseData()
      .setTopics(new OffsetDeleteResponseTopicCollection(List(
        new OffsetDeleteResponseTopic()
          .setName("foo")
          .setPartitions(new OffsetDeleteResponsePartitionCollection(List(
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)
          ).asJava.iterator)),
        new OffsetDeleteResponseTopic()
          .setName("bar")
          .setPartitions(new OffsetDeleteResponsePartitionCollection(List(
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)
          ).asJava.iterator)),
      ).asJava.iterator))

    val expectedOffsetDeleteResponse = new OffsetDeleteResponseData()
      .setTopics(new OffsetDeleteResponseTopicCollection(List(
        new OffsetDeleteResponseTopic()
          .setName("foo")
          .setPartitions(new OffsetDeleteResponsePartitionCollection(List(
            // foo-2 is first because partitions failing the validation
            // are put in the response first.
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(2)
              .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code),
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)
          ).asJava.iterator)),
        // zar is before bar because topics failing the validation are
        // put in the response first.
        new OffsetDeleteResponseTopic()
          .setName("zar")
          .setPartitions(new OffsetDeleteResponsePartitionCollection(List(
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code),
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
          ).asJava.iterator)),
        new OffsetDeleteResponseTopic()
          .setName("bar")
          .setPartitions(new OffsetDeleteResponsePartitionCollection(List(
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)
          ).asJava.iterator)),
      ).asJava.iterator))

    future.complete(offsetDeleteResponse)
    val response = verifyNoThrottling[OffsetDeleteResponse](requestChannelRequest)
    assertEquals(expectedOffsetDeleteResponse, response.data)
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

      // The group coordinator is called even if there are no
      // topic-partitions left after the validation.
      when(groupCoordinator.deleteOffsets(
        request.context,
        new OffsetDeleteRequestData().setGroupId(group),
        RequestLocal.NoCaching.bufferSupplier
      )).thenReturn(CompletableFuture.completedFuture(
        new OffsetDeleteResponseData()
      ))
      val kafkaApis = createKafkaApis()
      try {
        kafkaApis.handleOffsetDeleteRequest(request, RequestLocal.NoCaching)

        val response = verifyNoThrottling[OffsetDeleteResponse](request)

        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION,
          Errors.forCode(response.data.topics.find(topic).partitions.find(invalidPartitionId).errorCode))
      } finally {
        kafkaApis.close()
      }
    }

    checkInvalidPartition(-1)
    checkInvalidPartition(1) // topic has only one partition
  }

  @Test
  def testOffsetDeleteWithInvalidGroup(): Unit = {
    val group = "groupId"
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 1)

    val offsetDeleteRequest = new OffsetDeleteRequest.Builder(
      new OffsetDeleteRequestData().setGroupId(group)
    ).build()
    val request = buildRequest(offsetDeleteRequest)

    val future = new CompletableFuture[OffsetDeleteResponseData]()
    when(groupCoordinator.deleteOffsets(
      request.context,
      offsetDeleteRequest.data,
      RequestLocal.NoCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleOffsetDeleteRequest(request, RequestLocal.NoCaching)

    future.completeExceptionally(Errors.GROUP_ID_NOT_FOUND.exception)

    val response = verifyNoThrottling[OffsetDeleteResponse](request)

    assertEquals(Errors.GROUP_ID_NOT_FOUND, Errors.forCode(response.data.errorCode))
  }

  @Test
  def testOffsetDeleteWithInvalidGroupWithTopLevelError(): Unit = {
    val group = "groupId"
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 1)

    val offsetDeleteRequest = new OffsetDeleteRequest.Builder(
      new OffsetDeleteRequestData()
        .setGroupId(group)
        .setTopics(new OffsetDeleteRequestTopicCollection(Collections.singletonList(new OffsetDeleteRequestTopic()
          .setName("topic-unknown")
          .setPartitions(Collections.singletonList(new OffsetDeleteRequestPartition()
            .setPartitionIndex(0)
          ))
        ).iterator()))
    ).build()
    val request = buildRequest(offsetDeleteRequest)

    val future = new CompletableFuture[OffsetDeleteResponseData]()
    when(groupCoordinator.deleteOffsets(
      request.context,
      new OffsetDeleteRequestData().setGroupId(group), // Nonexistent topics won't be passed to groupCoordinator.
      RequestLocal.NoCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleOffsetDeleteRequest(request, RequestLocal.NoCaching)

    future.complete(new OffsetDeleteResponseData()
      .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code())
    )

    val response = verifyNoThrottling[OffsetDeleteResponse](request)

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
    kafkaApis = createKafkaApis()
    kafkaApis.handleListOffsetRequest(request)

    val response = verifyNoThrottling[ListOffsetsResponse](request)
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
      ArgumentMatchers.eq(ConfigType.TOPIC),
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
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleTopicMetadataRequest(repByTopicId)
    val metadataByTopicIdResp = verifyNoThrottling[MetadataResponse](repByTopicId)

    val metadataByTopicId = metadataByTopicIdResp.data().topics().asScala.groupBy(_.topicId()).map(kv => (kv._1, kv._2.head))

    metadataByTopicId.foreach { case (topicId, metadataResponseTopic) =>
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
    kafkaApis.close()

    // 4. Send TopicMetadataReq using topic name
    reset(clientRequestQuotaManager, requestChannel)
    val metadataReqByTopicName = new MetadataRequest.Builder(util.Arrays.asList(authorizedTopic, unauthorizedTopic), false).build()
    val repByTopicName = buildRequest(metadataReqByTopicName, plaintextListener)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleTopicMetadataRequest(repByTopicName)
    val metadataByTopicNameResp = verifyNoThrottling[MetadataResponse](repByTopicName)

    val metadataByTopicName = metadataByTopicNameResp.data().topics().asScala.groupBy(_.name()).map(kv => (kv._1, kv._2.head))

    metadataByTopicName.foreach { case (topicName, metadataResponseTopic) =>
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

    when(replicaManager.fetchMessages(
      any[FetchParams],
      any[Seq[(TopicIdPartition, FetchRequest.PartitionData)]],
      any[ReplicaQuota],
      any[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]()
    )).thenAnswer(invocation => {
      val callback = invocation.getArgument(3).asInstanceOf[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]
      val records = MemoryRecords.withRecords(CompressionType.NONE,
        new SimpleRecord(timestamp, "foo".getBytes(StandardCharsets.UTF_8)))
      callback(Seq(tidp -> new FetchPartitionData(Errors.NONE, hw, 0, records,
        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)))
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

    val fetchRequest = new FetchRequest.Builder(9, 9, -1, -1, 100, 0, fetchDataBuilder)
      .build()
    val request = buildRequest(fetchRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleFetchRequest(request)

    val response = verifyNoThrottling[FetchResponse](request)
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
    val replicaEpoch = if (replicaId < 0) -1 else 1
    val fetchRequest = new FetchRequest.Builder(ApiKeys.FETCH.latestVersion, ApiKeys.FETCH.latestVersion,
      replicaId, replicaEpoch, 100, 0, fetchDataBuilder).metadata(fetchMetadata).build()
    val request = buildRequest(fetchRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleFetchRequest(request)

    val response = verifyNoThrottling[FetchResponse](request)
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
  def testFetchResponseContainsNewLeaderOnNotLeaderOrFollower(): Unit = {
    val topicId = Uuid.randomUuid()
    val tidp = new TopicIdPartition(topicId, new TopicPartition("foo", 0))
    val tp = tidp.topicPartition
    addTopicToMetadataCache(tp.topic, numPartitions = 1, numBrokers = 3, topicId)

    when(replicaManager.getLogConfig(ArgumentMatchers.eq(tp))).thenReturn(Some(LogConfig.fromProps(
      Collections.emptyMap(),
      new Properties()
    )))

    val partition = mock(classOf[Partition])
    val newLeaderId = 2
    val newLeaderEpoch = 5

    when(replicaManager.getPartitionOrError(tp)).thenAnswer(_ => Right(partition))
    when(partition.leaderReplicaIdOpt).thenAnswer(_ => Some(newLeaderId))
    when(partition.getLeaderEpoch).thenAnswer(_ => newLeaderEpoch)

    when(replicaManager.fetchMessages(
      any[FetchParams],
      any[Seq[(TopicIdPartition, FetchRequest.PartitionData)]],
      any[ReplicaQuota],
      any[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]()
    )).thenAnswer(invocation => {
      val callback = invocation.getArgument(3).asInstanceOf[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]
      callback(Seq(tidp -> new FetchPartitionData(Errors.NOT_LEADER_OR_FOLLOWER, UnifiedLog.UnknownOffset, UnifiedLog.UnknownOffset, MemoryRecords.EMPTY,
        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)))
    })

    val fetchData = Map(tidp -> new FetchRequest.PartitionData(topicId, 0, 0, 1000,
      Optional.empty())).asJava
    val fetchDataBuilder = Map(tp -> new FetchRequest.PartitionData(topicId, 0, 0, 1000,
      Optional.empty())).asJava
    val fetchMetadata = new JFetchMetadata(0, 0)
    val fetchContext = new FullFetchContext(time, new FetchSessionCache(1000, 100),
      fetchMetadata, fetchData, true, false)
    when(fetchManager.newContext(
      any[Short],
      any[JFetchMetadata],
      any[Boolean],
      any[util.Map[TopicIdPartition, FetchRequest.PartitionData]],
      any[util.List[TopicIdPartition]],
      any[util.Map[Uuid, String]])).thenReturn(fetchContext)

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val fetchRequest = new FetchRequest.Builder(16, 16, -1, -1, 100, 0, fetchDataBuilder)
      .build()
    val request = buildRequest(fetchRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleFetchRequest(request)

    val response = verifyNoThrottling[FetchResponse](request)
    val responseData = response.responseData(metadataCache.topicIdsToNames(), 16)

    val partitionData = responseData.get(tp)
    assertEquals(Errors.NOT_LEADER_OR_FOLLOWER.code, partitionData.errorCode)
    assertEquals(newLeaderId, partitionData.currentLeader.leaderId())
    assertEquals(newLeaderEpoch, partitionData.currentLeader.leaderEpoch())
    val node = response.data.nodeEndpoints.asScala.head
    assertEquals(2, node.nodeId)
    assertEquals("broker2", node.host)
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.JOIN_GROUP)
  def testHandleJoinGroupRequest(version: Short): Unit = {
    val joinGroupRequest = new JoinGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setProtocolType("consumer")
      .setRebalanceTimeoutMs(1000)
      .setSessionTimeoutMs(2000)

    val requestChannelRequest = buildRequest(new JoinGroupRequest.Builder(joinGroupRequest).build(version))

    val expectedJoinGroupRequest = new JoinGroupRequestData()
      .setGroupId(joinGroupRequest.groupId)
      .setMemberId(joinGroupRequest.memberId)
      .setProtocolType(joinGroupRequest.protocolType)
      .setRebalanceTimeoutMs(if (version >= 1) joinGroupRequest.rebalanceTimeoutMs else joinGroupRequest.sessionTimeoutMs)
      .setSessionTimeoutMs(joinGroupRequest.sessionTimeoutMs)

    val future = new CompletableFuture[JoinGroupResponseData]()
    when(groupCoordinator.joinGroup(
      requestChannelRequest.context,
      expectedJoinGroupRequest,
      RequestLocal.NoCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleJoinGroupRequest(
      requestChannelRequest,
      RequestLocal.NoCaching
    )

    val expectedJoinGroupResponse = new JoinGroupResponseData()
      .setMemberId("member")
      .setGenerationId(0)
      .setLeader("leader")
      .setProtocolType(if (version >= 7) "consumer" else null)
      .setProtocolName("range")

    future.complete(expectedJoinGroupResponse)
    val response = verifyNoThrottling[JoinGroupResponse](requestChannelRequest)
    assertEquals(expectedJoinGroupResponse, response.data)
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.JOIN_GROUP)
  def testJoinGroupProtocolNameBackwardCompatibility(version: Short): Unit = {
    val joinGroupRequest = new JoinGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setProtocolType("consumer")
      .setRebalanceTimeoutMs(1000)
      .setSessionTimeoutMs(2000)

    val requestChannelRequest = buildRequest(new JoinGroupRequest.Builder(joinGroupRequest).build(version))

    val expectedJoinGroupRequest = new JoinGroupRequestData()
      .setGroupId(joinGroupRequest.groupId)
      .setMemberId(joinGroupRequest.memberId)
      .setProtocolType(joinGroupRequest.protocolType)
      .setRebalanceTimeoutMs(if (version >= 1) joinGroupRequest.rebalanceTimeoutMs else joinGroupRequest.sessionTimeoutMs)
      .setSessionTimeoutMs(joinGroupRequest.sessionTimeoutMs)

    val future = new CompletableFuture[JoinGroupResponseData]()
    when(groupCoordinator.joinGroup(
      requestChannelRequest.context,
      expectedJoinGroupRequest,
      RequestLocal.NoCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleJoinGroupRequest(
      requestChannelRequest,
      RequestLocal.NoCaching
    )

    val joinGroupResponse = new JoinGroupResponseData()
      .setErrorCode(Errors.INCONSISTENT_GROUP_PROTOCOL.code)
      .setMemberId("member")
      .setProtocolName(null)

    val expectedJoinGroupResponse = new JoinGroupResponseData()
      .setErrorCode(Errors.INCONSISTENT_GROUP_PROTOCOL.code)
      .setMemberId("member")
      .setProtocolName(if (version >= 7) null else kafka.coordinator.group.GroupCoordinator.NoProtocol)

    future.complete(joinGroupResponse)
    val response = verifyNoThrottling[JoinGroupResponse](requestChannelRequest)
    assertEquals(expectedJoinGroupResponse, response.data)
  }

  @Test
  def testHandleJoinGroupRequestFutureFailed(): Unit = {
    val joinGroupRequest = new JoinGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setProtocolType("consumer")
      .setRebalanceTimeoutMs(1000)
      .setSessionTimeoutMs(2000)

    val requestChannelRequest = buildRequest(new JoinGroupRequest.Builder(joinGroupRequest).build())

    val future = new CompletableFuture[JoinGroupResponseData]()
    when(groupCoordinator.joinGroup(
      requestChannelRequest.context,
      joinGroupRequest,
      RequestLocal.NoCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleJoinGroupRequest(
      requestChannelRequest,
      RequestLocal.NoCaching
    )

    future.completeExceptionally(Errors.REQUEST_TIMED_OUT.exception)
    val response = verifyNoThrottling[JoinGroupResponse](requestChannelRequest)
    assertEquals(Errors.REQUEST_TIMED_OUT, response.error)
  }

  @Test
  def testHandleJoinGroupRequestAuthorizationFailed(): Unit = {
    val joinGroupRequest = new JoinGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setProtocolType("consumer")
      .setRebalanceTimeoutMs(1000)
      .setSessionTimeoutMs(2000)

    val requestChannelRequest = buildRequest(new JoinGroupRequest.Builder(joinGroupRequest).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleJoinGroupRequest(
      requestChannelRequest,
      RequestLocal.NoCaching
    )

    val response = verifyNoThrottling[JoinGroupResponse](requestChannelRequest)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED, response.error)
  }

  @Test
  def testHandleJoinGroupRequestUnexpectedException(): Unit = {
    val joinGroupRequest = new JoinGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setProtocolType("consumer")
      .setRebalanceTimeoutMs(1000)
      .setSessionTimeoutMs(2000)

    val requestChannelRequest = buildRequest(new JoinGroupRequest.Builder(joinGroupRequest).build())

    val future = new CompletableFuture[JoinGroupResponseData]()
    when(groupCoordinator.joinGroup(
      requestChannelRequest.context,
      joinGroupRequest,
      RequestLocal.NoCaching.bufferSupplier
    )).thenReturn(future)

    var response: JoinGroupResponse = null
    when(requestChannel.sendResponse(any(), any(), any())).thenAnswer { _ =>
      throw new Exception("Something went wrong")
    }.thenAnswer { invocation =>
      response = invocation.getArgument(1, classOf[JoinGroupResponse])
    }
    kafkaApis = createKafkaApis()
    kafkaApis.handle(
      requestChannelRequest,
      RequestLocal.NoCaching
    )

    future.completeExceptionally(Errors.NOT_COORDINATOR.exception)

    // The exception expected here is the one thrown by `sendResponse`. As
    // `Exception` is not a Kafka errors, `UNKNOWN_SERVER_ERROR` is returned.
    assertEquals(Errors.UNKNOWN_SERVER_ERROR, response.error)
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.SYNC_GROUP)
  def testHandleSyncGroupRequest(version: Short): Unit = {
    val syncGroupRequest = new SyncGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setProtocolType("consumer")
      .setProtocolName("range")

    val requestChannelRequest = buildRequest(new SyncGroupRequest.Builder(syncGroupRequest).build(version))

    val expectedSyncGroupRequest = new SyncGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setProtocolType(if (version >= 5) "consumer" else null)
      .setProtocolName(if (version >= 5) "range" else null)

    val future = new CompletableFuture[SyncGroupResponseData]()
    when(groupCoordinator.syncGroup(
      requestChannelRequest.context,
      expectedSyncGroupRequest,
      RequestLocal.NoCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleSyncGroupRequest(
      requestChannelRequest,
      RequestLocal.NoCaching
    )

    val expectedSyncGroupResponse = new SyncGroupResponseData()
      .setProtocolType(if (version >= 5 ) "consumer" else null)
      .setProtocolName(if (version >= 5 ) "range" else null)

    future.complete(expectedSyncGroupResponse)
    val response = verifyNoThrottling[SyncGroupResponse](requestChannelRequest)
    assertEquals(expectedSyncGroupResponse, response.data)
  }

  @Test
  def testHandleSyncGroupRequestFutureFailed(): Unit = {
    val syncGroupRequest = new SyncGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setProtocolType("consumer")
      .setProtocolName("range")

    val requestChannelRequest = buildRequest(new SyncGroupRequest.Builder(syncGroupRequest).build())

    val expectedSyncGroupRequest = new SyncGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setProtocolType("consumer")
      .setProtocolName("range")

    val future = new CompletableFuture[SyncGroupResponseData]()
    when(groupCoordinator.syncGroup(
      requestChannelRequest.context,
      expectedSyncGroupRequest,
      RequestLocal.NoCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleSyncGroupRequest(
      requestChannelRequest,
      RequestLocal.NoCaching
    )

    future.completeExceptionally(Errors.UNKNOWN_SERVER_ERROR.exception)
    val response = verifyNoThrottling[SyncGroupResponse](requestChannelRequest)
    assertEquals(Errors.UNKNOWN_SERVER_ERROR, response.error)
  }

  @Test
  def testHandleSyncGroupRequestAuthenticationFailed(): Unit = {
    val syncGroupRequest = new SyncGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setProtocolType("consumer")
      .setProtocolName("range")

    val requestChannelRequest = buildRequest(new SyncGroupRequest.Builder(syncGroupRequest).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleSyncGroupRequest(
      requestChannelRequest,
      RequestLocal.NoCaching
    )

    val response = verifyNoThrottling[SyncGroupResponse](requestChannelRequest)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED, response.error)
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.SYNC_GROUP)
  def testSyncGroupProtocolTypeAndNameAreMandatorySinceV5(version: Short): Unit = {
    val syncGroupRequest = new SyncGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")

    val requestChannelRequest = buildRequest(new SyncGroupRequest.Builder(syncGroupRequest).build(version))

    val expectedSyncGroupRequest = new SyncGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")

    val future = new CompletableFuture[SyncGroupResponseData]()
    when(groupCoordinator.syncGroup(
      requestChannelRequest.context,
      expectedSyncGroupRequest,
      RequestLocal.NoCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleSyncGroupRequest(
      requestChannelRequest,
      RequestLocal.NoCaching
    )

    if (version < 5) {
      future.complete(new SyncGroupResponseData()
        .setProtocolType("consumer")
        .setProtocolName("range"))
    }

    val response = verifyNoThrottling[SyncGroupResponse](requestChannelRequest)

    if (version < 5) {
      assertEquals(Errors.NONE, response.error)
    } else {
      assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL, response.error)
    }
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.HEARTBEAT)
  def testHandleHeartbeatRequest(version: Short): Unit = {
    val heartbeatRequest = new HeartbeatRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setGenerationId(0)

    val requestChannelRequest = buildRequest(new HeartbeatRequest.Builder(heartbeatRequest).build(version))

    val expectedHeartbeatRequest = new HeartbeatRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setGenerationId(0)

    val future = new CompletableFuture[HeartbeatResponseData]()
    when(groupCoordinator.heartbeat(
      requestChannelRequest.context,
      expectedHeartbeatRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleHeartbeatRequest(requestChannelRequest)

    val expectedHeartbeatResponse = new HeartbeatResponseData()
    future.complete(expectedHeartbeatResponse)
    val response = verifyNoThrottling[HeartbeatResponse](requestChannelRequest)
    assertEquals(expectedHeartbeatResponse, response.data)
  }

  @Test
  def testHandleHeartbeatRequestFutureFailed(): Unit = {
    val heartbeatRequest = new HeartbeatRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setGenerationId(0)

    val requestChannelRequest = buildRequest(new HeartbeatRequest.Builder(heartbeatRequest).build())

    val expectedHeartbeatRequest = new HeartbeatRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setGenerationId(0)

    val future = new CompletableFuture[HeartbeatResponseData]()
    when(groupCoordinator.heartbeat(
      requestChannelRequest.context,
      expectedHeartbeatRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleHeartbeatRequest(requestChannelRequest)

    future.completeExceptionally(Errors.UNKNOWN_SERVER_ERROR.exception)
    val response = verifyNoThrottling[HeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.UNKNOWN_SERVER_ERROR, response.error)
  }

  @Test
  def testHandleHeartbeatRequestAuthenticationFailed(): Unit = {
    val heartbeatRequest = new HeartbeatRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setGenerationId(0)

    val requestChannelRequest = buildRequest(new HeartbeatRequest.Builder(heartbeatRequest).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleHeartbeatRequest(
      requestChannelRequest
    )

    val response = verifyNoThrottling[HeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED, response.error)
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
    kafkaApis = createKafkaApis(IBP_2_2_IV1)
    kafkaApis.handleJoinGroupRequest(requestChannelRequest, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[JoinGroupResponse](requestChannelRequest)
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
    kafkaApis = createKafkaApis(IBP_2_2_IV1)
    kafkaApis.handleSyncGroupRequest(requestChannelRequest, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[SyncGroupResponse](requestChannelRequest)
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
    kafkaApis = createKafkaApis(IBP_2_2_IV1)
    kafkaApis.handleHeartbeatRequest(requestChannelRequest)

    val response = verifyNoThrottling[HeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.UNSUPPORTED_VERSION, response.error())
  }

  @Test
  def rejectOffsetCommitRequestWhenStaticMembershipNotSupported(): Unit = {
    val offsetCommitRequest = new OffsetCommitRequest.Builder(
      new OffsetCommitRequestData()
        .setGroupId("test")
        .setMemberId("test")
        .setGroupInstanceId("instanceId")
        .setGenerationIdOrMemberEpoch(100)
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
    kafkaApis = createKafkaApis(IBP_2_2_IV1)
    kafkaApis.handleOffsetCommitRequest(requestChannelRequest, RequestLocal.withThreadConfinedCaching)

    val expectedTopicErrors = Collections.singletonList(
      new OffsetCommitResponseData.OffsetCommitResponseTopic()
        .setName("test")
        .setPartitions(Collections.singletonList(
          new OffsetCommitResponseData.OffsetCommitResponsePartition()
            .setPartitionIndex(0)
            .setErrorCode(Errors.UNSUPPORTED_VERSION.code)
        ))
    )
    val response = verifyNoThrottling[OffsetCommitResponse](requestChannelRequest)
    assertEquals(expectedTopicErrors, response.data.topics())
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.LEAVE_GROUP)
  def testHandleLeaveGroupWithMultipleMembers(version: Short): Unit = {
    def makeRequest(version: Short): RequestChannel.Request = {
      buildRequest(new LeaveGroupRequest.Builder(
        "group",
        List(
          new MemberIdentity()
            .setMemberId("member-1")
            .setGroupInstanceId("instance-1"),
          new MemberIdentity()
            .setMemberId("member-2")
            .setGroupInstanceId("instance-2")
        ).asJava
      ).build(version))
    }

    if (version < 3) {
      // Request version earlier than version 3 do not support batching members.
      assertThrows(classOf[UnsupportedVersionException], () => makeRequest(version))
    } else {
      val requestChannelRequest = makeRequest(version)

      val expectedLeaveGroupRequest = new LeaveGroupRequestData()
        .setGroupId("group")
        .setMembers(List(
          new MemberIdentity()
            .setMemberId("member-1")
            .setGroupInstanceId("instance-1"),
          new MemberIdentity()
            .setMemberId("member-2")
            .setGroupInstanceId("instance-2")
        ).asJava)

      val future = new CompletableFuture[LeaveGroupResponseData]()
      when(groupCoordinator.leaveGroup(
        requestChannelRequest.context,
        expectedLeaveGroupRequest
      )).thenReturn(future)
      kafkaApis = createKafkaApis()
      kafkaApis.handleLeaveGroupRequest(requestChannelRequest)

      val expectedLeaveResponse = new LeaveGroupResponseData()
        .setErrorCode(Errors.NONE.code)
        .setMembers(List(
          new LeaveGroupResponseData.MemberResponse()
            .setMemberId("member-1")
            .setGroupInstanceId("instance-1"),
          new LeaveGroupResponseData.MemberResponse()
            .setMemberId("member-2")
            .setGroupInstanceId("instance-2"),
        ).asJava)

      future.complete(expectedLeaveResponse)
      val response = verifyNoThrottling[LeaveGroupResponse](requestChannelRequest)
      assertEquals(expectedLeaveResponse, response.data)
    }
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.LEAVE_GROUP)
  def testHandleLeaveGroupWithSingleMember(version: Short): Unit = {
    val requestChannelRequest = buildRequest(new LeaveGroupRequest.Builder(
      "group",
      List(
        new MemberIdentity()
          .setMemberId("member-1")
          .setGroupInstanceId("instance-1")
      ).asJava
    ).build(version))

    val expectedLeaveGroupRequest = new LeaveGroupRequestData()
      .setGroupId("group")
      .setMembers(List(
        new MemberIdentity()
          .setMemberId("member-1")
          .setGroupInstanceId(if (version >= 3) "instance-1" else null)
      ).asJava)

    val future = new CompletableFuture[LeaveGroupResponseData]()
    when(groupCoordinator.leaveGroup(
      requestChannelRequest.context,
      expectedLeaveGroupRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleLeaveGroupRequest(requestChannelRequest)

    val leaveGroupResponse = new LeaveGroupResponseData()
      .setErrorCode(Errors.NONE.code)
      .setMembers(List(
        new LeaveGroupResponseData.MemberResponse()
          .setMemberId("member-1")
          .setGroupInstanceId("instance-1")
      ).asJava)

    val expectedLeaveResponse = if (version >= 3) {
      new LeaveGroupResponseData()
        .setErrorCode(Errors.NONE.code)
        .setMembers(List(
          new LeaveGroupResponseData.MemberResponse()
            .setMemberId("member-1")
            .setGroupInstanceId("instance-1")
        ).asJava)
    } else {
      new LeaveGroupResponseData()
        .setErrorCode(Errors.NONE.code)
    }

    future.complete(leaveGroupResponse)
    val response = verifyNoThrottling[LeaveGroupResponse](requestChannelRequest)
    assertEquals(expectedLeaveResponse, response.data)
  }

  @Test
  def testHandleLeaveGroupFutureFailed(): Unit = {
    val requestChannelRequest = buildRequest(new LeaveGroupRequest.Builder(
      "group",
      List(
        new MemberIdentity()
          .setMemberId("member-1")
          .setGroupInstanceId("instance-1")
      ).asJava
    ).build(ApiKeys.LEAVE_GROUP.latestVersion))

    val expectedLeaveGroupRequest = new LeaveGroupRequestData()
      .setGroupId("group")
      .setMembers(List(
        new MemberIdentity()
          .setMemberId("member-1")
          .setGroupInstanceId("instance-1")
      ).asJava)

    val future = new CompletableFuture[LeaveGroupResponseData]()
    when(groupCoordinator.leaveGroup(
      requestChannelRequest.context,
      expectedLeaveGroupRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleLeaveGroupRequest(requestChannelRequest)

    future.completeExceptionally(Errors.UNKNOWN_SERVER_ERROR.exception)
    val response = verifyNoThrottling[LeaveGroupResponse](requestChannelRequest)
    assertEquals(Errors.UNKNOWN_SERVER_ERROR, response.error)
  }

  @Test
  def testHandleLeaveGroupAuthenticationFailed(): Unit = {
    val requestChannelRequest = buildRequest(new LeaveGroupRequest.Builder(
      "group",
      List(
        new MemberIdentity()
          .setMemberId("member-1")
          .setGroupInstanceId("instance-1")
      ).asJava
    ).build(ApiKeys.LEAVE_GROUP.latestVersion))

    val expectedLeaveGroupRequest = new LeaveGroupRequestData()
      .setGroupId("group")
      .setMembers(List(
        new MemberIdentity()
          .setMemberId("member-1")
          .setGroupInstanceId("instance-1")
      ).asJava)

    val future = new CompletableFuture[LeaveGroupResponseData]()
    when(groupCoordinator.leaveGroup(
      requestChannelRequest.context,
      expectedLeaveGroupRequest
    )).thenReturn(future)

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleLeaveGroupRequest(requestChannelRequest)

    val response = verifyNoThrottling[LeaveGroupResponse](requestChannelRequest)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED, response.error)
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH)
  def testHandleOffsetFetchWithMultipleGroups(version: Short): Unit = {
    // Version 0 gets offsets from Zookeeper. We are not interested
    // in testing this here.
    if (version == 0) return

    def makeRequest(version: Short): RequestChannel.Request = {
      val groups = Map(
        "group-1" -> List(
          new TopicPartition("foo", 0),
          new TopicPartition("foo", 1)
        ).asJava,
        "group-2" -> null,
        "group-3" -> null,
        "group-4" -> null,
      ).asJava
      buildRequest(new OffsetFetchRequest.Builder(groups, false, false).build(version))
    }

    if (version < 8) {
      // Request version earlier than version 8 do not support batching groups.
      assertThrows(classOf[UnsupportedVersionException], () => makeRequest(version))
    } else {
      val requestChannelRequest = makeRequest(version)

      val group1Future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
      when(groupCoordinator.fetchOffsets(
        requestChannelRequest.context,
        new OffsetFetchRequestData.OffsetFetchRequestGroup()
          .setGroupId("group-1")
          .setTopics(List(
            new OffsetFetchRequestData.OffsetFetchRequestTopics()
              .setName("foo")
              .setPartitionIndexes(List[Integer](0, 1).asJava)).asJava),
        false
      )).thenReturn(group1Future)

      val group2Future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
      when(groupCoordinator.fetchAllOffsets(
        requestChannelRequest.context,
        new OffsetFetchRequestData.OffsetFetchRequestGroup()
          .setGroupId("group-2")
          .setTopics(null),
        false
      )).thenReturn(group2Future)

      val group3Future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
      when(groupCoordinator.fetchAllOffsets(
        requestChannelRequest.context,
        new OffsetFetchRequestData.OffsetFetchRequestGroup()
          .setGroupId("group-3")
          .setTopics(null),
        false
      )).thenReturn(group3Future)

      val group4Future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
      when(groupCoordinator.fetchAllOffsets(
        requestChannelRequest.context,
        new OffsetFetchRequestData.OffsetFetchRequestGroup()
          .setGroupId("group-4")
          .setTopics(null),
        false
      )).thenReturn(group4Future)
      kafkaApis = createKafkaApis()
      kafkaApis.handleOffsetFetchRequest(requestChannelRequest)

      val group1Response = new OffsetFetchResponseData.OffsetFetchResponseGroup()
        .setGroupId("group-1")
        .setTopics(List(
          new OffsetFetchResponseData.OffsetFetchResponseTopics()
            .setName("foo")
            .setPartitions(List(
              new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                .setPartitionIndex(0)
                .setCommittedOffset(100)
                .setCommittedLeaderEpoch(1),
              new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                .setPartitionIndex(1)
                .setCommittedOffset(200)
                .setCommittedLeaderEpoch(2)
            ).asJava)
        ).asJava)

      val group2Response = new OffsetFetchResponseData.OffsetFetchResponseGroup()
        .setGroupId("group-2")
        .setTopics(List(
          new OffsetFetchResponseData.OffsetFetchResponseTopics()
            .setName("bar")
            .setPartitions(List(
              new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                .setPartitionIndex(0)
                .setCommittedOffset(100)
                .setCommittedLeaderEpoch(1),
              new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                .setPartitionIndex(1)
                .setCommittedOffset(200)
                .setCommittedLeaderEpoch(2),
              new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                .setPartitionIndex(2)
                .setCommittedOffset(300)
                .setCommittedLeaderEpoch(3)
            ).asJava)
        ).asJava)

      val group3Response = new OffsetFetchResponseData.OffsetFetchResponseGroup()
        .setGroupId("group-3")
        .setErrorCode(Errors.INVALID_GROUP_ID.code)

      val group4Response = new OffsetFetchResponseData.OffsetFetchResponseGroup()
        .setGroupId("group-4")
        .setErrorCode(Errors.INVALID_GROUP_ID.code)

      val expectedGroups = List(group1Response, group2Response, group3Response, group4Response)

      group1Future.complete(group1Response)
      group2Future.complete(group2Response)
      group3Future.completeExceptionally(Errors.INVALID_GROUP_ID.exception)
      group4Future.complete(group4Response)

      val response = verifyNoThrottling[OffsetFetchResponse](requestChannelRequest)
      assertEquals(expectedGroups.toSet, response.data.groups().asScala.toSet)
    }
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH)
  def testHandleOffsetFetchWithSingleGroup(version: Short): Unit = {
    // Version 0 gets offsets from Zookeeper. We are not interested
    // in testing this here.
    if (version == 0) return

    def makeRequest(version: Short): RequestChannel.Request = {
      buildRequest(new OffsetFetchRequest.Builder(
        "group-1",
        false,
        List(
          new TopicPartition("foo", 0),
          new TopicPartition("foo", 1)
        ).asJava,
        false
      ).build(version))
    }

    val requestChannelRequest = makeRequest(version)

    val future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
    when(groupCoordinator.fetchOffsets(
      requestChannelRequest.context,
      new OffsetFetchRequestData.OffsetFetchRequestGroup()
        .setGroupId("group-1")
        .setTopics(List(new OffsetFetchRequestData.OffsetFetchRequestTopics()
          .setName("foo")
          .setPartitionIndexes(List[Integer](0, 1).asJava)).asJava),
      false
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleOffsetFetchRequest(requestChannelRequest)

    val group1Response = new OffsetFetchResponseData.OffsetFetchResponseGroup()
      .setGroupId("group-1")
      .setTopics(List(
        new OffsetFetchResponseData.OffsetFetchResponseTopics()
          .setName("foo")
          .setPartitions(List(
            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(0)
              .setCommittedOffset(100)
              .setCommittedLeaderEpoch(1),
            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(1)
              .setCommittedOffset(200)
              .setCommittedLeaderEpoch(2)
          ).asJava)
      ).asJava)

    val expectedOffsetFetchResponse = if (version >= 8) {
      new OffsetFetchResponseData()
        .setGroups(List(group1Response).asJava)
    } else {
      new OffsetFetchResponseData()
        .setTopics(List(
          new OffsetFetchResponseData.OffsetFetchResponseTopic()
            .setName("foo")
            .setPartitions(List(
              new OffsetFetchResponseData.OffsetFetchResponsePartition()
                .setPartitionIndex(0)
                .setCommittedOffset(100)
                .setCommittedLeaderEpoch(if (version >= 5) 1 else -1),
              new OffsetFetchResponseData.OffsetFetchResponsePartition()
                .setPartitionIndex(1)
                .setCommittedOffset(200)
                .setCommittedLeaderEpoch(if (version >= 5) 2 else -1)
            ).asJava)
        ).asJava)
    }

    future.complete(group1Response)

    val response = verifyNoThrottling[OffsetFetchResponse](requestChannelRequest)
    assertEquals(expectedOffsetFetchResponse, response.data)
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH)
  def testHandleOffsetFetchAllOffsetsWithSingleGroup(version: Short): Unit = {
    // Version 0 gets offsets from Zookeeper. Version 1 does not support fetching all
    // offsets request. We are not interested in testing these here.
    if (version < 2) return

    def makeRequest(version: Short): RequestChannel.Request = {
      buildRequest(new OffsetFetchRequest.Builder(
        "group-1",
        false,
        null, // all offsets.
        false
      ).build(version))
    }

    val requestChannelRequest = makeRequest(version)

    val future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
    when(groupCoordinator.fetchAllOffsets(
      requestChannelRequest.context,
      new OffsetFetchRequestData.OffsetFetchRequestGroup()
        .setGroupId("group-1")
        .setTopics(null),
      false
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleOffsetFetchRequest(requestChannelRequest)

    val group1Response = new OffsetFetchResponseData.OffsetFetchResponseGroup()
      .setGroupId("group-1")
      .setTopics(List(
        new OffsetFetchResponseData.OffsetFetchResponseTopics()
          .setName("foo")
          .setPartitions(List(
            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(0)
              .setCommittedOffset(100)
              .setCommittedLeaderEpoch(1),
            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(1)
              .setCommittedOffset(200)
              .setCommittedLeaderEpoch(2)
          ).asJava)
      ).asJava)

    val expectedOffsetFetchResponse = if (version >= 8) {
      new OffsetFetchResponseData()
        .setGroups(List(group1Response).asJava)
    } else {
      new OffsetFetchResponseData()
        .setTopics(List(
          new OffsetFetchResponseData.OffsetFetchResponseTopic()
            .setName("foo")
            .setPartitions(List(
              new OffsetFetchResponseData.OffsetFetchResponsePartition()
                .setPartitionIndex(0)
                .setCommittedOffset(100)
                .setCommittedLeaderEpoch(if (version >= 5) 1 else -1),
              new OffsetFetchResponseData.OffsetFetchResponsePartition()
                .setPartitionIndex(1)
                .setCommittedOffset(200)
                .setCommittedLeaderEpoch(if (version >= 5) 2 else -1)
            ).asJava)
        ).asJava)
    }

    future.complete(group1Response)

    val response = verifyNoThrottling[OffsetFetchResponse](requestChannelRequest)
    assertEquals(expectedOffsetFetchResponse, response.data)
  }

  @Test
  def testHandleOffsetFetchAuthorization(): Unit = {
    def makeRequest(version: Short): RequestChannel.Request = {
      val groups = Map(
        "group-1" -> List(
          new TopicPartition("foo", 0),
          new TopicPartition("bar", 0)
        ).asJava,
        "group-2" -> List(
          new TopicPartition("foo", 0),
          new TopicPartition("bar", 0)
        ).asJava,
        "group-3" -> null,
        "group-4" -> null,
      ).asJava
      buildRequest(new OffsetFetchRequest.Builder(groups, false, false).build(version))
    }

    val requestChannelRequest = makeRequest(ApiKeys.OFFSET_FETCH.latestVersion)

    val authorizer: Authorizer = mock(classOf[Authorizer])

    val acls = Map(
      "group-1" -> AuthorizationResult.ALLOWED,
      "group-2" -> AuthorizationResult.DENIED,
      "group-3" -> AuthorizationResult.ALLOWED,
      "group-4" -> AuthorizationResult.DENIED,
      "foo" -> AuthorizationResult.DENIED,
      "bar" -> AuthorizationResult.ALLOWED
    )

    when(authorizer.authorize(
      any[RequestContext],
      any[util.List[Action]]
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      actions.asScala.map { action =>
        acls.getOrElse(action.resourcePattern.name, AuthorizationResult.DENIED)
      }.asJava
    }

    // group-1 is allowed and bar is allowed.
    val group1Future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
    when(groupCoordinator.fetchOffsets(
      requestChannelRequest.context,
      new OffsetFetchRequestData.OffsetFetchRequestGroup()
        .setGroupId("group-1")
        .setTopics(List(new OffsetFetchRequestData.OffsetFetchRequestTopics()
          .setName("bar")
          .setPartitionIndexes(List[Integer](0).asJava)).asJava),
      false
    )).thenReturn(group1Future)

    // group-3 is allowed and bar is allowed.
    val group3Future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
    when(groupCoordinator.fetchAllOffsets(
      requestChannelRequest.context,
      new OffsetFetchRequestData.OffsetFetchRequestGroup()
        .setGroupId("group-3")
        .setTopics(null),
      false
    )).thenReturn(group3Future)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handle(requestChannelRequest, RequestLocal.NoCaching)

    val group1ResponseFromCoordinator = new OffsetFetchResponseData.OffsetFetchResponseGroup()
      .setGroupId("group-1")
      .setTopics(List(
        new OffsetFetchResponseData.OffsetFetchResponseTopics()
          .setName("bar")
          .setPartitions(List(
            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(0)
              .setCommittedOffset(100)
              .setCommittedLeaderEpoch(1)
          ).asJava)
      ).asJava)

    val group3ResponseFromCoordinator = new OffsetFetchResponseData.OffsetFetchResponseGroup()
      .setGroupId("group-3")
      .setTopics(List(
        // foo should be filtered out.
        new OffsetFetchResponseData.OffsetFetchResponseTopics()
          .setName("foo")
          .setPartitions(List(
            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(0)
              .setCommittedOffset(100)
              .setCommittedLeaderEpoch(1)
          ).asJava),
        new OffsetFetchResponseData.OffsetFetchResponseTopics()
          .setName("bar")
          .setPartitions(List(
            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(0)
              .setCommittedOffset(100)
              .setCommittedLeaderEpoch(1)
          ).asJava)
      ).asJava)

    val expectedOffsetFetchResponse = new OffsetFetchResponseData()
      .setGroups(List(
        // group-1 is authorized but foo is not.
        new OffsetFetchResponseData.OffsetFetchResponseGroup()
          .setGroupId("group-1")
          .setTopics(List(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
              .setName("bar")
              .setPartitions(List(
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(0)
                  .setCommittedOffset(100)
                  .setCommittedLeaderEpoch(1)
              ).asJava),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
              .setName("foo")
              .setPartitions(List(
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(0)
                  .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
                  .setCommittedOffset(-1)
              ).asJava)
          ).asJava),
        // group-2 is not authorized.
        new OffsetFetchResponseData.OffsetFetchResponseGroup()
          .setGroupId("group-2")
          .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code),
        // group-3 is authorized but foo is not.
        new OffsetFetchResponseData.OffsetFetchResponseGroup()
          .setGroupId("group-3")
          .setTopics(List(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
              .setName("bar")
              .setPartitions(List(
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(0)
                  .setCommittedOffset(100)
                  .setCommittedLeaderEpoch(1)
              ).asJava)
          ).asJava),
        // group-4 is not authorized.
        new OffsetFetchResponseData.OffsetFetchResponseGroup()
          .setGroupId("group-4")
          .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code),
      ).asJava)

    group1Future.complete(group1ResponseFromCoordinator)
    group3Future.complete(group3ResponseFromCoordinator)

    val response = verifyNoThrottling[OffsetFetchResponse](requestChannelRequest)
    assertEquals(expectedOffsetFetchResponse, response.data)
  }

  @Test
  def testHandleOffsetFetchWithUnauthorizedTopicAndTopLevelError(): Unit = {
    def makeRequest(version: Short): RequestChannel.Request = {
      val groups = Map(
        "group-1" -> List(
          new TopicPartition("foo", 0),
          new TopicPartition("bar", 0)
        ).asJava,
        "group-2" -> List(
          new TopicPartition("foo", 0),
          new TopicPartition("bar", 0)
        ).asJava
      ).asJava
      buildRequest(new OffsetFetchRequest.Builder(groups, false, false).build(version))
    }

    val requestChannelRequest = makeRequest(ApiKeys.OFFSET_FETCH.latestVersion)

    val authorizer: Authorizer = mock(classOf[Authorizer])

    val acls = Map(
      "group-1" -> AuthorizationResult.ALLOWED,
      "group-2" -> AuthorizationResult.ALLOWED,
      "foo" -> AuthorizationResult.DENIED,
      "bar" -> AuthorizationResult.ALLOWED
    )

    when(authorizer.authorize(
      any[RequestContext],
      any[util.List[Action]]
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      actions.asScala.map { action =>
        acls.getOrElse(action.resourcePattern.name, AuthorizationResult.DENIED)
      }.asJava
    }

    // group-1 and group-2 are allowed and bar is allowed.
    val group1Future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
    when(groupCoordinator.fetchOffsets(
      requestChannelRequest.context,
      new OffsetFetchRequestData.OffsetFetchRequestGroup()
        .setGroupId("group-1")
        .setTopics(List(new OffsetFetchRequestData.OffsetFetchRequestTopics()
          .setName("bar")
          .setPartitionIndexes(List[Integer](0).asJava)).asJava),
      false
    )).thenReturn(group1Future)

    val group2Future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
    when(groupCoordinator.fetchOffsets(
      requestChannelRequest.context,
      new OffsetFetchRequestData.OffsetFetchRequestGroup()
        .setGroupId("group-2")
        .setTopics(List(new OffsetFetchRequestData.OffsetFetchRequestTopics()
          .setName("bar")
          .setPartitionIndexes(List[Integer](0).asJava)).asJava),
      false
    )).thenReturn(group1Future)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handle(requestChannelRequest, RequestLocal.NoCaching)

    // group-2 mocks using the new group coordinator.
    // When the coordinator is not active, a response with top-level error code is returned
    // despite that the requested topic is not authorized and fails.
    val group2ResponseFromCoordinator = new OffsetFetchResponseData.OffsetFetchResponseGroup()
      .setGroupId("group-2")
      .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code)

    val expectedOffsetFetchResponse = new OffsetFetchResponseData()
      .setGroups(List(
        new OffsetFetchResponseData.OffsetFetchResponseGroup()
          .setGroupId("group-1")
          .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code),
        group2ResponseFromCoordinator
      ).asJava)

    group1Future.completeExceptionally(Errors.COORDINATOR_NOT_AVAILABLE.exception)
    group2Future.complete(group2ResponseFromCoordinator)

    val response = verifyNoThrottling[OffsetFetchResponse](requestChannelRequest)
    assertEquals(expectedOffsetFetchResponse, response.data)
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
      ApiKeys.FETCH.oldestVersion(), ApiKeys.FETCH.latestVersion(), 1, 1, 1000, 0, fetchDataBuilder).build())

    val records = MemoryRecords.withRecords(CompressionType.NONE,
      new SimpleRecord(1000, "foo".getBytes(StandardCharsets.UTF_8)))
    when(replicaManager.fetchMessages(
      any[FetchParams],
      any[Seq[(TopicIdPartition, FetchRequest.PartitionData)]],
      any[ReplicaQuota],
      any[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]()
    )).thenAnswer(invocation => {
      val callback = invocation.getArgument(3).asInstanceOf[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]
      callback(Seq(tidp0 -> new FetchPartitionData(Errors.NONE, hw, 0, records,
        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), isReassigning)))
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
    kafkaApis = createKafkaApis()
    kafkaApis.handle(fetchFromFollower, RequestLocal.withThreadConfinedCaching)
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
    kafkaApis = createKafkaApis(IBP_2_2_IV1)
    kafkaApis.handleInitProducerIdRequest(requestChannelRequest, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[InitProducerIdResponse](requestChannelRequest)
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
    kafkaApis = createKafkaApis(IBP_2_2_IV1)
    kafkaApis.handleInitProducerIdRequest(requestChannelRequest, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[InitProducerIdResponse](requestChannelRequest)
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
    kafkaApis = createKafkaApis()
    kafkaApis.handleUpdateMetadataRequest(request, RequestLocal.withThreadConfinedCaching)
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
        .setPartitionEpoch(2)
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
    kafkaApis = createKafkaApis()
    kafkaApis.handleLeaderAndIsrRequest(request)
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
    kafkaApis = createKafkaApis()
    kafkaApis.handleStopReplicaRequest(request)
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None)
    )
    val stopReplicaResponse = capturedResponse.getValue
    assertEquals(expectedError, stopReplicaResponse.error())
    if (expectedError != Errors.STALE_BROKER_EPOCH) {
      verify(replicaManager).stopReplicas(
        ArgumentMatchers.eq(request.context.correlationId),
        ArgumentMatchers.eq(controllerId),
        ArgumentMatchers.eq(controllerEpoch),
        ArgumentMatchers.eq(stopReplicaRequest.partitionStates().asScala)
      )
    }
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.LIST_GROUPS)
  def testListGroupsRequest(version: Short): Unit = {
    val listGroupsRequest = new ListGroupsRequestData()
      .setStatesFilter(if (version >= 4) List("Stable", "Empty").asJava else List.empty.asJava)
      .setTypesFilter(if (version >= 5) List("classic", "consumer").asJava else List.empty.asJava)

    val requestChannelRequest = buildRequest(new ListGroupsRequest.Builder(listGroupsRequest).build(version))

    val expectedListGroupsRequest = new ListGroupsRequestData()
      .setStatesFilter(if (version >= 4) List("Stable", "Empty").asJava else List.empty.asJava)
      .setTypesFilter(if (version >= 5) List("classic", "consumer").asJava else List.empty.asJava)

    val future = new CompletableFuture[ListGroupsResponseData]()
    when(groupCoordinator.listGroups(
      requestChannelRequest.context,
      expectedListGroupsRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleListGroupsRequest(requestChannelRequest)

    val expectedListGroupsResponse = new ListGroupsResponseData()
      .setGroups(List(
        new ListGroupsResponseData.ListedGroup()
          .setGroupId("group1")
          .setProtocolType("protocol1")
          .setGroupState(if (version >= 4) "Stable" else "")
          .setGroupType(if (version >= 5) "consumer" else ""),
        new ListGroupsResponseData.ListedGroup()
          .setGroupId("group2")
          .setProtocolType("protocol2")
          .setGroupState(if (version >= 4) "Empty" else "")
          .setGroupType(if (version >= 5) "classic" else ""),
        new ListGroupsResponseData.ListedGroup()
          .setGroupId("group3")
          .setProtocolType("protocol3")
          .setGroupState(if (version >= 4) "Stable" else "")
          .setGroupType(if (version >= 5) "classic" else ""),
      ).asJava)

    future.complete(expectedListGroupsResponse)
    val response = verifyNoThrottling[ListGroupsResponse](requestChannelRequest)
    assertEquals(expectedListGroupsResponse, response.data)
  }

  @Test
  def testListGroupsRequestFutureFailed(): Unit = {
    val listGroupsRequest = new ListGroupsRequestData()
      .setStatesFilter(List("Stable", "Empty").asJava)
      .setTypesFilter(List("classic", "consumer").asJava)

    val requestChannelRequest = buildRequest(new ListGroupsRequest.Builder(listGroupsRequest).build())

    val expectedListGroupsRequest = new ListGroupsRequestData()
      .setStatesFilter(List("Stable", "Empty").asJava)
      .setTypesFilter(List("classic", "consumer").asJava)

    val future = new CompletableFuture[ListGroupsResponseData]()
    when(groupCoordinator.listGroups(
      requestChannelRequest.context,
      expectedListGroupsRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleListGroupsRequest(requestChannelRequest)

    future.completeExceptionally(Errors.UNKNOWN_SERVER_ERROR.exception)
    val response = verifyNoThrottling[ListGroupsResponse](requestChannelRequest)
    assertEquals(Errors.UNKNOWN_SERVER_ERROR.code, response.data.errorCode)
  }

  @Test
  def testListGroupsRequestFiltersUnauthorizedGroupsWithDescribeCluster(): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])

    authorizeResource(
      authorizer,
      AclOperation.DESCRIBE,
      ResourceType.GROUP,
      "group1",
      AuthorizationResult.DENIED,
      logIfDenied = false
    )
    authorizeResource(
      authorizer,
      AclOperation.DESCRIBE,
      ResourceType.GROUP,
      "group2",
      AuthorizationResult.DENIED,
      logIfDenied = false
    )
    authorizeResource(
      authorizer,
      AclOperation.DESCRIBE,
      ResourceType.CLUSTER,
      Resource.CLUSTER_NAME,
      AuthorizationResult.ALLOWED,
      logIfDenied = false
    )

    testListGroupsRequestFiltersUnauthorizedGroups(
      authorizer,
      List("group1", "group2"),
      List("group1", "group2")
    )
  }

  @Test
  def testListGroupsRequestFiltersUnauthorizedGroupsWithDescribeGroups(): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])

    authorizeResource(
      authorizer,
      AclOperation.DESCRIBE,
      ResourceType.GROUP,
      "group1",
      AuthorizationResult.DENIED,
      logIfDenied = false
    )
    authorizeResource(
      authorizer,
      AclOperation.DESCRIBE,
      ResourceType.GROUP,
      "group2",
      AuthorizationResult.ALLOWED,
      logIfDenied = false
    )
    authorizeResource(
      authorizer,
      AclOperation.DESCRIBE,
      ResourceType.CLUSTER,
      Resource.CLUSTER_NAME,
      AuthorizationResult.DENIED,
      logIfDenied = false
    )

    testListGroupsRequestFiltersUnauthorizedGroups(
      authorizer,
      List("group1", "group2"),
      List("group2")
    )
  }

  def testListGroupsRequestFiltersUnauthorizedGroups(
    authorizer: Authorizer,
    groups: List[String],
    expectedGroups: List[String],
  ): Unit = {
    val listGroupsRequest = new ListGroupsRequestData()

    val requestChannelRequest = buildRequest(new ListGroupsRequest.Builder(listGroupsRequest).build())

    val expectedListGroupsRequest = new ListGroupsRequestData()

    val future = new CompletableFuture[ListGroupsResponseData]()
    when(groupCoordinator.listGroups(
      requestChannelRequest.context,
      expectedListGroupsRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleListGroupsRequest(requestChannelRequest)

    val listGroupsResponse = new ListGroupsResponseData()
    groups.foreach { groupId =>
      listGroupsResponse.groups.add(new ListGroupsResponseData.ListedGroup()
        .setGroupId(groupId)
      )
    }

    val expectedListGroupsResponse = new ListGroupsResponseData()
    expectedGroups.foreach { groupId =>
      expectedListGroupsResponse.groups.add(new ListGroupsResponseData.ListedGroup()
        .setGroupId(groupId)
      )
    }

    future.complete(listGroupsResponse)
    val response = verifyNoThrottling[ListGroupsResponse](requestChannelRequest)
    assertEquals(expectedListGroupsResponse, response.data)
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
    kafkaApis = createKafkaApis()
    kafkaApis.handleDescribeCluster(request)

    val describeClusterResponse = verifyNoThrottling[DescribeClusterResponse](request)

    assertEquals(metadataCache.getControllerId.get.id, describeClusterResponse.data.controllerId)
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
    kafkaApis = createKafkaApis()
    kafkaApis.handleTopicMetadataRequest(requestChannelRequest)

    verifyNoThrottling[MetadataResponse](requestChannelRequest)
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
    kafkaApis = createKafkaApis()
    kafkaApis.handleListOffsetRequest(request)

    val response = verifyNoThrottling[ListOffsetsResponse](request)
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
                           requestHeader: Option[RequestHeader] = None,
                           requestMetrics: RequestChannel.Metrics = requestChannelMetrics): RequestChannel.Request = {
    val buffer = request.serializeWithHeader(
      requestHeader.getOrElse(new RequestHeader(request.apiKey, request.version, clientId, 0)))

    // read the header from the buffer first so that the body can be read next from the Request constructor
    val header = RequestHeader.parse(buffer)
    // DelegationTokens require the context authenticated to be non SecurityProtocol.PLAINTEXT
    // and have a non KafkaPrincipal.ANONYMOUS principal. This test is done before the check
    // for forwarding because after forwarding the context will have a different context. 
    // We validate the context authenticated failure case in other integration tests.
    val context = new RequestContext(header, "1", InetAddress.getLocalHost, Optional.empty(),
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice"), listenerName, SecurityProtocol.SSL,
      ClientInformation.EMPTY, fromPrivilegedListener, Optional.of(kafkaPrincipalSerde))
    new RequestChannel.Request(processor = 1, context = context, startTimeNanos = 0, MemoryPool.NONE, buffer,
      requestMetrics, envelope = None)
  }

  private def verifyNoThrottling[T <: AbstractResponse](
    request: RequestChannel.Request
  ): T = {
    val capturedResponse: ArgumentCaptor[AbstractResponse] = ArgumentCaptor.forClass(classOf[AbstractResponse])
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      any()
    )
    val response = capturedResponse.getValue
    val buffer = MessageUtil.toByteBuffer(
      response.data,
      request.context.header.apiVersion
    )
    AbstractResponse.parseResponse(
      request.context.header.apiKey,
      buffer,
      request.context.header.apiVersion,
    ).asInstanceOf[T]
  }

  private def verifyNoThrottlingAndUpdateMetrics[T <: AbstractResponse](
    request: RequestChannel.Request
  ): T = {
    val capturedResponse: ArgumentCaptor[AbstractResponse] = ArgumentCaptor.forClass(classOf[AbstractResponse])
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      any()
    )
    val response = capturedResponse.getValue
    val buffer = MessageUtil.toByteBuffer(
      response.data,
      request.context.header.apiVersion
    )

    // Create the RequestChannel.Response that is created when sendResponse is called in order to update the metrics.
    val sendResponse = new RequestChannel.SendResponse(
      request,
      request.buildResponseSend(response),
      request.responseNode(response),
      None
    )
    request.updateRequestMetrics(time.milliseconds(), sendResponse)

    AbstractResponse.parseResponse(
      request.context.header.apiKey,
      buffer,
      request.context.header.apiVersion,
    ).asInstanceOf[T]
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
    kafkaApis = createKafkaApis()
    kafkaApis.handleAlterReplicaLogDirsRequest(request)

    val response = verifyNoThrottling[AlterReplicaLogDirsResponse](request)
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
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleDescribeProducersRequest(request)

    val response = verifyNoThrottling[DescribeProducersResponse](request)
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
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleDescribeTransactionsRequest(request)

    val response = verifyNoThrottling[DescribeTransactionsResponse](request)
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
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleDescribeTransactionsRequest(request)

    val response = verifyNoThrottling[DescribeTransactionsResponse](request)
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

    when(txnCoordinator.handleListTransactions(Set.empty[Long], Set.empty[String], -1L))
      .thenReturn(new ListTransactionsResponseData()
        .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code))
    kafkaApis = createKafkaApis()
    kafkaApis.handleListTransactionsRequest(request)

    val response = verifyNoThrottling[ListTransactionsResponse](request)
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

    when(txnCoordinator.handleListTransactions(Set.empty[Long], Set.empty[String], -1L))
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
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleListTransactionsRequest(request)

    val response = verifyNoThrottling[ListTransactionsResponse](request)
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
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleDeleteTopicsRequest(request)
    verify(authorizer, times(2)).authorize(any(), any())

    val deleteResponse = verifyNoThrottling[DeleteTopicsResponse](request)

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
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleDeleteTopicsRequest(request)
    verify(authorizer, times(2)).authorize(any(), any())

    val deleteResponse = verifyNoThrottling[DeleteTopicsResponse](request)

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
    kafkaApis = createKafkaApis(raftSupport = true)
    verifyShouldNeverHandleErrorMessage(kafkaApis.handleLeaderAndIsrRequest)
  }

  @Test
  def testRaftShouldNeverHandleStopReplicaRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    kafkaApis = createKafkaApis(raftSupport = true)
    verifyShouldNeverHandleErrorMessage(kafkaApis.handleStopReplicaRequest)
  }

  @Test
  def testRaftShouldNeverHandleUpdateMetadataRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    kafkaApis = createKafkaApis(raftSupport = true)
    verifyShouldNeverHandleErrorMessage(kafkaApis.handleUpdateMetadataRequest(_, RequestLocal.withThreadConfinedCaching))
  }

  @Test
  def testRaftShouldNeverHandleControlledShutdownRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    kafkaApis = createKafkaApis(raftSupport = true)
    verifyShouldNeverHandleErrorMessage(kafkaApis.handleControlledShutdownRequest)
  }

  @Test
  def testRaftShouldNeverHandleAlterPartitionRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    kafkaApis = createKafkaApis(raftSupport = true)
    verifyShouldNeverHandleErrorMessage(kafkaApis.handleAlterPartitionRequest)
  }

  @Test
  def testRaftShouldNeverHandleEnvelope(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    kafkaApis = createKafkaApis(raftSupport = true)
    verifyShouldNeverHandleErrorMessage(kafkaApis.handleEnvelope(_, RequestLocal.withThreadConfinedCaching))
  }

  @Test
  def testRaftShouldAlwaysForwardCreateTopicsRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    kafkaApis = createKafkaApis(raftSupport = true)
    verifyShouldAlwaysForwardErrorMessage(kafkaApis.handleCreateTopicsRequest)
  }

  @Test
  def testRaftShouldAlwaysForwardCreatePartitionsRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    kafkaApis = createKafkaApis(raftSupport = true)
    verifyShouldAlwaysForwardErrorMessage(kafkaApis.handleCreatePartitionsRequest)
  }

  @Test
  def testRaftShouldAlwaysForwardDeleteTopicsRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    kafkaApis = createKafkaApis(raftSupport = true)
    verifyShouldAlwaysForwardErrorMessage(kafkaApis.handleDeleteTopicsRequest)
  }

  @Test
  def testRaftShouldAlwaysForwardCreateAcls(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    kafkaApis = createKafkaApis(raftSupport = true)
    verifyShouldAlwaysForwardErrorMessage(kafkaApis.handleCreateAcls)
  }

  @Test
  def testRaftShouldAlwaysForwardDeleteAcls(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    kafkaApis = createKafkaApis(raftSupport = true)
    verifyShouldAlwaysForwardErrorMessage(kafkaApis.handleDeleteAcls)
  }

  @Test
  def testEmptyLegacyAlterConfigsRequestWithKRaft(): Unit = {
    val request = buildRequest(new AlterConfigsRequest(new AlterConfigsRequestData(), 1.toShort))
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    kafkaApis = createKafkaApis(raftSupport = true)
    kafkaApis.handleAlterConfigsRequest(request)
    val response = verifyNoThrottling[AlterConfigsResponse](request)
    assertEquals(new AlterConfigsResponseData(), response.data())
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
    kafkaApis = createKafkaApis(raftSupport = true)
    kafkaApis.handleAlterConfigsRequest(request)
    val response = verifyNoThrottling[AlterConfigsResponse](request)
    assertEquals(new AlterConfigsResponseData().setResponses(asList(
      new LAlterConfigsResourceResponse().
        setErrorCode(Errors.INVALID_REQUEST.code()).
        setErrorMessage("Null value not supported for : foo").
        setResourceName(brokerId.toString).
        setResourceType(BROKER.id()))),
      response.data())
  }

  @Test
  def testRaftShouldAlwaysForwardAlterPartitionReassignmentsRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    kafkaApis = createKafkaApis(raftSupport = true)
    verifyShouldAlwaysForwardErrorMessage(kafkaApis.handleAlterPartitionReassignmentsRequest)
  }

  @Test
  def testEmptyIncrementalAlterConfigsRequestWithKRaft(): Unit = {
    val request = buildRequest(new IncrementalAlterConfigsRequest(new IncrementalAlterConfigsRequestData(), 1.toShort))
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    kafkaApis = createKafkaApis(raftSupport = true)
    kafkaApis.handleIncrementalAlterConfigsRequest(request)
    val response = verifyNoThrottling[IncrementalAlterConfigsResponse](request)
    assertEquals(new IncrementalAlterConfigsResponseData(), response.data())
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
    kafkaApis = createKafkaApis(raftSupport = true)
    kafkaApis.handleIncrementalAlterConfigsRequest(request)
    val response = verifyNoThrottling[IncrementalAlterConfigsResponse](request)
    assertEquals(new IncrementalAlterConfigsResponseData().setResponses(asList(
      new IAlterConfigsResourceResponse().
        setErrorCode(0.toShort).
        setErrorMessage(null).
        setResourceName(brokerId.toString).
        setResourceType(BROKER_LOGGER.id()))),
      response.data())
  }

  @Test
  // Test that in KRaft mode, a request that isn't forwarded gets the correct error message.
  // We skip the pre-forward checks in handleCreateTokenRequest 
  def testRaftShouldAlwaysForwardCreateTokenRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    kafkaApis = createKafkaApis(raftSupport = true)
    verifyShouldAlwaysForwardErrorMessage(kafkaApis.handleCreateTokenRequestZk)
  }

  @Test
  // Test that in KRaft mode, a request that isn't forwarded gets the correct error message.
  // We skip the pre-forward checks in handleRenewTokenRequest 
  def testRaftShouldAlwaysForwardRenewTokenRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    kafkaApis = createKafkaApis(raftSupport = true)
    verifyShouldAlwaysForwardErrorMessage(kafkaApis.handleRenewTokenRequestZk)
  }

  @Test
  // Test that in KRaft mode, a request that isn't forwarded gets the correct error message.
  // We skip the pre-forward checks in handleExpireTokenRequest 
  def testRaftShouldAlwaysForwardExpireTokenRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    kafkaApis = createKafkaApis(raftSupport = true)
    verifyShouldAlwaysForwardErrorMessage(kafkaApis.handleExpireTokenRequestZk)
  }

  @Test
  def testRaftShouldAlwaysForwardAlterClientQuotasRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    kafkaApis = createKafkaApis(raftSupport = true)
    verifyShouldAlwaysForwardErrorMessage(kafkaApis.handleAlterClientQuotasRequest)
  }

  @Test
  def testRaftShouldAlwaysForwardAlterUserScramCredentialsRequest(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    kafkaApis = createKafkaApis(raftSupport = true)
    verifyShouldAlwaysForwardErrorMessage(kafkaApis.handleAlterUserScramCredentialsRequest)
  }

  @Test
  def testRaftShouldAlwaysForwardUpdateFeatures(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    kafkaApis = createKafkaApis(raftSupport = true)
    verifyShouldAlwaysForwardErrorMessage(kafkaApis.handleUpdateFeatures)
  }

  @Test
  def testRaftShouldAlwaysForwardElectLeaders(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    kafkaApis = createKafkaApis(raftSupport = true)
    verifyShouldAlwaysForwardErrorMessage(kafkaApis.handleElectLeaders)
  }

  @Test
  def testRaftShouldAlwaysForwardListPartitionReassignments(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    kafkaApis = createKafkaApis(raftSupport = true)
    verifyShouldAlwaysForwardErrorMessage(kafkaApis.handleListPartitionReassignmentsRequest)
  }

  @Test
  def testConsumerGroupHeartbeatReturnsUnsupportedVersion(): Unit = {
    val consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new ConsumerGroupHeartbeatRequest.Builder(consumerGroupHeartbeatRequest, true).build())
    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.NoCaching)

    val expectedHeartbeatResponse = new ConsumerGroupHeartbeatResponseData()
      .setErrorCode(Errors.UNSUPPORTED_VERSION.code)
    val response = verifyNoThrottling[ConsumerGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(expectedHeartbeatResponse, response.data)
  }

  @Test
  def testConsumerGroupHeartbeatRequest(): Unit = {
    val consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new ConsumerGroupHeartbeatRequest.Builder(consumerGroupHeartbeatRequest, true).build())

    val future = new CompletableFuture[ConsumerGroupHeartbeatResponseData]()
    when(groupCoordinator.consumerGroupHeartbeat(
      requestChannelRequest.context,
      consumerGroupHeartbeatRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis(overrideProperties = Map(
      KafkaConfig.NewGroupCoordinatorEnableProp -> "true"
    ))
    kafkaApis.handle(requestChannelRequest, RequestLocal.NoCaching)

    val consumerGroupHeartbeatResponse = new ConsumerGroupHeartbeatResponseData()
      .setMemberId("member")

    future.complete(consumerGroupHeartbeatResponse)
    val response = verifyNoThrottling[ConsumerGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(consumerGroupHeartbeatResponse, response.data)
  }

  @Test
  def testConsumerGroupHeartbeatRequestFutureFailed(): Unit = {
    val consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new ConsumerGroupHeartbeatRequest.Builder(consumerGroupHeartbeatRequest, true).build())

    val future = new CompletableFuture[ConsumerGroupHeartbeatResponseData]()
    when(groupCoordinator.consumerGroupHeartbeat(
      requestChannelRequest.context,
      consumerGroupHeartbeatRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis(overrideProperties = Map(
      KafkaConfig.NewGroupCoordinatorEnableProp -> "true"
    ))
    kafkaApis.handle(requestChannelRequest, RequestLocal.NoCaching)

    future.completeExceptionally(Errors.FENCED_MEMBER_EPOCH.exception)
    val response = verifyNoThrottling[ConsumerGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.FENCED_MEMBER_EPOCH.code, response.data.errorCode)
  }

  @Test
  def testConsumerGroupHeartbeatRequestAuthorizationFailed(): Unit = {
    val consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new ConsumerGroupHeartbeatRequest.Builder(consumerGroupHeartbeatRequest, true).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava)
    kafkaApis = createKafkaApis(
      authorizer = Some(authorizer),
      overrideProperties = Map(KafkaConfig.NewGroupCoordinatorEnableProp -> "true")
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.NoCaching)

    val response = verifyNoThrottling[ConsumerGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, response.data.errorCode)
  }

  @Test
  def testConsumerGroupDescribe(): Unit = {
    val groupIds = List("group-id-0", "group-id-1", "group-id-2").asJava
    val consumerGroupDescribeRequestData = new ConsumerGroupDescribeRequestData()
    consumerGroupDescribeRequestData.groupIds.addAll(groupIds)
    val requestChannelRequest = buildRequest(new ConsumerGroupDescribeRequest.Builder(consumerGroupDescribeRequestData, true).build())

    val future = new CompletableFuture[util.List[ConsumerGroupDescribeResponseData.DescribedGroup]]()
    when(groupCoordinator.consumerGroupDescribe(
      any[RequestContext],
      any[util.List[String]]
    )).thenReturn(future)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(KafkaConfig.NewGroupCoordinatorEnableProp -> "true")
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.NoCaching)

    val describedGroups = List(
      new DescribedGroup().setGroupId(groupIds.get(0)),
      new DescribedGroup().setGroupId(groupIds.get(1)),
      new DescribedGroup().setGroupId(groupIds.get(2))
    ).asJava

    future.complete(describedGroups)
    val expectedConsumerGroupDescribeResponseData = new ConsumerGroupDescribeResponseData()
      .setGroups(describedGroups)

    val response = verifyNoThrottling[ConsumerGroupDescribeResponse](requestChannelRequest)

    assertEquals(expectedConsumerGroupDescribeResponseData, response.data)
  }

  @Test
  def testConsumerGroupDescribeReturnsUnsupportedVersion(): Unit = {
    val groupId = "group0"
    val consumerGroupDescribeRequestData = new ConsumerGroupDescribeRequestData()
    consumerGroupDescribeRequestData.groupIds.add(groupId)
    val requestChannelRequest = buildRequest(new ConsumerGroupDescribeRequest.Builder(consumerGroupDescribeRequestData, true).build())

    val errorCode = Errors.UNSUPPORTED_VERSION.code
    val expectedDescribedGroup = new DescribedGroup().setGroupId(groupId).setErrorCode(errorCode)
    val expectedResponse = new ConsumerGroupDescribeResponseData()
    expectedResponse.groups.add(expectedDescribedGroup)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.NoCaching)
    val response = verifyNoThrottling[ConsumerGroupDescribeResponse](requestChannelRequest)

    assertEquals(expectedResponse, response.data)
  }

  @Test
  def testConsumerGroupDescribeAuthorizationFailed(): Unit = {
    val consumerGroupDescribeRequestData = new ConsumerGroupDescribeRequestData()
    consumerGroupDescribeRequestData.groupIds.add("group-id")
    val requestChannelRequest = buildRequest(new ConsumerGroupDescribeRequest.Builder(consumerGroupDescribeRequestData, true).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava)

    val future = new CompletableFuture[util.List[ConsumerGroupDescribeResponseData.DescribedGroup]]()
    when(groupCoordinator.consumerGroupDescribe(
      any[RequestContext],
      any[util.List[String]]
    )).thenReturn(future)
    future.complete(List().asJava)
    kafkaApis = createKafkaApis(
      authorizer = Some(authorizer),
      overrideProperties = Map(KafkaConfig.NewGroupCoordinatorEnableProp -> "true")
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.NoCaching)

    val response = verifyNoThrottling[ConsumerGroupDescribeResponse](requestChannelRequest)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, response.data.groups.get(0).errorCode)
  }

  @Test
  def testConsumerGroupDescribeFutureFailed(): Unit = {
    val consumerGroupDescribeRequestData = new ConsumerGroupDescribeRequestData()
    consumerGroupDescribeRequestData.groupIds.add("group-id")
    val requestChannelRequest = buildRequest(new ConsumerGroupDescribeRequest.Builder(consumerGroupDescribeRequestData, true).build())

    val future = new CompletableFuture[util.List[ConsumerGroupDescribeResponseData.DescribedGroup]]()
    when(groupCoordinator.consumerGroupDescribe(
      any[RequestContext],
      any[util.List[String]]
    )).thenReturn(future)
    kafkaApis = createKafkaApis(overrideProperties = Map(
      KafkaConfig.NewGroupCoordinatorEnableProp -> "true"
    ))
    kafkaApis.handle(requestChannelRequest, RequestLocal.NoCaching)

    future.completeExceptionally(Errors.FENCED_MEMBER_EPOCH.exception)
    val response = verifyNoThrottling[ConsumerGroupDescribeResponse](requestChannelRequest)
    assertEquals(Errors.FENCED_MEMBER_EPOCH.code, response.data.groups.get(0).errorCode)
  }

  @Test
  def testGetTelemetrySubscriptionsNotAllowedForZkClusters(): Unit = {
    val data = new GetTelemetrySubscriptionsRequestData()

    val request = buildRequest(new GetTelemetrySubscriptionsRequest.Builder(data, true).build())
    kafkaApis = createKafkaApis(enableForwarding = true)
    kafkaApis.handle(request, RequestLocal.NoCaching)

    val response = verifyNoThrottling[GetTelemetrySubscriptionsResponse](request)
    assertEquals(Errors.UNKNOWN_SERVER_ERROR, Errors.forCode(response.data.errorCode))
  }

  @Test
  def testGetTelemetrySubscriptions(): Unit = {
    val request = buildRequest(new GetTelemetrySubscriptionsRequest.Builder(
      new GetTelemetrySubscriptionsRequestData(), true).build())

    when(clientMetricsManager.isTelemetryReceiverConfigured).thenReturn(true)
    when(clientMetricsManager.processGetTelemetrySubscriptionRequest(any[GetTelemetrySubscriptionsRequest](),
      any[RequestContext]())).thenReturn(new GetTelemetrySubscriptionsResponse(
      new GetTelemetrySubscriptionsResponseData()))

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    kafkaApis = createKafkaApis(raftSupport = true)
    kafkaApis.handle(request, RequestLocal.NoCaching)

    val response = verifyNoThrottling[GetTelemetrySubscriptionsResponse](request)

    val expectedResponse = new GetTelemetrySubscriptionsResponseData()
    assertEquals(expectedResponse, response.data)
  }

  @Test
  def testGetTelemetrySubscriptionsWithException(): Unit = {
    val request = buildRequest(new GetTelemetrySubscriptionsRequest.Builder(
      new GetTelemetrySubscriptionsRequestData(), true).build())

    when(clientMetricsManager.isTelemetryReceiverConfigured).thenReturn(true)
    when(clientMetricsManager.processGetTelemetrySubscriptionRequest(any[GetTelemetrySubscriptionsRequest](),
      any[RequestContext]())).thenThrow(new RuntimeException("test"))

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    kafkaApis = createKafkaApis(raftSupport = true)
    kafkaApis.handle(request, RequestLocal.NoCaching)

    val response = verifyNoThrottling[GetTelemetrySubscriptionsResponse](request)

    val expectedResponse = new GetTelemetrySubscriptionsResponseData().setErrorCode(Errors.INVALID_REQUEST.code)
    assertEquals(expectedResponse, response.data)
  }

  @Test
  def testPushTelemetryNotAllowedForZkClusters(): Unit = {
    val data = new PushTelemetryRequestData()

    val request = buildRequest(new PushTelemetryRequest.Builder(data, true).build())
    kafkaApis = createKafkaApis(enableForwarding = true)
    kafkaApis.handle(request, RequestLocal.NoCaching)

    val response = verifyNoThrottling[PushTelemetryResponse](request)
    assertEquals(Errors.UNKNOWN_SERVER_ERROR, Errors.forCode(response.data.errorCode))
  }

  @Test
  def testPushTelemetry(): Unit = {
    val request = buildRequest(new PushTelemetryRequest.Builder(new PushTelemetryRequestData(), true).build())

    when(clientMetricsManager.isTelemetryReceiverConfigured).thenReturn(true)
    when(clientMetricsManager.processPushTelemetryRequest(any[PushTelemetryRequest](), any[RequestContext]()))
      .thenReturn(new PushTelemetryResponse(new PushTelemetryResponseData()))

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    kafkaApis = createKafkaApis(raftSupport = true)
    kafkaApis.handle(request, RequestLocal.NoCaching)
    val response = verifyNoThrottling[PushTelemetryResponse](request)

    val expectedResponse = new PushTelemetryResponseData().setErrorCode(Errors.NONE.code)
    assertEquals(expectedResponse, response.data)
  }

  @Test
  def testPushTelemetryWithException(): Unit = {
    val request = buildRequest(new PushTelemetryRequest.Builder(new PushTelemetryRequestData(), true).build())

    when(clientMetricsManager.isTelemetryReceiverConfigured()).thenReturn(true)
    when(clientMetricsManager.processPushTelemetryRequest(any[PushTelemetryRequest](), any[RequestContext]()))
      .thenThrow(new RuntimeException("test"))

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)
    kafkaApis = createKafkaApis(raftSupport = true)
    kafkaApis.handle(request, RequestLocal.NoCaching)
    val response = verifyNoThrottling[PushTelemetryResponse](request)

    val expectedResponse = new PushTelemetryResponseData().setErrorCode(Errors.INVALID_REQUEST.code)
    assertEquals(expectedResponse, response.data)
  }

  @Test
  def testListClientMetricsResourcesNotAllowedForZkClusters(): Unit = {
    val request = buildRequest(new ListClientMetricsResourcesRequest.Builder(new ListClientMetricsResourcesRequestData()).build())
    kafkaApis = createKafkaApis(enableForwarding = true)
    kafkaApis.handle(request, RequestLocal.NoCaching)

    val response = verifyNoThrottling[ListClientMetricsResourcesResponse](request)
    assertEquals(Errors.UNKNOWN_SERVER_ERROR, Errors.forCode(response.data.errorCode))
  }

  @Test
  def testListClientMetricsResources(): Unit = {
    val request = buildRequest(new ListClientMetricsResourcesRequest.Builder(new ListClientMetricsResourcesRequestData()).build())
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)

    val resources = new mutable.HashSet[String]
    resources.add("test1")
    resources.add("test2")
    when(clientMetricsManager.listClientMetricsResources).thenReturn(resources.asJava)
    kafkaApis = createKafkaApis(raftSupport = true)
    kafkaApis.handle(request, RequestLocal.NoCaching)
    val response = verifyNoThrottling[ListClientMetricsResourcesResponse](request)
    val expectedResponse = new ListClientMetricsResourcesResponseData().setClientMetricsResources(
      resources.map(resource => new ClientMetricsResource().setName(resource)).toBuffer.asJava)
    assertEquals(expectedResponse, response.data)
  }

  @Test
  def testListClientMetricsResourcesEmptyResponse(): Unit = {
    val request = buildRequest(new ListClientMetricsResourcesRequest.Builder(new ListClientMetricsResourcesRequestData()).build())
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)

    val resources = new mutable.HashSet[String]
    when(clientMetricsManager.listClientMetricsResources).thenReturn(resources.asJava)
    kafkaApis = createKafkaApis(raftSupport = true)
    kafkaApis.handle(request, RequestLocal.NoCaching)
    val response = verifyNoThrottling[ListClientMetricsResourcesResponse](request)
    val expectedResponse = new ListClientMetricsResourcesResponseData()
    assertEquals(expectedResponse, response.data)
  }

  @Test
  def testListClientMetricsResourcesWithException(): Unit = {
    val request = buildRequest(new ListClientMetricsResourcesRequest.Builder(new ListClientMetricsResourcesRequestData()).build())
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId)

    when(clientMetricsManager.listClientMetricsResources).thenThrow(new RuntimeException("test"))
    kafkaApis = createKafkaApis(raftSupport = true)
    kafkaApis.handle(request, RequestLocal.NoCaching)
    val response = verifyNoThrottling[ListClientMetricsResourcesResponse](request)

    val expectedResponse = new ListClientMetricsResourcesResponseData().setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code)
    assertEquals(expectedResponse, response.data)
  }
}
