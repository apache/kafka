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

import kafka.cluster.Partition
import kafka.coordinator.transaction.{InitProducerIdResult, TransactionCoordinator}
import kafka.network.RequestChannel
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.share.SharePartitionManager
import kafka.utils.{Logging, TestUtils}
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.clients.admin.{AlterConfigOp, ConfigEntry}
import org.apache.kafka.clients.consumer.AcknowledgeType
import org.apache.kafka.common._
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.ConfigResource.Type.{BROKER, BROKER_LOGGER}
import org.apache.kafka.common.errors.{ClusterAuthorizationException, UnsupportedVersionException}
import org.apache.kafka.common.internals.{Plugin, Topic}
import org.apache.kafka.common.internals.Topic.SHARE_GROUP_STATE_TOPIC_NAME
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.{AddPartitionsToTxnTopic, AddPartitionsToTxnTopicCollection, AddPartitionsToTxnTransaction, AddPartitionsToTxnTransactionCollection}
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnResult
import org.apache.kafka.common.message.AlterConfigsRequestData.{AlterConfigsResource => LAlterConfigsResource, AlterConfigsResourceCollection => LAlterConfigsResourceCollection, AlterableConfig => LAlterableConfig, AlterableConfigCollection => LAlterableConfigCollection}
import org.apache.kafka.common.message.AlterConfigsResponseData.{AlterConfigsResourceResponse => LAlterConfigsResourceResponse}
import org.apache.kafka.common.message.AlterShareGroupOffsetsRequestData.{AlterShareGroupOffsetsRequestPartition, AlterShareGroupOffsetsRequestTopic, AlterShareGroupOffsetsRequestTopicCollection}
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData.{DescribedGroup, TopicPartitions}
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult
import org.apache.kafka.common.message.DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic
import org.apache.kafka.common.message.DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic
import org.apache.kafka.common.message.DescribeShareGroupOffsetsRequestData.{DescribeShareGroupOffsetsRequestGroup, DescribeShareGroupOffsetsRequestTopic}
import org.apache.kafka.common.message.DescribeShareGroupOffsetsResponseData.{DescribeShareGroupOffsetsResponseGroup, DescribeShareGroupOffsetsResponsePartition, DescribeShareGroupOffsetsResponseTopic}
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.{AlterConfigsResource => IAlterConfigsResource, AlterConfigsResourceCollection => IAlterConfigsResourceCollection, AlterableConfig => IAlterableConfig, AlterableConfigCollection => IAlterableConfigCollection}
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData.{AlterConfigsResourceResponse => IAlterConfigsResourceResponse}
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity
import org.apache.kafka.common.message.ListOffsetsRequestData.{ListOffsetsPartition, ListOffsetsTopic}
import org.apache.kafka.common.message.ListOffsetsResponseData.{ListOffsetsPartitionResponse, ListOffsetsTopicResponse}
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic
import org.apache.kafka.common.message.OffsetDeleteRequestData.{OffsetDeleteRequestPartition, OffsetDeleteRequestTopic, OffsetDeleteRequestTopicCollection}
import org.apache.kafka.common.message.OffsetDeleteResponseData.{OffsetDeleteResponsePartition, OffsetDeleteResponsePartitionCollection, OffsetDeleteResponseTopic, OffsetDeleteResponseTopicCollection}
import org.apache.kafka.common.message.ShareFetchRequestData.{AcknowledgementBatch, ForgottenTopic}
import org.apache.kafka.common.message.ShareFetchResponseData.{AcquiredRecords, PartitionData, ShareFetchableTopicResponse}
import org.apache.kafka.common.metadata.{FeatureLevelRecord, PartitionRecord, RegisterBrokerRecord, TopicRecord}
import org.apache.kafka.common.metadata.RegisterBrokerRecord.{BrokerEndpoint, BrokerEndpointCollection}
import org.apache.kafka.common.protocol.ApiMessage
import org.apache.kafka.common.message._
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.{ApiKeys, Errors, MessageUtil}
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType
import org.apache.kafka.common.requests.MetadataResponse.TopicMetadata
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests.WriteTxnMarkersRequest.TxnMarkerEntry
import org.apache.kafka.common.requests.{FetchMetadata => JFetchMetadata, _}
import org.apache.kafka.common.resource.{PatternType, Resource, ResourcePattern, ResourceType}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, KafkaPrincipalSerde, SecurityProtocol}
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource
import org.apache.kafka.common.utils.{ImplicitLinkedHashCollection, ProducerIdAndEpoch, SecurityUtils, Utils}
import org.apache.kafka.coordinator.group.GroupConfig.{CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, CONSUMER_SESSION_TIMEOUT_MS_CONFIG, SHARE_AUTO_OFFSET_RESET_CONFIG, SHARE_HEARTBEAT_INTERVAL_MS_CONFIG, SHARE_ISOLATION_LEVEL_CONFIG, SHARE_RECORD_LOCK_DURATION_MS_CONFIG, SHARE_SESSION_TIMEOUT_MS_CONFIG, STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG, STREAMS_INITIAL_REBALANCE_DELAY_MS_CONFIG, STREAMS_NUM_STANDBY_REPLICAS_CONFIG, STREAMS_SESSION_TIMEOUT_MS_CONFIG}
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig
import org.apache.kafka.coordinator.group.{GroupConfig, GroupConfigManager, GroupCoordinator, GroupCoordinatorConfig}
import org.apache.kafka.coordinator.group.streams.StreamsGroupHeartbeatResult
import org.apache.kafka.coordinator.share.{ShareCoordinator, ShareCoordinatorTestConfig}
import org.apache.kafka.coordinator.transaction.TransactionLogConfig
import org.apache.kafka.image.{MetadataDelta, MetadataImage, MetadataProvenance}
import org.apache.kafka.metadata.{ConfigRepository, KRaftMetadataCache, MetadataCache, MockConfigRepository}
import org.apache.kafka.network.Session
import org.apache.kafka.network.metrics.{RequestChannelMetrics, RequestMetrics}
import org.apache.kafka.raft.{KRaftConfigs, QuorumConfig}
import org.apache.kafka.security.authorizer.AclEntry
import org.apache.kafka.server.{ClientMetricsManager, SimpleApiVersionManager}
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.{FeatureVersion, FinalizedFeatures, GroupVersion, KRaftVersion, MetadataVersion, RequestLocal, ShareVersion, StreamsVersion, TransactionVersion}
import org.apache.kafka.server.config.{ReplicationConfigs, ServerConfigs, ServerLogConfigs}
import org.apache.kafka.server.logger.LoggingController
import org.apache.kafka.server.metrics.ClientMetricsTestUtils
import org.apache.kafka.server.share.{CachedSharePartition, ErroneousAndValidPartitionData, SharePartitionKey}
import org.apache.kafka.server.quota.{ClientQuotaManager, ControllerMutationQuota, ControllerMutationQuotaManager, ThrottleCallback}
import org.apache.kafka.server.share.acknowledge.ShareAcknowledgementBatch
import org.apache.kafka.server.share.context.{FinalContext, ShareSessionContext}
import org.apache.kafka.server.share.session.{ShareSession, ShareSessionKey}
import org.apache.kafka.server.storage.log.{FetchParams, FetchPartitionData}
import org.apache.kafka.server.util.{FutureUtils, MockTime}
import org.apache.kafka.storage.internals.log.{AppendOrigin, LogConfig, UnifiedLog}
import org.apache.kafka.storage.log.metrics.BrokerTopicStats
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, EnumSource, ValueSource}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.{ArgumentCaptor, ArgumentMatchers, Mockito}

import java.lang.{Byte => JByte}
import java.net.InetAddress
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.{CompletableFuture, TimeUnit}
import java.util.function.Consumer
import java.util.{Comparator, Optional, OptionalInt, OptionalLong, Properties}
import scala.collection.{Map, Seq, mutable}
import scala.jdk.CollectionConverters._

class KafkaApisTest extends Logging {
  private val requestChannel: RequestChannel = mock(classOf[RequestChannel])
  private val requestChannelMetrics: RequestChannelMetrics = mock(classOf[RequestChannelMetrics])
  private val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
  private val groupCoordinator: GroupCoordinator = mock(classOf[GroupCoordinator])
  private val shareCoordinator: ShareCoordinator = mock(classOf[ShareCoordinator])
  private val txnCoordinator: TransactionCoordinator = mock(classOf[TransactionCoordinator])
  private val forwardingManager: ForwardingManager = mock(classOf[ForwardingManager])
  private val autoTopicCreationManager: AutoTopicCreationManager = mock(classOf[AutoTopicCreationManager])

  private val kafkaPrincipalSerde = new KafkaPrincipalSerde {
    override def serialize(principal: KafkaPrincipal): Array[Byte] = Utils.utf8(principal.toString)
    override def deserialize(bytes: Array[Byte]): KafkaPrincipal = SecurityUtils.parseKafkaPrincipal(Utils.utf8(bytes))
  }
  private val metrics = new Metrics()
  private val brokerId = 1
  private var metadataCache: MetadataCache = new KRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
  private val clientQuotaManager: ClientQuotaManager = mock(classOf[ClientQuotaManager])
  private val clientRequestQuotaManager: ClientRequestQuotaManager = mock(classOf[ClientRequestQuotaManager])
  private val clientControllerQuotaManager: ControllerMutationQuotaManager = mock(classOf[ControllerMutationQuotaManager])
  private val replicaQuotaManager: ReplicationQuotaManager = mock(classOf[ReplicationQuotaManager])
  private val quotas = new QuotaManagers(clientQuotaManager, clientQuotaManager, clientRequestQuotaManager,
    clientControllerQuotaManager, replicaQuotaManager, replicaQuotaManager, replicaQuotaManager, util.Optional.empty())
  private val fetchManager: FetchManager = mock(classOf[FetchManager])
  private val sharePartitionManager: SharePartitionManager = mock(classOf[SharePartitionManager])
  private val clientMetricsManager: ClientMetricsManager = mock(classOf[ClientMetricsManager])
  private val groupConfigManager: GroupConfigManager = mock(classOf[GroupConfigManager])
  private val brokerTopicStats = new BrokerTopicStats
  private val clusterId = "clusterId"
  private val time = new MockTime
  private val clientId = ""
  private var kafkaApis: KafkaApis = _

  @AfterEach
  def tearDown(): Unit = {
    Utils.swallow(this.logger.underlying, () => quotas.shutdown())
    if (kafkaApis != null)
      Utils.swallow(this.logger.underlying, () => kafkaApis.close())
    TestUtils.clearYammerMetrics()
    metrics.close()
  }

  def createKafkaApis(
    authorizer: Option[Authorizer] = None,
    configRepository: ConfigRepository = new MockConfigRepository(),
    overrideProperties: Map[String, String] = Map.empty,
    featureVersions: Seq[FeatureVersion] = Seq.empty,
    autoTopicCreationManager: Option[AutoTopicCreationManager] = None
  ): KafkaApis = {

    val properties = TestUtils.createBrokerConfig(brokerId)
    properties.put(KRaftConfigs.NODE_ID_CONFIG, brokerId.toString)
    properties.put(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
    val voterId = brokerId + 1
    properties.put(QuorumConfig.QUORUM_VOTERS_CONFIG, s"$voterId@localhost:9093")

    overrideProperties.foreach( p => properties.put(p._1, p._2))
    val config = new KafkaConfig(properties)

    val apiVersionManager = new SimpleApiVersionManager(
      ListenerType.BROKER,
      true,
      () => new FinalizedFeatures(MetadataVersion.latestTesting(), util.Map.of[String, java.lang.Short], 0))

    setupFeatures(featureVersions)

    new KafkaApis(
      requestChannel = requestChannel,
      forwardingManager = forwardingManager,
      replicaManager = replicaManager,
      groupCoordinator = groupCoordinator,
      txnCoordinator = txnCoordinator,
      shareCoordinator = shareCoordinator,
      autoTopicCreationManager = autoTopicCreationManager.getOrElse(this.autoTopicCreationManager),
      brokerId = brokerId,
      config = config,
      configRepository = configRepository,
      metadataCache = metadataCache,
      metrics = metrics,
      authorizerPlugin = authorizer.map(Plugin.wrapInstance(_, null, "authorizer.class.name")),
      quotas = quotas,
      fetchManager = fetchManager,
      sharePartitionManager = sharePartitionManager,
      brokerTopicStats = brokerTopicStats,
      clusterId = clusterId,
      time = time,
      tokenManager = null,
      apiVersionManager = apiVersionManager,
      clientMetricsManager = clientMetricsManager,
      groupConfigManager = groupConfigManager)
  }

  private def setupFeatures(featureVersions: Seq[FeatureVersion]): Unit = {
    if (featureVersions.isEmpty) return

    when(metadataCache.features()).thenReturn {
      new FinalizedFeatures(
        MetadataVersion.latestTesting,
        featureVersions.map { featureVersion =>
          featureVersion.featureName -> featureVersion.featureLevel.asInstanceOf[java.lang.Short]
        }.toMap.asJava,
        0)
    }
  }

  def initializeMetadataCacheWithShareGroupsEnabled(enableShareGroups: Boolean = true): MetadataCache = {
    val cache = new KRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_1)
    val delta = new MetadataDelta(MetadataImage.EMPTY)
    delta.replay(new FeatureLevelRecord()
      .setName(MetadataVersion.FEATURE_NAME)
      .setFeatureLevel(MetadataVersion.MINIMUM_VERSION.featureLevel())
    )
    if (enableShareGroups) {
      delta.replay(new FeatureLevelRecord()
        .setName(ShareVersion.FEATURE_NAME)
        .setFeatureLevel(ShareVersion.SV_1.featureLevel())
      )
    } else {
      delta.replay(new FeatureLevelRecord()
        .setName(ShareVersion.FEATURE_NAME)
        .setFeatureLevel(ShareVersion.SV_0.featureLevel())
      )
    }
    cache.setImage(delta.apply(MetadataProvenance.EMPTY))
    cache
  }

  @Test
  def testDescribeConfigsWithAuthorizer(): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])

    val operation = AclOperation.DESCRIBE_CONFIGS
    val resourceType = ResourceType.TOPIC
    val resourceName = "topic-1"
    val requestHeader = new RequestHeader(ApiKeys.DESCRIBE_CONFIGS, ApiKeys.DESCRIBE_CONFIGS.latestVersion,
      clientId, 0)

    val expectedActions = util.List.of(
      new Action(operation, new ResourcePattern(resourceType, resourceName, PatternType.LITERAL),
        1, true, true)
    )

    // Verify that authorize is only called once
    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(expectedActions)))
      .thenReturn(util.List.of(AuthorizationResult.ALLOWED))

    val configRepository: ConfigRepository = mock(classOf[ConfigRepository])
    val topicConfigs = new Properties()
    val propName = "min.insync.replicas"
    val propValue = "3"
    topicConfigs.put(propName, propValue)
    when(configRepository.topicConfig(resourceName)).thenReturn(topicConfigs)

    metadataCache = mock(classOf[KRaftMetadataCache])
    when(metadataCache.contains(resourceName)).thenReturn(true)

    val describeConfigsRequest = new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData()
      .setIncludeSynonyms(true)
      .setResources(util.List.of(new DescribeConfigsRequestData.DescribeConfigsResource()
        .setResourceName(resourceName)
        .setResourceType(ConfigResource.Type.TOPIC.id))))
      .build(requestHeader.apiVersion)
    val request = buildRequest(describeConfigsRequest, requestHeader = Option(requestHeader))

    kafkaApis = createKafkaApis(authorizer = Some(authorizer), configRepository = configRepository)
    kafkaApis.handleDescribeConfigsRequest(request)

    verify(authorizer).authorize(any(), ArgumentMatchers.eq(expectedActions))
    val response = verifyNoThrottling[DescribeConfigsResponse](request)
    val results = response.data.results
    assertEquals(1, results.size)
    val describeConfigsResult = results.get(0)
    assertEquals(ConfigResource.Type.TOPIC.id, describeConfigsResult.resourceType)
    assertEquals(resourceName, describeConfigsResult.resourceName)
    val configs = describeConfigsResult.configs.asScala.filter(_.name == propName)
    assertEquals(1, configs.length)
    val describeConfigsResponseData = configs.head
    assertEquals(propName, describeConfigsResponseData.name)
    assertEquals(propValue, describeConfigsResponseData.value)
  }

  @Test
  def testElectLeadersForwarding(): Unit = {
    val requestBuilder = new ElectLeadersRequest.Builder(ElectionType.PREFERRED, null, 30000)
    testKraftForwarding(ApiKeys.ELECT_LEADERS, requestBuilder)
  }

  @Test
  def testIncrementalConsumerGroupAlterConfigs(): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])

    val consumerGroupId = "consumer_group_1"
    val resource = new ConfigResource(ConfigResource.Type.GROUP, consumerGroupId)

    authorizeResource(authorizer, AclOperation.ALTER_CONFIGS, ResourceType.GROUP,
      consumerGroupId, AuthorizationResult.ALLOWED)

    val requestHeader = new RequestHeader(ApiKeys.INCREMENTAL_ALTER_CONFIGS,
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.latestVersion, clientId, 0)

    val incrementalAlterConfigsRequest = getIncrementalAlterConfigRequestBuilder(
      Seq(resource), "consumer.session.timeout.ms", "45000").build(requestHeader.apiVersion)
    val request = buildRequest(incrementalAlterConfigsRequest, requestHeader = Option(requestHeader))

    metadataCache = new KRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    createKafkaApis(authorizer = Some(authorizer)).handleIncrementalAlterConfigsRequest(request)
    verify(forwardingManager, times(1)).forwardRequest(
      any(),
      any(),
      any()
    )
  }

  @Test
  def testDescribeConfigsConsumerGroup(): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])
    val operation = AclOperation.DESCRIBE_CONFIGS
    val resourceType = ResourceType.GROUP
    val consumerGroupId = "consumer_group_1"
    val requestHeader =
      new RequestHeader(ApiKeys.DESCRIBE_CONFIGS, ApiKeys.DESCRIBE_CONFIGS.latestVersion, clientId, 0)
    val expectedActions = util.List.of(
      new Action(operation, new ResourcePattern(resourceType, consumerGroupId, PatternType.LITERAL),
        1, true, true)
    )

    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(expectedActions)))
      .thenReturn(util.List.of(AuthorizationResult.ALLOWED))

    val configRepository: ConfigRepository = mock(classOf[ConfigRepository])
    val cgConfigs = new Properties()
    cgConfigs.put(CONSUMER_SESSION_TIMEOUT_MS_CONFIG, GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_DEFAULT.toString)
    cgConfigs.put(CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_DEFAULT.toString)
    cgConfigs.put(SHARE_SESSION_TIMEOUT_MS_CONFIG, GroupCoordinatorConfig.SHARE_GROUP_SESSION_TIMEOUT_MS_DEFAULT.toString)
    cgConfigs.put(SHARE_HEARTBEAT_INTERVAL_MS_CONFIG, GroupCoordinatorConfig.SHARE_GROUP_HEARTBEAT_INTERVAL_MS_DEFAULT.toString)
    cgConfigs.put(SHARE_RECORD_LOCK_DURATION_MS_CONFIG, ShareGroupConfig.SHARE_GROUP_RECORD_LOCK_DURATION_MS_DEFAULT.toString)
    cgConfigs.put(SHARE_AUTO_OFFSET_RESET_CONFIG, GroupConfig.SHARE_AUTO_OFFSET_RESET_DEFAULT)
    cgConfigs.put(SHARE_ISOLATION_LEVEL_CONFIG, GroupConfig.SHARE_ISOLATION_LEVEL_DEFAULT)
    cgConfigs.put(STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG, GroupCoordinatorConfig.STREAMS_GROUP_HEARTBEAT_INTERVAL_MS_DEFAULT.toString)
    cgConfigs.put(STREAMS_SESSION_TIMEOUT_MS_CONFIG, GroupCoordinatorConfig.STREAMS_GROUP_SESSION_TIMEOUT_MS_DEFAULT.toString)
    cgConfigs.put(STREAMS_NUM_STANDBY_REPLICAS_CONFIG, GroupCoordinatorConfig.STREAMS_GROUP_NUM_STANDBY_REPLICAS_DEFAULT.toString)
    cgConfigs.put(STREAMS_INITIAL_REBALANCE_DELAY_MS_CONFIG, GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_DEFAULT.toString)

    when(configRepository.groupConfig(consumerGroupId)).thenReturn(cgConfigs)

    val describeConfigsRequest = new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData()
      .setIncludeSynonyms(true)
      .setResources(util.List.of(new DescribeConfigsRequestData.DescribeConfigsResource()
        .setResourceName(consumerGroupId)
        .setResourceType(ConfigResource.Type.GROUP.id))))
      .build(requestHeader.apiVersion)
    val request = buildRequest(describeConfigsRequest,
      requestHeader = Option(requestHeader))
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    createKafkaApis(authorizer = Some(authorizer), configRepository = configRepository)
      .handleDescribeConfigsRequest(request)

    val response = verifyNoThrottling[DescribeConfigsResponse](request)
    // Verify that authorize is only called once
    verify(authorizer, times(1)).authorize(any(), any())
    val results = response.data.results
    assertEquals(1, results.size)
    val describeConfigsResult = results.get(0)

    assertEquals(ConfigResource.Type.GROUP.id, describeConfigsResult.resourceType)
    assertEquals(consumerGroupId, describeConfigsResult.resourceName)
    val configs = describeConfigsResult.configs
    assertEquals(cgConfigs.size, configs.size)
  }

  @Test
  def testAlterConfigsClientMetrics(): Unit = {
    val subscriptionName = "client_metric_subscription_1"
    val authorizedResource = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, subscriptionName)

    val props = ClientMetricsTestUtils.defaultTestProperties
    val configEntries = new util.ArrayList[AlterConfigsRequest.ConfigEntry]()
    props.forEach((x, y) =>
      configEntries.add(new AlterConfigsRequest.ConfigEntry(x.asInstanceOf[String], y.asInstanceOf[String])))

    val configs = util.Map.of(authorizedResource, new AlterConfigsRequest.Config(configEntries))

    val requestHeader = new RequestHeader(ApiKeys.ALTER_CONFIGS, ApiKeys.ALTER_CONFIGS.latestVersion, clientId, 0)
    val apiRequest = new AlterConfigsRequest.Builder(configs, false).build(requestHeader.apiVersion)
    val request = buildRequest(apiRequest)

    metadataCache = new KRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis()
    kafkaApis.handleAlterConfigsRequest(request)
    verify(forwardingManager, times(1)).forwardRequest(
      any(),
      any(),
      any()
    )
  }

  @Test
  def testIncrementalClientMetricAlterConfigs(): Unit = {
    val subscriptionName = "client_metric_subscription_1"
    val resource = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, subscriptionName)

    val requestHeader = new RequestHeader(ApiKeys.INCREMENTAL_ALTER_CONFIGS,
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.latestVersion, clientId, 0)

    val incrementalAlterConfigsRequest = getIncrementalAlterConfigRequestBuilder(
      Seq(resource), "metrics", "foo.bar").build(requestHeader.apiVersion)
    val request = buildRequest(incrementalAlterConfigsRequest, requestHeader = Option(requestHeader))

    metadataCache = new KRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis()
    kafkaApis.handleIncrementalAlterConfigsRequest(request)
    verify(forwardingManager, times(1)).forwardRequest(
      any(),
      any(),
      any()
    )
  }

  private def getIncrementalAlterConfigRequestBuilder(configResources: Seq[ConfigResource],
                                                      configName: String,
                                                      configValue: String): IncrementalAlterConfigsRequest.Builder = {
    val resourceMap = configResources.map(configResource => {
      val entryToBeModified = new ConfigEntry(configName, configValue)
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
    val expectedActions = util.List.of(
      new Action(operation, new ResourcePattern(resourceType, Resource.CLUSTER_NAME, PatternType.LITERAL),
        1, true, true)
    )

    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(expectedActions)))
      .thenReturn(util.List.of(AuthorizationResult.ALLOWED))

    val resource = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, subscriptionName)
    val configRepository: ConfigRepository = mock(classOf[ConfigRepository])
    val cmConfigs = ClientMetricsTestUtils.defaultTestProperties
    when(configRepository.config(resource)).thenReturn(cmConfigs)

    metadataCache = mock(classOf[KRaftMetadataCache])
    when(metadataCache.contains(subscriptionName)).thenReturn(true)

    val describeConfigsRequest = new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData()
      .setIncludeSynonyms(true)
      .setResources(util.List.of(new DescribeConfigsRequestData.DescribeConfigsResource()
        .setResourceName(subscriptionName)
        .setResourceType(ConfigResource.Type.CLIENT_METRICS.id))))
      .build(requestHeader.apiVersion)
    val request = buildRequest(describeConfigsRequest,
      requestHeader = Option(requestHeader))

    kafkaApis = createKafkaApis(authorizer = Some(authorizer), configRepository = configRepository)
    kafkaApis.handleDescribeConfigsRequest(request)

    val response = verifyNoThrottling[DescribeConfigsResponse](request)
    // Verify that authorize is only called once
    verify(authorizer, times(1)).authorize(any(), any())
    val results = response.data.results
    assertEquals(1, results.size)
    val describeConfigsResult = results.get(0)

    assertEquals(ConfigResource.Type.CLIENT_METRICS.id, describeConfigsResult.resourceType)
    assertEquals(subscriptionName, describeConfigsResult.resourceName)
    val configs = describeConfigsResult.configs
    assertEquals(cmConfigs.size, configs.size)
  }

  @Test
  def testDescribeQuorumForwardedForKRaftClusters(): Unit = {
    val requestData = DescribeQuorumRequest.singletonRequest(KafkaRaftServer.MetadataPartition)
    val requestBuilder = new DescribeQuorumRequest.Builder(requestData)
    metadataCache = new KRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis()
    testForwardableApi(kafkaApis = kafkaApis,
      ApiKeys.DESCRIBE_QUORUM,
      requestBuilder
    )
  }

  private def testKraftForwarding(
    apiKey: ApiKeys,
    requestBuilder: AbstractRequest.Builder[_ <: AbstractRequest]
  ): Unit = {
    metadataCache = new KRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis()
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

    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(util.List.of(expectedAuthorizedAction))))
      .thenReturn(util.List.of(result))
  }

  @Test
  def testIncrementalAlterConfigsWithAuthorizer(): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])

    val localResource = new ConfigResource(ConfigResource.Type.BROKER_LOGGER, "localResource")
    val forwardedResource = new ConfigResource(ConfigResource.Type.GROUP, "forwardedResource")

    val requestHeader = new RequestHeader(ApiKeys.INCREMENTAL_ALTER_CONFIGS, ApiKeys.INCREMENTAL_ALTER_CONFIGS.latestVersion, clientId, 0)

    val incrementalAlterConfigsRequest = getIncrementalAlterConfigRequestBuilder(Seq(localResource, forwardedResource))
      .build(requestHeader.apiVersion)
    val request = buildRequest(incrementalAlterConfigsRequest, requestHeader = Option(requestHeader))

    metadataCache = new KRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleIncrementalAlterConfigsRequest(request)

    verify(authorizer, times(1)).authorize(any(), any())
    verify(forwardingManager, times(1)).forwardRequest(
      any(),
      any(),
      any()
    )
  }

  private def getIncrementalAlterConfigRequestBuilder(configResources: Seq[ConfigResource]): IncrementalAlterConfigsRequest.Builder = {
    val resourceMap = configResources.map(configResource => {
      configResource -> Set(
        new AlterConfigOp(new ConfigEntry("foo", "bar"),
        OpType.SET)).asJavaCollection
    }).toMap.asJava

    new IncrementalAlterConfigsRequest.Builder(resourceMap, false)
  }

  @ParameterizedTest
  @CsvSource(value = Array("0,1500", "1500,0", "3000,1000"))
  def testKRaftControllerThrottleTimeEnforced(
    controllerThrottleTimeMs: Int,
    requestThrottleTimeMs: Int
  ): Unit = {
    metadataCache = new KRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)

    val topicToCreate = new CreatableTopic()
      .setName("topic")
      .setNumPartitions(1)
      .setReplicationFactor(1.toShort)

    val requestData = new CreateTopicsRequestData()
    requestData.topics().add(topicToCreate)

    val requestBuilder = new CreateTopicsRequest.Builder(requestData).build()
    val request = buildRequest(requestBuilder)

    kafkaApis = createKafkaApis()
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
      ArgumentMatchers.eq(request.header.clientId()),
      ArgumentMatchers.eq(request.session),
      any[ThrottleCallback](),
      ArgumentMatchers.eq(expectedThrottleTimeMs)
    )

    assertEquals(expectedThrottleTimeMs, responseData.throttleTimeMs)
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

  @Test
  def testFindCoordinatorTooOldForShareStateTopic(): Unit = {
    testFindCoordinatorWithTopicCreation(CoordinatorType.SHARE, checkAutoCreateTopic = false, version = 5)
  }

  @Test
  def testFindCoordinatorNoShareCoordinatorForShareStateTopic(): Unit = {
    testFindCoordinatorWithTopicCreation(CoordinatorType.SHARE, checkAutoCreateTopic = false)
  }

  private def testFindCoordinatorWithTopicCreation(coordinatorType: CoordinatorType,
                                                   hasEnoughLiveBrokers: Boolean = true,
                                                   checkAutoCreateTopic: Boolean = true,
                                                   version: Short = ApiKeys.FIND_COORDINATOR.latestVersion): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])

    val requestHeader = new RequestHeader(ApiKeys.FIND_COORDINATOR, version, clientId, 0)

    val numBrokersNeeded = 3

    setupBrokerMetadata(hasEnoughLiveBrokers, numBrokersNeeded)

    val requestTimeout = 10
    val topicConfigOverride = mutable.Map.empty[String, String]
    topicConfigOverride.put(ServerConfigs.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout.toString)

    val groupId = "group"
    val topicId = Uuid.randomUuid
    val partition = 0
    var key:String = groupId

    val topicName =
      coordinatorType match {
        case CoordinatorType.GROUP =>
          topicConfigOverride.put(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, numBrokersNeeded.toString)
          topicConfigOverride.put(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, numBrokersNeeded.toString)
          when(groupCoordinator.groupMetadataTopicConfigs).thenReturn(new Properties)
          authorizeResource(authorizer, AclOperation.DESCRIBE, ResourceType.GROUP,
            groupId, AuthorizationResult.ALLOWED)
          Topic.GROUP_METADATA_TOPIC_NAME
        case CoordinatorType.TRANSACTION =>
          topicConfigOverride.put(TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, numBrokersNeeded.toString)
          topicConfigOverride.put(TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, numBrokersNeeded.toString)
          when(txnCoordinator.transactionTopicConfigs).thenReturn(new Properties)
          authorizeResource(authorizer, AclOperation.DESCRIBE, ResourceType.TRANSACTIONAL_ID,
            groupId, AuthorizationResult.ALLOWED)
          Topic.TRANSACTION_STATE_TOPIC_NAME
        case CoordinatorType.SHARE =>
          authorizeResource(authorizer, AclOperation.CLUSTER_ACTION, ResourceType.CLUSTER,
            Resource.CLUSTER_NAME, AuthorizationResult.ALLOWED)
          key = "%s:%s:%d" format(groupId, topicId, partition)
          Topic.SHARE_GROUP_STATE_TOPIC_NAME
        case _ =>
          throw new IllegalStateException(s"Unknown coordinator type $coordinatorType")
      }

    val findCoordinatorRequestBuilder = if (version >= 4) {
      new FindCoordinatorRequest.Builder(
        new FindCoordinatorRequestData()
          .setKeyType(coordinatorType.id())
          .setCoordinatorKeys(util.List.of(key)))
    } else {
      new FindCoordinatorRequest.Builder(
        new FindCoordinatorRequestData()
          .setKeyType(coordinatorType.id())
          .setKey(key))
    }
    val request = buildRequest(findCoordinatorRequestBuilder.build(requestHeader.apiVersion))
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    val capturedRequest = verifyTopicCreation(topicName, enableAutoTopicCreation = true, isInternal = true, request)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer),
      overrideProperties = topicConfigOverride)
    kafkaApis.handleFindCoordinatorRequest(request)

    val response = verifyNoThrottling[FindCoordinatorResponse](request)
    if (coordinatorType == CoordinatorType.SHARE && version < 6) {
      assertEquals(Errors.INVALID_REQUEST.code, response.data.coordinators.get(0).errorCode)
    } else if (version >= 4) {
      assertEquals(Errors.COORDINATOR_NOT_AVAILABLE.code, response.data.coordinators.get(0).errorCode)
      assertEquals(key, response.data.coordinators.get(0).key)
    } else {
      assertEquals(Errors.COORDINATOR_NOT_AVAILABLE.code, response.data.errorCode)
      assertTrue(capturedRequest.getValue.isEmpty)
    }
    if (checkAutoCreateTopic) {
      assertTrue(capturedRequest.getValue.isEmpty)
    }
  }

  @Test
  def testFindCoordinatorWithInvalidSharePartitionKey(): Unit = {
    val request = new FindCoordinatorRequestData()
      .setKeyType(CoordinatorType.SHARE.id)
      .setCoordinatorKeys(util.List.of(""))

    val requestChannelRequest = buildRequest(new FindCoordinatorRequest.Builder(request).build())

    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val expectedResponse = new FindCoordinatorResponseData()
      .setCoordinators(util.List.of(
        new FindCoordinatorResponseData.Coordinator()
          .setKey("")
          .setErrorCode(Errors.INVALID_REQUEST.code)
          .setNodeId(-1)
          .setHost("")
          .setPort(-1)))

    val response = verifyNoThrottling[FindCoordinatorResponse](requestChannelRequest)
    assertEquals(expectedResponse, response.data)
  }

  @Test
  def testFindCoordinatorWithValidSharePartitionKey(): Unit = {
    addTopicToMetadataCache(SHARE_GROUP_STATE_TOPIC_NAME, 10, 3)
    val key = SharePartitionKey.getInstance("foo", Uuid.randomUuid(), 10)

    val request = new FindCoordinatorRequestData()
      .setKeyType(CoordinatorType.SHARE.id)
      .setCoordinatorKeys(util.List.of(key.asCoordinatorKey))

    val requestChannelRequest = buildRequest(new FindCoordinatorRequest.Builder(request).build())

    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    when(shareCoordinator.partitionFor(ArgumentMatchers.eq(key))).thenReturn(10)

    val expectedResponse = new FindCoordinatorResponseData()
      .setCoordinators(util.List.of(
        new FindCoordinatorResponseData.Coordinator()
          .setKey(key.asCoordinatorKey)
          .setNodeId(0)
          .setHost("broker0")
          .setPort(9092)))

    val response = verifyNoThrottling[FindCoordinatorResponse](requestChannelRequest)
    assertEquals(expectedResponse, response.data)
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
          topicConfigOverride.put(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, numBrokersNeeded.toString)
          topicConfigOverride.put(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, numBrokersNeeded.toString)
          when(groupCoordinator.groupMetadataTopicConfigs).thenReturn(new Properties)
          true

        case Topic.TRANSACTION_STATE_TOPIC_NAME =>
          topicConfigOverride.put(TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, numBrokersNeeded.toString)
          topicConfigOverride.put(TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, numBrokersNeeded.toString)
          when(txnCoordinator.transactionTopicConfigs).thenReturn(new Properties)
          true
        case _ =>
          topicConfigOverride.put(ServerLogConfigs.NUM_PARTITIONS_CONFIG, numBrokersNeeded.toString)
          topicConfigOverride.put(ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, numBrokersNeeded.toString)
          false
      }

    val metadataRequest = new MetadataRequest.Builder(
      util.List.of(topicName), enableAutoTopicCreation
    ).build(requestHeader.apiVersion)
    val request = buildRequest(metadataRequest)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    val capturedRequest = verifyTopicCreation(topicName, enableAutoTopicCreation, isInternal, request)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer), overrideProperties = topicConfigOverride)
    kafkaApis.handleTopicMetadataRequest(request)

    val response = verifyNoThrottling[MetadataResponse](request)
    val expectedMetadataResponse = util.List.of(new TopicMetadata(
      expectedError,
      topicName,
      isInternal,
      util.List.of()
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

      when(clientControllerQuotaManager.newPermissiveQuotaFor(
        ArgumentMatchers.eq(request.session),
        ArgumentMatchers.eq(request.header.clientId())
      )).thenReturn(ControllerMutationQuota.UNBOUNDED_CONTROLLER_MUTATION_QUOTA)

      when(autoTopicCreationManager.createTopics(
        ArgumentMatchers.eq(Set(topicName)),
        ArgumentMatchers.eq(ControllerMutationQuota.UNBOUNDED_CONTROLLER_MUTATION_QUOTA),
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
        val metadataRequestData = new MetadataRequestData().setTopics(util.List.of(topic))
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

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_COMMIT)
  def testHandleOffsetCommitRequest(version: Short): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    addTopicToMetadataCache(topicName, topicId = topicId, numPartitions = 1)

    val offsetCommitRequest = new OffsetCommitRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setTopics(util.List.of(
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setTopicId(if (version >= 10) topicId else Uuid.ZERO_UUID)
          .setName(if (version < 10) topicName else "")
          .setPartitions(util.List.of(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(10)))))

    val expectedOffsetCommitRequest = new OffsetCommitRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setTopics(util.List.of(
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setTopicId(if (version >= 10) topicId else Uuid.ZERO_UUID)
          .setName(topicName)
          .setPartitions(util.List.of(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(10)))))

    val requestChannelRequest = buildRequest(OffsetCommitRequest.Builder.forTopicIdsOrNames(offsetCommitRequest).build(version))

    val future = new CompletableFuture[OffsetCommitResponseData]()
    when(groupCoordinator.commitOffsets(
      requestChannelRequest.context,
      expectedOffsetCommitRequest,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    // This is the response returned by the group coordinator.
    val offsetCommitResponse = new OffsetCommitResponseData()
      .setTopics(util.List.of(
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setTopicId(if (version >= 10) topicId else Uuid.ZERO_UUID)
          .setName(if (version < 10) topicName else "")
          .setPartitions(util.List.of(
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code)))))

    future.complete(offsetCommitResponse)
    val response = verifyNoThrottling[OffsetCommitResponse](requestChannelRequest)
    assertEquals(offsetCommitResponse, response.data)
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_COMMIT)
  def testHandleOffsetCommitRequestFutureFailed(version: Short): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    addTopicToMetadataCache(topicName, topicId = topicId, numPartitions = 1)

    val offsetCommitRequest = new OffsetCommitRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setTopics(util.List.of(
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setTopicId(if (version >= 10) topicId else Uuid.ZERO_UUID)
          .setName(if (version < 10) topicName else "")
          .setPartitions(util.List.of(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(10)))))

    val expectedOffsetCommitRequest = new OffsetCommitRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setTopics(util.List.of(
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setTopicId(if (version >= 10) topicId else Uuid.ZERO_UUID)
          .setName(topicName)
          .setPartitions(util.List.of(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(10)))))

    val requestChannelRequest = buildRequest(OffsetCommitRequest.Builder.forTopicIdsOrNames(offsetCommitRequest).build(version))

    val future = new CompletableFuture[OffsetCommitResponseData]()
    when(groupCoordinator.commitOffsets(
      requestChannelRequest.context,
      expectedOffsetCommitRequest,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)

    kafkaApis = createKafkaApis()
    kafkaApis.handle(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    val expectedOffsetCommitResponse = new OffsetCommitResponseData()
      .setTopics(util.List.of(
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setTopicId(if (version >= 10) topicId else Uuid.ZERO_UUID)
          .setName(if (version < 10) topicName else "")
          .setPartitions(util.List.of(
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NOT_COORDINATOR.code)))))

    future.completeExceptionally(Errors.NOT_COORDINATOR.exception)
    val response = verifyNoThrottling[OffsetCommitResponse](requestChannelRequest)
    assertEquals(expectedOffsetCommitResponse, response.data)
  }

  @Test
  def testHandleOffsetCommitRequestTopicsAndPartitionsValidationWithTopicIds(): Unit = {
    val fooId = Uuid.randomUuid()
    val barId = Uuid.randomUuid()
    val zarId = Uuid.randomUuid()
    val fooName = "foo"
    val barName = "bar"
    addTopicToMetadataCache(fooName, topicId = fooId, numPartitions = 2)
    addTopicToMetadataCache(barName, topicId = barId, numPartitions = 2)

    val offsetCommitRequest = new OffsetCommitRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setTopics(util.List.of(
        // foo exists but only has 2 partitions.
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setTopicId(fooId)
          .setPartitions(util.List.of(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(10),
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(20),
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(2)
              .setCommittedOffset(30))),
        // bar exists.
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setTopicId(barId)
          .setPartitions(util.List.of(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(40),
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(50))),
        // zar does not exist.
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setTopicId(zarId)
          .setPartitions(util.List.of(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(60),
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(70)))))

    val requestChannelRequest = buildRequest(OffsetCommitRequest.Builder.forTopicIdsOrNames(offsetCommitRequest).build())

    // This is the request expected by the group coordinator.
    val expectedOffsetCommitRequest = new OffsetCommitRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setTopics(util.List.of(
        // foo exists but only has 2 partitions.
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setTopicId(fooId)
          .setName(fooName)
          .setPartitions(util.List.of(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(10),
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(20))),
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setTopicId(barId)
          .setName(barName)
          .setPartitions(util.List.of(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(40),
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(50)))))

    val future = new CompletableFuture[OffsetCommitResponseData]()
    when(groupCoordinator.commitOffsets(
      requestChannelRequest.context,
      expectedOffsetCommitRequest,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    // This is the response returned by the group coordinator.
    val offsetCommitResponse = new OffsetCommitResponseData()
      .setTopics(util.List.of(
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setTopicId(fooId)
          .setName(fooName)
          .setPartitions(util.List.of(
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code))),
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setTopicId(barId)
          .setName(barName)
          .setPartitions(util.List.of(
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)))))

    val expectedOffsetCommitResponse = new OffsetCommitResponseData()
      .setTopics(util.List.of(
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setTopicId(fooId)
          .setPartitions(util.List.of(
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
              .setErrorCode(Errors.NONE.code))),
        // zar is before bar because topics failing the validation are
        // put in the response first.
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setTopicId(zarId)
          .setPartitions(util.List.of(
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.UNKNOWN_TOPIC_ID.code),
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.UNKNOWN_TOPIC_ID.code))),
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setTopicId(barId)
          .setPartitions(util.List.of(
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)))))

    future.complete(offsetCommitResponse)
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
      .setTopics(util.List.of(
        // foo exists but only has 2 partitions.
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName("foo")
          .setPartitions(util.List.of(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(10),
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(20),
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(2)
              .setCommittedOffset(30))),
        // bar exists.
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName("bar")
          .setPartitions(util.List.of(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(40),
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(50))),
        // zar does not exist.
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName("zar")
          .setPartitions(util.List.of(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(60),
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(70)))))

    val requestChannelRequest = buildRequest(OffsetCommitRequest.Builder.forTopicNames(offsetCommitRequest).build())

    // This is the request expected by the group coordinator.
    val expectedOffsetCommitRequest = new OffsetCommitRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setTopics(util.List.of(
        // foo exists but only has 2 partitions.
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName("foo")
          .setPartitions(util.List.of(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(10),
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(20))),
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName("bar")
          .setPartitions(util.List.of(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(40),
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(50)))))

    val future = new CompletableFuture[OffsetCommitResponseData]()
    when(groupCoordinator.commitOffsets(
      requestChannelRequest.context,
      expectedOffsetCommitRequest,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    // This is the response returned by the group coordinator.
    val offsetCommitResponse = new OffsetCommitResponseData()
      .setTopics(util.List.of(
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setName("foo")
          .setPartitions(util.List.of(
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code))),
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setName("bar")
          .setPartitions(util.List.of(
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)))))

    val expectedOffsetCommitResponse = new OffsetCommitResponseData()
      .setTopics(util.List.of(
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setName("foo")
          .setPartitions(util.List.of(
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
              .setErrorCode(Errors.NONE.code))),
        // zar is before bar because topics failing the validation are
        // put in the response first.
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setName("zar")
          .setPartitions(util.List.of(
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code),
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code))),
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setName("bar")
          .setPartitions(util.List.of(
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)))))

    future.complete(offsetCommitResponse)
    val response = verifyNoThrottling[OffsetCommitResponse](requestChannelRequest)
    assertEquals(expectedOffsetCommitResponse, response.data)
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
        util.Map.of(invalidTopicPartition, partitionOffsetCommitData),
        true
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
      .setTopics(util.List.of(
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName("foo")
          .setPartitions(util.List.of(
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(10)))))

    val requestChannelRequest = buildRequest(new TxnOffsetCommitRequest.Builder(txnOffsetCommitRequest).build())

    val future = new CompletableFuture[TxnOffsetCommitResponseData]()
    when(txnCoordinator.partitionFor(txnOffsetCommitRequest.transactionalId)).thenReturn(0)
    when(groupCoordinator.commitTransactionalOffsets(
      requestChannelRequest.context,
      txnOffsetCommitRequest,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    // This is the response returned by the group coordinator.
    val txnOffsetCommitResponse = new TxnOffsetCommitResponseData()
      .setTopics(util.List.of(
        new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
          .setName("foo")
          .setPartitions(util.List.of(
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code)))))

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
      .setTopics(util.List.of(
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName("foo")
          .setPartitions(util.List.of(
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(10)))))

    val requestChannelRequest = buildRequest(new TxnOffsetCommitRequest.Builder(txnOffsetCommitRequest).build())

    val future = new CompletableFuture[TxnOffsetCommitResponseData]()
    when(txnCoordinator.partitionFor(txnOffsetCommitRequest.transactionalId)).thenReturn(0)
    when(groupCoordinator.commitTransactionalOffsets(
      requestChannelRequest.context,
      txnOffsetCommitRequest,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    val expectedTxnOffsetCommitResponse = new TxnOffsetCommitResponseData()
      .setTopics(util.List.of(
        new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
          .setName("foo")
          .setPartitions(util.List.of(
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NOT_COORDINATOR.code)))))

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
      .setTopics(util.List.of(
        // foo exists but only has 2 partitions.
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName("foo")
          .setPartitions(util.List.of(
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(10),
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(20),
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(2)
              .setCommittedOffset(30))),
        // bar exists.
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName("bar")
          .setPartitions(util.List.of(
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(40),
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(50))),
        // zar does not exist.
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName("zar")
          .setPartitions(util.List.of(
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(60),
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(70)))))

    val requestChannelRequest = buildRequest(new TxnOffsetCommitRequest.Builder(txnOffsetCommitRequest).build())

    // This is the request expected by the group coordinator.
    val expectedTxnOffsetCommitRequest = new TxnOffsetCommitRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setTopics(util.List.of(
        // foo exists but only has 2 partitions.
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName("foo")
          .setPartitions(util.List.of(
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(10),
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(20))),
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName("bar")
          .setPartitions(util.List.of(
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(40),
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(50)))))

    val future = new CompletableFuture[TxnOffsetCommitResponseData]()
    when(txnCoordinator.partitionFor(expectedTxnOffsetCommitRequest.transactionalId)).thenReturn(0)
    when(groupCoordinator.commitTransactionalOffsets(
      requestChannelRequest.context,
      expectedTxnOffsetCommitRequest,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    // This is the response returned by the group coordinator.
    val txnOffsetCommitResponse = new TxnOffsetCommitResponseData()
      .setTopics(util.List.of(
        new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
          .setName("foo")
          .setPartitions(util.List.of(
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code))),
        new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
          .setName("bar")
          .setPartitions(util.List.of(
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)))))

    val expectedTxnOffsetCommitResponse = new TxnOffsetCommitResponseData()
      .setTopics(util.List.of(
        new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
          .setName("foo")
          .setPartitions(util.List.of(
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
              .setErrorCode(Errors.NONE.code))),
        // zar is before bar because topics failing the validation are
        // put in the response first.
        new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
          .setName("zar")
          .setPartitions(util.List.of(
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code),
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code))),
        new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
          .setName("bar")
          .setPartitions(util.List.of(
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)))))

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
      util.Map.of(topicPartition, partitionOffsetCommitData),
      version >= TxnOffsetCommitRequest.LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2
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
      .setTopics(util.List.of(
        new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
          .setName(topicPartition.topic)
          .setPartitions(util.List.of(
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(topicPartition.partition)
              .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code)
          ))
      )))
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
          .setEnable2Pc(false)
          .setKeepPreparedTxn(false)
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
        ArgumentMatchers.eq(false),
        ArgumentMatchers.eq(false),
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
        ArgumentMatchers.eq(util.Set.of(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, partition))),
        responseCallback.capture(),
        ArgumentMatchers.eq(TransactionVersion.TV_0),
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
        util.List.of(topicPartition)
      ).build(version.toShort)
      val request = buildRequest(addPartitionsToTxnRequest)

      val requestLocal = RequestLocal.withThreadConfinedCaching
      when(txnCoordinator.handleAddPartitionsToTransaction(
        ArgumentMatchers.eq(transactionalId),
        ArgumentMatchers.eq(producerId),
        ArgumentMatchers.eq(epoch),
        ArgumentMatchers.eq(util.Set.of(topicPartition)),
        responseCallback.capture(),
        ArgumentMatchers.eq(TransactionVersion.TV_0),
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
          assertEquals(util.Map.of(topicPartition, Errors.INVALID_PRODUCER_EPOCH), response.errors().get(AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID))
        } else {
          assertEquals(util.Map.of(topicPartition, Errors.PRODUCER_FENCED), response.errors().get(AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID))
        }
      } finally {
        kafkaApis.close()
      }
    }
  }

  @Test
  def testInitProducerIdWithEnable2PcFailsWithoutTwoPhaseCommitAcl(): Unit = {
    val transactionalId = "txnId"
    addTopicToMetadataCache("topic", numPartitions = 1)

    val initProducerIdRequest = new InitProducerIdRequest.Builder(
      new InitProducerIdRequestData()
        .setTransactionalId(transactionalId)
        .setTransactionTimeoutMs(TimeUnit.MINUTES.toMillis(15).toInt)
        .setEnable2Pc(true)
        .setProducerId(RecordBatch.NO_PRODUCER_ID)
        .setProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH)
    ).build(6.toShort) // Use version 6 which supports enable2Pc

    val request = buildRequest(initProducerIdRequest)
    val requestLocal = RequestLocal.withThreadConfinedCaching
    val authorizer: Authorizer = mock(classOf[Authorizer])
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))

    // Allow WRITE but deny TWO_PHASE_COMMIT
    when(authorizer.authorize(
      any(),
      ArgumentMatchers.eq(util.List.of(new Action(
        AclOperation.WRITE,
        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, transactionalId, PatternType.LITERAL),
        1,
        true,
        true)))
    )).thenReturn(util.List.of(AuthorizationResult.ALLOWED))

    when(authorizer.authorize(
      any(),
      ArgumentMatchers.eq(util.List.of(new Action(
        AclOperation.TWO_PHASE_COMMIT,
        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, transactionalId, PatternType.LITERAL),
        1,
        true,
        true)))
    )).thenReturn(util.List.of(AuthorizationResult.DENIED))

    val capturedResponse = ArgumentCaptor.forClass(classOf[InitProducerIdResponse])

    kafkaApis.handleInitProducerIdRequest(request, requestLocal)

    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None)
    )

    assertEquals(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code, capturedResponse.getValue.data.errorCode)
  }

  @Test
  def testInitProducerIdWithEnable2PcSucceedsWithTwoPhaseCommitAcl(): Unit = {
    val transactionalId = "txnId"
    addTopicToMetadataCache("topic", numPartitions = 1)

    val initProducerIdRequest = new InitProducerIdRequest.Builder(
      new InitProducerIdRequestData()
        .setTransactionalId(transactionalId)
        .setTransactionTimeoutMs(TimeUnit.MINUTES.toMillis(15).toInt)
        .setEnable2Pc(true)
        .setProducerId(RecordBatch.NO_PRODUCER_ID)
        .setProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH)
    ).build(6.toShort) // Use version 6 which supports enable2Pc

    val request = buildRequest(initProducerIdRequest)
    val requestLocal = RequestLocal.withThreadConfinedCaching
    val authorizer: Authorizer = mock(classOf[Authorizer])
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))

    // Both permissions are allowed
    when(authorizer.authorize(
      any(),
      ArgumentMatchers.eq(util.List.of(new Action(
        AclOperation.WRITE,
        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, transactionalId, PatternType.LITERAL),
        1,
        true,
        true)))
    )).thenReturn(util.List.of(AuthorizationResult.ALLOWED))

    when(authorizer.authorize(
      any(),
      ArgumentMatchers.eq(util.List.of(new Action(
        AclOperation.TWO_PHASE_COMMIT,
        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, transactionalId, PatternType.LITERAL),
        1,
        true,
        true)))
    )).thenReturn(util.List.of(AuthorizationResult.ALLOWED))

    val responseCallback = ArgumentCaptor.forClass(classOf[InitProducerIdResult => Unit])

    when(txnCoordinator.handleInitProducerId(
      ArgumentMatchers.eq(transactionalId),
      anyInt(),
      ArgumentMatchers.eq(true), // enable2Pc = true
      anyBoolean(),
      any(),
      responseCallback.capture(),
      ArgumentMatchers.eq(requestLocal)
    )).thenAnswer(_ => responseCallback.getValue.apply(InitProducerIdResult(15L, 0.toShort, Errors.NONE)))

    kafkaApis.handleInitProducerIdRequest(request, requestLocal)

    // Verify coordinator was called with enable2Pc=true
    verify(txnCoordinator).handleInitProducerId(
      ArgumentMatchers.eq(transactionalId),
      anyInt(),
      ArgumentMatchers.eq(true), // enable2Pc = true
      anyBoolean(),
      any(),
      any(),
      ArgumentMatchers.eq(requestLocal)
    )

    val capturedResponse = ArgumentCaptor.forClass(classOf[InitProducerIdResponse])
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None)
    )

    assertEquals(Errors.NONE.code, capturedResponse.getValue.data.errorCode)
    assertEquals(15L, capturedResponse.getValue.data.producerId)
    assertEquals(0, capturedResponse.getValue.data.producerEpoch)
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
        util.List.of(new AddPartitionsToTxnTransaction()
          .setTransactionalId(transactionalId1)
          .setProducerId(producerId)
          .setProducerEpoch(epoch)
          .setVerifyOnly(false)
          .setTopics(new AddPartitionsToTxnTopicCollection(
            util.List.of(new AddPartitionsToTxnTopic()
              .setName(tp0.topic)
              .setPartitions(util.List.of(tp0.partition))
            ).iterator())
          ), new AddPartitionsToTxnTransaction()
          .setTransactionalId(transactionalId2)
          .setProducerId(producerId)
          .setProducerEpoch(epoch)
          .setVerifyOnly(true)
          .setTopics(new AddPartitionsToTxnTopicCollection(
            util.List.of(new AddPartitionsToTxnTopic()
              .setName(tp1.topic)
              .setPartitions(util.List.of(tp1.partition))
            ).iterator())
          )
        ).iterator()
      )
    ).build(4.toShort)
    val request = buildRequest(addPartitionsToTxnRequest)

    val requestLocal = RequestLocal.withThreadConfinedCaching
    when(txnCoordinator.handleAddPartitionsToTransaction(
      ArgumentMatchers.eq(transactionalId1),
      ArgumentMatchers.eq(producerId),
      ArgumentMatchers.eq(epoch),
      ArgumentMatchers.eq(util.Set.of(tp0)),
      responseCallback.capture(),
      any[TransactionVersion],
      ArgumentMatchers.eq(requestLocal)
    )).thenAnswer(_ => responseCallback.getValue.apply(Errors.NONE))

    when(txnCoordinator.handleVerifyPartitionsInTransaction(
      ArgumentMatchers.eq(transactionalId2),
      ArgumentMatchers.eq(producerId),
      ArgumentMatchers.eq(epoch),
      ArgumentMatchers.eq(util.Set.of(tp1)),
      verifyPartitionsCallback.capture(),
    )).thenAnswer(_ => verifyPartitionsCallback.getValue.apply(AddPartitionsToTxnResponse.resultForTransaction(transactionalId2, util.Map.of(tp1, Errors.PRODUCER_FENCED))))
    kafkaApis = createKafkaApis()
    kafkaApis.handleAddPartitionsToTxnRequest(request, requestLocal)

    val response = verifyNoThrottling[AddPartitionsToTxnResponse](request)

    val expectedErrors = util.Map.of(
      transactionalId1, util.Map.of(tp0, Errors.NONE),
      transactionalId2, util.Map.of(tp1, Errors.PRODUCER_FENCED)
    )

    assertEquals(expectedErrors, response.errors())
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.ADD_PARTITIONS_TO_TXN)
  def testHandleAddPartitionsToTxnAuthorizationFailedAndMetrics(version: Short): Unit = {
    val requestMetrics = new RequestChannelMetrics(util.Set.of(ApiKeys.ADD_PARTITIONS_TO_TXN))
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
            util.List.of(tp)).build(version)
        else
          AddPartitionsToTxnRequest.Builder.forBroker(
            new AddPartitionsToTxnTransactionCollection(
              util.List.of(new AddPartitionsToTxnTransaction()
                .setTransactionalId(transactionalId)
                .setProducerId(producerId)
                .setProducerEpoch(epoch)
                .setVerifyOnly(true)
                .setTopics(new AddPartitionsToTxnTopicCollection(
                  util.List.of(new AddPartitionsToTxnTopic()
                    .setName(tp.topic)
                    .setPartitions(util.List.of(tp.partition))
                  ).iterator()))
              ).iterator())).build(version)

      val requestChannelRequest = buildRequest(addPartitionsToTxnRequest, requestMetrics = requestMetrics)

      val authorizer: Authorizer = mock(classOf[Authorizer])
      when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
        .thenReturn(util.List.of(AuthorizationResult.DENIED))
      kafkaApis = createKafkaApis(authorizer = Some(authorizer))
      kafkaApis.handle(
        requestChannelRequest,
        RequestLocal.noCaching
      )

      val response = verifyNoThrottlingAndUpdateMetrics[AddPartitionsToTxnResponse](requestChannelRequest)
      val error = if (version < 4)
        response.errors().get(AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID).get(tp)
      else
        Errors.forCode(response.data().errorCode)

      val expectedError = if (version < 4) Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED else Errors.CLUSTER_AUTHORIZATION_FAILED
      assertEquals(expectedError, error)

      val metricName = if (version < 4) ApiKeys.ADD_PARTITIONS_TO_TXN.name else RequestMetrics.VERIFY_PARTITIONS_IN_TXN_METRIC_NAME
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
        util.List.of(tp0, tp1)).build(version)
    else
      AddPartitionsToTxnRequest.Builder.forBroker(
        new AddPartitionsToTxnTransactionCollection(
          util.List.of(new AddPartitionsToTxnTransaction()
            .setTransactionalId(transactionalId)
            .setProducerId(producerId)
            .setProducerEpoch(epoch)
            .setVerifyOnly(true)
            .setTopics(new AddPartitionsToTxnTopicCollection(
              util.List.of(new AddPartitionsToTxnTopic()
                .setName(tp0.topic)
                .setPartitions(util.List.of[Integer](tp0.partition, tp1.partition()))
              ).iterator()))
          ).iterator())).build(version)

    val requestChannelRequest = buildRequest(addPartitionsToTxnRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleAddPartitionsToTxnRequest(
      requestChannelRequest,
      RequestLocal.noCaching
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
      val responseCallback: ArgumentCaptor[(Errors, Long, Short) => Unit] = ArgumentCaptor.forClass(classOf[(Errors, Long, Short) => Unit])

      val transactionalId = "txnId"
      val producerId = 15L
      val epoch = 0.toShort

      val clientTransactionVersion = if (version > 4) TransactionVersion.TV_2 else TransactionVersion.TV_0
      val isTransactionV2Enabled = clientTransactionVersion.equals(TransactionVersion.TV_2)

      val endTxnRequest = new EndTxnRequest.Builder(
        new EndTxnRequestData()
          .setTransactionalId(transactionalId)
          .setProducerId(producerId)
          .setProducerEpoch(epoch)
          .setCommitted(true),
        isTransactionV2Enabled
      ).build(version.toShort)
      val request = buildRequest(endTxnRequest)

      val requestLocal = RequestLocal.withThreadConfinedCaching
      when(txnCoordinator.handleEndTransaction(
        ArgumentMatchers.eq(transactionalId),
        ArgumentMatchers.eq(producerId),
        ArgumentMatchers.eq(epoch),
        ArgumentMatchers.eq(TransactionResult.COMMIT),
        ArgumentMatchers.eq(clientTransactionVersion),
        responseCallback.capture(),
        ArgumentMatchers.eq(requestLocal)
      )).thenAnswer(_ => responseCallback.getValue.apply(Errors.PRODUCER_FENCED, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH))
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
    val topicId = Uuid.fromString("d2Gg8tgzJa2JYK2eTHUapg")
    val tp = new TopicIdPartition(topicId, 0, "topic")
    addTopicToMetadataCache(topic, numPartitions = 2, topicId = topicId)

    for (version <- ApiKeys.PRODUCE.oldestVersion to ApiKeys.PRODUCE.latestVersion) {

      reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      val responseCallback: ArgumentCaptor[Map[TopicIdPartition, PartitionResponse] => Unit] = ArgumentCaptor.forClass(classOf[Map[TopicIdPartition, PartitionResponse] => Unit])

      val produceData = new ProduceRequestData.TopicProduceData()
        .setPartitionData(util.List.of(
          new ProduceRequestData.PartitionProduceData()
            .setIndex(tp.partition)
            .setRecords(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("test".getBytes)))))

      if (version >= 13 ) {
        produceData.setTopicId(topicId)
      } else {
        produceData.setName(tp.topic)
      }

      val produceRequest = ProduceRequest.builder(new ProduceRequestData()
        .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
          util.List.of(produceData)
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
        any[Session](), anyString, anyDouble, anyLong)).thenReturn(0)
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
  def testHandleShareFetchRequestQuotaTagsVerification(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: String = Uuid.randomUuid().toString
    val groupId = "group"
    
    // Create test principal and client address to verify quota tags
    val testPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "test-user")
    val testClientAddress = InetAddress.getByName("192.168.1.100")
    val testClientId = "test-client-id"
    
    // Mock share partition manager responses
    val records = memoryRecords(10, 0)
    when(sharePartitionManager.fetchMessages(any(), any(), any(), any(), anyInt(), anyInt(), anyInt(), any())).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)),
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setAcknowledgeErrorCode(Errors.NONE.code)
            .setRecords(records)
            .setAcquiredRecords(new util.ArrayList(util.List.of(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            ))))))

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any(), any(), any())).thenReturn(
      new ShareSessionContext(0, util.List.of(
        new TopicIdPartition(topicId, partitionIndex, topicName)))
    )

    // Create argument captors to verify session information passed to quota managers
    val sessionCaptorFetch = ArgumentCaptor.forClass(classOf[Session])
    val clientIdCaptor = ArgumentCaptor.forClass(classOf[String])
    val requestCaptor = ArgumentCaptor.forClass(classOf[RequestChannel.Request])
    
    // Mock quota manager responses and capture arguments
    when(quotas.fetch.maybeRecordAndGetThrottleTimeMs(
      sessionCaptorFetch.capture(), clientIdCaptor.capture(), anyDouble, anyLong)).thenReturn(0)
    when(quotas.request.maybeRecordAndGetThrottleTimeMs(
      requestCaptor.capture(), anyLong)).thenReturn(0)

    // Create ShareFetch request
    val shareFetchRequestData = new ShareFetchRequestData()
      .setGroupId(groupId)
      .setMemberId(memberId)
      .setShareSessionEpoch(0)
      .setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(new ShareFetchRequestData.FetchTopic()
        .setTopicId(topicId)
        .setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(partitionIndex)
        ).iterator))
      ).iterator))

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    
    // Create request with custom principal and client address to test quota tags
    val requestHeader = new RequestHeader(shareFetchRequest.apiKey, shareFetchRequest.version, testClientId, 0)
    val request = buildRequest(shareFetchRequest, testPrincipal, testClientAddress, 
      ListenerName.forSecurityProtocol(SecurityProtocol.SSL), fromPrivilegedListener = false, Some(requestHeader), requestChannelMetrics)
    
    // Test that the request itself contains the proper tags and information
    assertEquals(testClientId, request.header.clientId)
    assertEquals(testPrincipal, request.context.principal)
    assertEquals(testClientAddress, request.context.clientAddress)
    assertEquals(ApiKeys.SHARE_FETCH, request.header.apiKey)
    assertEquals("1", request.context.connectionId)

    kafkaApis = createKafkaApis()
    kafkaApis.handleShareFetchRequest(request)
    val response = verifyNoThrottling[ShareFetchResponse](request)
    
    // Verify response is successful
    val responseData = response.data()
    assertEquals(Errors.NONE.code, responseData.errorCode)
    
    // Verify that quota methods were called and captured session information
    verify(quotas.fetch, times(1)).maybeRecordAndGetThrottleTimeMs(
      any[Session](), anyString, anyDouble, anyLong)
    verify(quotas.request, times(1)).maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)
    
    // Verify the Session data passed to fetch quota manager is exactly what was defined in the test
    val capturedSession = sessionCaptorFetch.getValue
    assertNotNull(capturedSession)
    assertNotNull(capturedSession.principal)
    assertEquals(KafkaPrincipal.USER_TYPE, capturedSession.principal.getPrincipalType)
    assertEquals("test-user", capturedSession.principal.getName)
    assertEquals(testClientAddress, capturedSession.clientAddress)
    assertEquals("test-user", capturedSession.sanitizedUser)
    
    // Verify client ID passed to fetch quota manager matches what was defined
    val capturedClientId = clientIdCaptor.getValue
    assertEquals(testClientId, capturedClientId)
    
    // Verify the Request data passed to request quota manager is exactly what was defined
    val capturedRequest = requestCaptor.getValue
    assertNotNull(capturedRequest)
    assertEquals(testClientId, capturedRequest.header.clientId)
    assertEquals(testPrincipal, capturedRequest.context.principal)
    assertEquals(testClientAddress, capturedRequest.context.clientAddress)
    assertEquals(ApiKeys.SHARE_FETCH, capturedRequest.header.apiKey)
  }

  @Test
  def testHandleShareAcknowledgeRequestQuotaTagsVerification(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.randomUuid()
    val groupId = "group"
    
    // Create test principal and client address to verify quota tags
    val testPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "test-user")
    val testClientAddress = InetAddress.getByName("192.168.1.100")
    val testClientId = "test-client-id"
    
    // Mock share partition manager acknowledge response
    when(sharePartitionManager.acknowledge(any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)),
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(partitionIndex)
            .setErrorCode(Errors.NONE.code))))

    // Create argument captors to verify session information passed to quota managers
    val requestCaptor = ArgumentCaptor.forClass(classOf[RequestChannel.Request])
    
    // Mock quota manager responses and capture arguments
    // For ShareAcknowledge, we only verify Request quota (not fetch quota)
    when(quotas.request.maybeRecordAndGetThrottleTimeMs(
      requestCaptor.capture(), anyLong)).thenReturn(0)

    // Create ShareAcknowledge request
    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData()
      .setGroupId(groupId)
      .setMemberId(memberId.toString)
      .setShareSessionEpoch(1)
      .setTopics(new ShareAcknowledgeRequestData.AcknowledgeTopicCollection(
        util.List.of(new ShareAcknowledgeRequestData.AcknowledgeTopic()
          .setTopicId(topicId)
          .setPartitions(new ShareAcknowledgeRequestData.AcknowledgePartitionCollection(
            util.List.of(new ShareAcknowledgeRequestData.AcknowledgePartition()
              .setPartitionIndex(partitionIndex)
              .setAcknowledgementBatches(util.List.of(
                new ShareAcknowledgeRequestData.AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(util.List.of(1.toByte))
              ))
            ).iterator))
        ).iterator))

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData).build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    
    // Create request with custom principal and client address to test quota tags
    val requestHeader = new RequestHeader(shareAcknowledgeRequest.apiKey, shareAcknowledgeRequest.version, testClientId, 0)
    val request = buildRequest(shareAcknowledgeRequest, testPrincipal, testClientAddress,
      ListenerName.forSecurityProtocol(SecurityProtocol.SSL), fromPrivilegedListener = false, Some(requestHeader), requestChannelMetrics)
    
    // Test that the request itself contains the proper tags and information
    assertEquals(testClientId, request.header.clientId)
    assertEquals(testPrincipal, request.context.principal)
    assertEquals(testClientAddress, request.context.clientAddress)
    assertEquals(ApiKeys.SHARE_ACKNOWLEDGE, request.header.apiKey)
    assertEquals("1", request.context.connectionId)

    kafkaApis = createKafkaApis()
    kafkaApis.handleShareAcknowledgeRequest(request)
    val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
    
    // Verify response is successful
    val responseData = response.data()
    assertEquals(Errors.NONE.code, responseData.errorCode)
    
    // Verify that request quota method was called
    verify(quotas.request, times(1)).maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)
    
    // Verify that fetch quota method was NOT called (ShareAcknowledge only uses request quota)
    verify(quotas.fetch, times(0)).maybeRecordAndGetThrottleTimeMs(
      any[Session](), anyString, anyDouble, anyLong)
    
    // Verify the Request data passed to request quota manager is exactly what was defined
    val capturedRequest = requestCaptor.getValue
    assertNotNull(capturedRequest)
    assertEquals(testClientId, capturedRequest.header.clientId)
    assertEquals(testPrincipal, capturedRequest.context.principal)
    assertEquals(testClientAddress, capturedRequest.context.clientAddress)
    assertEquals(ApiKeys.SHARE_ACKNOWLEDGE, capturedRequest.header.apiKey)
  }

  @Test
  def testHandleShareFetchWithAcknowledgementQuotaTagsVerification(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: String = Uuid.randomUuid().toString
    val groupId = "group"
    
    // Create test principal and client address to verify quota tags
    val testPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "test-user")
    val testClientAddress = InetAddress.getByName("192.168.1.100")
    val testClientId = "test-client-id"
    
    // Mock share partition manager responses for both fetch and acknowledge
    val records = memoryRecords(10, 0)
    when(sharePartitionManager.fetchMessages(any(), any(), any(), any(), anyInt(), anyInt(), anyInt(), any())).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)),
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setAcknowledgeErrorCode(Errors.NONE.code)
            .setRecords(records)
            .setAcquiredRecords(new util.ArrayList(util.List.of(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            ))))))

    when(sharePartitionManager.acknowledge(any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)),
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(partitionIndex)
            .setErrorCode(Errors.NONE.code))))

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any(), any(), any())).thenReturn(
      new ShareSessionContext(1, util.List.of(
        new TopicIdPartition(topicId, partitionIndex, topicName)))
    )

    // Create argument captors to verify session information passed to quota managers
    val sessionCaptorFetch = ArgumentCaptor.forClass(classOf[Session])
    val clientIdCaptor = ArgumentCaptor.forClass(classOf[String])
    val requestCaptor = ArgumentCaptor.forClass(classOf[RequestChannel.Request])
    
    // Mock quota manager responses and capture arguments
    when(quotas.fetch.maybeRecordAndGetThrottleTimeMs(
      sessionCaptorFetch.capture(), clientIdCaptor.capture(), anyDouble, anyLong)).thenReturn(0)
    when(quotas.request.maybeRecordAndGetThrottleTimeMs(
      requestCaptor.capture(), anyLong)).thenReturn(0)

    // Create ShareFetch request with acknowledgement data
    val shareFetchRequestData = new ShareFetchRequestData()
      .setGroupId(groupId)
      .setMemberId(memberId)
      .setShareSessionEpoch(1)
      .setMaxWaitMs(100)
      .setMinBytes(1)
      .setMaxBytes(1000000)
      .setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(new ShareFetchRequestData.FetchTopic()
        .setTopicId(topicId)
        .setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(partitionIndex)
            .setAcknowledgementBatches(util.List.of(
              new ShareFetchRequestData.AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(util.List.of(1.toByte))
            ))
        ).iterator))
      ).iterator))

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    
    // Create request with custom principal and client address to test quota tags
    val requestHeader = new RequestHeader(shareFetchRequest.apiKey, shareFetchRequest.version, testClientId, 0)
    val request = buildRequest(shareFetchRequest, testPrincipal, testClientAddress,
      ListenerName.forSecurityProtocol(SecurityProtocol.SSL), fromPrivilegedListener = false, Some(requestHeader), requestChannelMetrics)
    
    // Test that the request itself contains the proper tags and information
    assertEquals(testClientId, request.header.clientId)
    assertEquals(testPrincipal, request.context.principal)
    assertEquals(testClientAddress, request.context.clientAddress)
    assertEquals(ApiKeys.SHARE_FETCH, request.header.apiKey)
    assertEquals("1", request.context.connectionId)

    kafkaApis = createKafkaApis()
    kafkaApis.handleShareFetchRequest(request)
    val response = verifyNoThrottling[ShareFetchResponse](request)
    
    // Verify response is successful
    val responseData = response.data()
    assertEquals(Errors.NONE.code, responseData.errorCode)
    
    // Verify that quota methods were called exactly once each (not twice despite having acknowledgements)
    verify(quotas.fetch, times(1)).maybeRecordAndGetThrottleTimeMs(
      any[Session](), anyString, anyDouble, anyLong)
    verify(quotas.request, times(1)).maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)
    
    // Verify the Session data passed to fetch quota manager is exactly what was defined in the test
    val capturedSession = sessionCaptorFetch.getValue
    assertNotNull(capturedSession)
    assertNotNull(capturedSession.principal)
    assertEquals(KafkaPrincipal.USER_TYPE, capturedSession.principal.getPrincipalType)
    assertEquals("test-user", capturedSession.principal.getName)
    assertEquals(testClientAddress, capturedSession.clientAddress)
    assertEquals("test-user", capturedSession.sanitizedUser)
    
    // Verify client ID passed to fetch quota manager matches what was defined
    val capturedClientId = clientIdCaptor.getValue
    assertEquals(testClientId, capturedClientId)
    
    // Verify the Request data passed to request quota manager is exactly what was defined
    val capturedRequest = requestCaptor.getValue
    assertNotNull(capturedRequest)
    assertEquals(testClientId, capturedRequest.header.clientId)
    assertEquals(testPrincipal, capturedRequest.context.principal)
    assertEquals(testClientAddress, capturedRequest.context.clientAddress)
    assertEquals(ApiKeys.SHARE_FETCH, capturedRequest.header.apiKey)
  }

  @Test
  def testProduceResponseContainsNewLeaderOnNotLeaderOrFollower(): Unit = {
    val topic = "topic"
    val topicId = Uuid.fromString("d2Gg8tgzJa2JYK2eTHUapg")
    addTopicToMetadataCache(topic, numPartitions = 2, numBrokers = 3, topicId = topicId)

    for (version <- 10 to ApiKeys.PRODUCE.latestVersion) {

      reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      val responseCallback: ArgumentCaptor[Map[TopicIdPartition, PartitionResponse] => Unit] = ArgumentCaptor.forClass(classOf[Map[TopicIdPartition, PartitionResponse] => Unit])

      val tp = new TopicIdPartition(topicId, 0, topic)
      val partition = mock(classOf[Partition])
      val newLeaderId = 2
      val newLeaderEpoch = 5

      val produceData = new ProduceRequestData.TopicProduceData()
        .setPartitionData(util.List.of(
          new ProduceRequestData.PartitionProduceData()
            .setIndex(tp.partition)
            .setRecords(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("test".getBytes)))))

      if (version >= 13 ) {
        produceData.setTopicId(topicId)
      } else {
        produceData.setName(tp.topic)
      }
      val produceRequest = ProduceRequest.builder(new ProduceRequestData()
        .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
          util.List.of(produceData).iterator))
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

      when(replicaManager.getPartitionOrError(tp.topicPartition())).thenAnswer(_ => Right(partition))
      when(partition.leaderReplicaIdOpt).thenAnswer(_ => Some(newLeaderId))
      when(partition.getLeaderEpoch).thenAnswer(_ => newLeaderEpoch)

      when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
        any[Long])).thenReturn(0)
      when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
       any[Session](), anyString, anyDouble, anyLong)).thenReturn(0)
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
    val topicId = Uuid.fromString("d2Gg8tgzJa2JYK2eTHUapg")
    addTopicToMetadataCache(topic, numPartitions = 2, numBrokers = 3, topicId = topicId)

    for (version <- 10 to ApiKeys.PRODUCE.latestVersion) {

      reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      val responseCallback: ArgumentCaptor[Map[TopicIdPartition, PartitionResponse] => Unit] = ArgumentCaptor.forClass(classOf[Map[TopicIdPartition, PartitionResponse] => Unit])

      val tp = new TopicIdPartition(topicId, 0, topic)

      val produceData = new ProduceRequestData.TopicProduceData()
        .setPartitionData(util.List.of(
          new ProduceRequestData.PartitionProduceData()
            .setIndex(tp.partition)
            .setRecords(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("test".getBytes)))))

      if (version >= 13 ) {
        produceData.setTopicId(topicId)
      } else {
        produceData.setName(tp.topic)
      }
      val produceRequest = ProduceRequest.builder(new ProduceRequestData()
        .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
          util.List.of(produceData).iterator))
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

      when(replicaManager.getPartitionOrError(tp.topicPartition())).thenAnswer(_ => Left(Errors.UNKNOWN_TOPIC_OR_PARTITION))

      when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
        any[Long])).thenReturn(0)
      when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
        any[Session](), anyString, anyDouble, anyLong)).thenReturn(0)
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
    val topicId = Uuid.fromString("d2Gg8tgzJa2JYK2eTHUapg")
    metadataCache = mock(classOf[KRaftMetadataCache])

    for (version <- 10 to ApiKeys.PRODUCE.latestVersion) {

      reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      val responseCallback: ArgumentCaptor[Map[TopicIdPartition, PartitionResponse] => Unit] = ArgumentCaptor.forClass(classOf[Map[TopicIdPartition, PartitionResponse] => Unit])

      val tp = new TopicIdPartition(topicId, 0, topic)

      val topicProduceData = new ProduceRequestData.TopicProduceData()

      if (version >= 13 ) {
        topicProduceData.setTopicId(topicId)
      } else {
        topicProduceData.setName(tp.topic)
      }

      val produceRequest = ProduceRequest.builder(new ProduceRequestData()
        .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
          util.List.of(topicProduceData
            .setPartitionData(util.List.of(
            new ProduceRequestData.PartitionProduceData()
              .setIndex(tp.partition)
              .setRecords(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("test".getBytes))))))
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

      when(replicaManager.getPartitionOrError(tp.topicPartition)).thenAnswer(_ => Left(Errors.UNKNOWN_TOPIC_OR_PARTITION))

      when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
        any[Long])).thenReturn(0)
      when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
        any[Session](), anyString, anyDouble, anyLong)).thenReturn(0)
      when(metadataCache.contains(tp.topicPartition())).thenAnswer(_ => true)
      when(metadataCache.getLeaderAndIsr(tp.topic(), tp.partition())).thenAnswer(_ => Optional.empty())
      when(metadataCache.getAliveBrokerNode(any(), any())).thenReturn(Optional.empty())
      if (version >= 13) {
        when(metadataCache.getTopicName(tp.topicId())).thenReturn(Optional.of(tp.topic()))
      } else {
        when(metadataCache.getTopicId(tp.topic())).thenReturn(tp.topicId())
      }
      val kafkaApis = createKafkaApis()
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

    val topicId = Uuid.fromString("d2Gg8tgzJa2JYK2eTHUapg")
    val tp = new TopicIdPartition(topicId, 0, "topic")
    addTopicToMetadataCache(topic, numPartitions = 2, topicId = tp.topicId())

    for (version <- ApiKeys.PRODUCE.oldestVersion to ApiKeys.PRODUCE.latestVersion) {

      reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      val produceData = new ProduceRequestData.TopicProduceData()
        .setPartitionData(util.List.of(
          new ProduceRequestData.PartitionProduceData()
            .setIndex(tp.partition)
            .setRecords(MemoryRecords.withTransactionalRecords(Compression.NONE, 0, 0, 0, new SimpleRecord("test".getBytes)))))

      if (version >= 13 ) {
        produceData.setTopicId(topicId)
      } else {
        produceData.setName(tp.topic)
      }
      val produceRequest = ProduceRequest.builder(new ProduceRequestData()
        .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
          util.List.of(produceData)
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
        "txnlId", 15L, 0.toShort, util.List.of(invalidTopicPartition)
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
  def requiredAclsNotPresentWriteTxnMarkersThrowsAuthorizationException(): Unit = {
    val topicPartition = new TopicPartition("t", 0)
    val (_, request) = createWriteTxnMarkersRequest(util.List.of(topicPartition))

    val authorizer: Authorizer = mock(classOf[Authorizer])
    val clusterResource = new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL)
    val alterActions = util.List.of(new Action(AclOperation.ALTER, clusterResource, 1, true, false))
    val clusterActions = util.List.of(new Action(AclOperation.CLUSTER_ACTION, clusterResource, 1, true, true))
    val deniedList = util.List.of(AuthorizationResult.DENIED)
    when(authorizer.authorize(
      request.context,
      alterActions
    )).thenReturn(deniedList)
    when(authorizer.authorize(
      request.context,
      clusterActions
    )).thenReturn(deniedList)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))

    assertThrows(classOf[ClusterAuthorizationException],
      () => kafkaApis.handleWriteTxnMarkersRequest(request, RequestLocal.withThreadConfinedCaching))
  }

  @Test
  def shouldRespondWithUnknownTopicWhenPartitionIsNotHosted(): Unit = {
    val topicPartition = new TopicPartition("t", 0)
    val (_, request) = createWriteTxnMarkersRequest(util.List.of(topicPartition))
    val expectedErrors = util.Map.of(topicPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION)
    val capturedResponse: ArgumentCaptor[WriteTxnMarkersResponse] = ArgumentCaptor.forClass(classOf[WriteTxnMarkersResponse])

    when(replicaManager.onlinePartition(topicPartition))
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
  def testWriteTxnMarkersShouldAllBeIncludedInTheResponse(): Unit = {
    // This test verifies the response will not be sent prematurely because of calling replicaManager append
    // with no records.
    val topicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)
    val writeTxnMarkersRequest = new WriteTxnMarkersRequest.Builder(
      util.List.of(
        new TxnMarkerEntry(1, 1.toShort, 0, TransactionResult.COMMIT, util.List.of(topicPartition), TransactionVersion.TV_2.featureLevel()),
        new TxnMarkerEntry(2, 1.toShort, 0, TransactionResult.COMMIT, util.List.of(topicPartition), TransactionVersion.TV_2.featureLevel())
      )).build()
    val request = buildRequest(writeTxnMarkersRequest)
    val capturedResponse: ArgumentCaptor[WriteTxnMarkersResponse] = ArgumentCaptor.forClass(classOf[WriteTxnMarkersResponse])

    when(replicaManager.onlinePartition(any()))
      .thenReturn(Some(mock(classOf[Partition])))
    when(groupCoordinator.completeTransaction(
      ArgumentMatchers.eq(topicPartition),
      any(),
      ArgumentMatchers.eq(1.toShort),
      ArgumentMatchers.eq(0),
      ArgumentMatchers.eq(TransactionResult.COMMIT),
      ArgumentMatchers.eq(TransactionVersion.TV_2.featureLevel())
    )).thenReturn(CompletableFuture.completedFuture[Void](null))

    kafkaApis = createKafkaApis()
    kafkaApis.handleWriteTxnMarkersRequest(request, RequestLocal.withThreadConfinedCaching)

    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None)
    )
    val markersResponse = capturedResponse.getValue
    assertEquals(2, markersResponse.errorsByProducerId.size())
  }

  @Test
  def shouldRespondWithUnknownTopicOrPartitionForBadPartitionAndNoErrorsForGoodPartition(): Unit = {
    val tp1 = new TopicPartition("t", 0)
    val tp2 = new TopicPartition("t1", 0)
    val topicId = Uuid.fromString("d2Gg8tgzJa2JYK2eTHUapg")
    val (_, request) = createWriteTxnMarkersRequest(util.List.of(tp1, tp2))
    val expectedErrors = util.Map.of(tp1, Errors.UNKNOWN_TOPIC_OR_PARTITION, tp2, Errors.NONE)

    val capturedResponse: ArgumentCaptor[WriteTxnMarkersResponse] = ArgumentCaptor.forClass(classOf[WriteTxnMarkersResponse])
    val responseCallback: ArgumentCaptor[Map[TopicIdPartition, PartitionResponse] => Unit] = ArgumentCaptor.forClass(classOf[Map[TopicIdPartition, PartitionResponse] => Unit])

    when(replicaManager.onlinePartition(tp1))
      .thenReturn(None)
    when(replicaManager.onlinePartition(tp2))
      .thenReturn(Some(mock(classOf[Partition])))

    val requestLocal = RequestLocal.withThreadConfinedCaching
    when(replicaManager.appendRecords(anyLong,
      anyShort,
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      any(),
      responseCallback.capture(),
      any(),
      ArgumentMatchers.eq(requestLocal),
      any(),
      any()
    )).thenAnswer(_ => responseCallback.getValue.apply(Map(new TopicIdPartition(topicId,tp2) -> new PartitionResponse(Errors.NONE))))
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

  @ParameterizedTest
  @ValueSource(strings = Array("ALTER", "CLUSTER_ACTION"))
  def shouldAppendToLogOnWriteTxnMarkersWhenCorrectMagicVersion(allowedAclOperation: String): Unit = {
    val topicPartition = new TopicPartition("t", 0)
    val request = createWriteTxnMarkersRequest(util.List.of(topicPartition))._2
    when(replicaManager.onlinePartition(topicPartition))
      .thenReturn(Some(mock(classOf[Partition])))

    val requestLocal = RequestLocal.withThreadConfinedCaching

    // Allowing WriteTxnMarkers API with the help of allowedAclOperation parameter.
    val authorizer: Authorizer = mock(classOf[Authorizer])
    val clusterResource = new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL)
    val allowedAction = util.List.of(new Action(
      AclOperation.fromString(allowedAclOperation),
      clusterResource,
      1,
      true,
      allowedAclOperation.equals("CLUSTER_ACTION")
    ))
    val deniedList = util.List.of(AuthorizationResult.DENIED)
    val allowedList = util.List.of(AuthorizationResult.ALLOWED)
    when(authorizer.authorize(
      ArgumentMatchers.eq(request.context),
      any()
    )).thenReturn(deniedList)
    when(authorizer.authorize(
      request.context,
      allowedAction
    )).thenReturn(allowedList)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))

    kafkaApis.handleWriteTxnMarkersRequest(request, requestLocal)
    verify(replicaManager).appendRecords(anyLong,
      anyShort,
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      any(),
      any(),
      any(),
      ArgumentMatchers.eq(requestLocal),
      any(),
      any())
  }

  @Test
  def testHandleWriteTxnMarkersRequest(): Unit = {
    val offset0 = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)
    val offset1 = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 1)
    val foo0 = new TopicPartition("foo", 0)
    val foo1 = new TopicPartition("foo", 1)

    val topicIds = Map(
      Topic.GROUP_METADATA_TOPIC_NAME -> Uuid.fromString("JaTH2JYK2ed2GzUapg8tgg"),
      "foo" -> Uuid.fromString("d2Gg8tgzJa2JYK2eTHUapg"))
    val allPartitions = List(
      offset0,
      offset1,
      foo0,
      foo1
    )

    val writeTxnMarkersRequest = new WriteTxnMarkersRequest.Builder(
      util.List.of(
        new TxnMarkerEntry(
          1L,
          1.toShort,
          0,
          TransactionResult.COMMIT,
          util.List.of(offset0, foo0),
          TransactionVersion.TV_2.featureLevel()
        ),
        new TxnMarkerEntry(
          2L,
          1.toShort,
          0,
          TransactionResult.ABORT,
          util.List.of(offset1, foo1),
          TransactionVersion.TV_2.featureLevel()
        )
      )
    ).build()

    val requestChannelRequest = buildRequest(writeTxnMarkersRequest)

    allPartitions.foreach { tp =>
      when(replicaManager.onlinePartition(tp)).thenReturn(Some(mock(classOf[Partition])))
      when(replicaManager.topicIdPartition(tp)).thenReturn(new TopicIdPartition(topicIds.get(tp.topic()).getOrElse(Uuid.ZERO_UUID), tp))
    }

    when(groupCoordinator.completeTransaction(
      ArgumentMatchers.eq(offset0),
      ArgumentMatchers.eq(1L),
      ArgumentMatchers.eq(1.toShort),
      ArgumentMatchers.eq(0),
      ArgumentMatchers.eq(TransactionResult.COMMIT),
      ArgumentMatchers.eq(TransactionVersion.TV_2.featureLevel())
    )).thenReturn(CompletableFuture.completedFuture[Void](null))

    when(groupCoordinator.completeTransaction(
      ArgumentMatchers.eq(offset1),
      ArgumentMatchers.eq(2L),
      ArgumentMatchers.eq(1.toShort),
      ArgumentMatchers.eq(0),
      ArgumentMatchers.eq(TransactionResult.ABORT),
      ArgumentMatchers.eq(TransactionVersion.TV_2.featureLevel())
    )).thenReturn(CompletableFuture.completedFuture[Void](null))

    val entriesPerPartition: ArgumentCaptor[Map[TopicIdPartition, MemoryRecords]] =
      ArgumentCaptor.forClass(classOf[Map[TopicIdPartition, MemoryRecords]])
    val responseCallback: ArgumentCaptor[Map[TopicIdPartition, PartitionResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Map[TopicIdPartition, PartitionResponse] => Unit])

    when(replicaManager.appendRecords(
      ArgumentMatchers.eq(ServerConfigs.REQUEST_TIMEOUT_MS_DEFAULT.toLong),
      ArgumentMatchers.eq(-1),
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      entriesPerPartition.capture(),
      responseCallback.capture(),
      any(),
      ArgumentMatchers.eq(RequestLocal.noCaching),
      any(),
      any()
    )).thenAnswer { _ =>
      responseCallback.getValue.apply(
        entriesPerPartition.getValue.keySet.map { tp =>
          tp -> new PartitionResponse(Errors.NONE)
        }.toMap
      )
    }
    kafkaApis = createKafkaApis()
    kafkaApis.handleWriteTxnMarkersRequest(requestChannelRequest, RequestLocal.noCaching)

    val expectedResponse = new WriteTxnMarkersResponseData()
      .setMarkers(util.List.of(
        new WriteTxnMarkersResponseData.WritableTxnMarkerResult()
          .setProducerId(1L)
          .setTopics(util.List.of(
            new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
              .setName(Topic.GROUP_METADATA_TOPIC_NAME)
              .setPartitions(util.List.of(
                new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                  .setPartitionIndex(0)
                  .setErrorCode(Errors.NONE.code)
              )),
            new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
              .setName("foo")
              .setPartitions(util.List.of(
                new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                  .setPartitionIndex(0)
                  .setErrorCode(Errors.NONE.code)
              ))
          )),
        new WriteTxnMarkersResponseData.WritableTxnMarkerResult()
          .setProducerId(2L)
          .setTopics(util.List.of(
            new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
              .setName(Topic.GROUP_METADATA_TOPIC_NAME)
              .setPartitions(util.List.of(
                new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                  .setPartitionIndex(1)
                  .setErrorCode(Errors.NONE.code)
              )),
            new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
              .setName("foo")
              .setPartitions(util.List.of(
                new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                  .setPartitionIndex(1)
                  .setErrorCode(Errors.NONE.code)
              ))
          ))
      ))

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
  def testHandleWriteTxnMarkersRequestErrorTranslation(error: Errors): Unit = {
    val offset0 = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)

    val writeTxnMarkersRequest = new WriteTxnMarkersRequest.Builder(
      util.List.of(
        new TxnMarkerEntry(
          1L,
          1.toShort,
          0,
          TransactionResult.COMMIT,
          util.List.of(offset0),
          TransactionVersion.TV_2.featureLevel()
        )
      )
    ).build()

    val requestChannelRequest = buildRequest(writeTxnMarkersRequest)

    when(replicaManager.onlinePartition(offset0))
      .thenReturn(Some(mock(classOf[Partition])))

    when(groupCoordinator.completeTransaction(
      ArgumentMatchers.eq(offset0),
      ArgumentMatchers.eq(1L),
      ArgumentMatchers.eq(1.toShort),
      ArgumentMatchers.eq(0),
      ArgumentMatchers.eq(TransactionResult.COMMIT),
      ArgumentMatchers.eq(TransactionVersion.TV_2.featureLevel())
    )).thenReturn(FutureUtils.failedFuture[Void](error.exception()))
    kafkaApis = createKafkaApis()
    kafkaApis.handleWriteTxnMarkersRequest(requestChannelRequest, RequestLocal.noCaching)

    val expectedError = error match {
      case Errors.COORDINATOR_NOT_AVAILABLE | Errors.COORDINATOR_LOAD_IN_PROGRESS | Errors.NOT_COORDINATOR =>
        Errors.NOT_LEADER_OR_FOLLOWER
      case error =>
        error
    }

    val expectedResponse = new WriteTxnMarkersResponseData()
      .setMarkers(util.List.of(
        new WriteTxnMarkersResponseData.WritableTxnMarkerResult()
          .setProducerId(1L)
          .setTopics(util.List.of(
            new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
              .setName(Topic.GROUP_METADATA_TOPIC_NAME)
              .setPartitions(util.List.of(
                new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                  .setPartitionIndex(0)
                  .setErrorCode(expectedError.code)
              ))
          ))
      ))

    val response = verifyNoThrottling[WriteTxnMarkersResponse](requestChannelRequest)
    assertEquals(normalize(expectedResponse), normalize(response.data))
  }

  @ParameterizedTest(name = "testWriteTxnMarkersEpochValidationError with transactionVersion={0}")
  @ValueSource(shorts = Array(1, 2))
  def testWriteTxnMarkersEpochValidationError(transactionVersion: Short): Unit = {
    // Test that epoch validation errors (INVALID_PRODUCER_EPOCH) are properly
    // propagated from ReplicaManager (mocked) through KafkaApis and returned
    // in WriteTxnMarkersResponse.
    val topicPartition = new TopicPartition("test-topic", 0)
    val topicId = Uuid.randomUuid()
    val producerId = 1L
    val currentEpoch = 5.toShort
    // For TV2, same epoch is ALSO invalid; for TV1, old epoch is invalid
    val oldEpoch = if (transactionVersion >= 2) currentEpoch else (currentEpoch - 1).toShort
    
    val writeTxnMarkersRequest = new WriteTxnMarkersRequest.Builder(
      util.List.of(
        new TxnMarkerEntry(
          producerId,
          oldEpoch,
          0,
          TransactionResult.COMMIT,
          util.List.of(topicPartition),
          transactionVersion
        )
      )
    ).build()
    
    val requestChannelRequest = buildRequest(writeTxnMarkersRequest)
    
    // Set up partition and log
    val partition = mock(classOf[Partition])
    when(replicaManager.onlinePartition(topicPartition))
      .thenReturn(Some(partition))
    when(replicaManager.topicIdPartition(topicPartition))
      .thenReturn(new TopicIdPartition(topicId, topicPartition))
    
    // Set up appendRecords to simulate epoch validation failure
    val entriesPerPartition: ArgumentCaptor[Map[TopicIdPartition, MemoryRecords]] =
      ArgumentCaptor.forClass(classOf[Map[TopicIdPartition, MemoryRecords]])
    val responseCallback: ArgumentCaptor[Map[TopicIdPartition, PartitionResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Map[TopicIdPartition, PartitionResponse] => Unit])
    
    when(replicaManager.appendRecords(
      ArgumentMatchers.eq(ServerConfigs.REQUEST_TIMEOUT_MS_DEFAULT.toLong),
      ArgumentMatchers.eq(-1),
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      entriesPerPartition.capture(),
      responseCallback.capture(),
      any(),
      ArgumentMatchers.eq(RequestLocal.noCaching),
      any(),
      ArgumentMatchers.eq(transactionVersion)
    )).thenAnswer { _ =>
      // Simulate epoch validation failure by calling callback with INVALID_PRODUCER_EPOCH error
      val topicIdPartition = new TopicIdPartition(topicId, topicPartition)
      responseCallback.getValue.apply(
        Map(topicIdPartition -> new PartitionResponse(Errors.INVALID_PRODUCER_EPOCH))
      )
    }
    
    kafkaApis = createKafkaApis()
    kafkaApis.handleWriteTxnMarkersRequest(requestChannelRequest, RequestLocal.noCaching)
    
    // Verify the response contains INVALID_PRODUCER_EPOCH error
    val expectedResponse = new WriteTxnMarkersResponseData()
      .setMarkers(util.List.of(
        new WriteTxnMarkersResponseData.WritableTxnMarkerResult()
          .setProducerId(producerId)
          .setTopics(util.List.of(
            new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
              .setName(topicPartition.topic())
              .setPartitions(util.List.of(
                new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                  .setPartitionIndex(topicPartition.partition())
                  .setErrorCode(Errors.INVALID_PRODUCER_EPOCH.code)
              ))
          ))
      ))
    
    val response = verifyNoThrottling[WriteTxnMarkersResponse](requestChannelRequest)
    assertEquals(normalize(expectedResponse), normalize(response.data))
    
    // Verify appendRecords was called with the correct transactionVersion
    verify(replicaManager).appendRecords(
      anyLong,
      anyShort,
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      any(),
      any(),
      any(),
      ArgumentMatchers.eq(RequestLocal.noCaching),
      any(),
      ArgumentMatchers.eq(transactionVersion)
    )
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
    val deleteGroupsRequest = new DeleteGroupsRequestData().setGroupsNames(util.List.of(
      "group-1",
      "group-2",
      "group-3"
    ))

    val requestChannelRequest = buildRequest(new DeleteGroupsRequest.Builder(deleteGroupsRequest).build())

    val future = new CompletableFuture[DeleteGroupsResponseData.DeletableGroupResultCollection]()
    when(groupCoordinator.deleteGroups(
      requestChannelRequest.context,
      util.List.of("group-1", "group-2", "group-3"),
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleDeleteGroupsRequest(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    val results = new DeleteGroupsResponseData.DeletableGroupResultCollection(util.List.of(
      new DeleteGroupsResponseData.DeletableGroupResult()
        .setGroupId("group-1")
        .setErrorCode(Errors.NONE.code),
      new DeleteGroupsResponseData.DeletableGroupResult()
        .setGroupId("group-2")
        .setErrorCode(Errors.NOT_CONTROLLER.code),
      new DeleteGroupsResponseData.DeletableGroupResult()
        .setGroupId("group-3")
        .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code),
    ).iterator)

    future.complete(results)

    val expectedDeleteGroupsResponse = new DeleteGroupsResponseData()
      .setResults(results)

    val response = verifyNoThrottling[DeleteGroupsResponse](requestChannelRequest)
    assertEquals(expectedDeleteGroupsResponse, response.data)
  }

  @Test
  def testHandleDeleteGroupsFutureFailed(): Unit = {
    val deleteGroupsRequest = new DeleteGroupsRequestData().setGroupsNames(util.List.of(
      "group-1",
      "group-2",
      "group-3"
    ))

    val requestChannelRequest = buildRequest(new DeleteGroupsRequest.Builder(deleteGroupsRequest).build())

    val future = new CompletableFuture[DeleteGroupsResponseData.DeletableGroupResultCollection]()
    when(groupCoordinator.deleteGroups(
      requestChannelRequest.context,
      util.List.of("group-1", "group-2", "group-3"),
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleDeleteGroupsRequest(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    future.completeExceptionally(Errors.NOT_CONTROLLER.exception)

    val expectedDeleteGroupsResponse = new DeleteGroupsResponseData()
      .setResults(new DeleteGroupsResponseData.DeletableGroupResultCollection(util.List.of(
        new DeleteGroupsResponseData.DeletableGroupResult()
          .setGroupId("group-1")
          .setErrorCode(Errors.NOT_CONTROLLER.code),
        new DeleteGroupsResponseData.DeletableGroupResult()
          .setGroupId("group-2")
          .setErrorCode(Errors.NOT_CONTROLLER.code),
        new DeleteGroupsResponseData.DeletableGroupResult()
          .setGroupId("group-3")
          .setErrorCode(Errors.NOT_CONTROLLER.code),
      ).iterator))

    val response = verifyNoThrottling[DeleteGroupsResponse](requestChannelRequest)
    assertEquals(expectedDeleteGroupsResponse, response.data)
  }

  @Test
  def testHandleDeleteGroupsAuthenticationFailed(): Unit = {
    val deleteGroupsRequest = new DeleteGroupsRequestData().setGroupsNames(util.List.of(
      "group-1",
      "group-2",
      "group-3"
    ))

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
      actions.stream()
        .map(action => acls.getOrElse(action.resourcePattern.name, AuthorizationResult.DENIED))
        .toList
    }

    val future = new CompletableFuture[DeleteGroupsResponseData.DeletableGroupResultCollection]()
    when(groupCoordinator.deleteGroups(
      requestChannelRequest.context,
      util.List.of("group-2", "group-3"),
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleDeleteGroupsRequest(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    future.complete(new DeleteGroupsResponseData.DeletableGroupResultCollection(util.List.of(
      new DeleteGroupsResponseData.DeletableGroupResult()
        .setGroupId("group-2")
        .setErrorCode(Errors.NONE.code),
      new DeleteGroupsResponseData.DeletableGroupResult()
        .setGroupId("group-3")
        .setErrorCode(Errors.NONE.code)
    ).iterator))

    val expectedDeleteGroupsResponse = new DeleteGroupsResponseData()
      .setResults(new DeleteGroupsResponseData.DeletableGroupResultCollection(util.List.of(
        new DeleteGroupsResponseData.DeletableGroupResult()
          .setGroupId("group-2")
          .setErrorCode(Errors.NONE.code),
        new DeleteGroupsResponseData.DeletableGroupResult()
          .setGroupId("group-3")
          .setErrorCode(Errors.NONE.code),
        new DeleteGroupsResponseData.DeletableGroupResult()
          .setGroupId("group-1")
          .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code)).iterator))

    val response = verifyNoThrottling[DeleteGroupsResponse](requestChannelRequest)
    assertEquals(expectedDeleteGroupsResponse, response.data)
  }

  @Test
  def testHandleDescribeGroups(): Unit = {
    val describeGroupsRequest = new DescribeGroupsRequestData().setGroups(util.List.of(
      "group-1",
      "group-2",
      "group-3",
      "group-4"
    ))

    val requestChannelRequest = buildRequest(new DescribeGroupsRequest.Builder(describeGroupsRequest).build())

    val future = new CompletableFuture[util.List[DescribeGroupsResponseData.DescribedGroup]]()
    when(groupCoordinator.describeGroups(
      requestChannelRequest.context,
      describeGroupsRequest.groups
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleDescribeGroupsRequest(requestChannelRequest)

    val groupResults = util.List.of(
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-1")
        .setProtocolType("consumer")
        .setProtocolData("range")
        .setGroupState("Stable")
        .setMembers(util.List.of(
          new DescribeGroupsResponseData.DescribedGroupMember()
            .setMemberId("member-1"))),
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-2")
        .setErrorCode(Errors.NOT_COORDINATOR.code),
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-3")
        .setErrorCode(Errors.REQUEST_TIMED_OUT.code),
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-4")
        .setGroupState("Dead")
        .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code)
        .setErrorMessage("Group group-4 is not a classic group.")
    )

    future.complete(groupResults)

    val expectedDescribeGroupsResponse = new DescribeGroupsResponseData().setGroups(groupResults)
    val response = verifyNoThrottling[DescribeGroupsResponse](requestChannelRequest)
    assertEquals(expectedDescribeGroupsResponse, response.data)
  }

  @Test
  def testHandleDescribeGroupsFutureFailed(): Unit = {
    val describeGroupsRequest = new DescribeGroupsRequestData().setGroups(util.List.of(
      "group-1",
      "group-2",
      "group-3"
    ))

    val requestChannelRequest = buildRequest(new DescribeGroupsRequest.Builder(describeGroupsRequest).build())

    val future = new CompletableFuture[util.List[DescribeGroupsResponseData.DescribedGroup]]()
    when(groupCoordinator.describeGroups(
      requestChannelRequest.context,
      describeGroupsRequest.groups
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleDescribeGroupsRequest(requestChannelRequest)

    val expectedDescribeGroupsResponse = new DescribeGroupsResponseData().setGroups(util.List.of(
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-1")
        .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code),
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-2")
        .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code),
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-3")
        .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code)
    ))

    future.completeExceptionally(Errors.UNKNOWN_SERVER_ERROR.exception)

    val response = verifyNoThrottling[DescribeGroupsResponse](requestChannelRequest)
    assertEquals(expectedDescribeGroupsResponse, response.data)
  }

  @Test
  def testHandleDescribeGroupsAuthenticationFailed(): Unit = {
    val describeGroupsRequest = new DescribeGroupsRequestData().setGroups(util.List.of(
      "group-1",
      "group-2",
      "group-3"
    ))

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
      actions.stream().
        map(action => acls.getOrElse(action.resourcePattern.name, AuthorizationResult.DENIED))
        .toList
    }

    val future = new CompletableFuture[util.List[DescribeGroupsResponseData.DescribedGroup]]()
    when(groupCoordinator.describeGroups(
      requestChannelRequest.context,
      util.List.of("group-2")
    )).thenReturn(future)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleDescribeGroupsRequest(requestChannelRequest)

    future.complete(util.List.of(
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-2")
        .setErrorCode(Errors.NOT_COORDINATOR.code)
    ))

    val expectedDescribeGroupsResponse = new DescribeGroupsResponseData().setGroups(util.List.of(
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
    ))

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
      .setPartitions(util.List.of(
        new OffsetDeleteRequestPartition().setPartitionIndex(0),
        new OffsetDeleteRequestPartition().setPartitionIndex(1))))
    topics.add(new OffsetDeleteRequestTopic()
      .setName("topic-2")
      .setPartitions(util.List.of(
        new OffsetDeleteRequestPartition().setPartitionIndex(0),
        new OffsetDeleteRequestPartition().setPartitionIndex(1))))

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
      .setTopics(new OffsetDeleteResponseData.OffsetDeleteResponseTopicCollection(util.List.of(
        new OffsetDeleteResponseData.OffsetDeleteResponseTopic()
          .setName("topic-1")
          .setPartitions(new OffsetDeleteResponseData.OffsetDeleteResponsePartitionCollection(util.List.of(
            new OffsetDeleteResponseData.OffsetDeleteResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetDeleteResponseData.OffsetDeleteResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)
          ).iterator)),
        new OffsetDeleteResponseData.OffsetDeleteResponseTopic()
          .setName("topic-2")
          .setPartitions(new OffsetDeleteResponseData.OffsetDeleteResponsePartitionCollection(util.List.of(
            new OffsetDeleteResponseData.OffsetDeleteResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetDeleteResponseData.OffsetDeleteResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)
          ).iterator))
      ).iterator()))

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
      .setTopics(new OffsetDeleteRequestTopicCollection(util.List.of(
        // foo exists but has only 2 partitions.
        new OffsetDeleteRequestTopic()
          .setName("foo")
          .setPartitions(util.List.of(
            new OffsetDeleteRequestPartition().setPartitionIndex(0),
            new OffsetDeleteRequestPartition().setPartitionIndex(1),
            new OffsetDeleteRequestPartition().setPartitionIndex(2)
          )),
        // bar exists.
        new OffsetDeleteRequestTopic()
          .setName("bar")
          .setPartitions(util.List.of(
            new OffsetDeleteRequestPartition().setPartitionIndex(0),
            new OffsetDeleteRequestPartition().setPartitionIndex(1)
          )),
        // zar does not exist.
        new OffsetDeleteRequestTopic()
          .setName("zar")
          .setPartitions(util.List.of(
            new OffsetDeleteRequestPartition().setPartitionIndex(0),
            new OffsetDeleteRequestPartition().setPartitionIndex(1)
          )),
      ).iterator))

    val requestChannelRequest = buildRequest(new OffsetDeleteRequest.Builder(offsetDeleteRequest).build())

    // This is the request expected by the group coordinator. It contains
    // only existing topic-partitions.
    val expectedOffsetDeleteRequest = new OffsetDeleteRequestData()
      .setGroupId(group)
      .setTopics(new OffsetDeleteRequestTopicCollection(util.List.of(
        new OffsetDeleteRequestTopic()
          .setName("foo")
          .setPartitions(util.List.of(
            new OffsetDeleteRequestPartition().setPartitionIndex(0),
            new OffsetDeleteRequestPartition().setPartitionIndex(1)
          )),
        new OffsetDeleteRequestTopic()
          .setName("bar")
          .setPartitions(util.List.of(
            new OffsetDeleteRequestPartition().setPartitionIndex(0),
            new OffsetDeleteRequestPartition().setPartitionIndex(1)
          ))
      ).iterator))

    val future = new CompletableFuture[OffsetDeleteResponseData]()
    when(groupCoordinator.deleteOffsets(
      requestChannelRequest.context,
      expectedOffsetDeleteRequest,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    // This is the response returned by the group coordinator.
    val offsetDeleteResponse = new OffsetDeleteResponseData()
      .setTopics(new OffsetDeleteResponseTopicCollection(util.List.of(
        new OffsetDeleteResponseTopic()
          .setName("foo")
          .setPartitions(new OffsetDeleteResponsePartitionCollection(util.List.of(
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)
          ).iterator)),
        new OffsetDeleteResponseTopic()
          .setName("bar")
          .setPartitions(new OffsetDeleteResponsePartitionCollection(util.List.of(
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)
          ).iterator)),
      ).iterator))

    val expectedOffsetDeleteResponse = new OffsetDeleteResponseData()
      .setTopics(new OffsetDeleteResponseTopicCollection(util.List.of(
        new OffsetDeleteResponseTopic()
          .setName("foo")
          .setPartitions(new OffsetDeleteResponsePartitionCollection(util.List.of(
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
          ).iterator)),
        // zar is before bar because topics failing the validation are
        // put in the response first.
        new OffsetDeleteResponseTopic()
          .setName("zar")
          .setPartitions(new OffsetDeleteResponsePartitionCollection(util.List.of(
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code),
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
          ).iterator)),
        new OffsetDeleteResponseTopic()
          .setName("bar")
          .setPartitions(new OffsetDeleteResponsePartitionCollection(util.List.of(
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)
          ).iterator)),
      ).iterator))

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
        .setPartitions(util.List.of(
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
        RequestLocal.noCaching.bufferSupplier
      )).thenReturn(CompletableFuture.completedFuture(
        new OffsetDeleteResponseData()
      ))
      val kafkaApis = createKafkaApis()
      try {
        kafkaApis.handleOffsetDeleteRequest(request, RequestLocal.noCaching)

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
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleOffsetDeleteRequest(request, RequestLocal.noCaching)

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
        .setTopics(new OffsetDeleteRequestTopicCollection(util.List.of(new OffsetDeleteRequestTopic()
          .setName("topic-unknown")
          .setPartitions(util.List.of(new OffsetDeleteRequestPartition()
            .setPartitionIndex(0)
          ))
        ).iterator()))
    ).build()
    val request = buildRequest(offsetDeleteRequest)

    val future = new CompletableFuture[OffsetDeleteResponseData]()
    when(groupCoordinator.deleteOffsets(
      request.context,
      new OffsetDeleteRequestData().setGroupId(group), // Nonexistent topics won't be passed to groupCoordinator.
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleOffsetDeleteRequest(request, RequestLocal.noCaching)

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

    when(replicaManager.fetchOffset(
      ArgumentMatchers.any[Seq[ListOffsetsTopic]](),
      ArgumentMatchers.eq(Set.empty[TopicPartition]),
      ArgumentMatchers.eq(isolationLevel),
      ArgumentMatchers.eq(ListOffsetsRequest.CONSUMER_REPLICA_ID),
      ArgumentMatchers.eq[String](clientId),
      ArgumentMatchers.anyInt(), // correlationId
      ArgumentMatchers.anyShort(), // version
      ArgumentMatchers.any[(Errors, ListOffsetsPartition) => ListOffsetsPartitionResponse](),
      ArgumentMatchers.any[Consumer[util.Collection[ListOffsetsTopicResponse]]],
      ArgumentMatchers.anyInt() // timeoutMs
    )).thenAnswer(ans => {
      val callback = ans.getArgument[Consumer[util.List[ListOffsetsTopicResponse]]](8)
      val partitionResponse = new ListOffsetsPartitionResponse()
        .setErrorCode(error.code())
        .setOffset(ListOffsetsResponse.UNKNOWN_OFFSET)
        .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP)
        .setPartitionIndex(tp.partition())
      callback.accept(util.List.of(new ListOffsetsTopicResponse().setName(tp.topic()).setPartitions(util.List.of(partitionResponse))))
    })

    val targetTimes = util.List.of(new ListOffsetsTopic()
      .setName(tp.topic)
      .setPartitions(util.List.of(new ListOffsetsPartition()
        .setPartitionIndex(tp.partition)
        .setTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP)
        .setCurrentLeaderEpoch(currentLeaderEpoch.get))))
    val listOffsetRequest = ListOffsetsRequest.Builder.forConsumer(true, isolationLevel)
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

  @Test
  def testListOffsetMaxTimestampWithUnsupportedVersion(): Unit = {
    testConsumerListOffsetWithUnsupportedVersion(ListOffsetsRequest.MAX_TIMESTAMP, 6)
  }

  @Test
  def testListOffsetEarliestLocalTimestampWithUnsupportedVersion(): Unit = {
    testConsumerListOffsetWithUnsupportedVersion(ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP, 7)
  }

  @Test
  def testListOffsetLatestTieredTimestampWithUnsupportedVersion(): Unit = {
    testConsumerListOffsetWithUnsupportedVersion(ListOffsetsRequest.LATEST_TIERED_TIMESTAMP, 8)
  }

  @Test
  def testListOffsetNegativeTimestampWithOneOrAboveVersion(): Unit = {
    testConsumerListOffsetWithUnsupportedVersion(-6, 1)
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
    metadataCache = mock(classOf[KRaftMetadataCache])
    when(metadataCache.getAliveBrokerNodes(any())).thenReturn(util.List.of(new Node(brokerId,"localhost", 0)))
    when(metadataCache.getRandomAliveBrokerId).thenReturn(util.Optional.empty())

    // 2 topics returned for authorization in during handle
    val topicsReturnedFromMetadataCacheForAuthorization = util.Set.of("remaining-topic", "later-deleted-topic")
    when(metadataCache.getAllTopics).thenReturn(topicsReturnedFromMetadataCacheForAuthorization)
    // 1 topic is deleted from metadata right at the time between authorization and the next getTopicMetadata() call
    when(metadataCache.getTopicMetadata(
      ArgumentMatchers.eq(topicsReturnedFromMetadataCacheForAuthorization),
      any[ListenerName],
      anyBoolean,
      anyBoolean
    )).thenReturn(util.List.of(
      new MetadataResponseTopic()
        .setErrorCode(Errors.NONE.code)
        .setName("remaining-topic")
        .setIsInternal(false)
    ))

    val response = sendMetadataRequestWithInconsistentListeners(new ListenerName("PLAINTEXT"))
    val responseTopics = response.topicMetadata().asScala.map { metadata => metadata.topic() }

    // verify we don't create topic when getAllTopicMetadata
    verify(autoTopicCreationManager, never).createTopics(any(), any(), any())
    assertEquals(List("remaining-topic"), responseTopics)
    assertTrue(response.topicsByError(Errors.UNKNOWN_TOPIC_OR_PARTITION).isEmpty)
  }

  @Test
  def testUnauthorizedTopicMetadataRequest(): Unit = {
    // 1. Set up broker information
    val plaintextListener = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val endpoints = new BrokerEndpointCollection()
    endpoints.add(
      new BrokerEndpoint()
        .setHost("broker0")
        .setPort(9092)
        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
        .setName(plaintextListener.value)
    )
    MetadataCacheTest.updateCache(metadataCache,
      Seq(new RegisterBrokerRecord().setBrokerId(0).setRack("rack").setFenced(false).setEndPoints(endpoints))
    )

    // 2. Set up authorizer
    val authorizer: Authorizer = mock(classOf[Authorizer])
    val unauthorizedTopic = "unauthorized-topic"
    val authorizedTopic = "authorized-topic"

    val expectedActions = util.List.of(
      new Action(AclOperation.DESCRIBE, new ResourcePattern(ResourceType.TOPIC, unauthorizedTopic, PatternType.LITERAL), 1, true, true),
      new Action(AclOperation.DESCRIBE, new ResourcePattern(ResourceType.TOPIC, authorizedTopic, PatternType.LITERAL), 1, true, true)
    )

    when(authorizer.authorize(any[RequestContext], argThat((t: java.util.List[Action]) => t.containsAll(expectedActions))))
      .thenAnswer { invocation =>
        val actions = invocation.getArgument(1, classOf[util.List[Action]])
        val results = new util.ArrayList[AuthorizationResult]()
        actions.forEach { a =>
          results.add(if (a.resourcePattern.name == authorizedTopic) AuthorizationResult.ALLOWED else AuthorizationResult.DENIED)
        }
        results
      }

    // 3. Set up MetadataCache
    val authorizedTopicId = Uuid.randomUuid()
    val unauthorizedTopicId = Uuid.randomUuid()
    addTopicToMetadataCache(authorizedTopic, 1, topicId = authorizedTopicId)
    addTopicToMetadataCache(unauthorizedTopic, 1, topicId = unauthorizedTopicId)

    def createDummyPartitionRecord(topicId: Uuid) = {
      new PartitionRecord()
        .setTopicId(topicId)
        .setPartitionId(0)
        .setLeader(0)
        .setLeaderEpoch(0)
        .setReplicas(util.List.of(0))
        .setIsr(util.List.of(0))
    }

    val partitionRecords = Seq(authorizedTopicId, unauthorizedTopicId).map(createDummyPartitionRecord)
    MetadataCacheTest.updateCache(metadataCache, partitionRecords)

    // 4. Send TopicMetadataReq using topicId
    val metadataReqByTopicId = MetadataRequest.Builder.forTopicIds(util.Set.of(authorizedTopicId, unauthorizedTopicId)).build()
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
    val metadataReqByTopicName = new MetadataRequest.Builder(util.List.of(authorizedTopic, unauthorizedTopic), false).build()
    val repByTopicName = buildRequest(metadataReqByTopicName, plaintextListener)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleTopicMetadataRequest(repByTopicName)
    val metadataByTopicNameResp = verifyNoThrottling[MetadataResponse](repByTopicName)

    val metadataByTopicName = metadataByTopicNameResp.data().topics().asScala.groupBy(_.name()).map(kv => (kv._1, kv._2.head))

    metadataByTopicName.foreach { case (topicName, metadataResponseTopic) =>
      if (topicName == unauthorizedTopic) {
        assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code(), metadataResponseTopic.errorCode())
        // Do not return topicId on unauthorized error
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
      val records = MemoryRecords.withRecords(Compression.NONE,
        new SimpleRecord(timestamp, "foo".getBytes(StandardCharsets.UTF_8)))
      callback(Seq(tidp -> new FetchPartitionData(Errors.NONE, hw, 0, records,
        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)))
    })

    val fetchData = util.Map.of(tidp, new FetchRequest.PartitionData(Uuid.ZERO_UUID, 0, 0, 1000,
      Optional.empty()))
    val fetchDataBuilder = util.Map.of(tp, new FetchRequest.PartitionData(Uuid.ZERO_UUID, 0, 0, 1000,
      Optional.empty()))
    val fetchMetadata = new JFetchMetadata(0, 0)
    val fetchContext = new FullFetchContext(time, new FetchSessionCacheShard(1000, 100),
      fetchMetadata, fetchData, false, false)
    when(fetchManager.newContext(
      any[Short],
      any[JFetchMetadata],
      any[Boolean],
      any[util.Map[TopicIdPartition, FetchRequest.PartitionData]],
      any[util.List[TopicIdPartition]],
      any[util.Map[Uuid, String]])).thenReturn(fetchContext)

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[Session](), anyString, anyDouble, anyLong)).thenReturn(0)

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
    val fetchData = util.Map.of(new TopicIdPartition(foo.topicId, new TopicPartition(null, foo.partition)),
      new FetchRequest.PartitionData(foo.topicId, 0, 0, 1000, Optional.empty()))
    val fetchDataBuilder = util.Map.of(foo.topicPartition, new FetchRequest.PartitionData(foo.topicId, 0, 0, 1000,
      Optional.empty()))
    val fetchMetadata = new JFetchMetadata(0, 0)
    val fetchContext = new FullFetchContext(time, new FetchSessionCacheShard(1000, 100),
      fetchMetadata, fetchData, true, replicaId >= 0)
    // We expect to have the resolved partition, but we will simulate an unknown one with the fetchContext we return.
    when(fetchManager.newContext(
      ApiKeys.FETCH.latestVersion,
      fetchMetadata,
      replicaId >= 0,
      util.Map.of(foo, new FetchRequest.PartitionData(foo.topicId, 0, 0, 1000, Optional.empty())),
      util.List.of[TopicIdPartition],
      metadataCache.topicIdsToNames())
    ).thenReturn(fetchContext)

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[Session](), anyString, anyDouble, anyLong)).thenReturn(0)

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
      util.Map.of(),
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
      callback(Seq(tidp -> new FetchPartitionData(Errors.NOT_LEADER_OR_FOLLOWER, UnifiedLog.UNKNOWN_OFFSET, UnifiedLog.UNKNOWN_OFFSET, MemoryRecords.EMPTY,
        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)))
    })

    val fetchData = util.Map.of(tidp, new FetchRequest.PartitionData(topicId, 0, 0, 1000,
      Optional.empty()))
    val fetchDataBuilder = util.Map.of(tp, new FetchRequest.PartitionData(topicId, 0, 0, 1000,
      Optional.empty()))
    val fetchMetadata = new JFetchMetadata(0, 0)
    val fetchContext = new FullFetchContext(time, new FetchSessionCacheShard(1000, 100),
      fetchMetadata, fetchData, true, false)
    when(fetchManager.newContext(
      any[Short],
      any[JFetchMetadata],
      any[Boolean],
      any[util.Map[TopicIdPartition, FetchRequest.PartitionData]],
      any[util.List[TopicIdPartition]],
      any[util.Map[Uuid, String]])).thenReturn(fetchContext)

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[Session](), anyString, anyDouble, anyLong)).thenReturn(0)

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
    val node = response.data.nodeEndpoints.asScala
    assertEquals(Seq(2), node.map(_.nodeId))
    assertEquals(Seq("broker2"), node.map(_.host))
  }

  @Test
  def testHandleShareFetchRequestSuccessWithoutAcknowledgements(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: String = Uuid.randomUuid().toString

    val shareSessionEpoch = 0

    val records = memoryRecords(10, 0)

    when(sharePartitionManager.fetchMessages(any(), any(), any(), any(), anyInt(), anyInt(), anyInt(), any())).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)),
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setAcknowledgeErrorCode(Errors.NONE.code)
            .setRecords(records)
            .setAcquiredRecords(new util.ArrayList(util.List.of(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            )))
      ))
    )

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any(), any(), any())).thenReturn(
      new ShareSessionContext(shareSessionEpoch, util.List.of(
        new TopicIdPartition(topicId, partitionIndex, topicName)))
    )

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[Session](), anyString, anyDouble, anyLong)).thenReturn(0)

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId("group").
      setMemberId(memberId).
      setShareSessionEpoch(shareSessionEpoch).
      setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(partitionIndex)).iterator))).iterator))

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleShareFetchRequest(request)
    val response = verifyNoThrottling[ShareFetchResponse](request)
    val responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    val topicResponse = topicResponses.stream.findFirst.get
    assertEquals(topicId, topicResponse.topicId)
    assertEquals(1, topicResponse.partitions.size())
    assertEquals(partitionIndex, topicResponse.partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponse.partitions.get(0).errorCode)
    assertEquals(records, topicResponse.partitions.get(0).records)
    assertArrayEquals(expectedAcquiredRecords(0, 9, 1).toArray(), topicResponse.partitions.get(0).acquiredRecords.toArray())
  }

  @Test
  def testHandleShareFetchRequestPropagatesAcknowledgementErrorMessage(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.randomUuid()
    val groupId = "group"
    val shareSessionEpoch = 1
    val records = memoryRecords(5, 0)
    val ackErrorMessage = "custom acknowledgement error"

    when(sharePartitionManager.fetchMessages(any(), any(), any(), any(), anyInt(), anyInt(), anyInt(), any())).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)),
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setAcknowledgeErrorCode(Errors.NONE.code)
            .setRecords(records)
            .setAcquiredRecords(new util.ArrayList(util.List.of(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(4)
                .setDeliveryCount(1)
            )))
      ))
    )

    when(sharePartitionManager.acknowledge(any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)),
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(partitionIndex)
            .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
            .setErrorMessage(ackErrorMessage)
      ))
    )

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any(), any(), any())).thenReturn(
      new ShareSessionContext(shareSessionEpoch, util.List.of(
        new TopicIdPartition(topicId, partitionIndex, topicName)))
    )

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[Session](), anyString, anyDouble, anyLong)).thenReturn(0)

    val shareFetchRequestData = new ShareFetchRequestData()
      .setGroupId(groupId)
      .setMemberId(memberId.toString)
      .setShareSessionEpoch(shareSessionEpoch)
      .setMaxWaitMs(100)
      .setMinBytes(1)
      .setMaxBytes(1000000)
      .setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(new ShareFetchRequestData.FetchTopic()
        .setTopicId(topicId)
        .setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(partitionIndex)
            .setAcknowledgementBatches(util.List.of(
              new ShareFetchRequestData.AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(4)
                .setAcknowledgeTypes(util.List.of(1.toByte))
            ))
        ).iterator))
      ).iterator))

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleShareFetchRequest(request)
    val response = verifyNoThrottling[ShareFetchResponse](request)

    val topicResponses = response.data.responses()
    val topicResponse = topicResponses.stream.findFirst.get
    val partitionResponse = topicResponse.partitions.get(0)
    assertEquals(Errors.NONE.code, partitionResponse.errorCode)
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code, partitionResponse.acknowledgeErrorCode)
    assertEquals(ackErrorMessage, partitionResponse.acknowledgeErrorMessage)
  }

  @Test
  def testHandleShareFetchRequestInvalidRequestOnInitialEpoch(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: String = Uuid.randomUuid().toString

    val groupId = "group"
    val partitionIndex = 0

    val records = memoryRecords(10, 0)

    when(sharePartitionManager.fetchMessages(any(), any(), any(), any(), anyInt(), anyInt(), anyInt(), any())).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)),
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setAcknowledgeErrorCode(Errors.NONE.code)
            .setRecords(records)
            .setAcquiredRecords(new util.ArrayList(util.List.of(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            )))
      ))
    )

    val cachedSharePartitions = new ImplicitLinkedHashCollection[CachedSharePartition]
    cachedSharePartitions.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId, partitionIndex, topicName), false))

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[Session](), anyString, anyDouble, anyLong)).thenReturn(0)

    var shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId).
      setShareSessionEpoch(0).
      setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(partitionIndex)
            .setAcknowledgementBatches(util.List.of(
            new AcknowledgementBatch()
              .setFirstOffset(0)
              .setLastOffset(9)
              .setAcknowledgeTypes(util.List.of(1.toByte))
          ))
        ).iterator))
      ).iterator))

    var shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    var request = buildRequest(shareFetchRequest)

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any(), any(), any())).thenThrow(
      Errors.INVALID_REQUEST.exception()
    ).thenReturn(new ShareSessionContext(1, new ShareSession(
      new ShareSessionKey(groupId, memberId), cachedSharePartitions, 2, request.context.connectionId)
    ))

    kafkaApis = createKafkaApis()
    kafkaApis.handleShareFetchRequest(request)
    var response = verifyNoThrottling[ShareFetchResponse](request)
    var responseData = response.data()

    assertEquals(Errors.INVALID_REQUEST.code, responseData.errorCode)

    // Testing whether the subsequent request with the incremented share session epoch works or not.
    shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId).
      setShareSessionEpoch(1).
      setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)
        ).iterator))
      ).iterator))

    shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    request = buildRequest(shareFetchRequest)
    kafkaApis.handleShareFetchRequest(request)
    response = verifyNoThrottling[ShareFetchResponse](request)
    responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    val topicResponse = topicResponses.stream.findFirst.get
    assertEquals(topicId, topicResponse.topicId)
    assertEquals(1, topicResponse.partitions.size())
    assertEquals(partitionIndex, topicResponse.partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponse.partitions.get(0).errorCode)
    assertEquals(records, topicResponse.partitions.get(0).records)
    assertArrayEquals(expectedAcquiredRecords(0, 9, 1).toArray(), topicResponse.partitions.get(0).acquiredRecords.toArray())
  }

  @Test
  def testHandleShareFetchRequestInvalidRequestOnFinalEpoch(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: String = Uuid.randomUuid().toString

    val groupId = "group"

    val records = memoryRecords(10, 0)

    when(sharePartitionManager.fetchMessages(any(), any(), any(), any(), anyInt(), anyInt(), anyInt(), any())).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)),
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setAcknowledgeErrorCode(Errors.NONE.code)
            .setRecords(records)
            .setAcquiredRecords(new util.ArrayList(util.List.of(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            )))
      ))
    )

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any(), any(), any())).thenReturn(
      new ShareSessionContext(0, util.List.of(
        new TopicIdPartition(topicId, partitionIndex, topicName)
      ))
    ).thenThrow(Errors.INVALID_REQUEST.exception)

    when(sharePartitionManager.releaseSession(any(), any())).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)),
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(partitionIndex)
            .setErrorCode(Errors.NONE.code)
      ))
    )

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[Session](), anyString, anyDouble, anyLong)).thenReturn(0)

    var shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId).
      setShareSessionEpoch(0).
      setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)).iterator))).iterator))

    var shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    var request = buildRequest(shareFetchRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleShareFetchRequest(request)
    var response = verifyNoThrottling[ShareFetchResponse](request)
    var responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    val topicResponse = topicResponses.stream.findFirst.get
    assertEquals(topicId, topicResponse.topicId)
    assertEquals(1, topicResponse.partitions.size())
    assertEquals(partitionIndex, topicResponse.partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponse.partitions.get(0).errorCode)
    assertEquals(records, topicResponse.partitions.get(0).records)
    assertArrayEquals(expectedAcquiredRecords(0, 9, 1).toArray(), topicResponse.partitions.get(0).acquiredRecords.toArray())

    shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId).
      setShareSessionEpoch(-1).
      setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)
            .setAcknowledgementBatches(util.List.of(
              new AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(util.List.of(1.toByte))
            ))
        ).iterator))
      ).iterator))

    shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    request = buildRequest(shareFetchRequest)
    kafkaApis.handleShareFetchRequest(request)
    response = verifyNoThrottling[ShareFetchResponse](request)
    responseData = response.data()

    assertEquals(Errors.INVALID_REQUEST.code, responseData.errorCode)
  }

  @Test
  def testHandleShareFetchRequestFetchThrowsException(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: String = Uuid.randomUuid().toString

    when(sharePartitionManager.fetchMessages(any(), any(), any(), any(), anyInt(), anyInt(), anyInt(), any())).thenReturn(
      FutureUtils.failedFuture[util.Map[TopicIdPartition, ShareFetchResponseData.PartitionData]](Errors.UNKNOWN_SERVER_ERROR.exception())
    )

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any(), any(), any())).thenReturn(
      new ShareSessionContext(0, util.List.of(
        new TopicIdPartition(topicId, partitionIndex, topicName)
      ))
    )

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[Session](), anyString, anyDouble, anyLong)).thenReturn(0)

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId("group").
      setMemberId(memberId).
      setShareSessionEpoch(0).
      setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)).iterator))).iterator))

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleShareFetchRequest(request)
    val response = verifyNoThrottling[ShareFetchResponse](request)
    val responseData = response.data()

    assertEquals(Errors.UNKNOWN_SERVER_ERROR.code, responseData.errorCode)
  }

  @Test
  def testHandleShareFetchRequestAcknowledgeThrowsException(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: String = Uuid.randomUuid().toString

    val groupId = "group"

    val records = memoryRecords(10, 0)

    when(sharePartitionManager.fetchMessages(any(), any(), any(), any(), anyInt(), anyInt(), anyInt(), any())).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)),
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setRecords(records)
            .setAcquiredRecords(new util.ArrayList(util.List.of(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            )))
      ))
    )

    when(sharePartitionManager.acknowledge(any(), any(), any())).thenReturn(
      FutureUtils.failedFuture[util.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]](Errors.UNKNOWN_SERVER_ERROR.exception())
    )

    val cachedSharePartitions = new ImplicitLinkedHashCollection[CachedSharePartition]
    cachedSharePartitions.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId, partitionIndex, topicName), false))

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[Session](), anyString, anyDouble, anyLong)).thenReturn(0)

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId).
      setShareSessionEpoch(1).
      setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)
            .setAcknowledgementBatches(util.List.of(
              new AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(util.List.of(1.toByte))
            ))
        ).iterator))
      ).iterator))

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any(), any(), any()))
      .thenReturn(new ShareSessionContext(1, new ShareSession(
        new ShareSessionKey(groupId, memberId), cachedSharePartitions, 2, request.context.connectionId))
      )

    kafkaApis = createKafkaApis()
    kafkaApis.handleShareFetchRequest(request)
    val response = verifyNoThrottling[ShareFetchResponse](request)
    val responseData = response.data()

    assertEquals(Errors.UNKNOWN_SERVER_ERROR.code, responseData.errorCode)
  }

  @Test
  def testHandleShareFetchRequestFetchAndAcknowledgeThrowsException(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: String = Uuid.randomUuid().toString

    val groupId = "group"

    when(sharePartitionManager.fetchMessages(any(), any(), any(), any(), anyInt(), anyInt(), anyInt(), any())).thenReturn(
      FutureUtils.failedFuture[util.Map[TopicIdPartition, ShareFetchResponseData.PartitionData]](Errors.UNKNOWN_SERVER_ERROR.exception())
    )

    when(sharePartitionManager.acknowledge(any(), any(), any())).thenReturn(
      FutureUtils.failedFuture[util.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]](Errors.UNKNOWN_SERVER_ERROR.exception())
    )

    val cachedSharePartitions = new ImplicitLinkedHashCollection[CachedSharePartition]
    cachedSharePartitions.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId, partitionIndex, topicName), false))

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[Session](), anyString, anyDouble, anyLong)).thenReturn(0)

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId).
      setShareSessionEpoch(1).
      setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)
            .setAcknowledgementBatches(util.List.of(
              new AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(util.List.of(1.toByte))
            ))
        ).iterator))
      ).iterator))

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any(), any(), any()))
      .thenReturn(new ShareSessionContext(1, new ShareSession(
        new ShareSessionKey(groupId, memberId), cachedSharePartitions, 2, request.context.connectionId))
      )

    kafkaApis = createKafkaApis()
    kafkaApis.handleShareFetchRequest(request)
    val response = verifyNoThrottling[ShareFetchResponse](request)
    val responseData = response.data()

    assertEquals(Errors.UNKNOWN_SERVER_ERROR.code, responseData.errorCode)
  }

  @Test
  def testHandleShareFetchRequestErrorInReadingPartition(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: String = Uuid.randomUuid().toString

    val records = MemoryRecords.EMPTY

    when(sharePartitionManager.fetchMessages(any(), any(), any(), any(), anyInt(), anyInt(), anyInt(), any())).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)),
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.REPLICA_NOT_AVAILABLE.code)
            .setRecords(records)
            .setAcquiredRecords(new util.ArrayList(util.List.of))
      ))
    )

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any(), any(), any())).thenReturn(
      new ShareSessionContext(0, util.List.of(
        new TopicIdPartition(topicId, partitionIndex, topicName)
      ))
    )

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[Session](), anyString, anyDouble, anyLong)).thenReturn(0)

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId("group").
      setMemberId(memberId).
      setShareSessionEpoch(0).
      setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)).iterator))).iterator))

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleShareFetchRequest(request)
    val response = verifyNoThrottling[ShareFetchResponse](request)
    val responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    val topicResponse = topicResponses.stream.findFirst.get
    assertEquals(topicId, topicResponse.topicId)
    assertEquals(1, topicResponse.partitions.size())
    assertEquals(partitionIndex, topicResponse.partitions.get(0).partitionIndex)
    assertEquals(Errors.REPLICA_NOT_AVAILABLE.code, topicResponse.partitions.get(0).errorCode)
    assertEquals(records, topicResponse.partitions.get(0).records)
    assertTrue(topicResponse.partitions.get(0).acquiredRecords.toArray().isEmpty)
  }

  @Test
  def testHandleShareFetchRequestShareSessionNotFoundError(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: String = Uuid.randomUuid().toString

    val groupId = "group"
    val records = memoryRecords(10, 0)

    when(sharePartitionManager.fetchMessages(any(), any(), any(), any(), anyInt(), anyInt(), anyInt(), any())).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, partitionIndex, topicName),
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setRecords(records)
            .setAcquiredRecords(new util.ArrayList(util.List.of(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            )))
      ))
    )

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any(), any(), any())).thenReturn(
      new ShareSessionContext(0, util.List.of(
        new TopicIdPartition(topicId, partitionIndex, topicName)
      ))
    ).thenThrow(Errors.SHARE_SESSION_NOT_FOUND.exception)

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[Session](), anyString, anyDouble, anyLong)).thenReturn(0)

    var shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId).
      setShareSessionEpoch(0).
      setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)).iterator))).iterator))

    var shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    var request = buildRequest(shareFetchRequest)
    // First share fetch request is to establish the share session with the broker.
    kafkaApis = createKafkaApis()
    kafkaApis.handleShareFetchRequest(request)
    var response = verifyNoThrottling[ShareFetchResponse](request)
    var responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    val topicResponse = topicResponses.stream.findFirst.get
    assertEquals(topicId, topicResponse.topicId)
    assertEquals(1, topicResponse.partitions.size())
    assertEquals(partitionIndex, topicResponse.partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponse.partitions.get(0).errorCode)
    assertEquals(records, topicResponse.partitions.get(0).records)
    assertArrayEquals(expectedAcquiredRecords(0, 9, 1).toArray(), topicResponse.partitions.get(0).acquiredRecords.toArray())

    val memberId2 = Uuid.randomUuid()

    // Using wrong member ID.
    shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId2.toString).
      setShareSessionEpoch(1).
      setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)).iterator))).iterator))

    shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    request = buildRequest(shareFetchRequest)
    kafkaApis.handleShareFetchRequest(request)
    response = verifyNoThrottling[ShareFetchResponse](request)
    responseData = response.data()

    assertEquals(Errors.SHARE_SESSION_NOT_FOUND.code, responseData.errorCode)
  }

  @Test
  def testHandleShareFetchRequestInvalidShareSessionError(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: String = Uuid.randomUuid().toString

    val groupId = "group"
    val records = memoryRecords(10, 0)

    when(sharePartitionManager.fetchMessages(any(), any(), any(), any(), anyInt(), anyInt(), anyInt(), any())).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)),
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setRecords(records)
            .setAcquiredRecords(new util.ArrayList(util.List.of(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            )))
      ))
    )

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any(), any(), any())).thenReturn(
      new ShareSessionContext(0, util.List.of(
        new TopicIdPartition(topicId, partitionIndex, topicName)
      ))
    ).thenThrow(Errors.INVALID_SHARE_SESSION_EPOCH.exception)

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[Session](), anyString, anyDouble, anyLong)).thenReturn(0)

    var shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId).
      setShareSessionEpoch(0).
      setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)).iterator))).iterator))

    var shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    var request = buildRequest(shareFetchRequest)
    // First share fetch request is to establish the share session with the broker.
    kafkaApis = createKafkaApis()
    kafkaApis.handleShareFetchRequest(request)
    var response = verifyNoThrottling[ShareFetchResponse](request)
    var responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    val topicResponse = topicResponses.stream.findFirst.get
    assertEquals(topicId, topicResponse.topicId)
    assertEquals(1, topicResponse.partitions.size())
    assertEquals(partitionIndex, topicResponse.partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponse.partitions.get(0).errorCode)
    assertEquals(records, topicResponse.partitions.get(0).records)
    assertArrayEquals(expectedAcquiredRecords(0, 9, 1).toArray(), topicResponse.partitions.get(0).acquiredRecords.toArray())

    shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId).
      setShareSessionEpoch(2). // Invalid share session epoch, should have 1 for the second request.
      setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)).iterator))).iterator))

    shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    request = buildRequest(shareFetchRequest)
    kafkaApis.handleShareFetchRequest(request)
    response = verifyNoThrottling[ShareFetchResponse](request)
    responseData = response.data()

    assertEquals(Errors.INVALID_SHARE_SESSION_EPOCH.code, responseData.errorCode)
  }

  @Test
  def testHandleShareFetchRequestWhenShareSessionCacheIsFull(): Unit = {
    val topicId = Uuid.randomUuid()
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache("foo", 1, topicId = topicId)

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any(), any(), any()))
      .thenThrow(Errors.SHARE_SESSION_LIMIT_REACHED.exception)

    when(sharePartitionManager.createIdleShareFetchTimerTask(anyLong()))
      .thenReturn(CompletableFuture.completedFuture(null))

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId("group").
      setMemberId(Uuid.randomUuid.toString).
      setShareSessionEpoch(0).
      setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)).iterator))).iterator))

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleShareFetchRequest(request)
    val response = verifyNoThrottling[ShareFetchResponse](request)
    val responseData = response.data()

    assertEquals(Errors.SHARE_SESSION_LIMIT_REACHED.code, responseData.errorCode)
  }

  @Test
  def testHandleShareFetchRequestShareSessionSuccessfullyEstablished(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: String = Uuid.randomUuid().toString

    val groupId = "group"

    val records1 = memoryRecords(10, 0)
    val records2 = memoryRecords(10, 10)
    val records3 = memoryRecords(10, 20)

    when(sharePartitionManager.fetchMessages(any(), any(), any(), any(), anyInt(), anyInt(), anyInt(), any())).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)),
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setRecords(records1)
            .setAcquiredRecords(new util.ArrayList(util.List.of(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            )))
      ))
    ).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)),
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setAcknowledgeErrorCode(Errors.NONE.code)
            .setRecords(records2)
            .setAcquiredRecords(new util.ArrayList(util.List.of(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(10)
                .setLastOffset(19)
                .setDeliveryCount(1)
            )))
      ))
    ).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)),
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setAcknowledgeErrorCode(Errors.NONE.code)
            .setRecords(records3)
            .setAcquiredRecords(new util.ArrayList(util.List.of(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(20)
                .setLastOffset(29)
                .setDeliveryCount(1)
            )))
      ))
    )

    when(sharePartitionManager.acknowledge(any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)),
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(partitionIndex)
            .setErrorCode(Errors.NONE.code)
      ))
    ).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)),
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(partitionIndex)
            .setErrorCode(Errors.NONE.code)
      ))
    )

    val cachedSharePartitions = new ImplicitLinkedHashCollection[CachedSharePartition]
    cachedSharePartitions.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId, partitionIndex, topicName), false)
    )

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[Session](), anyString, anyDouble, anyLong)).thenReturn(0)

    var shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId).
      setShareSessionEpoch(0).
      setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(partitionIndex)).iterator))).iterator))

    var shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    var request = buildRequest(shareFetchRequest)

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any(), any(), any())).thenReturn(
      new ShareSessionContext(0, util.List.of(
        new TopicIdPartition(topicId, partitionIndex, topicName)
      ))
    ).thenReturn(new ShareSessionContext(1, new ShareSession(
      new ShareSessionKey(groupId, memberId), cachedSharePartitions, 2, request.context.connectionId))
    ).thenReturn(new ShareSessionContext(2, new ShareSession(
      new ShareSessionKey(groupId, memberId), cachedSharePartitions, 3, request.context.connectionId))
    )

    // First share fetch request is to establish the share session with the broker.
    kafkaApis = createKafkaApis()
    kafkaApis.handleShareFetchRequest(request)
    var response = verifyNoThrottling[ShareFetchResponse](request)
    var responseData = response.data()
    var topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    var topicResponse = topicResponses.stream.findFirst.get
    assertEquals(topicId, topicResponse.topicId)
    assertEquals(1, topicResponse.partitions.size())

    compareResponsePartitions(
      partitionIndex,
      Errors.NONE.code,
      Errors.NONE.code,
      records1,
      expectedAcquiredRecords(0, 9, 1),
      topicResponse.partitions.get(0)
    )

    shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId).
      setShareSessionEpoch(1).
      setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
          new ShareFetchRequestData.FetchPartition().
            setPartitionIndex(partitionIndex).
            setAcknowledgementBatches(util.List.of(
              new ShareFetchRequestData.AcknowledgementBatch().
                setFirstOffset(0).
                setLastOffset(9).
                setAcknowledgeTypes(util.List.of[java.lang.Byte](1.toByte))))).iterator))).iterator))

    shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    request = buildRequest(shareFetchRequest)

    kafkaApis.handleShareFetchRequest(request)
    response = verifyNoThrottling[ShareFetchResponse](request)
    responseData = response.data()
    topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    topicResponse = topicResponses.stream.findFirst.get
    assertEquals(topicId, topicResponse.topicId)
    assertEquals(1, topicResponse.partitions.size())

    compareResponsePartitions(
      partitionIndex,
      Errors.NONE.code,
      Errors.NONE.code,
      records2,
      expectedAcquiredRecords(10, 19, 1),
      topicResponse.partitions.get(0)
    )

    shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId).
      setShareSessionEpoch(2).
      setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
          new ShareFetchRequestData.FetchPartition().
            setPartitionIndex(partitionIndex).
            setAcknowledgementBatches(util.List.of(
              new ShareFetchRequestData.AcknowledgementBatch().
                setFirstOffset(10).
                setLastOffset(19).
                setAcknowledgeTypes(util.List.of[java.lang.Byte](1.toByte))))).iterator))).iterator))

    shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    request = buildRequest(shareFetchRequest)

    kafkaApis.handleShareFetchRequest(request)
    response = verifyNoThrottling[ShareFetchResponse](request)
    responseData = response.data()
    topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    topicResponse = topicResponses.stream.findFirst.get
    assertEquals(topicId, topicResponse.topicId)
    assertEquals(1, topicResponse.partitions.size())

    compareResponsePartitions(
      partitionIndex,
      Errors.NONE.code,
      Errors.NONE.code,
      records3,
      expectedAcquiredRecords(20, 29, 1),
      topicResponse.partitions.get(0)
    )
  }

  @Test
  def testHandleShareFetchRequestSuccessfulShareSessionLifecycle(): Unit = {
    val topicName1 = "foo1"
    val topicId1 = Uuid.randomUuid()

    val topicName2 = "foo2"
    val topicId2 = Uuid.randomUuid()

    val topicName3 = "foo3"
    val topicId3 = Uuid.randomUuid()

    val topicName4 = "foo4"
    val topicId4 = Uuid.randomUuid()

    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName1, 2, topicId = topicId1)
    addTopicToMetadataCache(topicName2, 2, topicId = topicId2)
    addTopicToMetadataCache(topicName3, 1, topicId = topicId3)
    addTopicToMetadataCache(topicName4, 1, topicId = topicId4)
    val memberId: String = Uuid.randomUuid().toString

    val records_t1_p1_1 = memoryRecords(10, 0)
    val records_t1_p2_1 = memoryRecords(10, 10)

    val records_t2_p1_1 = memoryRecords(10, 43)
    val records_t2_p2_1 = memoryRecords(10, 17)

    val records_t3_p1_1 = memoryRecords(20, 54)
    val records_t3_p1_2 = memoryRecords(20, 74)

    val records_t4_p1_1 = memoryRecords(15, 10)

    val groupId = "group"

    when(sharePartitionManager.fetchMessages(any(), any(), any(), any(), anyInt(), anyInt(), anyInt(), any())).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId1, new TopicPartition(topicName1, 0)),
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setRecords(records_t1_p1_1)
            .setAcquiredRecords(new util.ArrayList(util.List.of(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            ))),
        new TopicIdPartition(topicId1, new TopicPartition(topicName1, 1)),
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setRecords(records_t1_p2_1)
            .setAcquiredRecords(new util.ArrayList(util.List.of(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(10)
                .setLastOffset(19)
                .setDeliveryCount(1)
            ))),
        new TopicIdPartition(topicId2, new TopicPartition(topicName2, 0)),
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setRecords(records_t2_p1_1)
            .setAcquiredRecords(new util.ArrayList(util.List.of(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(43)
                .setLastOffset(52)
                .setDeliveryCount(1)
            ))),
        new TopicIdPartition(topicId2, new TopicPartition(topicName2, 1)),
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setRecords(records_t2_p2_1)
            .setAcquiredRecords(new util.ArrayList(util.List.of(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(17)
                .setLastOffset(26)
                .setDeliveryCount(1)
            )))
      ))
    ).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId3, new TopicPartition(topicName3, 0)),
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setRecords(records_t3_p1_1)
            .setAcquiredRecords(new util.ArrayList(util.List.of(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(54)
                .setLastOffset(73)
                .setDeliveryCount(1)
            )))
      ))
    ).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId3, new TopicPartition(topicName3, 0)),
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setRecords(records_t3_p1_2)
            .setAcquiredRecords(new util.ArrayList(util.List.of(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(74)
                .setLastOffset(93)
                .setDeliveryCount(1)
            ))),
        new TopicIdPartition(topicId4, new TopicPartition(topicName4, 0)),
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setRecords(records_t4_p1_1)
            .setAcquiredRecords(new util.ArrayList(util.List.of(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(10)
                .setLastOffset(24)
                .setDeliveryCount(1)
            )))
      ))
    )

    val cachedSharePartitions1 = new ImplicitLinkedHashCollection[CachedSharePartition]
    cachedSharePartitions1.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId1, 0, topicName1), false
    ))
    cachedSharePartitions1.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId1, 1, topicName1), false
    ))
    cachedSharePartitions1.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId2, 0, topicName2), false
    ))
    cachedSharePartitions1.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId2, 1, topicName2), false
    ))
    cachedSharePartitions1.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId3, 0, topicName3), false
    ))

    val cachedSharePartitions2 = new ImplicitLinkedHashCollection[CachedSharePartition]
    cachedSharePartitions2.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId3, 0, topicName3), false
    ))
    cachedSharePartitions2.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId4, 0, topicName4), false
    ))

    when(sharePartitionManager.releaseSession(any(), any())).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId3, new TopicPartition(topicName3, 0)),
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
        new TopicIdPartition(topicId4, new TopicPartition(topicName4, 0)),
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code)
      ))
    )

    when(sharePartitionManager.acknowledge(any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId1, new TopicPartition(topicName1, 0)),
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
        new TopicIdPartition(topicId1, new TopicPartition(topicName1, 1)),
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(1)
            .setErrorCode(Errors.NONE.code),
        new TopicIdPartition(topicId2, new TopicPartition(topicName2, 0)),
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
        new TopicIdPartition(topicId2, new TopicPartition(topicName2, 1)),
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(1)
            .setErrorCode(Errors.NONE.code),
        new TopicIdPartition(topicId3, new TopicPartition(topicName3, 0)),
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
        new TopicIdPartition(topicId4, new TopicPartition(topicName4, 0)),
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
      ))
    )

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[Session](), anyString, anyDouble, anyLong)).thenReturn(0)

    var shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId).
      setShareSessionEpoch(0).
      setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId1).
          setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0),
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(1)
          ).iterator)),
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId2).
          setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0),
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(1)
          ).iterator))
      ).iterator))

    var shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    var request = buildRequest(shareFetchRequest)

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any(), any(), any())).thenReturn(
      new ShareSessionContext(0, util.List.of(
        new TopicIdPartition(topicId1, new TopicPartition(topicName1, 0)),
        new TopicIdPartition(topicId1, new TopicPartition(topicName1, 1)),
        new TopicIdPartition(topicId2, new TopicPartition(topicName2, 0)),
        new TopicIdPartition(topicId2, new TopicPartition(topicName2, 1))
      ))
    ).thenReturn(new ShareSessionContext(1, new ShareSession(
      new ShareSessionKey(groupId, memberId), cachedSharePartitions1, 2, request.context.connectionId))
    ).thenReturn(new ShareSessionContext(2, new ShareSession(
      new ShareSessionKey(groupId, memberId), cachedSharePartitions2, 3, request.context.connectionId))
    ).thenReturn(new FinalContext())

    // First share fetch request is to establish the share session with the broker.
    kafkaApis = createKafkaApis()
    kafkaApis.handleShareFetchRequest(request)
    var response = verifyNoThrottling[ShareFetchResponse](request)
    var responseData = response.data()
    var topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(2, topicResponses.size())
    var topicResponsesScala = topicResponses.asScala.toList
    var topicResponsesMap: Map[Uuid, ShareFetchableTopicResponse] = topicResponsesScala.map(topic => topic.topicId -> topic).toMap
    assertTrue(topicResponsesMap.contains(topicId1))
    val topicIdResponse1: ShareFetchableTopicResponse = topicResponsesMap.getOrElse(topicId1, null)
    assertEquals(2, topicIdResponse1.partitions.size())
    val partitionsScala1 = topicIdResponse1.partitions.asScala.toList
    val partitionsMap1: Map[Int, PartitionData] = partitionsScala1.map(partition => partition.partitionIndex -> partition).toMap
    assertTrue(partitionsMap1.contains(0))
    val partition11: PartitionData = partitionsMap1.getOrElse(0, null)

    compareResponsePartitions(
      0,
      Errors.NONE.code,
      Errors.NONE.code,
      records_t1_p1_1,
      expectedAcquiredRecords(0, 9, 1),
      partition11
    )

    assertTrue(partitionsMap1.contains(1))
    val partition12: PartitionData = partitionsMap1.getOrElse(1, null)

    compareResponsePartitions(
      1,
      Errors.NONE.code,
      Errors.NONE.code,
      records_t1_p2_1,
      expectedAcquiredRecords(10, 19, 1),
      partition12
    )

    assertTrue(topicResponsesMap.contains(topicId2))
    val topicIdResponse2: ShareFetchableTopicResponse = topicResponsesMap.getOrElse(topicId2, null)
    assertEquals(2, topicIdResponse2.partitions.size())
    val partitionsScala2 = topicIdResponse2.partitions.asScala.toList
    val partitionsMap2: Map[Int, PartitionData] = partitionsScala2.map(partition => partition.partitionIndex -> partition).toMap
    assertTrue(partitionsMap2.contains(0))
    val partition21: PartitionData = partitionsMap2.getOrElse(0, null)

    compareResponsePartitions(
      0,
      Errors.NONE.code,
      Errors.NONE.code,
      records_t2_p1_1,
      expectedAcquiredRecords(43, 52, 1),
      partition21
    )

    assertTrue(partitionsMap2.contains(1))
    val partition22: PartitionData = partitionsMap2.getOrElse(1, null)

    compareResponsePartitions(
      1,
      Errors.NONE.code,
      Errors.NONE.code,
      records_t2_p2_1,
      expectedAcquiredRecords(17, 26, 1),
      partition22
    )

    shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId).
      setShareSessionEpoch(1).
      setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId3).
          setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
          ).iterator)),
      ).iterator))

    shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    request = buildRequest(shareFetchRequest)
    kafkaApis.handleShareFetchRequest(request)
    response = verifyNoThrottling[ShareFetchResponse](request)
    responseData = response.data()
    topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    val topicResponse = topicResponses.stream.findFirst.get
    assertEquals(topicId3, topicResponse.topicId)
    assertEquals(1, topicResponse.partitions.size())

    compareResponsePartitions(
      0,
      Errors.NONE.code,
      Errors.NONE.code,
      records_t3_p1_1,
      expectedAcquiredRecords(54, 73, 1),
      topicResponse.partitions.get(0)
    )

    shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId).
      setShareSessionEpoch(2).
      setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId4).
          setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
          ).iterator)),
      ).iterator))
      .setForgottenTopicsData(util.List.of(
        new ForgottenTopic()
          .setTopicId(topicId1)
          .setPartitions(util.List.of(Integer.valueOf(0), Integer.valueOf(1))),
        new ForgottenTopic()
          .setTopicId(topicId2)
          .setPartitions(util.List.of(Integer.valueOf(0), Integer.valueOf(1)))
      ))

    shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    request = buildRequest(shareFetchRequest)
    kafkaApis.handleShareFetchRequest(request)
    response = verifyNoThrottling[ShareFetchResponse](request)
    responseData = response.data()
    topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(2, topicResponses.size())
    topicResponsesScala = topicResponses.asScala.toList
    topicResponsesMap = topicResponsesScala.map(topic => topic.topicId -> topic).toMap
    assertTrue(topicResponsesMap.contains(topicId3))
    val topicIdResponse3 = topicResponsesMap.getOrElse(topicId3, null)
    assertEquals(1, topicIdResponse3.partitions.size())

    compareResponsePartitions(
      0,
      Errors.NONE.code,
      Errors.NONE.code,
      records_t3_p1_2,
      expectedAcquiredRecords(74, 93, 1),
      topicIdResponse3.partitions.get(0)
    )

    assertTrue(topicResponsesMap.contains(topicId4))
    val topicIdResponse4 = topicResponsesMap.getOrElse(topicId4, null)
    assertEquals(1, topicIdResponse4.partitions.size())

    compareResponsePartitions(
      0,
      Errors.NONE.code,
      Errors.NONE.code,
      records_t4_p1_1,
      expectedAcquiredRecords(10, 24, 1),
      topicIdResponse4.partitions.get(0)
    )

    // Final request with acknowledgements.
    shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId).
      setShareSessionEpoch(-1).
      setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId1).
          setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
              .setAcknowledgementBatches(util.List.of(
                new AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(util.List.of(1.toByte)),
              )),
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(1)
              .setAcknowledgementBatches(util.List.of(
                new AcknowledgementBatch()
                  .setFirstOffset(10)
                  .setLastOffset(19)
                  .setAcknowledgeTypes(util.List.of(1.toByte)),
              ))
          ).iterator)),
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId2).
          setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
              .setAcknowledgementBatches(util.List.of(
                new AcknowledgementBatch()
                  .setFirstOffset(43)
                  .setLastOffset(52)
                  .setAcknowledgeTypes(util.List.of(1.toByte)),
              )),
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(1)
              .setAcknowledgementBatches(util.List.of(
                new AcknowledgementBatch()
                  .setFirstOffset(17)
                  .setLastOffset(26)
                  .setAcknowledgeTypes(util.List.of(1.toByte)),
              ))
          ).iterator)),
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId3).
          setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
              .setAcknowledgementBatches(util.List.of(
                new AcknowledgementBatch()
                  .setFirstOffset(54)
                  .setLastOffset(93)
                  .setAcknowledgeTypes(util.List.of(1.toByte)),
              )),
          ).iterator)),
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId4).
          setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
              .setAcknowledgementBatches(util.List.of(
                new AcknowledgementBatch()
                  .setFirstOffset(10)
                  .setLastOffset(24)
                  .setAcknowledgeTypes(util.List.of(1.toByte)),
              )),
          ).iterator)),
      ).iterator))

    shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    request = buildRequest(shareFetchRequest)
    kafkaApis.handleShareFetchRequest(request)
    response = verifyNoThrottling[ShareFetchResponse](request)
    responseData = response.data()
    topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
  }

  @Test
  def testHandleFetchFromShareFetchRequestSuccess(): Unit = {
    val shareSessionEpoch = 0
    val topicName1 = "foo1"
    val topicName2 = "foo2"
    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()

    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName1, 1, topicId = topicId1)
    addTopicToMetadataCache(topicName2, 2, topicId = topicId2)

    val memberId: Uuid = Uuid.ZERO_UUID
    val groupId: String = "group"

    val records_t1_p1 = memoryRecords(10, 0)
    val records_t2_p1 = memoryRecords(15, 0)
    val records_t2_p2 = memoryRecords(20, 0)

    val tp1 = new TopicIdPartition(topicId1, new TopicPartition(topicName1, 0))
    val tp2 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 0))
    val tp3 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 1))

    when(sharePartitionManager.fetchMessages(any(), any(), any(), any(), anyInt(), anyInt(), anyInt(), any())).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareFetchResponseData.PartitionData](
        tp1,
          new ShareFetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code)
            .setRecords(records_t1_p1)
            .setAcquiredRecords(new util.ArrayList(util.List.of(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            ))),
        tp2,
          new ShareFetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code)
            .setRecords(records_t2_p1)
            .setAcquiredRecords(new util.ArrayList(util.List.of(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(14)
                .setDeliveryCount(1)
            ))),
        tp3,
          new ShareFetchResponseData.PartitionData()
            .setPartitionIndex(1)
            .setErrorCode(Errors.NONE.code)
            .setRecords(records_t2_p2)
            .setAcquiredRecords(new util.ArrayList(util.List.of(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(19)
                .setDeliveryCount(1)
            ))),
      ))
    )

    val erroneousPartitions: util.Map[TopicIdPartition, ShareFetchResponseData.PartitionData] = new util.HashMap()

    val validPartitions: util.List[TopicIdPartition] = util.List.of(tp1, tp2, tp3)

    val erroneousAndValidPartitionData: ErroneousAndValidPartitionData =
      new ErroneousAndValidPartitionData(erroneousPartitions, validPartitions)

    var authorizedTopics: Set[String] = Set.empty[String]
    authorizedTopics = authorizedTopics + topicName1
    authorizedTopics = authorizedTopics + topicName2

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(shareSessionEpoch).
      setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId1).
          setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
          ).iterator)),
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId2).
          setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0),
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(1)
          ).iterator)),
      ).iterator))

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)
    // First share fetch request is to establish the share session with the broker.
    kafkaApis = createKafkaApis()
    val fetchResult: Map[TopicIdPartition, ShareFetchResponseData.PartitionData] =
      kafkaApis.handleFetchFromShareFetchRequest(
        request,
        0,
        erroneousAndValidPartitionData,
        sharePartitionManager,
        authorizedTopics
      ).get()

    assertEquals(3, fetchResult.size)

    assertTrue(fetchResult.contains(tp1))
    val partitionData1: PartitionData = fetchResult.getOrElse(tp1, null)
    assertNotNull(partitionData1)

    compareResponsePartitions(
      0,
      Errors.NONE.code,
      Errors.NONE.code,
      records_t1_p1,
      expectedAcquiredRecords(0, 9, 1),
      partitionData1
    )

    assertTrue(fetchResult.contains(tp2))
    val partitionData2: PartitionData = fetchResult.getOrElse(tp2, null)
    assertNotNull(partitionData2)

    compareResponsePartitions(
      0,
      Errors.NONE.code,
      Errors.NONE.code,
      records_t2_p1,
      expectedAcquiredRecords(0, 14, 1),
      partitionData2
    )

    assertTrue(fetchResult.contains(tp3))
    val partitionData3: PartitionData = fetchResult.getOrElse(tp3, null)
    assertNotNull(partitionData3)

    compareResponsePartitions(
      1,
      Errors.NONE.code,
      Errors.NONE.code,
      records_t2_p2,
      expectedAcquiredRecords(0, 19, 1),
      partitionData3
    )
  }

  @Test
  def testHandleShareFetchFromShareFetchRequestWithErroneousPartitions(): Unit = {
    val shareSessionEpoch = 0
    val topicName1 = "foo1"
    val topicName2 = "foo2"
    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()

    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName1, 1, topicId = topicId1)
    addTopicToMetadataCache(topicName2, 2, topicId = topicId2)

    val memberId: Uuid = Uuid.ZERO_UUID
    val groupId: String = "group"

    val records_t1_p1 = memoryRecords(10, 0)

    val tp1 = new TopicIdPartition(topicId1, new TopicPartition(topicName1, 0))
    val tp2 = new TopicIdPartition(topicId1, new TopicPartition(topicName1, 1))
    val tp3 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 0))

    when(sharePartitionManager.fetchMessages(any(), any(), any(), any(), anyInt(), anyInt(), anyInt(), any())).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareFetchResponseData.PartitionData](
        tp1,
          new ShareFetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code)
            .setRecords(records_t1_p1)
            .setAcquiredRecords(new util.ArrayList(util.List.of(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            )))
      ))
    )

    val erroneousPartitions = util.Map.of[TopicIdPartition, ShareFetchResponseData.PartitionData](
      tp2,
      new ShareFetchResponseData.PartitionData()
        .setPartitionIndex(1)
        .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code),
      tp3,
      new ShareFetchResponseData.PartitionData()
        .setPartitionIndex(0)
        .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
    )

    val validPartitions: util.List[TopicIdPartition] = util.List.of(tp1)

    val erroneousAndValidPartitionData: ErroneousAndValidPartitionData =
      new ErroneousAndValidPartitionData(erroneousPartitions, validPartitions)

    var authorizedTopics: Set[String] = Set.empty[String]
    authorizedTopics = authorizedTopics + topicName1
    authorizedTopics = authorizedTopics + topicName2

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(shareSessionEpoch).
      setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId1).
          setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0),
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(1)
          ).iterator)),
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId2).
          setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0),
          ).iterator)),
      ).iterator))

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)
    // First share fetch request is to establish the share session with the broker.
    kafkaApis = createKafkaApis()
    val fetchResult: Map[TopicIdPartition, ShareFetchResponseData.PartitionData] =
      kafkaApis.handleFetchFromShareFetchRequest(
        request,
        0,
        erroneousAndValidPartitionData,
        sharePartitionManager,
        authorizedTopics
      ).get()

    assertEquals(3, fetchResult.size)

    assertTrue(fetchResult.contains(tp1))
    val partitionData1: PartitionData = fetchResult.getOrElse(tp1, null)
    assertNotNull(partitionData1)

    compareResponsePartitions(
      0,
      Errors.NONE.code,
      Errors.NONE.code,
      records_t1_p1,
      expectedAcquiredRecords(0, 9, 1),
      partitionData1
    )

    assertTrue(fetchResult.contains(tp2))
    val partitionData2: PartitionData = fetchResult.getOrElse(tp2, null)
    assertNotNull(partitionData2)

    compareResponsePartitionsFetchError(
      1,
      Errors.UNKNOWN_TOPIC_OR_PARTITION.code,
      partitionData2
    )

    assertTrue(fetchResult.contains(tp3))
    val partitionData3: PartitionData = fetchResult.getOrElse(tp3, null)
    assertNotNull(partitionData3)

    compareResponsePartitionsFetchError(
      0,
      Errors.UNKNOWN_TOPIC_OR_PARTITION.code,
      partitionData3
    )
  }

  @Test
  def testHandleShareFetchFetchMessagesReturnErrorCode(): Unit = {
    val shareSessionEpoch = 0
    val topicName1 = "foo1"
    val topicName2 = "foo2"
    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()

    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName1, 1, topicId = topicId1)
    addTopicToMetadataCache(topicName2, 2, topicId = topicId2)

    val memberId: Uuid = Uuid.ZERO_UUID
    val groupId: String = "group"

    val emptyRecords = MemoryRecords.EMPTY

    val tp1 = new TopicIdPartition(topicId1, new TopicPartition(topicName1, 0))
    val tp2 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 0))
    val tp3 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 1))

    when(sharePartitionManager.fetchMessages(any(), any(), any(), any(), anyInt(), anyInt(), anyInt(), any())).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareFetchResponseData.PartitionData](
        tp1,
          new ShareFetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code)
            .setRecords(emptyRecords)
            .setAcquiredRecords(new util.ArrayList(util.List.of)),
        tp2,
          new ShareFetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code)
            .setRecords(emptyRecords)
            .setAcquiredRecords(new util.ArrayList(util.List.of)),
        tp3,
          new ShareFetchResponseData.PartitionData()
            .setPartitionIndex(1)
            .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code)
            .setRecords(emptyRecords)
            .setAcquiredRecords(new util.ArrayList(util.List.of))
      ))
    )

    val erroneousPartitions: util.Map[TopicIdPartition, ShareFetchResponseData.PartitionData] = new util.HashMap()

    val validPartitions: util.List[TopicIdPartition] = util.List.of(tp1, tp2, tp3)

    val erroneousAndValidPartitionData: ErroneousAndValidPartitionData =
      new ErroneousAndValidPartitionData(erroneousPartitions, validPartitions)

    var authorizedTopics: Set[String] = Set.empty[String]
    authorizedTopics = authorizedTopics + topicName1
    authorizedTopics = authorizedTopics + topicName2

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(shareSessionEpoch).
      setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId1).
          setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
          ).iterator)),
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId2).
          setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0),
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(1)
          ).iterator)),
      ).iterator))

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)
    // First share fetch request is to establish the share session with the broker.
    kafkaApis = createKafkaApis()
    val fetchResult: Map[TopicIdPartition, ShareFetchResponseData.PartitionData] =
      kafkaApis.handleFetchFromShareFetchRequest(
        request,
        0,
        erroneousAndValidPartitionData,
        sharePartitionManager,
        authorizedTopics
      ).get()

    assertEquals(3, fetchResult.size)

    assertTrue(fetchResult.contains(tp1))
    val partitionData1: PartitionData = fetchResult.getOrElse(tp1, null)
    assertNotNull(partitionData1)

    compareResponsePartitions(
      0,
      Errors.UNKNOWN_SERVER_ERROR.code,
      Errors.NONE.code,
      emptyRecords,
      util.List.of[AcquiredRecords](),
      partitionData1
    )

    assertTrue(fetchResult.contains(tp2))
    val partitionData2: PartitionData = fetchResult.getOrElse(tp2, null)
    assertNotNull(partitionData2)

    compareResponsePartitions(
      0,
      Errors.UNKNOWN_SERVER_ERROR.code,
      Errors.NONE.code,
      emptyRecords,
      util.List.of[AcquiredRecords](),
      partitionData2
    )

    assertTrue(fetchResult.contains(tp3))
    val partitionData3: PartitionData = fetchResult.getOrElse(tp3, null)
    assertNotNull(partitionData3)

    compareResponsePartitions(
      1,
      Errors.UNKNOWN_SERVER_ERROR.code,
      Errors.NONE.code,
      emptyRecords,
      util.List.of[AcquiredRecords](),
      partitionData3
    )
  }

  @Test
  def testHandleShareFetchFromShareFetchRequestErrorTopicsInRequest(): Unit = {
    val shareSessionEpoch = 0
    val topicName1 = "foo1"
    val topicName2 = "foo2"
    val topicName3 = "foo3"
    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()
    val topicId3 = Uuid.randomUuid()

    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName1, 1, topicId = topicId1)
    addTopicToMetadataCache(topicName2, 2, topicId = topicId2)
    // topicName3 is not in the metadataCache.

    val memberId: Uuid = Uuid.ZERO_UUID
    val groupId: String = "group"

    val records1 = memoryRecords(10, 0)
    val records2 = memoryRecords(20, 0)

    val tp1 = new TopicIdPartition(topicId1, new TopicPartition(topicName1, 0))
    val tp2 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 0))
    val tp3 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 1))
    val tp4 = new TopicIdPartition(topicId3, new TopicPartition(topicName3, 0))

    when(sharePartitionManager.fetchMessages(any(), any(), any(), any(), anyInt(), anyInt(), anyInt(), any())).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareFetchResponseData.PartitionData](
        tp2,
          new ShareFetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code)
            .setRecords(records1)
            .setAcquiredRecords(new util.ArrayList(util.List.of(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            ))),
        tp3,
          new ShareFetchResponseData.PartitionData()
            .setPartitionIndex(1)
            .setErrorCode(Errors.NONE.code)
            .setRecords(records2)
            .setAcquiredRecords(new util.ArrayList(util.List.of(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(19)
                .setDeliveryCount(1)
            )))
      ))
    )

    val erroneousPartitions: util.Map[TopicIdPartition, ShareFetchResponseData.PartitionData] = new util.HashMap()

    val validPartitions: util.List[TopicIdPartition] = util.List.of(tp1, tp2, tp3, tp4)

    val erroneousAndValidPartitionData: ErroneousAndValidPartitionData =
      new ErroneousAndValidPartitionData(erroneousPartitions, validPartitions)

    var authorizedTopics: Set[String] = Set.empty[String]
    // topicName1 is not in authorizedTopic.
    authorizedTopics = authorizedTopics + topicName2
    authorizedTopics = authorizedTopics + topicName3

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(shareSessionEpoch).
      setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId1).
          setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
          ).iterator)),
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId2).
          setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0),
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(1)
          ).iterator)),
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId3).
          setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
          ).iterator)),
      ).iterator))

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)
    kafkaApis = createKafkaApis()
    val fetchResult: Map[TopicIdPartition, ShareFetchResponseData.PartitionData] =
      kafkaApis.handleFetchFromShareFetchRequest(
        request,
        0,
        erroneousAndValidPartitionData,
        sharePartitionManager,
        authorizedTopics
      ).get()

    assertEquals(4, fetchResult.size)

    assertTrue(fetchResult.contains(tp1))
    val partitionData1: PartitionData = fetchResult.getOrElse(tp1, null)
    assertNotNull(partitionData1)

    compareResponsePartitions(
      0,
      Errors.TOPIC_AUTHORIZATION_FAILED.code,
      Errors.NONE.code,
      MemoryRecords.EMPTY,
      util.List.of[AcquiredRecords](),
      partitionData1
    )

    assertTrue(fetchResult.contains(tp2))
    val partitionData2: PartitionData = fetchResult.getOrElse(tp2, null)
    assertNotNull(partitionData2)

    compareResponsePartitions(
      0,
      Errors.NONE.code,
      Errors.NONE.code,
      records1,
      expectedAcquiredRecords(0, 9, 1),
      partitionData2
    )

    assertTrue(fetchResult.contains(tp3))
    val partitionData3: PartitionData = fetchResult.getOrElse(tp3, null)
    assertNotNull(partitionData3)

    compareResponsePartitions(
      1,
      Errors.NONE.code,
      Errors.NONE.code,
      records2,
      expectedAcquiredRecords(0, 19, 1),
      partitionData3
    )

    assertTrue(fetchResult.contains(tp4))
    val partitionData4: PartitionData = fetchResult.getOrElse(tp4, null)
    assertNotNull(partitionData4)

    compareResponsePartitions(
      0,
      Errors.UNKNOWN_TOPIC_OR_PARTITION.code,
      Errors.NONE.code,
      MemoryRecords.EMPTY,
      util.List.of[AcquiredRecords](),
      partitionData4
    )
  }

  private def compareResponsePartitions(expPartitionIndex: Int,
                                        expErrorCode: Short,
                                        expAckErrorCode: Short,
                                        expRecords: MemoryRecords,
                                        expAcquiredRecords: util.List[AcquiredRecords],
                                        partitionData: PartitionData): Unit = {
    assertEquals(expPartitionIndex, partitionData.partitionIndex)
    assertEquals(expErrorCode, partitionData.errorCode)
    assertEquals(expAckErrorCode, partitionData.acknowledgeErrorCode)
    assertEquals(expRecords, partitionData.records)
    assertArrayEquals(expAcquiredRecords.toArray(), partitionData.acquiredRecords.toArray())
  }

  private def compareResponsePartitionsFetchError(
                                                   expPartitionIndex: Int,
                                                   expErrorCode: Short,
                                                   partitionData: PartitionData
                                                 ): Unit = {
    assertEquals(expPartitionIndex, partitionData.partitionIndex)
    assertEquals(expErrorCode, partitionData.errorCode)
  }

  @Test
  def testHandleShareFetchRequestSuccessWithAcknowledgements(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: String = Uuid.randomUuid().toString

    val records1 = memoryRecords(10, 0)
    val records2 = memoryRecords(10, 10)

    val groupId = "group"

    when(sharePartitionManager.fetchMessages(any(), any(), any(), any(), anyInt(), anyInt(), anyInt(), any())).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)),
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setAcknowledgeErrorCode(Errors.NONE.code)
            .setRecords(records1)
            .setAcquiredRecords(new util.ArrayList(util.List.of(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            )))
      ))
    ).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)),
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setAcknowledgeErrorCode(Errors.NONE.code)
            .setRecords(records2)
            .setAcquiredRecords(new util.ArrayList(util.List.of(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(10)
                .setLastOffset(19)
                .setDeliveryCount(1)
            )))
      ))
    )

    val cachedSharePartitions = new ImplicitLinkedHashCollection[CachedSharePartition]
    cachedSharePartitions.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId, 0, topicName), false
    ))

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[Session](), anyString, anyDouble, anyLong)).thenReturn(0)

    when(sharePartitionManager.acknowledge(any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, 0)),
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
      ))
    )

    var shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId).
      setShareSessionEpoch(0).
      setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)).iterator))).iterator))

    var shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    var request = buildRequest(shareFetchRequest)

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any(), any(), any())).thenReturn(
      new ShareSessionContext(0, util.List.of(
        new TopicIdPartition(topicId, partitionIndex, topicName)
      ))
    ).thenReturn(new ShareSessionContext(1, new ShareSession(
      new ShareSessionKey(groupId, memberId), cachedSharePartitions, 2, request.context.connectionId))
    )

    kafkaApis = createKafkaApis()
    kafkaApis.handleShareFetchRequest(request)
    var response = verifyNoThrottling[ShareFetchResponse](request)
    var responseData = response.data()
    var topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    var topicResponse = topicResponses.stream.findFirst.get
    assertEquals(topicId, topicResponse.topicId)
    assertEquals(1, topicResponse.partitions.size())
    assertEquals(partitionIndex, topicResponse.partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponse.partitions.get(0).errorCode)
    assertEquals(records1, topicResponse.partitions.get(0).records)
    assertArrayEquals(expectedAcquiredRecords(0, 9, 1).toArray(), topicResponse.partitions.get(0).acquiredRecords.toArray())

    shareFetchRequestData = new ShareFetchRequestData().
      setGroupId("group").
      setMemberId(memberId).
      setShareSessionEpoch(1).
      setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)
            .setAcknowledgementBatches(util.List.of(
              new AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(util.List.of(1.toByte))
            ))
        ).iterator))
      ).iterator))

    shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    request = buildRequest(shareFetchRequest)
    kafkaApis.handleShareFetchRequest(request)
    response = verifyNoThrottling[ShareFetchResponse](request)
    responseData = response.data()
    topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    topicResponse = topicResponses.stream.findFirst.get
    assertEquals(topicId, topicResponse.topicId)
    assertEquals(1, topicResponse.partitions.size())
    assertEquals(partitionIndex, topicResponse.partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponse.partitions.get(0).errorCode)
    assertEquals(Errors.NONE.code, topicResponse.partitions.get(0).acknowledgeErrorCode)
    assertEquals(records2, topicResponse.partitions.get(0).records)
    assertArrayEquals(expectedAcquiredRecords(10, 19, 1).toArray(), topicResponse.partitions.get(0).acquiredRecords.toArray())
  }

  @Test
  def testHandleShareFetchRequestSuccessWithRenewAcknowledgements(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: String = Uuid.randomUuid().toString

    val records1 = memoryRecords(10, 0)

    val groupId = "group"

    when(sharePartitionManager.fetchMessages(any(), any(), any(), any(), anyInt(), anyInt(), anyInt(), any())).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)),
        new ShareFetchResponseData.PartitionData()
          .setErrorCode(Errors.NONE.code)
          .setAcknowledgeErrorCode(Errors.NONE.code)
          .setRecords(records1)
          .setAcquiredRecords(new util.ArrayList(util.List.of(
            new ShareFetchResponseData.AcquiredRecords()
              .setFirstOffset(0)
              .setLastOffset(9)
              .setDeliveryCount(1)
          )))
      ))
    )

    val cachedSharePartitions = new ImplicitLinkedHashCollection[CachedSharePartition]
    cachedSharePartitions.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId, 0, topicName), false
    ))

    when(sharePartitionManager.acknowledge(any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, 0)),
        new ShareAcknowledgeResponseData.PartitionData()
          .setPartitionIndex(0)
          .setErrorCode(Errors.NONE.code),
      ))
    )

    // First request to get some records.
    var shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId).
      setShareSessionEpoch(0).
      setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)).iterator))).iterator))

    var shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    var request = buildRequest(shareFetchRequest)

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any(), any(), any())).thenReturn(
      new ShareSessionContext(0, util.List.of(
        new TopicIdPartition(topicId, partitionIndex, topicName)
      ))
    ).thenReturn(new ShareSessionContext(1, new ShareSession(
      new ShareSessionKey(groupId, memberId), cachedSharePartitions, 2, request.context.connectionId))
    )

    kafkaApis = createKafkaApis()
    kafkaApis.handleShareFetchRequest(request)
    var response = verifyNoThrottling[ShareFetchResponse](request)
    var responseData = response.data()
    var topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    var topicResponse = topicResponses.stream.findFirst.get
    assertEquals(topicId, topicResponse.topicId)
    assertEquals(1, topicResponse.partitions.size())
    assertEquals(partitionIndex, topicResponse.partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponse.partitions.get(0).errorCode)
    assertEquals(records1, topicResponse.partitions.get(0).records)
    assertArrayEquals(expectedAcquiredRecords(0, 9, 1).toArray(), topicResponse.partitions.get(0).acquiredRecords.toArray())

    // Second request with RENEW ack.
    shareFetchRequestData = new ShareFetchRequestData().
      setGroupId("group").
      setMemberId(memberId).
      setShareSessionEpoch(1).
      setIsRenewAck(true).
      setMaxBytes(0).
      setMinBytes(0).
      setMaxWaitMs(0).
      setMaxRecords(0).
      setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setAcknowledgementBatches(util.List.of(new AcknowledgementBatch()
              .setFirstOffset(0)
              .setLastOffset(8)
              .setAcknowledgeTypes(util.List.of(AcknowledgeType.ACCEPT.id)),
              new AcknowledgementBatch()
                .setFirstOffset(9)
                .setLastOffset(9)
                .setAcknowledgeTypes(util.List.of(AcknowledgeType.RENEW.id))))
            .setPartitionIndex(0)).iterator))).iterator))

    shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    request = buildRequest(shareFetchRequest)
    kafkaApis.handleShareFetchRequest(request)
    response = verifyNoThrottling[ShareFetchResponse](request)
    responseData = response.data()
    topicResponses = responseData.responses()
    val expectedRecords = memoryRecords(0, 0)

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    topicResponse = topicResponses.stream.findFirst.get
    assertEquals(topicId, topicResponse.topicId)
    assertEquals(1, topicResponse.partitions.size())
    assertEquals(partitionIndex, topicResponse.partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponse.partitions.get(0).errorCode)
    assertEquals(Errors.NONE.code, topicResponse.partitions.get(0).acknowledgeErrorCode)
    assertEquals(expectedRecords, topicResponse.partitions.get(0).records)
    assertEquals(0, topicResponse.partitions.get(0).acquiredRecords.size())
    // fetchMessages only called once for 1st ShareFetch.
    verify(sharePartitionManager, times(1)).fetchMessages(any(), any(), any(), any(), anyInt(), anyInt(), anyInt(), any())
  }

  @ParameterizedTest
  @CsvSource(value=Array("true,false", "false,true"))
  def testHandleShareAcknowledgeRequestWithRenewAcknowledgements(isRenewAck: Boolean, shouldFail: Boolean): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.randomUuid()

    val groupId = "group"

    when(sharePartitionManager.acknowledge(any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, 0)),
        new ShareAcknowledgeResponseData.PartitionData()
          .setPartitionIndex(0)
          .setErrorCode(Errors.NONE.code)
      ))
    )

    doNothing().when(sharePartitionManager).acknowledgeSessionUpdate(any(), any(), any())

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(1).
      setIsRenewAck(isRenewAck).
      setTopics(new ShareAcknowledgeRequestData.AcknowledgeTopicCollection(util.List.of(new ShareAcknowledgeRequestData.AcknowledgeTopic().
        setTopicId(topicId).
        setPartitions(new ShareAcknowledgeRequestData.AcknowledgePartitionCollection(util.List.of(
          new ShareAcknowledgeRequestData.AcknowledgePartition()
            .setPartitionIndex(partitionIndex)
            .setAcknowledgementBatches(util.List.of(
              new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(8)
                .setAcknowledgeTypes(util.List.of(AcknowledgeType.ACCEPT.id)),
              new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(9)
                .setLastOffset(9)
                .setAcknowledgeTypes(util.List.of(AcknowledgeType.RENEW.id))
            ))
        ).iterator))).iterator))

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData)
      .build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleShareAcknowledgeRequest(request)
    val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
    val responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    val topicResponse = topicResponses.stream.findFirst.get
    assertEquals(topicId, topicResponse.topicId)
    assertEquals(1, topicResponse.partitions.size())
    assertEquals(partitionIndex, topicResponse.partitions.get(0).partitionIndex)
    if(shouldFail) {
      assertEquals(Errors.INVALID_REQUEST.code, topicResponse.partitions.get(0).errorCode)
    } else {
      assertEquals(Errors.NONE.code, topicResponse.partitions.get(0).errorCode)
    }
  }

  @Test
  def testHandleShareFetchShareGroupDisabled(): Unit = {
    val topicId = Uuid.randomUuid()
    val memberId: Uuid = Uuid.randomUuid()
    val groupId = "group"

    metadataCache = {
      val cache = new KRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_1)
      val delta = new MetadataDelta(MetadataImage.EMPTY)
      delta.replay(new FeatureLevelRecord()
        .setName(MetadataVersion.FEATURE_NAME)
        .setFeatureLevel(MetadataVersion.MINIMUM_VERSION.featureLevel())
      )
      delta.replay(new FeatureLevelRecord()
        .setName(ShareVersion.FEATURE_NAME)
        .setFeatureLevel(ShareVersion.SV_0.featureLevel())
      )
      cache.setImage(delta.apply(MetadataProvenance.EMPTY))
      cache
    }

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(1).
      setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)
            .setAcknowledgementBatches(util.List.of(
              new AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(util.List.of(1.toByte))
            ))
        ).iterator))
      ).iterator))

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)

    kafkaApis = createKafkaApis()
    kafkaApis.handleShareFetchRequest(request)

    val response = verifyNoThrottling[ShareFetchResponse](request)
    val responseData = response.data()

    assertEquals(Errors.UNSUPPORTED_VERSION.code, responseData.errorCode)
  }

  @Test
  def testHandleShareFetchRequestGroupAuthorizationError(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.ZERO_UUID

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[Session](), anyString, anyDouble, anyLong)).thenReturn(0)

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId("group").
      setMemberId(memberId.toString).
      setShareSessionEpoch(1).
      setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(partitionIndex)
            .setAcknowledgementBatches(util.List.of(
              new ShareFetchRequestData.AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(util.List.of(1.toByte))
            ))
        ).iterator))
      ).iterator))

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any(), any())).thenReturn(util.List.of[AuthorizationResult](
      AuthorizationResult.DENIED
    ))

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData)
      .build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareFetchRequest)
    kafkaApis = createKafkaApis(
      authorizer = Option(authorizer),
      )
    kafkaApis.handleShareFetchRequest(request)

    val response = verifyNoThrottling[ShareFetchResponse](request)
    val responseData = response.data()

    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, responseData.errorCode)
  }

  @Test
  def testHandleShareFetchRequestReleaseAcquiredRecordsThrowError(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: String = Uuid.randomUuid().toString

    val groupId = "group"

    when(sharePartitionManager.acknowledge(any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, 0)),
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
      ))
    )

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any(), any(), any())).thenReturn(
      new FinalContext()
    )

    when(sharePartitionManager.releaseSession(any(), any())).thenReturn(
      FutureUtils.failedFuture[util.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]](Errors.UNKNOWN_SERVER_ERROR.exception())
    )

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[Session](), anyString, anyDouble, anyLong)).thenReturn(0)

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId).
      setShareSessionEpoch(ShareRequestMetadata.FINAL_EPOCH).
      setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)
            .setAcknowledgementBatches(util.List.of(
              new AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(util.List.of(1.toByte))
            ))
        ).iterator))).iterator))

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleShareFetchRequest(request)
    val response = verifyNoThrottling[ShareFetchResponse](request)
    val responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    val topicResponse = topicResponses.stream.findFirst.get
    assertEquals(topicId, topicResponse.topicId)
    assertEquals(1, topicResponse.partitions.size())
    assertEquals(partitionIndex, topicResponse.partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponse.partitions.get(0).errorCode)
    assertEquals(Errors.NONE.code, topicResponse.partitions.get(0).acknowledgeErrorCode)
    assertEquals(MemoryRecords.EMPTY, topicResponse.partitions.get(0).records)
    assertEquals(0, topicResponse.partitions.get(0).acquiredRecords.toArray().length)
  }

  @Test
  def testHandleShareAcknowledgeRequestSuccess(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.randomUuid()

    val groupId = "group"

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[Session](), anyString, anyDouble, anyLong)).thenReturn(0)

    when(sharePartitionManager.acknowledge(any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, 0)),
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code)
      ))
    )

    doNothing().when(sharePartitionManager).acknowledgeSessionUpdate(any(), any(), any())

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(1).
      setTopics(new ShareAcknowledgeRequestData.AcknowledgeTopicCollection(util.List.of(new ShareAcknowledgeRequestData.AcknowledgeTopic().
        setTopicId(topicId).
        setPartitions(new ShareAcknowledgeRequestData.AcknowledgePartitionCollection(util.List.of(
          new ShareAcknowledgeRequestData.AcknowledgePartition()
            .setPartitionIndex(partitionIndex)
            .setAcknowledgementBatches(util.List.of(
              new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(util.List.of(1.toByte))
            ))
        ).iterator))).iterator))

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData)
      .build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleShareAcknowledgeRequest(request)
    val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
    val responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    val topicResponse = topicResponses.stream.findFirst.get
    assertEquals(topicId, topicResponse.topicId)
    assertEquals(1, topicResponse.partitions.size())
    assertEquals(partitionIndex, topicResponse.partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponse.partitions.get(0).errorCode)
  }

  @Test
  def testHandleShareAcknowledgeShareGroupDisabled(): Unit = {
    val topicId = Uuid.randomUuid()
    val memberId: Uuid = Uuid.randomUuid()
    val groupId = "group"

    metadataCache = {
      val cache = new KRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_1)
      val delta = new MetadataDelta(MetadataImage.EMPTY)
      delta.replay(new FeatureLevelRecord()
        .setName(MetadataVersion.FEATURE_NAME)
        .setFeatureLevel(MetadataVersion.MINIMUM_VERSION.featureLevel())
      )
      delta.replay(new FeatureLevelRecord()
        .setName(ShareVersion.FEATURE_NAME)
        .setFeatureLevel(ShareVersion.SV_0.featureLevel())
      )
      cache.setImage(delta.apply(MetadataProvenance.EMPTY))
      cache
    }

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData()
      .setGroupId(groupId)
      .setMemberId(memberId.toString)
      .setShareSessionEpoch(1)
      .setTopics(new ShareAcknowledgeRequestData.AcknowledgeTopicCollection(util.List.of(new ShareAcknowledgeRequestData.AcknowledgeTopic()
        .setTopicId(topicId)
        .setPartitions(new ShareAcknowledgeRequestData.AcknowledgePartitionCollection(util.List.of(
          new ShareAcknowledgeRequestData.AcknowledgePartition()
            .setPartitionIndex(0)
            .setAcknowledgementBatches(util.List.of(
              new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(util.List.of(1.toByte))
            ))
        ).iterator))).iterator))

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData).build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)

    kafkaApis = createKafkaApis()
    kafkaApis.handleShareAcknowledgeRequest(request)

    val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
    val responseData = response.data()

    assertEquals(Errors.UNSUPPORTED_VERSION.code, responseData.errorCode)
  }

  @Test
  def testHandleShareAcknowledgeRequestGroupAuthorizationError(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.ZERO_UUID

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[Session](), anyString, anyDouble, anyLong)).thenReturn(0)

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId("group").
      setMemberId(memberId.toString).
      setShareSessionEpoch(1).
      setTopics(new ShareAcknowledgeRequestData.AcknowledgeTopicCollection(util.List.of(new ShareAcknowledgeRequestData.AcknowledgeTopic().
        setTopicId(topicId).
        setPartitions(new ShareAcknowledgeRequestData.AcknowledgePartitionCollection(util.List.of(
          new ShareAcknowledgeRequestData.AcknowledgePartition()
            .setPartitionIndex(partitionIndex)
            .setAcknowledgementBatches(util.List.of(
              new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(util.List.of(1.toByte))
            ))
        ).iterator))).iterator))

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any(), any())).thenReturn(util.List.of[AuthorizationResult](
      AuthorizationResult.DENIED
    ))

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData)
      .build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)
    kafkaApis = createKafkaApis(
      authorizer = Option(authorizer),
      )
    kafkaApis.handleShareAcknowledgeRequest(request)

    val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
    val responseData = response.data()

    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, responseData.errorCode)
  }

  @Test
  def testHandleShareAcknowledgeRequestInvalidRequestOnInitialEpoch(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: String = Uuid.randomUuid().toString

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[Session](), anyString, anyDouble, anyLong)).thenReturn(0)

    when(sharePartitionManager.acknowledgeSessionUpdate(any(), any(), any())).thenThrow(
      Errors.INVALID_SHARE_SESSION_EPOCH.exception
    )

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId("group").
      setMemberId(memberId).
      setShareSessionEpoch(0).
      setTopics(new ShareAcknowledgeRequestData.AcknowledgeTopicCollection(util.List.of(new ShareAcknowledgeRequestData.AcknowledgeTopic().
        setTopicId(topicId).
        setPartitions(new ShareAcknowledgeRequestData.AcknowledgePartitionCollection(util.List.of(
          new ShareAcknowledgeRequestData.AcknowledgePartition()
            .setPartitionIndex(partitionIndex)
            .setAcknowledgementBatches(util.List.of(
              new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(util.List.of(1.toByte))
            ))
        ).iterator))).iterator))

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData)
      .build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)

    kafkaApis = createKafkaApis()
    kafkaApis.handleShareAcknowledgeRequest(request)

    val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
    val responseData = response.data()

    assertEquals(Errors.INVALID_SHARE_SESSION_EPOCH.code, responseData.errorCode)
  }

  @Test
  def testHandleShareAcknowledgeRequestSessionNotFound(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: String = Uuid.randomUuid().toString

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[Session](), anyString, anyDouble, anyLong)).thenReturn(0)

    when(sharePartitionManager.acknowledgeSessionUpdate(any(), any(), any())).thenThrow(
      Errors.SHARE_SESSION_NOT_FOUND.exception
    )

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId("group").
      setMemberId(memberId).
      setShareSessionEpoch(0).
      setTopics(new ShareAcknowledgeRequestData.AcknowledgeTopicCollection(util.List.of(new ShareAcknowledgeRequestData.AcknowledgeTopic().
        setTopicId(topicId).
        setPartitions(new ShareAcknowledgeRequestData.AcknowledgePartitionCollection(util.List.of(
          new ShareAcknowledgeRequestData.AcknowledgePartition()
            .setPartitionIndex(partitionIndex)
            .setAcknowledgementBatches(util.List.of(
              new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(util.List.of(1.toByte))
            ))
        ).iterator))).iterator))

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData)
      .build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)

    kafkaApis = createKafkaApis()
    kafkaApis.handleShareAcknowledgeRequest(request)

    val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
    val responseData = response.data()

    assertEquals(Errors.SHARE_SESSION_NOT_FOUND.code, responseData.errorCode)
  }

  @Test
  def testHandleShareAcknowledgeRequestBatchValidationError(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val groupId: String = "group"
    val memberId: String = Uuid.randomUuid().toString

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[Session](), anyString, anyDouble, anyLong)).thenReturn(0)

    doNothing().when(sharePartitionManager).acknowledgeSessionUpdate(any(), any(), any())

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId(groupId).
      setMemberId(memberId).
      setShareSessionEpoch(1).
      setTopics(new ShareAcknowledgeRequestData.AcknowledgeTopicCollection(util.List.of(new ShareAcknowledgeRequestData.AcknowledgeTopic().
        setTopicId(topicId).
        setPartitions(new ShareAcknowledgeRequestData.AcknowledgePartitionCollection(util.List.of(
          new ShareAcknowledgeRequestData.AcknowledgePartition()
            .setPartitionIndex(partitionIndex)
            .setAcknowledgementBatches(util.List.of(
              new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(10)
                .setLastOffset(4) // end offset is less than base offset
                .setAcknowledgeTypes(util.List.of(1.toByte))
            ))
        ).iterator))).iterator))

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData)
      .build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleShareAcknowledgeRequest(request)

    val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
    val responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    val topicResponse = topicResponses.stream.findFirst.get
    assertEquals(topicId, topicResponse.topicId)
    assertEquals(1, topicResponse.partitions.size())
    assertEquals(partitionIndex, topicResponse.partitions.get(0).partitionIndex)
    assertEquals(Errors.INVALID_REQUEST.code, topicResponse.partitions.get(0).errorCode)
  }

  @Test
  def testHandleShareAcknowledgeResponseContainsNewLeaderOnNotLeaderOrFollower(): Unit = {
    val topicId = Uuid.randomUuid()
    val topicName = "foo"
    val partitionIndex = 0
    val topicIdPartition = new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex))
    val topicPartition = topicIdPartition.topicPartition
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicPartition.topic, numPartitions = 1, numBrokers = 3, topicId)
    val memberId: String = Uuid.randomUuid().toString

    val partition = mock(classOf[Partition])
    val newLeaderId = 2
    val newLeaderEpoch = 5

    when(replicaManager.getPartitionOrError(topicPartition)).thenAnswer(_ => Right(partition))
    when(partition.leaderReplicaIdOpt).thenAnswer(_ => Some(newLeaderId))
    when(partition.getLeaderEpoch).thenAnswer(_ => newLeaderEpoch)

    doNothing().when(sharePartitionManager).acknowledgeSessionUpdate(any(), any(), any())

    when(sharePartitionManager.acknowledge(
      any(),
      any(),
      any()
    )).thenReturn(CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
      new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)),
        new ShareAcknowledgeResponseData.PartitionData()
          .setPartitionIndex(partitionIndex)
          .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code())
    )))

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[Session](), anyString, anyDouble, anyLong)).thenReturn(0)

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId("group").
      setMemberId(memberId).
      setShareSessionEpoch(1).
      setTopics(new ShareAcknowledgeRequestData.AcknowledgeTopicCollection(util.List.of(new ShareAcknowledgeRequestData.AcknowledgeTopic().
        setTopicId(topicId).
        setPartitions(new ShareAcknowledgeRequestData.AcknowledgePartitionCollection(util.List.of(
          new ShareAcknowledgeRequestData.AcknowledgePartition()
            .setPartitionIndex(0)
            .setAcknowledgementBatches(util.List.of(
              new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(10)
                .setLastOffset(20)
                .setAcknowledgeTypes(util.List.of(1.toByte,1.toByte,0.toByte,1.toByte,1.toByte,1.toByte,1.toByte,1.toByte,1.toByte,1.toByte,1.toByte))
            ))
        ).iterator))).iterator))

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData)
      .build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleShareAcknowledgeRequest(request)

    val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
    val responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    val topicResponse = topicResponses.stream.findFirst.get
    assertEquals(topicId, topicResponse.topicId)
    assertEquals(1, topicResponse.partitions.size())
    assertEquals(partitionIndex, topicResponse.partitions.get(0).partitionIndex)
    assertEquals(Errors.NOT_LEADER_OR_FOLLOWER.code, topicResponse.partitions.get(0).errorCode)
    assertEquals(newLeaderId, topicResponse.partitions.get(0).currentLeader.leaderId)
    assertEquals(newLeaderEpoch, topicResponse.partitions.get(0).currentLeader.leaderEpoch)
    assertEquals(2, responseData.nodeEndpoints.asScala.head.nodeId)
  }

  @Test
  def testHandleShareAcknowledgeRequestAcknowledgeThrowsError(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.randomUuid()

    val groupId = "group"

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[Session](), anyString, anyDouble, anyLong)).thenReturn(0)

    when(sharePartitionManager.acknowledge(any(), any(), any())).thenReturn(
      FutureUtils.failedFuture[util.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]](Errors.UNKNOWN_SERVER_ERROR.exception())
    )

    doNothing().when(sharePartitionManager).acknowledgeSessionUpdate(any(), any(), any())

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(1).
      setTopics(new ShareAcknowledgeRequestData.AcknowledgeTopicCollection(util.List.of(new ShareAcknowledgeRequestData.AcknowledgeTopic().
        setTopicId(topicId).
        setPartitions(new ShareAcknowledgeRequestData.AcknowledgePartitionCollection(util.List.of(
          new ShareAcknowledgeRequestData.AcknowledgePartition()
            .setPartitionIndex(partitionIndex)
            .setAcknowledgementBatches(util.List.of(
              new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(util.List.of(1.toByte))
            ))
        ).iterator))).iterator))

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData)
      .build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleShareAcknowledgeRequest(request)
    val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
    val responseData = response.data()

    assertEquals(Errors.UNKNOWN_SERVER_ERROR.code, responseData.errorCode)
  }

  @Test
  def testHandleShareAcknowledgeRequestSuccessOnFinalEpoch(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.randomUuid()

    val groupId = "group"

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[Session](), anyString, anyDouble, anyLong)).thenReturn(0)

    when(sharePartitionManager.acknowledge(any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, 0)),
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code)
      ))
    )

    doNothing().when(sharePartitionManager).acknowledgeSessionUpdate(any(), any(), any())

    when(sharePartitionManager.releaseSession(any(), any())).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, 0)),
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code)
      ))
    )

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(ShareRequestMetadata.FINAL_EPOCH).
      setTopics(new ShareAcknowledgeRequestData.AcknowledgeTopicCollection(util.List.of(new ShareAcknowledgeRequestData.AcknowledgeTopic().
        setTopicId(topicId).
        setPartitions(new ShareAcknowledgeRequestData.AcknowledgePartitionCollection(util.List.of(
          new ShareAcknowledgeRequestData.AcknowledgePartition()
            .setPartitionIndex(partitionIndex)
            .setAcknowledgementBatches(util.List.of(
              new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(util.List.of(1.toByte))
            ))
        ).iterator))).iterator))

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData)
      .build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleShareAcknowledgeRequest(request)
    val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
    val responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    val topicResponse = topicResponses.stream.findFirst.get
    assertEquals(topicId, topicResponse.topicId)
    assertEquals(1, topicResponse.partitions.size())
    assertEquals(partitionIndex, topicResponse.partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponse.partitions.get(0).errorCode)
  }

  @Test
  def testHandleShareAcknowledgeRequestReleaseAcquiredRecordsThrowError(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.randomUuid()

    val groupId = "group"

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[Session](), anyString, anyDouble, anyLong)).thenReturn(0)

    when(sharePartitionManager.acknowledge(any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, 0)),
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code)
      ))
    )

    doNothing().when(sharePartitionManager).acknowledgeSessionUpdate(any(), any(), any())

    when(sharePartitionManager.releaseSession(any(), any())).thenReturn(
      FutureUtils.failedFuture[util.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]](Errors.UNKNOWN_SERVER_ERROR.exception())
    )

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(ShareRequestMetadata.FINAL_EPOCH).
      setTopics(new ShareAcknowledgeRequestData.AcknowledgeTopicCollection(util.List.of(new ShareAcknowledgeRequestData.AcknowledgeTopic().
        setTopicId(topicId).
        setPartitions(new ShareAcknowledgeRequestData.AcknowledgePartitionCollection(util.List.of(
          new ShareAcknowledgeRequestData.AcknowledgePartition()
            .setPartitionIndex(partitionIndex)
            .setAcknowledgementBatches(util.List.of(
              new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(util.List.of(1.toByte))
            ))
        ).iterator))).iterator))

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData)
      .build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleShareAcknowledgeRequest(request)
    val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
    val responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    val topicResponse = topicResponses.stream.findFirst.get
    assertEquals(topicId, topicResponse.topicId)
    assertEquals(1, topicResponse.partitions.size())
    assertEquals(partitionIndex, topicResponse.partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponse.partitions.get(0).errorCode)
  }

  private def expectedAcquiredRecords(firstOffset: Long, lastOffset: Long, deliveryCount: Int): util.List[AcquiredRecords] = {
    val acquiredRecordsList: util.List[AcquiredRecords] = new util.ArrayList()
    acquiredRecordsList.add(new AcquiredRecords()
      .setFirstOffset(firstOffset)
      .setLastOffset(lastOffset)
      .setDeliveryCount(deliveryCount.toShort))
    acquiredRecordsList
  }

  @Test
  def testGetAcknowledgeBatchesFromShareFetchRequest(): Unit = {
    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId("group").
      setMemberId(Uuid.randomUuid().toString).
      setShareSessionEpoch(0).
      setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId1).
          setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
              .setAcknowledgementBatches(util.List.of(
                new ShareFetchRequestData.AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(util.List.of(1.toByte)),
                new ShareFetchRequestData.AcknowledgementBatch()
                  .setFirstOffset(10)
                  .setLastOffset(17)
                  .setAcknowledgeTypes(util.List.of(1.toByte))
              )),
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(1)
              .setAcknowledgementBatches(util.List.of(
                new ShareFetchRequestData.AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(util.List.of(2.toByte))
              ))
          ).iterator)),
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId2).
          setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
              .setAcknowledgementBatches(util.List.of(
                new ShareFetchRequestData.AcknowledgementBatch()
                  .setFirstOffset(24)
                  .setLastOffset(65)
                  .setAcknowledgeTypes(util.List.of(3.toByte))
              )),
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(1)
          ).iterator))
      ).iterator))
    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val topicNames = util.Map.of(topicId1, "foo1", topicId2, "foo2")
    val erroneous = mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]()

    kafkaApis = createKafkaApis()
    val acknowledgeBatches = kafkaApis.getAcknowledgeBatchesFromShareFetchRequest(shareFetchRequest, topicNames, erroneous)

    assertEquals(4, acknowledgeBatches.size)
    assertTrue(acknowledgeBatches.contains(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0))))
    assertTrue(acknowledgeBatches.contains(new TopicIdPartition(topicId1, new TopicPartition("foo1", 1))))
    assertTrue(acknowledgeBatches.contains(new TopicIdPartition(topicId2, new TopicPartition("foo2", 0))))

    assertTrue(compareAcknowledgementBatches(0, 9, 1, acknowledgeBatches.getOrElse(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0)), null).get(0)))
    assertTrue(compareAcknowledgementBatches(10, 17, 1, acknowledgeBatches.getOrElse(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0)), null).get(1)))
    assertTrue(compareAcknowledgementBatches(0, 9, 2, acknowledgeBatches.getOrElse(new TopicIdPartition(topicId1, new TopicPartition("foo1", 1)), null).get(0)))
    assertTrue(compareAcknowledgementBatches(24, 65, 3, acknowledgeBatches.getOrElse(new TopicIdPartition(topicId2, new TopicPartition("foo2", 0)), null).get(0)))
  }

  @Test
  def testGetAcknowledgeBatchesFromShareFetchRequestError(): Unit = {
    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId("group").
      setMemberId(Uuid.randomUuid().toString).
      setShareSessionEpoch(0).
      setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId1).
          setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
              .setAcknowledgementBatches(util.List.of(
                new ShareFetchRequestData.AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(util.List.of(7.toByte)) // wrong acknowledgement type here (can only be 0, 1, 2 or 3)
              )),
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(1)
              .setAcknowledgementBatches(util.List.of(
                new ShareFetchRequestData.AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(util.List.of()) // wrong acknowledgement type here (can only be 0, 1, 2 or 3)
              ))
          ).iterator)),
        new ShareFetchRequestData.FetchTopic()
          .setTopicId(topicId2)
          .setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
              .setAcknowledgementBatches(util.List.of(
                new ShareFetchRequestData.AcknowledgementBatch()
                  .setFirstOffset(24)
                  .setLastOffset(65)
                  .setAcknowledgeTypes(util.List.of(3.toByte))
              ))
          ).iterator))
      ).iterator))
    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val topicIdNames = util.Map.of(topicId1, "foo1") // topicId2 is not present in topicIdNames
    val erroneous = mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]()

    kafkaApis = createKafkaApis()
    val acknowledgeBatches = kafkaApis.getAcknowledgeBatchesFromShareFetchRequest(shareFetchRequest, topicIdNames, erroneous)
    val erroneousTopicIdPartitions = kafkaApis.validateAcknowledgementBatches(acknowledgeBatches, erroneous, supportsRenewAcknowledgements = true, isRenewAck = false)

    assertEquals(3, erroneous.size)
    assertEquals(2, erroneousTopicIdPartitions.size)
    assertTrue(erroneous.contains(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0))))
    assertTrue(erroneous.contains(new TopicIdPartition(topicId1, new TopicPartition("foo1", 1))))
    assertTrue(erroneous.contains(new TopicIdPartition(topicId2, new TopicPartition(null, 0))))
    assertEquals(Errors.INVALID_REQUEST.code, erroneous(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0))).errorCode)
    assertEquals(Errors.INVALID_REQUEST.code, erroneous(new TopicIdPartition(topicId1, new TopicPartition("foo1", 1))).errorCode)
    assertEquals(Errors.UNKNOWN_TOPIC_ID.code, erroneous(new TopicIdPartition(topicId2, new TopicPartition(null, 0))).errorCode)
  }

  @Test
  def testGetAcknowledgeBatchesFromShareAcknowledgeRequest(): Unit = {
    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()

    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId("group").
      setMemberId(Uuid.randomUuid().toString).
      setShareSessionEpoch(0).
      setTopics(new ShareAcknowledgeRequestData.AcknowledgeTopicCollection(util.List.of(
        new ShareAcknowledgeRequestData.AcknowledgeTopic().
          setTopicId(topicId1).
          setPartitions(new ShareAcknowledgeRequestData.AcknowledgePartitionCollection(util.List.of(
            new ShareAcknowledgeRequestData.AcknowledgePartition()
              .setPartitionIndex(0)
              .setAcknowledgementBatches(util.List.of(
                new ShareAcknowledgeRequestData.AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(util.List.of(1.toByte)),
                new ShareAcknowledgeRequestData.AcknowledgementBatch()
                  .setFirstOffset(10)
                  .setLastOffset(17)
                  .setAcknowledgeTypes(util.List.of(1.toByte))
              )),
            new ShareAcknowledgeRequestData.AcknowledgePartition()
              .setPartitionIndex(1)
              .setAcknowledgementBatches(util.List.of(
                new ShareAcknowledgeRequestData.AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(util.List.of(2.toByte))
              ))
          ).iterator)),
        new ShareAcknowledgeRequestData.AcknowledgeTopic().
          setTopicId(topicId2).
          setPartitions(new ShareAcknowledgeRequestData.AcknowledgePartitionCollection(util.List.of(
            new ShareAcknowledgeRequestData.AcknowledgePartition()
              .setPartitionIndex(0)
              .setAcknowledgementBatches(util.List.of(
                new ShareAcknowledgeRequestData.AcknowledgementBatch()
                  .setFirstOffset(24)
                  .setLastOffset(65)
                  .setAcknowledgeTypes(util.List.of(3.toByte))
              ))
          ).iterator))
      ).iterator))

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData).build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val topicNames = util.Map.of(topicId1, "foo1", topicId2, "foo2")
    val erroneous = mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]()

    kafkaApis = createKafkaApis()
    val acknowledgeBatches = kafkaApis.getAcknowledgeBatchesFromShareAcknowledgeRequest(shareAcknowledgeRequest, topicNames, erroneous)

    assertEquals(3, acknowledgeBatches.size)
    assertTrue(acknowledgeBatches.contains(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0))))
    assertTrue(acknowledgeBatches.contains(new TopicIdPartition(topicId1, new TopicPartition("foo1", 1))))
    assertTrue(acknowledgeBatches.contains(new TopicIdPartition(topicId2, new TopicPartition("foo2", 0))))

    assertTrue(compareAcknowledgementBatches(0, 9, 1, acknowledgeBatches.getOrElse(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0)), null).get(0)))
    assertTrue(compareAcknowledgementBatches(10, 17, 1, acknowledgeBatches.getOrElse(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0)), null).get(1)))
    assertTrue(compareAcknowledgementBatches(0, 9, 2, acknowledgeBatches.getOrElse(new TopicIdPartition(topicId1, new TopicPartition("foo1", 1)), null).get(0)))
    assertTrue(compareAcknowledgementBatches(24, 65, 3, acknowledgeBatches.getOrElse(new TopicIdPartition(topicId2, new TopicPartition("foo2", 0)), null).get(0)))
  }

  @Test
  def testGetAcknowledgeBatchesFromShareAcknowledgeRequestError(): Unit = {
    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId("group").
      setMemberId(Uuid.randomUuid().toString).
      setShareSessionEpoch(0).
      setTopics(new ShareAcknowledgeRequestData.AcknowledgeTopicCollection(util.List.of(
        new ShareAcknowledgeRequestData.AcknowledgeTopic().
          setTopicId(topicId1).
          setPartitions(new ShareAcknowledgeRequestData.AcknowledgePartitionCollection(util.List.of(
            new ShareAcknowledgeRequestData.AcknowledgePartition()
              .setPartitionIndex(0)
              .setAcknowledgementBatches(util.List.of(
                new ShareAcknowledgeRequestData.AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(util.List.of(7.toByte)) // wrong acknowledgement type here (can only be 0, 1, 2 or 3)
              )),
            new ShareAcknowledgeRequestData.AcknowledgePartition()
              .setPartitionIndex(1)
              .setAcknowledgementBatches(util.List.of(
                new ShareAcknowledgeRequestData.AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(util.List.of) // wrong acknowledgement type here (can only be 0, 1, 2 or 3)
              ))
          ).iterator)),
        new ShareAcknowledgeRequestData.AcknowledgeTopic().
          setTopicId(topicId2).
          setPartitions(new ShareAcknowledgeRequestData.AcknowledgePartitionCollection(util.List.of(
            new ShareAcknowledgeRequestData.AcknowledgePartition()
              .setPartitionIndex(0)
              .setAcknowledgementBatches(util.List.of(
                new ShareAcknowledgeRequestData.AcknowledgementBatch()
                  .setFirstOffset(24)
                  .setLastOffset(65)
                  .setAcknowledgeTypes(util.List.of(3.toByte))
              ))
          ).iterator))
      ).iterator))

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData).build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val topicIdNames = util.Map.of(topicId1, "foo1") // topicId2 not present in topicIdNames
    val erroneous = mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]()

    kafkaApis = createKafkaApis()
    val acknowledgeBatches = kafkaApis.getAcknowledgeBatchesFromShareAcknowledgeRequest(shareAcknowledgeRequest, topicIdNames, erroneous)
    val erroneousTopicIdPartitions = kafkaApis.validateAcknowledgementBatches(acknowledgeBatches, erroneous, supportsRenewAcknowledgements = true, isRenewAck = false)

    assertEquals(3, erroneous.size)
    assertEquals(2, erroneousTopicIdPartitions.size)
    assertTrue(erroneous.contains(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0))))
    assertTrue(erroneous.contains(new TopicIdPartition(topicId1, new TopicPartition("foo1", 1))))
    assertTrue(erroneous.contains(new TopicIdPartition(topicId2, new TopicPartition(null, 0))))

    assertTrue(erroneous.contains(new TopicIdPartition(topicId2, new TopicPartition(null, 0))))
    assertEquals(Errors.INVALID_REQUEST.code, erroneous(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0))).errorCode)
    assertEquals(Errors.INVALID_REQUEST.code, erroneous(new TopicIdPartition(topicId1, new TopicPartition("foo1", 1))).errorCode)
    assertEquals(Errors.UNKNOWN_TOPIC_ID.code, erroneous(new TopicIdPartition(topicId2, new TopicPartition(null, 0))).errorCode)
  }

  @Test
  def testHandleAcknowledgementsSuccess(): Unit = {
    val groupId = "group"

    val topicName1 = "foo1"
    val topicName2 = "foo2"

    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()
    val memberId = Uuid.randomUuid()

    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName1, 1, topicId = topicId1)
    addTopicToMetadataCache(topicName2, 2, topicId = topicId2)

    val tp1 = new TopicIdPartition(topicId1, new TopicPartition(topicName1, 0))
    val tp2 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 0))
    val tp3 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 1))

    when(sharePartitionManager.acknowledge(any(), any(), any()))
      .thenReturn(CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        tp1,
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
        tp2,
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
        tp3,
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(1)
            .setErrorCode(Errors.NONE.code)
      )))

    val acknowledgementData = mutable.Map[TopicIdPartition, util.List[ShareAcknowledgementBatch]]()

    acknowledgementData += (tp1 -> util.List.of(
      new ShareAcknowledgementBatch(0, 9, util.List.of(1.toByte)),
      new ShareAcknowledgementBatch(10, 19, util.List.of(2.toByte))
    ))
    acknowledgementData += (tp2 -> util.List.of(
      new ShareAcknowledgementBatch(5, 19, util.List.of(2.toByte))
    ))
    acknowledgementData += (tp3 -> util.List.of(
      new ShareAcknowledgementBatch(34, 56, util.List.of(1.toByte))
    ))

    val authorizedTopics: Set[String] = Set(topicName1, topicName2)

    val erroneous = mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]()

    kafkaApis = createKafkaApis()
    val ackResult = kafkaApis.handleAcknowledgements(
      acknowledgementData,
      erroneous,
      sharePartitionManager,
      authorizedTopics,
      groupId,
      memberId.toString,
      supportsRenewAcknowledgements = true,
      isRenewAck = false
    ).get()

    assertEquals(3, ackResult.size)
    assertTrue(ackResult.contains(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0))))
    assertTrue(ackResult.contains(new TopicIdPartition(topicId2, new TopicPartition("foo2", 0))))
    assertTrue(ackResult.contains(new TopicIdPartition(topicId2, new TopicPartition("foo2", 1))))

    assertTrue(compareAcknowledgeResponsePartitionData(0, Errors.NONE.code, ackResult.getOrElse(
      new TopicIdPartition(topicId1, new TopicPartition("foo1", 0)), null)))
    assertTrue(compareAcknowledgeResponsePartitionData(0, Errors.NONE.code, ackResult.getOrElse(
      new TopicIdPartition(topicId2, new TopicPartition("foo2", 0)), null)))
    assertTrue(compareAcknowledgeResponsePartitionData(1, Errors.NONE.code, ackResult.getOrElse(
      new TopicIdPartition(topicId2, new TopicPartition("foo2", 1)), null)))
  }

  @Test
  def testHandleAcknowledgementsInvalidAcknowledgementBatches(): Unit = {
    val groupId = "group"

    val topicName1 = "foo1"
    val topicName2 = "foo2"

    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()
    val memberId = Uuid.randomUuid()

    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName1, 1, topicId = topicId1)
    addTopicToMetadataCache(topicName2, 2, topicId = topicId2)

    val tp1 = new TopicIdPartition(topicId1, new TopicPartition(topicName1, 0))
    val tp2 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 0))
    val tp3 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 1))

    when(sharePartitionManager.acknowledge(any(), any(), any()))
      .thenReturn(CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId1, new TopicPartition("foo1", 0)),
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
        new TopicIdPartition(topicId2, new TopicPartition("foo2", 0)),
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
        new TopicIdPartition(topicId2, new TopicPartition("foo2", 1)),
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(1)
            .setErrorCode(Errors.NONE.code)
      )))

    val acknowledgementData = mutable.Map[TopicIdPartition, util.List[ShareAcknowledgementBatch]]()
    acknowledgementData += (tp1 -> util.List.of(
      new ShareAcknowledgementBatch(39, 24, util.List.of(1.toByte)), // this is an invalid batch because last offset is less than base offset
      new ShareAcknowledgementBatch(43, 56, util.List.of(2.toByte))
    ))
    acknowledgementData += (tp2 -> util.List.of(
      new ShareAcknowledgementBatch(5, 19, util.List.of(0.toByte, 2.toByte))
    ))
    acknowledgementData += (tp3 -> util.List.of(
      new ShareAcknowledgementBatch(34, 56, util.List.of(1.toByte)),
      new ShareAcknowledgementBatch(10, 19, util.List.of(1.toByte)) // this is an invalid batch because start is offset is less than previous end offset
    ))

    val authorizedTopics: Set[String] = Set(topicName1, topicName2)

    val erroneous = mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]()

    kafkaApis = createKafkaApis()
    val ackResult = kafkaApis.handleAcknowledgements(
      acknowledgementData,
      erroneous,
      sharePartitionManager,
      authorizedTopics,
      groupId,
      memberId.toString,
      supportsRenewAcknowledgements = true,
      isRenewAck = false
    ).get()

    assertEquals(3, ackResult.size)
    assertTrue(ackResult.contains(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0))))
    assertTrue(ackResult.contains(new TopicIdPartition(topicId2, new TopicPartition("foo2", 0))))
    assertTrue(ackResult.contains(new TopicIdPartition(topicId2, new TopicPartition("foo2", 1))))

    assertTrue(compareAcknowledgeResponsePartitionData(0, Errors.INVALID_REQUEST.code, ackResult.getOrElse(
      new TopicIdPartition(topicId1, new TopicPartition("foo1", 0)), null)))
    assertTrue(compareAcknowledgeResponsePartitionData(0, Errors.INVALID_REQUEST.code, ackResult.getOrElse(
      new TopicIdPartition(topicId2, new TopicPartition("foo2", 0)), null)))
    assertTrue(compareAcknowledgeResponsePartitionData(1, Errors.INVALID_REQUEST.code, ackResult.getOrElse(
      new TopicIdPartition(topicId2, new TopicPartition("foo2", 1)), null)))
  }

  @Test
  def testHandleAcknowledgementsUnauthorizedTopics(): Unit = {
    val groupId = "group"

    val topicName1 = "foo1"
    val topicName2 = "foo2"

    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()
    val memberId = Uuid.randomUuid()

    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    // Topic with id topicId1 is not present in Metadata Cache
    addTopicToMetadataCache(topicName2, 2, topicId = topicId2)

    val tp1 = new TopicIdPartition(topicId1, new TopicPartition(topicName1, 0))
    val tp2 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 0))
    val tp3 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 1))

    when(sharePartitionManager.acknowledge(any(), any(), any()))
      .thenReturn(CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId1, new TopicPartition("foo1", 0)),
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
        new TopicIdPartition(topicId2, new TopicPartition("foo2", 0)),
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
        new TopicIdPartition(topicId2, new TopicPartition("foo2", 1)),
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(1)
            .setErrorCode(Errors.NONE.code)
      )))

    val acknowledgementData = mutable.Map[TopicIdPartition, util.List[ShareAcknowledgementBatch]]()

    acknowledgementData += (tp1 -> util.List.of(
      new ShareAcknowledgementBatch(24, 39, util.List.of(1.toByte)),
      new ShareAcknowledgementBatch(43, 56, util.List.of(2.toByte))
    ))
    acknowledgementData += (tp2 -> util.List.of(
      new ShareAcknowledgementBatch(5, 19, util.List.of(2.toByte))
    ))
    acknowledgementData += (tp3 -> util.List.of(
      new ShareAcknowledgementBatch(34, 56, util.List.of(1.toByte)),
      new ShareAcknowledgementBatch(67, 87, util.List.of(1.toByte))
    ))

    val authorizedTopics: Set[String] = Set(topicName1) // Topic with topicId2 is not authorized

    val erroneous = mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]()

    kafkaApis = createKafkaApis()
    val ackResult = kafkaApis.handleAcknowledgements(
      acknowledgementData,
      erroneous,
      sharePartitionManager,
      authorizedTopics,
      groupId,
      memberId.toString,
      supportsRenewAcknowledgements = true,
      isRenewAck = false
    ).get()

    assertEquals(3, ackResult.size)
    assertTrue(ackResult.contains(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0))))
    assertTrue(ackResult.contains(new TopicIdPartition(topicId2, new TopicPartition("foo2", 0))))
    assertTrue(ackResult.contains(new TopicIdPartition(topicId2, new TopicPartition("foo2", 1))))

    assertTrue(compareAcknowledgeResponsePartitionData(0, Errors.UNKNOWN_TOPIC_OR_PARTITION.code, ackResult.getOrElse(
      new TopicIdPartition(topicId1, new TopicPartition("foo1", 0)), null)))
    assertTrue(compareAcknowledgeResponsePartitionData(0, Errors.TOPIC_AUTHORIZATION_FAILED.code, ackResult.getOrElse(
      new TopicIdPartition(topicId2, new TopicPartition("foo2", 0)), null)))
    assertTrue(compareAcknowledgeResponsePartitionData(1, Errors.TOPIC_AUTHORIZATION_FAILED.code, ackResult.getOrElse(
      new TopicIdPartition(topicId2, new TopicPartition("foo2", 1)), null)))
  }

  @Test
  def testHandleAcknowledgementsWithErroneous(): Unit = {
    val groupId = "group"

    val topicName1 = "foo1"
    val topicName2 = "foo2"

    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()
    val memberId = Uuid.randomUuid()

    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName1, 1, topicId = topicId1)
    addTopicToMetadataCache(topicName2, 2, topicId = topicId2)

    val tp1 = new TopicIdPartition(topicId1, new TopicPartition(topicName1, 0))
    val tp2 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 0))
    val tp3 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 1))

    when(sharePartitionManager.acknowledge(any(), any(), any()))
      .thenReturn(CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        tp1,
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
        tp2,
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code)
      )))

    val acknowledgementData = mutable.Map[TopicIdPartition, util.List[ShareAcknowledgementBatch]]()

    acknowledgementData += (tp1 -> util.List.of(
      new ShareAcknowledgementBatch(0, 9, util.List.of(1.toByte)),
      new ShareAcknowledgementBatch(10, 19, util.List.of(2.toByte))
    ))
    acknowledgementData += (tp2 -> util.List.of(
      new ShareAcknowledgementBatch(5, 19, util.List.of(2.toByte))
    ))

    val authorizedTopics: Set[String] = Set(topicName1, topicName2)

    val erroneous = mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]()

    erroneous += (tp3 -> ShareAcknowledgeResponse.partitionResponse(tp3, Errors.UNKNOWN_TOPIC_ID))

    kafkaApis = createKafkaApis()
    val ackResult = kafkaApis.handleAcknowledgements(
      acknowledgementData,
      erroneous,
      sharePartitionManager,
      authorizedTopics,
      groupId,
      memberId.toString,
      supportsRenewAcknowledgements = true,
      isRenewAck = false
    ).get()

    assertEquals(3, ackResult.size)
    assertTrue(ackResult.contains(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0))))
    assertTrue(ackResult.contains(new TopicIdPartition(topicId2, new TopicPartition("foo2", 0))))
    assertTrue(ackResult.contains(new TopicIdPartition(topicId2, new TopicPartition("foo2", 1))))

    assertTrue(compareAcknowledgeResponsePartitionData(0, Errors.NONE.code, ackResult.getOrElse(
      new TopicIdPartition(topicId1, new TopicPartition("foo1", 0)), null)))
    assertTrue(compareAcknowledgeResponsePartitionData(0, Errors.NONE.code, ackResult.getOrElse(
      new TopicIdPartition(topicId2, new TopicPartition("foo2", 0)), null)))
    assertTrue(compareAcknowledgeResponsePartitionData(1, Errors.UNKNOWN_TOPIC_ID.code, ackResult.getOrElse(
      new TopicIdPartition(topicId2, new TopicPartition("foo2", 1)), null)))
  }

  @Test
  def testProcessShareAcknowledgeResponse(): Unit = {
    val groupId = "group"

    val memberId = Uuid.randomUuid()

    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()

    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    val responseAcknowledgeData: mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData] = mutable.Map()
    responseAcknowledgeData += (new TopicIdPartition(topicId1, new TopicPartition("foo", 0)) ->
      new ShareAcknowledgeResponseData.PartitionData().setPartitionIndex(0).setErrorCode(Errors.NONE.code))
    responseAcknowledgeData += (new TopicIdPartition(topicId1, new TopicPartition("foo", 1)) ->
      new ShareAcknowledgeResponseData.PartitionData().setPartitionIndex(0).setErrorCode(Errors.INVALID_REQUEST.code))
    responseAcknowledgeData += (new TopicIdPartition(topicId2, new TopicPartition("bar", 0)) ->
      new ShareAcknowledgeResponseData.PartitionData().setPartitionIndex(0).setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code))
    responseAcknowledgeData += (new TopicIdPartition(topicId2, new TopicPartition("bar", 1)) ->
      new ShareAcknowledgeResponseData.PartitionData().setPartitionIndex(0).setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code))

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(1).
      setTopics(new ShareAcknowledgeRequestData.AcknowledgeTopicCollection(util.List.of(
        new ShareAcknowledgeRequestData.AcknowledgeTopic().
          setTopicId(topicId1).
          setPartitions(new ShareAcknowledgeRequestData.AcknowledgePartitionCollection(util.List.of(
            new ShareAcknowledgeRequestData.AcknowledgePartition()
              .setPartitionIndex(0)
              .setAcknowledgementBatches(util.List.of(
                new ShareAcknowledgeRequestData.AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(util.List.of(1.toByte))
              )),
            new ShareAcknowledgeRequestData.AcknowledgePartition()
              .setPartitionIndex(1)
              .setAcknowledgementBatches(util.List.of(
                new ShareAcknowledgeRequestData.AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(util.List.of(1.toByte))
              ))
          ).iterator)),
        new ShareAcknowledgeRequestData.AcknowledgeTopic().
          setTopicId(topicId2).
          setPartitions(new ShareAcknowledgeRequestData.AcknowledgePartitionCollection(util.List.of(
            new ShareAcknowledgeRequestData.AcknowledgePartition()
              .setPartitionIndex(0)
              .setAcknowledgementBatches(util.List.of(
                new ShareAcknowledgeRequestData.AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(util.List.of(1.toByte))
              )),
            new ShareAcknowledgeRequestData.AcknowledgePartition()
              .setPartitionIndex(1)
              .setAcknowledgementBatches(util.List.of(
                new ShareAcknowledgeRequestData.AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(util.List.of(1.toByte))
              ))
          ).iterator))
      ).iterator))

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData)
      .build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)
    kafkaApis = createKafkaApis(
    )
    val response = kafkaApis.processShareAcknowledgeResponse(responseAcknowledgeData, request)
    val responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(2, topicResponses.size())

    val topicResponsesScala = topicResponses.asScala.toList
    val topicResponsesMap: Map[Uuid, ShareAcknowledgeResponseData.ShareAcknowledgeTopicResponse] = topicResponsesScala.map(topic => topic.topicId -> topic).toMap
    assertTrue(topicResponsesMap.contains(topicId1))

    val topicIdResponse1: ShareAcknowledgeResponseData.ShareAcknowledgeTopicResponse = topicResponsesMap.getOrElse(topicId1, null)
    assertEquals(2, topicIdResponse1.partitions().size())

    val partitionResponses1 = topicIdResponse1.partitions().asScala.toList
    val partitionResponsesMap1: Map[Int, ShareAcknowledgeResponseData.PartitionData] = partitionResponses1.map(partition => partition.partitionIndex -> partition).toMap
    assertTrue(partitionResponsesMap1.contains(0))
    assertTrue(partitionResponsesMap1.contains(1))
    assertTrue(partitionResponsesMap1.getOrElse(0, null).errorCode == Errors.NONE.code)
    assertTrue(partitionResponsesMap1.getOrElse(1, null).errorCode == Errors.INVALID_REQUEST.code)

    assertTrue(topicResponsesMap.contains(topicId2))

    val topicIdResponse2: ShareAcknowledgeResponseData.ShareAcknowledgeTopicResponse = topicResponsesMap.getOrElse(topicId2, null)
    assertEquals(2, topicIdResponse2.partitions().size())

    val partitionResponses2 = topicIdResponse2.partitions().asScala.toList
    val partitionResponsesMap2: Map[Int, ShareAcknowledgeResponseData.PartitionData] = partitionResponses2.map(partition => partition.partitionIndex -> partition).toMap
    assertTrue(partitionResponsesMap2.contains(0))
    assertTrue(partitionResponsesMap2.contains(1))
    assertTrue(partitionResponsesMap2.getOrElse(0, null).errorCode == Errors.TOPIC_AUTHORIZATION_FAILED.code)
    assertTrue(partitionResponsesMap2.getOrElse(1, null).errorCode == Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
  }

  private def compareAcknowledgementBatches(baseOffset: Long,
                                            endOffset: Long,
                                            acknowledgeType: Byte,
                                            acknowledgementBatch: ShareAcknowledgementBatch
                                           ): Boolean = {
    if (baseOffset == acknowledgementBatch.firstOffset()
      && endOffset == acknowledgementBatch.lastOffset()
      && acknowledgeType == acknowledgementBatch.acknowledgeTypes().get(0)) {
      return true
    }
    false
  }

  private def compareAcknowledgeResponsePartitionData(partitionIndex: Int,
                                              ackErrorCode: Short,
                                              partitionData: ShareAcknowledgeResponseData.PartitionData
                                             ): Boolean = {
    if (partitionIndex == partitionData.partitionIndex() && ackErrorCode == partitionData.errorCode()) {
      return true
    }
    false
  }

  private def memoryRecordsBuilder(numOfRecords: Int, startOffset: Long): MemoryRecordsBuilder = {

    val buffer: ByteBuffer = ByteBuffer.allocate(1024)
    val compression: Compression = Compression.of(CompressionType.NONE).build()
    val timestampType: TimestampType = TimestampType.CREATE_TIME

    val builder: MemoryRecordsBuilder = MemoryRecords.builder(buffer, compression, timestampType, startOffset)
    for (i <- 0 until numOfRecords) {
      builder.appendWithOffset(startOffset + i, 0L, TestUtils.randomBytes(10), TestUtils.randomBytes(10))
    }
    builder
  }

  private def memoryRecords(numOfRecords: Int, startOffset: Long): MemoryRecords = {
    memoryRecordsBuilder(numOfRecords, startOffset).build()
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
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleJoinGroupRequest(
      requestChannelRequest,
      RequestLocal.noCaching
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
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleJoinGroupRequest(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    val joinGroupResponse = new JoinGroupResponseData()
      .setErrorCode(Errors.INCONSISTENT_GROUP_PROTOCOL.code)
      .setMemberId("member")
      .setProtocolName(null)

    val expectedJoinGroupResponse = new JoinGroupResponseData()
      .setErrorCode(Errors.INCONSISTENT_GROUP_PROTOCOL.code)
      .setMemberId("member")
      .setProtocolName(if (version >= 7) null else "")

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
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleJoinGroupRequest(
      requestChannelRequest,
      RequestLocal.noCaching
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
      .thenReturn(util.List.of(AuthorizationResult.DENIED))
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleJoinGroupRequest(
      requestChannelRequest,
      RequestLocal.noCaching
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
      RequestLocal.noCaching.bufferSupplier
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
      RequestLocal.noCaching
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
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleSyncGroupRequest(
      requestChannelRequest,
      RequestLocal.noCaching
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
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleSyncGroupRequest(
      requestChannelRequest,
      RequestLocal.noCaching
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
      .thenReturn(util.List.of(AuthorizationResult.DENIED))
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleSyncGroupRequest(
      requestChannelRequest,
      RequestLocal.noCaching
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
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleSyncGroupRequest(
      requestChannelRequest,
      RequestLocal.noCaching
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
      .thenReturn(util.List.of(AuthorizationResult.DENIED))
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleHeartbeatRequest(
      requestChannelRequest
    )

    val response = verifyNoThrottling[HeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED, response.error)
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.LEAVE_GROUP)
  def testHandleLeaveGroupWithMultipleMembers(version: Short): Unit = {
    def makeRequest(version: Short): RequestChannel.Request = {
      buildRequest(new LeaveGroupRequest.Builder(
        "group",
        util.List.of(
          new MemberIdentity()
            .setMemberId("member-1")
            .setGroupInstanceId("instance-1"),
          new MemberIdentity()
            .setMemberId("member-2")
            .setGroupInstanceId("instance-2")
        )).build(version))
    }

    if (version < 3) {
      // Request version earlier than version 3 do not support batching members.
      assertThrows(classOf[UnsupportedVersionException], () => makeRequest(version))
    } else {
      val requestChannelRequest = makeRequest(version)

      val expectedLeaveGroupRequest = new LeaveGroupRequestData()
        .setGroupId("group")
        .setMembers(util.List.of(
          new MemberIdentity()
            .setMemberId("member-1")
            .setGroupInstanceId("instance-1"),
          new MemberIdentity()
            .setMemberId("member-2")
            .setGroupInstanceId("instance-2")
        ))

      val future = new CompletableFuture[LeaveGroupResponseData]()
      when(groupCoordinator.leaveGroup(
        requestChannelRequest.context,
        expectedLeaveGroupRequest
      )).thenReturn(future)
      kafkaApis = createKafkaApis()
      kafkaApis.handleLeaveGroupRequest(requestChannelRequest)

      val expectedLeaveResponse = new LeaveGroupResponseData()
        .setErrorCode(Errors.NONE.code)
        .setMembers(util.List.of(
          new LeaveGroupResponseData.MemberResponse()
            .setMemberId("member-1")
            .setGroupInstanceId("instance-1"),
          new LeaveGroupResponseData.MemberResponse()
            .setMemberId("member-2")
            .setGroupInstanceId("instance-2"),
        ))

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
      util.List.of(
        new MemberIdentity()
          .setMemberId("member-1")
          .setGroupInstanceId("instance-1")
      )
    ).build(version))

    val expectedLeaveGroupRequest = new LeaveGroupRequestData()
      .setGroupId("group")
      .setMembers(util.List.of(
        new MemberIdentity()
          .setMemberId("member-1")
          .setGroupInstanceId(if (version >= 3) "instance-1" else null)
      ))

    val future = new CompletableFuture[LeaveGroupResponseData]()
    when(groupCoordinator.leaveGroup(
      requestChannelRequest.context,
      expectedLeaveGroupRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleLeaveGroupRequest(requestChannelRequest)

    val leaveGroupResponse = new LeaveGroupResponseData()
      .setErrorCode(Errors.NONE.code)
      .setMembers(util.List.of(
        new LeaveGroupResponseData.MemberResponse()
          .setMemberId("member-1")
          .setGroupInstanceId("instance-1")
      ))

    val expectedLeaveResponse = if (version >= 3) {
      new LeaveGroupResponseData()
        .setErrorCode(Errors.NONE.code)
        .setMembers(util.List.of(
          new LeaveGroupResponseData.MemberResponse()
            .setMemberId("member-1")
            .setGroupInstanceId("instance-1")
        ))
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
      util.List.of(
        new MemberIdentity()
          .setMemberId("member-1")
          .setGroupInstanceId("instance-1")
      )
    ).build(ApiKeys.LEAVE_GROUP.latestVersion))

    val expectedLeaveGroupRequest = new LeaveGroupRequestData()
      .setGroupId("group")
      .setMembers(util.List.of(
        new MemberIdentity()
          .setMemberId("member-1")
          .setGroupInstanceId("instance-1")
      ))

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
      util.List.of(
        new MemberIdentity()
          .setMemberId("member-1")
          .setGroupInstanceId("instance-1")
      )
    ).build(ApiKeys.LEAVE_GROUP.latestVersion))

    val expectedLeaveGroupRequest = new LeaveGroupRequestData()
      .setGroupId("group")
      .setMembers(util.List.of(
        new MemberIdentity()
          .setMemberId("member-1")
          .setGroupInstanceId("instance-1")
      ))

    val future = new CompletableFuture[LeaveGroupResponseData]()
    when(groupCoordinator.leaveGroup(
      requestChannelRequest.context,
      expectedLeaveGroupRequest
    )).thenReturn(future)

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(util.List.of(AuthorizationResult.DENIED))
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleLeaveGroupRequest(requestChannelRequest)

    val response = verifyNoThrottling[LeaveGroupResponse](requestChannelRequest)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED, response.error)
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH)
  def testHandleOffsetFetchWithMultipleGroups(version: Short): Unit = {
    val foo = "foo"
    val bar = "bar"
    val fooId = Uuid.randomUuid()
    addTopicToMetadataCache(foo, topicId = fooId, numPartitions = 2)

    def makeRequest(version: Short): RequestChannel.Request = {
      buildRequest(
        OffsetFetchRequest.Builder.forTopicIdsOrNames(
          new OffsetFetchRequestData()
            .setGroups(util.List.of(
              new OffsetFetchRequestData.OffsetFetchRequestGroup()
                .setGroupId("group-1")
                .setTopics(util.List.of(
                  new OffsetFetchRequestData.OffsetFetchRequestTopics()
                    .setName(foo)
                    .setTopicId(fooId)
                    .setPartitionIndexes(util.List.of[Integer](0, 1))
                )),
              new OffsetFetchRequestData.OffsetFetchRequestGroup()
                .setGroupId("group-2")
                .setTopics(null),
              new OffsetFetchRequestData.OffsetFetchRequestGroup()
                .setGroupId("group-3")
                .setTopics(null),
              new OffsetFetchRequestData.OffsetFetchRequestGroup()
                .setGroupId("group-4")
                .setTopics(null),
            )),
          false
        ).build(version)
      )
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
          .setTopics(util.List.of(
            new OffsetFetchRequestData.OffsetFetchRequestTopics()
              .setTopicId(if (version >= 10) fooId else Uuid.ZERO_UUID)
              .setName("foo")
              .setPartitionIndexes(util.List.of[Integer](0, 1)))),
        false
      )).thenReturn(group1Future)

      val group2Future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
      when(groupCoordinator.fetchOffsets(
        requestChannelRequest.context,
        new OffsetFetchRequestData.OffsetFetchRequestGroup()
          .setGroupId("group-2")
          .setTopics(null),
        false
      )).thenReturn(group2Future)

      val group3Future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
      when(groupCoordinator.fetchOffsets(
        requestChannelRequest.context,
        new OffsetFetchRequestData.OffsetFetchRequestGroup()
          .setGroupId("group-3")
          .setTopics(null),
        false
      )).thenReturn(group3Future)

      val group4Future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
      when(groupCoordinator.fetchOffsets(
        requestChannelRequest.context,
        new OffsetFetchRequestData.OffsetFetchRequestGroup()
          .setGroupId("group-4")
          .setTopics(null),
        false
      )).thenReturn(group4Future)
      kafkaApis = createKafkaApis()
      kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

      val group1Response = new OffsetFetchResponseData.OffsetFetchResponseGroup()
        .setGroupId("group-1")
        .setTopics(util.List.of(
          new OffsetFetchResponseData.OffsetFetchResponseTopics()
            .setTopicId(fooId)
            .setName(foo)
            .setPartitions(util.List.of(
              new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                .setPartitionIndex(0)
                .setCommittedOffset(100)
                .setCommittedLeaderEpoch(1),
              new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                .setPartitionIndex(1)
                .setCommittedOffset(200)
                .setCommittedLeaderEpoch(2)
            ))
        ))

      val expectedGroup1Response = new OffsetFetchResponseData.OffsetFetchResponseGroup()
        .setGroupId("group-1")
        .setTopics(util.List.of(
          new OffsetFetchResponseData.OffsetFetchResponseTopics()
            .setTopicId(if (version >= 10) fooId else Uuid.ZERO_UUID)
            .setName(if (version < 10) foo else "")
            .setPartitions(util.List.of(
              new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                .setPartitionIndex(0)
                .setCommittedOffset(100)
                .setCommittedLeaderEpoch(1),
              new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                .setPartitionIndex(1)
                .setCommittedOffset(200)
                .setCommittedLeaderEpoch(2)
            ))
        ))

      val group2Response = new OffsetFetchResponseData.OffsetFetchResponseGroup()
        .setGroupId("group-2")
        .setTopics(util.List.of(
          new OffsetFetchResponseData.OffsetFetchResponseTopics()
            .setName(bar)
            .setPartitions(util.List.of(
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
            ))
        ))

      val group3Response = new OffsetFetchResponseData.OffsetFetchResponseGroup()
        .setGroupId("group-3")
        .setErrorCode(Errors.INVALID_GROUP_ID.code)

      val group4Response = new OffsetFetchResponseData.OffsetFetchResponseGroup()
        .setGroupId("group-4")
        .setErrorCode(Errors.INVALID_GROUP_ID.code)

      val expectedGroups = List(expectedGroup1Response, group2Response, group3Response, group4Response)

      group1Future.complete(group1Response)
      group2Future.complete(group2Response)
      group3Future.completeExceptionally(Errors.INVALID_GROUP_ID.exception)
      group4Future.complete(group4Response)

      val response = verifyNoThrottling[OffsetFetchResponse](requestChannelRequest)
      assertEquals(expectedGroups.toSet, response.data.groups.asScala.toSet)
    }
  }

  @ParameterizedTest
  // We only test with topic ids.
  @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH, fromVersion = 10)
  def testHandleOffsetFetchWithUnknownTopicIds(version: Short): Unit = {
    val foo = "foo"
    val bar = "bar"
    val fooId = Uuid.randomUuid()
    val barId = Uuid.randomUuid()
    addTopicToMetadataCache(foo, topicId = fooId, numPartitions = 2)

    def makeRequest(version: Short): RequestChannel.Request = {
      buildRequest(
        OffsetFetchRequest.Builder.forTopicIdsOrNames(
          new OffsetFetchRequestData()
            .setGroups(util.List.of(
              new OffsetFetchRequestData.OffsetFetchRequestGroup()
                .setGroupId("group-1")
                .setTopics(util.List.of(
                  new OffsetFetchRequestData.OffsetFetchRequestTopics()
                    .setName(foo)
                    .setTopicId(fooId)
                    .setPartitionIndexes(util.List.of[Integer](0)),
                  // bar does not exist so it must return UNKNOWN_TOPIC_ID.
                  new OffsetFetchRequestData.OffsetFetchRequestTopics()
                    .setName(bar)
                    .setTopicId(barId)
                    .setPartitionIndexes(util.List.of[Integer](0))
                )),
              new OffsetFetchRequestData.OffsetFetchRequestGroup()
                .setGroupId("group-2")
                .setTopics(null)
            )),
          false
        ).build(version)
      )
    }

    val requestChannelRequest = makeRequest(version)

    val group1Future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
    when(groupCoordinator.fetchOffsets(
      requestChannelRequest.context,
      new OffsetFetchRequestData.OffsetFetchRequestGroup()
        .setGroupId("group-1")
        .setTopics(util.List.of(
          new OffsetFetchRequestData.OffsetFetchRequestTopics()
            .setTopicId(fooId)
            .setName("foo")
            .setPartitionIndexes(util.List.of[Integer](0)))),
      false
    )).thenReturn(group1Future)

    val group2Future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
    when(groupCoordinator.fetchOffsets(
      requestChannelRequest.context,
      new OffsetFetchRequestData.OffsetFetchRequestGroup()
        .setGroupId("group-2")
        .setTopics(null),
      false
    )).thenReturn(group2Future)

    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val group1Response = new OffsetFetchResponseData.OffsetFetchResponseGroup()
      .setGroupId("group-1")
      .setTopics(util.List.of(
        new OffsetFetchResponseData.OffsetFetchResponseTopics()
          .setTopicId(fooId)
          .setName(foo)
          .setPartitions(util.List.of(
            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(0)
              .setCommittedOffset(100)
              .setCommittedLeaderEpoch(1)
          ))
      ))

    val group2Response = new OffsetFetchResponseData.OffsetFetchResponseGroup()
      .setGroupId("group-2")
      .setTopics(util.List.of(
        new OffsetFetchResponseData.OffsetFetchResponseTopics()
          .setName(foo)
          .setPartitions(util.List.of(
            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(0)
              .setCommittedOffset(100)
              .setCommittedLeaderEpoch(1)
          )),
        // bar does not exist so it must be filtered out.
        new OffsetFetchResponseData.OffsetFetchResponseTopics()
          .setName(bar)
          .setPartitions(util.List.of(
            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(0)
              .setCommittedOffset(100)
              .setCommittedLeaderEpoch(1)
          ))
      ))

    val expectedResponse = new OffsetFetchResponseData()
      .setGroups(util.List.of(
        new OffsetFetchResponseData.OffsetFetchResponseGroup()
          .setGroupId("group-1")
          .setTopics(util.List.of(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
              .setTopicId(fooId)
              .setPartitions(util.List.of(
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(0)
                  .setCommittedOffset(100)
                  .setCommittedLeaderEpoch(1)
              )),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
              .setTopicId(barId)
              .setPartitions(util.List.of(
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(0)
                  .setCommittedOffset(-1)
                  .setErrorCode(Errors.UNKNOWN_TOPIC_ID.code)
              ))
          )),
        new OffsetFetchResponseData.OffsetFetchResponseGroup()
          .setGroupId("group-2")
          .setTopics(util.List.of(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
              .setTopicId(fooId)
              .setPartitions(util.List.of(
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(0)
                  .setCommittedOffset(100)
                  .setCommittedLeaderEpoch(1)
              ))
          ))
      ))

    group1Future.complete(group1Response)
    group2Future.complete(group2Response)

    val response = verifyNoThrottling[OffsetFetchResponse](requestChannelRequest)
    assertEquals(expectedResponse, response.data)
  }

  @ParameterizedTest
  // The single group builder does not support topic ids.
  @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH, toVersion = 9)
  def testHandleOffsetFetchWithSingleGroup(version: Short): Unit = {
    def makeRequest(version: Short): RequestChannel.Request = {
      buildRequest(OffsetFetchRequest.Builder.forTopicNames(
        new OffsetFetchRequestData()
          .setRequireStable(false)
          .setGroups(util.List.of(
            new OffsetFetchRequestData.OffsetFetchRequestGroup()
              .setGroupId("group-1")
              .setTopics(util.List.of(
                new OffsetFetchRequestData.OffsetFetchRequestTopics()
                  .setName("foo")
                  .setPartitionIndexes(util.List.of[Integer](0, 1))
              ))
          )),
        false
      ).build(version))
    }

    val requestChannelRequest = makeRequest(version)

    val future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
    when(groupCoordinator.fetchOffsets(
      requestChannelRequest.context,
      new OffsetFetchRequestData.OffsetFetchRequestGroup()
        .setGroupId("group-1")
        .setTopics(util.List.of(new OffsetFetchRequestData.OffsetFetchRequestTopics()
          .setName("foo")
          .setPartitionIndexes(util.List.of[Integer](0, 1)))),
      false
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleOffsetFetchRequest(requestChannelRequest)

    val group1Response = new OffsetFetchResponseData.OffsetFetchResponseGroup()
      .setGroupId("group-1")
      .setTopics(util.List.of(
        new OffsetFetchResponseData.OffsetFetchResponseTopics()
          .setName("foo")
          .setPartitions(util.List.of(
            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(0)
              .setCommittedOffset(100)
              .setCommittedLeaderEpoch(1),
            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(1)
              .setCommittedOffset(200)
              .setCommittedLeaderEpoch(2)
          ))
      ))

    val expectedOffsetFetchResponse = if (version >= 8) {
      new OffsetFetchResponseData()
        .setGroups(util.List.of(group1Response))
    } else {
      new OffsetFetchResponseData()
        .setTopics(util.List.of(
          new OffsetFetchResponseData.OffsetFetchResponseTopic()
            .setName("foo")
            .setPartitions(util.List.of(
              new OffsetFetchResponseData.OffsetFetchResponsePartition()
                .setPartitionIndex(0)
                .setCommittedOffset(100)
                .setCommittedLeaderEpoch(if (version >= 5) 1 else -1),
              new OffsetFetchResponseData.OffsetFetchResponsePartition()
                .setPartitionIndex(1)
                .setCommittedOffset(200)
                .setCommittedLeaderEpoch(if (version >= 5) 2 else -1)
            ))
        ))
    }

    future.complete(group1Response)

    val response = verifyNoThrottling[OffsetFetchResponse](requestChannelRequest)
    assertEquals(expectedOffsetFetchResponse, response.data)
  }

  @ParameterizedTest
  // Version 1 does not support fetching offsets for all topics.
  @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH, fromVersion = 2)
  def testHandleOffsetFetchAllOffsetsWithSingleGroup(version: Short): Unit = {
    val foo = "foo"
    val fooId = Uuid.randomUuid()
    addTopicToMetadataCache(foo, topicId = fooId, numPartitions = 2)

    def makeRequest(version: Short): RequestChannel.Request = {
      buildRequest(OffsetFetchRequest.Builder.forTopicIdsOrNames(
        new OffsetFetchRequestData()
          .setRequireStable(false)
          .setGroups(util.List.of(
            new OffsetFetchRequestData.OffsetFetchRequestGroup()
              .setGroupId("group-1")
              .setTopics(null) // all offsets.
          )),
        false
      ).build(version))
    }

    val requestChannelRequest = makeRequest(version)

    val future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
    when(groupCoordinator.fetchOffsets(
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
      .setTopics(util.List.of(
        new OffsetFetchResponseData.OffsetFetchResponseTopics()
          .setName(foo)
          .setPartitions(util.List.of(
            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(0)
              .setCommittedOffset(100)
              .setCommittedLeaderEpoch(1),
            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(1)
              .setCommittedOffset(200)
              .setCommittedLeaderEpoch(2)
          ))
      ))

    val expectedOffsetFetchResponse = if (version >= 8) {
      new OffsetFetchResponseData()
        .setGroups(util.List.of(
          new OffsetFetchResponseData.OffsetFetchResponseGroup()
            .setGroupId("group-1")
            .setTopics(util.List.of(
              new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName(if (version < 10) foo else "")
                .setTopicId(if (version >= 10) fooId else Uuid.ZERO_UUID)
                .setPartitions(util.List.of(
                  new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                    .setPartitionIndex(0)
                    .setCommittedOffset(100)
                    .setCommittedLeaderEpoch(1),
                  new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                    .setPartitionIndex(1)
                    .setCommittedOffset(200)
                    .setCommittedLeaderEpoch(2)
                ))
            ))
        ))
    } else {
      new OffsetFetchResponseData()
        .setTopics(util.List.of(
          new OffsetFetchResponseData.OffsetFetchResponseTopic()
            .setName("foo")
            .setPartitions(util.List.of(
              new OffsetFetchResponseData.OffsetFetchResponsePartition()
                .setPartitionIndex(0)
                .setCommittedOffset(100)
                .setCommittedLeaderEpoch(if (version >= 5) 1 else -1),
              new OffsetFetchResponseData.OffsetFetchResponsePartition()
                .setPartitionIndex(1)
                .setCommittedOffset(200)
                .setCommittedLeaderEpoch(if (version >= 5) 2 else -1)
            ))
        ))
    }

    future.complete(group1Response)

    val response = verifyNoThrottling[OffsetFetchResponse](requestChannelRequest)
    assertEquals(expectedOffsetFetchResponse, response.data)
  }

  @ParameterizedTest
  // We don't test the non batched API.
  @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH, fromVersion = 8)
  def testHandleOffsetFetchAuthorization(version: Short): Unit = {
    val foo = "foo"
    val bar = "bar"
    val fooId = Uuid.randomUuid()
    val barId = Uuid.randomUuid()
    addTopicToMetadataCache(foo, topicId = fooId, numPartitions = 2)
    addTopicToMetadataCache(bar, topicId = barId, numPartitions = 2)

    def makeRequest(version: Short): RequestChannel.Request = {
      buildRequest(
        OffsetFetchRequest.Builder.forTopicIdsOrNames(
          new OffsetFetchRequestData()
            .setGroups(util.List.of(
              new OffsetFetchRequestData.OffsetFetchRequestGroup()
                .setGroupId("group-1")
                .setTopics(util.List.of(
                  new OffsetFetchRequestData.OffsetFetchRequestTopics()
                    .setName(foo)
                    .setTopicId(fooId)
                    .setPartitionIndexes(util.List.of[Integer](0)),
                  new OffsetFetchRequestData.OffsetFetchRequestTopics()
                    .setName(bar)
                    .setTopicId(barId)
                    .setPartitionIndexes(util.List.of[Integer](0))
                )),
              new OffsetFetchRequestData.OffsetFetchRequestGroup()
                .setGroupId("group-2")
                .setTopics(util.List.of(
                  new OffsetFetchRequestData.OffsetFetchRequestTopics()
                    .setName(foo)
                    .setTopicId(fooId)
                    .setPartitionIndexes(util.List.of[Integer](0)),
                  new OffsetFetchRequestData.OffsetFetchRequestTopics()
                    .setName(bar)
                    .setTopicId(barId)
                    .setPartitionIndexes(util.List.of[Integer](0))
                )),
              new OffsetFetchRequestData.OffsetFetchRequestGroup()
                .setGroupId("group-3")
                .setTopics(null),
              new OffsetFetchRequestData.OffsetFetchRequestGroup()
                .setGroupId("group-4")
                .setTopics(null),
            )),
          false
        ).build(version)
      )
    }

    val requestChannelRequest = makeRequest(version)

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
      actions.stream()
        .map(action => acls.getOrElse(action.resourcePattern.name, AuthorizationResult.DENIED))
        .toList
    }

    // group-1 is allowed and bar is allowed.
    val group1Future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
    when(groupCoordinator.fetchOffsets(
      requestChannelRequest.context,
      new OffsetFetchRequestData.OffsetFetchRequestGroup()
        .setGroupId("group-1")
        .setTopics(util.List.of(new OffsetFetchRequestData.OffsetFetchRequestTopics()
          .setName(bar)
          .setTopicId(if (version >= 10) barId else Uuid.ZERO_UUID)
          .setPartitionIndexes(util.List.of[Integer](0)))),
      false
    )).thenReturn(group1Future)

    // group-3 is allowed and bar is allowed.
    val group3Future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
    when(groupCoordinator.fetchOffsets(
      requestChannelRequest.context,
      new OffsetFetchRequestData.OffsetFetchRequestGroup()
        .setGroupId("group-3")
        .setTopics(null),
      false
    )).thenReturn(group3Future)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val group1ResponseFromCoordinator = new OffsetFetchResponseData.OffsetFetchResponseGroup()
      .setGroupId("group-1")
      .setTopics(util.List.of(
        new OffsetFetchResponseData.OffsetFetchResponseTopics()
          .setName(bar)
          .setTopicId(barId)
          .setPartitions(util.List.of(
            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(0)
              .setCommittedOffset(100)
              .setCommittedLeaderEpoch(1)
          ))
      ))

    val group3ResponseFromCoordinator = new OffsetFetchResponseData.OffsetFetchResponseGroup()
      .setGroupId("group-3")
      .setTopics(util.List.of(
        // foo should be filtered out.
        new OffsetFetchResponseData.OffsetFetchResponseTopics()
          .setName(foo)
          .setTopicId(fooId)
          .setPartitions(util.List.of(
            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(0)
              .setCommittedOffset(100)
              .setCommittedLeaderEpoch(1)
          )),
        new OffsetFetchResponseData.OffsetFetchResponseTopics()
          .setName(bar)
          .setTopicId(barId)
          .setPartitions(util.List.of(
            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(0)
              .setCommittedOffset(100)
              .setCommittedLeaderEpoch(1)
          ))
      ))

    val expectedOffsetFetchResponse = new OffsetFetchResponseData()
      .setGroups(util.List.of(
        // group-1 is authorized but foo is not.
        new OffsetFetchResponseData.OffsetFetchResponseGroup()
          .setGroupId("group-1")
          .setTopics(util.List.of(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
              .setName(if (version < 10) bar else "")
              .setTopicId(if (version >= 10) barId else Uuid.ZERO_UUID)
              .setPartitions(util.List.of(
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(0)
                  .setCommittedOffset(100)
                  .setCommittedLeaderEpoch(1)
              )),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
              .setName(if (version < 10) foo else "")
              .setTopicId(if (version >= 10) fooId else Uuid.ZERO_UUID)
              .setPartitions(util.List.of(
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(0)
                  .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
                  .setCommittedOffset(-1)
              ))
          )),
        // group-2 is not authorized.
        new OffsetFetchResponseData.OffsetFetchResponseGroup()
          .setGroupId("group-2")
          .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code),
        // group-3 is authorized but foo is not.
        new OffsetFetchResponseData.OffsetFetchResponseGroup()
          .setGroupId("group-3")
          .setTopics(util.List.of(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
              .setName(if (version < 10) bar else "")
              .setTopicId(if (version >= 10) barId else Uuid.ZERO_UUID)
              .setPartitions(util.List.of(
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(0)
                  .setCommittedOffset(100)
                  .setCommittedLeaderEpoch(1)
              ))
          )),
        // group-4 is not authorized.
        new OffsetFetchResponseData.OffsetFetchResponseGroup()
          .setGroupId("group-4")
          .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code),
      ))

    group1Future.complete(group1ResponseFromCoordinator)
    group3Future.complete(group3ResponseFromCoordinator)

    val response = verifyNoThrottling[OffsetFetchResponse](requestChannelRequest)
    assertEquals(expectedOffsetFetchResponse, response.data)
  }

  @ParameterizedTest
  // We don't test the non batched API.
  @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH, fromVersion = 8)
  def testHandleOffsetFetchWithUnauthorizedTopicAndTopLevelError(version: Short): Unit = {
    val foo = "foo"
    val bar = "bar"
    val fooId = Uuid.randomUuid()
    val barId = Uuid.randomUuid()
    addTopicToMetadataCache(foo, topicId = fooId, numPartitions = 2)
    addTopicToMetadataCache(bar, topicId = barId, numPartitions = 2)

    def makeRequest(version: Short): RequestChannel.Request = {
      buildRequest(
        OffsetFetchRequest.Builder.forTopicIdsOrNames(
          new OffsetFetchRequestData()
            .setGroups(util.List.of(
              new OffsetFetchRequestData.OffsetFetchRequestGroup()
                .setGroupId("group-1")
                .setTopics(util.List.of(
                  new OffsetFetchRequestData.OffsetFetchRequestTopics()
                    .setName(foo)
                    .setTopicId(fooId)
                    .setPartitionIndexes(util.List.of[Integer](0)),
                  new OffsetFetchRequestData.OffsetFetchRequestTopics()
                    .setName(bar)
                    .setTopicId(barId)
                    .setPartitionIndexes(util.List.of[Integer](0))
                )),
              new OffsetFetchRequestData.OffsetFetchRequestGroup()
                .setGroupId("group-2")
                .setTopics(util.List.of(
                  new OffsetFetchRequestData.OffsetFetchRequestTopics()
                    .setName(foo)
                    .setTopicId(fooId)
                    .setPartitionIndexes(util.List.of[Integer](0)),
                  new OffsetFetchRequestData.OffsetFetchRequestTopics()
                    .setName(bar)
                    .setTopicId(barId)
                    .setPartitionIndexes(util.List.of[Integer](0))
                ))
            )),
          false
        ).build(version)
      )
    }

    val requestChannelRequest = makeRequest(version)

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
      actions.stream()
        .map(action => acls.getOrElse(action.resourcePattern.name, AuthorizationResult.DENIED))
        .toList
    }

    // group-1 and group-2 are allowed and bar is allowed.
    val group1Future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
    when(groupCoordinator.fetchOffsets(
      requestChannelRequest.context,
      new OffsetFetchRequestData.OffsetFetchRequestGroup()
        .setGroupId("group-1")
        .setTopics(util.List.of(new OffsetFetchRequestData.OffsetFetchRequestTopics()
          .setName(bar)
          .setTopicId(if (version >= 10) barId else Uuid.ZERO_UUID)
          .setPartitionIndexes(util.List.of[Integer](0)))),
      false
    )).thenReturn(group1Future)

    val group2Future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
    when(groupCoordinator.fetchOffsets(
      requestChannelRequest.context,
      new OffsetFetchRequestData.OffsetFetchRequestGroup()
        .setGroupId("group-2")
        .setTopics(util.List.of(new OffsetFetchRequestData.OffsetFetchRequestTopics()
          .setName(bar)
          .setTopicId(if (version >= 10) barId else Uuid.ZERO_UUID)
          .setPartitionIndexes(util.List.of[Integer](0)))),
      false
    )).thenReturn(group1Future)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    // group-2 mocks using the new group coordinator.
    // When the coordinator is not active, a response with top-level error code is returned
    // despite that the requested topic is not authorized and fails.
    val group2ResponseFromCoordinator = new OffsetFetchResponseData.OffsetFetchResponseGroup()
      .setGroupId("group-2")
      .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code)

    val expectedOffsetFetchResponse = new OffsetFetchResponseData()
      .setGroups(util.List.of(
        new OffsetFetchResponseData.OffsetFetchResponseGroup()
          .setGroupId("group-1")
          .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code),
        group2ResponseFromCoordinator
      ))

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

    val fetchDataBuilder = util.Map.of(tp0, new FetchRequest.PartitionData(Uuid.ZERO_UUID, 0, 0, Int.MaxValue, Optional.of(leaderEpoch)))
    val fetchData = util.Map.of(tidp0, new FetchRequest.PartitionData(Uuid.ZERO_UUID, 0, 0, Int.MaxValue, Optional.of(leaderEpoch)))
    val fetchFromFollower = buildRequest(new FetchRequest.Builder(
      ApiKeys.FETCH.oldestVersion(), ApiKeys.FETCH.latestVersion(), 1, 1, 1000, 0, fetchDataBuilder).build())

    val records = MemoryRecords.withRecords(Compression.NONE,
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
    val fetchContext = new FullFetchContext(time, new FetchSessionCacheShard(1000, 100),
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

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.LIST_GROUPS)
  def testListGroupsRequest(version: Short): Unit = {
    val listGroupsRequest = new ListGroupsRequestData()
      .setStatesFilter(if (version >= 4) util.List.of("Stable", "Empty") else util.List.of)
      .setTypesFilter(if (version >= 5) util.List.of("classic", "consumer") else util.List.of)

    val requestChannelRequest = buildRequest(new ListGroupsRequest.Builder(listGroupsRequest).build(version))

    val expectedListGroupsRequest = new ListGroupsRequestData()
      .setStatesFilter(if (version >= 4) util.List.of("Stable", "Empty") else util.List.of)
      .setTypesFilter(if (version >= 5) util.List.of("classic", "consumer") else util.List.of)

    val future = new CompletableFuture[ListGroupsResponseData]()
    when(groupCoordinator.listGroups(
      requestChannelRequest.context,
      expectedListGroupsRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleListGroupsRequest(requestChannelRequest)

    val expectedListGroupsResponse = new ListGroupsResponseData()
      .setGroups(util.List.of(
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
      ))

    future.complete(expectedListGroupsResponse)
    val response = verifyNoThrottling[ListGroupsResponse](requestChannelRequest)
    assertEquals(expectedListGroupsResponse, response.data)
  }

  @Test
  def testListGroupsRequestFutureFailed(): Unit = {
    val listGroupsRequest = new ListGroupsRequestData()
      .setStatesFilter(util.List.of("Stable", "Empty"))
      .setTypesFilter(util.List.of("classic", "consumer"))

    val requestChannelRequest = buildRequest(new ListGroupsRequest.Builder(listGroupsRequest).build())

    val expectedListGroupsRequest = new ListGroupsRequestData()
      .setStatesFilter(util.List.of("Stable", "Empty"))
      .setTypesFilter(util.List.of("classic", "consumer"))

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
    val endpoints = new BrokerEndpointCollection()
    endpoints.add(
      new BrokerEndpoint()
        .setHost("broker0")
        .setPort(9092)
        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
        .setName(plaintextListener.value)
    )
    endpoints.add(
      new BrokerEndpoint()
        .setHost("broker1")
        .setPort(9092)
        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
        .setName(plaintextListener.value)
    )

    MetadataCacheTest.updateCache(metadataCache,
      Seq(new RegisterBrokerRecord()
        .setBrokerId(brokerId)
        .setRack("rack")
        .setFenced(false)
        .setEndPoints(endpoints)))

    val describeClusterRequest = new DescribeClusterRequest.Builder(new DescribeClusterRequestData()
      .setIncludeClusterAuthorizedOperations(true)).build()

    val request = buildRequest(describeClusterRequest, plaintextListener)
    kafkaApis = createKafkaApis()
    kafkaApis.handleDescribeCluster(request)

    val describeClusterResponse = verifyNoThrottling[DescribeClusterResponse](request)

    assertEquals(clusterId, describeClusterResponse.data.clusterId)
    assertEquals(8096, describeClusterResponse.data.clusterAuthorizedOperations)
    assertEquals(util.Set.copyOf(metadataCache.getAliveBrokerNodes(plaintextListener)),
      util.Set.copyOf(describeClusterResponse.nodes.values))
  }

  /**
   * Return pair of listener names in the metadataCache: PLAINTEXT and LISTENER2 respectively.
   */
  private def updateMetadataCacheWithInconsistentListeners(): (ListenerName, ListenerName) = {
    val plaintextListener = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val anotherListener = new ListenerName("LISTENER2")

    val endpoints0 = new BrokerEndpointCollection()
    endpoints0.add(
      new BrokerEndpoint()
        .setHost("broker0")
        .setPort(9092)
        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
        .setName(plaintextListener.value)
    )
    endpoints0.add(
      new BrokerEndpoint()
        .setHost("broker0")
        .setPort(9093)
        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
        .setName(anotherListener.value)
    )

    val endpoints1 = new BrokerEndpointCollection()
    endpoints1.add(
      new BrokerEndpoint()
        .setHost("broker1")
        .setPort(9092)
        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
        .setName(plaintextListener.value)
    )

    MetadataCacheTest.updateCache(metadataCache,
      Seq(new RegisterBrokerRecord().setBrokerId(0).setRack("rack").setFenced(false).setEndPoints(endpoints0),
      new RegisterBrokerRecord().setBrokerId(1).setRack("rack").setFenced(false).setEndPoints(endpoints1))
    )

    (plaintextListener, anotherListener)
  }

  private def sendMetadataRequestWithInconsistentListeners(requestListener: ListenerName): MetadataResponse = {
    val metadataRequest = MetadataRequest.Builder.allTopics.build()
    val requestChannelRequest = buildRequest(metadataRequest, requestListener)
    kafkaApis = createKafkaApis()
    kafkaApis.handleTopicMetadataRequest(requestChannelRequest)

    verifyNoThrottling[MetadataResponse](requestChannelRequest)
  }

  private def testConsumerListOffsetWithUnsupportedVersion(timestamp: Long, version: Short): Unit = {
    val tp = new TopicPartition("foo", 0)
    val targetTimes = util.List.of(new ListOffsetsTopic()
      .setName(tp.topic)
      .setPartitions(util.List.of(new ListOffsetsPartition()
        .setPartitionIndex(tp.partition)
        .setTimestamp(timestamp))))

    when(replicaManager.fetchOffset(
      ArgumentMatchers.any[Seq[ListOffsetsTopic]](),
      ArgumentMatchers.eq(Set.empty[TopicPartition]),
      ArgumentMatchers.eq(IsolationLevel.READ_UNCOMMITTED),
      ArgumentMatchers.eq(ListOffsetsRequest.CONSUMER_REPLICA_ID),
      ArgumentMatchers.eq[String](clientId),
      ArgumentMatchers.anyInt(), // correlationId
      ArgumentMatchers.anyShort(), // version
      ArgumentMatchers.any[(Errors, ListOffsetsPartition) => ListOffsetsPartitionResponse](),
      ArgumentMatchers.any[Consumer[util.Collection[ListOffsetsTopicResponse]]],
      ArgumentMatchers.anyInt() // timeoutMs
    )).thenAnswer(ans => {
      val version = ans.getArgument[Short](6)
      val callback = ans.getArgument[Consumer[util.List[ListOffsetsTopicResponse]]](8)
      val errorCode = if (ReplicaManager.isListOffsetsTimestampUnsupported(timestamp, version))
        Errors.UNSUPPORTED_VERSION.code()
      else
        Errors.INVALID_REQUEST.code()
      val partitionResponse = new ListOffsetsPartitionResponse()
        .setErrorCode(errorCode)
        .setOffset(ListOffsetsResponse.UNKNOWN_OFFSET)
        .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP)
        .setPartitionIndex(tp.partition())
      callback.accept(util.List.of(new ListOffsetsTopicResponse().setName(tp.topic()).setPartitions(util.List.of(partitionResponse))))
    })

    val data = new ListOffsetsRequestData().setTopics(targetTimes).setReplicaId(ListOffsetsRequest.CONSUMER_REPLICA_ID)
    val listOffsetRequest = ListOffsetsRequest.parse(MessageUtil.toByteBufferAccessor(data, version), version)
    val request = buildRequest(listOffsetRequest)

    kafkaApis = createKafkaApis()
    kafkaApis.handleListOffsetRequest(request)

    val response = verifyNoThrottling[ListOffsetsResponse](request)
    val partitionDataOptional = response.topics.asScala.find(_.name == tp.topic).get
      .partitions.asScala.find(_.partitionIndex == tp.partition)
    assertTrue(partitionDataOptional.isDefined)

    val partitionData = partitionDataOptional.get
    assertEquals(Errors.UNSUPPORTED_VERSION.code, partitionData.errorCode)
  }

  private def testConsumerListOffsetLatest(isolationLevel: IsolationLevel): Unit = {
    val tp = new TopicPartition("foo", 0)
    val latestOffset = 15L

    val targetTimes = util.List.of(new ListOffsetsTopic()
      .setName(tp.topic)
      .setPartitions(util.List.of(new ListOffsetsPartition()
        .setPartitionIndex(tp.partition)
        .setTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP))))

    when(replicaManager.fetchOffset(
      ArgumentMatchers.eq(targetTimes.asScala.toSeq),
      ArgumentMatchers.eq(Set.empty[TopicPartition]),
      ArgumentMatchers.eq(isolationLevel),
      ArgumentMatchers.eq(ListOffsetsRequest.CONSUMER_REPLICA_ID),
      ArgumentMatchers.eq[String](clientId),
      ArgumentMatchers.anyInt(), // correlationId
      ArgumentMatchers.anyShort(), // version
      ArgumentMatchers.any[(Errors, ListOffsetsPartition) => ListOffsetsPartitionResponse](),
      ArgumentMatchers.any[Consumer[util.Collection[ListOffsetsTopicResponse]]],
      ArgumentMatchers.anyInt() // timeoutMs
    )).thenAnswer(ans => {
      val callback = ans.getArgument[Consumer[util.List[ListOffsetsTopicResponse]]](8)
      val partitionResponse = new ListOffsetsPartitionResponse()
        .setErrorCode(Errors.NONE.code())
        .setOffset(latestOffset)
        .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP)
        .setPartitionIndex(tp.partition())
      callback.accept(util.List.of(new ListOffsetsTopicResponse().setName(tp.topic()).setPartitions(util.List.of(partitionResponse))))
    })

    val listOffsetRequest = ListOffsetsRequest.Builder.forConsumer(true, isolationLevel)
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
    val writeTxnMarkersRequest = new WriteTxnMarkersRequest.Builder(
      util.List.of(new TxnMarkerEntry(1, 1.toShort, 0, TransactionResult.COMMIT, partitions, TransactionVersion.TV_1.featureLevel()))).build()
    (writeTxnMarkersRequest, buildRequest(writeTxnMarkersRequest))
  }

  private def buildRequest(request: AbstractRequest,
                           listenerName: ListenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
                           fromPrivilegedListener: Boolean = false,
                           requestHeader: Option[RequestHeader] = None,
                           requestMetrics: RequestChannelMetrics = requestChannelMetrics): RequestChannel.Request = {
    buildRequest(request, new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice"), InetAddress.getLocalHost, listenerName,
      fromPrivilegedListener, requestHeader, requestMetrics)
  }

    private def buildRequest(request: AbstractRequest,
                             principal: KafkaPrincipal,
                             clientAddress: InetAddress,
                             listenerName: ListenerName,
                             fromPrivilegedListener: Boolean,
                             requestHeader: Option[RequestHeader],
                             requestMetrics: RequestChannelMetrics): RequestChannel.Request = {
    val buffer = request.serializeWithHeader(
      requestHeader.getOrElse(new RequestHeader(request.apiKey, request.version, clientId, 0)))

    // read the header from the buffer first so that the body can be read next from the Request constructor
    val header = RequestHeader.parse(buffer)
    // DelegationTokens require the context authenticated to be non SecurityProtocol.PLAINTEXT
    // and have a non KafkaPrincipal.ANONYMOUS principal. This test is done before the check
    // for forwarding because after forwarding the context will have a different context.
    // We validate the context authenticated failure case in other integration tests.
    val context = new RequestContext(header, "1", clientAddress, Optional.empty(),
      principal, listenerName, SecurityProtocol.SSL,
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
    val readable = MessageUtil.toByteBufferAccessor(
      response.data,
      request.context.header.apiVersion
    )
    AbstractResponse.parseResponse(
      request.context.header.apiKey,
      readable,
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
    val readable = MessageUtil.toByteBufferAccessor(
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
      readable,
      request.context.header.apiVersion,
    ).asInstanceOf[T]
  }

  private def createBasicMetadata(topic: String,
                                  numPartitions: Int,
                                  brokerEpoch: Long,
                                  numBrokers: Int,
                                  topicId: Uuid): Seq[ApiMessage] = {

    val results = new mutable.ArrayBuffer[ApiMessage]()
    val topicRecord = new TopicRecord().setName(topic).setTopicId(topicId)
    results += topicRecord

    val replicas = util.List.of(0.asInstanceOf[Integer])

    def createPartitionRecord(partition: Int) = new PartitionRecord()
      .setTopicId(topicId)
      .setPartitionId(partition)
      .setLeader(0)
      .setLeaderEpoch(1)
      .setReplicas(replicas)
      .setIsr(replicas)

    val plaintextListener = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val partitionRecords = (0 until numPartitions).map(createPartitionRecord)
    val liveBrokers = (0 until numBrokers).map(
      brokerId => createMetadataBroker(brokerId, plaintextListener, brokerEpoch))
    partitionRecords.foreach(record => results += record)
    liveBrokers.foreach(record => results +=record)

    results.toSeq
  }

  private def setupBasicMetadataCache(topic: String, numPartitions: Int, numBrokers: Int, topicId: Uuid): Unit = {
    val updateMetadata = createBasicMetadata(topic, numPartitions, 0, numBrokers, topicId)
    MetadataCacheTest.updateCache(metadataCache, updateMetadata)
  }

  private def addTopicToMetadataCache(topic: String, numPartitions: Int, numBrokers: Int = 1, topicId: Uuid = Uuid.ZERO_UUID): Unit = {
    val updateMetadata = createBasicMetadata(topic, numPartitions, 0, numBrokers, topicId)
    MetadataCacheTest.updateCache(metadataCache, updateMetadata)
  }

  private def createMetadataBroker(brokerId: Int,
                                   listener: ListenerName,
                                   brokerEpoch: Long): RegisterBrokerRecord = {
    val endpoints = new BrokerEndpointCollection()
    endpoints.add(
      new BrokerEndpoint()
        .setHost("broker" + brokerId)
        .setPort(9092)
        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
        .setName(listener.value)
    )

    new RegisterBrokerRecord()
      .setBrokerId(brokerId)
      .setRack("rack")
      .setFenced(false)
      .setEndPoints(endpoints)
      .setBrokerEpoch(brokerEpoch)
  }

  @Test
  def testAlterReplicaLogDirs(): Unit = {
    val data = new AlterReplicaLogDirsRequestData()
    val dir = new AlterReplicaLogDirsRequestData.AlterReplicaLogDir()
      .setPath("/foo")
    dir.topics().add(new AlterReplicaLogDirsRequestData.AlterReplicaLogDirTopic().setName("t0").setPartitions(util.List.of(0, 1, 2)))
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
    assertEquals(util.Map.of(Errors.NONE, 1,
      Errors.LOG_DIR_NOT_FOUND, 1,
      Errors.INVALID_TOPIC_EXCEPTION, 1), response.errorCounts)
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
            .setRecords(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(100, raw.getBytes(StandardCharsets.UTF_8))))
      }.toMap.asJava)

      data.foreach{case (tp, _) =>
        topicIds.put(tp.topicPartition.topic, tp.topicId)
        topicNames.put(tp.topicId, tp.topicPartition.topic)
      }
      FetchResponse.of(Errors.NONE, 100, 100, responseData, List.empty.asJava)
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
    val data = new DescribeProducersRequestData().setTopics(util.List.of(
      new DescribeProducersRequestData.TopicRequest()
        .setName(tp1.topic)
        .setPartitionIndexes(util.List.of(Int.box(tp1.partition))),
      new DescribeProducersRequestData.TopicRequest()
        .setName(tp2.topic)
        .setPartitionIndexes(util.List.of(Int.box(tp2.partition))),
      new DescribeProducersRequestData.TopicRequest()
        .setName(tp3.topic)
        .setPartitionIndexes(util.List.of(Int.box(tp3.partition))),
      new DescribeProducersRequestData.TopicRequest()
        .setName(tp4.topic)
        .setPartitionIndexes(util.List.of(Int.box(tp4.partition)))
    ))

    def buildExpectedActions(topic: String): util.List[Action] = {
      val pattern = new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL)
      val action = new Action(AclOperation.READ, pattern, 1, true, true)
      util.List.of(action)
    }

    // Topic `foo` is authorized and present in the metadata
    addTopicToMetadataCache(tp1.topic, 4) // We will only access the first topic
    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(buildExpectedActions(tp1.topic))))
      .thenReturn(util.List.of(AuthorizationResult.ALLOWED))

    // Topic `bar` is not authorized
    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(buildExpectedActions(tp2.topic))))
      .thenReturn(util.List.of(AuthorizationResult.DENIED))

    // Topic `baz` is authorized, but not present in the metadata
    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(buildExpectedActions(tp3.topic))))
      .thenReturn(util.List.of(AuthorizationResult.ALLOWED))

    when(replicaManager.activeProducerState(tp1))
      .thenReturn(new DescribeProducersResponseData.PartitionResponse()
        .setErrorCode(Errors.NONE.code)
        .setPartitionIndex(tp1.partition)
        .setActiveProducers(util.List.of(
          new DescribeProducersResponseData.ProducerState()
            .setProducerId(12345L)
            .setProducerEpoch(15)
            .setLastSequence(100)
            .setLastTimestamp(time.milliseconds())
            .setCurrentTxnStartOffset(-1)
            .setCoordinatorEpoch(200)
        )))

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
      .setTransactionalIds(util.List.of("foo", "bar"))
    val describeTransactionsRequest = new DescribeTransactionsRequest.Builder(data).build()
    val request = buildRequest(describeTransactionsRequest)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    def buildExpectedActions(transactionalId: String): util.List[Action] = {
      val pattern = new ResourcePattern(ResourceType.TRANSACTIONAL_ID, transactionalId, PatternType.LITERAL)
      val action = new Action(AclOperation.DESCRIBE, pattern, 1, true, true)
      util.List.of(action)
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
      .thenReturn(util.List.of(AuthorizationResult.ALLOWED))

    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(buildExpectedActions("bar"))))
      .thenReturn(util.List.of(AuthorizationResult.DENIED))
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
      .setTransactionalIds(util.List.of(transactionalId))
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
      val actions = util.List.of(action)

      when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(actions)))
        .thenReturn(util.List.of(result))
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

    when(txnCoordinator.handleListTransactions(Set.empty[Long], Set.empty[String], -1L, null))
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
    val data = new ListTransactionsRequestData().setTransactionalIdPattern("my.*")
    val listTransactionsRequest = new ListTransactionsRequest.Builder(data).build()
    val request = buildRequest(listTransactionsRequest)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    val transactionStates = new util.ArrayList[ListTransactionsResponseData.TransactionState]()
    transactionStates.add(new ListTransactionsResponseData.TransactionState()
      .setTransactionalId("myFoo")
      .setProducerId(12345L)
      .setTransactionState("Ongoing"))
    transactionStates.add(new ListTransactionsResponseData.TransactionState()
      .setTransactionalId("myBar")
      .setProducerId(98765)
      .setTransactionState("PrepareAbort"))

    when(txnCoordinator.handleListTransactions(Set.empty[Long], Set.empty[String], -1L, "my.*"))
      .thenReturn(new ListTransactionsResponseData()
        .setErrorCode(Errors.NONE.code)
        .setTransactionStates(transactionStates))

    def buildExpectedActions(transactionalId: String): util.List[Action] = {
      val pattern = new ResourcePattern(ResourceType.TRANSACTIONAL_ID, transactionalId, PatternType.LITERAL)
      val action = new Action(AclOperation.DESCRIBE, pattern, 1, true, true)
      util.List.of(action)
    }

    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(buildExpectedActions("myFoo"))))
      .thenReturn(util.List.of(AuthorizationResult.ALLOWED))

    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(buildExpectedActions("myBar"))))
      .thenReturn(util.List.of(AuthorizationResult.DENIED))
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleListTransactionsRequest(request)

    val response = verifyNoThrottling[ListTransactionsResponse](request)
    assertEquals(1, response.data.transactionStates.size())
    val transactionState = response.data.transactionStates.get(0)
    assertEquals("myFoo", transactionState.transactionalId)
    assertEquals(12345L, transactionState.producerId)
    assertEquals("Ongoing", transactionState.transactionState)
  }

  @Test
  def testEmptyLegacyAlterConfigsRequestWithKRaft(): Unit = {
    val request = buildRequest(new AlterConfigsRequest(new AlterConfigsRequestData(), 1.toShort))
    metadataCache = new KRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    kafkaApis = createKafkaApis()
    kafkaApis.handleAlterConfigsRequest(request)
    val response = verifyNoThrottling[AlterConfigsResponse](request)
    assertEquals(new AlterConfigsResponseData(), response.data())
  }

  @Test
  def testInvalidLegacyAlterConfigsRequestWithKRaft(): Unit = {
    val request = buildRequest(new AlterConfigsRequest(new AlterConfigsRequestData().
      setValidateOnly(true).
      setResources(new LAlterConfigsResourceCollection(util.List.of(
        new LAlterConfigsResource().
          setResourceName(brokerId.toString).
          setResourceType(BROKER.id()).
          setConfigs(new LAlterableConfigCollection(util.List.of(new LAlterableConfig().
            setName("foo").
            setValue(null)).iterator()))).iterator())), 1.toShort))
    metadataCache = new KRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    kafkaApis = createKafkaApis()
    kafkaApis.handleAlterConfigsRequest(request)
    val response = verifyNoThrottling[AlterConfigsResponse](request)
    assertEquals(new AlterConfigsResponseData().setResponses(util.List.of(
      new LAlterConfigsResourceResponse().
        setErrorCode(Errors.INVALID_REQUEST.code()).
        setErrorMessage("Null value not supported for : foo").
        setResourceName(brokerId.toString).
        setResourceType(BROKER.id()))),
      response.data())
  }

  @Test
  def testEmptyIncrementalAlterConfigsRequestWithKRaft(): Unit = {
    val alterConfigsRequest = new IncrementalAlterConfigsRequest(new IncrementalAlterConfigsRequestData(), 1.toShort)
    assertEquals(
      "IncrementalAlterConfigsRequestData(resources=[], validateOnly=false)",
      alterConfigsRequest.toString
    )
    val request = buildRequest(alterConfigsRequest)
    metadataCache = new KRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    kafkaApis = createKafkaApis()
    kafkaApis.handleIncrementalAlterConfigsRequest(request)
    val response = verifyNoThrottling[IncrementalAlterConfigsResponse](request)
    assertEquals(new IncrementalAlterConfigsResponseData(), response.data())
  }

  @Test
  def testLog4jIncrementalAlterConfigsRequestWithKRaft(): Unit = {
    val alterConfigsRequest = new IncrementalAlterConfigsRequest(new IncrementalAlterConfigsRequestData().
      setValidateOnly(true).
      setResources(new IAlterConfigsResourceCollection(util.List.of(new IAlterConfigsResource().
        setResourceName(brokerId.toString).
        setResourceType(BROKER_LOGGER.id()).
        setConfigs(new IAlterableConfigCollection(util.List.of(new IAlterableConfig().
          setName(LoggingController.ROOT_LOGGER).
          setValue("TRACE")).iterator()))).iterator())), 1.toShort)
    assertEquals(
      "IncrementalAlterConfigsRequestData(resources=[" +
        "AlterConfigsResource(resourceType=" + BROKER_LOGGER.id() + ", " +
        "resourceName='"+ brokerId + "', " +
        "configs=[AlterableConfig(name='" + LoggingController.ROOT_LOGGER + "', configOperation=0, value='REDACTED')])], " +
        "validateOnly=true)",
      alterConfigsRequest.toString
    )
    val request = buildRequest(alterConfigsRequest)
    metadataCache = new KRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    kafkaApis = createKafkaApis()
    kafkaApis.handleIncrementalAlterConfigsRequest(request)
    val response = verifyNoThrottling[IncrementalAlterConfigsResponse](request)
    assertEquals(new IncrementalAlterConfigsResponseData().setResponses(util.List.of(
      new IAlterConfigsResourceResponse().
        setErrorCode(0.toShort).
        setErrorMessage(null).
        setResourceName(brokerId.toString).
        setResourceType(BROKER_LOGGER.id()))),
      response.data())
  }

  @Test
  def testConsumerGroupHeartbeatReturnsUnsupportedVersion(): Unit = {
    val consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new ConsumerGroupHeartbeatRequest.Builder(consumerGroupHeartbeatRequest).build())
    metadataCache = {
      val cache = new KRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_1)
      val delta = new MetadataDelta(MetadataImage.EMPTY)
      delta.replay(new FeatureLevelRecord()
        .setName(MetadataVersion.FEATURE_NAME)
        .setFeatureLevel(MetadataVersion.MINIMUM_VERSION.featureLevel())
      )
      cache.setImage(delta.apply(MetadataProvenance.EMPTY))
      cache
    }
    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val expectedHeartbeatResponse = new ConsumerGroupHeartbeatResponseData()
      .setErrorCode(Errors.UNSUPPORTED_VERSION.code)
    val response = verifyNoThrottling[ConsumerGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(expectedHeartbeatResponse, response.data)
  }

  @Test
  def testConsumerGroupHeartbeatRequest(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])

    val consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new ConsumerGroupHeartbeatRequest.Builder(consumerGroupHeartbeatRequest).build())

    val future = new CompletableFuture[ConsumerGroupHeartbeatResponseData]()
    when(groupCoordinator.consumerGroupHeartbeat(
      requestChannelRequest.context,
      consumerGroupHeartbeatRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis(
      featureVersions = Seq(GroupVersion.GV_1)
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val consumerGroupHeartbeatResponse = new ConsumerGroupHeartbeatResponseData()
      .setMemberId("member")

    future.complete(consumerGroupHeartbeatResponse)
    val response = verifyNoThrottling[ConsumerGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(consumerGroupHeartbeatResponse, response.data)
  }

  @Test
  def testConsumerGroupHeartbeatRequestFutureFailed(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])

    val consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new ConsumerGroupHeartbeatRequest.Builder(consumerGroupHeartbeatRequest).build())

    val future = new CompletableFuture[ConsumerGroupHeartbeatResponseData]()
    when(groupCoordinator.consumerGroupHeartbeat(
      requestChannelRequest.context,
      consumerGroupHeartbeatRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis(
      featureVersions = Seq(GroupVersion.GV_1)
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    future.completeExceptionally(Errors.FENCED_MEMBER_EPOCH.exception)
    val response = verifyNoThrottling[ConsumerGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.FENCED_MEMBER_EPOCH.code, response.data.errorCode)
  }

  @Test
  def testConsumerGroupHeartbeatRequestGroupAuthorizationFailed(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])

    val consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new ConsumerGroupHeartbeatRequest.Builder(consumerGroupHeartbeatRequest).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(util.List.of(AuthorizationResult.DENIED))
    kafkaApis = createKafkaApis(
      authorizer = Some(authorizer),
      featureVersions = Seq(GroupVersion.GV_1)
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val response = verifyNoThrottling[ConsumerGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, response.data.errorCode)
  }

  @Test
  def testConsumerGroupHeartbeatRequestTopicAuthorizationFailed(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])
    val groupId = "group"
    val fooTopicName = "foo"
    val barTopicName = "bar"
    val zarTopicName = "zar"

    val consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequestData()
      .setGroupId(groupId)
      .setSubscribedTopicNames(util.List.of(fooTopicName, barTopicName, zarTopicName))

    val requestChannelRequest = buildRequest(new ConsumerGroupHeartbeatRequest.Builder(consumerGroupHeartbeatRequest).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    val acls = Map(
      groupId -> AuthorizationResult.ALLOWED,
      fooTopicName -> AuthorizationResult.ALLOWED,
      barTopicName -> AuthorizationResult.DENIED,
    )
    when(authorizer.authorize(
      any[RequestContext],
      any[util.List[Action]]
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      actions.stream()
        .map(action => acls.getOrElse(action.resourcePattern.name, AuthorizationResult.DENIED))
        .toList
    }

    kafkaApis = createKafkaApis(
      authorizer = Some(authorizer),
      featureVersions = Seq(GroupVersion.GV_1)
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val response = verifyNoThrottling[ConsumerGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, response.data.errorCode)
  }

  @Test
  def testStreamsGroupHeartbeatReturnsUnsupportedVersion(): Unit = {
    val streamsGroupHeartbeatRequest = new StreamsGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new StreamsGroupHeartbeatRequest.Builder(streamsGroupHeartbeatRequest).build())
    metadataCache = {
      val cache = new KRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_1)
      val delta = new MetadataDelta(MetadataImage.EMPTY)
      delta.replay(new FeatureLevelRecord()
        .setName(MetadataVersion.FEATURE_NAME)
        .setFeatureLevel(MetadataVersion.MINIMUM_VERSION.featureLevel())
      )
      cache.setImage(delta.apply(MetadataProvenance.EMPTY))
      cache
    }
    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val expectedHeartbeatResponse = new StreamsGroupHeartbeatResponseData()
      .setErrorCode(Errors.UNSUPPORTED_VERSION.code)
    val response = verifyNoThrottling[StreamsGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(expectedHeartbeatResponse, response.data)
  }

  @Test
  def testStreamsGroupHeartbeatRequest(): Unit = {
    val features = mock(classOf[FinalizedFeatures])
    when(features.finalizedFeatures()).thenReturn(util.Map.of(StreamsVersion.FEATURE_NAME, 1.toShort))

    metadataCache = mock(classOf[KRaftMetadataCache])
    when(metadataCache.features()).thenReturn(features)

    val streamsGroupHeartbeatRequest = new StreamsGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new StreamsGroupHeartbeatRequest.Builder(streamsGroupHeartbeatRequest).build())

    val future = new CompletableFuture[StreamsGroupHeartbeatResult]()
    when(groupCoordinator.streamsGroupHeartbeat(
      requestChannelRequest.context,
      streamsGroupHeartbeatRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val streamsGroupHeartbeatResponse = new StreamsGroupHeartbeatResponseData()
      .setMemberId("member")

    future.complete(new StreamsGroupHeartbeatResult(streamsGroupHeartbeatResponse, util.Map.of()))
    val response = verifyNoThrottling[StreamsGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(streamsGroupHeartbeatResponse, response.data)
  }

  @Test
  def testStreamsGroupHeartbeatRequestWithAuthorizedTopology(): Unit = {
    val features = mock(classOf[FinalizedFeatures])
    when(features.finalizedFeatures()).thenReturn(util.Map.of(StreamsVersion.FEATURE_NAME, 1.toShort))

    metadataCache = mock(classOf[KRaftMetadataCache])
    when(metadataCache.features()).thenReturn(features)

    val groupId = "group"
    val fooTopicName = "foo"
    val barTopicName = "bar"
    val zarTopicName = "zar"
    val tarTopicName = "tar"
    val booTopicName = "boo"

    val streamsGroupHeartbeatRequest = new StreamsGroupHeartbeatRequestData().setGroupId(groupId).setTopology(
      new StreamsGroupHeartbeatRequestData.Topology()
        .setEpoch(3)
        .setSubtopologies(
          util.List.of(
            new StreamsGroupHeartbeatRequestData.Subtopology().setSubtopologyId("subtopology1")
              .setSourceTopics(util.List.of(fooTopicName))
              .setRepartitionSinkTopics(util.List.of(barTopicName))
              .setStateChangelogTopics(util.List.of(new StreamsGroupHeartbeatRequestData.TopicInfo().setName(tarTopicName))),
            new StreamsGroupHeartbeatRequestData.Subtopology().setSubtopologyId("subtopology2")
              .setSourceTopics(util.List.of(zarTopicName))
              .setRepartitionSourceTopics(util.List.of(new StreamsGroupHeartbeatRequestData.TopicInfo().setName(barTopicName)))
          )
        )
    )

    val requestChannelRequest = buildRequest(new StreamsGroupHeartbeatRequest.Builder(streamsGroupHeartbeatRequest).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    val acls = Map(
      groupId -> AuthorizationResult.ALLOWED,
      fooTopicName -> AuthorizationResult.ALLOWED,
      barTopicName -> AuthorizationResult.ALLOWED,
      zarTopicName -> AuthorizationResult.ALLOWED,
      tarTopicName -> AuthorizationResult.ALLOWED,
      booTopicName -> AuthorizationResult.ALLOWED
    )
    when(authorizer.authorize(
      any[RequestContext],
      any[util.List[Action]]
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      actions.stream()
        .map(action => acls.getOrElse(action.resourcePattern.name, AuthorizationResult.DENIED))
        .toList
    }

    val future = new CompletableFuture[StreamsGroupHeartbeatResult]()
    when(groupCoordinator.streamsGroupHeartbeat(
      requestChannelRequest.context,
      streamsGroupHeartbeatRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis(
      authorizer = Some(authorizer)
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val streamsGroupHeartbeatResponse = new StreamsGroupHeartbeatResponseData()
      .setMemberId("member")

    future.complete(new StreamsGroupHeartbeatResult(streamsGroupHeartbeatResponse, util.Map.of()))
    val response = verifyNoThrottling[StreamsGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(streamsGroupHeartbeatResponse, response.data)
  }

  @Test
  def testStreamsGroupHeartbeatRequestFutureFailed(): Unit = {
    val features = mock(classOf[FinalizedFeatures])
    when(features.finalizedFeatures()).thenReturn(util.Map.of(StreamsVersion.FEATURE_NAME, 1.toShort))

    metadataCache = mock(classOf[KRaftMetadataCache])
    when(metadataCache.features()).thenReturn(features)

    val streamsGroupHeartbeatRequest = new StreamsGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new StreamsGroupHeartbeatRequest.Builder(streamsGroupHeartbeatRequest).build())

    val future = new CompletableFuture[StreamsGroupHeartbeatResult]()
    when(groupCoordinator.streamsGroupHeartbeat(
      requestChannelRequest.context,
      streamsGroupHeartbeatRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    future.completeExceptionally(Errors.FENCED_MEMBER_EPOCH.exception)
    val response = verifyNoThrottling[StreamsGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.FENCED_MEMBER_EPOCH.code, response.data.errorCode)
  }

  @Test
  def testStreamsGroupHeartbeatRequestGroupAuthorizationFailed(): Unit = {
    val features = mock(classOf[FinalizedFeatures])
    when(features.finalizedFeatures()).thenReturn(util.Map.of(StreamsVersion.FEATURE_NAME, 1.toShort))

    metadataCache = mock(classOf[KRaftMetadataCache])
    when(metadataCache.features()).thenReturn(features)

    val streamsGroupHeartbeatRequest = new StreamsGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new StreamsGroupHeartbeatRequest.Builder(streamsGroupHeartbeatRequest).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(util.List.of(AuthorizationResult.DENIED))
    kafkaApis = createKafkaApis(
      authorizer = Some(authorizer)
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val response = verifyNoThrottling[StreamsGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, response.data.errorCode)
  }

  @Test
  def testStreamsGroupHeartbeatRequestTopicAuthorizationFailed(): Unit = {
    val features = mock(classOf[FinalizedFeatures])
    when(features.finalizedFeatures()).thenReturn(util.Map.of(StreamsVersion.FEATURE_NAME, 1.toShort))

    metadataCache = mock(classOf[KRaftMetadataCache])
    when(metadataCache.features()).thenReturn(features)

    val groupId = "group"
    val fooTopicName = "foo"
    val barTopicName = "bar"
    val zarTopicName = "zar"
    val tarTopicName = "tar"

    val streamsGroupHeartbeatRequest = new StreamsGroupHeartbeatRequestData().setGroupId(groupId).setTopology(
      new StreamsGroupHeartbeatRequestData.Topology()
        .setEpoch(3)
        .setSubtopologies(
          util.List.of(new StreamsGroupHeartbeatRequestData.Subtopology().setSubtopologyId("subtopology")
            .setSourceTopics(util.List.of(fooTopicName))
            .setRepartitionSinkTopics(util.List.of(barTopicName))
            .setRepartitionSourceTopics(util.List.of(new StreamsGroupHeartbeatRequestData.TopicInfo().setName(zarTopicName)))
            .setStateChangelogTopics(util.List.of(new StreamsGroupHeartbeatRequestData.TopicInfo().setName(tarTopicName)))
          )
        )
    )

    val requestChannelRequest = buildRequest(new StreamsGroupHeartbeatRequest.Builder(streamsGroupHeartbeatRequest).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    val acls = Map(
      groupId -> AuthorizationResult.ALLOWED,
      fooTopicName -> AuthorizationResult.ALLOWED,
      barTopicName -> AuthorizationResult.DENIED,
      zarTopicName -> AuthorizationResult.ALLOWED,
      tarTopicName -> AuthorizationResult.ALLOWED
    )
    when(authorizer.authorize(
      any[RequestContext],
      any[util.List[Action]]
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      actions.stream()
        .map(action => acls.getOrElse(action.resourcePattern.name, AuthorizationResult.DENIED))
        .toList
    }

    kafkaApis = createKafkaApis(
      authorizer = Some(authorizer)
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val response = verifyNoThrottling[StreamsGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, response.data.errorCode)
  }

  @Test
  def testStreamsGroupHeartbeatRequestProtocolDisabledViaConfig(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])

    val streamsGroupHeartbeatRequest = new StreamsGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new StreamsGroupHeartbeatRequest.Builder(streamsGroupHeartbeatRequest).build())

    kafkaApis = createKafkaApis(
      overrideProperties = Map(GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG -> "classic,consumer")
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val response = verifyNoThrottling[StreamsGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.UNSUPPORTED_VERSION.code, response.data.errorCode)
  }

  @Test
  def testStreamsGroupHeartbeatRequestProtocolDisabledViaFeature(): Unit = {
    val features = mock(classOf[FinalizedFeatures])
    when(features.finalizedFeatures()).thenReturn(util.Map.of(StreamsVersion.FEATURE_NAME, 0.toShort))

    metadataCache = mock(classOf[KRaftMetadataCache])
    when(metadataCache.features()).thenReturn(features)

    val streamsGroupHeartbeatRequest = new StreamsGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new StreamsGroupHeartbeatRequest.Builder(streamsGroupHeartbeatRequest).build())

    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val response = verifyNoThrottling[StreamsGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.UNSUPPORTED_VERSION.code, response.data.errorCode)
  }

  @Test
  def testStreamsGroupHeartbeatRequestInvalidTopicNames(): Unit = {
    val features = mock(classOf[FinalizedFeatures])
    when(features.finalizedFeatures()).thenReturn(util.Map.of(StreamsVersion.FEATURE_NAME, 1.toShort))

    metadataCache = mock(classOf[KRaftMetadataCache])
    when(metadataCache.features()).thenReturn(features)

    val streamsGroupHeartbeatRequest = new StreamsGroupHeartbeatRequestData().setGroupId("group").setTopology(
      new StreamsGroupHeartbeatRequestData.Topology()
        .setEpoch(3)
        .setSubtopologies(
          util.List.of(new StreamsGroupHeartbeatRequestData.Subtopology().setSubtopologyId("subtopology")
            .setSourceTopics(util.List.of("a "))
            .setRepartitionSinkTopics(util.List.of("b?"))
            .setRepartitionSourceTopics(util.List.of(new StreamsGroupHeartbeatRequestData.TopicInfo().setName("c!")))
            .setStateChangelogTopics(util.List.of(new StreamsGroupHeartbeatRequestData.TopicInfo().setName("d/")))
          )
        )
    )

    val requestChannelRequest = buildRequest(new StreamsGroupHeartbeatRequest.Builder(streamsGroupHeartbeatRequest).build())

    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val response = verifyNoThrottling[StreamsGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.STREAMS_INVALID_TOPOLOGY.code, response.data.errorCode)
    assertEquals("Topic names a ,b?,c!,d/ are not valid topic names.", response.data.errorMessage())
  }

  @Test
  def testStreamsGroupHeartbeatRequestInternalTopicNames(): Unit = {
    val features = mock(classOf[FinalizedFeatures])
    when(features.finalizedFeatures()).thenReturn(util.Map.of(StreamsVersion.FEATURE_NAME, 1.toShort))

    metadataCache = mock(classOf[KRaftMetadataCache])
    when(metadataCache.features()).thenReturn(features)

    val streamsGroupHeartbeatRequest = new StreamsGroupHeartbeatRequestData().setGroupId("group").setTopology(
      new StreamsGroupHeartbeatRequestData.Topology()
        .setEpoch(3)
        .setSubtopologies(
          util.List.of(new StreamsGroupHeartbeatRequestData.Subtopology().setSubtopologyId("subtopology")
            .setSourceTopics(util.List.of("__consumer_offsets"))
            .setRepartitionSinkTopics(util.List.of("__transaction_state"))
            .setRepartitionSourceTopics(util.List.of(new StreamsGroupHeartbeatRequestData.TopicInfo().setName("__share_group_state")))
          )
        )
    )

    val requestChannelRequest = buildRequest(new StreamsGroupHeartbeatRequest.Builder(streamsGroupHeartbeatRequest).build())

    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val response = verifyNoThrottling[StreamsGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.STREAMS_INVALID_TOPOLOGY.code, response.data.errorCode)
    assertEquals("Use of Kafka internal topics __consumer_offsets,__transaction_state,__share_group_state in a Kafka Streams topology is prohibited.", response.data.errorMessage())
  }

  @Test
  def testStreamsGroupHeartbeatRequestWithInternalTopicsToCreate(): Unit = {
    val features = mock(classOf[FinalizedFeatures])
    when(features.finalizedFeatures()).thenReturn(util.Map.of(StreamsVersion.FEATURE_NAME, 1.toShort))

    metadataCache = mock(classOf[KRaftMetadataCache])
    when(metadataCache.features()).thenReturn(features)

    val streamsGroupHeartbeatRequest = new StreamsGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new StreamsGroupHeartbeatRequest.Builder(streamsGroupHeartbeatRequest).build())

    val future = new CompletableFuture[StreamsGroupHeartbeatResult]()
    when(groupCoordinator.streamsGroupHeartbeat(
      requestChannelRequest.context,
      streamsGroupHeartbeatRequest
    )).thenReturn(future)

    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val missingTopics = Map("test" -> new CreatableTopic())
    val streamsGroupHeartbeatResponse = new StreamsGroupHeartbeatResponseData()
      .setMemberId("member")

    future.complete(new StreamsGroupHeartbeatResult(streamsGroupHeartbeatResponse, missingTopics.asJava))
    val response = verifyNoThrottling[StreamsGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(streamsGroupHeartbeatResponse, response.data)
    verify(autoTopicCreationManager).createStreamsInternalTopics(any(), any(), anyLong())
  }

  @Test
  def testStreamsGroupHeartbeatRequestWithInternalTopicsToCreateMissingCreateACL(): Unit = {
    val features = mock(classOf[FinalizedFeatures])
    when(features.finalizedFeatures()).thenReturn(util.Map.of(StreamsVersion.FEATURE_NAME, 1.toShort))

    metadataCache = mock(classOf[KRaftMetadataCache])
    when(metadataCache.features()).thenReturn(features)

    val streamsGroupHeartbeatRequest = new StreamsGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new StreamsGroupHeartbeatRequest.Builder(streamsGroupHeartbeatRequest).build())

    val future = new CompletableFuture[StreamsGroupHeartbeatResult]()
    when(groupCoordinator.streamsGroupHeartbeat(
      requestChannelRequest.context,
      streamsGroupHeartbeatRequest
    )).thenReturn(future)

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], isNotNull[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1).asInstanceOf[util.List[Action]]
      val results: util.List[AuthorizationResult] = new util.ArrayList[AuthorizationResult](actions.size())
      actions.forEach { action =>
        val result = if (action.resourcePattern.name == "test" && action.operation == AclOperation.CREATE && action.resourcePattern.resourceType == ResourceType.TOPIC) {
          AuthorizationResult.DENIED
        } else if (action.operation == AclOperation.CREATE && action.resourcePattern.resourceType == ResourceType.CLUSTER) {
          AuthorizationResult.DENIED
        } else {
          AuthorizationResult.ALLOWED
        }
        results.add(result)
      }
      results
    })
    kafkaApis = createKafkaApis(
      authorizer = Some(authorizer)
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val missingTopics = util.Map.of("test", new CreatableTopic())
    val streamsGroupHeartbeatResponse = new StreamsGroupHeartbeatResponseData()
      .setMemberId("member")

    future.complete(new StreamsGroupHeartbeatResult(streamsGroupHeartbeatResponse, missingTopics))
    val response = verifyNoThrottling[StreamsGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.NONE.code, response.data.errorCode())
    assertEquals(null, response.data.errorMessage())
    assertEquals(
      java.util.List.of(
        new StreamsGroupHeartbeatResponseData.Status()
          .setStatusCode(StreamsGroupHeartbeatResponse.Status.MISSING_INTERNAL_TOPICS.code())
          .setStatusDetail("Unauthorized to CREATE on topics test.")
      ),
      response.data.status()
    )
  }

  @Test
  def testStreamsGroupHeartbeatRequestWithCachedTopicCreationErrors(): Unit = {
    val features = mock(classOf[FinalizedFeatures])
    when(features.finalizedFeatures()).thenReturn(util.Map.of(StreamsVersion.FEATURE_NAME, 1.toShort))

    metadataCache = mock(classOf[KRaftMetadataCache])
    when(metadataCache.features()).thenReturn(features)

    val streamsGroupHeartbeatRequest = new StreamsGroupHeartbeatRequestData().setGroupId("group")
    val requestChannelRequest = buildRequest(new StreamsGroupHeartbeatRequest.Builder(streamsGroupHeartbeatRequest).build())

    val future = new CompletableFuture[StreamsGroupHeartbeatResult]()
    when(groupCoordinator.streamsGroupHeartbeat(
      requestChannelRequest.context,
      streamsGroupHeartbeatRequest
    )).thenReturn(future)

    // Mock AutoTopicCreationManager to return cached errors
    val mockAutoTopicCreationManager = mock(classOf[AutoTopicCreationManager])
    when(mockAutoTopicCreationManager.getStreamsInternalTopicCreationErrors(ArgumentMatchers.eq(Set("test-topic")), any()))
      .thenReturn(Map("test-topic" -> "INVALID_REPLICATION_FACTOR"))
    // Mock the createStreamsInternalTopics method to do nothing (simulate topic creation attempt)
    doNothing().when(mockAutoTopicCreationManager).createStreamsInternalTopics(any(), any(), anyLong())

    kafkaApis = createKafkaApis(autoTopicCreationManager = Some(mockAutoTopicCreationManager))
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    // Group coordinator returns MISSING_INTERNAL_TOPICS status and topics to create
    val missingTopics = util.Map.of("test-topic", new CreatableTopic())
    val streamsGroupHeartbeatResponse = new StreamsGroupHeartbeatResponseData()
      .setMemberId("member")
      .setStatus(util.List.of(
        new StreamsGroupHeartbeatResponseData.Status()
          .setStatusCode(StreamsGroupHeartbeatResponse.Status.MISSING_INTERNAL_TOPICS.code())
          .setStatusDetail("Internal topics are missing: [test-topic]")
      ))

    future.complete(new StreamsGroupHeartbeatResult(streamsGroupHeartbeatResponse, missingTopics))
    val response = verifyNoThrottling[StreamsGroupHeartbeatResponse](requestChannelRequest)
    
    assertEquals(Errors.NONE.code, response.data.errorCode())
    assertEquals(null, response.data.errorMessage())
    
    // Verify that the cached error was appended to the existing status detail
    assertEquals(1, response.data.status().size())
    val status = response.data.status().get(0)
    assertEquals(StreamsGroupHeartbeatResponse.Status.MISSING_INTERNAL_TOPICS.code(), status.statusCode())
    assertTrue(status.statusDetail().contains("Internal topics are missing: [test-topic]"))
    assertTrue(status.statusDetail().contains("Creation failed: test-topic (INVALID_REPLICATION_FACTOR)"))
    
    // Verify that createStreamsInternalTopics was called
    verify(mockAutoTopicCreationManager).createStreamsInternalTopics(any(), any(), anyLong())
    verify(mockAutoTopicCreationManager).getStreamsInternalTopicCreationErrors(ArgumentMatchers.eq(Set("test-topic")), any())
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testConsumerGroupDescribe(includeAuthorizedOperations: Boolean): Unit = {
    val fooTopicName = "foo"
    val barTopicName = "bar"
    metadataCache = mock(classOf[KRaftMetadataCache])

    val groupIds = util.List.of("group-id-0", "group-id-1", "group-id-2")
    val consumerGroupDescribeRequestData = new ConsumerGroupDescribeRequestData()
      .setIncludeAuthorizedOperations(includeAuthorizedOperations)
    consumerGroupDescribeRequestData.groupIds.addAll(groupIds)
    val requestChannelRequest = buildRequest(new ConsumerGroupDescribeRequest.Builder(consumerGroupDescribeRequestData, true).build())

    val future = new CompletableFuture[util.List[ConsumerGroupDescribeResponseData.DescribedGroup]]()
    when(groupCoordinator.consumerGroupDescribe(
      any[RequestContext],
      any[util.List[String]]
    )).thenReturn(future)
    kafkaApis = createKafkaApis(
      featureVersions = Seq(GroupVersion.GV_1)
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val member0 = new ConsumerGroupDescribeResponseData.Member()
      .setMemberId("member0")
      .setAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(util.List.of(
          new TopicPartitions().setTopicName(fooTopicName))))
      .setTargetAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(util.List.of(
          new TopicPartitions().setTopicName(fooTopicName))))

    val member1 = new ConsumerGroupDescribeResponseData.Member()
      .setMemberId("member1")
      .setAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(util.List.of(
          new TopicPartitions().setTopicName(fooTopicName))))
      .setTargetAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(util.List.of(
          new TopicPartitions().setTopicName(fooTopicName),
          new TopicPartitions().setTopicName(barTopicName))))

    val member2 = new ConsumerGroupDescribeResponseData.Member()
      .setMemberId("member2")
      .setAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(util.List.of(
          new TopicPartitions().setTopicName(barTopicName))))
      .setTargetAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(util.List.of(
          new TopicPartitions().setTopicName(fooTopicName))))

    future.complete(util.List.of(
      new DescribedGroup()
        .setGroupId(groupIds.get(0))
        .setMembers(util.List.of(member0)),
      new DescribedGroup()
        .setGroupId(groupIds.get(1))
        .setMembers(util.List.of(member0, member1)),
      new DescribedGroup()
        .setGroupId(groupIds.get(2))
        .setMembers(util.List.of(member2))
    ))

    var authorizedOperationsInt = Int.MinValue
    if (includeAuthorizedOperations) {
      authorizedOperationsInt = Utils.to32BitField(
        AclEntry.supportedOperations(ResourceType.GROUP).asScala
          .map(_.code.asInstanceOf[JByte]).asJava)
    }

    // Can't reuse the above list here because we would not test the implementation in KafkaApis then
    val describedGroups = List(
      new DescribedGroup()
        .setGroupId(groupIds.get(0))
        .setMembers(util.List.of(member0)),
      new DescribedGroup()
        .setGroupId(groupIds.get(1))
        .setMembers(util.List.of(member0, member1)),
      new DescribedGroup()
        .setGroupId(groupIds.get(2))
        .setMembers(util.List.of(member2))
    ).map(group => group.setAuthorizedOperations(authorizedOperationsInt))
    val expectedConsumerGroupDescribeResponseData = new ConsumerGroupDescribeResponseData()
      .setGroups(describedGroups.asJava)

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
    metadataCache = {
      val cache = new KRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_1)
      val delta = new MetadataDelta(MetadataImage.EMPTY)
      delta.replay(new FeatureLevelRecord()
        .setName(MetadataVersion.FEATURE_NAME)
        .setFeatureLevel(MetadataVersion.MINIMUM_VERSION.featureLevel())
      )
      cache.setImage(delta.apply(MetadataProvenance.EMPTY))
      cache
    }
    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)
    val response = verifyNoThrottling[ConsumerGroupDescribeResponse](requestChannelRequest)

    assertEquals(expectedResponse, response.data)
  }

  @Test
  def testConsumerGroupDescribeAuthorizationFailed(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])

    val consumerGroupDescribeRequestData = new ConsumerGroupDescribeRequestData()
    consumerGroupDescribeRequestData.groupIds.add("group-id")
    val requestChannelRequest = buildRequest(new ConsumerGroupDescribeRequest.Builder(consumerGroupDescribeRequestData, true).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(util.List.of(AuthorizationResult.DENIED))

    val future = new CompletableFuture[util.List[ConsumerGroupDescribeResponseData.DescribedGroup]]()
    when(groupCoordinator.consumerGroupDescribe(
      any[RequestContext],
      any[util.List[String]]
    )).thenReturn(future)
    future.complete(util.List.of)
    kafkaApis = createKafkaApis(
      authorizer = Some(authorizer),
      featureVersions = Seq(GroupVersion.GV_1)
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val response = verifyNoThrottling[ConsumerGroupDescribeResponse](requestChannelRequest)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, response.data.groups.get(0).errorCode)
  }

  @Test
  def testConsumerGroupDescribeFutureFailed(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])

    val consumerGroupDescribeRequestData = new ConsumerGroupDescribeRequestData()
    consumerGroupDescribeRequestData.groupIds.add("group-id")
    val requestChannelRequest = buildRequest(new ConsumerGroupDescribeRequest.Builder(consumerGroupDescribeRequestData, true).build())

    val future = new CompletableFuture[util.List[ConsumerGroupDescribeResponseData.DescribedGroup]]()
    when(groupCoordinator.consumerGroupDescribe(
      any[RequestContext],
      any[util.List[String]]
    )).thenReturn(future)
    kafkaApis = createKafkaApis(
      featureVersions = Seq(GroupVersion.GV_1)
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    future.completeExceptionally(Errors.FENCED_MEMBER_EPOCH.exception)
    val response = verifyNoThrottling[ConsumerGroupDescribeResponse](requestChannelRequest)
    assertEquals(Errors.FENCED_MEMBER_EPOCH.code, response.data.groups.get(0).errorCode)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testStreamsGroupDescribe(includeAuthorizedOperations: Boolean): Unit = {
    val features = mock(classOf[FinalizedFeatures])
    when(features.finalizedFeatures()).thenReturn(util.Map.of(StreamsVersion.FEATURE_NAME, 1.toShort))

    metadataCache = mock(classOf[KRaftMetadataCache])
    when(metadataCache.features()).thenReturn(features)

    val fooTopicName = "foo"
    val barTopicName = "bar"

    val groupIds = util.List.of("group-id-0", "group-id-1", "group-id-2")
    val streamsGroupDescribeRequestData = new StreamsGroupDescribeRequestData()
      .setIncludeAuthorizedOperations(includeAuthorizedOperations)
    streamsGroupDescribeRequestData.groupIds.addAll(groupIds)
    val requestChannelRequest = buildRequest(new StreamsGroupDescribeRequest.Builder(streamsGroupDescribeRequestData).build())

    val future = new CompletableFuture[util.List[StreamsGroupDescribeResponseData.DescribedGroup]]()
    when(groupCoordinator.streamsGroupDescribe(
      any[RequestContext],
      any[util.List[String]]
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val subtopology0 = new StreamsGroupDescribeResponseData.Subtopology()
      .setSubtopologyId("subtopology0")
      .setSourceTopics(util.List.of(fooTopicName))

    val subtopology1 = new StreamsGroupDescribeResponseData.Subtopology()
      .setSubtopologyId("subtopology1")
      .setRepartitionSinkTopics(util.List.of(barTopicName))

    val subtopology2 = new StreamsGroupDescribeResponseData.Subtopology()
      .setSubtopologyId("subtopology2")
      .setSourceTopics(util.List.of(fooTopicName))
      .setRepartitionSinkTopics(util.List.of(barTopicName))

    future.complete(util.List.of(
      new StreamsGroupDescribeResponseData.DescribedGroup()
        .setGroupId(groupIds.get(0))
        .setTopology(new StreamsGroupDescribeResponseData.Topology()
          .setSubtopologies(util.List.of(subtopology0))),
      new StreamsGroupDescribeResponseData.DescribedGroup()
        .setGroupId(groupIds.get(1))
        .setTopology(new StreamsGroupDescribeResponseData.Topology()
          .setSubtopologies(util.List.of(subtopology1))),
      new StreamsGroupDescribeResponseData.DescribedGroup()
        .setGroupId(groupIds.get(2))
        .setTopology(new StreamsGroupDescribeResponseData.Topology()
          .setSubtopologies(util.List.of(subtopology2)))
    ))

    var authorizedOperationsInt = Int.MinValue
    if (includeAuthorizedOperations) {
      authorizedOperationsInt = Utils.to32BitField(
        AclEntry.supportedOperations(ResourceType.GROUP).asScala
          .map(_.code.asInstanceOf[JByte]).asJava)
    }

    // Can't reuse the above list here because we would not test the implementation in KafkaApis then
    val describedGroups = List(
      new StreamsGroupDescribeResponseData.DescribedGroup()
        .setGroupId(groupIds.get(0))
        .setTopology(new StreamsGroupDescribeResponseData.Topology()
          .setSubtopologies(util.List.of(subtopology0))),
      new StreamsGroupDescribeResponseData.DescribedGroup()
        .setGroupId(groupIds.get(1))
        .setTopology(new StreamsGroupDescribeResponseData.Topology()
          .setSubtopologies(util.List.of(subtopology1))),
      new StreamsGroupDescribeResponseData.DescribedGroup()
        .setGroupId(groupIds.get(2))
        .setTopology(new StreamsGroupDescribeResponseData.Topology()
          .setSubtopologies(util.List.of(subtopology2)))
    ).map(group => group.setAuthorizedOperations(authorizedOperationsInt))
    val expectedStreamsGroupDescribeResponseData = new StreamsGroupDescribeResponseData()
      .setGroups(describedGroups.asJava)

    val response = verifyNoThrottling[StreamsGroupDescribeResponse](requestChannelRequest)

    assertEquals(expectedStreamsGroupDescribeResponseData, response.data)
  }

  @Test
  def testStreamsGroupDescribeReturnsUnsupportedVersion(): Unit = {
    val groupId = "group0"
    val streamsGroupDescribeRequestData = new StreamsGroupDescribeRequestData()
    streamsGroupDescribeRequestData.groupIds.add(groupId)
    val requestChannelRequest = buildRequest(new StreamsGroupDescribeRequest.Builder(streamsGroupDescribeRequestData).build())

    val errorCode = Errors.UNSUPPORTED_VERSION.code
    val expectedDescribedGroup = new StreamsGroupDescribeResponseData.DescribedGroup().setGroupId(groupId).setErrorCode(errorCode)
    val expectedResponse = new StreamsGroupDescribeResponseData()
    expectedResponse.groups.add(expectedDescribedGroup)
    metadataCache = {
      val cache = new KRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_1)
      val delta = new MetadataDelta(MetadataImage.EMPTY)
      delta.replay(new FeatureLevelRecord()
        .setName(MetadataVersion.FEATURE_NAME)
        .setFeatureLevel(MetadataVersion.MINIMUM_VERSION.featureLevel())
      )
      cache.setImage(delta.apply(MetadataProvenance.EMPTY))
      cache
    }
    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)
    val response = verifyNoThrottling[StreamsGroupDescribeResponse](requestChannelRequest)

    assertEquals(expectedResponse, response.data)
  }

  @Test
  def testStreamsGroupDescribeAuthorizationFailed(): Unit = {
    val features = mock(classOf[FinalizedFeatures])
    when(features.finalizedFeatures()).thenReturn(util.Map.of(StreamsVersion.FEATURE_NAME, 1.toShort))

    metadataCache = mock(classOf[KRaftMetadataCache])
    when(metadataCache.features()).thenReturn(features)

    val streamsGroupDescribeRequestData = new StreamsGroupDescribeRequestData()
    streamsGroupDescribeRequestData.groupIds.add("group-id")
    val requestChannelRequest = buildRequest(new StreamsGroupDescribeRequest.Builder(streamsGroupDescribeRequestData).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(util.List.of(AuthorizationResult.DENIED))

    val future = new CompletableFuture[util.List[StreamsGroupDescribeResponseData.DescribedGroup]]()
    when(groupCoordinator.streamsGroupDescribe(
      any[RequestContext],
      any[util.List[String]]
    )).thenReturn(future)
    future.complete(util.List.of)
    kafkaApis = createKafkaApis(
      authorizer = Some(authorizer)
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val response = verifyNoThrottling[StreamsGroupDescribeResponse](requestChannelRequest)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, response.data.groups.get(0).errorCode)
  }

  @Test
  def testStreamsGroupDescribeFutureFailed(): Unit = {
    val features = mock(classOf[FinalizedFeatures])
    when(features.finalizedFeatures()).thenReturn(util.Map.of(StreamsVersion.FEATURE_NAME, 1.toShort))

    metadataCache = mock(classOf[KRaftMetadataCache])
    when(metadataCache.features()).thenReturn(features)

    val streamsGroupDescribeRequestData = new StreamsGroupDescribeRequestData()
    streamsGroupDescribeRequestData.groupIds.add("group-id")
    val requestChannelRequest = buildRequest(new StreamsGroupDescribeRequest.Builder(streamsGroupDescribeRequestData).build())

    val future = new CompletableFuture[util.List[StreamsGroupDescribeResponseData.DescribedGroup]]()
    when(groupCoordinator.streamsGroupDescribe(
      any[RequestContext],
      any[util.List[String]]
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    future.completeExceptionally(Errors.FENCED_MEMBER_EPOCH.exception)
    val response = verifyNoThrottling[StreamsGroupDescribeResponse](requestChannelRequest)
    assertEquals(Errors.FENCED_MEMBER_EPOCH.code, response.data.groups.get(0).errorCode)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testStreamsGroupDescribeFilterUnauthorizedTopics(includeAuthorizedOperations: Boolean): Unit = {
    val fooTopicName = "foo"
    val barTopicName = "bar"
    val errorMessage = "The described group uses topics that the client is not authorized to describe."

    val features = mock(classOf[FinalizedFeatures])
    when(features.finalizedFeatures()).thenReturn(util.Map.of(StreamsVersion.FEATURE_NAME, 1.toShort))

    metadataCache = mock(classOf[KRaftMetadataCache])
    when(metadataCache.features()).thenReturn(features)

    val groupIds = util.List.of("group-id-0", "group-id-1", "group-id-2")
    val streamsGroupDescribeRequestData = new StreamsGroupDescribeRequestData()
      .setIncludeAuthorizedOperations(includeAuthorizedOperations)
    streamsGroupDescribeRequestData.groupIds.addAll(groupIds)
    val requestChannelRequest = buildRequest(new StreamsGroupDescribeRequest.Builder(streamsGroupDescribeRequestData).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    val acls = Map(
      groupIds.get(0) -> AuthorizationResult.ALLOWED,
      groupIds.get(1) -> AuthorizationResult.ALLOWED,
      groupIds.get(2) -> AuthorizationResult.ALLOWED,
      fooTopicName    -> AuthorizationResult.ALLOWED,
      barTopicName    -> AuthorizationResult.DENIED,
    )
    when(authorizer.authorize(
      any[RequestContext],
      any[util.List[Action]]
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      actions.stream()
        .map(action => acls.getOrElse(action.resourcePattern.name, AuthorizationResult.DENIED))
        .toList
    }

    val future = new CompletableFuture[util.List[StreamsGroupDescribeResponseData.DescribedGroup]]()
    when(groupCoordinator.streamsGroupDescribe(
      any[RequestContext],
      any[util.List[String]]
    )).thenReturn(future)
    kafkaApis = createKafkaApis(
      authorizer = Some(authorizer)
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val subtopology0 = new StreamsGroupDescribeResponseData.Subtopology()
      .setSubtopologyId("subtopology0")
      .setSourceTopics(util.List.of(fooTopicName))

    val subtopology1 = new StreamsGroupDescribeResponseData.Subtopology()
      .setSubtopologyId("subtopology1")
      .setRepartitionSinkTopics(util.List.of(barTopicName))

    val subtopology2 = new StreamsGroupDescribeResponseData.Subtopology()
      .setSubtopologyId("subtopology2")
      .setSourceTopics(util.List.of(fooTopicName))
      .setRepartitionSinkTopics(util.List.of(barTopicName))

    future.complete(util.List.of(
      new StreamsGroupDescribeResponseData.DescribedGroup()
        .setGroupId(groupIds.get(0))
        .setTopology(new StreamsGroupDescribeResponseData.Topology()
          .setSubtopologies(util.List.of(subtopology0))),
      new StreamsGroupDescribeResponseData.DescribedGroup()
        .setGroupId(groupIds.get(1))
        .setTopology(new StreamsGroupDescribeResponseData.Topology()
          .setSubtopologies(util.List.of(subtopology1))),
      new StreamsGroupDescribeResponseData.DescribedGroup()
        .setGroupId(groupIds.get(2))
        .setTopology(new StreamsGroupDescribeResponseData.Topology()
          .setSubtopologies(util.List.of(subtopology2)))
    ))

    val response = verifyNoThrottling[StreamsGroupDescribeResponse](requestChannelRequest)
    assertNotNull(response.data)
    assertEquals(3, response.data.groups.size)
    assertEquals(Errors.NONE.code(), response.data.groups.get(0).errorCode())
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code(), response.data.groups.get(1).errorCode())
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code(), response.data.groups.get(2).errorCode())
    assertEquals(errorMessage, response.data.groups.get(1).errorMessage())
    assertEquals(errorMessage, response.data.groups.get(2).errorMessage())
  }

  @Test
  def testConsumerGroupDescribeFilterUnauthorizedTopics(): Unit = {
    val fooTopicName = "foo"
    val barTopicName = "bar"
    val errorMessage = "The group has described topic(s) that the client is not authorized to describe."

    metadataCache = mock(classOf[KRaftMetadataCache])

    val groupIds = util.List.of("group-id-0", "group-id-1", "group-id-2")
    val consumerGroupDescribeRequestData = new ConsumerGroupDescribeRequestData()
      .setGroupIds(groupIds)
    val requestChannelRequest = buildRequest(new ConsumerGroupDescribeRequest.Builder(consumerGroupDescribeRequestData, true).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    val acls = Map(
      groupIds.get(0) -> AuthorizationResult.ALLOWED,
      groupIds.get(1) -> AuthorizationResult.ALLOWED,
      groupIds.get(2) -> AuthorizationResult.ALLOWED,
      fooTopicName    -> AuthorizationResult.ALLOWED,
      barTopicName    -> AuthorizationResult.DENIED,
    )
    when(authorizer.authorize(
      any[RequestContext],
      any[util.List[Action]]
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      actions.stream()
        .map(action => acls.getOrElse(action.resourcePattern.name, AuthorizationResult.DENIED))
        .toList
    }

    val future = new CompletableFuture[util.List[ConsumerGroupDescribeResponseData.DescribedGroup]]()
    when(groupCoordinator.consumerGroupDescribe(
      any[RequestContext],
      any[util.List[String]]
    )).thenReturn(future)
    kafkaApis = createKafkaApis(
      authorizer = Some(authorizer),
      featureVersions = Seq(GroupVersion.GV_1)
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val member0 = new ConsumerGroupDescribeResponseData.Member()
      .setMemberId("member0")
      .setAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(util.List.of(
          new TopicPartitions().setTopicName(fooTopicName))))
      .setTargetAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(util.List.of(
          new TopicPartitions().setTopicName(fooTopicName))))

    val member1 = new ConsumerGroupDescribeResponseData.Member()
      .setMemberId("member1")
      .setAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(util.List.of(
          new TopicPartitions().setTopicName(fooTopicName))))
      .setTargetAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(util.List.of(
          new TopicPartitions().setTopicName(fooTopicName),
          new TopicPartitions().setTopicName(barTopicName))))

    val member2 = new ConsumerGroupDescribeResponseData.Member()
      .setMemberId("member2")
      .setAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(util.List.of(
          new TopicPartitions().setTopicName(barTopicName))))
      .setTargetAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(util.List.of(
          new TopicPartitions().setTopicName(fooTopicName))))

    future.complete(util.List.of(
      new DescribedGroup()
        .setGroupId(groupIds.get(0))
        .setMembers(util.List.of(member0)),
      new DescribedGroup()
        .setGroupId(groupIds.get(1))
        .setMembers(util.List.of(member0, member1)),
      new DescribedGroup()
        .setGroupId(groupIds.get(2))
        .setMembers(util.List.of(member2))
    ))

    val expectedConsumerGroupDescribeResponseData = new ConsumerGroupDescribeResponseData()
      .setGroups(util.List.of(
        new DescribedGroup()
          .setGroupId(groupIds.get(0))
          .setMembers(util.List.of(member0)),
        new DescribedGroup()
          .setGroupId(groupIds.get(1))
          .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
          .setErrorMessage(errorMessage),
        new DescribedGroup()
          .setGroupId(groupIds.get(2))
          .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
          .setErrorMessage(errorMessage)
      ))

    val response = verifyNoThrottling[ConsumerGroupDescribeResponse](requestChannelRequest)

    assertEquals(expectedConsumerGroupDescribeResponseData, response.data)
  }

  @Test
  def testGetTelemetrySubscriptions(): Unit = {
    val request = buildRequest(new GetTelemetrySubscriptionsRequest.Builder(
      new GetTelemetrySubscriptionsRequestData(), true).build())

    when(clientMetricsManager.isTelemetryExporterConfigured).thenReturn(true)
    when(clientMetricsManager.processGetTelemetrySubscriptionRequest(any[GetTelemetrySubscriptionsRequest](),
      any[RequestContext]())).thenReturn(new GetTelemetrySubscriptionsResponse(
      new GetTelemetrySubscriptionsResponseData()))

    metadataCache = new KRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(request, RequestLocal.noCaching)

    val response = verifyNoThrottling[GetTelemetrySubscriptionsResponse](request)

    val expectedResponse = new GetTelemetrySubscriptionsResponseData()
    assertEquals(expectedResponse, response.data)
  }

  @Test
  def testGetTelemetrySubscriptionsWithException(): Unit = {
    val request = buildRequest(new GetTelemetrySubscriptionsRequest.Builder(
      new GetTelemetrySubscriptionsRequestData(), true).build())

    when(clientMetricsManager.isTelemetryExporterConfigured).thenReturn(true)
    when(clientMetricsManager.processGetTelemetrySubscriptionRequest(any[GetTelemetrySubscriptionsRequest](),
      any[RequestContext]())).thenThrow(new RuntimeException("test"))

    metadataCache = new KRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(request, RequestLocal.noCaching)

    val response = verifyNoThrottling[GetTelemetrySubscriptionsResponse](request)

    val expectedResponse = new GetTelemetrySubscriptionsResponseData().setErrorCode(Errors.INVALID_REQUEST.code)
    assertEquals(expectedResponse, response.data)
  }

  @Test
  def testPushTelemetry(): Unit = {
    val request = buildRequest(new PushTelemetryRequest.Builder(new PushTelemetryRequestData(), true).build())

    when(clientMetricsManager.isTelemetryExporterConfigured).thenReturn(true)
    when(clientMetricsManager.processPushTelemetryRequest(any[PushTelemetryRequest](), any[RequestContext]()))
      .thenReturn(new PushTelemetryResponse(new PushTelemetryResponseData()))

    metadataCache = new KRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(request, RequestLocal.noCaching)
    val response = verifyNoThrottling[PushTelemetryResponse](request)

    val expectedResponse = new PushTelemetryResponseData().setErrorCode(Errors.NONE.code)
    assertEquals(expectedResponse, response.data)
  }

  @Test
  def testPushTelemetryWithException(): Unit = {
    val request = buildRequest(new PushTelemetryRequest.Builder(new PushTelemetryRequestData(), true).build())

    when(clientMetricsManager.isTelemetryExporterConfigured).thenReturn(true)
    when(clientMetricsManager.processPushTelemetryRequest(any[PushTelemetryRequest](), any[RequestContext]()))
      .thenThrow(new RuntimeException("test"))

    metadataCache = new KRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(request, RequestLocal.noCaching)
    val response = verifyNoThrottling[PushTelemetryResponse](request)

    val expectedResponse = new PushTelemetryResponseData().setErrorCode(Errors.INVALID_REQUEST.code)
    assertEquals(expectedResponse, response.data)
  }

  @Test
  def testListConfigResourcesV0(): Unit = {
    val requestMetrics = new RequestChannelMetrics(util.Set.of(ApiKeys.LIST_CONFIG_RESOURCES))
    try {
      val request = buildRequest(new ListConfigResourcesRequest.Builder(
        new ListConfigResourcesRequestData().setResourceTypes(util.List.of(ConfigResource.Type.CLIENT_METRICS.id))).build(0),
        requestMetrics = requestMetrics)
      metadataCache = mock(classOf[KRaftMetadataCache])

      val resources = util.Set.of("client-metric1", "client-metric2")
      when(clientMetricsManager.listClientMetricsResources).thenReturn(resources)

      kafkaApis = createKafkaApis()
      kafkaApis.handle(request, RequestLocal.noCaching)
      val response = verifyNoThrottlingAndUpdateMetrics[ListConfigResourcesResponse](request)
      val expectedResponseData = new ListConfigResourcesResponseData()
        .setConfigResources(
          resources.stream.map(resource =>
            new ListConfigResourcesResponseData.ConfigResource().setResourceName(resource)
          ).collect(util.stream.Collectors.toList[ListConfigResourcesResponseData.ConfigResource]))
      assertEquals(expectedResponseData, response.data)

      verify(metadataCache, never).getAllTopics
      verify(groupConfigManager, never).groupIds
      verify(metadataCache, never).getBrokerNodes(any)
      assertTrue(requestMetrics.apply(ApiKeys.LIST_CONFIG_RESOURCES.name).requestQueueTimeHist.count > 0)
      assertTrue(requestMetrics.apply(RequestMetrics.LIST_CLIENT_METRICS_RESOURCES_METRIC_NAME).requestQueueTimeHist.count > 0)
    } finally {
      requestMetrics.close()
    }
  }

  @Test
  def testListConfigResourcesV1WithEmptyResourceTypes(): Unit = {
    val requestMetrics = new RequestChannelMetrics(util.Set.of(ApiKeys.LIST_CONFIG_RESOURCES))
    try {
      val request = buildRequest(new ListConfigResourcesRequest.Builder(new ListConfigResourcesRequestData()).build(1),
        requestMetrics = requestMetrics)
      metadataCache = mock(classOf[KRaftMetadataCache])

      val clientMetrics = util.Set.of("client-metric1", "client-metric2")
      val topics = util.Set.of("topic1", "topic2")
      val groupIds = util.List.of("group1", "group2")
      val nodeIds = util.List.of(1, 2)
      when(clientMetricsManager.listClientMetricsResources).thenReturn(clientMetrics)
      when(metadataCache.getAllTopics).thenReturn(topics)
      when(groupConfigManager.groupIds).thenReturn(groupIds)
      when(metadataCache.getBrokerNodes(any())).thenReturn(
        nodeIds.stream().map(id => new Node(id, "localhost", 1234)).collect(java.util.stream.Collectors.toList()))

      kafkaApis = createKafkaApis()
      kafkaApis.handle(request, RequestLocal.noCaching)
      val response = verifyNoThrottlingAndUpdateMetrics[ListConfigResourcesResponse](request)
      val expectedResponseData = new ListConfigResourcesResponseData()
        .setConfigResources(
          util.stream.Stream.of(
            groupIds.stream().map(resource =>
              new ListConfigResourcesResponseData.ConfigResource().setResourceName(resource).setResourceType(ConfigResource.Type.GROUP.id)
            ).toList,
            clientMetrics.stream.map(resource =>
              new ListConfigResourcesResponseData.ConfigResource().setResourceName(resource).setResourceType(ConfigResource.Type.CLIENT_METRICS.id)
            ).toList,
            nodeIds.stream().map(resource =>
              new ListConfigResourcesResponseData.ConfigResource().setResourceName(resource.toString).setResourceType(ConfigResource.Type.BROKER_LOGGER.id)
            ).toList,
            nodeIds.stream().map(resource =>
              new ListConfigResourcesResponseData.ConfigResource().setResourceName(resource.toString).setResourceType(ConfigResource.Type.BROKER.id)
            ).toList,
            topics.stream().map(resource =>
              new ListConfigResourcesResponseData.ConfigResource().setResourceName(resource).setResourceType(ConfigResource.Type.TOPIC.id)
            ).toList
          ).flatMap(s => s.stream).collect(util.stream.Collectors.toList[ListConfigResourcesResponseData.ConfigResource]))
      assertEquals(expectedResponseData, response.data)
      assertTrue(requestMetrics.apply(ApiKeys.LIST_CONFIG_RESOURCES.name).requestQueueTimeHist.count > 0)
      assertEquals(0, requestMetrics.apply(RequestMetrics.LIST_CLIENT_METRICS_RESOURCES_METRIC_NAME).requestQueueTimeHist.count)
    } finally {
      requestMetrics.close()
    }
  }

  @Test
  def testListConfigResourcesV1WithGroup(): Unit = {
    val request = buildRequest(new ListConfigResourcesRequest.Builder(new ListConfigResourcesRequestData()
      .setResourceTypes(util.List.of(ConfigResource.Type.GROUP.id))).build(1))
    metadataCache = mock(classOf[KRaftMetadataCache])

    val groupIds = util.List.of("group1", "group2")
    when(groupConfigManager.groupIds).thenReturn(groupIds)

    kafkaApis = createKafkaApis()
    kafkaApis.handle(request, RequestLocal.noCaching)
    val response = verifyNoThrottling[ListConfigResourcesResponse](request)
    val expectedResponseData = new ListConfigResourcesResponseData()
        .setConfigResources(
          groupIds.stream().map(resource =>
            new ListConfigResourcesResponseData.ConfigResource().setResourceName(resource).setResourceType(ConfigResource.Type.GROUP.id)
          ).collect(util.stream.Collectors.toList[ListConfigResourcesResponseData.ConfigResource]))
    assertEquals(expectedResponseData, response.data)

    verify(metadataCache, never).getAllTopics
    verify(clientMetricsManager, never).listClientMetricsResources
    verify(metadataCache, never).getBrokerNodes(any)
  }

  @Test
  def testListConfigResourcesV1WithClientMetrics(): Unit = {
    val request = buildRequest(new ListConfigResourcesRequest.Builder(new ListConfigResourcesRequestData()
      .setResourceTypes(util.List.of(ConfigResource.Type.CLIENT_METRICS.id))).build(1))
    metadataCache = mock(classOf[KRaftMetadataCache])

    val clientMetrics = util.Set.of("client-metric1", "client-metric2")
    when(clientMetricsManager.listClientMetricsResources).thenReturn(clientMetrics)

    kafkaApis = createKafkaApis()
    kafkaApis.handle(request, RequestLocal.noCaching)
    val response = verifyNoThrottling[ListConfigResourcesResponse](request)
    val expectedResponseData = new ListConfigResourcesResponseData()
        .setConfigResources(
          clientMetrics.stream.map(resource =>
            new ListConfigResourcesResponseData.ConfigResource().setResourceName(resource).setResourceType(ConfigResource.Type.CLIENT_METRICS.id)
          ).collect(util.stream.Collectors.toList[ListConfigResourcesResponseData.ConfigResource]))
    assertEquals(expectedResponseData, response.data)

    verify(metadataCache, never).getAllTopics
    verify(groupConfigManager, never).groupIds
    verify(metadataCache, never).getBrokerNodes(any)
  }

  @Test
  def testListConfigResourcesV1WithBrokerLogger(): Unit = {
    val request = buildRequest(new ListConfigResourcesRequest.Builder(new ListConfigResourcesRequestData()
      .setResourceTypes(util.List.of(ConfigResource.Type.BROKER_LOGGER.id))).build(1))
    metadataCache = mock(classOf[KRaftMetadataCache])

    val nodeIds = util.List.of(1, 2)
    when(metadataCache.getBrokerNodes(any())).thenReturn(
      nodeIds.stream().map(id => new Node(id, "localhost", 1234)).collect(java.util.stream.Collectors.toList()))

    kafkaApis = createKafkaApis()
    kafkaApis.handle(request, RequestLocal.noCaching)
    val response = verifyNoThrottling[ListConfigResourcesResponse](request)
    val expectedResponseData = new ListConfigResourcesResponseData()
        .setConfigResources(
          nodeIds.stream().map(resource =>
            new ListConfigResourcesResponseData.ConfigResource().setResourceName(resource.toString).setResourceType(ConfigResource.Type.BROKER_LOGGER.id)
          ).collect(java.util.stream.Collectors.toList[ListConfigResourcesResponseData.ConfigResource]))
    assertEquals(expectedResponseData, response.data)

    verify(metadataCache, never).getAllTopics
    verify(groupConfigManager, never).groupIds
    verify(clientMetricsManager, never).listClientMetricsResources
  }

  @Test
  def testListConfigResourcesV1WithBroker(): Unit = {
    val request = buildRequest(new ListConfigResourcesRequest.Builder(new ListConfigResourcesRequestData()
      .setResourceTypes(util.List.of(ConfigResource.Type.BROKER.id))).build(1))
    metadataCache = mock(classOf[KRaftMetadataCache])

    val nodeIds = util.List.of(1, 2)
    when(metadataCache.getBrokerNodes(any())).thenReturn(
      nodeIds.stream().map(id => new Node(id, "localhost", 1234)).collect(java.util.stream.Collectors.toList()))

    kafkaApis = createKafkaApis()
    kafkaApis.handle(request, RequestLocal.noCaching)
    val response = verifyNoThrottling[ListConfigResourcesResponse](request)
    val expectedResponseData = new ListConfigResourcesResponseData()
        .setConfigResources(
          nodeIds.stream().map(resource =>
            new ListConfigResourcesResponseData.ConfigResource().setResourceName(resource.toString).setResourceType(ConfigResource.Type.BROKER.id)
          ).collect(java.util.stream.Collectors.toList[ListConfigResourcesResponseData.ConfigResource]))
    assertEquals(expectedResponseData, response.data)

    verify(metadataCache, never).getAllTopics
    verify(groupConfigManager, never).groupIds
    verify(clientMetricsManager, never).listClientMetricsResources
  }

  @Test
  def testListConfigResourcesV1WithTopic(): Unit = {
    val request = buildRequest(new ListConfigResourcesRequest.Builder(new ListConfigResourcesRequestData()
      .setResourceTypes(util.List.of(ConfigResource.Type.TOPIC.id))).build(1))
    metadataCache = mock(classOf[KRaftMetadataCache])

    val topics = util.Set.of("topic1", "topic2")
    when(metadataCache.getAllTopics).thenReturn(topics)

    kafkaApis = createKafkaApis()
    kafkaApis.handle(request, RequestLocal.noCaching)
    val response = verifyNoThrottling[ListConfigResourcesResponse](request)
    val expectedResponseData = new ListConfigResourcesResponseData()
        .setConfigResources(
          topics.stream().map(resource =>
            new ListConfigResourcesResponseData.ConfigResource().setResourceName(resource).setResourceType(ConfigResource.Type.TOPIC.id)
          ).collect(java.util.stream.Collectors.toList[ListConfigResourcesResponseData.ConfigResource]))
    assertEquals(expectedResponseData, response.data)

    verify(groupConfigManager, never).groupIds
    verify(clientMetricsManager, never).listClientMetricsResources
    verify(metadataCache, never).getBrokerNodes(any)
  }

  @Test
  def testListConfigResourcesEmptyResponse(): Unit = {
    val request = buildRequest(new ListConfigResourcesRequest.Builder(new ListConfigResourcesRequestData()).build())
    metadataCache = mock(classOf[KRaftMetadataCache])

    when(clientMetricsManager.listClientMetricsResources).thenReturn(util.Set.of)
    when(metadataCache.getAllTopics).thenReturn(util.Set.of)
    when(groupConfigManager.groupIds).thenReturn(util.List.of)
    when(metadataCache.getBrokerNodes(any())).thenReturn(util.List.of)

    kafkaApis = createKafkaApis()
    kafkaApis.handle(request, RequestLocal.noCaching)
    val response = verifyNoThrottling[ListConfigResourcesResponse](request)
    val expectedResponse = new ListConfigResourcesResponseData()
    assertEquals(expectedResponse, response.data)
  }

  @Test
  def testListConfigResourcesV1WithUnknown(): Unit = {
    val request = buildRequest(new ListConfigResourcesRequest.Builder(new ListConfigResourcesRequestData()
      .setResourceTypes(util.List.of(ConfigResource.Type.UNKNOWN.id))).build(1))
    metadataCache = mock(classOf[KRaftMetadataCache])

    kafkaApis = createKafkaApis()
    kafkaApis.handle(request, RequestLocal.noCaching)
    val response = verifyNoThrottling[ListConfigResourcesResponse](request)
    assertEquals(Errors.UNSUPPORTED_VERSION.code(), response.data.errorCode())

    verify(metadataCache, never).getAllTopics
    verify(groupConfigManager, never).groupIds
    verify(clientMetricsManager, never).listClientMetricsResources
    verify(metadataCache, never).getBrokerNodes(any)
  }

  @Test
  def testListConfigResourcesWithException(): Unit = {
    val request = buildRequest(new ListConfigResourcesRequest.Builder(new ListConfigResourcesRequestData()).build())
    metadataCache = new KRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)

    when(clientMetricsManager.listClientMetricsResources).thenThrow(new RuntimeException("test"))
    kafkaApis = createKafkaApis()
    kafkaApis.handle(request, RequestLocal.noCaching)
    val response = verifyNoThrottling[ListConfigResourcesResponse](request)

    val expectedResponse = new ListConfigResourcesResponseData().setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code)
    assertEquals(expectedResponse, response.data)
  }

  @Test
  def testShareGroupHeartbeatReturnsUnsupportedVersion(): Unit = {
    val shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new ShareGroupHeartbeatRequest.Builder(shareGroupHeartbeatRequest).build())
    metadataCache = mock(classOf[KRaftMetadataCache])
    kafkaApis = createKafkaApis(
      featureVersions = Seq(ShareVersion.SV_0)
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val expectedHeartbeatResponse = new ShareGroupHeartbeatResponseData()
      .setErrorCode(Errors.UNSUPPORTED_VERSION.code)
    val response = verifyNoThrottling[ShareGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(expectedHeartbeatResponse, response.data)
  }

  @Test
  def testShareGroupHeartbeatRequest(): Unit = {
    val shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequestData().setGroupId("group").setMemberId(Uuid.randomUuid().toString)

    val requestChannelRequest = buildRequest(new ShareGroupHeartbeatRequest.Builder(shareGroupHeartbeatRequest).build())

    val future = new CompletableFuture[ShareGroupHeartbeatResponseData]()
    when(groupCoordinator.shareGroupHeartbeat(
      requestChannelRequest.context,
      shareGroupHeartbeatRequest
    )).thenReturn(future)
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val shareGroupHeartbeatResponse = new ShareGroupHeartbeatResponseData()
      .setMemberId("member")

    future.complete(shareGroupHeartbeatResponse)
    val response = verifyNoThrottling[ShareGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(shareGroupHeartbeatResponse, response.data)
  }

  @Test
  def testShareGroupHeartbeatRequestGroupAuthorizationFailed(): Unit = {
    val shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new ShareGroupHeartbeatRequest.Builder(shareGroupHeartbeatRequest).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(util.List.of(AuthorizationResult.DENIED))
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    kafkaApis = createKafkaApis(
      authorizer = Some(authorizer),
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val response = verifyNoThrottling[ShareGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, response.data.errorCode)
  }

  @Test
  def testShareGroupHeartbeatRequestTopicAuthorizationFailed(): Unit = {
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    val groupId = "group"
    val fooTopicName = "foo"
    val barTopicName = "bar"
    val zarTopicName = "zar"

    val shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequestData()
      .setGroupId(groupId)
      .setMemberId(Uuid.randomUuid().toString)
      .setSubscribedTopicNames(util.List.of(fooTopicName, barTopicName, zarTopicName))

    val requestChannelRequest = buildRequest(new ShareGroupHeartbeatRequest.Builder(shareGroupHeartbeatRequest).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    val acls = Map(
      groupId -> AuthorizationResult.ALLOWED,
      fooTopicName -> AuthorizationResult.ALLOWED,
      barTopicName -> AuthorizationResult.DENIED,
    )
    when(authorizer.authorize(
      any[RequestContext],
      any[util.List[Action]]
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      actions.stream()
        .map(action => acls.getOrElse(action.resourcePattern.name, AuthorizationResult.DENIED))
        .toList
    }

    kafkaApis = createKafkaApis(
      authorizer = Some(authorizer),
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val response = verifyNoThrottling[ShareGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, response.data.errorCode)
  }

  @Test
  def testShareGroupHeartbeatRequestFutureFailed(): Unit = {
    val shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequestData().setGroupId("group").setMemberId(Uuid.randomUuid().toString)

    val requestChannelRequest = buildRequest(new ShareGroupHeartbeatRequest.Builder(shareGroupHeartbeatRequest).build())

    val future = new CompletableFuture[ShareGroupHeartbeatResponseData]()
    when(groupCoordinator.shareGroupHeartbeat(
      requestChannelRequest.context,
      shareGroupHeartbeatRequest
    )).thenReturn(future)
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    future.completeExceptionally(Errors.FENCED_MEMBER_EPOCH.exception)
    val response = verifyNoThrottling[ShareGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.FENCED_MEMBER_EPOCH.code, response.data.errorCode)
  }

  @Test
  def testShareGroupDescribeSuccess(): Unit = {
    val fooTopicName = "foo"
    val barTopicName = "bar"

    val groupIds = util.List.of("share-group-id-0", "share-group-id-1", "share-group_id-2")

    val member0 = new ShareGroupDescribeResponseData.Member()
      .setMemberId("member0")
      .setAssignment(new ShareGroupDescribeResponseData.Assignment()
        .setTopicPartitions(util.List.of(
          new ShareGroupDescribeResponseData.TopicPartitions().setTopicName(fooTopicName))))

    val member1 = new ShareGroupDescribeResponseData.Member()
      .setMemberId("member1")
      .setAssignment(new ShareGroupDescribeResponseData.Assignment()
        .setTopicPartitions(util.List.of(
          new ShareGroupDescribeResponseData.TopicPartitions().setTopicName(fooTopicName),
          new ShareGroupDescribeResponseData.TopicPartitions().setTopicName(barTopicName))))

    val member2 = new ShareGroupDescribeResponseData.Member()
      .setMemberId("member2")
      .setAssignment(new ShareGroupDescribeResponseData.Assignment()
        .setTopicPartitions(util.List.of(
          new ShareGroupDescribeResponseData.TopicPartitions().setTopicName(barTopicName))))

    val describedGroups: util.List[ShareGroupDescribeResponseData.DescribedGroup] = util.List.of(
      new ShareGroupDescribeResponseData.DescribedGroup().setGroupId(groupIds.get(0)).setMembers(util.List.of(member0)),
      new ShareGroupDescribeResponseData.DescribedGroup().setGroupId(groupIds.get(1)).setMembers(util.List.of(member1)),
      new ShareGroupDescribeResponseData.DescribedGroup().setGroupId(groupIds.get(2)).setMembers(util.List.of(member2))
    )
    getShareGroupDescribeResponse(groupIds, enableShareGroups = true, verifyNoErr = true, null, describedGroups)
  }

  @Test
  def testShareGroupDescribeReturnsUnsupportedVersion(): Unit = {
    val groupIds = util.List.of("share-group-id-0", "share-group-id-1")
    val describedGroups: util.List[ShareGroupDescribeResponseData.DescribedGroup] = util.List.of(
      new ShareGroupDescribeResponseData.DescribedGroup().setGroupId(groupIds.get(0)),
      new ShareGroupDescribeResponseData.DescribedGroup().setGroupId(groupIds.get(1))
    )
    val response = getShareGroupDescribeResponse(groupIds, enableShareGroups = false, verifyNoErr = false, null, describedGroups)
    assertNotNull(response.data)
    assertEquals(2, response.data.groups.size)
    response.data.groups.forEach(group => assertEquals(Errors.UNSUPPORTED_VERSION.code(), group.errorCode()))
  }

  @Test
  def testShareGroupDescribeRequestAuthorizationFailed(): Unit = {
    val groupIds = util.List.of("share-group-id-0", "share-group-id-1")
    val describedGroups: util.List[ShareGroupDescribeResponseData.DescribedGroup] = util.List.of
    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(util.List.of(AuthorizationResult.DENIED))
    val response = getShareGroupDescribeResponse(groupIds, enableShareGroups = true, verifyNoErr = false, authorizer, describedGroups)
    assertNotNull(response.data)
    assertEquals(2, response.data.groups.size)
    response.data.groups.forEach(group => assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code(), group.errorCode()))
  }

  @Test
  def testShareGroupDescribeRequestAuthorizationFailedForOneGroup(): Unit = {
    val groupIds = util.List.of("share-group-id-fail-0", "share-group-id-1")
    val describedGroups: util.List[ShareGroupDescribeResponseData.DescribedGroup] = util.List.of(
      new ShareGroupDescribeResponseData.DescribedGroup().setGroupId(groupIds.get(1))
    )

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(util.List.of(AuthorizationResult.DENIED), util.List.of(AuthorizationResult.ALLOWED))

    val response = getShareGroupDescribeResponse(groupIds, enableShareGroups = true, verifyNoErr = false, authorizer, describedGroups)

    assertNotNull(response.data)
    assertEquals(2, response.data.groups.size)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code(), response.data.groups.get(0).errorCode())
    assertEquals(Errors.NONE.code(), response.data.groups.get(1).errorCode())
  }

  @Test
  def testShareGroupDescribeFilterUnauthorizedTopics(): Unit = {
    val fooTopicName = "foo"
    val barTopicName = "bar"
    val errorMessage = "The group has described topic(s) that the client is not authorized to describe."

    val groupIds = util.List.of("share-group-id-0", "share-group-id-1", "share-group_id-2")

    val authorizer: Authorizer = mock(classOf[Authorizer])
    val acls = Map(
      groupIds.get(0) -> AuthorizationResult.ALLOWED,
      groupIds.get(1) -> AuthorizationResult.ALLOWED,
      groupIds.get(2) -> AuthorizationResult.ALLOWED,
      fooTopicName    -> AuthorizationResult.ALLOWED,
      barTopicName    -> AuthorizationResult.DENIED,
    )
    when(authorizer.authorize(
      any[RequestContext],
      any[util.List[Action]]
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      actions.stream()
        .map(action => acls.getOrElse(action.resourcePattern.name, AuthorizationResult.DENIED))
        .toList
    }
     val member0 = new ShareGroupDescribeResponseData.Member()
      .setMemberId("member0")
      .setAssignment(new ShareGroupDescribeResponseData.Assignment()
        .setTopicPartitions(util.List.of(
          new ShareGroupDescribeResponseData.TopicPartitions().setTopicName(fooTopicName))))

    val member1 = new ShareGroupDescribeResponseData.Member()
      .setMemberId("member1")
      .setAssignment(new ShareGroupDescribeResponseData.Assignment()
        .setTopicPartitions(util.List.of(
          new ShareGroupDescribeResponseData.TopicPartitions().setTopicName(fooTopicName),
          new ShareGroupDescribeResponseData.TopicPartitions().setTopicName(barTopicName))))

    val member2 = new ShareGroupDescribeResponseData.Member()
      .setMemberId("member2")
      .setAssignment(new ShareGroupDescribeResponseData.Assignment()
        .setTopicPartitions(util.List.of(
          new ShareGroupDescribeResponseData.TopicPartitions().setTopicName(barTopicName))))

    val describedGroups: util.List[ShareGroupDescribeResponseData.DescribedGroup] = util.List.of(
      new ShareGroupDescribeResponseData.DescribedGroup()
        .setGroupId(groupIds.get(0))
        .setMembers(util.List.of(member0)),
      new ShareGroupDescribeResponseData.DescribedGroup()
        .setGroupId(groupIds.get(1))
        .setMembers(util.List.of(member1)),
      new ShareGroupDescribeResponseData.DescribedGroup()
        .setGroupId(groupIds.get(2))
        .setMembers(util.List.of(member2)))

    val response = getShareGroupDescribeResponse(groupIds, enableShareGroups = true, verifyNoErr = false, authorizer, describedGroups)

    assertNotNull(response.data)
    assertEquals(3, response.data.groups.size)
    assertEquals(Errors.NONE.code(), response.data.groups.get(0).errorCode())
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code(), response.data.groups.get(1).errorCode())
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code(), response.data.groups.get(2).errorCode())
    assertEquals(errorMessage, response.data.groups.get(1).errorMessage())
    assertEquals(errorMessage, response.data.groups.get(2).errorMessage())
  }

  @Test
  def testReadShareGroupStateSuccess(): Unit = {
    val topicId = Uuid.randomUuid()
    val readRequestData = new ReadShareGroupStateRequestData()
      .setGroupId("group1")
      .setTopics(util.List.of(
        new ReadShareGroupStateRequestData.ReadStateData()
          .setTopicId(topicId)
          .setPartitions(util.List.of(
            new ReadShareGroupStateRequestData.PartitionData()
              .setPartition(1)
              .setLeaderEpoch(1)
          ))
      ))

    val readStateResultData: util.List[ReadShareGroupStateResponseData.ReadStateResult] = util.List.of(
      new ReadShareGroupStateResponseData.ReadStateResult()
        .setTopicId(topicId)
        .setPartitions(util.List.of(
          new ReadShareGroupStateResponseData.PartitionResult()
            .setPartition(1)
            .setErrorCode(Errors.NONE.code())
            .setErrorMessage(null)
            .setStateEpoch(1)
            .setStartOffset(10)
            .setStateBatches(util.List.of(
              new ReadShareGroupStateResponseData.StateBatch()
                .setFirstOffset(11)
                .setLastOffset(15)
                .setDeliveryState(0)
                .setDeliveryCount(1)
            ))
        ))
    )

    val response = getReadShareGroupStateResponse(
      readRequestData,
      ShareCoordinatorTestConfig.testConfigMap().asScala,
      verifyNoErr = true,
      null,
      readStateResultData
    )

    assertNotNull(response.data)
    assertEquals(1, response.data.results.size)
  }

  @Test
  def testReadShareGroupStateAuthorizationFailed(): Unit = {
    val topicId = Uuid.randomUuid()
    val readRequestData = new ReadShareGroupStateRequestData()
      .setGroupId("group1")
      .setTopics(util.List.of(
        new ReadShareGroupStateRequestData.ReadStateData()
          .setTopicId(topicId)
          .setPartitions(util.List.of(
            new ReadShareGroupStateRequestData.PartitionData()
              .setPartition(1)
              .setLeaderEpoch(1)
          ))
      ))

    val readStateResultData: util.List[ReadShareGroupStateResponseData.ReadStateResult] = util.List.of(
      new ReadShareGroupStateResponseData.ReadStateResult()
        .setTopicId(topicId)
        .setPartitions(util.List.of(
          new ReadShareGroupStateResponseData.PartitionResult()
            .setPartition(1)
            .setErrorCode(Errors.NONE.code())
            .setErrorMessage(null)
            .setStateEpoch(1)
            .setStartOffset(10)
            .setStateBatches(util.List.of(
              new ReadShareGroupStateResponseData.StateBatch()
                .setFirstOffset(11)
                .setLastOffset(15)
                .setDeliveryState(0)
                .setDeliveryCount(1)
            ))
        ))
    )

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(util.List.of(AuthorizationResult.DENIED), util.List.of(AuthorizationResult.ALLOWED))

    val response = getReadShareGroupStateResponse(
      readRequestData,
      ShareCoordinatorTestConfig.testConfigMap().asScala,
      verifyNoErr = false,
      authorizer,
      readStateResultData
    )

    assertNotNull(response.data)
    assertEquals(1, response.data.results.size)
    response.data.results.forEach(readResult => {
      assertEquals(1, readResult.partitions.size)
      assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code(), readResult.partitions.get(0).errorCode())
    })
  }

  @Test
  def testReadShareGroupStateSummarySuccess(): Unit = {
    val topicId = Uuid.randomUuid()
    val readSummaryRequestData = new ReadShareGroupStateSummaryRequestData()
      .setGroupId("group1")
      .setTopics(util.List.of(
        new ReadShareGroupStateSummaryRequestData.ReadStateSummaryData()
          .setTopicId(topicId)
          .setPartitions(util.List.of(
            new ReadShareGroupStateSummaryRequestData.PartitionData()
              .setPartition(1)
              .setLeaderEpoch(1)
          ))
      ))

    val readStateSummaryResultData: util.List[ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult] = util.List.of(
      new ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult()
        .setTopicId(topicId)
        .setPartitions(util.List.of(
          new ReadShareGroupStateSummaryResponseData.PartitionResult()
            .setPartition(1)
            .setErrorCode(Errors.NONE.code())
            .setErrorMessage(null)
            .setStateEpoch(1)
            .setStartOffset(10)
            .setDeliveryCompleteCount(0)
        ))
    )

    val response = getReadShareGroupStateSummaryResponse(
      readSummaryRequestData,
      ShareCoordinatorTestConfig.testConfigMap().asScala,
      verifyNoErr = true,
      null,
      readStateSummaryResultData
    )

    assertNotNull(response.data)
    assertEquals(1, response.data.results.size)
  }

  @Test
  def testReadShareGroupStateSummaryAuthorizationFailed(): Unit = {
    val topicId = Uuid.randomUuid()
    val readSummaryRequestData = new ReadShareGroupStateSummaryRequestData()
      .setGroupId("group1")
      .setTopics(util.List.of(
        new ReadShareGroupStateSummaryRequestData.ReadStateSummaryData()
          .setTopicId(topicId)
          .setPartitions(util.List.of(
            new ReadShareGroupStateSummaryRequestData.PartitionData()
              .setPartition(1)
              .setLeaderEpoch(1)
          ))
      ))

    val readStateSummaryResultData: util.List[ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult] = util.List.of(
      new ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult()
        .setTopicId(topicId)
        .setPartitions(util.List.of(
          new ReadShareGroupStateSummaryResponseData.PartitionResult()
            .setPartition(1)
            .setErrorCode(Errors.NONE.code())
            .setErrorMessage(null)
            .setStateEpoch(1)
            .setStartOffset(10)
            .setDeliveryCompleteCount(0)
        ))
    )

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(util.List.of(AuthorizationResult.DENIED), util.List.of(AuthorizationResult.ALLOWED))

    val response = getReadShareGroupStateSummaryResponse(
      readSummaryRequestData,
      ShareCoordinatorTestConfig.testConfigMap().asScala,
      verifyNoErr = false,
      authorizer,
      readStateSummaryResultData
    )

    assertNotNull(response.data)
    assertEquals(1, response.data.results.size)
    response.data.results.forEach(readResult => {
      assertEquals(1, readResult.partitions.size)
      assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code(), readResult.partitions.get(0).errorCode())
    })
  }

  @Test
  def testDescribeShareGroupOffsetsReturnsUnsupportedVersion(): Unit = {
    val describeShareGroupOffsetsRequest = new DescribeShareGroupOffsetsRequestData().setGroups(
      util.List.of(new DescribeShareGroupOffsetsRequestGroup().setGroupId("group").setTopics(
        util.List.of(new DescribeShareGroupOffsetsRequestTopic().setTopicName("topic-1").setPartitions(util.List.of(1)))
      ))
    )

    val requestChannelRequest = buildRequest(new DescribeShareGroupOffsetsRequest.Builder(describeShareGroupOffsetsRequest).build())
    metadataCache = new KRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val response = verifyNoThrottling[DescribeShareGroupOffsetsResponse](requestChannelRequest)
    response.data.groups.forEach(group => group.topics.forEach(topic => topic.partitions.forEach(partition => assertEquals(Errors.UNSUPPORTED_VERSION.code, partition.errorCode))))
  }

  @Test
  def testDescribeShareGroupOffsetsRequestGroupAuthorizationFailed(): Unit = {
    val describeShareGroupOffsetsRequest = new DescribeShareGroupOffsetsRequestData().setGroups(
      util.List.of(new DescribeShareGroupOffsetsRequestGroup().setGroupId("group").setTopics(
        util.List.of(new DescribeShareGroupOffsetsRequestTopic().setTopicName("topic-1").setPartitions(util.List.of(1)))
      ))
    )

    val requestChannelRequest = buildRequest(new DescribeShareGroupOffsetsRequest.Builder(describeShareGroupOffsetsRequest).build)

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(util.List.of(AuthorizationResult.DENIED))
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    kafkaApis = createKafkaApis(
      authorizer = Some(authorizer),
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val response = verifyNoThrottling[DescribeShareGroupOffsetsResponse](requestChannelRequest)
    response.data.groups.forEach(
      group => group.topics.forEach(
        topic => topic.partitions.forEach(
          partition => assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, partition.errorCode)
        )
      )
    )
  }

  @Test
  def testDescribeShareGroupAllOffsetsRequestGroupAuthorizationFailed(): Unit = {
    val describeShareGroupOffsetsRequest = new DescribeShareGroupOffsetsRequestData().setGroups(
      util.List.of(new DescribeShareGroupOffsetsRequestGroup().setGroupId("group").setTopics(null))
    )

    val requestChannelRequest = buildRequest(new DescribeShareGroupOffsetsRequest.Builder(describeShareGroupOffsetsRequest).build)

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(util.List.of(AuthorizationResult.DENIED))
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    kafkaApis = createKafkaApis(
      authorizer = Some(authorizer),
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val response = verifyNoThrottling[DescribeShareGroupOffsetsResponse](requestChannelRequest)
    response.data.groups.forEach(
      group => group.topics.forEach(
        topic => topic.partitions.forEach(
          partition => assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, partition.errorCode)
        )
      )
    )
  }

  @Test
  def testDescribeShareGroupOffsetsRequestSuccess(): Unit = {
    val topicName1 = "topic-1"
    val topicId1 = Uuid.randomUuid
    val topicName2 = "topic-2"
    val topicId2 = Uuid.randomUuid
    val topicName3 = "topic-3"
    val topicId3 = Uuid.randomUuid
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName1, 1, topicId = topicId1)
    addTopicToMetadataCache(topicName2, 1, topicId = topicId2)
    addTopicToMetadataCache(topicName3, 1, topicId = topicId3)

    val describeShareGroupOffsetsRequestGroup1 = new DescribeShareGroupOffsetsRequestGroup().setGroupId("group1").setTopics(
      util.List.of(
        new DescribeShareGroupOffsetsRequestTopic().setTopicName(topicName1).setPartitions(util.List.of(1, 2, 3)),
        new DescribeShareGroupOffsetsRequestTopic().setTopicName(topicName2).setPartitions(util.List.of(10, 20)),
      )
    )

    val describeShareGroupOffsetsRequestGroup2 = new DescribeShareGroupOffsetsRequestGroup().setGroupId("group2").setTopics(
      util.List.of(
        new DescribeShareGroupOffsetsRequestTopic().setTopicName(topicName3).setPartitions(util.List.of(0)),
      )
    )

    val describeShareGroupOffsetsRequest = new DescribeShareGroupOffsetsRequestData()
      .setGroups(util.List.of(describeShareGroupOffsetsRequestGroup1, describeShareGroupOffsetsRequestGroup2))

    val requestChannelRequest = buildRequest(new DescribeShareGroupOffsetsRequest.Builder(describeShareGroupOffsetsRequest).build)

    val futureGroup1 = new CompletableFuture[DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup]
    when(groupCoordinator.describeShareGroupOffsets(
      requestChannelRequest.context,
      describeShareGroupOffsetsRequestGroup1
    )).thenReturn(futureGroup1)
    val futureGroup2 = new CompletableFuture[DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup]
    when(groupCoordinator.describeShareGroupOffsets(
      requestChannelRequest.context,
      describeShareGroupOffsetsRequestGroup2
    )).thenReturn(futureGroup2)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val describeShareGroupOffsetsResponseGroup1 = new DescribeShareGroupOffsetsResponseGroup()
      .setGroupId("group1")
      .setTopics(util.List.of(
        new DescribeShareGroupOffsetsResponseTopic()
          .setTopicName(topicName1)
          .setTopicId(topicId1)
          .setPartitions(util.List.of(
            new DescribeShareGroupOffsetsResponsePartition()
              .setPartitionIndex(1)
              .setStartOffset(0)
              .setLag(0)
              .setLeaderEpoch(1)
              .setErrorMessage(null)
              .setErrorCode(0),
            new DescribeShareGroupOffsetsResponsePartition()
              .setPartitionIndex(2)
              .setStartOffset(0)
              .setLag(0)
              .setLeaderEpoch(1)
              .setErrorMessage(null)
              .setErrorCode(0),
            new DescribeShareGroupOffsetsResponsePartition()
              .setPartitionIndex(3)
              .setStartOffset(0)
              .setLag(0)
              .setLeaderEpoch(1)
              .setErrorMessage(null)
              .setErrorCode(0)
          )),
        new DescribeShareGroupOffsetsResponseTopic()
          .setTopicName(topicName2)
          .setTopicId(topicId2)
          .setPartitions(util.List.of(
            new DescribeShareGroupOffsetsResponsePartition()
              .setPartitionIndex(10)
              .setStartOffset(0)
              .setLag(0)
              .setLeaderEpoch(1)
              .setErrorMessage(null)
              .setErrorCode(0),
            new DescribeShareGroupOffsetsResponsePartition()
              .setPartitionIndex(20)
              .setStartOffset(0)
              .setLag(0)
              .setLeaderEpoch(1)
              .setErrorMessage(null)
              .setErrorCode(0)
          ))
      ))

    val describeShareGroupOffsetsResponseGroup2 = new DescribeShareGroupOffsetsResponseGroup()
      .setGroupId("group2")
      .setTopics(util.List.of(
        new DescribeShareGroupOffsetsResponseTopic()
          .setTopicName(topicName3)
          .setTopicId(topicId3)
          .setPartitions(util.List.of(
            new DescribeShareGroupOffsetsResponsePartition()
              .setPartitionIndex(0)
              .setStartOffset(0)
              .setLag(0)
              .setLeaderEpoch(1)
              .setErrorMessage(null)
              .setErrorCode(0)
          ))
      ))

    val describeShareGroupOffsetsResponse = new DescribeShareGroupOffsetsResponseData()
      .setGroups(util.List.of(describeShareGroupOffsetsResponseGroup1, describeShareGroupOffsetsResponseGroup2))

    futureGroup1.complete(describeShareGroupOffsetsResponseGroup1)
    futureGroup2.complete(describeShareGroupOffsetsResponseGroup2)
    val response = verifyNoThrottling[DescribeShareGroupOffsetsResponse](requestChannelRequest)
    assertEquals(describeShareGroupOffsetsResponse, response.data)
  }

  @Test
  def testDescribeShareGroupOffsetsRequestTopicAuthorizationFailed(): Unit = {
    val topicName1 = "topic-1"
    val topicId1 = Uuid.randomUuid
    val topicName2 = "topic-2"
    val topicId2 = Uuid.randomUuid
    val topicName3 = "topic-3"
    val topicId3 = Uuid.randomUuid
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName1, 1, topicId = topicId1)
    addTopicToMetadataCache(topicName2, 1, topicId = topicId2)
    addTopicToMetadataCache(topicName3, 1, topicId = topicId3)

    val describeShareGroupOffsetsRequestGroup1 = new DescribeShareGroupOffsetsRequestGroup().setGroupId("group1").setTopics(
      util.List.of(
        new DescribeShareGroupOffsetsRequestTopic().setTopicName(topicName1).setPartitions(util.List.of(1, 2, 3)),
        new DescribeShareGroupOffsetsRequestTopic().setTopicName(topicName2).setPartitions(util.List.of(10, 20)),
      )
    )

    val describeShareGroupOffsetsRequestGroup2 = new DescribeShareGroupOffsetsRequestGroup().setGroupId("group2").setTopics(
      util.List.of(
        new DescribeShareGroupOffsetsRequestTopic().setTopicName(topicName3).setPartitions(util.List.of(0)),
      )
    )

    val describeShareGroupOffsetsRequest = new DescribeShareGroupOffsetsRequestData()
      .setGroups(util.List.of(describeShareGroupOffsetsRequestGroup1, describeShareGroupOffsetsRequestGroup2))

    val requestChannelRequest = buildRequest(new DescribeShareGroupOffsetsRequest.Builder(describeShareGroupOffsetsRequest).build)

    // The group coordinator will only be asked for information about topics which are authorized
    val futureGroup1 = new CompletableFuture[DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup]
    when(groupCoordinator.describeShareGroupOffsets(
      requestChannelRequest.context,
      new DescribeShareGroupOffsetsRequestGroup().setGroupId("group1").setTopics(
        util.List.of(
          new DescribeShareGroupOffsetsRequestTopic().setTopicName(topicName1).setPartitions(util.List.of(1, 2, 3)),
        )
      )
    )).thenReturn(futureGroup1)

    val futureGroup2 = new CompletableFuture[DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup]
    when(groupCoordinator.describeShareGroupOffsets(
      requestChannelRequest.context,
      new DescribeShareGroupOffsetsRequestGroup().setGroupId("group2").setTopics(
        util.List.of(
        )
      )
    )).thenReturn(futureGroup2)

    val authorizer: Authorizer = mock(classOf[Authorizer])
    val acls = Map(
      "group1" -> AuthorizationResult.ALLOWED,
      "group2" -> AuthorizationResult.ALLOWED,
      topicName1 -> AuthorizationResult.ALLOWED,
      topicName2 -> AuthorizationResult.DENIED,
      topicName3 -> AuthorizationResult.DENIED
    )
    when(authorizer.authorize(
      any[RequestContext],
      any[util.List[Action]]
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      actions.stream()
        .map(action => acls.getOrElse(action.resourcePattern.name, AuthorizationResult.DENIED))
        .toList
    }
    kafkaApis = createKafkaApis(
      authorizer = Some(authorizer)
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    // These are the responses to the KafkaApis request, complete with authorization errors
    val describeShareGroupOffsetsResponseGroup1 = new DescribeShareGroupOffsetsResponseGroup()
      .setGroupId("group1")
      .setTopics(util.List.of(
        new DescribeShareGroupOffsetsResponseTopic()
          .setTopicName(topicName1)
          .setTopicId(topicId1)
          .setPartitions(util.List.of(
            new DescribeShareGroupOffsetsResponsePartition()
              .setPartitionIndex(1)
              .setStartOffset(0)
              .setLag(0)
              .setLeaderEpoch(1)
              .setErrorMessage(null)
              .setErrorCode(0),
            new DescribeShareGroupOffsetsResponsePartition()
              .setPartitionIndex(2)
              .setStartOffset(0)
              .setLag(0)
              .setLeaderEpoch(1)
              .setErrorMessage(null)
              .setErrorCode(0),
            new DescribeShareGroupOffsetsResponsePartition()
              .setPartitionIndex(3)
              .setStartOffset(0)
              .setLag(0)
              .setLeaderEpoch(1)
              .setErrorMessage(null)
              .setErrorCode(0)
          )),
        new DescribeShareGroupOffsetsResponseTopic()
          .setTopicName(topicName2)
          .setTopicId(Uuid.ZERO_UUID)
          .setPartitions(util.List.of(
            new DescribeShareGroupOffsetsResponsePartition()
              .setPartitionIndex(10)
              .setStartOffset(-1)
              .setLag(-1)
              .setLeaderEpoch(0)
              .setErrorMessage(Errors.TOPIC_AUTHORIZATION_FAILED.message)
              .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code),
            new DescribeShareGroupOffsetsResponsePartition()
              .setPartitionIndex(20)
              .setStartOffset(-1)
              .setLag(-1)
              .setLeaderEpoch(0)
              .setErrorMessage(Errors.TOPIC_AUTHORIZATION_FAILED.message)
              .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
          ))
      ))

    val describeShareGroupOffsetsResponseGroup2 = new DescribeShareGroupOffsetsResponseGroup()
      .setGroupId("group2")
      .setTopics(util.List.of(
        new DescribeShareGroupOffsetsResponseTopic()
          .setTopicName(topicName3)
          .setTopicId(Uuid.ZERO_UUID)
          .setPartitions(util.List.of(
            new DescribeShareGroupOffsetsResponsePartition()
              .setPartitionIndex(0)
              .setStartOffset(-1)
              .setLag(-1)
              .setLeaderEpoch(0)
              .setErrorMessage(Errors.TOPIC_AUTHORIZATION_FAILED.message)
              .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
          ))
      ))

    val describeShareGroupOffsetsResponse = new DescribeShareGroupOffsetsResponseData()
      .setGroups(util.List.of(describeShareGroupOffsetsResponseGroup1, describeShareGroupOffsetsResponseGroup2))

    // And these are the responses to the topics which were authorized
    val describeShareGroupOffsetsGroupCoordinatorResponseGroup1 = new DescribeShareGroupOffsetsResponseGroup()
      .setGroupId("group1")
      .setTopics(util.List.of(
        new DescribeShareGroupOffsetsResponseTopic()
          .setTopicName(topicName1)
          .setTopicId(topicId1)
          .setPartitions(util.List.of(
            new DescribeShareGroupOffsetsResponsePartition()
              .setPartitionIndex(1)
              .setStartOffset(0)
              .setLag(0)
              .setLeaderEpoch(1)
              .setErrorMessage(null)
              .setErrorCode(0),
            new DescribeShareGroupOffsetsResponsePartition()
              .setPartitionIndex(2)
              .setStartOffset(0)
              .setLag(0)
              .setLeaderEpoch(1)
              .setErrorMessage(null)
              .setErrorCode(0),
            new DescribeShareGroupOffsetsResponsePartition()
              .setPartitionIndex(3)
              .setStartOffset(0)
              .setLag(0)
              .setLeaderEpoch(1)
              .setErrorMessage(null)
              .setErrorCode(0)
          ))
      ))

    val describeShareGroupOffsetsGroupCoordinatorResponseGroup2 = new DescribeShareGroupOffsetsResponseGroup()
      .setGroupId("group2")
      .setTopics(util.List.of())

    futureGroup1.complete(describeShareGroupOffsetsGroupCoordinatorResponseGroup1)
    futureGroup2.complete(describeShareGroupOffsetsGroupCoordinatorResponseGroup2)
    val response = verifyNoThrottling[DescribeShareGroupOffsetsResponse](requestChannelRequest)
    assertEquals(describeShareGroupOffsetsResponse, response.data)
  }

  @Test
  def testDescribeShareGroupAllOffsetsRequestTopicAuthorizationFailed(): Unit = {
    val topicName1 = "topic-1"
    val topicId1 = Uuid.randomUuid
    val topicName2 = "topic-2"
    val topicId2 = Uuid.randomUuid
    val topicName3 = "topic-3"
    val topicId3 = Uuid.randomUuid
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName1, 1, topicId = topicId1)
    addTopicToMetadataCache(topicName2, 1, topicId = topicId2)
    addTopicToMetadataCache(topicName3, 1, topicId = topicId3)

    val describeShareGroupOffsetsRequestGroup1 = new DescribeShareGroupOffsetsRequestGroup().setGroupId("group1").setTopics(null)

    val describeShareGroupOffsetsRequestGroup2 = new DescribeShareGroupOffsetsRequestGroup().setGroupId("group2").setTopics(null)

    val describeShareGroupOffsetsRequest = new DescribeShareGroupOffsetsRequestData()
      .setGroups(util.List.of(describeShareGroupOffsetsRequestGroup1, describeShareGroupOffsetsRequestGroup2))

    val requestChannelRequest = buildRequest(new DescribeShareGroupOffsetsRequest.Builder(describeShareGroupOffsetsRequest).build)

    // The group coordinator is being asked for information about all topics, not just those which are authorized
    val futureGroup1 = new CompletableFuture[DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup]
    when(groupCoordinator.describeShareGroupAllOffsets(
      requestChannelRequest.context,
      new DescribeShareGroupOffsetsRequestGroup().setGroupId("group1").setTopics(null)
    )).thenReturn(futureGroup1)

    val futureGroup2 = new CompletableFuture[DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup]
    when(groupCoordinator.describeShareGroupAllOffsets(
      requestChannelRequest.context,
      new DescribeShareGroupOffsetsRequestGroup().setGroupId("group2").setTopics(null)
    )).thenReturn(futureGroup2)

    val authorizer: Authorizer = mock(classOf[Authorizer])
    val acls = Map(
      "group1" -> AuthorizationResult.ALLOWED,
      "group2" -> AuthorizationResult.ALLOWED,
      topicName1 -> AuthorizationResult.ALLOWED,
      topicName2 -> AuthorizationResult.DENIED,
      topicName3 -> AuthorizationResult.DENIED
    )
    when(authorizer.authorize(
      any[RequestContext],
      any[util.List[Action]]
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      actions.stream()
        .map(action => acls.getOrElse(action.resourcePattern.name, AuthorizationResult.DENIED))
        .toList
    }
    kafkaApis = createKafkaApis(
      authorizer = Some(authorizer)
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    // These are the responses to the KafkaApis request, with unauthorized topics filtered out
    val describeShareGroupOffsetsResponseGroup1 = new DescribeShareGroupOffsetsResponseGroup()
      .setGroupId("group1")
      .setTopics(util.List.of(
        new DescribeShareGroupOffsetsResponseTopic()
          .setTopicName(topicName1)
          .setTopicId(topicId1)
          .setPartitions(util.List.of(
            new DescribeShareGroupOffsetsResponsePartition()
              .setPartitionIndex(1)
              .setStartOffset(0)
              .setLag(0)
              .setLeaderEpoch(1)
              .setErrorMessage(null)
              .setErrorCode(0),
            new DescribeShareGroupOffsetsResponsePartition()
              .setPartitionIndex(2)
              .setStartOffset(0)
              .setLag(0)
              .setLeaderEpoch(1)
              .setErrorMessage(null)
              .setErrorCode(0),
            new DescribeShareGroupOffsetsResponsePartition()
              .setPartitionIndex(3)
              .setStartOffset(0)
              .setLag(0)
              .setLeaderEpoch(1)
              .setErrorMessage(null)
              .setErrorCode(0)
          ))
      ))

    val describeShareGroupOffsetsResponseGroup2 = new DescribeShareGroupOffsetsResponseGroup()
      .setGroupId("group2")
      .setTopics(util.List.of())

    // And these are the responses from the group coordinator for all topics, even those which are not authorized
    val describeShareGroupOffsetsGroupCoordinatorResponseGroup1 = new DescribeShareGroupOffsetsResponseGroup()
      .setGroupId("group1")
      .setTopics(util.List.of(
        new DescribeShareGroupOffsetsResponseTopic()
          .setTopicName(topicName1)
          .setTopicId(topicId1)
          .setPartitions(util.List.of(
            new DescribeShareGroupOffsetsResponsePartition()
              .setPartitionIndex(1)
              .setStartOffset(0)
              .setLag(0)
              .setLeaderEpoch(1)
              .setErrorMessage(null)
              .setErrorCode(0),
            new DescribeShareGroupOffsetsResponsePartition()
              .setPartitionIndex(2)
              .setStartOffset(0)
              .setLag(0)
              .setLeaderEpoch(1)
              .setErrorMessage(null)
              .setErrorCode(0),
            new DescribeShareGroupOffsetsResponsePartition()
              .setPartitionIndex(3)
              .setStartOffset(0)
              .setLag(0)
              .setLeaderEpoch(1)
              .setErrorMessage(null)
              .setErrorCode(0)
          )),
        new DescribeShareGroupOffsetsResponseTopic()
          .setTopicName(topicName2)
          .setTopicId(topicId2)
          .setPartitions(util.List.of(
            new DescribeShareGroupOffsetsResponsePartition()
              .setPartitionIndex(10)
              .setStartOffset(0)
              .setLag(0)
              .setLeaderEpoch(1)
              .setErrorMessage(null)
              .setErrorCode(0),
            new DescribeShareGroupOffsetsResponsePartition()
              .setPartitionIndex(20)
              .setStartOffset(0)
              .setLag(0)
              .setLeaderEpoch(1)
              .setErrorMessage(null)
              .setErrorCode(0)
          ))
      ))

    val describeShareGroupOffsetsGroupCoordinatorResponseGroup2 = new DescribeShareGroupOffsetsResponseGroup()
      .setGroupId("group2")
      .setTopics(util.List.of(
        new DescribeShareGroupOffsetsResponseTopic()
          .setTopicName(topicName3)
          .setTopicId(topicId3)
          .setPartitions(util.List.of(
            new DescribeShareGroupOffsetsResponsePartition()
              .setPartitionIndex(0)
              .setStartOffset(0)
              .setLag(0)
              .setLeaderEpoch(1)
              .setErrorMessage(null)
              .setErrorCode(0)
          ))
      ))

    val describeShareGroupOffsetsResponse = new DescribeShareGroupOffsetsResponseData()
      .setGroups(util.List.of(describeShareGroupOffsetsResponseGroup1, describeShareGroupOffsetsResponseGroup2))

    futureGroup1.complete(describeShareGroupOffsetsGroupCoordinatorResponseGroup1)
    futureGroup2.complete(describeShareGroupOffsetsGroupCoordinatorResponseGroup2)
    val response = verifyNoThrottling[DescribeShareGroupOffsetsResponse](requestChannelRequest)
    assertEquals(describeShareGroupOffsetsResponse, response.data)
  }

  @Test
  def testDescribeShareGroupAllOffsetsRequestSuccess(): Unit = {
    val topicName1 = "topic-1"
    val topicId1 = Uuid.randomUuid
    val topicName2 = "topic-2"
    val topicId2 = Uuid.randomUuid
    val topicName3 = "topic-3"
    val topicId3 = Uuid.randomUuid
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName1, 1, topicId = topicId1)
    addTopicToMetadataCache(topicName2, 1, topicId = topicId2)
    addTopicToMetadataCache(topicName3, 1, topicId = topicId3)

    val describeShareGroupOffsetsRequestGroup1 = new DescribeShareGroupOffsetsRequestGroup().setGroupId("group1").setTopics(null)

    val describeShareGroupOffsetsRequestGroup2 = new DescribeShareGroupOffsetsRequestGroup().setGroupId("group2").setTopics(null)

    val describeShareGroupOffsetsRequest = new DescribeShareGroupOffsetsRequestData()
      .setGroups(util.List.of(describeShareGroupOffsetsRequestGroup1, describeShareGroupOffsetsRequestGroup2))

    val requestChannelRequest = buildRequest(new DescribeShareGroupOffsetsRequest.Builder(describeShareGroupOffsetsRequest).build)

    val futureGroup1 = new CompletableFuture[DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup]
    when(groupCoordinator.describeShareGroupAllOffsets(
      requestChannelRequest.context,
      describeShareGroupOffsetsRequestGroup1
    )).thenReturn(futureGroup1)
    val futureGroup2 = new CompletableFuture[DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup]
    when(groupCoordinator.describeShareGroupAllOffsets(
      requestChannelRequest.context,
      describeShareGroupOffsetsRequestGroup2
    )).thenReturn(futureGroup2)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val describeShareGroupOffsetsResponseGroup1 = new DescribeShareGroupOffsetsResponseGroup()
      .setGroupId("group1")
      .setTopics(util.List.of(
        new DescribeShareGroupOffsetsResponseTopic()
          .setTopicName(topicName1)
          .setTopicId(topicId1)
          .setPartitions(util.List.of(
            new DescribeShareGroupOffsetsResponsePartition()
              .setPartitionIndex(1)
              .setStartOffset(0)
              .setLag(0)
              .setLeaderEpoch(1)
              .setErrorMessage(null)
              .setErrorCode(0),
            new DescribeShareGroupOffsetsResponsePartition()
              .setPartitionIndex(2)
              .setStartOffset(0)
              .setLag(0)
              .setLeaderEpoch(1)
              .setErrorMessage(null)
              .setErrorCode(0),
            new DescribeShareGroupOffsetsResponsePartition()
              .setPartitionIndex(3)
              .setStartOffset(0)
              .setLag(0)
              .setLeaderEpoch(1)
              .setErrorMessage(null)
              .setErrorCode(0)
          )),
        new DescribeShareGroupOffsetsResponseTopic()
          .setTopicName(topicName2)
          .setTopicId(topicId2)
          .setPartitions(util.List.of(
            new DescribeShareGroupOffsetsResponsePartition()
              .setPartitionIndex(10)
              .setStartOffset(0)
              .setLag(0)
              .setLeaderEpoch(1)
              .setErrorMessage(null)
              .setErrorCode(0),
            new DescribeShareGroupOffsetsResponsePartition()
              .setPartitionIndex(20)
              .setStartOffset(0)
              .setLag(0)
              .setLeaderEpoch(1)
              .setErrorMessage(null)
              .setErrorCode(0)
          ))
      ))

    val describeShareGroupOffsetsResponseGroup2 = new DescribeShareGroupOffsetsResponseGroup()
      .setGroupId("group2")
      .setTopics(util.List.of(
        new DescribeShareGroupOffsetsResponseTopic()
          .setTopicName(topicName3)
          .setTopicId(topicId3)
          .setPartitions(util.List.of(
            new DescribeShareGroupOffsetsResponsePartition()
              .setPartitionIndex(0)
              .setStartOffset(0)
              .setLag(0)
              .setLeaderEpoch(1)
              .setErrorMessage(null)
              .setErrorCode(0)
          ))
      ))

    val describeShareGroupOffsetsResponse = new DescribeShareGroupOffsetsResponseData()
      .setGroups(util.List.of(describeShareGroupOffsetsResponseGroup1, describeShareGroupOffsetsResponseGroup2))

    futureGroup1.complete(describeShareGroupOffsetsResponseGroup1)
    futureGroup2.complete(describeShareGroupOffsetsResponseGroup2)
    val response = verifyNoThrottling[DescribeShareGroupOffsetsResponse](requestChannelRequest)
    assertEquals(describeShareGroupOffsetsResponse, response.data)
  }

  @Test
  def testDescribeShareGroupOffsetsRequestEmptyGroupsSuccess(): Unit = {
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()

    val describeShareGroupOffsetsRequest = new DescribeShareGroupOffsetsRequestData

    val requestChannelRequest = buildRequest(new DescribeShareGroupOffsetsRequest.Builder(describeShareGroupOffsetsRequest).build)

    val future = new CompletableFuture[DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup]
    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val describeShareGroupOffsetsResponseGroup = new DescribeShareGroupOffsetsResponseGroup()

    val describeShareGroupOffsetsResponse = new DescribeShareGroupOffsetsResponseData()

    future.complete(describeShareGroupOffsetsResponseGroup)
    val response = verifyNoThrottling[DescribeShareGroupOffsetsResponse](requestChannelRequest)
    assertEquals(describeShareGroupOffsetsResponse, response.data)
  }

  @Test
  def testDescribeShareGroupOffsetsRequestEmptyTopicsSuccess(): Unit = {
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()

    val describeShareGroupOffsetsRequestGroup = new DescribeShareGroupOffsetsRequestGroup().setGroupId("group")

    val describeShareGroupOffsetsRequest = new DescribeShareGroupOffsetsRequestData().setGroups(util.List.of(describeShareGroupOffsetsRequestGroup))

    val requestChannelRequest = buildRequest(new DescribeShareGroupOffsetsRequest.Builder(describeShareGroupOffsetsRequest).build)

    val future = new CompletableFuture[DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup]
    when(groupCoordinator.describeShareGroupOffsets(
      requestChannelRequest.context,
      describeShareGroupOffsetsRequestGroup
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val describeShareGroupOffsetsResponseGroup = new DescribeShareGroupOffsetsResponseGroup()
      .setGroupId("group")
      .setTopics(util.List.of())

    val describeShareGroupOffsetsResponse = new DescribeShareGroupOffsetsResponseData().setGroups(util.List.of(describeShareGroupOffsetsResponseGroup))

    future.complete(describeShareGroupOffsetsResponseGroup)
    val response = verifyNoThrottling[DescribeShareGroupOffsetsResponse](requestChannelRequest)
    assertEquals(describeShareGroupOffsetsResponse, response.data)
  }

  @Test
  def testDeleteShareGroupOffsetsReturnsUnsupportedVersion(): Unit = {
    val deleteShareGroupOffsetsRequest = new DeleteShareGroupOffsetsRequestData()
      .setGroupId("group")
      .setTopics(util.List.of(new DeleteShareGroupOffsetsRequestTopic().setTopicName("topic-1")))

    val requestChannelRequest = buildRequest(new DeleteShareGroupOffsetsRequest.Builder(deleteShareGroupOffsetsRequest).build())
    metadataCache = new KRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val response = verifyNoThrottling[DeleteShareGroupOffsetsResponse](requestChannelRequest)
    response.data.responses.forEach(topic => assertEquals(Errors.UNSUPPORTED_VERSION.code, topic.errorCode))
  }

  @Test
  def testDeleteShareGroupOffsetsRequestsGroupAuthorizationFailed(): Unit = {
    val deleteShareGroupOffsetsRequest = new DeleteShareGroupOffsetsRequestData()
      .setGroupId("group")
      .setTopics(util.List.of(new DeleteShareGroupOffsetsRequestTopic().setTopicName("topic-1")))

    val requestChannelRequest = buildRequest(new DeleteShareGroupOffsetsRequest.Builder(deleteShareGroupOffsetsRequest).build)

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(util.List.of(AuthorizationResult.DENIED))
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    kafkaApis = createKafkaApis(
      authorizer = Some(authorizer),
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val response = verifyNoThrottling[DeleteShareGroupOffsetsResponse](requestChannelRequest)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, response.data.errorCode)
  }

  @Test
  def testDeleteShareGroupOffsetsRequestsTopicAuthorizationFailed(): Unit = {

    def buildExpectedActionsTopic(topic: String): util.List[Action] = {
      val pattern = new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL)
      val action = new Action(AclOperation.READ, pattern, 1, true, true)
      util.List.of(action)
    }

    def buildExpectedActionsGroup(topic: String): util.List[Action] = {
      val pattern = new ResourcePattern(ResourceType.GROUP, topic, PatternType.LITERAL)
      val action = new Action(AclOperation.DELETE, pattern, 1, true, true)
      util.List.of(action)
    }

    val groupId = "group"

    val topicName1 = "topic-1"
    val topicId1 = Uuid.randomUuid
    val topicName2 = "topic-2"
    val topicId2 = Uuid.randomUuid
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName1, 2, topicId = topicId1)
    addTopicToMetadataCache(topicName2, 2, topicId = topicId2)

    val deleteShareGroupOffsetsRequestTopic1 = new DeleteShareGroupOffsetsRequestTopic()
      .setTopicName(topicName1)

    val deleteShareGroupOffsetsRequestTopic2 = new DeleteShareGroupOffsetsRequestTopic()
      .setTopicName(topicName2)

    val deleteShareGroupOffsetsRequestData = new DeleteShareGroupOffsetsRequestData()
      .setGroupId(groupId)
      .setTopics(util.List.of(deleteShareGroupOffsetsRequestTopic1, deleteShareGroupOffsetsRequestTopic2))

    val deleteShareGroupOffsetsGroupCoordinatorRequestData = new DeleteShareGroupOffsetsRequestData()
      .setGroupId(groupId)
      .setTopics(util.List.of(deleteShareGroupOffsetsRequestTopic2))

    val requestChannelRequest = buildRequest(new DeleteShareGroupOffsetsRequest.Builder(deleteShareGroupOffsetsRequestData).build)

    val resultFuture = new CompletableFuture[DeleteShareGroupOffsetsResponseData]
    when(groupCoordinator.deleteShareGroupOffsets(
      requestChannelRequest.context,
      deleteShareGroupOffsetsGroupCoordinatorRequestData
    )).thenReturn(resultFuture)

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(buildExpectedActionsGroup(groupId))))
      .thenReturn(util.List.of(AuthorizationResult.ALLOWED))
    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(buildExpectedActionsTopic(topicName1))))
      .thenReturn(util.List.of(AuthorizationResult.DENIED))
    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(buildExpectedActionsTopic(topicName2))))
      .thenReturn(util.List.of(AuthorizationResult.ALLOWED))

    kafkaApis = createKafkaApis(
      authorizer = Some(authorizer)
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val deleteShareGroupOffsetsResponseData = new DeleteShareGroupOffsetsResponseData()
      .setErrorMessage(null)
      .setErrorCode(Errors.NONE.code())
      .setResponses(util.List.of(
        new DeleteShareGroupOffsetsResponseTopic()
          .setTopicName(topicName2)
          .setTopicId(topicId2)
          .setErrorMessage(null)
          .setErrorCode(Errors.NONE.code())
          )
      )

    val expectedResponseTopics: util.List[DeleteShareGroupOffsetsResponseTopic] = new util.ArrayList[DeleteShareGroupOffsetsResponseTopic]()

    expectedResponseTopics.add(
      new DeleteShareGroupOffsetsResponseTopic()
        .setTopicName(topicName1)
        .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
        .setErrorMessage(Errors.TOPIC_AUTHORIZATION_FAILED.message())
    )

    deleteShareGroupOffsetsResponseData.responses.forEach{ topic => {
      expectedResponseTopics.add(topic)
    }}

    val expectedResponseData: DeleteShareGroupOffsetsResponseData = new DeleteShareGroupOffsetsResponseData()
      .setErrorCode(Errors.NONE.code())
      .setErrorMessage(null)
      .setResponses(expectedResponseTopics)

    resultFuture.complete(deleteShareGroupOffsetsResponseData)
    val response = verifyNoThrottling[DeleteShareGroupOffsetsResponse](requestChannelRequest)
    assertEquals(expectedResponseData, response.data)
  }

  @Test
  def testDeleteShareGroupOffsetsRequestSuccess(): Unit = {
    val topicName1 = "topic-1"
    val topicId1 = Uuid.randomUuid
    val topicName2 = "topic-2"
    val topicId2 = Uuid.randomUuid
    val topicName3 = "topic-3"
    val topicId3 = Uuid.randomUuid
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName1, 1, topicId = topicId1)
    addTopicToMetadataCache(topicName2, 2, topicId = topicId2)
    addTopicToMetadataCache(topicName3, 3, topicId = topicId3)

    val deleteShareGroupOffsetsRequestTopic1 = new DeleteShareGroupOffsetsRequestTopic()
      .setTopicName(topicName1)

    val deleteShareGroupOffsetsRequestTopic2 = new DeleteShareGroupOffsetsRequestTopic()
      .setTopicName(topicName2)

    val deleteShareGroupOffsetsRequestTopic3 = new DeleteShareGroupOffsetsRequestTopic()
      .setTopicName(topicName3)

    val deleteShareGroupOffsetsRequestData = new DeleteShareGroupOffsetsRequestData()
      .setGroupId("group")
      .setTopics(util.List.of(deleteShareGroupOffsetsRequestTopic1, deleteShareGroupOffsetsRequestTopic2, deleteShareGroupOffsetsRequestTopic3))

    val requestChannelRequest = buildRequest(new DeleteShareGroupOffsetsRequest.Builder(deleteShareGroupOffsetsRequestData).build)

    val resultFuture = new CompletableFuture[DeleteShareGroupOffsetsResponseData]
    when(groupCoordinator.deleteShareGroupOffsets(
      requestChannelRequest.context,
      deleteShareGroupOffsetsRequestData
    )).thenReturn(resultFuture)

    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val deleteShareGroupOffsetsResponseData = new DeleteShareGroupOffsetsResponseData()
      .setErrorMessage(null)
      .setErrorCode(Errors.NONE.code())
      .setResponses(util.List.of(
        new DeleteShareGroupOffsetsResponseTopic()
          .setTopicName(topicName1)
          .setTopicId(topicId1)
          .setErrorMessage(null)
          .setErrorCode(Errors.NONE.code()),
        new DeleteShareGroupOffsetsResponseTopic()
          .setTopicName(topicName2)
          .setTopicId(topicId2)
          .setErrorMessage(null)
          .setErrorCode(Errors.NONE.code()),
        new DeleteShareGroupOffsetsResponseTopic()
          .setTopicName(topicName3)
          .setTopicId(topicId3)
          .setErrorMessage(null)
          .setErrorCode(Errors.NONE.code()),
      ))

    resultFuture.complete(deleteShareGroupOffsetsResponseData)
    val response = verifyNoThrottling[DeleteShareGroupOffsetsResponse](requestChannelRequest)
    assertEquals(deleteShareGroupOffsetsResponseData, response.data)
  }

  @Test
  def testDeleteShareGroupOffsetsRequestGroupCoordinatorThrowsError(): Unit = {
    val topicName1 = "topic-1"
    val topicId1 = Uuid.randomUuid
    val topicName2 = "topic-2"
    val topicId2 = Uuid.randomUuid
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName1, 1, topicId = topicId1)
    addTopicToMetadataCache(topicName2, 2, topicId = topicId2)

    val deleteShareGroupOffsetsRequestTopic1 = new DeleteShareGroupOffsetsRequestTopic()
      .setTopicName(topicName1)

    val deleteShareGroupOffsetsRequestTopic2 = new DeleteShareGroupOffsetsRequestTopic()
      .setTopicName(topicName2)

    val deleteShareGroupOffsetsRequestData = new DeleteShareGroupOffsetsRequestData()
      .setGroupId("group")
      .setTopics(util.List.of(deleteShareGroupOffsetsRequestTopic1, deleteShareGroupOffsetsRequestTopic2))

    val requestChannelRequest = buildRequest(new DeleteShareGroupOffsetsRequest.Builder(deleteShareGroupOffsetsRequestData).build)

    when(groupCoordinator.deleteShareGroupOffsets(
      requestChannelRequest.context,
      deleteShareGroupOffsetsRequestData
    )).thenReturn(CompletableFuture.failedFuture(Errors.UNKNOWN_SERVER_ERROR.exception))

    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val deleteShareGroupOffsetsResponseData = new DeleteShareGroupOffsetsResponseData()
      .setErrorMessage(Errors.UNKNOWN_SERVER_ERROR.message())
      .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())

    val response = verifyNoThrottling[DeleteShareGroupOffsetsResponse](requestChannelRequest)
    assertEquals(deleteShareGroupOffsetsResponseData, response.data)
  }

  @Test
  def testDeleteShareGroupOffsetsRequestGroupCoordinatorErrorResponse(): Unit = {
    val topicName1 = "topic-1"
    val topicId1 = Uuid.randomUuid
    val topicName2 = "topic-2"
    val topicId2 = Uuid.randomUuid
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName1, 1, topicId = topicId1)
    addTopicToMetadataCache(topicName2, 2, topicId = topicId2)

    val deleteShareGroupOffsetsRequestTopic1 = new DeleteShareGroupOffsetsRequestTopic()
      .setTopicName(topicName1)

    val deleteShareGroupOffsetsRequestTopic2 = new DeleteShareGroupOffsetsRequestTopic()
      .setTopicName(topicName2)

    val deleteShareGroupOffsetsRequestData = new DeleteShareGroupOffsetsRequestData()
      .setGroupId("group")
      .setTopics(util.List.of(deleteShareGroupOffsetsRequestTopic1, deleteShareGroupOffsetsRequestTopic2))

    val requestChannelRequest = buildRequest(new DeleteShareGroupOffsetsRequest.Builder(deleteShareGroupOffsetsRequestData).build)

    val groupCoordinatorResponse: DeleteShareGroupOffsetsResponseData = new DeleteShareGroupOffsetsResponseData()
      .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
      .setErrorMessage(Errors.UNKNOWN_SERVER_ERROR.message())

    when(groupCoordinator.deleteShareGroupOffsets(
      requestChannelRequest.context,
      deleteShareGroupOffsetsRequestData
    )).thenReturn(CompletableFuture.completedFuture(groupCoordinatorResponse))

    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val deleteShareGroupOffsetsResponseData = new DeleteShareGroupOffsetsResponseData()
      .setErrorMessage(Errors.UNKNOWN_SERVER_ERROR.message())
      .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())

    val response = verifyNoThrottling[DeleteShareGroupOffsetsResponse](requestChannelRequest)
    assertEquals(deleteShareGroupOffsetsResponseData, response.data)
  }

  @Test
  def testDeleteShareGroupOffsetsRequestEmptyTopicsSuccess(): Unit = {
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()

    val deleteShareGroupOffsetsRequestData = new DeleteShareGroupOffsetsRequestData()
      .setGroupId("group")

    val requestChannelRequest = buildRequest(new DeleteShareGroupOffsetsRequest.Builder(deleteShareGroupOffsetsRequestData).build)

    val groupCoordinatorResponse: DeleteShareGroupOffsetsResponseData = new DeleteShareGroupOffsetsResponseData()
      .setErrorCode(Errors.NONE.code())

    when(groupCoordinator.deleteShareGroupOffsets(
      requestChannelRequest.context,
      deleteShareGroupOffsetsRequestData
    )).thenReturn(CompletableFuture.completedFuture(groupCoordinatorResponse))

    val resultFuture = new CompletableFuture[DeleteShareGroupOffsetsResponseData]
    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val deleteShareGroupOffsetsResponse = new DeleteShareGroupOffsetsResponseData()

    resultFuture.complete(deleteShareGroupOffsetsResponse)
    val response = verifyNoThrottling[DeleteShareGroupOffsetsResponse](requestChannelRequest)
    assertEquals(deleteShareGroupOffsetsResponse, response.data)
  }

  @Test
  def testWriteShareGroupStateSuccess(): Unit = {
    val topicId = Uuid.randomUuid()
    val writeRequestData = new WriteShareGroupStateRequestData()
      .setGroupId("group1")
      .setTopics(util.List.of(
        new WriteShareGroupStateRequestData.WriteStateData()
          .setTopicId(topicId)
          .setPartitions(util.List.of(
            new WriteShareGroupStateRequestData.PartitionData()
              .setPartition(1)
              .setLeaderEpoch(1)
              .setStateEpoch(2)
              .setStartOffset(10)
              .setDeliveryCompleteCount(5)
              .setStateBatches(util.List.of(
                new WriteShareGroupStateRequestData.StateBatch()
                  .setFirstOffset(11)
                  .setLastOffset(15)
                  .setDeliveryCount(1)
                  .setDeliveryState(0)
              ))
          ))
      ))

    val writeStateResultData: util.List[WriteShareGroupStateResponseData.WriteStateResult] = util.List.of(
      new WriteShareGroupStateResponseData.WriteStateResult()
        .setTopicId(topicId)
        .setPartitions(util.List.of(
          new WriteShareGroupStateResponseData.PartitionResult()
            .setPartition(1)
            .setErrorCode(Errors.NONE.code())
            .setErrorMessage(null)
        ))
    )

    val response = getWriteShareGroupStateResponse(
      writeRequestData,
      ShareCoordinatorTestConfig.testConfigMap().asScala,
      verifyNoErr = true,
      null,
      writeStateResultData
    )

    assertNotNull(response.data)
    assertEquals(1, response.data.results.size)
  }

  @Test
  def testWriteShareGroupStateAuthorizationFailed(): Unit = {
    val topicId = Uuid.randomUuid()
    val writeRequestData = new WriteShareGroupStateRequestData()
      .setGroupId("group1")
      .setTopics(util.List.of(
        new WriteShareGroupStateRequestData.WriteStateData()
          .setTopicId(topicId)
          .setPartitions(util.List.of(
            new WriteShareGroupStateRequestData.PartitionData()
              .setPartition(1)
              .setLeaderEpoch(1)
              .setStateEpoch(2)
              .setStartOffset(10)
              .setDeliveryCompleteCount(5)
              .setStateBatches(util.List.of(
                new WriteShareGroupStateRequestData.StateBatch()
                  .setFirstOffset(11)
                  .setLastOffset(15)
                  .setDeliveryCount(1)
                  .setDeliveryState(0)
              ))
          ))
      ))

    val writeStateResultData: util.List[WriteShareGroupStateResponseData.WriteStateResult] = util.List.of(
      new WriteShareGroupStateResponseData.WriteStateResult()
        .setTopicId(topicId)
        .setPartitions(util.List.of(
          new WriteShareGroupStateResponseData.PartitionResult()
            .setPartition(1)
            .setErrorCode(Errors.NONE.code())
            .setErrorMessage(null)
        ))
    )

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(util.List.of(AuthorizationResult.DENIED), util.List.of(AuthorizationResult.ALLOWED))

    val response = getWriteShareGroupStateResponse(
      writeRequestData,
      ShareCoordinatorTestConfig.testConfigMap().asScala,
      verifyNoErr = false,
      authorizer,
      writeStateResultData
    )

    assertNotNull(response.data)
    assertEquals(1, response.data.results.size)
    response.data.results.forEach(writeResult => {
      assertEquals(1, writeResult.partitions.size)
      assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code(), writeResult.partitions.get(0).errorCode())
    })
  }

  @Test
  def testDeleteShareGroupStateSuccess(): Unit = {
    val topicId = Uuid.randomUuid()
    val deleteRequestData = new DeleteShareGroupStateRequestData()
      .setGroupId("group1")
      .setTopics(util.List.of(
        new DeleteShareGroupStateRequestData.DeleteStateData()
          .setTopicId(topicId)
          .setPartitions(util.List.of(
            new DeleteShareGroupStateRequestData.PartitionData()
              .setPartition(1)
          ))
      ))

    val deleteStateResultData: util.List[DeleteShareGroupStateResponseData.DeleteStateResult] = util.List.of(
      new DeleteShareGroupStateResponseData.DeleteStateResult()
        .setTopicId(topicId)
        .setPartitions(util.List.of(
          new DeleteShareGroupStateResponseData.PartitionResult()
            .setPartition(1)
            .setErrorCode(Errors.NONE.code())
            .setErrorMessage(null)
        ))
    )

    val response = getDeleteShareGroupStateResponse(
      deleteRequestData,
      ShareCoordinatorTestConfig.testConfigMap().asScala,
      verifyNoErr = true,
      null,
      deleteStateResultData
    )

    assertNotNull(response.data)
    assertEquals(1, response.data.results.size)
  }

  @Test
  def testDeleteShareGroupStateAuthorizationFailed(): Unit = {
    val topicId = Uuid.randomUuid()
    val deleteRequestData = new DeleteShareGroupStateRequestData()
      .setGroupId("group1")
      .setTopics(util.List.of(
        new DeleteShareGroupStateRequestData.DeleteStateData()
          .setTopicId(topicId)
          .setPartitions(util.List.of(
            new DeleteShareGroupStateRequestData.PartitionData()
              .setPartition(1)
          ))
      ))

    val deleteStateResultData: util.List[DeleteShareGroupStateResponseData.DeleteStateResult] = util.List.of(
      new DeleteShareGroupStateResponseData.DeleteStateResult()
        .setTopicId(topicId)
        .setPartitions(util.List.of(
          new DeleteShareGroupStateResponseData.PartitionResult()
            .setPartition(1)
            .setErrorCode(Errors.NONE.code())
            .setErrorMessage(null)
        ))
    )

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(util.List.of(AuthorizationResult.DENIED), util.List.of(AuthorizationResult.ALLOWED))

    val response = getDeleteShareGroupStateResponse(
      deleteRequestData,
      ShareCoordinatorTestConfig.testConfigMap().asScala,
      verifyNoErr = false,
      authorizer,
      deleteStateResultData
    )

    assertNotNull(response.data)
    assertEquals(1, response.data.results.size)
    response.data.results.forEach(deleteResult => {
      assertEquals(1, deleteResult.partitions.size)
      assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code(), deleteResult.partitions.get(0).errorCode())
    })
  }

  @Test
  def testInitializeShareGroupStateSuccess(): Unit = {
    val topicId = Uuid.randomUuid()
    val initRequestData = new InitializeShareGroupStateRequestData()
      .setGroupId("group1")
      .setTopics(util.List.of(
        new InitializeShareGroupStateRequestData.InitializeStateData()
          .setTopicId(topicId)
          .setPartitions(util.List.of(
            new InitializeShareGroupStateRequestData.PartitionData()
              .setPartition(1)
              .setStateEpoch(0)
          ))
      ))

    val initStateResultData: util.List[InitializeShareGroupStateResponseData.InitializeStateResult] = util.List.of(
      new InitializeShareGroupStateResponseData.InitializeStateResult()
        .setTopicId(topicId)
        .setPartitions(util.List.of(
          new InitializeShareGroupStateResponseData.PartitionResult()
            .setPartition(1)
            .setErrorCode(Errors.NONE.code())
            .setErrorMessage(null)
        ))
    )

    val response = getInitializeShareGroupStateResponse(
      initRequestData,
      ShareCoordinatorTestConfig.testConfigMap().asScala,
      verifyNoErr = true,
      null,
      initStateResultData
    )

    assertNotNull(response.data)
    assertEquals(1, response.data.results.size)
  }

  @Test
  def testInitializeShareGroupStateAuthorizationFailed(): Unit = {
    val topicId = Uuid.randomUuid()
    val initRequestData = new InitializeShareGroupStateRequestData()
      .setGroupId("group1")
      .setTopics(util.List.of(
        new InitializeShareGroupStateRequestData.InitializeStateData()
          .setTopicId(topicId)
          .setPartitions(util.List.of(
            new InitializeShareGroupStateRequestData.PartitionData()
              .setPartition(1)
              .setStateEpoch(0)
          ))
      ))

    val initStateResultData: util.List[InitializeShareGroupStateResponseData.InitializeStateResult] = util.List.of(
      new InitializeShareGroupStateResponseData.InitializeStateResult()
        .setTopicId(topicId)
        .setPartitions(util.List.of(
          new InitializeShareGroupStateResponseData.PartitionResult()
            .setPartition(1)
            .setErrorCode(Errors.NONE.code())
            .setErrorMessage(null)
        ))
    )

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(util.List.of(AuthorizationResult.DENIED), util.List.of(AuthorizationResult.ALLOWED))

    val response = getInitializeShareGroupStateResponse(
      initRequestData,
      ShareCoordinatorTestConfig.testConfigMap().asScala,
      verifyNoErr = false,
      authorizer,
      initStateResultData
    )

    assertNotNull(response.data)
    assertEquals(1, response.data.results.size)
    response.data.results.forEach(deleteResult => {
      assertEquals(1, deleteResult.partitions.size)
      assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code(), deleteResult.partitions.get(0).errorCode())
    })
  }

  @Test
  def testAlterShareGroupOffsetsReturnsUnsupportedVersion(): Unit = {
    val alterShareGroupOffsetsRequest = new AlterShareGroupOffsetsRequestData()
      .setGroupId("group")
      .setTopics(
        new AlterShareGroupOffsetsRequestTopicCollection(
          util.List.of(
            new AlterShareGroupOffsetsRequestTopic()
              .setTopicName("topic-1")
              .setPartitions(util.List.of(
                new AlterShareGroupOffsetsRequestPartition().setPartitionIndex(0).setStartOffset(0),
                new AlterShareGroupOffsetsRequestPartition().setPartitionIndex(1).setStartOffset(0))
              ),
            new AlterShareGroupOffsetsRequestTopic()
              .setTopicName("topic-2")
              .setPartitions(util.List.of(
                new AlterShareGroupOffsetsRequestPartition().setPartitionIndex(0).setStartOffset(0))
              )
          ).iterator()
        )
      )

    val requestChannelRequest = buildRequest(new AlterShareGroupOffsetsRequest.Builder(alterShareGroupOffsetsRequest).build())
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled(enableShareGroups = false)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val response = verifyNoThrottling[AlterShareGroupOffsetsResponse](requestChannelRequest)
    response.data.responses.forEach(topic => {
      topic.partitions().forEach(partition => assertEquals(Errors.UNSUPPORTED_VERSION.code, partition.errorCode))
    })
  }

  @Test
  def testAlterShareGroupOffsetsSuccess(): Unit = {
    val groupId = "group"
    val topicName1 = "foo"
    val topicId1 = Uuid.randomUuid
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName1, 2, topicId = topicId1)
    val topicCollection = new AlterShareGroupOffsetsRequestTopicCollection();
    topicCollection.addAll(util.List.of(
      new AlterShareGroupOffsetsRequestData.AlterShareGroupOffsetsRequestTopic()
        .setTopicName(topicName1)
        .setPartitions(List(
          new AlterShareGroupOffsetsRequestData.AlterShareGroupOffsetsRequestPartition()
            .setPartitionIndex(0)
            .setStartOffset(0L),
          new AlterShareGroupOffsetsRequestData.AlterShareGroupOffsetsRequestPartition()
            .setPartitionIndex(1)
            .setStartOffset(0L)
        ).asJava)))

    val alterRequestData = new AlterShareGroupOffsetsRequestData()
      .setGroupId(groupId)
      .setTopics(topicCollection)

    val requestChannelRequest = buildRequest(new AlterShareGroupOffsetsRequest.Builder(alterRequestData).build)
    val resultFuture = new CompletableFuture[AlterShareGroupOffsetsResponseData]
    when(groupCoordinator.alterShareGroupOffsets(
      any(),
      ArgumentMatchers.eq[String](groupId),
      ArgumentMatchers.any(classOf[AlterShareGroupOffsetsRequestData])
    )).thenReturn(resultFuture)

    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val alterShareGroupOffsetsResponse = new AlterShareGroupOffsetsResponseData()
    resultFuture.complete(alterShareGroupOffsetsResponse)
    val response = verifyNoThrottling[AlterShareGroupOffsetsResponse](requestChannelRequest)
    assertEquals(alterShareGroupOffsetsResponse, response.data)
  }

  @Test
  def testAlterShareGroupOffsetsAuthorizationFailed(): Unit = {
    val groupId = "group"
    val topicName1 = "foo"
    val topicId1 = Uuid.randomUuid
    val topicName2 = "bar"
    val topicId2 = Uuid.randomUuid
    val topicName3 = "zoo"
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName1, 2, topicId = topicId1)
    addTopicToMetadataCache(topicName2, 1, topicId = topicId2)
    val topicCollection = new AlterShareGroupOffsetsRequestTopicCollection();
    topicCollection.addAll(util.List.of(
      new AlterShareGroupOffsetsRequestData.AlterShareGroupOffsetsRequestTopic()
        .setTopicName(topicName1)
        .setPartitions(List(
          new AlterShareGroupOffsetsRequestData.AlterShareGroupOffsetsRequestPartition()
            .setPartitionIndex(0)
            .setStartOffset(0L),
          new AlterShareGroupOffsetsRequestData.AlterShareGroupOffsetsRequestPartition()
            .setPartitionIndex(1)
            .setStartOffset(0L)
        ).asJava),
      new AlterShareGroupOffsetsRequestData.AlterShareGroupOffsetsRequestTopic()
        .setTopicName(topicName2)
        .setPartitions(List(
          new AlterShareGroupOffsetsRequestData.AlterShareGroupOffsetsRequestPartition()
            .setPartitionIndex(0)
            .setStartOffset(0L)
        ).asJava),
      new AlterShareGroupOffsetsRequestData.AlterShareGroupOffsetsRequestTopic()
        .setTopicName(topicName3)
        setPartitions(List(
        new AlterShareGroupOffsetsRequestData.AlterShareGroupOffsetsRequestPartition()
          .setPartitionIndex(0)
          .setStartOffset(0L)
        ).asJava))
    )

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Seq(AuthorizationResult.ALLOWED).asJava, Seq(AuthorizationResult.DENIED).asJava, Seq(AuthorizationResult.ALLOWED).asJava, Seq(AuthorizationResult.ALLOWED).asJava)

    val alterRequestData = new AlterShareGroupOffsetsRequestData()
      .setGroupId(groupId)
      .setTopics(topicCollection)

    val requestChannelRequest = buildRequest(new AlterShareGroupOffsetsRequest.Builder(alterRequestData).build)
    val resultFuture = new CompletableFuture[AlterShareGroupOffsetsResponseData]
    when(groupCoordinator.alterShareGroupOffsets(
      any(),
      ArgumentMatchers.eq[String](groupId),
      ArgumentMatchers.any(classOf[AlterShareGroupOffsetsRequestData])
    )).thenReturn(resultFuture)

    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val alterShareGroupOffsetsResponse = new AlterShareGroupOffsetsResponseData()
      .setResponses(new AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponseTopicCollection(util.List.of(
        new AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponseTopic()
          .setTopicName(topicName2)
          .setTopicId(topicId2)
          .setPartitions(List(
            new AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code())
              .setErrorMessage(Errors.NONE.message())
          ).asJava)
      ).iterator))
    resultFuture.complete(alterShareGroupOffsetsResponse)
    val response = verifyNoThrottling[AlterShareGroupOffsetsResponse](requestChannelRequest)

    assertNotNull(response.data)
    assertEquals(1, response.errorCounts().get(Errors.UNKNOWN_TOPIC_OR_PARTITION))
    assertEquals(2, response.errorCounts().get(Errors.TOPIC_AUTHORIZATION_FAILED))
    assertEquals(3, response.data().responses().size())

    val bar = response.data().responses().find("bar")
    val foo = response.data().responses().find("foo")
    val zoo = response.data().responses().find("zoo")
    assertEquals(topicName1, foo.topicName())
    assertEquals(topicId1, foo.topicId())
    assertEquals(topicName2, bar.topicName())
    assertEquals(topicId2, bar.topicId())
    assertEquals(topicName3, zoo.topicName())
    assertEquals(Uuid.ZERO_UUID, zoo.topicId())
    foo.partitions().forEach(partition => {
      assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code(), partition.errorCode())
    })
  }

  @Test
  def testAlterShareGroupOffsetsRequestGroupCoordinatorThrowsError(): Unit = {
    val groupId = "group"
    val topicName1 = "foo"
    val topicId1 = Uuid.randomUuid
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName1, 2, topicId = topicId1)
    val topicCollection = new AlterShareGroupOffsetsRequestTopicCollection();
    topicCollection.addAll(util.List.of(
      new AlterShareGroupOffsetsRequestData.AlterShareGroupOffsetsRequestTopic()
        .setTopicName(topicName1)
        .setPartitions(List(
          new AlterShareGroupOffsetsRequestData.AlterShareGroupOffsetsRequestPartition()
            .setPartitionIndex(0)
            .setStartOffset(0L),
          new AlterShareGroupOffsetsRequestData.AlterShareGroupOffsetsRequestPartition()
            .setPartitionIndex(1)
            .setStartOffset(0L)
        ).asJava)))

    val alterRequestData = new AlterShareGroupOffsetsRequestData()
      .setGroupId(groupId)
      .setTopics(topicCollection)

    val requestChannelRequest = buildRequest(new AlterShareGroupOffsetsRequest.Builder(alterRequestData).build)
    when(groupCoordinator.alterShareGroupOffsets(
      any(),
      ArgumentMatchers.eq[String](groupId),
      ArgumentMatchers.any(classOf[AlterShareGroupOffsetsRequestData])
    )).thenReturn(CompletableFuture.failedFuture(Errors.UNKNOWN_SERVER_ERROR.exception))

    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val alterShareGroupOffsetsResponseData = new AlterShareGroupOffsetsResponseData()
      .setErrorMessage(Errors.UNKNOWN_SERVER_ERROR.message())
      .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())

    val response = verifyNoThrottling[AlterShareGroupOffsetsResponse](requestChannelRequest)
    assertEquals(alterShareGroupOffsetsResponseData, response.data)
  }

  @ParameterizedTest
  @CsvSource(value = Array("1,true,true", "1,false,true", "2,true,false", "2,false,true"))
  def testValidateAcknowledgementBatchesForRenew(version: Short, isRenew: Boolean, shouldFail: Boolean): Unit = {
    kafkaApis = createKafkaApis()
    val tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("topic", 0))
    val ackMap = mutable.Map(tp -> util.List.of(new ShareAcknowledgementBatch(0, 0, util.List.of(AcknowledgeType.RENEW.id))))
    val erroneous:mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData] = mutable.Map()
    val errorSet = kafkaApis.validateAcknowledgementBatches(ackMap, erroneous, supportsRenewAcknowledgements = version == 2, isRenewAck = isRenew)
    if (shouldFail) {
      assertEquals(1, errorSet.size, s"expected error topic partition, version=${version}, isRenew=${isRenew}")
      assertTrue(errorSet.contains(tp), s"error topic partition mismatch, version=${version}, isRenew=${isRenew}")
    } else {
      assertEquals(0, errorSet.size, s"unexpected error topic partition, version=${version}, isRenew=${isRenew}")
    }
  }

  @Test
  def testHandleShareFetchRenewInvalidRequest(): Unit = {
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    val groupId = "group"
    val memberId = Uuid.randomUuid()
    val testPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "test-user")
    val testClientAddress = InetAddress.getByName("192.168.1.100")
    val testClientId = "test-client-id"
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any(), any(), any())).thenReturn(
      new FinalContext()
    )

    val shareFetchRequestData = new ShareFetchRequestData()
      .setGroupId(groupId)
      .setMemberId(memberId.toString)
      .setShareSessionEpoch(0)
      .setIsRenewAck(true)
      .setMinBytes(10)
      .setMaxBytes(20)
      .setMaxRecords(30)
      .setMaxWaitMs(40)
      .setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(new ShareFetchRequestData.FetchTopic()
        .setTopicId(topicId)
        .setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setAcknowledgementBatches(util.List.of(new AcknowledgementBatch()
              .setFirstOffset(0)
              .setLastOffset(0)
              .setAcknowledgeTypes(util.List.of(AcknowledgeType.RENEW.id))))
            .setPartitionIndex(partitionIndex)
        ).iterator))
      ).iterator))

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)

    // Create request with custom principal and client address to test quota tags
    val requestHeader = new RequestHeader(shareFetchRequest.apiKey, shareFetchRequest.version, testClientId, 0)
    val request = buildRequest(shareFetchRequest, testPrincipal, testClientAddress,
      ListenerName.forSecurityProtocol(SecurityProtocol.SSL), fromPrivilegedListener = false, Some(requestHeader), requestChannelMetrics)

    val kafkaApis = createKafkaApis()
    kafkaApis.handleShareFetchRequest(request)
    val response = verifyNoThrottling[ShareFetchResponse](request)
    val responseData = response.data()

    assertEquals(Errors.INVALID_REQUEST.code, responseData.errorCode)
  }

  @Test
  def testHandleShareFetchRequestSuccessHavingMemberIdViolatingUuidBase64UrlDecoder(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId = "/FvjN95PRhGaAuA8aQMuTw"

    val records1 = memoryRecords(10, 0)
    val records2 = memoryRecords(10, 10)

    val groupId = "group"

    when(sharePartitionManager.fetchMessages(any(), any(), any(), any(), anyInt(), anyInt(), anyInt(), any())).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)),
        new ShareFetchResponseData.PartitionData()
          .setErrorCode(Errors.NONE.code)
          .setAcknowledgeErrorCode(Errors.NONE.code)
          .setRecords(records1)
          .setAcquiredRecords(new util.ArrayList(util.List.of(
            new ShareFetchResponseData.AcquiredRecords()
              .setFirstOffset(0)
              .setLastOffset(9)
              .setDeliveryCount(1)
          )))
      ))
    ).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)),
        new ShareFetchResponseData.PartitionData()
          .setErrorCode(Errors.NONE.code)
          .setAcknowledgeErrorCode(Errors.NONE.code)
          .setRecords(records2)
          .setAcquiredRecords(new util.ArrayList(util.List.of(
            new ShareFetchResponseData.AcquiredRecords()
              .setFirstOffset(10)
              .setLastOffset(19)
              .setDeliveryCount(1)
          )))
      ))
    )

    val cachedSharePartitions = new ImplicitLinkedHashCollection[CachedSharePartition]
    cachedSharePartitions.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId, 0, topicName), false
    ))

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[Session](), anyString, anyDouble, anyLong)).thenReturn(0)

    when(sharePartitionManager.acknowledge(any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, 0)),
        new ShareAcknowledgeResponseData.PartitionData()
          .setPartitionIndex(0)
          .setErrorCode(Errors.NONE.code),
      ))
    )

    var shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId).
      setShareSessionEpoch(0).
      setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)).iterator))).iterator))

    var shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    var request = buildRequest(shareFetchRequest)

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any(), any(), any())).thenReturn(
      new ShareSessionContext(0, util.List.of(
        new TopicIdPartition(topicId, partitionIndex, topicName)
      ))
    ).thenReturn(new ShareSessionContext(1, new ShareSession(
      new ShareSessionKey(groupId, memberId), cachedSharePartitions, 2, request.context.connectionId))
    )

    kafkaApis = createKafkaApis()
    kafkaApis.handleShareFetchRequest(request)
    var response = verifyNoThrottling[ShareFetchResponse](request)
    var responseData = response.data()
    var topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    var topicResponse = topicResponses.stream.findFirst.get
    assertEquals(topicId, topicResponse.topicId)
    assertEquals(1, topicResponse.partitions.size())
    assertEquals(partitionIndex, topicResponse.partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponse.partitions.get(0).errorCode)
    assertEquals(records1, topicResponse.partitions.get(0).records)
    assertArrayEquals(expectedAcquiredRecords(0, 9, 1).toArray(), topicResponse.partitions.get(0).acquiredRecords.toArray())

    shareFetchRequestData = new ShareFetchRequestData().
      setGroupId("group").
      setMemberId(memberId).
      setShareSessionEpoch(1).
      setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)
            .setAcknowledgementBatches(util.List.of(
              new AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(util.List.of(1.toByte))
            ))
        ).iterator))
      ).iterator))

    shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    request = buildRequest(shareFetchRequest)
    kafkaApis.handleShareFetchRequest(request)
    response = verifyNoThrottling[ShareFetchResponse](request)
    responseData = response.data()
    topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    topicResponse = topicResponses.stream.findFirst.get
    assertEquals(topicId, topicResponse.topicId)
    assertEquals(1, topicResponse.partitions.size())
    assertEquals(partitionIndex, topicResponse.partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponse.partitions.get(0).errorCode)
    assertEquals(Errors.NONE.code, topicResponse.partitions.get(0).acknowledgeErrorCode)
    assertEquals(records2, topicResponse.partitions.get(0).records)
    assertArrayEquals(expectedAcquiredRecords(10, 19, 1).toArray(), topicResponse.partitions.get(0).acquiredRecords.toArray())
  }

  @Test
  def testHandleShareAcknowledgeRequestSuccessHavingMemberIdViolatingUuidBase64UrlDecoder(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId = "/FvjN95PRhGaAuA8aQMuTw"

    val groupId = "group"

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[Session](), anyString, anyDouble, anyLong)).thenReturn(0)

    when(sharePartitionManager.acknowledge(any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, 0)),
        new ShareAcknowledgeResponseData.PartitionData()
          .setPartitionIndex(0)
          .setErrorCode(Errors.NONE.code)
      ))
    )

    doNothing().when(sharePartitionManager).acknowledgeSessionUpdate(any(), any(), any())

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId(groupId).
      setMemberId(memberId).
      setShareSessionEpoch(1).
      setTopics(new ShareAcknowledgeRequestData.AcknowledgeTopicCollection(util.List.of(new ShareAcknowledgeRequestData.AcknowledgeTopic().
        setTopicId(topicId).
        setPartitions(new ShareAcknowledgeRequestData.AcknowledgePartitionCollection(util.List.of(
          new ShareAcknowledgeRequestData.AcknowledgePartition()
            .setPartitionIndex(partitionIndex)
            .setAcknowledgementBatches(util.List.of(
              new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(util.List.of(1.toByte))
            ))
        ).iterator))).iterator))

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData)
      .build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleShareAcknowledgeRequest(request)
    val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
    val responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    val topicResponse = topicResponses.stream.findFirst.get
    assertEquals(topicId, topicResponse.topicId)
    assertEquals(1, topicResponse.partitions.size())
    assertEquals(partitionIndex, topicResponse.partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponse.partitions.get(0).errorCode)
  }

  @Test
  def testIsMemberIdValid(): Unit = {
    kafkaApis = createKafkaApis()
    assertTrue(kafkaApis.isMemberIdValid(Uuid.randomUuid().toString))
    assertTrue(kafkaApis.isMemberIdValid(Uuid.ZERO_UUID.toString))
    assertFalse(kafkaApis.isMemberIdValid(""))
    assertTrue(kafkaApis.isMemberIdValid("       "))
    var validStr1: String = ""
    var validStr2: String = ""
    var inValidStr: String = ""
    var counter = 1
    while (counter < 40) {
      if (counter < 36) {
        validStr1 += (counter % 10)
      }
      if (counter < 37) {
        validStr2 += (counter % 10)
      }
      inValidStr += (counter % 10)
      counter += 1
    }
    assertTrue(kafkaApis.isMemberIdValid(validStr1))
    assertTrue(kafkaApis.isMemberIdValid(validStr2))
    assertFalse(kafkaApis.isMemberIdValid(inValidStr))
  }

  @Test
  def testShareGroupHeartbeatRequestWithInvalidMemberId(): Unit = {
    val shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequestData().setGroupId("group").setMemberId("")

    val requestChannelRequest = buildRequest(new ShareGroupHeartbeatRequest.Builder(shareGroupHeartbeatRequest).build())

    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)
    val response = verifyNoThrottling[ShareGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.INVALID_REQUEST.code(), response.data().errorCode())
  }

  @Test
  def testShareFetchRequestWithInvalidMemberId(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: String = ""
    val groupId = "group"
    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId).
      setShareSessionEpoch(0).
      setTopics(new ShareFetchRequestData.FetchTopicCollection(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(new ShareFetchRequestData.FetchPartitionCollection(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)
        ).iterator))
      ).iterator))

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleShareFetchRequest(request)
    val response = verifyNoThrottling[ShareFetchResponse](request)
    val responseData = response.data()
    assertEquals(Errors.INVALID_REQUEST.code, responseData.errorCode)
  }

  @Test
  def testHandleShareAcknowledgeWithInvalidMemberId(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId = ""

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId("group").
      setMemberId(memberId).
      setShareSessionEpoch(1).
      setTopics(new ShareAcknowledgeRequestData.AcknowledgeTopicCollection(util.List.of(new ShareAcknowledgeRequestData.AcknowledgeTopic().
        setTopicId(topicId).
        setPartitions(new ShareAcknowledgeRequestData.AcknowledgePartitionCollection(util.List.of(
          new ShareAcknowledgeRequestData.AcknowledgePartition()
            .setPartitionIndex(partitionIndex)
            .setAcknowledgementBatches(util.List.of(
              new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(util.List.of(1.toByte))
            ))
        ).iterator))).iterator))

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any(), any())).thenReturn(util.List.of[AuthorizationResult](
      AuthorizationResult.ALLOWED
    ))

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData)
      .build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)
    kafkaApis = createKafkaApis(
      authorizer = Option(authorizer),
    )
    kafkaApis.handleShareAcknowledgeRequest(request)

    val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
    val responseData = response.data()

    assertEquals(Errors.INVALID_REQUEST.code, responseData.errorCode)
  }

  def getShareGroupDescribeResponse(groupIds: util.List[String], enableShareGroups: Boolean = true,
                                    verifyNoErr: Boolean = true, authorizer: Authorizer = null,
                                    describedGroups: util.List[ShareGroupDescribeResponseData.DescribedGroup]): ShareGroupDescribeResponse = {
    val shareGroupDescribeRequestData = new ShareGroupDescribeRequestData()
    shareGroupDescribeRequestData.groupIds.addAll(groupIds)
    val requestChannelRequest = buildRequest(new ShareGroupDescribeRequest.Builder(shareGroupDescribeRequestData).build())

    val future = new CompletableFuture[util.List[ShareGroupDescribeResponseData.DescribedGroup]]()
    when(groupCoordinator.shareGroupDescribe(
      any[RequestContext],
      any[util.List[String]]
    )).thenReturn(future)
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled(enableShareGroups)
    kafkaApis = createKafkaApis(
      authorizer = Option(authorizer),
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    future.complete(describedGroups)

    val response = verifyNoThrottling[ShareGroupDescribeResponse](requestChannelRequest)
    if (verifyNoErr) {
      val expectedShareGroupDescribeResponseData = new ShareGroupDescribeResponseData()
        .setGroups(describedGroups)
      assertEquals(expectedShareGroupDescribeResponseData, response.data)
    }
    response
  }

  def getReadShareGroupStateResponse(requestData: ReadShareGroupStateRequestData, configOverrides: Map[String, String] = Map.empty,
                                     verifyNoErr: Boolean = true, authorizer: Authorizer = null,
                                     readStateResult: util.List[ReadShareGroupStateResponseData.ReadStateResult]): ReadShareGroupStateResponse = {
    val requestChannelRequest = buildRequest(new ReadShareGroupStateRequest.Builder(requestData).build())

    val future = new CompletableFuture[ReadShareGroupStateResponseData]()
    when(shareCoordinator.readState(
      any[RequestContext],
      any[ReadShareGroupStateRequestData]
    )).thenReturn(future)
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    kafkaApis = createKafkaApis(
      overrideProperties = configOverrides,
      authorizer = Option(authorizer),
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching())

    future.complete(new ReadShareGroupStateResponseData()
      .setResults(readStateResult))

    val response = verifyNoThrottling[ReadShareGroupStateResponse](requestChannelRequest)
    if (verifyNoErr) {
      val expectedReadShareGroupStateResponseData = new ReadShareGroupStateResponseData()
        .setResults(readStateResult)
      assertEquals(expectedReadShareGroupStateResponseData, response.data)
    }
    response
  }

  def getReadShareGroupStateSummaryResponse(requestData: ReadShareGroupStateSummaryRequestData, configOverrides: Map[String, String] = Map.empty,
                                            verifyNoErr: Boolean = true, authorizer: Authorizer = null,
                                            readStateSummaryResult: util.List[ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult]): ReadShareGroupStateSummaryResponse = {
    val requestChannelRequest = buildRequest(new ReadShareGroupStateSummaryRequest.Builder(requestData).build())

    val future = new CompletableFuture[ReadShareGroupStateSummaryResponseData]()
    when(shareCoordinator.readStateSummary(
      any[RequestContext],
      any[ReadShareGroupStateSummaryRequestData]
    )).thenReturn(future)
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    kafkaApis = createKafkaApis(
      overrideProperties = configOverrides,
      authorizer = Option(authorizer),
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching())

    future.complete(new ReadShareGroupStateSummaryResponseData()
      .setResults(readStateSummaryResult))

    val response = verifyNoThrottling[ReadShareGroupStateSummaryResponse](requestChannelRequest)
    if (verifyNoErr) {
      val expectedReadShareGroupStateSummaryResponseData = new ReadShareGroupStateSummaryResponseData()
        .setResults(readStateSummaryResult)
      assertEquals(expectedReadShareGroupStateSummaryResponseData, response.data)
    }
    response
  }

  def getWriteShareGroupStateResponse(requestData: WriteShareGroupStateRequestData, configOverrides: Map[String, String] = Map.empty,
                                      verifyNoErr: Boolean = true, authorizer: Authorizer = null,
                                      writeStateResult: util.List[WriteShareGroupStateResponseData.WriteStateResult]): WriteShareGroupStateResponse = {
    val requestChannelRequest = buildRequest(new WriteShareGroupStateRequest.Builder(requestData).build())

    val future = new CompletableFuture[WriteShareGroupStateResponseData]()
    when(shareCoordinator.writeState(
      any[RequestContext],
      any[WriteShareGroupStateRequestData]
    )).thenReturn(future)
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    kafkaApis = createKafkaApis(
      overrideProperties = configOverrides,
      authorizer = Option(authorizer),
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching())

    future.complete(new WriteShareGroupStateResponseData()
      .setResults(writeStateResult))

    val response = verifyNoThrottling[WriteShareGroupStateResponse](requestChannelRequest)
    if (verifyNoErr) {
      val expectedWriteShareGroupStateResponseData = new WriteShareGroupStateResponseData()
        .setResults(writeStateResult)
      assertEquals(expectedWriteShareGroupStateResponseData, response.data)
    }
    response
  }

  def getDeleteShareGroupStateResponse(requestData: DeleteShareGroupStateRequestData, configOverrides: Map[String, String] = Map.empty,
                                       verifyNoErr: Boolean = true, authorizer: Authorizer = null,
                                       deleteStateResult: util.List[DeleteShareGroupStateResponseData.DeleteStateResult]): DeleteShareGroupStateResponse = {
    val requestChannelRequest = buildRequest(new DeleteShareGroupStateRequest.Builder(requestData).build())

    val future = new CompletableFuture[DeleteShareGroupStateResponseData]()
    when(shareCoordinator.deleteState(
      any[RequestContext],
      any[DeleteShareGroupStateRequestData]
    )).thenReturn(future)
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    kafkaApis = createKafkaApis(
      overrideProperties = configOverrides,
      authorizer = Option(authorizer),
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching())

    future.complete(new DeleteShareGroupStateResponseData()
      .setResults(deleteStateResult))

    val response = verifyNoThrottling[DeleteShareGroupStateResponse](requestChannelRequest)
    if (verifyNoErr) {
      val expectedDeleteShareGroupStateResponseData = new DeleteShareGroupStateResponseData()
        .setResults(deleteStateResult)
      assertEquals(expectedDeleteShareGroupStateResponseData, response.data)
    }
    response
  }

  def getInitializeShareGroupStateResponse(requestData: InitializeShareGroupStateRequestData, configOverrides: Map[String, String] = Map.empty,
                                           verifyNoErr: Boolean = true, authorizer: Authorizer = null,
                                           initStateResult: util.List[InitializeShareGroupStateResponseData.InitializeStateResult]): InitializeShareGroupStateResponse = {
    val requestChannelRequest = buildRequest(new InitializeShareGroupStateRequest.Builder(requestData).build())

    val future = new CompletableFuture[InitializeShareGroupStateResponseData]()
    when(shareCoordinator.initializeState(
      any[RequestContext],
      any[InitializeShareGroupStateRequestData]
    )).thenReturn(future)
    metadataCache = initializeMetadataCacheWithShareGroupsEnabled()
    kafkaApis = createKafkaApis(
      overrideProperties = configOverrides,
      authorizer = Option(authorizer),
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching())

    future.complete(new InitializeShareGroupStateResponseData()
      .setResults(initStateResult))

    val response = verifyNoThrottling[InitializeShareGroupStateResponse](requestChannelRequest)
    if (verifyNoErr) {
      val expectedInitShareGroupStateResponseData = new InitializeShareGroupStateResponseData()
        .setResults(initStateResult)
      assertEquals(expectedInitShareGroupStateResponseData, response.data)
    }
    response
  }
}
