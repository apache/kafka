/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package kafka.server

import java.net.InetAddress
import java.util
import java.util.concurrent.{Executors, Future, TimeUnit}
import java.util.{Collections, Optional, Properties}

import kafka.api.LeaderAndIsr
import kafka.log.LogConfig
import kafka.network.RequestChannel.Session
import kafka.security.authorizer.AclAuthorizer
import kafka.utils.TestUtils
import org.apache.kafka.common.acl._
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic
import org.apache.kafka.common.message.CreateTopicsRequestData.{CreatableTopic, CreatableTopicCollection}
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocolCollection
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity
import org.apache.kafka.common.message.ListOffsetsRequestData.{ListOffsetsPartition, ListOffsetsTopic}
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderPartition
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopic
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopicCollection
import org.apache.kafka.common.message.StopReplicaRequestData.{StopReplicaPartitionState, StopReplicaTopicState}
import org.apache.kafka.common.message.UpdateMetadataRequestData.{UpdateMetadataBroker, UpdateMetadataEndpoint, UpdateMetadataPartitionState}
import org.apache.kafka.common.message.{AddOffsetsToTxnRequestData, _}
import org.apache.kafka.common.metrics.{KafkaMetric, Quota, Sensor}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.quota.ClientQuotaFilter
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests._
import org.apache.kafka.common.resource.{PatternType, ResourceType => AdminResourceType}
import org.apache.kafka.common.security.auth._
import org.apache.kafka.common.utils.{Sanitizer, SecurityUtils}
import org.apache.kafka.common._
import org.apache.kafka.server.authorizer.{Action, AuthorizableRequestContext, AuthorizationResult}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

class RequestQuotaTest extends BaseRequestTest {

  override def brokerCount: Int = 1

  private val topic = "topic-1"
  private val numPartitions = 1
  private val tp = new TopicPartition(topic, 0)
  private val topicIds =  Collections.singletonMap(topic, Uuid.randomUuid())
  private val logDir = "logDir"
  private val unthrottledClientId = "unthrottled-client"
  private val smallQuotaProducerClientId = "small-quota-producer-client"
  private val smallQuotaConsumerClientId = "small-quota-consumer-client"
  private val brokerId: Integer = 0
  private var leaderNode: KafkaServer = _

  // Run tests concurrently since a throttle could be up to 1 second because quota percentage allocated is very low
  case class Task(apiKey: ApiKeys, future: Future[_])
  private val executor = Executors.newCachedThreadPool
  private val tasks = new ListBuffer[Task]

  override def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.put(KafkaConfig.ControlledShutdownEnableProp, "false")
    properties.put(KafkaConfig.OffsetsTopicReplicationFactorProp, "1")
    properties.put(KafkaConfig.OffsetsTopicPartitionsProp, "1")
    properties.put(KafkaConfig.GroupMinSessionTimeoutMsProp, "100")
    properties.put(KafkaConfig.GroupInitialRebalanceDelayMsProp, "0")
    properties.put(KafkaConfig.AuthorizerClassNameProp, classOf[RequestQuotaTest.TestAuthorizer].getName)
    properties.put(KafkaConfig.PrincipalBuilderClassProp, classOf[RequestQuotaTest.TestPrincipalBuilder].getName)
  }

  @BeforeEach
  override def setUp(): Unit = {
    RequestQuotaTest.principal = KafkaPrincipal.ANONYMOUS
    super.setUp()

    createTopic(topic, numPartitions)
    leaderNode = servers.head

    // Change default client-id request quota to a small value and a single unthrottledClient with a large quota
    val quotaProps = new Properties()
    quotaProps.put(DynamicConfig.Client.RequestPercentageOverrideProp, "0.01")
    quotaProps.put(DynamicConfig.Client.ProducerByteRateOverrideProp, "2000")
    quotaProps.put(DynamicConfig.Client.ConsumerByteRateOverrideProp, "2000")
    adminZkClient.changeClientIdConfig("<default>", quotaProps)
    quotaProps.put(DynamicConfig.Client.RequestPercentageOverrideProp, "2000")
    adminZkClient.changeClientIdConfig(Sanitizer.sanitize(unthrottledClientId), quotaProps)

    // Client ids with small producer and consumer (fetch) quotas. Quota values were picked so that both
    // producer/consumer and request quotas are violated on the first produce/consume operation, and the delay due to
    // producer/consumer quota violation will be longer than the delay due to request quota violation.
    quotaProps.put(DynamicConfig.Client.ProducerByteRateOverrideProp, "1")
    quotaProps.put(DynamicConfig.Client.RequestPercentageOverrideProp, "0.01")
    adminZkClient.changeClientIdConfig(Sanitizer.sanitize(smallQuotaProducerClientId), quotaProps)
    quotaProps.put(DynamicConfig.Client.ConsumerByteRateOverrideProp, "1")
    quotaProps.put(DynamicConfig.Client.RequestPercentageOverrideProp, "0.01")
    adminZkClient.changeClientIdConfig(Sanitizer.sanitize(smallQuotaConsumerClientId), quotaProps)

    TestUtils.retry(20000) {
      val quotaManager = servers.head.dataPlaneRequestProcessor.quotas.request
      assertEquals(Quota.upperBound(0.01), quotaManager.quota("some-user", "some-client"), s"Default request quota not set")
      assertEquals(Quota.upperBound(2000), quotaManager.quota("some-user", unthrottledClientId), s"Request quota override not set")
      val produceQuotaManager = servers.head.dataPlaneRequestProcessor.quotas.produce
      assertEquals(Quota.upperBound(1), produceQuotaManager.quota("some-user", smallQuotaProducerClientId), s"Produce quota override not set")
      val consumeQuotaManager = servers.head.dataPlaneRequestProcessor.quotas.fetch
      assertEquals(Quota.upperBound(1), consumeQuotaManager.quota("some-user", smallQuotaConsumerClientId), s"Consume quota override not set")
    }
  }

  @AfterEach
  override def tearDown(): Unit = {
    try executor.shutdownNow()
    finally super.tearDown()
  }

  @Test
  def testResponseThrottleTime(): Unit = {
    for (apiKey <- RequestQuotaTest.ClientActions)
      submitTest(apiKey, () => checkRequestThrottleTime(apiKey))

    waitAndCheckResults()
  }

  @Test
  def testResponseThrottleTimeWhenBothProduceAndRequestQuotasViolated(): Unit = {
    submitTest(ApiKeys.PRODUCE, () => checkSmallQuotaProducerRequestThrottleTime())
    waitAndCheckResults()
  }

  @Test
  def testResponseThrottleTimeWhenBothFetchAndRequestQuotasViolated(): Unit = {
    submitTest(ApiKeys.FETCH, () => checkSmallQuotaConsumerRequestThrottleTime())
    waitAndCheckResults()
  }

  @Test
  def testUnthrottledClient(): Unit = {
    for (apiKey <- RequestQuotaTest.ClientActions) {
      submitTest(apiKey, () => checkUnthrottledClient(apiKey))
    }

    waitAndCheckResults()
  }

  @Test
  def testExemptRequestTime(): Unit = {
    for (apiKey <- RequestQuotaTest.ClusterActions) {
      submitTest(apiKey, () => checkExemptRequestMetric(apiKey))
    }

    waitAndCheckResults()
  }

  @Test
  def testUnauthorizedThrottle(): Unit = {
    RequestQuotaTest.principal = RequestQuotaTest.UnauthorizedPrincipal

    for (apiKey <- ApiKeys.brokerApis.asScala) {
      submitTest(apiKey, () => checkUnauthorizedRequestThrottle(apiKey))
    }

    waitAndCheckResults()
  }

  def session(user: String): Session = Session(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, user), null)

  private def throttleTimeMetricValue(clientId: String): Double = {
    throttleTimeMetricValueForQuotaType(clientId, QuotaType.Request)
  }

  private def throttleTimeMetricValueForQuotaType(clientId: String, quotaType: QuotaType): Double = {
    val metricName = leaderNode.metrics.metricName("throttle-time", quotaType.toString,
      "", "user", "", "client-id", clientId)
    val sensor = leaderNode.quotaManagers.request.getOrCreateQuotaSensors(session("ANONYMOUS"),
      clientId).throttleTimeSensor
    metricValue(leaderNode.metrics.metrics.get(metricName), sensor)
  }

  private def requestTimeMetricValue(clientId: String): Double = {
    val metricName = leaderNode.metrics.metricName("request-time", QuotaType.Request.toString,
      "", "user", "", "client-id", clientId)
    val sensor = leaderNode.quotaManagers.request.getOrCreateQuotaSensors(session("ANONYMOUS"),
      clientId).quotaSensor
    metricValue(leaderNode.metrics.metrics.get(metricName), sensor)
  }

  private def exemptRequestMetricValue: Double = {
    val metricName = leaderNode.metrics.metricName("exempt-request-time", QuotaType.Request.toString, "")
    metricValue(leaderNode.metrics.metrics.get(metricName), leaderNode.quotaManagers.request.exemptSensor)
  }

  private def metricValue(metric: KafkaMetric, sensor: Sensor): Double = {
    sensor.synchronized {
      if (metric == null) -1.0 else metric.metricValue.asInstanceOf[Double]
    }
  }

  private def requestBuilder(apiKey: ApiKeys): AbstractRequest.Builder[_ <: AbstractRequest] = {
    apiKey match {
        case ApiKeys.PRODUCE =>
          requests.ProduceRequest.forCurrentMagic(new ProduceRequestData()
            .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
              Collections.singletonList(new ProduceRequestData.TopicProduceData()
                .setName(tp.topic()).setPartitionData(Collections.singletonList(
                new ProduceRequestData.PartitionProduceData()
                  .setIndex(tp.partition())
                  .setRecords(MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord("test".getBytes))))))
                .iterator))
            .setAcks(1.toShort)
            .setTimeoutMs(5000))

        case ApiKeys.FETCH =>
          val partitionMap = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
          partitionMap.put(tp, new FetchRequest.PartitionData(0, 0, 100, Optional.of(15)))
          FetchRequest.Builder.forConsumer(0, 0, partitionMap)

        case ApiKeys.METADATA =>
          new MetadataRequest.Builder(List(topic).asJava, true)

        case ApiKeys.LIST_OFFSETS =>
          val topic = new ListOffsetsTopic()
            .setName(tp.topic)
            .setPartitions(List(new ListOffsetsPartition()
              .setPartitionIndex(tp.partition)
              .setTimestamp(0L)
              .setCurrentLeaderEpoch(15)).asJava)
          ListOffsetsRequest.Builder.forConsumer(false, IsolationLevel.READ_UNCOMMITTED)
            .setTargetTimes(List(topic).asJava)

        case ApiKeys.LEADER_AND_ISR =>
          new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, brokerId, Int.MaxValue, Long.MaxValue,
            Seq(new LeaderAndIsrPartitionState()
              .setTopicName(tp.topic)
              .setPartitionIndex(tp.partition)
              .setControllerEpoch(Int.MaxValue)
              .setLeader(brokerId)
              .setLeaderEpoch(Int.MaxValue)
              .setIsr(List(brokerId).asJava)
              .setZkVersion(2)
              .setReplicas(Seq(brokerId).asJava)
              .setIsNew(true)).asJava,
            topicIds,
            Set(new Node(brokerId, "localhost", 0)).asJava)

        case ApiKeys.STOP_REPLICA =>
          val topicStates = Seq(
            new StopReplicaTopicState()
              .setTopicName(tp.topic())
              .setPartitionStates(Seq(new StopReplicaPartitionState()
                .setPartitionIndex(tp.partition())
                .setLeaderEpoch(LeaderAndIsr.initialLeaderEpoch + 2)
                .setDeletePartition(true)).asJava)
          ).asJava
          new StopReplicaRequest.Builder(ApiKeys.STOP_REPLICA.latestVersion, brokerId,
            Int.MaxValue, Long.MaxValue, false, topicStates)

        case ApiKeys.UPDATE_METADATA =>
          val partitionState = Seq(new UpdateMetadataPartitionState()
            .setTopicName(tp.topic)
            .setPartitionIndex(tp.partition)
            .setControllerEpoch(Int.MaxValue)
            .setLeader(brokerId)
            .setLeaderEpoch(Int.MaxValue)
            .setIsr(List(brokerId).asJava)
            .setZkVersion(2)
            .setReplicas(Seq(brokerId).asJava)).asJava
          val securityProtocol = SecurityProtocol.PLAINTEXT
          val brokers = Seq(new UpdateMetadataBroker()
            .setId(brokerId)
            .setEndpoints(Seq(new UpdateMetadataEndpoint()
              .setHost("localhost")
              .setPort(0)
              .setSecurityProtocol(securityProtocol.id)
              .setListener(ListenerName.forSecurityProtocol(securityProtocol).value)).asJava)).asJava
          new UpdateMetadataRequest.Builder(ApiKeys.UPDATE_METADATA.latestVersion, brokerId, Int.MaxValue, Long.MaxValue,
            partitionState, brokers, Collections.emptyMap())

        case ApiKeys.CONTROLLED_SHUTDOWN =>
          new ControlledShutdownRequest.Builder(
              new ControlledShutdownRequestData()
                .setBrokerId(brokerId)
                .setBrokerEpoch(Long.MaxValue),
              ApiKeys.CONTROLLED_SHUTDOWN.latestVersion)

        case ApiKeys.OFFSET_COMMIT =>
          new OffsetCommitRequest.Builder(
            new OffsetCommitRequestData()
              .setGroupId("test-group")
              .setGenerationId(1)
              .setMemberId(JoinGroupRequest.UNKNOWN_MEMBER_ID)
              .setTopics(
                Collections.singletonList(
                  new OffsetCommitRequestData.OffsetCommitRequestTopic()
                    .setName(topic)
                    .setPartitions(
                      Collections.singletonList(
                        new OffsetCommitRequestData.OffsetCommitRequestPartition()
                          .setPartitionIndex(0)
                          .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                          .setCommittedOffset(0)
                          .setCommittedMetadata("metadata")
                      )
                    )
                )
              )
          )
        case ApiKeys.OFFSET_FETCH =>
          new OffsetFetchRequest.Builder("test-group", false, List(tp).asJava, false)

        case ApiKeys.FIND_COORDINATOR =>
          new FindCoordinatorRequest.Builder(
              new FindCoordinatorRequestData()
                .setKeyType(FindCoordinatorRequest.CoordinatorType.GROUP.id)
                .setKey("test-group"))

        case ApiKeys.JOIN_GROUP =>
          new JoinGroupRequest.Builder(
            new JoinGroupRequestData()
              .setGroupId("test-join-group")
              .setSessionTimeoutMs(200)
              .setMemberId(JoinGroupRequest.UNKNOWN_MEMBER_ID)
              .setGroupInstanceId(null)
              .setProtocolType("consumer")
              .setProtocols(
                new JoinGroupRequestProtocolCollection(
                  Collections.singletonList(new JoinGroupRequestData.JoinGroupRequestProtocol()
                    .setName("consumer-range")
                    .setMetadata("test".getBytes())).iterator()
                )
              )
              .setRebalanceTimeoutMs(100)
          )

        case ApiKeys.HEARTBEAT =>
          new HeartbeatRequest.Builder(
            new HeartbeatRequestData()
              .setGroupId("test-group")
              .setGenerationId(1)
              .setMemberId(JoinGroupRequest.UNKNOWN_MEMBER_ID)
          )

        case ApiKeys.LEAVE_GROUP =>
          new LeaveGroupRequest.Builder(
            "test-leave-group",
            Collections.singletonList(
              new MemberIdentity()
                .setMemberId(JoinGroupRequest.UNKNOWN_MEMBER_ID))
          )

        case ApiKeys.SYNC_GROUP =>
          new SyncGroupRequest.Builder(
            new SyncGroupRequestData()
              .setGroupId("test-sync-group")
              .setGenerationId(1)
              .setMemberId(JoinGroupRequest.UNKNOWN_MEMBER_ID)
              .setAssignments(Collections.emptyList())
          )

        case ApiKeys.DESCRIBE_GROUPS =>
          new DescribeGroupsRequest.Builder(new DescribeGroupsRequestData().setGroups(List("test-group").asJava))

        case ApiKeys.LIST_GROUPS =>
          new ListGroupsRequest.Builder(new ListGroupsRequestData())

        case ApiKeys.SASL_HANDSHAKE =>
          new SaslHandshakeRequest.Builder(new SaslHandshakeRequestData().setMechanism("PLAIN"))

        case ApiKeys.SASL_AUTHENTICATE =>
          new SaslAuthenticateRequest.Builder(new SaslAuthenticateRequestData().setAuthBytes(new Array[Byte](0)))

        case ApiKeys.API_VERSIONS =>
          new ApiVersionsRequest.Builder()

        case ApiKeys.CREATE_TOPICS =>
          new CreateTopicsRequest.Builder(
            new CreateTopicsRequestData().setTopics(
              new CreatableTopicCollection(Collections.singleton(
                new CreatableTopic().setName("topic-2").setNumPartitions(1).
                  setReplicationFactor(1.toShort)).iterator())))

        case ApiKeys.DELETE_TOPICS =>
          new DeleteTopicsRequest.Builder(
              new DeleteTopicsRequestData()
              .setTopicNames(Collections.singletonList("topic-2"))
              .setTimeoutMs(5000))

        case ApiKeys.DELETE_RECORDS =>
          new DeleteRecordsRequest.Builder(
            new DeleteRecordsRequestData()
              .setTimeoutMs(5000)
              .setTopics(Collections.singletonList(new DeleteRecordsRequestData.DeleteRecordsTopic()
                .setName(tp.topic())
                .setPartitions(Collections.singletonList(new DeleteRecordsRequestData.DeleteRecordsPartition()
                  .setPartitionIndex(tp.partition())
                  .setOffset(0L))))))

        case ApiKeys.INIT_PRODUCER_ID =>
          val requestData = new InitProducerIdRequestData()
            .setTransactionalId("test-transactional-id")
            .setTransactionTimeoutMs(5000)
          new InitProducerIdRequest.Builder(requestData)

        case ApiKeys.OFFSET_FOR_LEADER_EPOCH =>
          val epochs = new OffsetForLeaderTopicCollection()
          epochs.add(new OffsetForLeaderTopic()
            .setTopic(tp.topic())
            .setPartitions(List(new OffsetForLeaderPartition()
              .setPartition(tp.partition())
              .setLeaderEpoch(0)
              .setCurrentLeaderEpoch(15)).asJava))
          OffsetsForLeaderEpochRequest.Builder.forConsumer(epochs)

        case ApiKeys.ADD_PARTITIONS_TO_TXN =>
          new AddPartitionsToTxnRequest.Builder("test-transactional-id", 1, 0, List(tp).asJava)

        case ApiKeys.ADD_OFFSETS_TO_TXN =>
          new AddOffsetsToTxnRequest.Builder(new AddOffsetsToTxnRequestData()
            .setTransactionalId("test-transactional-id")
            .setProducerId(1)
            .setProducerEpoch(0)
            .setGroupId("test-txn-group")
          )

        case ApiKeys.END_TXN =>
          new EndTxnRequest.Builder(new EndTxnRequestData()
            .setTransactionalId("test-transactional-id")
            .setProducerId(1)
            .setProducerEpoch(0)
            .setCommitted(false)
          )

        case ApiKeys.WRITE_TXN_MARKERS =>
          new WriteTxnMarkersRequest.Builder(ApiKeys.WRITE_TXN_MARKERS.latestVersion(), List.empty.asJava)

        case ApiKeys.TXN_OFFSET_COMMIT =>
          new TxnOffsetCommitRequest.Builder(
            "test-transactional-id",
            "test-txn-group",
            2,
            0,
            Map.empty[TopicPartition, TxnOffsetCommitRequest.CommittedOffset].asJava,
            false)

        case ApiKeys.DESCRIBE_ACLS =>
          new DescribeAclsRequest.Builder(AclBindingFilter.ANY)

        case ApiKeys.CREATE_ACLS =>
          new CreateAclsRequest.Builder(new CreateAclsRequestData().setCreations(Collections.singletonList(
            new CreateAclsRequestData.AclCreation()
              .setResourceType(AdminResourceType.TOPIC.code)
              .setResourceName("mytopic")
              .setResourcePatternType(PatternType.LITERAL.code)
              .setPrincipal("User:ANONYMOUS")
              .setHost("*")
              .setOperation(AclOperation.WRITE.code)
              .setPermissionType(AclPermissionType.DENY.code))))
        case ApiKeys.DELETE_ACLS =>
          new DeleteAclsRequest.Builder(new DeleteAclsRequestData().setFilters(Collections.singletonList(
            new DeleteAclsRequestData.DeleteAclsFilter()
              .setResourceTypeFilter(AdminResourceType.TOPIC.code)
              .setResourceNameFilter(null)
              .setPatternTypeFilter(PatternType.LITERAL.code)
              .setPrincipalFilter("User:ANONYMOUS")
              .setHostFilter("*")
              .setOperation(AclOperation.ANY.code)
              .setPermissionType(AclPermissionType.DENY.code))))
        case ApiKeys.DESCRIBE_CONFIGS =>
          new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData()
            .setResources(Collections.singletonList(new DescribeConfigsRequestData.DescribeConfigsResource()
              .setResourceType(ConfigResource.Type.TOPIC.id)
              .setResourceName(tp.topic))))

        case ApiKeys.ALTER_CONFIGS =>
          new AlterConfigsRequest.Builder(
            Collections.singletonMap(new ConfigResource(ConfigResource.Type.TOPIC, tp.topic),
              new AlterConfigsRequest.Config(Collections.singleton(
                new AlterConfigsRequest.ConfigEntry(LogConfig.MaxMessageBytesProp, "1000000")
              ))), true)

        case ApiKeys.ALTER_REPLICA_LOG_DIRS =>
          val dir = new AlterReplicaLogDirsRequestData.AlterReplicaLogDir()
            .setPath(logDir)
          dir.topics.add(new AlterReplicaLogDirsRequestData.AlterReplicaLogDirTopic()
            .setName(tp.topic)
            .setPartitions(Collections.singletonList(tp.partition)))
          val data = new AlterReplicaLogDirsRequestData();
          data.dirs.add(dir)
          new AlterReplicaLogDirsRequest.Builder(data)

        case ApiKeys.DESCRIBE_LOG_DIRS =>
          val data = new DescribeLogDirsRequestData()
          data.topics.add(new DescribeLogDirsRequestData.DescribableLogDirTopic()
            .setTopic(tp.topic)
            .setPartitionIndex(Collections.singletonList(tp.partition)))
          new DescribeLogDirsRequest.Builder(data)

        case ApiKeys.CREATE_PARTITIONS =>
          val data = new CreatePartitionsRequestData()
            .setTimeoutMs(0)
            .setValidateOnly(false)
          data.topics().add(new CreatePartitionsTopic().setName("topic-2").setCount(1))
          new CreatePartitionsRequest.Builder(data)

        case ApiKeys.CREATE_DELEGATION_TOKEN =>
          new CreateDelegationTokenRequest.Builder(
              new CreateDelegationTokenRequestData()
                .setRenewers(Collections.singletonList(new CreateDelegationTokenRequestData.CreatableRenewers()
                .setPrincipalType("User")
                .setPrincipalName("test")))
                .setMaxLifetimeMs(1000)
          )

        case ApiKeys.EXPIRE_DELEGATION_TOKEN =>
          new ExpireDelegationTokenRequest.Builder(
              new ExpireDelegationTokenRequestData()
                .setHmac("".getBytes)
                .setExpiryTimePeriodMs(1000L))

        case ApiKeys.DESCRIBE_DELEGATION_TOKEN =>
          new DescribeDelegationTokenRequest.Builder(Collections.singletonList(SecurityUtils.parseKafkaPrincipal("User:test")))

        case ApiKeys.RENEW_DELEGATION_TOKEN =>
          new RenewDelegationTokenRequest.Builder(
              new RenewDelegationTokenRequestData()
                .setHmac("".getBytes)
                .setRenewPeriodMs(1000L))

        case ApiKeys.DELETE_GROUPS =>
          new DeleteGroupsRequest.Builder(new DeleteGroupsRequestData()
            .setGroupsNames(Collections.singletonList("test-group")))

        case ApiKeys.ELECT_LEADERS =>
          new ElectLeadersRequest.Builder(
            ElectionType.PREFERRED,
            Collections.singletonList(new TopicPartition("my_topic", 0)),
            0
          )

        case ApiKeys.INCREMENTAL_ALTER_CONFIGS =>
          new IncrementalAlterConfigsRequest.Builder(
            new IncrementalAlterConfigsRequestData())

        case ApiKeys.ALTER_PARTITION_REASSIGNMENTS =>
          new AlterPartitionReassignmentsRequest.Builder(
            new AlterPartitionReassignmentsRequestData()
          )

        case ApiKeys.LIST_PARTITION_REASSIGNMENTS =>
          new ListPartitionReassignmentsRequest.Builder(
            new ListPartitionReassignmentsRequestData()
          )

        case ApiKeys.OFFSET_DELETE =>
          new OffsetDeleteRequest.Builder(
            new OffsetDeleteRequestData()
              .setGroupId("test-group")
              .setTopics(new OffsetDeleteRequestData.OffsetDeleteRequestTopicCollection(
                Collections.singletonList(new OffsetDeleteRequestData.OffsetDeleteRequestTopic()
                  .setName("test-topic")
                  .setPartitions(Collections.singletonList(
                    new OffsetDeleteRequestData.OffsetDeleteRequestPartition()
                      .setPartitionIndex(0)))).iterator())))

        case ApiKeys.DESCRIBE_CLIENT_QUOTAS =>
          new DescribeClientQuotasRequest.Builder(ClientQuotaFilter.all())

        case ApiKeys.ALTER_CLIENT_QUOTAS =>
          new AlterClientQuotasRequest.Builder(List.empty.asJava, false)

        case ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS =>
          new DescribeUserScramCredentialsRequest.Builder(new DescribeUserScramCredentialsRequestData())

        case ApiKeys.ALTER_USER_SCRAM_CREDENTIALS =>
          new AlterUserScramCredentialsRequest.Builder(new AlterUserScramCredentialsRequestData())

        case ApiKeys.VOTE =>
          new VoteRequest.Builder(VoteRequest.singletonRequest(tp, 1, 2, 0, 10))

        case ApiKeys.BEGIN_QUORUM_EPOCH =>
          new BeginQuorumEpochRequest.Builder(BeginQuorumEpochRequest.singletonRequest(tp, 2, 5))

        case ApiKeys.END_QUORUM_EPOCH =>
          new EndQuorumEpochRequest.Builder(EndQuorumEpochRequest.singletonRequest(
            tp, 10, 5, Collections.singletonList(3)))

        case ApiKeys.ALTER_ISR =>
          new AlterIsrRequest.Builder(new AlterIsrRequestData())

        case ApiKeys.UPDATE_FEATURES =>
          new UpdateFeaturesRequest.Builder(new UpdateFeaturesRequestData())

        case ApiKeys.ENVELOPE =>
          val requestHeader = new RequestHeader(
            ApiKeys.ALTER_CLIENT_QUOTAS,
            ApiKeys.ALTER_CLIENT_QUOTAS.latestVersion,
            "client-id",
            0
          )
          val embedRequestData = RequestTestUtils.serializeRequestWithHeader(requestHeader,
            new AlterClientQuotasRequest.Builder(List.empty.asJava, false).build())
          new EnvelopeRequest.Builder(embedRequestData, new Array[Byte](0),
            InetAddress.getByName("192.168.1.1").getAddress)

        case ApiKeys.DESCRIBE_CLUSTER =>
          new DescribeClusterRequest.Builder(new DescribeClusterRequestData())

        case ApiKeys.DESCRIBE_PRODUCERS =>
          new DescribeProducersRequest.Builder(new DescribeProducersRequestData()
            .setTopics(List(new DescribeProducersRequestData.TopicRequest()
              .setName("test-topic")
              .setPartitionIndexes(List(1, 2, 3).map(Int.box).asJava)).asJava))

        case _ =>
          throw new IllegalArgumentException("Unsupported API key " + apiKey)
    }
  }

  case class Client(clientId: String, apiKey: ApiKeys) {
    var correlationId: Int = 0
    def runUntil(until: AbstractResponse => Boolean): Boolean = {
      val startMs = System.currentTimeMillis
      var done = false
      val socket = connect()
      try {
        while (!done && System.currentTimeMillis < startMs + 10000) {
          correlationId += 1
          val request = requestBuilder(apiKey).build()
          val response = sendAndReceive[AbstractResponse](request, socket, clientId, Some(correlationId))
          done = until.apply(response)
        }
      } finally {
        socket.close()
      }
      done
    }

    override def toString: String = {
      val requestTime = requestTimeMetricValue(clientId)
      val throttleTime = throttleTimeMetricValue(clientId)
      val produceThrottleTime = throttleTimeMetricValueForQuotaType(clientId, QuotaType.Produce)
      val consumeThrottleTime = throttleTimeMetricValueForQuotaType(clientId, QuotaType.Fetch)
      s"Client $clientId apiKey $apiKey requests $correlationId requestTime $requestTime " +
      s"throttleTime $throttleTime produceThrottleTime $produceThrottleTime consumeThrottleTime $consumeThrottleTime"
    }
  }

  private def submitTest(apiKey: ApiKeys, test: () => Unit): Unit = {
    val future = executor.submit(new Runnable() {
      def run(): Unit = {
        test.apply()
      }
    })
    tasks += Task(apiKey, future)
  }

  private def waitAndCheckResults(): Unit = {
    for (task <- tasks) {
      try {
        task.future.get(15, TimeUnit.SECONDS)
      } catch {
        case e: Throwable =>
          error(s"Test failed for api-key ${task.apiKey} with exception $e")
          throw e
      }
    }
  }

  private def checkRequestThrottleTime(apiKey: ApiKeys): Unit = {
    // Request until throttled using client-id with default small quota
    val clientId = apiKey.toString
    val client = Client(clientId, apiKey)
    val throttled = client.runUntil(_.throttleTimeMs > 0)

    assertTrue(throttled, s"Response not throttled: $client")
    assertTrue(throttleTimeMetricValue(clientId) > 0 , s"Throttle time metrics not updated: $client")
  }

  private def checkSmallQuotaProducerRequestThrottleTime(): Unit = {

    // Request until throttled using client-id with default small producer quota
    val smallQuotaProducerClient = Client(smallQuotaProducerClientId, ApiKeys.PRODUCE)
    val throttled = smallQuotaProducerClient.runUntil(_.throttleTimeMs > 0)

    assertTrue(throttled, s"Response not throttled: $smallQuotaProducerClient")
    assertTrue(throttleTimeMetricValueForQuotaType(smallQuotaProducerClientId, QuotaType.Produce) > 0,
      s"Throttle time metrics for produce quota not updated: $smallQuotaProducerClient")
    assertTrue(throttleTimeMetricValueForQuotaType(smallQuotaProducerClientId, QuotaType.Request).isNaN,
      s"Throttle time metrics for request quota updated: $smallQuotaProducerClient")
  }

  private def checkSmallQuotaConsumerRequestThrottleTime(): Unit = {

    // Request until throttled using client-id with default small consumer quota
    val smallQuotaConsumerClient =   Client(smallQuotaConsumerClientId, ApiKeys.FETCH)
    val throttled = smallQuotaConsumerClient.runUntil(_.throttleTimeMs > 0)

    assertTrue(throttled, s"Response not throttled: $smallQuotaConsumerClientId")
    assertTrue(throttleTimeMetricValueForQuotaType(smallQuotaConsumerClientId, QuotaType.Fetch) > 0,
      s"Throttle time metrics for consumer quota not updated: $smallQuotaConsumerClient")
    assertTrue(throttleTimeMetricValueForQuotaType(smallQuotaConsumerClientId, QuotaType.Request).isNaN,
      s"Throttle time metrics for request quota updated: $smallQuotaConsumerClient")
  }

  private def checkUnthrottledClient(apiKey: ApiKeys): Unit = {

    // Test that request from client with large quota is not throttled
    val unthrottledClient = Client(unthrottledClientId, apiKey)
    unthrottledClient.runUntil(_.throttleTimeMs <= 0.0)
    assertEquals(1, unthrottledClient.correlationId)
    assertTrue(throttleTimeMetricValue(unthrottledClientId).isNaN, s"Client should not have been throttled: $unthrottledClient")
  }

  private def checkExemptRequestMetric(apiKey: ApiKeys): Unit = {
    val exemptTarget = exemptRequestMetricValue + 0.02
    val clientId = apiKey.toString
    val client = Client(clientId, apiKey)
    val updated = client.runUntil(_ => exemptRequestMetricValue > exemptTarget)

    assertTrue(updated, s"Exempt-request-time metric not updated: $client")
    assertTrue(throttleTimeMetricValue(clientId).isNaN, s"Client should not have been throttled: $client")
  }

  private def checkUnauthorizedRequestThrottle(apiKey: ApiKeys): Unit = {
    val clientId = "unauthorized-" + apiKey.toString
    val client = Client(clientId, apiKey)
    val throttled = client.runUntil(_ => throttleTimeMetricValue(clientId) > 0.0)
    assertTrue(throttled, s"Unauthorized client should have been throttled: $client")
  }
}

object RequestQuotaTest {
  val ClusterActions = ApiKeys.brokerApis.asScala.filter(_.clusterAction).toSet
  val SaslActions = Set(ApiKeys.SASL_HANDSHAKE, ApiKeys.SASL_AUTHENTICATE)
  val ClientActions = ApiKeys.brokerApis.asScala.toSet -- ClusterActions -- SaslActions

  val UnauthorizedPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Unauthorized")
  // Principal used for all client connections. This is modified by tests which
  // check unauthorized code path
  var principal = KafkaPrincipal.ANONYMOUS
  class TestAuthorizer extends AclAuthorizer {
    override def authorize(requestContext: AuthorizableRequestContext, actions: util.List[Action]): util.List[AuthorizationResult] = {
      actions.asScala.map { _ =>
        if (requestContext.principal != UnauthorizedPrincipal) AuthorizationResult.ALLOWED else AuthorizationResult.DENIED
      }.asJava
    }
  }
  class TestPrincipalBuilder extends KafkaPrincipalBuilder with KafkaPrincipalSerde {
    override def build(context: AuthenticationContext): KafkaPrincipal = {
      principal
    }

    override def serialize(principal: KafkaPrincipal): Array[Byte] = {
      new Array[Byte](0)
    }

    override def deserialize(bytes: Array[Byte]): KafkaPrincipal = {
      principal
    }
  }
}
