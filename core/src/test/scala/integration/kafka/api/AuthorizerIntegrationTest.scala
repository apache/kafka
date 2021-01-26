/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package kafka.api

import java.lang.{Byte => JByte}
import java.time.Duration
import java.util
import java.util.concurrent.ExecutionException
import java.util.regex.Pattern
import java.util.{Collections, Optional, Properties}
import kafka.admin.ConsumerGroupCommand.{ConsumerGroupCommandOptions, ConsumerGroupService}
import kafka.log.LogConfig
import kafka.security.authorizer.AclEntry
import kafka.security.authorizer.AclEntry.WildcardHost
import kafka.server.{BaseRequestTest, KafkaConfig}
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, AlterConfigOp}
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.acl.AclOperation._
import org.apache.kafka.common.acl.AclPermissionType.{ALLOW, DENY}
import org.apache.kafka.common.acl.{AccessControlEntry, AccessControlEntryFilter, AclBindingFilter, AclOperation, AclPermissionType}
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.config.{ConfigResource, LogLevelConfig}
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.Topic.GROUP_METADATA_TOPIC_NAME
import org.apache.kafka.common.message.{AddOffsetsToTxnRequestData, AlterPartitionReassignmentsRequestData, AlterReplicaLogDirsRequestData, ControlledShutdownRequestData, CreateAclsRequestData, CreatePartitionsRequestData, CreateTopicsRequestData, DeleteAclsRequestData, DeleteGroupsRequestData, DeleteRecordsRequestData, DeleteTopicsRequestData, DescribeConfigsRequestData, DescribeGroupsRequestData, DescribeLogDirsRequestData, FindCoordinatorRequestData, HeartbeatRequestData, IncrementalAlterConfigsRequestData, JoinGroupRequestData, ListPartitionReassignmentsRequestData, OffsetCommitRequestData, ProduceRequestData, SyncGroupRequestData}
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic
import org.apache.kafka.common.message.CreateTopicsRequestData.{CreatableTopic, CreatableTopicCollection}
import org.apache.kafka.common.message.DescribeClusterRequestData
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.{AlterConfigsResource, AlterableConfig, AlterableConfigCollection}
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocolCollection
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity
import org.apache.kafka.common.message.ListOffsetsRequestData.{ListOffsetsPartition, ListOffsetsTopic}
import org.apache.kafka.common.message.MetadataRequestData
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderPartition
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopic
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopicCollection
import org.apache.kafka.common.message.StopReplicaRequestData.{StopReplicaPartitionState, StopReplicaTopicState}
import org.apache.kafka.common.message.UpdateMetadataRequestData.{UpdateMetadataBroker, UpdateMetadataEndpoint, UpdateMetadataPartitionState}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, RecordBatch, Records, SimpleRecord}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.resource.PatternType.{LITERAL, PREFIXED}
import org.apache.kafka.common.resource.ResourceType._
import org.apache.kafka.common.resource.{PatternType, Resource, ResourcePattern, ResourcePatternFilter, ResourceType}
import org.apache.kafka.common.security.auth.{AuthenticationContext, KafkaPrincipal, KafkaPrincipalBuilder, SecurityProtocol}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.{ElectionType, IsolationLevel, Node, TopicPartition, Uuid, requests}
import org.apache.kafka.test.{TestUtils => JTestUtils}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import scala.annotation.nowarn
import scala.jdk.CollectionConverters._
import scala.collection.mutable
import scala.collection.mutable.Buffer

object AuthorizerIntegrationTest {
  val BrokerPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "broker")
  val ClientPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "client")

  val BrokerListenerName = "BROKER"
  val ClientListenerName = "CLIENT"

  class PrincipalBuilder extends KafkaPrincipalBuilder {
    override def build(context: AuthenticationContext): KafkaPrincipal = {
      context.listenerName match {
        case BrokerListenerName => BrokerPrincipal
        case ClientListenerName => ClientPrincipal
        case listenerName => throw new IllegalArgumentException(s"No principal mapped to listener $listenerName")
      }
    }
  }
}

class AuthorizerIntegrationTest extends BaseRequestTest {
  import AuthorizerIntegrationTest._

  override def interBrokerListenerName: ListenerName = new ListenerName(BrokerListenerName)
  override def listenerName: ListenerName = new ListenerName(ClientListenerName)
  override def brokerCount: Int = 1

  def clientPrincipal: KafkaPrincipal = ClientPrincipal
  def brokerPrincipal: KafkaPrincipal = BrokerPrincipal

  val clientPrincipalString: String = clientPrincipal.toString

  val brokerId: Integer = 0
  val topic = "topic"
  val topicId = Uuid.randomUuid()
  val topicPattern = "topic.*"
  val transactionalId = "transactional.id"
  val producerId = 83392L
  val part = 0
  val correlationId = 0
  val clientId = "client-Id"
  val tp = new TopicPartition(topic, part)
  val topicIds = Collections.singletonMap(topic, topicId)
  val topicNames = Collections.singletonMap(topicId, topic)
  val logDir = "logDir"
  val group = "my-group"
  val protocolType = "consumer"
  val protocolName = "consumer-range"
  val clusterResource = new ResourcePattern(CLUSTER, Resource.CLUSTER_NAME, LITERAL)
  val topicResource = new ResourcePattern(TOPIC, topic, LITERAL)
  val groupResource = new ResourcePattern(GROUP, group, LITERAL)
  val transactionalIdResource = new ResourcePattern(TRANSACTIONAL_ID, transactionalId, LITERAL)

  val groupReadAcl = Map(groupResource -> Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)))
  val groupDescribeAcl = Map(groupResource -> Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DESCRIBE, ALLOW)))
  val groupDeleteAcl = Map(groupResource -> Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DELETE, ALLOW)))
  val clusterAcl = Map(clusterResource -> Set(new AccessControlEntry(clientPrincipalString, WildcardHost, CLUSTER_ACTION, ALLOW)))
  val clusterCreateAcl = Map(clusterResource -> Set(new AccessControlEntry(clientPrincipalString, WildcardHost, CREATE, ALLOW)))
  val clusterAlterAcl = Map(clusterResource -> Set(new AccessControlEntry(clientPrincipalString, WildcardHost, ALTER, ALLOW)))
  val clusterDescribeAcl = Map(clusterResource -> Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DESCRIBE, ALLOW)))
  val clusterAlterConfigsAcl = Map(clusterResource -> Set(new AccessControlEntry(clientPrincipalString, WildcardHost, ALTER_CONFIGS, ALLOW)))
  val clusterIdempotentWriteAcl = Map(clusterResource -> Set(new AccessControlEntry(clientPrincipalString, WildcardHost, IDEMPOTENT_WRITE, ALLOW)))
  val topicCreateAcl = Map(topicResource -> Set(new AccessControlEntry(clientPrincipalString, WildcardHost, CREATE, ALLOW)))
  val topicReadAcl = Map(topicResource -> Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)))
  val topicWriteAcl = Map(topicResource -> Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)))
  val topicDescribeAcl = Map(topicResource -> Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DESCRIBE, ALLOW)))
  val topicAlterAcl = Map(topicResource -> Set(new AccessControlEntry(clientPrincipalString, WildcardHost, ALTER, ALLOW)))
  val topicDeleteAcl = Map(topicResource -> Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DELETE, ALLOW)))
  val topicDescribeConfigsAcl = Map(topicResource -> Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DESCRIBE_CONFIGS, ALLOW)))
  val topicAlterConfigsAcl = Map(topicResource -> Set(new AccessControlEntry(clientPrincipalString, WildcardHost, ALTER_CONFIGS, ALLOW)))
  val transactionIdWriteAcl = Map(transactionalIdResource -> Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)))
  val transactionalIdDescribeAcl = Map(transactionalIdResource -> Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DESCRIBE, ALLOW)))

  val numRecords = 1
  val adminClients = Buffer[Admin]()

  producerConfig.setProperty(ProducerConfig.ACKS_CONFIG, "1")
  producerConfig.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "50000")
  consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group)

  override def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.put(KafkaConfig.AuthorizerClassNameProp, "kafka.security.auth.SimpleAclAuthorizer")
    properties.put(KafkaConfig.BrokerIdProp, brokerId.toString)
    properties.put(KafkaConfig.OffsetsTopicPartitionsProp, "1")
    properties.put(KafkaConfig.OffsetsTopicReplicationFactorProp, "1")
    properties.put(KafkaConfig.TransactionsTopicPartitionsProp, "1")
    properties.put(KafkaConfig.TransactionsTopicReplicationFactorProp, "1")
    properties.put(KafkaConfig.TransactionsTopicMinISRProp, "1")
    properties.put(BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG,
      classOf[PrincipalBuilder].getName)
  }

  @nowarn("cat=deprecation")
  val requestKeyToError = Map[ApiKeys, Nothing => Errors](
    ApiKeys.METADATA -> ((resp: requests.MetadataResponse) => resp.errors.asScala.find(_._1 == topic).getOrElse(("test", Errors.NONE))._2),
    ApiKeys.PRODUCE -> ((resp: requests.ProduceResponse) => resp.responses.asScala.find(_._1 == tp).get._2.error),
    ApiKeys.FETCH -> ((resp: requests.FetchResponse[Records]) => resp.responseData.asScala.find(_._1 == tp).get._2.error),
    ApiKeys.LIST_OFFSETS -> ((resp: ListOffsetsResponse) => {
      Errors.forCode(
        resp.data
          .topics.asScala.find(_.name == topic).get
          .partitions.asScala.find(_.partitionIndex == part).get
          .errorCode
      )
    }),
    ApiKeys.OFFSET_COMMIT -> ((resp: requests.OffsetCommitResponse) => Errors.forCode(
      resp.data().topics().get(0).partitions().get(0).errorCode())),
    ApiKeys.OFFSET_FETCH -> ((resp: requests.OffsetFetchResponse) => resp.error),
    ApiKeys.FIND_COORDINATOR -> ((resp: FindCoordinatorResponse) => resp.error),
    ApiKeys.UPDATE_METADATA -> ((resp: requests.UpdateMetadataResponse) => resp.error),
    ApiKeys.JOIN_GROUP -> ((resp: JoinGroupResponse) => resp.error),
    ApiKeys.SYNC_GROUP -> ((resp: SyncGroupResponse) => Errors.forCode(resp.data.errorCode())),
    ApiKeys.DESCRIBE_GROUPS -> ((resp: DescribeGroupsResponse) => {
      Errors.forCode(resp.data.groups.asScala.find(g => group == g.groupId).head.errorCode)
    }),
    ApiKeys.HEARTBEAT -> ((resp: HeartbeatResponse) => resp.error),
    ApiKeys.LEAVE_GROUP -> ((resp: LeaveGroupResponse) => resp.error),
    ApiKeys.DELETE_GROUPS -> ((resp: DeleteGroupsResponse) => resp.get(group)),
    ApiKeys.LEADER_AND_ISR -> ((resp: requests.LeaderAndIsrResponse) => Errors.forCode(
      resp.topics.asScala.find(t => topicNames.get(t.topicId) == tp.topic).get.partitionErrors.asScala.find(
        p => p.partitionIndex == tp.partition).get.errorCode)),
    ApiKeys.STOP_REPLICA -> ((resp: requests.StopReplicaResponse) => Errors.forCode(
      resp.partitionErrors.asScala.find(pe => pe.topicName == tp.topic && pe.partitionIndex == tp.partition).get.errorCode)),
    ApiKeys.CONTROLLED_SHUTDOWN -> ((resp: requests.ControlledShutdownResponse) => resp.error),
    ApiKeys.CREATE_TOPICS -> ((resp: CreateTopicsResponse) => Errors.forCode(resp.data.topics.find(topic).errorCode())),
    ApiKeys.DELETE_TOPICS -> ((resp: requests.DeleteTopicsResponse) => Errors.forCode(resp.data.responses.find(topic).errorCode())),
    ApiKeys.DELETE_RECORDS -> ((resp: requests.DeleteRecordsResponse) => Errors.forCode(
      resp.data.topics.find(tp.topic).partitions.find(tp.partition).errorCode)),
    ApiKeys.OFFSET_FOR_LEADER_EPOCH -> ((resp: OffsetsForLeaderEpochResponse) => Errors.forCode(
      resp.data.topics.find(tp.topic).partitions.asScala.find(_.partition == tp.partition).get.errorCode)),
    ApiKeys.DESCRIBE_CONFIGS -> ((resp: DescribeConfigsResponse) =>
      Errors.forCode(resp.resultMap.get(new ConfigResource(ConfigResource.Type.TOPIC, tp.topic)).errorCode)),
    ApiKeys.ALTER_CONFIGS -> ((resp: AlterConfigsResponse) =>
      resp.errors.get(new ConfigResource(ConfigResource.Type.TOPIC, tp.topic)).error),
    ApiKeys.INIT_PRODUCER_ID -> ((resp: InitProducerIdResponse) => resp.error),
    ApiKeys.WRITE_TXN_MARKERS -> ((resp: WriteTxnMarkersResponse) => resp.errorsByProducerId.get(producerId).get(tp)),
    ApiKeys.ADD_PARTITIONS_TO_TXN -> ((resp: AddPartitionsToTxnResponse) => resp.errors.get(tp)),
    ApiKeys.ADD_OFFSETS_TO_TXN -> ((resp: AddOffsetsToTxnResponse) => Errors.forCode(resp.data.errorCode)),
    ApiKeys.END_TXN -> ((resp: EndTxnResponse) => resp.error),
    ApiKeys.TXN_OFFSET_COMMIT -> ((resp: TxnOffsetCommitResponse) => resp.errors.get(tp)),
    ApiKeys.CREATE_ACLS -> ((resp: CreateAclsResponse) => Errors.forCode(resp.results.asScala.head.errorCode)),
    ApiKeys.DESCRIBE_ACLS -> ((resp: DescribeAclsResponse) => resp.error.error),
    ApiKeys.DELETE_ACLS -> ((resp: DeleteAclsResponse) => Errors.forCode(resp.filterResults.asScala.head.errorCode)),
    ApiKeys.ALTER_REPLICA_LOG_DIRS -> ((resp: AlterReplicaLogDirsResponse) => Errors.forCode(resp.data.results.asScala
      .find(x => x.topicName == tp.topic).get.partitions.asScala
      .find(p => p.partitionIndex == tp.partition).get.errorCode)),
    ApiKeys.DESCRIBE_LOG_DIRS -> ((resp: DescribeLogDirsResponse) =>
      if (resp.data.results.size() > 0) Errors.forCode(resp.data.results.get(0).errorCode) else Errors.CLUSTER_AUTHORIZATION_FAILED),
    ApiKeys.CREATE_PARTITIONS -> ((resp: CreatePartitionsResponse) => Errors.forCode(resp.data.results.asScala.head.errorCode())),
    ApiKeys.ELECT_LEADERS -> ((resp: ElectLeadersResponse) => Errors.forCode(resp.data().errorCode())),
    ApiKeys.INCREMENTAL_ALTER_CONFIGS -> ((resp: IncrementalAlterConfigsResponse) => {
      val topicResourceError = IncrementalAlterConfigsResponse.fromResponseData(resp.data()).get(new ConfigResource(ConfigResource.Type.TOPIC, tp.topic))
      if (topicResourceError == null)
        IncrementalAlterConfigsResponse.fromResponseData(resp.data()).get(new ConfigResource(ConfigResource.Type.BROKER_LOGGER, brokerId.toString)).error
      else
        topicResourceError.error()
    }),
    ApiKeys.ALTER_PARTITION_REASSIGNMENTS -> ((resp: AlterPartitionReassignmentsResponse) => Errors.forCode(resp.data().errorCode())),
    ApiKeys.LIST_PARTITION_REASSIGNMENTS -> ((resp: ListPartitionReassignmentsResponse) => Errors.forCode(resp.data().errorCode())),
    ApiKeys.OFFSET_DELETE -> ((resp: OffsetDeleteResponse) => {
      Errors.forCode(
        resp.data
          .topics.asScala.find(_.name == topic).get
          .partitions.asScala.find(_.partitionIndex == part).get
          .errorCode
      )
    })
  )

  val requestKeysToAcls = Map[ApiKeys, Map[ResourcePattern, Set[AccessControlEntry]]](
    ApiKeys.METADATA -> topicDescribeAcl,
    ApiKeys.PRODUCE -> (topicWriteAcl ++ transactionIdWriteAcl ++ clusterIdempotentWriteAcl),
    ApiKeys.FETCH -> topicReadAcl,
    ApiKeys.LIST_OFFSETS -> topicDescribeAcl,
    ApiKeys.OFFSET_COMMIT -> (topicReadAcl ++ groupReadAcl),
    ApiKeys.OFFSET_FETCH -> (topicReadAcl ++ groupDescribeAcl),
    ApiKeys.FIND_COORDINATOR -> (topicReadAcl ++ groupDescribeAcl ++ transactionalIdDescribeAcl),
    ApiKeys.UPDATE_METADATA -> clusterAcl,
    ApiKeys.JOIN_GROUP -> groupReadAcl,
    ApiKeys.SYNC_GROUP -> groupReadAcl,
    ApiKeys.DESCRIBE_GROUPS -> groupDescribeAcl,
    ApiKeys.HEARTBEAT -> groupReadAcl,
    ApiKeys.LEAVE_GROUP -> groupReadAcl,
    ApiKeys.DELETE_GROUPS -> groupDeleteAcl,
    ApiKeys.LEADER_AND_ISR -> clusterAcl,
    ApiKeys.STOP_REPLICA -> clusterAcl,
    ApiKeys.CONTROLLED_SHUTDOWN -> clusterAcl,
    ApiKeys.CREATE_TOPICS -> topicCreateAcl,
    ApiKeys.DELETE_TOPICS -> topicDeleteAcl,
    ApiKeys.DELETE_RECORDS -> topicDeleteAcl,
    ApiKeys.OFFSET_FOR_LEADER_EPOCH -> topicDescribeAcl,
    ApiKeys.DESCRIBE_CONFIGS -> topicDescribeConfigsAcl,
    ApiKeys.ALTER_CONFIGS -> topicAlterConfigsAcl,
    ApiKeys.INIT_PRODUCER_ID -> (transactionIdWriteAcl ++ clusterIdempotentWriteAcl),
    ApiKeys.WRITE_TXN_MARKERS -> clusterAcl,
    ApiKeys.ADD_PARTITIONS_TO_TXN -> (topicWriteAcl ++ transactionIdWriteAcl),
    ApiKeys.ADD_OFFSETS_TO_TXN -> (groupReadAcl ++ transactionIdWriteAcl),
    ApiKeys.END_TXN -> transactionIdWriteAcl,
    ApiKeys.TXN_OFFSET_COMMIT -> (groupReadAcl ++ transactionIdWriteAcl),
    ApiKeys.CREATE_ACLS -> clusterAlterAcl,
    ApiKeys.DESCRIBE_ACLS -> clusterDescribeAcl,
    ApiKeys.DELETE_ACLS -> clusterAlterAcl,
    ApiKeys.ALTER_REPLICA_LOG_DIRS -> clusterAlterAcl,
    ApiKeys.DESCRIBE_LOG_DIRS -> clusterDescribeAcl,
    ApiKeys.CREATE_PARTITIONS -> topicAlterAcl,
    ApiKeys.ELECT_LEADERS -> clusterAlterAcl,
    ApiKeys.INCREMENTAL_ALTER_CONFIGS -> topicAlterConfigsAcl,
    ApiKeys.ALTER_PARTITION_REASSIGNMENTS -> clusterAlterAcl,
    ApiKeys.LIST_PARTITION_REASSIGNMENTS -> clusterDescribeAcl,
    ApiKeys.OFFSET_DELETE -> groupReadAcl
  )

  @BeforeEach
  override def setUp(): Unit = {
    doSetup(createOffsetsTopic = false)

    // Allow inter-broker communication
    addAndVerifyAcls(Set(new AccessControlEntry(brokerPrincipal.toString, WildcardHost, CLUSTER_ACTION, ALLOW)), clusterResource)

    TestUtils.createOffsetsTopic(zkClient, servers)
  }

  @AfterEach
  override def tearDown(): Unit = {
    adminClients.foreach(_.close())
    removeAllClientAcls()
    super.tearDown()
  }

  private def createMetadataRequest(allowAutoTopicCreation: Boolean) = {
    new requests.MetadataRequest.Builder(List(topic).asJava, allowAutoTopicCreation).build()
  }

  private def createProduceRequest =
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
      .build()

  private def createFetchRequest = {
    val partitionMap = new util.LinkedHashMap[TopicPartition, requests.FetchRequest.PartitionData]
    partitionMap.put(tp, new requests.FetchRequest.PartitionData(0, 0, 100, Optional.of(27)))
    requests.FetchRequest.Builder.forConsumer(100, Int.MaxValue, partitionMap).build()
  }

  private def createFetchFollowerRequest = {
    val partitionMap = new util.LinkedHashMap[TopicPartition, requests.FetchRequest.PartitionData]
    partitionMap.put(tp, new requests.FetchRequest.PartitionData(0, 0, 100, Optional.of(27)))
    val version = ApiKeys.FETCH.latestVersion
    requests.FetchRequest.Builder.forReplica(version, 5000, 100, Int.MaxValue, partitionMap).build()
  }

  private def createListOffsetsRequest = {
    requests.ListOffsetsRequest.Builder.forConsumer(false, IsolationLevel.READ_UNCOMMITTED).setTargetTimes(
      List(new ListOffsetsTopic()
        .setName(tp.topic)
        .setPartitions(List(new ListOffsetsPartition()
          .setPartitionIndex(tp.partition)
          .setTimestamp(0L)
          .setCurrentLeaderEpoch(27)).asJava)).asJava
      ).
      build()
  }

  private def offsetsForLeaderEpochRequest: OffsetsForLeaderEpochRequest = {
    val epochs = new OffsetForLeaderTopicCollection()
    epochs.add(new OffsetForLeaderTopic()
      .setTopic(tp.topic())
      .setPartitions(List(new OffsetForLeaderPartition()
          .setPartition(tp.partition())
          .setLeaderEpoch(7)
          .setCurrentLeaderEpoch(27)).asJava))
    OffsetsForLeaderEpochRequest.Builder.forConsumer(epochs).build()
  }

  private def createOffsetFetchRequest = {
    new requests.OffsetFetchRequest.Builder(group, false, List(tp).asJava, false).build()
  }

  private def createFindCoordinatorRequest = {
    new FindCoordinatorRequest.Builder(
        new FindCoordinatorRequestData()
          .setKeyType(FindCoordinatorRequest.CoordinatorType.GROUP.id)
          .setKey(group)).build()
  }

  private def createUpdateMetadataRequest = {
    val partitionStates = Seq(new UpdateMetadataPartitionState()
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
    val version = ApiKeys.UPDATE_METADATA.latestVersion
    new requests.UpdateMetadataRequest.Builder(version, brokerId, Int.MaxValue, Long.MaxValue, partitionStates,
      brokers, Collections.emptyMap()).build()
  }

  private def createJoinGroupRequest = {
    val protocolSet = new JoinGroupRequestProtocolCollection(
      Collections.singletonList(new JoinGroupRequestData.JoinGroupRequestProtocol()
        .setName(protocolName)
        .setMetadata("test".getBytes())
    ).iterator())

    new JoinGroupRequest.Builder(
      new JoinGroupRequestData()
        .setGroupId(group)
        .setSessionTimeoutMs(10000)
        .setMemberId(JoinGroupRequest.UNKNOWN_MEMBER_ID)
        .setGroupInstanceId(null)
        .setProtocolType(protocolType)
        .setProtocols(protocolSet)
        .setRebalanceTimeoutMs(60000)
    ).build()
  }

  private def createSyncGroupRequest = {
    new SyncGroupRequest.Builder(
      new SyncGroupRequestData()
        .setGroupId(group)
        .setGenerationId(1)
        .setMemberId(JoinGroupRequest.UNKNOWN_MEMBER_ID)
        .setProtocolType(protocolType)
        .setProtocolName(protocolName)
        .setAssignments(Collections.emptyList())
    ).build()
  }

  private def createDescribeGroupsRequest = {
    new DescribeGroupsRequest.Builder(new DescribeGroupsRequestData().setGroups(List(group).asJava)).build()
  }

  private def createOffsetCommitRequest = {
    new requests.OffsetCommitRequest.Builder(
        new OffsetCommitRequestData()
          .setGroupId(group)
          .setMemberId(JoinGroupRequest.UNKNOWN_MEMBER_ID)
          .setGenerationId(1)
          .setTopics(Collections.singletonList(
            new OffsetCommitRequestData.OffsetCommitRequestTopic()
              .setName(topic)
              .setPartitions(Collections.singletonList(
                new OffsetCommitRequestData.OffsetCommitRequestPartition()
                  .setPartitionIndex(part)
                  .setCommittedOffset(0)
                  .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                  .setCommitTimestamp(OffsetCommitRequest.DEFAULT_TIMESTAMP)
                  .setCommittedMetadata("metadata")
              )))
          )
    ).build()
  }

  private def createPartitionsRequest = {
    val partitionTopic = new CreatePartitionsTopic()
      .setName(topic)
      .setCount(10)
      .setAssignments(null)
    val data = new CreatePartitionsRequestData()
      .setTimeoutMs(10000)
      .setValidateOnly(true)
    data.topics().add(partitionTopic)
    new CreatePartitionsRequest.Builder(data).build(0.toShort)
  }

  private def heartbeatRequest = new HeartbeatRequest.Builder(
    new HeartbeatRequestData()
      .setGroupId(group)
      .setGenerationId(1)
      .setMemberId(JoinGroupRequest.UNKNOWN_MEMBER_ID)).build()

  private def leaveGroupRequest = new LeaveGroupRequest.Builder(
    group, Collections.singletonList(
      new MemberIdentity()
        .setMemberId(JoinGroupRequest.UNKNOWN_MEMBER_ID)
    )).build()

  private def deleteGroupsRequest = new DeleteGroupsRequest.Builder(
    new DeleteGroupsRequestData()
      .setGroupsNames(Collections.singletonList(group))
  ).build()

  private def leaderAndIsrRequest: LeaderAndIsrRequest = {
    new requests.LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, brokerId, Int.MaxValue, Long.MaxValue,
      Seq(new LeaderAndIsrPartitionState()
        .setTopicName(tp.topic)
        .setPartitionIndex(tp.partition)
        .setControllerEpoch(Int.MaxValue)
        .setLeader(brokerId)
        .setLeaderEpoch(Int.MaxValue)
        .setIsr(List(brokerId).asJava)
        .setZkVersion(2)
        .setReplicas(Seq(brokerId).asJava)
        .setIsNew(false)).asJava,
      topicIds,
      Set(new Node(brokerId, "localhost", 0)).asJava).build()
  }

  private def stopReplicaRequest: StopReplicaRequest = {
    val topicStates = Seq(
      new StopReplicaTopicState()
        .setTopicName(tp.topic())
        .setPartitionStates(Seq(new StopReplicaPartitionState()
          .setPartitionIndex(tp.partition())
          .setLeaderEpoch(LeaderAndIsr.initialLeaderEpoch + 2)
          .setDeletePartition(true)).asJava)
    ).asJava
    new StopReplicaRequest.Builder(ApiKeys.STOP_REPLICA.latestVersion, brokerId, Int.MaxValue,
      Long.MaxValue, false, topicStates).build()
  }

  private def controlledShutdownRequest: ControlledShutdownRequest = {
    new ControlledShutdownRequest.Builder(
      new ControlledShutdownRequestData()
        .setBrokerId(brokerId)
        .setBrokerEpoch(Long.MaxValue),
      ApiKeys.CONTROLLED_SHUTDOWN.latestVersion).build()
  }

  private def createTopicsRequest: CreateTopicsRequest = {
    new CreateTopicsRequest.Builder(new CreateTopicsRequestData().setTopics(
      new CreatableTopicCollection(Collections.singleton(new CreatableTopic().
        setName(topic).setNumPartitions(1).
        setReplicationFactor(1.toShort)).iterator))).build()
  }

  private def deleteTopicsRequest: DeleteTopicsRequest = {
    new DeleteTopicsRequest.Builder(
      new DeleteTopicsRequestData()
        .setTopicNames(Collections.singletonList(topic))
        .setTimeoutMs(5000)).build()
  }

  private def deleteRecordsRequest = new DeleteRecordsRequest.Builder(
    new DeleteRecordsRequestData()
      .setTimeoutMs(5000)
      .setTopics(Collections.singletonList(new DeleteRecordsRequestData.DeleteRecordsTopic()
        .setName(tp.topic)
        .setPartitions(Collections.singletonList(new DeleteRecordsRequestData.DeleteRecordsPartition()
          .setPartitionIndex(tp.partition)
          .setOffset(0L)))))).build()

  private def describeConfigsRequest =
    new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData().setResources(Collections.singletonList(
      new DescribeConfigsRequestData.DescribeConfigsResource().setResourceType(ConfigResource.Type.TOPIC.id)
        .setResourceName(tp.topic)))).build()

  private def alterConfigsRequest =
    new AlterConfigsRequest.Builder(
      Collections.singletonMap(new ConfigResource(ConfigResource.Type.TOPIC, tp.topic),
        new AlterConfigsRequest.Config(Collections.singleton(
          new AlterConfigsRequest.ConfigEntry(LogConfig.MaxMessageBytesProp, "1000000")
        ))), true).build()

  private def incrementalAlterConfigsRequest = {
    val data = new IncrementalAlterConfigsRequestData
    val alterableConfig = new AlterableConfig
    alterableConfig.setName(LogConfig.MaxMessageBytesProp).
      setValue("1000000").setConfigOperation(AlterConfigOp.OpType.SET.id())
    val alterableConfigSet = new AlterableConfigCollection
    alterableConfigSet.add(alterableConfig)
    data.resources().add(new AlterConfigsResource().
      setResourceName(tp.topic).setResourceType(ConfigResource.Type.TOPIC.id()).
      setConfigs(alterableConfigSet))
    new IncrementalAlterConfigsRequest.Builder(data).build()
  }

  private def describeAclsRequest = new DescribeAclsRequest.Builder(AclBindingFilter.ANY).build()

  private def createAclsRequest: CreateAclsRequest = new CreateAclsRequest.Builder(
    new CreateAclsRequestData().setCreations(Collections.singletonList(
      new CreateAclsRequestData.AclCreation()
        .setResourceType(ResourceType.TOPIC.code)
        .setResourceName("mytopic")
        .setResourcePatternType(PatternType.LITERAL.code)
        .setPrincipal(clientPrincipalString)
        .setHost("*")
        .setOperation(AclOperation.WRITE.code)
        .setPermissionType(AclPermissionType.DENY.code)))
  ).build()

  private def deleteAclsRequest: DeleteAclsRequest = new DeleteAclsRequest.Builder(
    new DeleteAclsRequestData().setFilters(Collections.singletonList(
      new DeleteAclsRequestData.DeleteAclsFilter()
        .setResourceTypeFilter(ResourceType.TOPIC.code)
        .setResourceNameFilter(null)
        .setPatternTypeFilter(PatternType.LITERAL.code)
        .setPrincipalFilter(clientPrincipalString)
        .setHostFilter("*")
        .setOperation(AclOperation.ANY.code)
        .setPermissionType(AclPermissionType.DENY.code)))
  ).build()

  private def alterReplicaLogDirsRequest = {
    val dir = new AlterReplicaLogDirsRequestData.AlterReplicaLogDir()
      .setPath(logDir)
    dir.topics.add(new AlterReplicaLogDirsRequestData.AlterReplicaLogDirTopic()
      .setName(tp.topic)
      .setPartitions(Collections.singletonList(tp.partition)))
    val data = new AlterReplicaLogDirsRequestData();
    data.dirs.add(dir)
    new AlterReplicaLogDirsRequest.Builder(data).build()
  }

  private def describeLogDirsRequest = new DescribeLogDirsRequest.Builder(new DescribeLogDirsRequestData().setTopics(new DescribeLogDirsRequestData.DescribableLogDirTopicCollection(Collections.singleton(
    new DescribeLogDirsRequestData.DescribableLogDirTopic().setTopic(tp.topic).setPartitionIndex(Collections.singletonList(tp.partition))).iterator()))).build()

  private def addPartitionsToTxnRequest = new AddPartitionsToTxnRequest.Builder(transactionalId, 1, 1, Collections.singletonList(tp)).build()

  private def addOffsetsToTxnRequest = new AddOffsetsToTxnRequest.Builder(
    new AddOffsetsToTxnRequestData()
      .setTransactionalId(transactionalId)
      .setProducerId(1)
      .setProducerEpoch(1)
      .setGroupId(group)
  ).build()

  private def electLeadersRequest = new ElectLeadersRequest.Builder(
    ElectionType.PREFERRED,
    Collections.singleton(tp),
    10000
  ).build()

  private def alterPartitionReassignmentsRequest = new AlterPartitionReassignmentsRequest.Builder(
    new AlterPartitionReassignmentsRequestData().setTopics(
      List(new AlterPartitionReassignmentsRequestData.ReassignableTopic()
        .setName(topic)
        .setPartitions(
          List(new AlterPartitionReassignmentsRequestData.ReassignablePartition().setPartitionIndex(tp.partition())).asJava
        )).asJava
    )
  ).build()

  private def listPartitionReassignmentsRequest = new ListPartitionReassignmentsRequest.Builder(
    new ListPartitionReassignmentsRequestData().setTopics(
      List(new ListPartitionReassignmentsRequestData.ListPartitionReassignmentsTopics()
        .setName(topic)
        .setPartitionIndexes(
          List(Integer.valueOf(tp.partition)).asJava
        )).asJava
    )
  ).build()

  @Test
  def testAuthorizationWithTopicExisting(): Unit = {
    val requestKeyToRequest = mutable.LinkedHashMap[ApiKeys, AbstractRequest](
      // First create the topic
      ApiKeys.CREATE_TOPICS -> createTopicsRequest,

      ApiKeys.METADATA -> createMetadataRequest(allowAutoTopicCreation = true),
      ApiKeys.PRODUCE -> createProduceRequest,
      ApiKeys.FETCH -> createFetchRequest,
      ApiKeys.LIST_OFFSETS -> createListOffsetsRequest,
      ApiKeys.OFFSET_FETCH -> createOffsetFetchRequest,
      ApiKeys.FIND_COORDINATOR -> createFindCoordinatorRequest,
      ApiKeys.JOIN_GROUP -> createJoinGroupRequest,
      ApiKeys.SYNC_GROUP -> createSyncGroupRequest,
      ApiKeys.DESCRIBE_GROUPS -> createDescribeGroupsRequest,
      ApiKeys.OFFSET_COMMIT -> createOffsetCommitRequest,
      ApiKeys.HEARTBEAT -> heartbeatRequest,
      ApiKeys.LEAVE_GROUP -> leaveGroupRequest,
      ApiKeys.DELETE_RECORDS -> deleteRecordsRequest,
      ApiKeys.OFFSET_FOR_LEADER_EPOCH -> offsetsForLeaderEpochRequest,
      ApiKeys.DESCRIBE_CONFIGS -> describeConfigsRequest,
      ApiKeys.ALTER_CONFIGS -> alterConfigsRequest,
      ApiKeys.CREATE_ACLS -> createAclsRequest,
      ApiKeys.DELETE_ACLS -> deleteAclsRequest,
      ApiKeys.DESCRIBE_ACLS -> describeAclsRequest,
      ApiKeys.ALTER_REPLICA_LOG_DIRS -> alterReplicaLogDirsRequest,
      ApiKeys.DESCRIBE_LOG_DIRS -> describeLogDirsRequest,
      ApiKeys.CREATE_PARTITIONS -> createPartitionsRequest,
      ApiKeys.ADD_PARTITIONS_TO_TXN -> addPartitionsToTxnRequest,
      ApiKeys.ADD_OFFSETS_TO_TXN -> addOffsetsToTxnRequest,
      ApiKeys.ELECT_LEADERS -> electLeadersRequest,
      ApiKeys.INCREMENTAL_ALTER_CONFIGS -> incrementalAlterConfigsRequest,
      ApiKeys.ALTER_PARTITION_REASSIGNMENTS -> alterPartitionReassignmentsRequest,
      ApiKeys.LIST_PARTITION_REASSIGNMENTS -> listPartitionReassignmentsRequest,

      // Inter-broker APIs use an invalid broker epoch, so does not affect the test case
      ApiKeys.UPDATE_METADATA -> createUpdateMetadataRequest,
      ApiKeys.LEADER_AND_ISR -> leaderAndIsrRequest,
      ApiKeys.STOP_REPLICA -> stopReplicaRequest,
      ApiKeys.CONTROLLED_SHUTDOWN -> controlledShutdownRequest,

      // Delete the topic last
      ApiKeys.DELETE_TOPICS -> deleteTopicsRequest
    )

    for ((key, request) <- requestKeyToRequest) {
      removeAllClientAcls()
      val resources = requestKeysToAcls(key).map(_._1.resourceType).toSet
      sendRequestAndVerifyResponseError(request, resources, isAuthorized = false)

      val resourceToAcls = requestKeysToAcls(key)
      resourceToAcls.get(topicResource).foreach { acls =>
        val describeAcls = topicDescribeAcl(topicResource)
        val isAuthorized = describeAcls == acls
        addAndVerifyAcls(describeAcls, topicResource)
        sendRequestAndVerifyResponseError(request, resources, isAuthorized = isAuthorized)
        removeAllClientAcls()
      }

      for ((resource, acls) <- resourceToAcls)
        addAndVerifyAcls(acls, resource)
      sendRequestAndVerifyResponseError(request, resources, isAuthorized = true)
    }
  }

  /*
   * even if the topic doesn't exist, request APIs should not leak the topic name
   */
  @Test
  def testAuthorizationWithTopicNotExisting(): Unit = {
    val requestKeyToRequest = mutable.LinkedHashMap[ApiKeys, AbstractRequest](
      ApiKeys.METADATA -> createMetadataRequest(allowAutoTopicCreation = false),
      ApiKeys.PRODUCE -> createProduceRequest,
      ApiKeys.FETCH -> createFetchRequest,
      ApiKeys.LIST_OFFSETS -> createListOffsetsRequest,
      ApiKeys.OFFSET_COMMIT -> createOffsetCommitRequest,
      ApiKeys.OFFSET_FETCH -> createOffsetFetchRequest,
      ApiKeys.DELETE_TOPICS -> deleteTopicsRequest,
      ApiKeys.DELETE_RECORDS -> deleteRecordsRequest,
      ApiKeys.ADD_PARTITIONS_TO_TXN -> addPartitionsToTxnRequest,
      ApiKeys.ADD_OFFSETS_TO_TXN -> addOffsetsToTxnRequest,
      ApiKeys.CREATE_PARTITIONS -> createPartitionsRequest,
      ApiKeys.DELETE_GROUPS -> deleteGroupsRequest,
      ApiKeys.OFFSET_FOR_LEADER_EPOCH -> offsetsForLeaderEpochRequest,
      ApiKeys.ELECT_LEADERS -> electLeadersRequest
    )

    for ((key, request) <- requestKeyToRequest) {
      removeAllClientAcls()
      val resources = requestKeysToAcls(key).map(_._1.resourceType).toSet
      sendRequestAndVerifyResponseError(request, resources, isAuthorized = false, topicExists = false)

      val resourceToAcls = requestKeysToAcls(key)
      resourceToAcls.get(topicResource).foreach { acls =>
        val describeAcls = topicDescribeAcl(topicResource)
        val isAuthorized = describeAcls == acls
        addAndVerifyAcls(describeAcls, topicResource)
        sendRequestAndVerifyResponseError(request, resources, isAuthorized = isAuthorized, topicExists = false)
        removeAllClientAcls()
      }

      for ((resource, acls) <- resourceToAcls)
        addAndVerifyAcls(acls, resource)
      sendRequestAndVerifyResponseError(request, resources, isAuthorized = true, topicExists = false)
    }
  }

  @Test
  def testCreateTopicAuthorizationWithClusterCreate(): Unit = {
    removeAllClientAcls()
    val resources = Set[ResourceType](TOPIC)

    sendRequestAndVerifyResponseError(createTopicsRequest, resources, isAuthorized = false)

    for ((resource, acls) <- clusterCreateAcl)
      addAndVerifyAcls(acls, resource)
    sendRequestAndVerifyResponseError(createTopicsRequest, resources, isAuthorized = true)
  }

  @Test
  def testFetchFollowerRequest(): Unit = {
    createTopic(topic)

    val request = createFetchFollowerRequest

    removeAllClientAcls()
    val resources = Set(topicResource.resourceType, clusterResource.resourceType)
    sendRequestAndVerifyResponseError(request, resources, isAuthorized = false)

    val readAcls = topicReadAcl(topicResource)
    addAndVerifyAcls(readAcls, topicResource)
    sendRequestAndVerifyResponseError(request, resources, isAuthorized = false)

    val clusterAcls = clusterAcl(clusterResource)
    addAndVerifyAcls(clusterAcls, clusterResource)
    sendRequestAndVerifyResponseError(request, resources, isAuthorized = true)
  }

  @Test
  def testIncrementalAlterConfigsRequestRequiresClusterPermissionForBrokerLogger(): Unit = {
    createTopic(topic)

    val data = new IncrementalAlterConfigsRequestData
    val alterableConfig = new AlterableConfig().setName("kafka.controller.KafkaController").
      setValue(LogLevelConfig.DEBUG_LOG_LEVEL).setConfigOperation(AlterConfigOp.OpType.DELETE.id())
    val alterableConfigSet = new AlterableConfigCollection
    alterableConfigSet.add(alterableConfig)
    data.resources().add(new AlterConfigsResource().
      setResourceName(brokerId.toString).setResourceType(ConfigResource.Type.BROKER_LOGGER.id()).
      setConfigs(alterableConfigSet))
    val request = new IncrementalAlterConfigsRequest.Builder(data).build()

    removeAllClientAcls()
    val resources = Set(topicResource.resourceType, clusterResource.resourceType)
    sendRequestAndVerifyResponseError(request, resources, isAuthorized = false)

    val clusterAcls = clusterAlterConfigsAcl(clusterResource)
    addAndVerifyAcls(clusterAcls, clusterResource)
    sendRequestAndVerifyResponseError(request, resources, isAuthorized = true)
  }

  @Test
  def testOffsetsForLeaderEpochClusterPermission(): Unit = {
    createTopic(topic)

    val request = offsetsForLeaderEpochRequest

    removeAllClientAcls()

    val resources = Set(topicResource.resourceType, clusterResource.resourceType)
    sendRequestAndVerifyResponseError(request, resources, isAuthorized = false)

    // Although the OffsetsForLeaderEpoch API now accepts topic describe, we should continue
    // allowing cluster action for backwards compatibility
    val clusterAcls = clusterAcl(clusterResource)
    addAndVerifyAcls(clusterAcls, clusterResource)
    sendRequestAndVerifyResponseError(request, resources, isAuthorized = true)
  }

  @Test
  def testProduceWithNoTopicAccess(): Unit = {
    createTopic(topic)
    val producer = createProducer()
    assertThrows(classOf[TopicAuthorizationException], () => sendRecords(producer, numRecords, tp))
  }

  @Test
  def testProduceWithTopicDescribe(): Unit = {
    createTopic(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DESCRIBE, ALLOW)), topicResource)
    val producer = createProducer()
    assertThrows(classOf[TopicAuthorizationException], () => sendRecords(producer, numRecords, tp))
  }

  @Test
  def testProduceWithTopicRead(): Unit = {
    createTopic(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), topicResource)
    val producer = createProducer()
    assertThrows(classOf[TopicAuthorizationException], () => sendRecords(producer, numRecords, tp))
  }

  @Test
  def testProduceWithTopicWrite(): Unit = {
    createTopic(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, numRecords, tp)
  }

  @Test
  def testCreatePermissionOnTopicToWriteToNonExistentTopic(): Unit = {
    testCreatePermissionNeededToWriteToNonExistentTopic(TOPIC)
  }

  @Test
  def testCreatePermissionOnClusterToWriteToNonExistentTopic(): Unit = {
    testCreatePermissionNeededToWriteToNonExistentTopic(CLUSTER)
  }

  private def testCreatePermissionNeededToWriteToNonExistentTopic(resType: ResourceType): Unit = {
    val newTopicResource = new ResourcePattern(TOPIC, topic, LITERAL)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)), newTopicResource)
    val producer = createProducer()
    val e = assertThrows(classOf[TopicAuthorizationException], () => sendRecords(producer, numRecords, tp))
    assertEquals(Collections.singleton(tp.topic), e.unauthorizedTopics())

    val resource = if (resType == ResourceType.TOPIC) newTopicResource else clusterResource
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, CREATE, ALLOW)), resource)

    sendRecords(producer, numRecords, tp)
  }

  @Test
  def testConsumeUsingAssignWithNoAccess(): Unit = {
    createTopic(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllClientAcls()

    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    assertThrows(classOf[TopicAuthorizationException], () => consumeRecords(consumer))
  }

  @Test
  def testSimpleConsumeWithOffsetLookupAndNoGroupAccess(): Unit = {
    createTopic(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllClientAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), topicResource)

    // note this still depends on group access because we haven't set offsets explicitly, which means
    // they will first be fetched from the consumer coordinator (which requires group access)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    val e = assertThrows(classOf[GroupAuthorizationException], () => consumeRecords(consumer))
    assertEquals(group, e.groupId())
  }

  @Test
  def testSimpleConsumeWithExplicitSeekAndNoGroupAccess(): Unit = {
    createTopic(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllClientAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), topicResource)

    // in this case, we do an explicit seek, so there should be no need to query the coordinator at all
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumer.seekToBeginning(List(tp).asJava)
    consumeRecords(consumer)
  }

  @Test
  def testConsumeWithoutTopicDescribeAccess(): Unit = {
    createTopic(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllClientAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), groupResource)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)

    val e = assertThrows(classOf[TopicAuthorizationException], () => consumeRecords(consumer))
    assertEquals(Collections.singleton(topic), e.unauthorizedTopics())
  }

  @Test
  def testConsumeWithTopicDescribe(): Unit = {
    createTopic(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllClientAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DESCRIBE, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), groupResource)

    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    val e = assertThrows(classOf[TopicAuthorizationException], () => consumeRecords(consumer))
    assertEquals(Collections.singleton(topic), e.unauthorizedTopics())
  }

  @Test
  def testConsumeWithTopicWrite(): Unit = {
    createTopic(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllClientAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), groupResource)

    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    val e = assertThrows(classOf[TopicAuthorizationException], () => consumeRecords(consumer))
    assertEquals(Collections.singleton(topic), e.unauthorizedTopics())
  }

  @Test
  def testConsumeWithTopicAndGroupRead(): Unit = {
    createTopic(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllClientAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), groupResource)

    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumeRecords(consumer)
  }

  @nowarn("cat=deprecation")
  @Test
  def testPatternSubscriptionWithNoTopicAccess(): Unit = {
    createTopic(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllClientAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), groupResource)

    val consumer = createConsumer()
    consumer.subscribe(Pattern.compile(topicPattern), new NoOpConsumerRebalanceListener)
    consumer.poll(0)
    assertTrue(consumer.subscription.isEmpty)
  }

  @Test
  def testPatternSubscriptionWithTopicDescribeOnlyAndGroupRead(): Unit = {
    createTopic(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllClientAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DESCRIBE, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), groupResource)
    val consumer = createConsumer()
    consumer.subscribe(Pattern.compile(topicPattern))
    val e = assertThrows(classOf[TopicAuthorizationException], () => consumeRecords(consumer))
    assertEquals(Collections.singleton(topic), e.unauthorizedTopics())
  }

  @nowarn("cat=deprecation")
  @Test
  def testPatternSubscriptionWithTopicAndGroupRead(): Unit = {
    createTopic(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)

    // create an unmatched topic
    val unmatchedTopic = "unmatched"
    createTopic(unmatchedTopic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)),  new ResourcePattern(TOPIC, unmatchedTopic, LITERAL))
    sendRecords(producer, 1, new TopicPartition(unmatchedTopic, part))
    removeAllClientAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), groupResource)
    val consumer = createConsumer()
    consumer.subscribe(Pattern.compile(topicPattern))
    consumeRecords(consumer)

    // set the subscription pattern to an internal topic that the consumer has read permission to. Since
    // internal topics are not included, we should not be assigned any partitions from this topic
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)),  new ResourcePattern(TOPIC,
      GROUP_METADATA_TOPIC_NAME, LITERAL))
    consumer.subscribe(Pattern.compile(GROUP_METADATA_TOPIC_NAME))
    consumer.poll(0)
    assertTrue(consumer.subscription().isEmpty)
    assertTrue(consumer.assignment().isEmpty)
  }

  @nowarn("cat=deprecation")
  @Test
  def testPatternSubscriptionMatchingInternalTopic(): Unit = {
    createTopic(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllClientAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), groupResource)

    consumerConfig.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false")
    val consumer = createConsumer()
    // ensure that internal topics are not included if no permission
    consumer.subscribe(Pattern.compile(".*"))
    consumeRecords(consumer)
    assertEquals(Set(topic).asJava, consumer.subscription)

    // now authorize the user for the internal topic and verify that we can subscribe
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), new ResourcePattern(TOPIC,
      GROUP_METADATA_TOPIC_NAME, LITERAL))
    consumer.subscribe(Pattern.compile(GROUP_METADATA_TOPIC_NAME))
    consumer.poll(0)
    assertEquals(Set(GROUP_METADATA_TOPIC_NAME), consumer.subscription.asScala)
  }

  @Test
  def testPatternSubscriptionMatchingInternalTopicWithDescribeOnlyPermission(): Unit = {
    createTopic(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllClientAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), groupResource)
    val internalTopicResource = new ResourcePattern(TOPIC, GROUP_METADATA_TOPIC_NAME, LITERAL)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DESCRIBE, ALLOW)), internalTopicResource)

    consumerConfig.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false")
    val consumer = createConsumer()
    consumer.subscribe(Pattern.compile(".*"))
    val e = assertThrows(classOf[TopicAuthorizationException], () => {
        // It is possible that the first call returns records of "topic" and the second call throws TopicAuthorizationException
        consumeRecords(consumer)
        consumeRecords(consumer)
      })
    assertEquals(Collections.singleton(GROUP_METADATA_TOPIC_NAME), e.unauthorizedTopics())
  }

  @Test
  def testPatternSubscriptionNotMatchingInternalTopic(): Unit = {
    createTopic(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllClientAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), groupResource)

    consumerConfig.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false")
    val consumer = createConsumer()
    consumer.subscribe(Pattern.compile(topicPattern))
    consumeRecords(consumer)
  }

  @Test
  def testCreatePermissionOnTopicToReadFromNonExistentTopic(): Unit = {
    testCreatePermissionNeededToReadFromNonExistentTopic("newTopic",
      Set(new AccessControlEntry(clientPrincipalString, WildcardHost, CREATE, ALLOW)),
      TOPIC)
  }

  @Test
  def testCreatePermissionOnClusterToReadFromNonExistentTopic(): Unit = {
    testCreatePermissionNeededToReadFromNonExistentTopic("newTopic",
      Set(new AccessControlEntry(clientPrincipalString, WildcardHost, CREATE, ALLOW)),
      CLUSTER)
  }

  private def testCreatePermissionNeededToReadFromNonExistentTopic(newTopic: String, acls: Set[AccessControlEntry], resType: ResourceType): Unit = {
    val topicPartition = new TopicPartition(newTopic, 0)
    val newTopicResource = new ResourcePattern(TOPIC, newTopic, LITERAL)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), newTopicResource)
    addAndVerifyAcls(groupReadAcl(groupResource), groupResource)
    val consumer = createConsumer()
    consumer.assign(List(topicPartition).asJava)
    val unauthorizedTopics = assertThrows(classOf[TopicAuthorizationException],
      () => (0 until 10).foreach(_ => consumer.poll(Duration.ofMillis(50L)))).unauthorizedTopics
    assertEquals(Collections.singleton(newTopic), unauthorizedTopics)

    val resource = if (resType == TOPIC) newTopicResource else clusterResource
    addAndVerifyAcls(acls, resource)

    TestUtils.waitUntilTrue(() => {
      consumer.poll(Duration.ofMillis(50L))
      this.zkClient.topicExists(newTopic)
    }, "Expected topic was not created")
  }

  @Test
  def testCreatePermissionMetadataRequestAutoCreate(): Unit = {
    val readAcls = topicReadAcl(topicResource)
    addAndVerifyAcls(readAcls, topicResource)
    assertFalse(zkClient.topicExists(topic))

    val metadataRequest = new MetadataRequest.Builder(List(topic).asJava, true).build()
    val metadataResponse = connectAndReceive[MetadataResponse](metadataRequest)

    assertEquals(Set().asJava, metadataResponse.topicsByError(Errors.NONE))

    val createAcls = topicCreateAcl(topicResource)
    addAndVerifyAcls(createAcls, topicResource)

    // retry as topic being created can have MetadataResponse with Errors.LEADER_NOT_AVAILABLE
    TestUtils.retry(JTestUtils.DEFAULT_MAX_WAIT_MS) {
      val metadataResponse = connectAndReceive[MetadataResponse](metadataRequest)
      assertEquals(Set(topic).asJava, metadataResponse.topicsByError(Errors.NONE))
    }
  }

  @Test
  def testCommitWithNoAccess(): Unit = {
    val consumer = createConsumer()
    assertThrows(classOf[GroupAuthorizationException], () => consumer.commitSync(Map(tp -> new OffsetAndMetadata(5)).asJava))
  }

  @Test
  def testCommitWithNoTopicAccess(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), groupResource)
    val consumer = createConsumer()
    assertThrows(classOf[TopicAuthorizationException], () => consumer.commitSync(Map(tp -> new OffsetAndMetadata(5)).asJava))
  }

  @Test
  def testCommitWithTopicWrite(): Unit = {
    createTopic(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)), topicResource)
    val consumer = createConsumer()
    assertThrows(classOf[TopicAuthorizationException], () => consumer.commitSync(Map(tp -> new OffsetAndMetadata(5)).asJava))
  }

  @Test
  def testCommitWithTopicDescribe(): Unit = {
    createTopic(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DESCRIBE, ALLOW)), topicResource)
    val consumer = createConsumer()
    assertThrows(classOf[TopicAuthorizationException], () => consumer.commitSync(Map(tp -> new OffsetAndMetadata(5)).asJava))
  }

  @Test
  def testCommitWithNoGroupAccess(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), topicResource)
    val consumer = createConsumer()
    assertThrows(classOf[GroupAuthorizationException], () => consumer.commitSync(Map(tp -> new OffsetAndMetadata(5)).asJava))
  }

  @Test
  def testCommitWithTopicAndGroupRead(): Unit = {
    createTopic(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.commitSync(Map(tp -> new OffsetAndMetadata(5)).asJava)
  }

  @Test
  def testOffsetFetchWithNoAccess(): Unit = {
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    assertThrows(classOf[TopicAuthorizationException], () => consumer.position(tp))
  }

  @Test
  def testOffsetFetchWithNoGroupAccess(): Unit = {
    createTopic(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    assertThrows(classOf[GroupAuthorizationException], () => consumer.position(tp))
  }

  @Test
  def testOffsetFetchWithNoTopicAccess(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), groupResource)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    assertThrows(classOf[TopicAuthorizationException], () => consumer.position(tp))
  }

  @Test
  def testFetchAllOffsetsTopicAuthorization(): Unit = {
    createTopic(topic)

    val offset = 15L
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumer.commitSync(Map(tp -> new OffsetAndMetadata(offset)).asJava)

    removeAllClientAcls()
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), groupResource)

    // send offset fetch requests directly since the consumer does not expose an API to do so
    // note there's only one broker, so no need to lookup the group coordinator

    // without describe permission on the topic, we shouldn't be able to fetch offsets
    val offsetFetchRequest = new requests.OffsetFetchRequest.Builder(group, false, null, false).build()
    var offsetFetchResponse = connectAndReceive[OffsetFetchResponse](offsetFetchRequest)
    assertEquals(Errors.NONE, offsetFetchResponse.error)
    assertTrue(offsetFetchResponse.responseData.isEmpty)

    // now add describe permission on the topic and verify that the offset can be fetched
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DESCRIBE, ALLOW)), topicResource)
    offsetFetchResponse = connectAndReceive[OffsetFetchResponse](offsetFetchRequest)
    assertEquals(Errors.NONE, offsetFetchResponse.error)
    assertTrue(offsetFetchResponse.responseData.containsKey(tp))
    assertEquals(offset, offsetFetchResponse.responseData.get(tp).offset)
  }

  @Test
  def testOffsetFetchTopicDescribe(): Unit = {
    createTopic(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DESCRIBE, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DESCRIBE, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumer.position(tp)
  }

  @Test
  def testOffsetFetchWithTopicAndGroupRead(): Unit = {
    createTopic(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumer.position(tp)
  }

  @Test
  def testMetadataWithNoTopicAccess(): Unit = {
    val consumer = createConsumer()
    assertThrows(classOf[TopicAuthorizationException], () => consumer.partitionsFor(topic))
  }

  @Test
  def testMetadataWithTopicDescribe(): Unit = {
    createTopic(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DESCRIBE, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.partitionsFor(topic)
  }

  @Test
  def testListOffsetsWithNoTopicAccess(): Unit = {
    val consumer = createConsumer()
    assertThrows(classOf[TopicAuthorizationException], () => consumer.endOffsets(Set(tp).asJava))
  }

  @Test
  def testListOffsetsWithTopicDescribe(): Unit = {
    createTopic(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DESCRIBE, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.endOffsets(Set(tp).asJava)
  }

  @Test
  def testDescribeGroupApiWithNoGroupAcl(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DESCRIBE, ALLOW)), topicResource)
    val result = createAdminClient().describeConsumerGroups(Seq(group).asJava)
    TestUtils.assertFutureExceptionTypeEquals(result.describedGroups().get(group), classOf[GroupAuthorizationException])
  }

  @Test
  def testDescribeGroupApiWithGroupDescribe(): Unit = {
    createTopic(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DESCRIBE, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DESCRIBE, ALLOW)), topicResource)
    createAdminClient().describeConsumerGroups(Seq(group).asJava).describedGroups().get(group).get()
  }

  @Test
  def testDescribeGroupCliWithGroupDescribe(): Unit = {
    createTopic(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DESCRIBE, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DESCRIBE, ALLOW)), topicResource)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    val consumerGroupService = new ConsumerGroupService(opts)
    consumerGroupService.describeGroups()
    consumerGroupService.close()
  }

  @Test
  def testListGroupApiWithAndWithoutListGroupAcls(): Unit = {
    createTopic(topic)

    // write some record to the topic
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, numRecords = 1, tp)

    // use two consumers to write to two different groups
    val group2 = "other group"
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), new ResourcePattern(GROUP, group2, LITERAL))
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.subscribe(Collections.singleton(topic))
    consumeRecords(consumer)

    val otherConsumerProps = new Properties
    otherConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, group2)
    val otherConsumer = createConsumer(configOverrides = otherConsumerProps)
    otherConsumer.subscribe(Collections.singleton(topic))
    consumeRecords(otherConsumer)

    val adminClient = createAdminClient()

    // first use cluster describe permission
    removeAllClientAcls()
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DESCRIBE, ALLOW)), clusterResource)
    // it should list both groups (due to cluster describe permission)
    assertEquals(Set(group, group2), adminClient.listConsumerGroups().all().get().asScala.map(_.groupId()).toSet)

    // now replace cluster describe with group read permission
    removeAllClientAcls()
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), groupResource)
    // it should list only one group now
    val groupList = adminClient.listConsumerGroups().all().get().asScala.toList
    assertEquals(1, groupList.length)
    assertEquals(group, groupList.head.groupId)

    // now remove all acls and verify describe group access is required to list any group
    removeAllClientAcls()
    val listGroupResult = adminClient.listConsumerGroups()
    assertEquals(List(), listGroupResult.errors().get().asScala.toList)
    assertEquals(List(), listGroupResult.all().get().asScala.toList)
    otherConsumer.close()
  }

  @Test
  def testDeleteGroupApiWithDeleteGroupAcl(): Unit = {
    createTopic(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DELETE, ALLOW)), groupResource)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumer.commitSync(Map(tp -> new OffsetAndMetadata(5, "")).asJava)
    createAdminClient().deleteConsumerGroups(Seq(group).asJava).deletedGroups().get(group).get()
  }

  @Test
  def testDeleteGroupApiWithNoDeleteGroupAcl(): Unit = {
    createTopic(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumer.commitSync(Map(tp -> new OffsetAndMetadata(5, "")).asJava)
    val result = createAdminClient().deleteConsumerGroups(Seq(group).asJava)
    TestUtils.assertFutureExceptionTypeEquals(result.deletedGroups().get(group), classOf[GroupAuthorizationException])
  }

  @Test
  def testDeleteGroupApiWithNoDeleteGroupAcl2(): Unit = {
    val result = createAdminClient().deleteConsumerGroups(Seq(group).asJava)
    TestUtils.assertFutureExceptionTypeEquals(result.deletedGroups().get(group), classOf[GroupAuthorizationException])
  }

  @Test
  def testDeleteGroupOffsetsWithAcl(): Unit = {
    createTopic(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DELETE, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumer.commitSync(Map(tp -> new OffsetAndMetadata(5, "")).asJava)
    consumer.close()
    val result = createAdminClient().deleteConsumerGroupOffsets(group, Set(tp).asJava)
    assertNull(result.partitionResult(tp).get())
  }

  @Test
  def testDeleteGroupOffsetsWithoutDeleteAcl(): Unit = {
    createTopic(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumer.commitSync(Map(tp -> new OffsetAndMetadata(5, "")).asJava)
    consumer.close()
    val result = createAdminClient().deleteConsumerGroupOffsets(group, Set(tp).asJava)
    TestUtils.assertFutureExceptionTypeEquals(result.all(), classOf[GroupAuthorizationException])
  }

  @Test
  def testDeleteGroupOffsetsWithDeleteAclWithoutTopicAcl(): Unit = {
    createTopic(topic)
    // Create the consumer group
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumer.commitSync(Map(tp -> new OffsetAndMetadata(5, "")).asJava)
    consumer.close()

    // Remove the topic ACL & Check that it does not work without it
    removeAllClientAcls()
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DELETE, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), groupResource)
    val result = createAdminClient().deleteConsumerGroupOffsets(group, Set(tp).asJava)
    TestUtils.assertFutureExceptionTypeEquals(result.all(), classOf[TopicAuthorizationException])
    TestUtils.assertFutureExceptionTypeEquals(result.partitionResult(tp), classOf[TopicAuthorizationException])
  }

  @Test
  def testDeleteGroupOffsetsWithNoAcl(): Unit = {
    val result = createAdminClient().deleteConsumerGroupOffsets(group, Set(tp).asJava)
    TestUtils.assertFutureExceptionTypeEquals(result.all(), classOf[GroupAuthorizationException])
  }

  @Test
  def testUnauthorizedDeleteTopicsWithoutDescribe(): Unit = {
    val deleteResponse = connectAndReceive[DeleteTopicsResponse](deleteTopicsRequest)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, deleteResponse.data.responses.find(topic).errorCode)
  }

  @Test
  def testUnauthorizedDeleteTopicsWithDescribe(): Unit = {
    createTopic(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DESCRIBE, ALLOW)), topicResource)
    val deleteResponse = connectAndReceive[DeleteTopicsResponse](deleteTopicsRequest)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, deleteResponse.data.responses.find(topic).errorCode)
  }

  @Test
  def testDeleteTopicsWithWildCardAuth(): Unit = {
    createTopic(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DELETE, ALLOW)), new ResourcePattern(TOPIC, "*", LITERAL))
    val deleteResponse = connectAndReceive[DeleteTopicsResponse](deleteTopicsRequest)
    assertEquals(Errors.NONE.code, deleteResponse.data.responses.find(topic).errorCode)
  }

  @Test
  def testUnauthorizedDeleteRecordsWithoutDescribe(): Unit = {
    val deleteRecordsResponse = connectAndReceive[DeleteRecordsResponse](deleteRecordsRequest)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, deleteRecordsResponse.data.topics.asScala.head.
      partitions.asScala.head.errorCode)
  }

  @Test
  def testUnauthorizedDeleteRecordsWithDescribe(): Unit = {
    createTopic(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DESCRIBE, ALLOW)), topicResource)
    val deleteRecordsResponse = connectAndReceive[DeleteRecordsResponse](deleteRecordsRequest)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, deleteRecordsResponse.data.topics.asScala.head.
      partitions.asScala.head.errorCode)
  }

  @Test
  def testDeleteRecordsWithWildCardAuth(): Unit = {
    createTopic(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DELETE, ALLOW)), new ResourcePattern(TOPIC, "*", LITERAL))
    val deleteRecordsResponse = connectAndReceive[DeleteRecordsResponse](deleteRecordsRequest)
    assertEquals(Errors.NONE.code, deleteRecordsResponse.data.topics.asScala.head.
      partitions.asScala.head.errorCode)
  }

  @Test
  def testUnauthorizedCreatePartitions(): Unit = {
    val createPartitionsResponse = connectAndReceive[CreatePartitionsResponse](createPartitionsRequest)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code(), createPartitionsResponse.data.results.asScala.head.errorCode())
  }

  @Test
  def testCreatePartitionsWithWildCardAuth(): Unit = {
    createTopic(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, ALTER, ALLOW)), new ResourcePattern(TOPIC, "*", LITERAL))
    val createPartitionsResponse = connectAndReceive[CreatePartitionsResponse](createPartitionsRequest)
    assertEquals(Errors.NONE.code(), createPartitionsResponse.data.results.asScala.head.errorCode())
  }

  @Test
  def testTransactionalProducerInitTransactionsNoWriteTransactionalIdAcl(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DESCRIBE, ALLOW)), transactionalIdResource)
    val producer = buildTransactionalProducer()
    assertThrows(classOf[TransactionalIdAuthorizationException], () => producer.initTransactions())
  }

  @Test
  def testTransactionalProducerInitTransactionsNoDescribeTransactionalIdAcl(): Unit = {
    val producer = buildTransactionalProducer()
    assertThrows(classOf[TransactionalIdAuthorizationException], () => producer.initTransactions())
  }

  @Test
  def testSendOffsetsWithNoConsumerGroupDescribeAccess(): Unit = {
    createTopic(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, CLUSTER_ACTION, ALLOW)), clusterResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)), transactionalIdResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    producer.beginTransaction()

    assertThrows(classOf[GroupAuthorizationException],
      () => producer.sendOffsetsToTransaction(Map(tp -> new OffsetAndMetadata(0L)).asJava, group))
  }

  @Test
  def testSendOffsetsWithNoConsumerGroupWriteAccess(): Unit = {
    createTopic(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)), transactionalIdResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DESCRIBE, ALLOW)), groupResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    producer.beginTransaction()

    assertThrows(classOf[GroupAuthorizationException],
      () => producer.sendOffsetsToTransaction(Map(tp -> new OffsetAndMetadata(0L)).asJava, group))
  }

  @Test
  def testIdempotentProducerNoIdempotentWriteAclInInitProducerId(): Unit = {
    createTopic(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, READ, ALLOW)), topicResource)
    shouldIdempotentProducerFailInInitProducerId(true)
  }

  def shouldIdempotentProducerFailInInitProducerId(expectAuthException: Boolean): Unit = {
    val producer = buildIdempotentProducer()
    try {
      // the InitProducerId is sent asynchronously, so we expect the error either in the callback
      // or raised from send itself
      producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, "hi".getBytes)).get()
      if (expectAuthException)
        fail("Should have raised ClusterAuthorizationException")
    } catch {
      case e: ExecutionException =>
        assertTrue(e.getCause.isInstanceOf[ClusterAuthorizationException])
    }
    try {
      // the second time, the call to send itself should fail (the producer becomes unusable
      // if no producerId can be obtained)
      producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, "hi".getBytes)).get()
      if (expectAuthException)
        fail("Should have raised ClusterAuthorizationException")
    } catch {
      case e: ExecutionException =>
        assertTrue(e.getCause.isInstanceOf[ClusterAuthorizationException])
    }
  }

  @Test
  def testIdempotentProducerNoIdempotentWriteAclInProduce(): Unit = {
    createTopic(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, IDEMPOTENT_WRITE, ALLOW)), clusterResource)
    idempotentProducerShouldFailInProduce(() => removeAllClientAcls())
  }

  def idempotentProducerShouldFailInProduce(removeAclIdempotenceRequired: () => Unit): Unit = {
    val producer = buildIdempotentProducer()

    // first send should be fine since we have permission to get a ProducerId
    producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, "hi".getBytes)).get()

    // revoke the IdempotentWrite permission
    removeAclIdempotenceRequired()
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DESCRIBE, ALLOW)), topicResource)

    // the send should now fail with a cluster auth error
    var e = assertThrows(classOf[ExecutionException], () => producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, "hi".getBytes)).get())
    assertTrue(e.getCause.isInstanceOf[TopicAuthorizationException])

    // the second time, the call to send itself should fail (the producer becomes unusable
    // if no producerId can be obtained)
    e = assertThrows(classOf[ExecutionException], () => producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, "hi".getBytes)).get())
    assertTrue(e.getCause.isInstanceOf[TopicAuthorizationException])
  }

  @Test
  def shouldInitTransactionsWhenAclSet(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)), transactionalIdResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
  }

  @Test
  def testTransactionalProducerTopicAuthorizationExceptionInSendCallback(): Unit = {
    createTopic(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)), transactionalIdResource)
    // add describe access so that we can fetch metadata
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DESCRIBE, ALLOW)), topicResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    producer.beginTransaction()

    val future = producer.send(new ProducerRecord(tp.topic, tp.partition, "1".getBytes, "1".getBytes))
    val e = JTestUtils.assertFutureThrows(future, classOf[TopicAuthorizationException])
    assertEquals(Set(topic), e.unauthorizedTopics.asScala)
  }

  @Test
  def testTransactionalProducerTopicAuthorizationExceptionInCommit(): Unit = {
    createTopic(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)), transactionalIdResource)
    // add describe access so that we can fetch metadata
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DESCRIBE, ALLOW)), topicResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    producer.beginTransaction()

    assertThrows(classOf[TopicAuthorizationException], () => {
      producer.send(new ProducerRecord(tp.topic, tp.partition, "1".getBytes, "1".getBytes))
      producer.commitTransaction()
    })
  }

  @Test
  def shouldThrowTransactionalIdAuthorizationExceptionWhenNoTransactionAccessDuringSend(): Unit = {
    createTopic(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)), transactionalIdResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    removeAllClientAcls()
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)), topicResource)
    producer.beginTransaction()
    val future = producer.send(new ProducerRecord(tp.topic, tp.partition, "1".getBytes, "1".getBytes))
    JTestUtils.assertFutureThrows(future, classOf[TransactionalIdAuthorizationException])
  }

  @Test
  def shouldThrowTransactionalIdAuthorizationExceptionWhenNoTransactionAccessOnEndTransaction(): Unit = {
    createTopic(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)), transactionalIdResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)), topicResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    producer.beginTransaction()
    producer.send(new ProducerRecord(tp.topic, tp.partition, "1".getBytes, "1".getBytes)).get
    removeAllClientAcls()
    assertThrows(classOf[TransactionalIdAuthorizationException], () => producer.commitTransaction())
  }

  @Test
  def shouldSuccessfullyAbortTransactionAfterTopicAuthorizationException(): Unit = {
    createTopic(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)), transactionalIdResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DESCRIBE, ALLOW)),
      new ResourcePattern(TOPIC, topic, LITERAL))
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    producer.beginTransaction()
    producer.send(new ProducerRecord(tp.topic, tp.partition, "1".getBytes, "1".getBytes)).get
    // try and add a partition resulting in TopicAuthorizationException
    val future = producer.send(new ProducerRecord("otherTopic", 0, "1".getBytes, "1".getBytes))
    val e = JTestUtils.assertFutureThrows(future, classOf[TopicAuthorizationException])
    assertEquals(Set("otherTopic"), e.unauthorizedTopics.asScala)
    // now rollback
    producer.abortTransaction()
  }

  @Test
  def shouldThrowTransactionalIdAuthorizationExceptionWhenNoTransactionAccessOnSendOffsetsToTxn(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)), transactionalIdResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)), groupResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    producer.beginTransaction()
    removeAllClientAcls()
    assertThrows(classOf[TransactionalIdAuthorizationException], () => {
      val offsets = Map(tp -> new OffsetAndMetadata(1L)).asJava
      producer.sendOffsetsToTransaction(offsets, group)
      producer.commitTransaction()
    })
  }

  @Test
  def shouldSendSuccessfullyWhenIdempotentAndHasCorrectACL(): Unit = {
    createTopic(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, IDEMPOTENT_WRITE, ALLOW)), clusterResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)), topicResource)
    val producer = buildIdempotentProducer()
    producer.send(new ProducerRecord(tp.topic, tp.partition, "1".getBytes, "1".getBytes)).get
  }

  // Verify that metadata request without topics works without any ACLs and returns cluster id
  @Test
  def testClusterId(): Unit = {
    val request = new requests.MetadataRequest.Builder(List.empty.asJava, false).build()
    val response = connectAndReceive[MetadataResponse](request)
    assertEquals(Collections.emptyMap, response.errorCounts)
    assertFalse(response.clusterId.isEmpty, "Cluster id not returned")
  }

  @Test
  def testAuthorizeByResourceTypeMultipleAddAndRemove(): Unit = {
    createTopic(topic)

    for (_ <- 1 to 3) {
      addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DESCRIBE, ALLOW)), topicResource)
      shouldIdempotentProducerFailInInitProducerId(true)

      addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)), topicResource)
      shouldIdempotentProducerFailInInitProducerId(false)

      removeAllClientAcls()
      addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WildcardHost, DESCRIBE, ALLOW)), topicResource)
      shouldIdempotentProducerFailInInitProducerId(true)
    }
  }

  @Test
  def testAuthorizeByResourceTypeIsolationUnrelatedDenyWontDominateAllow(): Unit = {
    createTopic(topic)
    createTopic("topic-2")
    createTopic("to")

    val unrelatedPrincipalString = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "unrelated").toString
    val unrelatedTopicResource = new ResourcePattern(TOPIC, "topic-2", LITERAL)
    val unrelatedGroupResource = new ResourcePattern(GROUP, "to", PREFIXED)

    val acl1 = new AccessControlEntry(clientPrincipalString, WildcardHost, READ, DENY)
    val acl2 = new AccessControlEntry(unrelatedPrincipalString, WildcardHost, READ, DENY)
    val acl3 = new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, DENY)
    val acl4 = new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)
    val acl5 = new AccessControlEntry(clientPrincipalString, WildcardHost, DESCRIBE, ALLOW)

    addAndVerifyAcls(Set(acl1, acl4, acl5), topicResource)
    addAndVerifyAcls(Set(acl2, acl3), unrelatedTopicResource)
    addAndVerifyAcls(Set(acl2, acl3), unrelatedGroupResource)
    shouldIdempotentProducerFailInInitProducerId(false)
  }

  @Test
  def testAuthorizeByResourceTypeDenyTakesPrecedence(): Unit = {
    createTopic(topic)
    val allowWriteAce = new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)
    addAndVerifyAcls(Set(allowWriteAce), topicResource)
    shouldIdempotentProducerFailInInitProducerId(false)

    val denyWriteAce = new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, DENY)
    addAndVerifyAcls(Set(denyWriteAce), topicResource)
    shouldIdempotentProducerFailInInitProducerId(true)
  }

  @Test
  def testAuthorizeByResourceTypeWildcardResourceDenyDominate(): Unit = {
    createTopic(topic)
    val wildcard = new ResourcePattern(TOPIC, ResourcePattern.WILDCARD_RESOURCE, LITERAL)
    val prefixed = new ResourcePattern(TOPIC, "t", PREFIXED)
    val literal = new ResourcePattern(TOPIC, topic, LITERAL)
    val allowWriteAce = new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)
    val denyWriteAce = new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, DENY)

    addAndVerifyAcls(Set(allowWriteAce), prefixed)
    addAndVerifyAcls(Set(allowWriteAce), literal)
    shouldIdempotentProducerFailInInitProducerId(false)

    addAndVerifyAcls(Set(denyWriteAce), wildcard)
    shouldIdempotentProducerFailInInitProducerId(true)
  }

  @Test
  def testAuthorizeByResourceTypePrefixedResourceDenyDominate(): Unit = {
    createTopic(topic)
    val prefixed = new ResourcePattern(TOPIC, topic.substring(0, 1), PREFIXED)
    val literal = new ResourcePattern(TOPIC, topic, LITERAL)
    val allowWriteAce = new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, ALLOW)
    val denyWriteAce = new AccessControlEntry(clientPrincipalString, WildcardHost, WRITE, DENY)

    addAndVerifyAcls(Set(denyWriteAce), prefixed)
    addAndVerifyAcls(Set(allowWriteAce), literal)
    shouldIdempotentProducerFailInInitProducerId(true)
  }

  @Test
  def testMetadataClusterAuthorizedOperationsWithoutDescribeCluster(): Unit = {
    removeAllClientAcls()

    // MetadataRequest versions older than 1 are not supported.
    for (version <- 1 to ApiKeys.METADATA.latestVersion) {
      testMetadataClusterClusterAuthorizedOperations(version.toShort, 0)
    }
  }

  @Test
  def testMetadataClusterAuthorizedOperationsWithDescribeAndAlterCluster(): Unit = {
    removeAllClientAcls()

    val clusterResource = new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL)
    val acls = Set(
      new AccessControlEntry(clientPrincipalString, WildcardHost, DESCRIBE, ALLOW),
      new AccessControlEntry(clientPrincipalString, WildcardHost, ALTER, ALLOW)
    )
    addAndVerifyAcls(acls, clusterResource)

    val expectedClusterAuthorizedOperations = Utils.to32BitField(
      acls.map(_.operation.code.asInstanceOf[JByte]).asJava)

    // MetadataRequest versions older than 1 are not supported.
    for (version <- 1 to ApiKeys.METADATA.latestVersion) {
      testMetadataClusterClusterAuthorizedOperations(version.toShort, expectedClusterAuthorizedOperations)
    }
  }

  private def testMetadataClusterClusterAuthorizedOperations(
    version: Short,
    expectedClusterAuthorizedOperations: Int
  ): Unit = {
    val metadataRequest = new MetadataRequest.Builder(new MetadataRequestData()
      .setTopics(Collections.emptyList())
      .setAllowAutoTopicCreation(true)
      .setIncludeClusterAuthorizedOperations(true))
      .build(version)

    // The expected value is only verified if the request supports it.
    if (version >= 8 && version <= 10) {
      val metadataResponse = connectAndReceive[MetadataResponse](metadataRequest)
      assertEquals(expectedClusterAuthorizedOperations, metadataResponse.data.clusterAuthorizedOperations)
    } else {
      assertThrows(classOf[UnsupportedVersionException],
        () => connectAndReceive[MetadataResponse](metadataRequest))
    }
  }

  @Test
  def testDescribeClusterClusterAuthorizedOperationsWithoutDescribeCluster(): Unit = {
    removeAllClientAcls()

    for (version <- ApiKeys.DESCRIBE_CLUSTER.oldestVersion to ApiKeys.DESCRIBE_CLUSTER.latestVersion) {
      testDescribeClusterClusterAuthorizedOperations(version.toShort, 0)
    }
  }

  @Test
  def testDescribeClusterClusterAuthorizedOperationsWithDescribeAndAlterCluster(): Unit = {
    removeAllClientAcls()

    val clusterResource = new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL)
    val acls = Set(
      new AccessControlEntry(clientPrincipalString, WildcardHost, DESCRIBE, ALLOW),
      new AccessControlEntry(clientPrincipalString, WildcardHost, ALTER, ALLOW)
    )
    addAndVerifyAcls(acls, clusterResource)

    val expectedClusterAuthorizedOperations = Utils.to32BitField(
      acls.map(_.operation.code.asInstanceOf[JByte]).asJava)

    for (version <- ApiKeys.DESCRIBE_CLUSTER.oldestVersion to ApiKeys.DESCRIBE_CLUSTER.latestVersion) {
      testDescribeClusterClusterAuthorizedOperations(version.toShort, expectedClusterAuthorizedOperations)
    }
  }

  private def testDescribeClusterClusterAuthorizedOperations(
    version: Short,
    expectedClusterAuthorizedOperations: Int
  ): Unit = {
    val describeClusterRequest = new DescribeClusterRequest.Builder(new DescribeClusterRequestData()
      .setIncludeClusterAuthorizedOperations(true))
      .build(version)

    val describeClusterResponse = connectAndReceive[DescribeClusterResponse](describeClusterRequest)
    assertEquals(expectedClusterAuthorizedOperations, describeClusterResponse.data.clusterAuthorizedOperations)
  }

  def removeAllClientAcls(): Unit = {
    val authorizer = servers.head.dataPlaneRequestProcessor.authorizer.get
    val aclEntryFilter = new AccessControlEntryFilter(clientPrincipalString, null, AclOperation.ANY, AclPermissionType.ANY)
    val aclFilter = new AclBindingFilter(ResourcePatternFilter.ANY, aclEntryFilter)

    authorizer.deleteAcls(null, List(aclFilter).asJava).asScala.map(_.toCompletableFuture.get).flatMap { deletion =>
      deletion.aclBindingDeleteResults().asScala.map(_.aclBinding.pattern).toSet
    }.foreach { resource =>
      TestUtils.waitAndVerifyAcls(Set.empty[AccessControlEntry], authorizer, resource, aclEntryFilter)
    }
  }

  private def sendRequestAndVerifyResponseError(request: AbstractRequest,
                                                resources: Set[ResourceType],
                                                isAuthorized: Boolean,
                                                topicExists: Boolean = true): AbstractResponse = {
    val apiKey = request.apiKey
    val response = connectAndReceive[AbstractResponse](request)
    val error = requestKeyToError(apiKey).asInstanceOf[AbstractResponse => Errors](response)

    val authorizationErrors = resources.flatMap { resourceType =>
      if (resourceType == TOPIC) {
        if (isAuthorized)
          Set(Errors.UNKNOWN_TOPIC_OR_PARTITION, AclEntry.authorizationError(ResourceType.TOPIC))
        else
          Set(AclEntry.authorizationError(ResourceType.TOPIC))
      } else {
        Set(AclEntry.authorizationError(resourceType))
      }
    }

    if (topicExists)
      if (isAuthorized)
        assertFalse(authorizationErrors.contains(error), s"$apiKey should be allowed. Found unexpected authorization error $error")
      else
        assertTrue(authorizationErrors.contains(error), s"$apiKey should be forbidden. Found error $error but expected one of $authorizationErrors")
    else if (resources == Set(TOPIC))
      if (isAuthorized)
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, error, s"$apiKey had an unexpected error")
      else
        assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED, error, s"$apiKey had an unexpected error")

    response
  }

  private def sendRecords(producer: KafkaProducer[Array[Byte], Array[Byte]],
                          numRecords: Int,
                          tp: TopicPartition): Unit = {
    val futures = (0 until numRecords).map { i =>
      producer.send(new ProducerRecord(tp.topic(), tp.partition(), i.toString.getBytes, i.toString.getBytes))
    }
    try {
      futures.foreach(_.get)
    } catch {
      case e: ExecutionException => throw e.getCause
    }
  }

  private def addAndVerifyAcls(acls: Set[AccessControlEntry], resource: ResourcePattern): Unit = {
    TestUtils.addAndVerifyAcls(servers.head, acls, resource)
  }

  private def consumeRecords(consumer: Consumer[Array[Byte], Array[Byte]],
                             numRecords: Int = 1,
                             startingOffset: Int = 0,
                             topic: String = topic,
                             part: Int = part): Unit = {
    val records = TestUtils.consumeRecords(consumer, numRecords)

    for (i <- 0 until numRecords) {
      val record = records(i)
      val offset = startingOffset + i
      assertEquals(topic, record.topic)
      assertEquals(part, record.partition)
      assertEquals(offset.toLong, record.offset)
    }
  }

  private def buildTransactionalProducer(): KafkaProducer[Array[Byte], Array[Byte]] = {
    producerConfig.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId)
    producerConfig.setProperty(ProducerConfig.ACKS_CONFIG, "all")
    createProducer()
  }

  private def buildIdempotentProducer(): KafkaProducer[Array[Byte], Array[Byte]] = {
    producerConfig.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    producerConfig.setProperty(ProducerConfig.ACKS_CONFIG, "all")
    createProducer()
  }

  private def createAdminClient(): Admin = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    val adminClient = Admin.create(props)
    adminClients += adminClient
    adminClient
  }

}
