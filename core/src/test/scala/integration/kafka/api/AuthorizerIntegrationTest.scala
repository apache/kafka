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

import java.time.Duration
import java.util
import java.util.concurrent.ExecutionException
import java.util.regex.Pattern
import java.util.{Collections, Optional, Properties}

import kafka.admin.ConsumerGroupCommand.{ConsumerGroupCommandOptions, ConsumerGroupService}
import kafka.log.LogConfig
import kafka.security.auth.{SimpleAclAuthorizer, Topic, ResourceType => AuthResourceType}
import kafka.security.authorizer.AuthorizerUtils.WildcardHost
import kafka.server.{BaseRequestTest, KafkaConfig}
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.{Admin, AdminClient, AdminClientConfig, AlterConfigOp}
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.acl.AclOperation._
import org.apache.kafka.common.acl.AclPermissionType.{ALLOW, DENY}
import org.apache.kafka.common.acl.{AccessControlEntry, AccessControlEntryFilter, AclBinding, AclBindingFilter, AclOperation}
import org.apache.kafka.common.config.{ConfigResource, LogLevelConfig}
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.Topic.GROUP_METADATA_TOPIC_NAME
import org.apache.kafka.common.message.CreateTopicsRequestData.{CreatableTopic, CreatableTopicCollection}
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.{AlterConfigsResource, AlterableConfig, AlterableConfigCollection}
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocolCollection
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity
import org.apache.kafka.common.message.UpdateMetadataRequestData.{UpdateMetadataBroker, UpdateMetadataEndpoint, UpdateMetadataPartitionState}
import org.apache.kafka.common.message.{AlterPartitionReassignmentsRequestData, ControlledShutdownRequestData, CreateTopicsRequestData, DeleteGroupsRequestData, DeleteTopicsRequestData, DescribeGroupsRequestData, FindCoordinatorRequestData, HeartbeatRequestData, IncrementalAlterConfigsRequestData, JoinGroupRequestData, ListPartitionReassignmentsRequestData, OffsetCommitRequestData, SyncGroupRequestData}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, RecordBatch, Records, SimpleRecord}
import org.apache.kafka.common.requests.CreateAclsRequest.AclCreation
import org.apache.kafka.common.requests._
import org.apache.kafka.common.resource.PatternType.LITERAL
import org.apache.kafka.common.resource.ResourceType._
import org.apache.kafka.common.resource.{Resource, ResourcePattern, ResourcePatternFilter, ResourceType}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.{ElectionType, IsolationLevel, KafkaException, Node, TopicPartition, requests}
import org.apache.kafka.test.{TestUtils => JTestUtils}
import org.junit.Assert._
import org.junit.{After, Assert, Before, Test}
import org.scalatest.Assertions.intercept

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.Buffer
import scala.compat.java8.OptionConverters._

class AuthorizerIntegrationTest extends BaseRequestTest {

  override def brokerCount: Int = 1
  val brokerId: Integer = 0
  def userPrincipal = KafkaPrincipal.ANONYMOUS

  val topic = "topic"
  val topicPattern = "topic.*"
  val createTopic = "topic-new"
  val deleteTopic = "topic-delete"
  val transactionalId = "transactional.id"
  val producerId = 83392L
  val part = 0
  val correlationId = 0
  val clientId = "client-Id"
  val tp = new TopicPartition(topic, part)
  val logDir = "logDir"
  val deleteRecordsPartition = new TopicPartition(deleteTopic, part)
  val group = "my-group"
  val clusterResource = new ResourcePattern(CLUSTER, Resource.CLUSTER_NAME, LITERAL)
  val topicResource = new ResourcePattern(TOPIC, topic, LITERAL)
  val groupResource = new ResourcePattern(GROUP, group, LITERAL)
  val deleteTopicResource = new ResourcePattern(TOPIC, deleteTopic, LITERAL)
  val transactionalIdResource = new ResourcePattern(TRANSACTIONAL_ID, transactionalId, LITERAL)
  val createTopicResource = new ResourcePattern(TOPIC, createTopic, LITERAL)
  val userPrincipalStr = userPrincipal.toString

  val groupReadAcl = Map(groupResource -> Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)))
  val groupDescribeAcl = Map(groupResource -> Set(new AccessControlEntry(userPrincipalStr, WildcardHost, DESCRIBE, ALLOW)))
  val groupDeleteAcl = Map(groupResource -> Set(new AccessControlEntry(userPrincipalStr, WildcardHost, DELETE, ALLOW)))
  val clusterAcl = Map(clusterResource -> Set(new AccessControlEntry(userPrincipalStr, WildcardHost, CLUSTER_ACTION, ALLOW)))
  val clusterCreateAcl = Map(clusterResource -> Set(new AccessControlEntry(userPrincipalStr, WildcardHost, CREATE, ALLOW)))
  val clusterAlterAcl = Map(clusterResource -> Set(new AccessControlEntry(userPrincipalStr, WildcardHost, ALTER, ALLOW)))
  val clusterDescribeAcl = Map(clusterResource -> Set(new AccessControlEntry(userPrincipalStr, WildcardHost, DESCRIBE, ALLOW)))
  val clusterAlterConfigsAcl = Map(clusterResource -> Set(new AccessControlEntry(userPrincipalStr, WildcardHost, ALTER_CONFIGS, ALLOW)))
  val clusterIdempotentWriteAcl = Map(clusterResource -> Set(new AccessControlEntry(userPrincipalStr, WildcardHost, IDEMPOTENT_WRITE, ALLOW)))
  val topicCreateAcl = Map(createTopicResource -> Set(new AccessControlEntry(userPrincipalStr, WildcardHost, CREATE, ALLOW)))
  val topicReadAcl = Map(topicResource -> Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)))
  val topicWriteAcl = Map(topicResource -> Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)))
  val topicDescribeAcl = Map(topicResource -> Set(new AccessControlEntry(userPrincipalStr, WildcardHost, DESCRIBE, ALLOW)))
  val topicAlterAcl = Map(topicResource -> Set(new AccessControlEntry(userPrincipalStr, WildcardHost, ALTER, ALLOW)))
  val topicDeleteAcl = Map(deleteTopicResource -> Set(new AccessControlEntry(userPrincipalStr, WildcardHost, DELETE, ALLOW)))
  val topicDescribeConfigsAcl = Map(topicResource -> Set(new AccessControlEntry(userPrincipalStr, WildcardHost, DESCRIBE_CONFIGS, ALLOW)))
  val topicAlterConfigsAcl = Map(topicResource -> Set(new AccessControlEntry(userPrincipalStr, WildcardHost, ALTER_CONFIGS, ALLOW)))
  val transactionIdWriteAcl = Map(transactionalIdResource -> Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)))
  val transactionalIdDescribeAcl = Map(transactionalIdResource -> Set(new AccessControlEntry(userPrincipalStr, WildcardHost, DESCRIBE, ALLOW)))

  val numRecords = 1
  val adminClients = Buffer[Admin]()

  producerConfig.setProperty(ProducerConfig.ACKS_CONFIG, "1")
  producerConfig.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "50000")
  consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group)

  override def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.put(KafkaConfig.AuthorizerClassNameProp, classOf[SimpleAclAuthorizer].getName)
    properties.put(KafkaConfig.BrokerIdProp, brokerId.toString)
    properties.put(KafkaConfig.OffsetsTopicPartitionsProp, "1")
    properties.put(KafkaConfig.OffsetsTopicReplicationFactorProp, "1")
    properties.put(KafkaConfig.TransactionsTopicPartitionsProp, "1")
    properties.put(KafkaConfig.TransactionsTopicReplicationFactorProp, "1")
    properties.put(KafkaConfig.TransactionsTopicMinISRProp, "1")
  }

  val requestKeyToError = Map[ApiKeys, Nothing => Errors](
    ApiKeys.METADATA -> ((resp: requests.MetadataResponse) => resp.errors.asScala.find(_._1 == topic).getOrElse(("test", Errors.NONE))._2),
    ApiKeys.PRODUCE -> ((resp: requests.ProduceResponse) => resp.responses.asScala.find(_._1 == tp).get._2.error),
    ApiKeys.FETCH -> ((resp: requests.FetchResponse[Records]) => resp.responseData.asScala.find(_._1 == tp).get._2.error),
    ApiKeys.LIST_OFFSETS -> ((resp: requests.ListOffsetResponse) => resp.responseData.asScala.find(_._1 == tp).get._2.error),
    ApiKeys.OFFSET_COMMIT -> ((resp: requests.OffsetCommitResponse) => Errors.forCode(
      resp.data().topics().get(0).partitions().get(0).errorCode())),
    ApiKeys.OFFSET_FETCH -> ((resp: requests.OffsetFetchResponse) => resp.error),
    ApiKeys.FIND_COORDINATOR -> ((resp: FindCoordinatorResponse) => resp.error),
    ApiKeys.UPDATE_METADATA -> ((resp: requests.UpdateMetadataResponse) => resp.error),
    ApiKeys.JOIN_GROUP -> ((resp: JoinGroupResponse) => resp.error),
    ApiKeys.SYNC_GROUP -> ((resp: SyncGroupResponse) => Errors.forCode(resp.data.errorCode())),
    ApiKeys.DESCRIBE_GROUPS -> ((resp: DescribeGroupsResponse) => {
      val errorCode = resp.data().groups().asScala.find(g => group.equals(g.groupId())).head.errorCode()
      Errors.forCode(errorCode)
    }),
    ApiKeys.HEARTBEAT -> ((resp: HeartbeatResponse) => resp.error),
    ApiKeys.LEAVE_GROUP -> ((resp: LeaveGroupResponse) => resp.error),
    ApiKeys.DELETE_GROUPS -> ((resp: DeleteGroupsResponse) => resp.get(group)),
    ApiKeys.LEADER_AND_ISR -> ((resp: requests.LeaderAndIsrResponse) => Errors.forCode(
      resp.partitions.asScala.find(p => p.topicName == tp.topic && p.partitionIndex == tp.partition).get.errorCode)),
    ApiKeys.STOP_REPLICA -> ((resp: requests.StopReplicaResponse) => Errors.forCode(
      resp.partitionErrors.asScala.find(pe => pe.topicName == tp.topic && pe.partitionIndex == tp.partition).get.errorCode)),
    ApiKeys.CONTROLLED_SHUTDOWN -> ((resp: requests.ControlledShutdownResponse) => resp.error),
    ApiKeys.CREATE_TOPICS -> ((resp: CreateTopicsResponse) => Errors.forCode(resp.data.topics.find(createTopic).errorCode())),
    ApiKeys.DELETE_TOPICS -> ((resp: requests.DeleteTopicsResponse) => Errors.forCode(resp.data.responses.find(deleteTopic).errorCode())),
    ApiKeys.DELETE_RECORDS -> ((resp: requests.DeleteRecordsResponse) => resp.responses.get(deleteRecordsPartition).error),
    ApiKeys.OFFSET_FOR_LEADER_EPOCH -> ((resp: OffsetsForLeaderEpochResponse) => resp.responses.get(tp).error),
    ApiKeys.DESCRIBE_CONFIGS -> ((resp: DescribeConfigsResponse) =>
      resp.configs.get(new ConfigResource(ConfigResource.Type.TOPIC, tp.topic)).error.error),
    ApiKeys.ALTER_CONFIGS -> ((resp: AlterConfigsResponse) =>
      resp.errors.get(new ConfigResource(ConfigResource.Type.TOPIC, tp.topic)).error),
    ApiKeys.INIT_PRODUCER_ID -> ((resp: InitProducerIdResponse) => resp.error),
    ApiKeys.WRITE_TXN_MARKERS -> ((resp: WriteTxnMarkersResponse) => resp.errors(producerId).get(tp)),
    ApiKeys.ADD_PARTITIONS_TO_TXN -> ((resp: AddPartitionsToTxnResponse) => resp.errors.get(tp)),
    ApiKeys.ADD_OFFSETS_TO_TXN -> ((resp: AddOffsetsToTxnResponse) => resp.error),
    ApiKeys.END_TXN -> ((resp: EndTxnResponse) => resp.error),
    ApiKeys.TXN_OFFSET_COMMIT -> ((resp: TxnOffsetCommitResponse) => resp.errors.get(tp)),
    ApiKeys.CREATE_ACLS -> ((resp: CreateAclsResponse) => resp.aclCreationResponses.asScala.head.error.error),
    ApiKeys.DESCRIBE_ACLS -> ((resp: DescribeAclsResponse) => resp.error.error),
    ApiKeys.DELETE_ACLS -> ((resp: DeleteAclsResponse) => resp.responses.asScala.head.error.error),
    ApiKeys.ALTER_REPLICA_LOG_DIRS -> ((resp: AlterReplicaLogDirsResponse) => resp.responses.get(tp)),
    ApiKeys.DESCRIBE_LOG_DIRS -> ((resp: DescribeLogDirsResponse) =>
      if (resp.logDirInfos.size() > 0) resp.logDirInfos.asScala.head._2.error else Errors.CLUSTER_AUTHORIZATION_FAILED),
    ApiKeys.CREATE_PARTITIONS -> ((resp: CreatePartitionsResponse) => resp.errors.asScala.find(_._1 == topic).get._2.error),
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
          .topics().asScala.find(_.name() == topic).get
          .partitions().asScala.find(_.partitionIndex() == part).get
          .errorCode()
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

  @Before
  override def setUp(): Unit = {
    doSetup(createOffsetsTopic = false)

    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, CLUSTER_ACTION, ALLOW)), clusterResource)

    TestUtils.createOffsetsTopic(zkClient, servers)

    // create the test topic with all the brokers as replicas
    createTopic(topic)
    createTopic(deleteTopic)
  }

  @After
  override def tearDown() = {
    adminClients.foreach(_.close())
    removeAllAcls()
    super.tearDown()
  }

  private def createMetadataRequest(allowAutoTopicCreation: Boolean) = {
    new requests.MetadataRequest.Builder(List(topic).asJava, allowAutoTopicCreation).build()
  }

  private def createProduceRequest = {
    requests.ProduceRequest.Builder.forCurrentMagic(1, 5000,
      collection.mutable.Map(tp -> MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord("test".getBytes))).asJava).
      build()
  }

  private def createFetchRequest = {
    val partitionMap = new util.LinkedHashMap[TopicPartition, requests.FetchRequest.PartitionData]
    partitionMap.put(tp, new requests.FetchRequest.PartitionData(0, 0, 100, Optional.of(27)))
    requests.FetchRequest.Builder.forConsumer(100, Int.MaxValue, partitionMap).build()
  }

  private def createFetchFollowerRequest = {
    val partitionMap = new util.LinkedHashMap[TopicPartition, requests.FetchRequest.PartitionData]
    partitionMap.put(tp, new requests.FetchRequest.PartitionData(0, 0, 100, Optional.of(27)))
    requests.FetchRequest.Builder.forReplica(5000, 100, Int.MaxValue, partitionMap).build()
  }

  private def createListOffsetsRequest = {
    requests.ListOffsetRequest.Builder.forConsumer(false, IsolationLevel.READ_UNCOMMITTED).setTargetTimes(
      Map(tp -> new ListOffsetRequest.PartitionData(0L, Optional.of[Integer](27))).asJava).
      build()
  }

  private def offsetsForLeaderEpochRequest: OffsetsForLeaderEpochRequest = {
    val epochs = Map(tp -> new OffsetsForLeaderEpochRequest.PartitionData(Optional.of(27), 7))
    OffsetsForLeaderEpochRequest.Builder.forConsumer(epochs.asJava).build()
  }

  private def createOffsetFetchRequest = {
    new requests.OffsetFetchRequest.Builder(group, List(tp).asJava).build()
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
    new requests.UpdateMetadataRequest.Builder(brokerId, Int.MaxValue, Long.MaxValue, partitionStates, brokers)
      .build(version)
  }

  private def createJoinGroupRequest = {
    val protocolSet = new JoinGroupRequestProtocolCollection(
      Collections.singletonList(new JoinGroupRequestData.JoinGroupRequestProtocol()
        .setName("consumer-range")
        .setMetadata("test".getBytes())
    ).iterator())

    new JoinGroupRequest.Builder(
      new JoinGroupRequestData()
        .setGroupId(group)
        .setSessionTimeoutMs(10000)
        .setMemberId(JoinGroupRequest.UNKNOWN_MEMBER_ID)
        .setGroupInstanceId(null)
        .setProtocolType("consumer")
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
    new CreatePartitionsRequest.Builder(
      Map(topic -> new CreatePartitionsRequest.PartitionDetails(10)).asJava, 10000, true
    ).build()
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

  private def leaderAndIsrRequest = {
    new requests.LeaderAndIsrRequest.Builder(brokerId, Int.MaxValue, Long.MaxValue,
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
     Set(new Node(brokerId, "localhost", 0)).asJava)
      .build()
  }

  private def stopReplicaRequest = new StopReplicaRequest.Builder(brokerId, Int.MaxValue, Long.MaxValue, true, Set(tp).asJava).build()

  private def controlledShutdownRequest = new ControlledShutdownRequest.Builder(
      new ControlledShutdownRequestData()
        .setBrokerId(brokerId)
        .setBrokerEpoch(Long.MaxValue)).build()

  private def createTopicsRequest =
    new CreateTopicsRequest.Builder(new CreateTopicsRequestData().setTopics(
      new CreatableTopicCollection(Collections.singleton(new CreatableTopic().
        setName(createTopic).setNumPartitions(1).
          setReplicationFactor(1.toShort)).iterator))).build()

  private def deleteTopicsRequest =
    new DeleteTopicsRequest.Builder(
      new DeleteTopicsRequestData()
        .setTopicNames(Collections.singletonList(deleteTopic))
        .setTimeoutMs(5000)).build()

  private def deleteRecordsRequest = new DeleteRecordsRequest.Builder(5000, Collections.singletonMap(deleteRecordsPartition, 0L)).build()

  private def describeConfigsRequest =
    new DescribeConfigsRequest.Builder(Collections.singleton(new ConfigResource(ConfigResource.Type.TOPIC, tp.topic))).build()

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

  private def createAclsRequest = new CreateAclsRequest.Builder(
    Collections.singletonList(new AclCreation(new AclBinding(
      new ResourcePattern(ResourceType.TOPIC, "mytopic", LITERAL),
      new AccessControlEntry(userPrincipalStr, "*", AclOperation.WRITE, DENY))))).build()

  private def deleteAclsRequest = new DeleteAclsRequest.Builder(
    Collections.singletonList(new AclBindingFilter(
      new ResourcePatternFilter(ResourceType.TOPIC, null, LITERAL),
      new AccessControlEntryFilter(userPrincipalStr, "*", AclOperation.ANY, DENY)))).build()

  private def alterReplicaLogDirsRequest = new AlterReplicaLogDirsRequest.Builder(Collections.singletonMap(tp, logDir)).build()

  private def describeLogDirsRequest = new DescribeLogDirsRequest.Builder(Collections.singleton(tp)).build()

  private def addPartitionsToTxnRequest = new AddPartitionsToTxnRequest.Builder(transactionalId, 1, 1, Collections.singletonList(tp)).build()

  private def addOffsetsToTxnRequest = new AddOffsetsToTxnRequest.Builder(transactionalId, 1, 1, group).build()

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
      ApiKeys.METADATA -> createMetadataRequest(allowAutoTopicCreation = true),
      ApiKeys.PRODUCE -> createProduceRequest,
      ApiKeys.FETCH -> createFetchRequest,
      ApiKeys.LIST_OFFSETS -> createListOffsetsRequest,
      ApiKeys.OFFSET_FETCH -> createOffsetFetchRequest,
      ApiKeys.FIND_COORDINATOR -> createFindCoordinatorRequest,
      ApiKeys.UPDATE_METADATA -> createUpdateMetadataRequest,
      ApiKeys.JOIN_GROUP -> createJoinGroupRequest,
      ApiKeys.SYNC_GROUP -> createSyncGroupRequest,
      ApiKeys.DESCRIBE_GROUPS -> createDescribeGroupsRequest,
      ApiKeys.OFFSET_COMMIT -> createOffsetCommitRequest,
      ApiKeys.HEARTBEAT -> heartbeatRequest,
      ApiKeys.LEAVE_GROUP -> leaveGroupRequest,
      ApiKeys.LEADER_AND_ISR -> leaderAndIsrRequest,
      ApiKeys.CONTROLLED_SHUTDOWN -> controlledShutdownRequest,
      ApiKeys.CREATE_TOPICS -> createTopicsRequest,
      ApiKeys.DELETE_TOPICS -> deleteTopicsRequest,
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
      // Check StopReplica last since some APIs depend on replica availability
      ApiKeys.STOP_REPLICA -> stopReplicaRequest
    )

    for ((key, request) <- requestKeyToRequest) {
      removeAllAcls()
      val resources = requestKeysToAcls(key).map(_._1.resourceType).toSet
      sendRequestAndVerifyResponseError(request, resources, isAuthorized = false)

      val resourceToAcls = requestKeysToAcls(key)
      resourceToAcls.get(topicResource).foreach { acls =>
        val describeAcls = topicDescribeAcl(topicResource)
        val isAuthorized = describeAcls == acls
        addAndVerifyAcls(describeAcls, topicResource)
        sendRequestAndVerifyResponseError(request, resources, isAuthorized = isAuthorized)
        removeAllAcls()
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
    adminZkClient.deleteTopic(topic)
    TestUtils.verifyTopicDeletion(zkClient, topic, 1, servers)
    adminZkClient.deleteTopic(deleteTopic)
    TestUtils.verifyTopicDeletion(zkClient, deleteTopic, 1, servers)

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
      removeAllAcls()
      val resources = requestKeysToAcls(key).map(_._1.resourceType).toSet
      sendRequestAndVerifyResponseError(request, resources, isAuthorized = false, topicExists = false)

      val resourceToAcls = requestKeysToAcls(key)
      resourceToAcls.get(topicResource).foreach { acls =>
        val describeAcls = topicDescribeAcl(topicResource)
        val isAuthorized = describeAcls == acls
        addAndVerifyAcls(describeAcls, topicResource)
        sendRequestAndVerifyResponseError(request, resources, isAuthorized = isAuthorized, topicExists = false)
        removeAllAcls()
      }

      for ((resource, acls) <- resourceToAcls)
        addAndVerifyAcls(acls, resource)
      sendRequestAndVerifyResponseError(request, resources, isAuthorized = true, topicExists = false)
    }
  }

  @Test
  def testCreateTopicAuthorizationWithClusterCreate(): Unit = {
    removeAllAcls()
    val resources = Set[ResourceType](TOPIC)

    sendRequestAndVerifyResponseError(createTopicsRequest, resources, isAuthorized = false)

    for ((resource, acls) <- clusterCreateAcl)
      addAndVerifyAcls(acls, resource)
    sendRequestAndVerifyResponseError(createTopicsRequest, resources, isAuthorized = true)
  }

  @Test
  def testFetchFollowerRequest(): Unit = {
    val request = createFetchFollowerRequest

    removeAllAcls()
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
    val data = new IncrementalAlterConfigsRequestData
    val alterableConfig = new AlterableConfig().setName("kafka.controller.KafkaController").
      setValue(LogLevelConfig.DEBUG_LOG_LEVEL).setConfigOperation(AlterConfigOp.OpType.DELETE.id())
    val alterableConfigSet = new AlterableConfigCollection
    alterableConfigSet.add(alterableConfig)
    data.resources().add(new AlterConfigsResource().
      setResourceName(brokerId.toString).setResourceType(ConfigResource.Type.BROKER_LOGGER.id()).
      setConfigs(alterableConfigSet))
    val request = new IncrementalAlterConfigsRequest.Builder(data).build()

    removeAllAcls()
    val resources = Set(topicResource.resourceType, clusterResource.resourceType)
    sendRequestAndVerifyResponseError(request, resources, isAuthorized = false)

    val clusterAcls = clusterAlterConfigsAcl(clusterResource)
    addAndVerifyAcls(clusterAcls, clusterResource)
    sendRequestAndVerifyResponseError(request, resources, isAuthorized = true)
  }

  @Test
  def testOffsetsForLeaderEpochClusterPermission(): Unit = {
    val request = offsetsForLeaderEpochRequest

    removeAllAcls()

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
    try {
      val producer = createProducer()
      sendRecords(producer, numRecords, tp)
      fail("should have thrown exception")
    } catch {
      case _: TopicAuthorizationException => //expected
    }
  }

  @Test
  def testProduceWithTopicDescribe(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, DESCRIBE, ALLOW)), topicResource)
    try {
      val producer = createProducer()
      sendRecords(producer, numRecords, tp)
      fail("should have thrown exception")
    } catch {
      case e: TopicAuthorizationException =>
        assertEquals(Collections.singleton(topic), e.unauthorizedTopics())
    }
  }

  @Test
  def testProduceWithTopicRead(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), topicResource)
    try {
      val producer = createProducer()
      sendRecords(producer, numRecords, tp)
      fail("should have thrown exception")
    } catch {
      case e: TopicAuthorizationException =>
        assertEquals(Collections.singleton(topic), e.unauthorizedTopics())
    }
  }

  @Test
  def testProduceWithTopicWrite(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)), topicResource)
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
    val topicPartition = new TopicPartition(createTopic, 0)
    val newTopicResource = new ResourcePattern(TOPIC, createTopic, LITERAL)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)), newTopicResource)
    val producer = createProducer()
    try {
      sendRecords(producer, numRecords, topicPartition)
      Assert.fail("should have thrown exception")
    } catch {
      case e: TopicAuthorizationException =>
        assertEquals(Collections.singleton(createTopic), e.unauthorizedTopics())
    }

    val resource = if (resType == ResourceType.TOPIC) newTopicResource else clusterResource
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, CREATE, ALLOW)), resource)

    sendRecords(producer, numRecords, topicPartition)
  }

  @Test(expected = classOf[TopicAuthorizationException])
  def testConsumeUsingAssignWithNoAccess(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllAcls()

    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumeRecords(consumer)
  }

  @Test
  def testSimpleConsumeWithOffsetLookupAndNoGroupAccess(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), topicResource)
    try {
      // note this still depends on group access because we haven't set offsets explicitly, which means
      // they will first be fetched from the consumer coordinator (which requires group access)
      val consumer = createConsumer()
      consumer.assign(List(tp).asJava)
      consumeRecords(consumer)
      Assert.fail("should have thrown exception")
    } catch {
      case e: GroupAuthorizationException => assertEquals(group, e.groupId())
    }
  }

  @Test
  def testSimpleConsumeWithExplicitSeekAndNoGroupAccess(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), topicResource)

    // in this case, we do an explicit seek, so there should be no need to query the coordinator at all
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumer.seekToBeginning(List(tp).asJava)
    consumeRecords(consumer)
  }

  @Test(expected = classOf[KafkaException])
  def testConsumeWithoutTopicDescribeAccess(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), groupResource)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)

    // the consumer should raise an exception if it receives UNKNOWN_TOPIC_OR_PARTITION
    // from the ListOffsets response when looking up the initial position.
    consumeRecords(consumer)
  }

  @Test
  def testConsumeWithTopicDescribe(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, DESCRIBE, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), groupResource)
    try {
      val consumer = createConsumer()
      consumer.assign(List(tp).asJava)
      consumeRecords(consumer)
      Assert.fail("should have thrown exception")
    } catch {
      case e: TopicAuthorizationException => assertEquals(Collections.singleton(topic), e.unauthorizedTopics())
    }
  }

  @Test
  def testConsumeWithTopicWrite(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), groupResource)
    try {
      val consumer = createConsumer()
      consumer.assign(List(tp).asJava)
      consumeRecords(consumer)
      Assert.fail("should have thrown exception")
    } catch {
      case e: TopicAuthorizationException =>
        assertEquals(Collections.singleton(topic), e.unauthorizedTopics())
    }
  }

  @Test
  def testConsumeWithTopicAndGroupRead(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), groupResource)

    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumeRecords(consumer)
  }

  @Test
  def testPatternSubscriptionWithNoTopicAccess(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), groupResource)

    val consumer = createConsumer()
    consumer.subscribe(Pattern.compile(topicPattern), new NoOpConsumerRebalanceListener)
    consumer.poll(50)
    assertTrue(consumer.subscription.isEmpty)
  }

  @Test
  def testPatternSubscriptionWithTopicDescribeOnlyAndGroupRead(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, DESCRIBE, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), groupResource)
    val consumer = createConsumer()
    consumer.subscribe(Pattern.compile(topicPattern))
    try {
      consumeRecords(consumer)
      Assert.fail("Expected TopicAuthorizationException")
    } catch {
      case _: TopicAuthorizationException => //expected
    }
  }

  @Test
  def testPatternSubscriptionWithTopicAndGroupRead(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)

    // create an unmatched topic
    val unmatchedTopic = "unmatched"
    createTopic(unmatchedTopic)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)),  new ResourcePattern(TOPIC, unmatchedTopic, LITERAL))
    sendRecords(producer, 1, new TopicPartition(unmatchedTopic, part))
    removeAllAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), groupResource)
    val consumer = createConsumer()
    consumer.subscribe(Pattern.compile(topicPattern))
    consumeRecords(consumer)

    // set the subscription pattern to an internal topic that the consumer has read permission to. Since
    // internal topics are not included, we should not be assigned any partitions from this topic
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)),  new ResourcePattern(TOPIC,
      GROUP_METADATA_TOPIC_NAME, LITERAL))
    consumer.subscribe(Pattern.compile(GROUP_METADATA_TOPIC_NAME))
    consumer.poll(0)
    assertTrue(consumer.subscription().isEmpty)
    assertTrue(consumer.assignment().isEmpty)
  }

  @Test
  def testPatternSubscriptionMatchingInternalTopic(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), groupResource)

    consumerConfig.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false")
    val consumer = createConsumer()
    // ensure that internal topics are not included if no permission
    consumer.subscribe(Pattern.compile(".*"))
    consumeRecords(consumer)
    assertEquals(Set(topic).asJava, consumer.subscription)

    // now authorize the user for the internal topic and verify that we can subscribe
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), new ResourcePattern(TOPIC,
      GROUP_METADATA_TOPIC_NAME, LITERAL))
    consumer.subscribe(Pattern.compile(GROUP_METADATA_TOPIC_NAME))
    consumer.poll(0)
    assertEquals(Set(GROUP_METADATA_TOPIC_NAME), consumer.subscription.asScala)
  }

  @Test
  def testPatternSubscriptionMatchingInternalTopicWithDescribeOnlyPermission(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), groupResource)
    val internalTopicResource = new ResourcePattern(TOPIC, GROUP_METADATA_TOPIC_NAME, LITERAL)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, DESCRIBE, ALLOW)), internalTopicResource)

    consumerConfig.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false")
    val consumer = createConsumer()
    try {
      consumer.subscribe(Pattern.compile(".*"))
      // It is possible that the first call returns records of "topic" and the second call throws TopicAuthorizationException
      consumeRecords(consumer)
      consumeRecords(consumer)
      Assert.fail("Expected TopicAuthorizationException")
    } catch {
      case _: TopicAuthorizationException => //expected
    }
  }

  @Test
  def testPatternSubscriptionNotMatchingInternalTopic(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), groupResource)

    consumerConfig.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false")
    val consumer = createConsumer()
    try {
      consumer.subscribe(Pattern.compile(topicPattern))
      consumeRecords(consumer)
    } finally consumer.close()
  }

  @Test
  def testCreatePermissionOnTopicToReadFromNonExistentTopic(): Unit = {
    testCreatePermissionNeededToReadFromNonExistentTopic("newTopic",
      Set(new AccessControlEntry(userPrincipalStr, WildcardHost, CREATE, ALLOW)),
      TOPIC)
  }

  @Test
  def testCreatePermissionOnClusterToReadFromNonExistentTopic(): Unit = {
    testCreatePermissionNeededToReadFromNonExistentTopic("newTopic",
      Set(new AccessControlEntry(userPrincipalStr, WildcardHost, CREATE, ALLOW)),
      CLUSTER)
  }

  private def testCreatePermissionNeededToReadFromNonExistentTopic(newTopic: String, acls: Set[AccessControlEntry], resType: ResourceType): Unit = {
    val topicPartition = new TopicPartition(newTopic, 0)
    val newTopicResource = new ResourcePattern(TOPIC, newTopic, LITERAL)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), newTopicResource)
    addAndVerifyAcls(groupReadAcl(groupResource), groupResource)
    val consumer = createConsumer()
    consumer.assign(List(topicPartition).asJava)
    val unauthorizedTopics = intercept[TopicAuthorizationException] {
      (0 until 10).foreach(_ => consumer.poll(Duration.ofMillis(50L)))
    }.unauthorizedTopics
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
    assertTrue(zkClient.topicExists(topicResource.name))

    addAndVerifyAcls(readAcls, createTopicResource)
    assertFalse(zkClient.topicExists(createTopic))

    val metadataRequest = new MetadataRequest.Builder(List(topic, createTopic).asJava, true).build()
    val metadataResponse = connectAndReceive[MetadataResponse](metadataRequest)

    assertEquals(Set(topic).asJava, metadataResponse.topicsByError(Errors.NONE))
    assertEquals(Set(createTopic).asJava, metadataResponse.topicsByError(Errors.TOPIC_AUTHORIZATION_FAILED))

    val createAcls = topicCreateAcl(createTopicResource)
    addAndVerifyAcls(createAcls, createTopicResource)

    // retry as topic being created can have MetadataResponse with Errors.LEADER_NOT_AVAILABLE
    TestUtils.retry(JTestUtils.DEFAULT_MAX_WAIT_MS)(() => {
      val metadataResponse = connectAndReceive[MetadataResponse](metadataRequest)
      assertEquals(Set(topic, createTopic).asJava, metadataResponse.topicsByError(Errors.NONE))
    })
  }

  @Test(expected = classOf[AuthorizationException])
  def testCommitWithNoAccess(): Unit = {
    val consumer = createConsumer()
    consumer.commitSync(Map(tp -> new OffsetAndMetadata(5)).asJava)
  }

  @Test(expected = classOf[KafkaException])
  def testCommitWithNoTopicAccess(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), groupResource)
    val consumer = createConsumer()
    consumer.commitSync(Map(tp -> new OffsetAndMetadata(5)).asJava)
  }

  @Test(expected = classOf[TopicAuthorizationException])
  def testCommitWithTopicWrite(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.commitSync(Map(tp -> new OffsetAndMetadata(5)).asJava)
  }

  @Test(expected = classOf[TopicAuthorizationException])
  def testCommitWithTopicDescribe(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, DESCRIBE, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.commitSync(Map(tp -> new OffsetAndMetadata(5)).asJava)
  }

  @Test(expected = classOf[GroupAuthorizationException])
  def testCommitWithNoGroupAccess(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.commitSync(Map(tp -> new OffsetAndMetadata(5)).asJava)
  }

  @Test
  def testCommitWithTopicAndGroupRead(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.commitSync(Map(tp -> new OffsetAndMetadata(5)).asJava)
  }

  @Test(expected = classOf[AuthorizationException])
  def testOffsetFetchWithNoAccess(): Unit = {
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumer.position(tp)
  }

  @Test(expected = classOf[GroupAuthorizationException])
  def testOffsetFetchWithNoGroupAccess(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumer.position(tp)
  }

  @Test(expected = classOf[KafkaException])
  def testOffsetFetchWithNoTopicAccess(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), groupResource)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumer.position(tp)
  }

  @Test
  def testFetchAllOffsetsTopicAuthorization(): Unit = {
    val offset = 15L
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumer.commitSync(Map(tp -> new OffsetAndMetadata(offset)).asJava)

    removeAllAcls()
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), groupResource)

    // send offset fetch requests directly since the consumer does not expose an API to do so
    // note there's only one broker, so no need to lookup the group coordinator

    // without describe permission on the topic, we shouldn't be able to fetch offsets
    val offsetFetchRequest = requests.OffsetFetchRequest.Builder.allTopicPartitions(group).build()
    var offsetFetchResponse = connectAndReceive[OffsetFetchResponse](offsetFetchRequest)
    assertEquals(Errors.NONE, offsetFetchResponse.error)
    assertTrue(offsetFetchResponse.responseData.isEmpty)

    // now add describe permission on the topic and verify that the offset can be fetched
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, DESCRIBE, ALLOW)), topicResource)
    offsetFetchResponse = connectAndReceive[OffsetFetchResponse](offsetFetchRequest)
    assertEquals(Errors.NONE, offsetFetchResponse.error)
    assertTrue(offsetFetchResponse.responseData.containsKey(tp))
    assertEquals(offset, offsetFetchResponse.responseData.get(tp).offset)
  }

  @Test
  def testOffsetFetchTopicDescribe(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, DESCRIBE, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, DESCRIBE, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumer.position(tp)
  }

  @Test
  def testOffsetFetchWithTopicAndGroupRead(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumer.position(tp)
  }

  @Test(expected = classOf[TopicAuthorizationException])
  def testMetadataWithNoTopicAccess(): Unit = {
    val consumer = createConsumer()
    consumer.partitionsFor(topic)
  }

  @Test
  def testMetadataWithTopicDescribe(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, DESCRIBE, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.partitionsFor(topic)
  }

  @Test(expected = classOf[TopicAuthorizationException])
  def testListOffsetsWithNoTopicAccess(): Unit = {
    val consumer = createConsumer()
    consumer.endOffsets(Set(tp).asJava)
  }

  @Test
  def testListOffsetsWithTopicDescribe(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, DESCRIBE, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.endOffsets(Set(tp).asJava)
  }

  @Test
  def testDescribeGroupApiWithNoGroupAcl(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, DESCRIBE, ALLOW)), topicResource)
    val result = createAdminClient().describeConsumerGroups(Seq(group).asJava)
    TestUtils.assertFutureExceptionTypeEquals(result.describedGroups().get(group), classOf[GroupAuthorizationException])
  }

  @Test
  def testDescribeGroupApiWithGroupDescribe(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, DESCRIBE, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, DESCRIBE, ALLOW)), topicResource)
    createAdminClient().describeConsumerGroups(Seq(group).asJava).describedGroups().get(group).get()
  }

  @Test
  def testDescribeGroupCliWithGroupDescribe(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, DESCRIBE, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, DESCRIBE, ALLOW)), topicResource)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    val consumerGroupService = new ConsumerGroupService(opts)
    consumerGroupService.describeGroups()
    consumerGroupService.close()
  }

  @Test
  def testListGroupApiWithAndWithoutListGroupAcls(): Unit = {
    // write some record to the topic
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, numRecords = 1, tp)

    // use two consumers to write to two different groups
    val group2 = "other group"
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), new ResourcePattern(GROUP, group2, LITERAL))
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.subscribe(Collections.singleton(topic))
    consumeRecords(consumer)

    val otherConsumer = TestUtils.createConsumer(TestUtils.getBrokerListStrFromServers(servers), groupId = group2, securityProtocol = SecurityProtocol.PLAINTEXT)
    otherConsumer.subscribe(Collections.singleton(topic))
    consumeRecords(otherConsumer)

    val adminClient = createAdminClient()

    // first use cluster describe permission
    removeAllAcls()
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, DESCRIBE, ALLOW)), clusterResource)
    // it should list both groups (due to cluster describe permission)
    assertEquals(Set(group, group2), adminClient.listConsumerGroups().all().get().asScala.map(_.groupId()).toSet)

    // now replace cluster describe with group read permission
    removeAllAcls()
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), groupResource)
    // it should list only one group now
    val groupList = adminClient.listConsumerGroups().all().get().asScala.toList
    assertEquals(1, groupList.length)
    assertEquals(group, groupList.head.groupId)

    // now remove all acls and verify describe group access is required to list any group
    removeAllAcls()
    val listGroupResult = adminClient.listConsumerGroups()
    assertEquals(List(), listGroupResult.errors().get().asScala.toList)
    assertEquals(List(), listGroupResult.all().get().asScala.toList)
    otherConsumer.close()
  }

  @Test
  def testDeleteGroupApiWithDeleteGroupAcl(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, DELETE, ALLOW)), groupResource)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumer.commitSync(Map(tp -> new OffsetAndMetadata(5, "")).asJava)
    createAdminClient().deleteConsumerGroups(Seq(group).asJava).deletedGroups().get(group).get()
  }

  @Test
  def testDeleteGroupApiWithNoDeleteGroupAcl(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), topicResource)
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
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, DELETE, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumer.commitSync(Map(tp -> new OffsetAndMetadata(5, "")).asJava)
    consumer.close()
    val result = createAdminClient().deleteConsumerGroupOffsets(group, Set(tp).asJava)
    assertNull(result.partitionResult(tp).get())
  }

  @Test
  def testDeleteGroupOffsetsWithoutDeleteAcl(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumer.commitSync(Map(tp -> new OffsetAndMetadata(5, "")).asJava)
    consumer.close()
    val result = createAdminClient().deleteConsumerGroupOffsets(group, Set(tp).asJava)
    TestUtils.assertFutureExceptionTypeEquals(result.all(), classOf[GroupAuthorizationException])
  }

  @Test
  def testDeleteGroupOffsetsWithDeleteAclWithoutTopicAcl(): Unit = {
    // Create the consumer group
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumer.commitSync(Map(tp -> new OffsetAndMetadata(5, "")).asJava)
    consumer.close()

    // Remove the topic ACL & Check that it does not work without it
    removeAllAcls()
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, DELETE, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, READ, ALLOW)), groupResource)
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
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, deleteResponse.data.responses.find(deleteTopic).errorCode)
  }

  @Test
  def testUnauthorizedDeleteTopicsWithDescribe(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, DESCRIBE, ALLOW)), deleteTopicResource)
    val deleteResponse = connectAndReceive[DeleteTopicsResponse](deleteTopicsRequest)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, deleteResponse.data.responses.find(deleteTopic).errorCode)
  }

  @Test
  def testDeleteTopicsWithWildCardAuth(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, DELETE, ALLOW)), new ResourcePattern(TOPIC, "*", LITERAL))
    val deleteResponse = connectAndReceive[DeleteTopicsResponse](deleteTopicsRequest)
    assertEquals(Errors.NONE.code, deleteResponse.data.responses.find(deleteTopic).errorCode)
  }

  @Test
  def testUnauthorizedDeleteRecordsWithoutDescribe(): Unit = {
    val deleteRecordsResponse = connectAndReceive[DeleteRecordsResponse](deleteRecordsRequest)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED, deleteRecordsResponse.responses.asScala.head._2.error)
  }

  @Test
  def testUnauthorizedDeleteRecordsWithDescribe(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, DESCRIBE, ALLOW)), deleteTopicResource)
    val deleteRecordsResponse = connectAndReceive[DeleteRecordsResponse](deleteRecordsRequest)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED, deleteRecordsResponse.responses.asScala.head._2.error)
  }

  @Test
  def testDeleteRecordsWithWildCardAuth(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, DELETE, ALLOW)), new ResourcePattern(TOPIC, "*", LITERAL))
    val deleteRecordsResponse = connectAndReceive[DeleteRecordsResponse](deleteRecordsRequest)
    assertEquals(Errors.NONE, deleteRecordsResponse.responses.asScala.head._2.error)
  }

  @Test
  def testUnauthorizedCreatePartitions(): Unit = {
    val createPartitionsResponse = connectAndReceive[CreatePartitionsResponse](createPartitionsRequest)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED, createPartitionsResponse.errors.asScala.head._2.error)
  }

  @Test
  def testCreatePartitionsWithWildCardAuth(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, ALTER, ALLOW)), new ResourcePattern(TOPIC, "*", LITERAL))
    val createPartitionsResponse = connectAndReceive[CreatePartitionsResponse](createPartitionsRequest)
    assertEquals(Errors.NONE, createPartitionsResponse.errors.asScala.head._2.error)
  }

  @Test(expected = classOf[TransactionalIdAuthorizationException])
  def testTransactionalProducerInitTransactionsNoWriteTransactionalIdAcl(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, DESCRIBE, ALLOW)), transactionalIdResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
  }

  @Test(expected = classOf[TransactionalIdAuthorizationException])
  def testTransactionalProducerInitTransactionsNoDescribeTransactionalIdAcl(): Unit = {
    val producer = buildTransactionalProducer()
    producer.initTransactions()
  }

  @Test
  def testSendOffsetsWithNoConsumerGroupDescribeAccess(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, CLUSTER_ACTION, ALLOW)), clusterResource)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)), transactionalIdResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    producer.beginTransaction()
    try {
      producer.sendOffsetsToTransaction(Map(new TopicPartition(topic, 0) -> new OffsetAndMetadata(0L)).asJava, group)
      fail("Should have raised GroupAuthorizationException")
    } catch {
      case e: GroupAuthorizationException =>
    }
  }

  @Test
  def testSendOffsetsWithNoConsumerGroupWriteAccess(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)), transactionalIdResource)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, DESCRIBE, ALLOW)), groupResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    producer.beginTransaction()
    try {
      producer.sendOffsetsToTransaction(Map(new TopicPartition(topic, 0) -> new OffsetAndMetadata(0L)).asJava, group)
      fail("Should have raised GroupAuthorizationException")
    } catch {
      case e: GroupAuthorizationException =>
    }
  }

  @Test
  def testIdempotentProducerNoIdempotentWriteAclInInitProducerId(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)), topicResource)
    val producer = buildIdempotentProducer()
    try {
      // the InitProducerId is sent asynchronously, so we expect the error either in the callback
      // or raised from send itself
      producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, "hi".getBytes)).get()
      fail("Should have raised ClusterAuthorizationException")
    } catch {
      case e: ExecutionException =>
        assertTrue(e.getCause.isInstanceOf[ClusterAuthorizationException])
    }
    try {
      // the second time, the call to send itself should fail (the producer becomes unusable
      // if no producerId can be obtained)
      producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, "hi".getBytes)).get()
      fail("Should have raised ClusterAuthorizationException")
    } catch {
      case e: ExecutionException =>
        assertTrue(e.getCause.isInstanceOf[ClusterAuthorizationException])
    }
  }

  @Test
  def testIdempotentProducerNoIdempotentWriteAclInProduce(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, IDEMPOTENT_WRITE, ALLOW)), clusterResource)

    val producer = buildIdempotentProducer()

    // first send should be fine since we have permission to get a ProducerId
    producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, "hi".getBytes)).get()

    // revoke the IdempotentWrite permission
    removeAllAcls()
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)), topicResource)

    try {
      // the send should now fail with a cluster auth error
      producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, "hi".getBytes)).get()
      fail("Should have raised ClusterAuthorizationException")
    } catch {
      case e: ExecutionException =>
        assertTrue(e.getCause.isInstanceOf[ClusterAuthorizationException])
    }
    try {
      // the second time, the call to send itself should fail (the producer becomes unusable
      // if no producerId can be obtained)
      producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, "hi".getBytes)).get()
      fail("Should have raised ClusterAuthorizationException")
    } catch {
      case e: ExecutionException =>
        assertTrue(e.getCause.isInstanceOf[ClusterAuthorizationException])
    }
  }

  @Test
  def shouldInitTransactionsWhenAclSet(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)), transactionalIdResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
  }

  @Test
  def testTransactionalProducerTopicAuthorizationExceptionInSendCallback(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)), transactionalIdResource)
    // add describe access so that we can fetch metadata
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, DESCRIBE, ALLOW)), topicResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    producer.beginTransaction()
    try {
      producer.send(new ProducerRecord(tp.topic, tp.partition, "1".getBytes, "1".getBytes)).get
      Assert.fail("expected TopicAuthorizationException")
    } catch {
      case e: ExecutionException =>
        e.getCause match {
          case cause: TopicAuthorizationException =>
            assertEquals(Set(topic), cause.unauthorizedTopics().asScala)
          case other =>
            fail("Unexpected failure cause in send callback")
        }
    }
  }

  @Test
  def testTransactionalProducerTopicAuthorizationExceptionInCommit(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)), transactionalIdResource)
    // add describe access so that we can fetch metadata
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, DESCRIBE, ALLOW)), topicResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    producer.beginTransaction()
    try {
      producer.send(new ProducerRecord(tp.topic, tp.partition, "1".getBytes, "1".getBytes))
      producer.commitTransaction()
      Assert.fail("expected TopicAuthorizationException")
    } catch {
      case e: TopicAuthorizationException =>
        assertEquals(Set(topic), e.unauthorizedTopics().asScala)
    }
  }

  @Test
  def shouldThrowTransactionalIdAuthorizationExceptionWhenNoTransactionAccessDuringSend(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)), transactionalIdResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    removeAllAcls()
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)), topicResource)
    try {
      producer.beginTransaction()
      producer.send(new ProducerRecord(tp.topic, tp.partition, "1".getBytes, "1".getBytes)).get
      Assert.fail("expected TransactionalIdAuthorizationException")
    } catch {
      case e: ExecutionException => assertTrue(s"expected TransactionalIdAuthorizationException, but got ${e.getCause}",
        e.getCause.isInstanceOf[TransactionalIdAuthorizationException])
    }
  }

  @Test
  def shouldThrowTransactionalIdAuthorizationExceptionWhenNoTransactionAccessOnEndTransaction(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)), transactionalIdResource)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)), topicResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    producer.beginTransaction()
    producer.send(new ProducerRecord(tp.topic, tp.partition, "1".getBytes, "1".getBytes)).get
    removeAllAcls()
    try {
      producer.commitTransaction()
      Assert.fail("expected TransactionalIdAuthorizationException")
    } catch {
      case _: TransactionalIdAuthorizationException => // ok
    }
  }

  @Test
  def shouldSuccessfullyAbortTransactionAfterTopicAuthorizationException(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)), transactionalIdResource)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, DESCRIBE, ALLOW)), new ResourcePattern(TOPIC, deleteTopic, LITERAL))
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    producer.beginTransaction()
    producer.send(new ProducerRecord(tp.topic, tp.partition, "1".getBytes, "1".getBytes)).get
    // try and add a partition resulting in TopicAuthorizationException
    try {
      producer.send(new ProducerRecord(deleteTopic, 0, "1".getBytes, "1".getBytes)).get
    } catch {
      case e: ExecutionException =>
        assertTrue(e.getCause.isInstanceOf[TopicAuthorizationException])
    }
    // now rollback
    producer.abortTransaction()
  }

  @Test
  def shouldThrowTransactionalIdAuthorizationExceptionWhenNoTransactionAccessOnSendOffsetsToTxn(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)), transactionalIdResource)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)), groupResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    producer.beginTransaction()
    removeAllAcls()
    try {
      val offsets: util.Map[TopicPartition, OffsetAndMetadata] = Map(new TopicPartition(tp.topic, tp.partition) -> new OffsetAndMetadata(1L)).asJava
      producer.sendOffsetsToTransaction(offsets, group)
      Assert.fail("expected TransactionalIdAuthorizationException")
    } catch {
      case _: TransactionalIdAuthorizationException => // ok
    }
  }

  @Test
  def shouldSendSuccessfullyWhenIdempotentAndHasCorrectACL(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, IDEMPOTENT_WRITE, ALLOW)), clusterResource)
    addAndVerifyAcls(Set(new AccessControlEntry(userPrincipalStr, WildcardHost, WRITE, ALLOW)), topicResource)
    val producer = buildIdempotentProducer()
    producer.send(new ProducerRecord(tp.topic, tp.partition, "1".getBytes, "1".getBytes)).get
  }

  // Verify that metadata request without topics works without any ACLs and returns cluster id
  @Test
  def testClusterId(): Unit = {
    val request = new requests.MetadataRequest.Builder(List.empty.asJava, false).build()
    val response = connectAndReceive[MetadataResponse](request)
    assertEquals(Collections.emptyMap, response.errorCounts)
    assertFalse("Cluster id not returned", response.clusterId.isEmpty)
  }

  def removeAllAcls(): Unit = {
    val authorizer = servers.head.dataPlaneRequestProcessor.authorizer.get
    val aclFilter = AclBindingFilter.ANY
    authorizer.deleteAcls(null, List(aclFilter).asJava).asScala.map(_.toCompletableFuture.get).flatMap { deletion =>
      deletion.aclBindingDeleteResults().asScala.map(_.aclBinding.pattern).toSet
    }.foreach { resource =>
      TestUtils.waitAndVerifyAcls(Set.empty[AccessControlEntry], authorizer, resource)
    }
  }

  private def sendRequestAndVerifyResponseError(request: AbstractRequest,
                                                resources: Set[ResourceType],
                                                isAuthorized: Boolean,
                                                topicExists: Boolean = true): AbstractResponse = {
    val apiKey = request.api
    val response = connectAndReceive[AbstractResponse](request)
    val error = requestKeyToError(apiKey).asInstanceOf[AbstractResponse => Errors](response)

    val authorizationErrors = resources.flatMap { resourceType =>
      if (resourceType == TOPIC) {
        if (isAuthorized)
          Set(Errors.UNKNOWN_TOPIC_OR_PARTITION, Topic.error)
        else
          Set(Topic.error)
      } else {
        Set(AuthResourceType.fromJava(resourceType).error)
      }
    }

    if (topicExists)
      if (isAuthorized)
        assertFalse(s"$apiKey should be allowed. Found unexpected authorization error $error", authorizationErrors.contains(error))
      else
        assertTrue(s"$apiKey should be forbidden. Found error $error but expected one of $authorizationErrors", authorizationErrors.contains(error))
    else if (resources == Set(TOPIC))
      if (isAuthorized)
        assertEquals(s"$apiKey had an unexpected error", Errors.UNKNOWN_TOPIC_OR_PARTITION, error)
      else
        assertEquals(s"$apiKey had an unexpected error", Errors.TOPIC_AUTHORIZATION_FAILED, error)

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
    val aclBindings = acls.map { acl => new AclBinding(resource, acl) }
    servers.head.dataPlaneRequestProcessor.authorizer.get
      .createAcls(null, aclBindings.toList.asJava).asScala.map(_.toCompletableFuture.get).foreach {result =>
        result.exception.asScala.foreach { e => throw e }
      }
    val aclFilter = new AclBindingFilter(resource.toFilter, AccessControlEntryFilter.ANY)
    TestUtils.waitAndVerifyAcls(
      servers.head.dataPlaneRequestProcessor.authorizer.get.acls(aclFilter).asScala.map(_.entry).toSet ++ acls,
      servers.head.dataPlaneRequestProcessor.authorizer.get, resource)
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
    val adminClient = AdminClient.create(props)
    adminClients += adminClient
    adminClient
  }

}
