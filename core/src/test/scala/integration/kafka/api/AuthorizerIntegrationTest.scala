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
import java.util.concurrent.{ExecutionException, Semaphore}
import java.util.regex.Pattern
import java.util.{Collections, Optional, Properties}
import kafka.utils.{TestInfoUtils, TestUtils}
import kafka.utils.TestUtils.waitUntilTrue
import org.apache.kafka.clients.admin.{Admin, AlterConfigOp, NewTopic}
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.acl.AclOperation._
import org.apache.kafka.common.acl.AclPermissionType.{ALLOW, DENY}
import org.apache.kafka.common.acl.{AccessControlEntry, AccessControlEntryFilter, AclBindingFilter, AclOperation, AclPermissionType}
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.config.{ConfigResource, LogLevelConfig, TopicConfig}
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.Topic.GROUP_METADATA_TOPIC_NAME
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic
import org.apache.kafka.common.message.CreateTopicsRequestData.{CreatableTopic, CreatableTopicCollection}
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.{AlterConfigsResource, AlterableConfig, AlterableConfigCollection}
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocolCollection
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity
import org.apache.kafka.common.message.ListOffsetsRequestData.{ListOffsetsPartition, ListOffsetsTopic}
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.{OffsetForLeaderPartition, OffsetForLeaderTopic, OffsetForLeaderTopicCollection}
import org.apache.kafka.common.message.{AddOffsetsToTxnRequestData, AlterPartitionReassignmentsRequestData, AlterReplicaLogDirsRequestData, ConsumerGroupDescribeRequestData, ConsumerGroupHeartbeatRequestData, CreateAclsRequestData, CreatePartitionsRequestData, CreateTopicsRequestData, DeleteAclsRequestData, DeleteGroupsRequestData, DeleteRecordsRequestData, DeleteTopicsRequestData, DescribeClusterRequestData, DescribeConfigsRequestData, DescribeGroupsRequestData, DescribeLogDirsRequestData, DescribeProducersRequestData, DescribeTransactionsRequestData, FindCoordinatorRequestData, HeartbeatRequestData, IncrementalAlterConfigsRequestData, JoinGroupRequestData, ListPartitionReassignmentsRequestData, ListTransactionsRequestData, MetadataRequestData, OffsetCommitRequestData, ProduceRequestData, SyncGroupRequestData, WriteTxnMarkersRequestData}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.{MemoryRecords, RecordBatch, SimpleRecord}
import org.apache.kafka.common.requests.OffsetFetchResponse.PartitionData
import org.apache.kafka.common.requests._
import org.apache.kafka.common.resource.PatternType.{LITERAL, PREFIXED}
import org.apache.kafka.common.resource.ResourceType._
import org.apache.kafka.common.resource.{PatternType, Resource, ResourcePattern, ResourcePatternFilter, ResourceType}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.{ElectionType, IsolationLevel, KafkaException, TopicPartition, Uuid, requests}
import org.apache.kafka.test.{TestUtils => JTestUtils}
import org.apache.kafka.security.authorizer.AclEntry
import org.apache.kafka.security.authorizer.AclEntry.WILDCARD_HOST
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, MethodSource, ValueSource}

import java.util.Collections.singletonList
import org.apache.kafka.common.message.MetadataRequestData.MetadataRequestTopic
import org.apache.kafka.common.message.WriteTxnMarkersRequestData.{WritableTxnMarker, WritableTxnMarkerTopic}
import org.apache.kafka.coordinator.group.GroupConfig
import org.junit.jupiter.api.function.Executable

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class AuthorizerIntegrationTest extends AbstractAuthorizerIntegrationTest {
  val groupReadAcl = Map(groupResource -> Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)))
  val groupDescribeAcl = Map(groupResource -> Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)))
  val groupDeleteAcl = Map(groupResource -> Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DELETE, ALLOW)))
  val groupDescribeConfigsAcl = Map(groupResource -> Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE_CONFIGS, ALLOW)))
  val groupAlterConfigsAcl = Map(groupResource -> Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALTER_CONFIGS, ALLOW)))
  val clusterAcl = Map(clusterResource -> Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, CLUSTER_ACTION, ALLOW)))
  val clusterCreateAcl = Map(clusterResource -> Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, CREATE, ALLOW)))
  val clusterAlterAcl = Map(clusterResource -> Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALTER, ALLOW)))
  val clusterDescribeAcl = Map(clusterResource -> Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)))
  val clusterAlterConfigsAcl = Map(clusterResource -> Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALTER_CONFIGS, ALLOW)))
  val clusterIdempotentWriteAcl = Map(clusterResource -> Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, IDEMPOTENT_WRITE, ALLOW)))
  val topicCreateAcl = Map(topicResource -> Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, CREATE, ALLOW)))
  val topicReadAcl = Map(topicResource -> Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)))
  val topicWriteAcl = Map(topicResource -> Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)))
  val topicDescribeAcl = Map(topicResource -> Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)))
  val topicAlterAcl = Map(topicResource -> Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALTER, ALLOW)))
  val topicDeleteAcl = Map(topicResource -> Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DELETE, ALLOW)))
  val topicDescribeConfigsAcl = Map(topicResource -> Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE_CONFIGS, ALLOW)))
  val topicAlterConfigsAcl = Map(topicResource -> Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALTER_CONFIGS, ALLOW)))
  val transactionIdWriteAcl = Map(transactionalIdResource -> Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)))
  val transactionalIdDescribeAcl = Map(transactionalIdResource -> Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)))

  val numRecords = 1

  val requestKeyToError = (topicNames: Map[Uuid, String], version: Short) => Map[ApiKeys, Nothing => Errors](
    ApiKeys.METADATA -> ((resp: requests.MetadataResponse) => resp.errors.asScala.find(_._1 == topic).getOrElse(("test", Errors.NONE))._2),
    ApiKeys.PRODUCE -> ((resp: requests.ProduceResponse) => {
      Errors.forCode(
        resp.data
          .responses.find(topic)
          .partitionResponses.asScala.find(_.index == part).get
          .errorCode
      )
    }),
    // We may need to get the top level error if the topic does not exist in the response
    ApiKeys.FETCH -> ((resp: requests.FetchResponse) => Errors.forCode(resp.responseData(topicNames.asJava, version).asScala.find {
      case (topicPartition, _) => topicPartition == tp}.map { case (_, data) => data.errorCode }.getOrElse(resp.error.code()))),
    ApiKeys.LIST_OFFSETS -> ((resp: ListOffsetsResponse) => {
      Errors.forCode(
        resp.data
          .topics.asScala.find(_.name == topic).get
          .partitions.asScala.find(_.partitionIndex == part).get
          .errorCode
      )
    }),
    ApiKeys.OFFSET_COMMIT -> ((resp: requests.OffsetCommitResponse) => Errors.forCode(
      resp.data.topics().get(0).partitions().get(0).errorCode)),
    ApiKeys.OFFSET_FETCH -> ((resp: requests.OffsetFetchResponse) => resp.groupLevelError(group)),
    ApiKeys.FIND_COORDINATOR -> ((resp: FindCoordinatorResponse) => {
      Errors.forCode(resp.data.coordinators.asScala.find(g => group == g.key).head.errorCode)
    }),
    ApiKeys.UPDATE_METADATA -> ((resp: requests.UpdateMetadataResponse) => resp.error),
    ApiKeys.JOIN_GROUP -> ((resp: JoinGroupResponse) => resp.error),
    ApiKeys.SYNC_GROUP -> ((resp: SyncGroupResponse) => Errors.forCode(resp.data.errorCode)),
    ApiKeys.DESCRIBE_GROUPS -> ((resp: DescribeGroupsResponse) => {
      Errors.forCode(resp.data.groups.asScala.find(g => group == g.groupId).head.errorCode)
    }),
    ApiKeys.HEARTBEAT -> ((resp: HeartbeatResponse) => resp.error),
    ApiKeys.LEAVE_GROUP -> ((resp: LeaveGroupResponse) => resp.error),
    ApiKeys.DELETE_GROUPS -> ((resp: DeleteGroupsResponse) => resp.get(group)),
    ApiKeys.LEADER_AND_ISR -> ((resp: requests.LeaderAndIsrResponse) => Errors.forCode(
      resp.topics.asScala.find(t => topicNames(t.topicId) == tp.topic).get.partitionErrors.asScala.find(
        p => p.partitionIndex == tp.partition).get.errorCode)),
    ApiKeys.STOP_REPLICA -> ((resp: requests.StopReplicaResponse) => Errors.forCode(
      resp.partitionErrors.asScala.find(pe => pe.topicName == tp.topic && pe.partitionIndex == tp.partition).get.errorCode)),
    ApiKeys.CONTROLLED_SHUTDOWN -> ((resp: requests.ControlledShutdownResponse) => resp.error),
    ApiKeys.CREATE_TOPICS -> ((resp: CreateTopicsResponse) => Errors.forCode(resp.data.topics.find(topic).errorCode)),
    ApiKeys.DELETE_TOPICS -> ((resp: requests.DeleteTopicsResponse) => Errors.forCode(resp.data.responses.find(topic).errorCode)),
    ApiKeys.DELETE_RECORDS -> ((resp: requests.DeleteRecordsResponse) => Errors.forCode(
      resp.data.topics.find(tp.topic).partitions.find(tp.partition).errorCode)),
    ApiKeys.OFFSET_FOR_LEADER_EPOCH -> ((resp: OffsetsForLeaderEpochResponse) => Errors.forCode(
      resp.data.topics.find(tp.topic).partitions.asScala.find(_.partition == tp.partition).get.errorCode)),
    ApiKeys.DESCRIBE_CONFIGS -> ((resp: DescribeConfigsResponse) => {
      val resourceError = resp.resultMap.get(new ConfigResource(ConfigResource.Type.TOPIC, tp.topic))
      if (resourceError == null)
        Errors.forCode(resp.resultMap.get(new ConfigResource(ConfigResource.Type.GROUP, group)).errorCode)
      else
        Errors.forCode(resourceError.errorCode)
    }),
    ApiKeys.ALTER_CONFIGS -> ((resp: AlterConfigsResponse) =>
      resp.errors.get(new ConfigResource(ConfigResource.Type.TOPIC, tp.topic)).error),
    ApiKeys.INIT_PRODUCER_ID -> ((resp: InitProducerIdResponse) => resp.error),
    ApiKeys.WRITE_TXN_MARKERS -> ((resp: WriteTxnMarkersResponse) => resp.errorsByProducerId.get(producerId).get(tp)),
    ApiKeys.ADD_PARTITIONS_TO_TXN -> ((resp: AddPartitionsToTxnResponse) => resp.errors.get(AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID).get(tp)),
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
      Errors.forCode(if (resp.data.results.size > 0) resp.data.results.get(0).errorCode else resp.data.errorCode)),
    ApiKeys.CREATE_PARTITIONS -> ((resp: CreatePartitionsResponse) => Errors.forCode(resp.data.results.asScala.head.errorCode)),
    ApiKeys.ELECT_LEADERS -> ((resp: ElectLeadersResponse) => Errors.forCode(resp.data.errorCode)),
    ApiKeys.INCREMENTAL_ALTER_CONFIGS -> ((resp: IncrementalAlterConfigsResponse) => {
      var resourceError = IncrementalAlterConfigsResponse.fromResponseData(resp.data).get(new ConfigResource(ConfigResource.Type.TOPIC, tp.topic))
      if (resourceError == null) {
        resourceError = IncrementalAlterConfigsResponse.fromResponseData(resp.data).get(new ConfigResource(ConfigResource.Type.BROKER_LOGGER, brokerId.toString))
        if (resourceError == null)
          IncrementalAlterConfigsResponse.fromResponseData(resp.data).get(new ConfigResource(ConfigResource.Type.GROUP, group)).error
        else
          resourceError.error
      } else
        resourceError.error
    }),
    ApiKeys.ALTER_PARTITION_REASSIGNMENTS -> ((resp: AlterPartitionReassignmentsResponse) => Errors.forCode(resp.data.errorCode)),
    ApiKeys.LIST_PARTITION_REASSIGNMENTS -> ((resp: ListPartitionReassignmentsResponse) => Errors.forCode(resp.data.errorCode)),
    ApiKeys.OFFSET_DELETE -> ((resp: OffsetDeleteResponse) => {
      Errors.forCode(
        resp.data
          .topics.asScala.find(_.name == topic).get
          .partitions.asScala.find(_.partitionIndex == part).get
          .errorCode
      )
    }),
    ApiKeys.DESCRIBE_PRODUCERS -> ((resp: DescribeProducersResponse) => {
      Errors.forCode(
        resp.data
          .topics.asScala.find(_.name == topic).get
          .partitions.asScala.find(_.partitionIndex == part).get
          .errorCode
      )
    }),
    ApiKeys.DESCRIBE_TRANSACTIONS -> ((resp: DescribeTransactionsResponse) => {
      Errors.forCode(
        resp.data
          .transactionStates.asScala.find(_.transactionalId == transactionalId).get
          .errorCode
      )
    }),
    ApiKeys.CONSUMER_GROUP_HEARTBEAT -> ((resp: ConsumerGroupHeartbeatResponse) => Errors.forCode(resp.data.errorCode)),
    ApiKeys.CONSUMER_GROUP_DESCRIBE -> ((resp: ConsumerGroupDescribeResponse) =>
      Errors.forCode(resp.data.groups.asScala.find(g => group == g.groupId).head.errorCode))
  )

  def findErrorForTopicId(id: Uuid, response: AbstractResponse): Errors = {
    response match {
      case res: DeleteTopicsResponse =>
        Errors.forCode(res.data.responses.asScala.find(_.topicId == id).get.errorCode)
      case _ =>
        fail(s"Unexpected response type $response")
    }
  }

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
    ApiKeys.WRITE_TXN_MARKERS -> (clusterAcl ++ clusterAlterAcl),
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
    ApiKeys.OFFSET_DELETE -> groupReadAcl,
    ApiKeys.DESCRIBE_PRODUCERS -> topicReadAcl,
    ApiKeys.DESCRIBE_TRANSACTIONS -> transactionalIdDescribeAcl,
    ApiKeys.CONSUMER_GROUP_HEARTBEAT -> groupReadAcl,
    ApiKeys.CONSUMER_GROUP_DESCRIBE -> groupDescribeAcl
  )

  private def createMetadataRequest(allowAutoTopicCreation: Boolean) = {
    new requests.MetadataRequest.Builder(List(topic).asJava, allowAutoTopicCreation).build()
  }

  private def createProduceRequest =
    requests.ProduceRequest.forCurrentMagic(new ProduceRequestData()
      .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
        Collections.singletonList(new ProduceRequestData.TopicProduceData()
          .setName(tp.topic).setPartitionData(Collections.singletonList(
          new ProduceRequestData.PartitionProduceData()
            .setIndex(tp.partition)
            .setRecords(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("test".getBytes))))))
        .iterator))
      .setAcks(1.toShort)
      .setTimeoutMs(5000))
      .build()

  private def createFetchRequest = {
    val partitionMap = new util.LinkedHashMap[TopicPartition, requests.FetchRequest.PartitionData]
    partitionMap.put(tp, new requests.FetchRequest.PartitionData(getTopicIds().getOrElse(tp.topic, Uuid.ZERO_UUID),
      0, 0, 100, Optional.of(27)))
    requests.FetchRequest.Builder.forConsumer(ApiKeys.FETCH.latestVersion, 100, Int.MaxValue, partitionMap).build()
  }

  private def createFetchRequestWithUnknownTopic(id: Uuid, version: Short) = {
    val partitionMap = new util.LinkedHashMap[TopicPartition, requests.FetchRequest.PartitionData]
    partitionMap.put(tp,
      new requests.FetchRequest.PartitionData(id, 0, 0, 100, Optional.of(27)))
    requests.FetchRequest.Builder.forConsumer(version, 100, Int.MaxValue, partitionMap).build()
  }

  private def createFetchFollowerRequest = {
    val partitionMap = new util.LinkedHashMap[TopicPartition, requests.FetchRequest.PartitionData]
    partitionMap.put(tp, new requests.FetchRequest.PartitionData(getTopicIds().getOrElse(tp.topic, Uuid.ZERO_UUID),
      0, 0, 100, Optional.of(27)))
    val version = ApiKeys.FETCH.latestVersion
    requests.FetchRequest.Builder.forReplica(version, 5000, -1, 100, Int.MaxValue, partitionMap).build()
  }

  private def createListOffsetsRequest = {
    requests.ListOffsetsRequest.Builder.forConsumer(false, IsolationLevel.READ_UNCOMMITTED)
      .setTargetTimes(List(new ListOffsetsTopic()
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
      .setTopic(tp.topic)
      .setPartitions(List(new OffsetForLeaderPartition()
          .setPartition(tp.partition)
          .setLeaderEpoch(7)
          .setCurrentLeaderEpoch(27)).asJava))
    OffsetsForLeaderEpochRequest.Builder.forConsumer(epochs).build()
  }

  private def createOffsetFetchRequest: OffsetFetchRequest = {
    new requests.OffsetFetchRequest.Builder(group, false, List(tp).asJava, false).build()
  }

  private def createOffsetFetchRequestAllPartitions: OffsetFetchRequest = {
    new requests.OffsetFetchRequest.Builder(group, false, null, false).build()
  }

  private def createOffsetFetchRequest(groupToPartitionMap: util.Map[String, util.List[TopicPartition]]): OffsetFetchRequest = {
    new requests.OffsetFetchRequest.Builder(groupToPartitionMap, false, false).build()
  }

  private def createFindCoordinatorRequest = {
    new FindCoordinatorRequest.Builder(
        new FindCoordinatorRequestData()
          .setKeyType(FindCoordinatorRequest.CoordinatorType.GROUP.id)
          .setCoordinatorKeys(Collections.singletonList(group))).build()
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
          .setGenerationIdOrMemberEpoch(1)
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

  private def deleteTopicsWithIdsRequest(topicId: Uuid): DeleteTopicsRequest = {
    new DeleteTopicsRequest.Builder(
      new DeleteTopicsRequestData()
        .setTopics(Collections.singletonList(
          new DeleteTopicsRequestData.DeleteTopicState()
            .setTopicId(topicId)))
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
          new AlterConfigsRequest.ConfigEntry(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "1000000")
        ))), true).build()

  private def incrementalAlterConfigsRequest = {
    val data = new IncrementalAlterConfigsRequestData
    val alterableConfig = new AlterableConfig
    alterableConfig.setName(TopicConfig.MAX_MESSAGE_BYTES_CONFIG).
      setValue("1000000").setConfigOperation(AlterConfigOp.OpType.SET.id())
    val alterableConfigSet = new AlterableConfigCollection
    alterableConfigSet.add(alterableConfig)
    data.resources().add(new AlterConfigsResource().
      setResourceName(tp.topic).setResourceType(ConfigResource.Type.TOPIC.id()).
      setConfigs(alterableConfigSet))
    new IncrementalAlterConfigsRequest.Builder(data).build()
  }

  private def incrementalAlterGroupConfigsRequest = {
    val data = new IncrementalAlterConfigsRequestData
    val alterableConfig = new AlterableConfig().setName(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG).
      setValue("50000").setConfigOperation(AlterConfigOp.OpType.SET.id())
    val alterableConfigSet = new AlterableConfigCollection
    alterableConfigSet.add(alterableConfig)
    data.resources().add(new AlterConfigsResource().
      setResourceName(group).setResourceType(ConfigResource.Type.GROUP.id()).
      setConfigs(alterableConfigSet))
    new IncrementalAlterConfigsRequest.Builder(data).build()
  }

  private def describeGroupConfigsRequest = {
    new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData().setResources(Collections.singletonList(
      new DescribeConfigsRequestData.DescribeConfigsResource().setResourceType(ConfigResource.Type.GROUP.id)
        .setResourceName(group)))).build()
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
    val data = new AlterReplicaLogDirsRequestData()
    data.dirs.add(dir)
    new AlterReplicaLogDirsRequest.Builder(data).build()
  }

  private def describeLogDirsRequest = new DescribeLogDirsRequest.Builder(new DescribeLogDirsRequestData().setTopics(new DescribeLogDirsRequestData.DescribableLogDirTopicCollection(Collections.singleton(
    new DescribeLogDirsRequestData.DescribableLogDirTopic().setTopic(tp.topic).setPartitions(Collections.singletonList(tp.partition))).iterator()))).build()

  private def addPartitionsToTxnRequest = AddPartitionsToTxnRequest.Builder.forClient(transactionalId, 1, 1, Collections.singletonList(tp)).build()

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

  private def describeProducersRequest: DescribeProducersRequest = new DescribeProducersRequest.Builder(
    new DescribeProducersRequestData()
      .setTopics(List(
        new DescribeProducersRequestData.TopicRequest()
          .setName(tp.topic)
          .setPartitionIndexes(List(Int.box(tp.partition)).asJava)
      ).asJava)
  ).build()

  private def describeTransactionsRequest: DescribeTransactionsRequest = new DescribeTransactionsRequest.Builder(
    new DescribeTransactionsRequestData().setTransactionalIds(List(transactionalId).asJava)
  ).build()

  private def alterPartitionReassignmentsRequest = new AlterPartitionReassignmentsRequest.Builder(
    new AlterPartitionReassignmentsRequestData().setTopics(
      List(new AlterPartitionReassignmentsRequestData.ReassignableTopic()
        .setName(topic)
        .setPartitions(
          List(new AlterPartitionReassignmentsRequestData.ReassignablePartition().setPartitionIndex(tp.partition)).asJava
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

  private def writeTxnMarkersRequest: WriteTxnMarkersRequest = new WriteTxnMarkersRequest.Builder(
    new WriteTxnMarkersRequestData()
      .setMarkers(
        List(new WritableTxnMarker()
          .setProducerId(producerId)
          .setProducerEpoch(1)
          .setTransactionResult(false)
          .setTopics(List(new WritableTxnMarkerTopic()
            .setName(tp.topic())
            .setPartitionIndexes(List(Integer.valueOf(tp.partition())).asJava)
          ).asJava)
          .setCoordinatorEpoch(1)
        ).asJava
      )
  ).build()

  private def consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
    new ConsumerGroupHeartbeatRequestData()
      .setGroupId(group)
      .setMemberEpoch(0)).build()

  private def consumerGroupDescribeRequest = new ConsumerGroupDescribeRequest.Builder(
    new ConsumerGroupDescribeRequestData()
      .setGroupIds(List(group).asJava)
      .setIncludeAuthorizedOperations(false)).build()

  private def sendRequests(requestKeyToRequest: mutable.Map[ApiKeys, AbstractRequest], topicExists: Boolean = true,
                           topicNames: Map[Uuid, String] = getTopicNames()) = {
    for ((key, request) <- requestKeyToRequest) {
      removeAllClientAcls()
      val resources = requestKeysToAcls(key).map(_._1.resourceType).toSet
      sendRequestAndVerifyResponseError(request, resources, isAuthorized = false, topicExists = topicExists, topicNames = topicNames)

      val resourceToAcls = requestKeysToAcls(key)
      resourceToAcls.get(topicResource).foreach { acls =>
        val describeAcls = topicDescribeAcl(topicResource)
        val isAuthorized = if (key.equals(ApiKeys.DELETE_TOPICS) && !topicExists) {
          // In KRaft mode, trying to delete a topic that doesn't exist but that you do have
          // describe permission for will give UNKNOWN_TOPIC_OR_PARTITION.
          true
        } else {
          describeAcls == acls
        }
        addAndVerifyAcls(describeAcls, topicResource)
        sendRequestAndVerifyResponseError(request, resources, isAuthorized = isAuthorized, topicExists = topicExists, topicNames = topicNames)
        removeAllClientAcls()
      }

      for ((resource, acls) <- resourceToAcls)
        addAndVerifyAcls(acls, resource)
      sendRequestAndVerifyResponseError(request, resources, isAuthorized = true,  topicExists = topicExists, topicNames = topicNames)
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testAuthorizationWithTopicExisting(quorum: String): Unit = {
    //First create the topic so we have a valid topic ID
    sendRequests(mutable.Map(ApiKeys.CREATE_TOPICS -> createTopicsRequest))

    val requestKeyToRequest = mutable.LinkedHashMap[ApiKeys, AbstractRequest](
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
      ApiKeys.DESCRIBE_PRODUCERS -> describeProducersRequest,
      ApiKeys.DESCRIBE_TRANSACTIONS -> describeTransactionsRequest,
      ApiKeys.WRITE_TXN_MARKERS -> writeTxnMarkersRequest,
      ApiKeys.CONSUMER_GROUP_HEARTBEAT -> consumerGroupHeartbeatRequest,
      ApiKeys.CONSUMER_GROUP_DESCRIBE -> consumerGroupDescribeRequest,
      // Delete the topic last
      ApiKeys.DELETE_TOPICS -> deleteTopicsRequest
    )

    sendRequests(requestKeyToRequest, true)
  }

  /*
   * even if the topic doesn't exist, request APIs should not leak the topic name
   */
  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testAuthorizationWithTopicNotExisting(quorum: String): Unit = {
    val id = Uuid.randomUuid()
    val topicNames = Map(id -> "topic")
    val requestKeyToRequest = mutable.LinkedHashMap[ApiKeys, AbstractRequest](
      ApiKeys.METADATA -> createMetadataRequest(allowAutoTopicCreation = false),
      ApiKeys.PRODUCE -> createProduceRequest,
      ApiKeys.FETCH -> createFetchRequestWithUnknownTopic(id, ApiKeys.FETCH.latestVersion()),
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

    sendRequests(requestKeyToRequest, false, topicNames)
  }

  @ParameterizedTest
  @CsvSource(value = Array("kraft,false", "kraft,true"))
  def testTopicIdAuthorization(quorum: String, withTopicExisting: Boolean): Unit = {
    val topicId = if (withTopicExisting) {
      createTopicWithBrokerPrincipal(topic)
      getTopicIds()(topic)
    } else {
      Uuid.randomUuid()
    }

    val requestKeyToRequest = mutable.LinkedHashMap[ApiKeys, AbstractRequest](
      ApiKeys.DELETE_TOPICS -> deleteTopicsWithIdsRequest(topicId)
    )

    def sendAndVerify(
      request: AbstractRequest,
      isAuthorized: Boolean,
      isDescribeAuthorized: Boolean
    ): Unit = {
      val response = connectAndReceive[AbstractResponse](request)
      val error = findErrorForTopicId(topicId, response)
      if (!withTopicExisting) {
        assertEquals(Errors.UNKNOWN_TOPIC_ID, error)
      } else if (!isDescribeAuthorized || !isAuthorized) {
        assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED, error)
      }
    }

    for ((key, request) <- requestKeyToRequest) {
      removeAllClientAcls()
      sendAndVerify(request, isAuthorized = false, isDescribeAuthorized = false)

      val describeAcls = topicDescribeAcl(topicResource)
      addAndVerifyAcls(describeAcls, topicResource)

      val resourceToAcls = requestKeysToAcls(key)
      resourceToAcls.get(topicResource).foreach { acls =>
        val isAuthorized = describeAcls == acls
        sendAndVerify(request, isAuthorized = isAuthorized, isDescribeAuthorized = true)
      }

      removeAllClientAcls()
      for ((resource, acls) <- resourceToAcls) {
        addAndVerifyAcls(acls, resource)
      }

      sendAndVerify(request, isAuthorized = true, isDescribeAuthorized = true)
    }
  }

  /*
   * even if the topic doesn't exist, request APIs should not leak the topic name
   */
  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testAuthorizationFetchV12WithTopicNotExisting(quorum: String): Unit = {
    val id = Uuid.ZERO_UUID
    val topicNames = Map(id -> "topic")
    val requestKeyToRequest = mutable.LinkedHashMap[ApiKeys, AbstractRequest](
      ApiKeys.FETCH -> createFetchRequestWithUnknownTopic(id, 12),
    )

    sendRequests(requestKeyToRequest, false, topicNames)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testCreateTopicAuthorizationWithClusterCreate(quorum: String): Unit = {
    removeAllClientAcls()
    val resources = Set[ResourceType](TOPIC)

    sendRequestAndVerifyResponseError(createTopicsRequest, resources, isAuthorized = false)

    for ((resource, acls) <- clusterCreateAcl)
      addAndVerifyAcls(acls, resource)
    sendRequestAndVerifyResponseError(createTopicsRequest, resources, isAuthorized = true)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testFetchFollowerRequest(quorum: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

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

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testIncrementalAlterConfigsRequestRequiresClusterPermissionForBrokerLogger(quorum: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

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

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testOffsetsForLeaderEpochClusterPermission(quorum: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

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

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testProduceWithNoTopicAccess(quorum: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    val producer = createProducer()
    assertThrows(classOf[TopicAuthorizationException], () => sendRecords(producer, numRecords, tp))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testProduceWithTopicDescribe(quorum: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), topicResource)
    val producer = createProducer()
    assertThrows(classOf[TopicAuthorizationException], () => sendRecords(producer, numRecords, tp))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testProduceWithTopicRead(quorum: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)
    val producer = createProducer()
    assertThrows(classOf[TopicAuthorizationException], () => sendRecords(producer, numRecords, tp))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testProduceWithTopicWrite(quorum: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, numRecords, tp)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testCreatePermissionOnTopicToWriteToNonExistentTopic(quorum: String): Unit = {
    testCreatePermissionNeededToWriteToNonExistentTopic(TOPIC)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testCreatePermissionOnClusterToWriteToNonExistentTopic(quorum: String): Unit = {
    testCreatePermissionNeededToWriteToNonExistentTopic(CLUSTER)
  }

  private def testCreatePermissionNeededToWriteToNonExistentTopic(resType: ResourceType): Unit = {
    val newTopicResource = new ResourcePattern(TOPIC, topic, LITERAL)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), newTopicResource)
    val producer = createProducer()
    val e = assertThrows(classOf[TopicAuthorizationException], () => sendRecords(producer, numRecords, tp))
    assertEquals(Collections.singleton(tp.topic), e.unauthorizedTopics())

    val resource = if (resType == ResourceType.TOPIC) newTopicResource else clusterResource
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, CREATE, ALLOW)), resource)

    sendRecords(producer, numRecords, tp)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testConsumeUsingAssignWithNoAccess(quorum: String, groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllClientAcls()

    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    assertThrows(classOf[TopicAuthorizationException], () => consumeRecords(consumer))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testSimpleConsumeWithOffsetLookupAndNoGroupAccess(quorum: String, groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllClientAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)

    // note this still depends on group access because we haven't set offsets explicitly, which means
    // they will first be fetched from the consumer coordinator (which requires group access)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    val e = assertThrows(classOf[GroupAuthorizationException], () => consumeRecords(consumer))
    assertEquals(group, e.groupId())
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testSimpleConsumeWithExplicitSeekAndNoGroupAccess(quorum: String, groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllClientAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)

    // in this case, we do an explicit seek, so there should be no need to query the coordinator at all
    // remove the group.id config to avoid coordinator created
    val consumer = createConsumer(configsToRemove = List(ConsumerConfig.GROUP_ID_CONFIG))
    consumer.assign(List(tp).asJava)
    consumer.seekToBeginning(List(tp).asJava)
    consumeRecords(consumer)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testConsumeWithoutTopicDescribeAccess(quorum: String, groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllClientAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)

    val e = assertThrows(classOf[TopicAuthorizationException], () => consumeRecords(consumer))
    assertEquals(Collections.singleton(topic), e.unauthorizedTopics())
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testConsumeWithTopicDescribe(quorum: String, groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllClientAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)

    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    val e = assertThrows(classOf[TopicAuthorizationException], () => consumeRecords(consumer))
    assertEquals(Collections.singleton(topic), e.unauthorizedTopics())
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testConsumeWithTopicWrite(quorum: String, groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllClientAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)

    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    val e = assertThrows(classOf[TopicAuthorizationException], () => consumeRecords(consumer))
    assertEquals(Collections.singleton(topic), e.unauthorizedTopics())
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testConsumeWithTopicAndGroupRead(quorum: String, groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllClientAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)

    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumeRecords(consumer)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersClassicGroupProtocolOnly_KAFKA_17696"))
  def testPatternSubscriptionWithNoTopicAccess(quorum: String, groupProtocol: String): Unit = {
    val assignSemaphore = new Semaphore(0)
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllClientAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)

    val consumer = createConsumer()
    consumer.subscribe(Pattern.compile(topicPattern), new ConsumerRebalanceListener {
      def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
        assignSemaphore.release()
      }
      def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
      }})
    TestUtils.waitUntilTrue(() => {
      consumer.poll(Duration.ofMillis(500))
      assignSemaphore.tryAcquire()
    }, "Assignment did not complete on time")
    assertTrue(consumer.subscription.isEmpty)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testPatternSubscriptionWithTopicDescribeOnlyAndGroupRead(quorum: String, groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllClientAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)
    val consumer = createConsumer()
    consumer.subscribe(Pattern.compile(topicPattern))
    val e = assertThrows(classOf[TopicAuthorizationException], () => consumeRecords(consumer))
    assertEquals(Collections.singleton(topic), e.unauthorizedTopics())
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testPatternSubscriptionWithTopicAndGroupRead(quorum: String, groupProtocol: String): Unit = {
    val assignSemaphore = new Semaphore(0)
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)

    // create an unmatched topic
    val unmatchedTopic = "unmatched"
    createTopicWithBrokerPrincipal(unmatchedTopic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)),  new ResourcePattern(TOPIC, unmatchedTopic, LITERAL))
    sendRecords(producer, 1, new TopicPartition(unmatchedTopic, part))
    removeAllClientAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)
    val consumer = createConsumer()
    consumer.subscribe(Pattern.compile(topicPattern))
    consumeRecords(consumer)

    // set the subscription pattern to an internal topic that the consumer has read permission to. Since
    // internal topics are not included, we should not be assigned any partitions from this topic
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)),  new ResourcePattern(TOPIC,
      GROUP_METADATA_TOPIC_NAME, LITERAL))
    consumer.subscribe(Pattern.compile(GROUP_METADATA_TOPIC_NAME), new ConsumerRebalanceListener {
      def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
        assignSemaphore.release()
      }
      def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
      }})
    TestUtils.waitUntilTrue(() => {
      consumer.poll(Duration.ofMillis(500))
      assignSemaphore.tryAcquire()
    }, "Assignment did not complete on time")
    assertTrue(consumer.subscription().isEmpty)
    assertTrue(consumer.assignment().isEmpty)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testPatternSubscriptionMatchingInternalTopic(quorum: String, groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllClientAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)

    consumerConfig.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false")
    val consumer = createConsumer()
    // ensure that internal topics are not included if no permission
    consumer.subscribe(Pattern.compile(".*"))
    consumeRecords(consumer)
    assertEquals(Set(topic).asJava, consumer.subscription)

    // now authorize the user for the internal topic and verify that we can subscribe
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), new ResourcePattern(TOPIC,
      GROUP_METADATA_TOPIC_NAME, LITERAL))
    consumer.subscribe(Pattern.compile(GROUP_METADATA_TOPIC_NAME))
    TestUtils.retry(60000) {
      consumer.poll(Duration.ofMillis(500))
      assertEquals(Set(GROUP_METADATA_TOPIC_NAME), consumer.subscription.asScala)
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testPatternSubscriptionMatchingInternalTopicWithDescribeOnlyPermission(quorum: String, groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllClientAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)
    val internalTopicResource = new ResourcePattern(TOPIC, GROUP_METADATA_TOPIC_NAME, LITERAL)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), internalTopicResource)

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

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testPatternSubscriptionNotMatchingInternalTopic(quorum: String, groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllClientAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)

    consumerConfig.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false")
    val consumer = createConsumer()
    consumer.subscribe(Pattern.compile(topicPattern))
    consumeRecords(consumer)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testCreatePermissionOnTopicToReadFromNonExistentTopic(quorum: String, groupProtocol: String): Unit = {
    testCreatePermissionNeededToReadFromNonExistentTopic("newTopic",
      Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, CREATE, ALLOW)),
      TOPIC)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testCreatePermissionOnClusterToReadFromNonExistentTopic(quorum: String, groupProtocol: String): Unit = {
    testCreatePermissionNeededToReadFromNonExistentTopic("newTopic",
      Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, CREATE, ALLOW)),
      CLUSTER)
  }

  private def testCreatePermissionNeededToReadFromNonExistentTopic(newTopic: String, acls: Set[AccessControlEntry], resType: ResourceType): Unit = {
    val topicPartition = new TopicPartition(newTopic, 0)
    val newTopicResource = new ResourcePattern(TOPIC, newTopic, LITERAL)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), newTopicResource)
    addAndVerifyAcls(groupReadAcl(groupResource), groupResource)
    val consumer = createConsumer()
    consumer.assign(List(topicPartition).asJava)
    val unauthorizedTopics = assertThrows(classOf[TopicAuthorizationException],
      () => (0 until 10).foreach(_ => consumer.poll(Duration.ofMillis(50L)))).unauthorizedTopics
    assertEquals(Collections.singleton(newTopic), unauthorizedTopics)

    val resource = if (resType == TOPIC) newTopicResource else clusterResource
    addAndVerifyAcls(acls, resource)

    waitUntilTrue(() => {
      consumer.poll(Duration.ofMillis(50L))
      brokers.forall { broker =>
        broker.metadataCache.getPartitionInfo(newTopic, 0) match {
          case Some(partitionState) => FetchRequest.isValidBrokerId(partitionState.leader)
          case _ => false
        }
      }
    }, "Partition metadata not propagated.")
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testCreatePermissionMetadataRequestAutoCreate(quorum: String): Unit = {
    val readAcls = topicReadAcl(topicResource)
    addAndVerifyAcls(readAcls, topicResource)
    brokers.foreach(b => assertEquals(None, b.metadataCache.getPartitionInfo(topic, 0)))

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

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersClassicGroupProtocolOnly_KAFKA_17696"))
  def testCommitWithNoAccess(quorum: String, groupProtocol: String): Unit = {
    val consumer = createConsumer()
    assertThrows(classOf[GroupAuthorizationException], () => consumer.commitSync(Map(tp -> new OffsetAndMetadata(5)).asJava))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testCommitWithNoTopicAccess(quorum: String, groupProtocol: String): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)
    val consumer = createConsumer()
    assertThrows(classOf[TopicAuthorizationException], () => consumer.commitSync(Map(tp -> new OffsetAndMetadata(5)).asJava))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testCommitWithTopicWrite(quorum: String, groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
    val consumer = createConsumer()
    assertThrows(classOf[TopicAuthorizationException], () => consumer.commitSync(Map(tp -> new OffsetAndMetadata(5)).asJava))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testCommitWithTopicDescribe(quorum: String, groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), topicResource)
    val consumer = createConsumer()
    assertThrows(classOf[TopicAuthorizationException], () => consumer.commitSync(Map(tp -> new OffsetAndMetadata(5)).asJava))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersClassicGroupProtocolOnly_KAFKA_17696"))
  def testCommitWithNoGroupAccess(quorum: String, groupProtocol: String): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)
    val consumer = createConsumer()
    assertThrows(classOf[GroupAuthorizationException], () => consumer.commitSync(Map(tp -> new OffsetAndMetadata(5)).asJava))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testCommitWithTopicAndGroupRead(quorum: String, groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.commitSync(Map(tp -> new OffsetAndMetadata(5)).asJava)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersClassicGroupProtocolOnly_KAFKA_17696"))
  def testOffsetFetchWithNoAccess(quorum: String, groupProtocol: String): Unit = {
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    assertThrows(classOf[TopicAuthorizationException], () => consumer.position(tp))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersClassicGroupProtocolOnly_KAFKA_17696"))
  def testOffsetFetchWithNoGroupAccess(quorum: String, groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    assertThrows(classOf[GroupAuthorizationException], () => consumer.position(tp))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testOffsetFetchWithNoTopicAccess(quorum: String, groupProtocol: String): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    assertThrows(classOf[TopicAuthorizationException], () => consumer.position(tp))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testOffsetFetchAllTopicPartitionsAuthorization(quorum: String, groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    val offset = 15L
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumer.commitSync(Map(tp -> new OffsetAndMetadata(offset)).asJava)

    removeAllClientAcls()
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)

    // send offset fetch requests directly since the consumer does not expose an API to do so
    // note there's only one broker, so no need to lookup the group coordinator

    // without describe permission on the topic, we shouldn't be able to fetch offsets
    val offsetFetchRequest = createOffsetFetchRequestAllPartitions
    var offsetFetchResponse = connectAndReceive[OffsetFetchResponse](offsetFetchRequest)
    assertEquals(Errors.NONE, offsetFetchResponse.groupLevelError(group))
    assertTrue(offsetFetchResponse.partitionDataMap(group).isEmpty)

    // now add describe permission on the topic and verify that the offset can be fetched
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), topicResource)
    offsetFetchResponse = connectAndReceive[OffsetFetchResponse](offsetFetchRequest)
    assertEquals(Errors.NONE, offsetFetchResponse.groupLevelError(group))
    assertTrue(offsetFetchResponse.partitionDataMap(group).containsKey(tp))
    assertEquals(offset, offsetFetchResponse.partitionDataMap(group).get(tp).offset)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testOffsetFetchMultipleGroupsAuthorization(quorum: String, groupProtocol: String): Unit = {
    val groups: Seq[String] = (1 to 5).map(i => s"group$i")
    val groupResources = groups.map(group => new ResourcePattern(GROUP, group, LITERAL))
    val topics: Seq[String] = (1 to 3).map(i => s"topic$i")
    val topicResources = topics.map(topic => new ResourcePattern(TOPIC, topic, LITERAL))

    val topic1List = singletonList(new TopicPartition(topics(0), 0))
    val topic1And2List = util.Arrays.asList(
      new TopicPartition(topics(0), 0),
      new TopicPartition(topics(1), 0),
      new TopicPartition(topics(1), 1))
    val allTopicsList = util.Arrays.asList(
      new TopicPartition(topics(0), 0),
      new TopicPartition(topics(1), 0),
      new TopicPartition(topics(1), 1),
      new TopicPartition(topics(2), 0),
      new TopicPartition(topics(2), 1),
      new TopicPartition(topics(2), 2))

    // create group to partition map to build batched offsetFetch request
    val groupToPartitionMap = new util.HashMap[String, util.List[TopicPartition]]()
    groupToPartitionMap.put(groups(0), topic1List)
    groupToPartitionMap.put(groups(1), topic1And2List)
    groupToPartitionMap.put(groups(2), allTopicsList)
    groupToPartitionMap.put(groups(3), null)
    groupToPartitionMap.put(groups(4), null)

    createTopicWithBrokerPrincipal(topics(0))
    createTopicWithBrokerPrincipal(topics(1), numPartitions = 2)
    createTopicWithBrokerPrincipal(topics(2), numPartitions = 3)
    groupResources.foreach(r => {
      addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), r)
    })
    topicResources.foreach(t => {
      addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), t)
    })

    val offset = 15L
    val leaderEpoch: Optional[Integer] = Optional.of(1)
    val metadata = "metadata"

    def commitOffsets(tpList: util.List[TopicPartition]): Unit = {
      val consumer = createConsumer()
      consumer.assign(tpList)
      val offsets = tpList.asScala.map{
        tp => (tp, new OffsetAndMetadata(offset, leaderEpoch, metadata))
      }.toMap.asJava
      consumer.commitSync(offsets)
      consumer.close()
    }

    // create 5 consumers to commit offsets so we can fetch them later
    val partitionMap = groupToPartitionMap.asScala.map(e => (e._1, Option(e._2).getOrElse(allTopicsList)))
    groups.foreach { groupId =>
      consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
      commitOffsets(partitionMap(groupId))
    }

    removeAllClientAcls()

    def verifyPartitionData(partitionData: OffsetFetchResponse.PartitionData): Unit = {
      assertTrue(!partitionData.hasError)
      assertEquals(offset, partitionData.offset)
      assertEquals(metadata, partitionData.metadata)
      assertEquals(leaderEpoch.get(), partitionData.leaderEpoch.get())
    }

    def verifyResponse(groupLevelResponse: Errors,
                       partitionData: util.Map[TopicPartition, PartitionData],
                       topicList: util.List[TopicPartition]): Unit = {
      assertEquals(Errors.NONE, groupLevelResponse)
      assertTrue(partitionData.size() == topicList.size())
      topicList.forEach(t => verifyPartitionData(partitionData.get(t)))
    }

    // test handling partial errors, where one group is fully authorized, some groups don't have
    // the right topic authorizations, and some groups have no authorization
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResources(0))
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResources(1))
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResources(3))
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), topicResources(0))
    val offsetFetchRequest = createOffsetFetchRequest(groupToPartitionMap)
    var offsetFetchResponse = connectAndReceive[OffsetFetchResponse](offsetFetchRequest)
    offsetFetchResponse.data().groups().forEach(g =>
      g.groupId() match {
        case "group1" =>
          verifyResponse(offsetFetchResponse.groupLevelError(groups(0)), offsetFetchResponse
            .partitionDataMap(groups(0)), topic1List)
        case "group2" =>
          assertEquals(Errors.NONE, offsetFetchResponse.groupLevelError(groups(1)))
          val group2Response = offsetFetchResponse.partitionDataMap(groups(1))
          assertTrue(group2Response.size() == 3)
          assertTrue(group2Response.keySet().containsAll(topic1And2List))
          verifyPartitionData(group2Response.get(topic1And2List.get(0)))
          assertTrue(group2Response.get(topic1And2List.get(1)).hasError)
          assertTrue(group2Response.get(topic1And2List.get(2)).hasError)
          assertEquals(OffsetFetchResponse.UNAUTHORIZED_PARTITION, group2Response.get(topic1And2List.get(1)))
          assertEquals(OffsetFetchResponse.UNAUTHORIZED_PARTITION, group2Response.get(topic1And2List.get(2)))
        case "group3" =>
          assertEquals(Errors.GROUP_AUTHORIZATION_FAILED, offsetFetchResponse.groupLevelError(groups(2)))
          assertTrue(offsetFetchResponse.partitionDataMap(groups(2)).size() == 0)
        case "group4" =>
          verifyResponse(offsetFetchResponse.groupLevelError(groups(3)), offsetFetchResponse
            .partitionDataMap(groups(3)), topic1List)
        case "group5" =>
          assertEquals(Errors.GROUP_AUTHORIZATION_FAILED, offsetFetchResponse.groupLevelError(groups(4)))
          assertTrue(offsetFetchResponse.partitionDataMap(groups(4)).size() == 0)
      })

    // test that after adding some of the ACLs, we get no group level authorization errors, but
    // still get topic level authorization errors for topics we don't have ACLs for
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResources(2))
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResources(4))
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), topicResources(1))
    offsetFetchResponse = connectAndReceive[OffsetFetchResponse](offsetFetchRequest)
    offsetFetchResponse.data().groups().forEach(g =>
      g.groupId() match {
        case "group1" =>
          verifyResponse(offsetFetchResponse.groupLevelError(groups(0)), offsetFetchResponse
            .partitionDataMap(groups(0)), topic1List)
        case "group2" =>
          verifyResponse(offsetFetchResponse.groupLevelError(groups(1)), offsetFetchResponse
            .partitionDataMap(groups(1)), topic1And2List)
        case "group3" =>
          assertEquals(Errors.NONE, offsetFetchResponse.groupLevelError(groups(2)))
          val group3Response = offsetFetchResponse.partitionDataMap(groups(2))
          assertTrue(group3Response.size() == 6)
          assertTrue(group3Response.keySet().containsAll(allTopicsList))
          verifyPartitionData(group3Response.get(allTopicsList.get(0)))
          verifyPartitionData(group3Response.get(allTopicsList.get(1)))
          verifyPartitionData(group3Response.get(allTopicsList.get(2)))
          assertTrue(group3Response.get(allTopicsList.get(3)).hasError)
          assertTrue(group3Response.get(allTopicsList.get(4)).hasError)
          assertTrue(group3Response.get(allTopicsList.get(5)).hasError)
          assertEquals(OffsetFetchResponse.UNAUTHORIZED_PARTITION, group3Response.get(allTopicsList.get(3)))
          assertEquals(OffsetFetchResponse.UNAUTHORIZED_PARTITION, group3Response.get(allTopicsList.get(4)))
          assertEquals(OffsetFetchResponse.UNAUTHORIZED_PARTITION, group3Response.get(allTopicsList.get(5)))
        case "group4" =>
          verifyResponse(offsetFetchResponse.groupLevelError(groups(3)), offsetFetchResponse
            .partitionDataMap(groups(3)), topic1And2List)
        case "group5" =>
          verifyResponse(offsetFetchResponse.groupLevelError(groups(4)), offsetFetchResponse
            .partitionDataMap(groups(4)), topic1And2List)
      })

    // test that after adding all necessary ACLs, we get no partition level or group level errors
    // from the offsetFetch response
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), topicResources(2))
    offsetFetchResponse = connectAndReceive[OffsetFetchResponse](offsetFetchRequest)
    offsetFetchResponse.data.groups.asScala.map(_.groupId).foreach( groupId =>
      verifyResponse(offsetFetchResponse.groupLevelError(groupId), offsetFetchResponse.partitionDataMap(groupId), partitionMap(groupId))
    )
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testOffsetFetchTopicDescribe(quorum: String, groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumer.position(tp)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testOffsetFetchWithTopicAndGroupRead(quorum: String, groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumer.position(tp)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testMetadataWithNoTopicAccess(quorum: String, groupProtocol: String): Unit = {
    val consumer = createConsumer()
    assertThrows(classOf[TopicAuthorizationException], () => consumer.partitionsFor(topic))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testMetadataWithTopicDescribe(quorum: String, groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.partitionsFor(topic)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersClassicGroupProtocolOnly_KAFKA_17696"))
  def testListOffsetsWithNoTopicAccess(quorum: String, groupProtocol: String): Unit = {
    val consumer = createConsumer()
    assertThrows(classOf[TopicAuthorizationException], () => consumer.endOffsets(Set(tp).asJava))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testListOffsetsWithTopicDescribe(quorum: String, groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.endOffsets(Set(tp).asJava)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testDescribeGroupApiWithNoGroupAcl(quorum: String): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), topicResource)
    val result = createAdminClient().describeConsumerGroups(Seq(group).asJava)
    TestUtils.assertFutureExceptionTypeEquals(result.describedGroups().get(group), classOf[GroupAuthorizationException])
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testDescribeGroupApiWithGroupDescribe(quorum: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), topicResource)
    createAdminClient().describeConsumerGroups(Seq(group).asJava).describedGroups().get(group).get()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testListGroupApiWithAndWithoutListGroupAcls(quorum: String, groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    // write some record to the topic
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, numRecords = 1, tp)

    // use two consumers to write to two different groups
    val group2 = "other group"
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), new ResourcePattern(GROUP, group2, LITERAL))
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)
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
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), clusterResource)
    // it should list both groups (due to cluster describe permission)
    assertEquals(Set(group, group2), adminClient.listConsumerGroups().all().get().asScala.map(_.groupId()).toSet)

    // now replace cluster describe with group read permission
    removeAllClientAcls()
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)
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

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testDeleteGroupApiWithDeleteGroupAcl(quorum: String, groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DELETE, ALLOW)), groupResource)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumer.commitSync(Map(tp -> new OffsetAndMetadata(5, "")).asJava)
    createAdminClient().deleteConsumerGroups(Seq(group).asJava).deletedGroups().get(group).get()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testDeleteGroupApiWithNoDeleteGroupAcl(quorum: String, groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumer.commitSync(Map(tp -> new OffsetAndMetadata(5, "")).asJava)
    val result = createAdminClient().deleteConsumerGroups(Seq(group).asJava)
    TestUtils.assertFutureExceptionTypeEquals(result.deletedGroups().get(group), classOf[GroupAuthorizationException])
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testDeleteGroupApiWithNoDeleteGroupAcl2(quorum: String): Unit = {
    val result = createAdminClient().deleteConsumerGroups(Seq(group).asJava)
    TestUtils.assertFutureExceptionTypeEquals(result.deletedGroups().get(group), classOf[GroupAuthorizationException])
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testDeleteGroupOffsetsWithAcl(quorum: String, groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DELETE, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumer.commitSync(Map(tp -> new OffsetAndMetadata(5, "")).asJava)
    consumer.close()
    val result = createAdminClient().deleteConsumerGroupOffsets(group, Set(tp).asJava)
    assertNull(result.partitionResult(tp).get())
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testDeleteGroupOffsetsWithoutDeleteAcl(quorum: String, groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumer.commitSync(Map(tp -> new OffsetAndMetadata(5, "")).asJava)
    consumer.close()
    val result = createAdminClient().deleteConsumerGroupOffsets(group, Set(tp).asJava)
    TestUtils.assertFutureExceptionTypeEquals(result.all(), classOf[GroupAuthorizationException])
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testDeleteGroupOffsetsWithDeleteAclWithoutTopicAcl(quorum: String, groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    // Create the consumer group
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumer.commitSync(Map(tp -> new OffsetAndMetadata(5, "")).asJava)
    consumer.close()

    // Remove the topic ACL & Check that it does not work without it
    removeAllClientAcls()
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DELETE, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)
    val result = createAdminClient().deleteConsumerGroupOffsets(group, Set(tp).asJava)
    TestUtils.assertFutureExceptionTypeEquals(result.all(), classOf[TopicAuthorizationException])
    TestUtils.assertFutureExceptionTypeEquals(result.partitionResult(tp), classOf[TopicAuthorizationException])
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testDeleteGroupOffsetsWithNoAcl(quorum: String): Unit = {
    val result = createAdminClient().deleteConsumerGroupOffsets(group, Set(tp).asJava)
    TestUtils.assertFutureExceptionTypeEquals(result.all(), classOf[GroupAuthorizationException])
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testIncrementalAlterGroupConfigsWithAlterAcl(quorum: String): Unit = {
    addAndVerifyAcls(groupAlterConfigsAcl(groupResource), groupResource)

    val request = incrementalAlterGroupConfigsRequest
    val resource = Set[ResourceType](GROUP)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testIncrementalAlterGroupConfigsWithOperationAll(quorum: String): Unit = {
    val allowAllOpsAcl = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALL, ALLOW)
    addAndVerifyAcls(Set(allowAllOpsAcl), groupResource)

    val request = incrementalAlterGroupConfigsRequest
    val resource = Set[ResourceType](GROUP)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testIncrementalAlterGroupConfigsWithoutAlterAcl(quorum: String): Unit = {
    removeAllClientAcls()

    val request = incrementalAlterGroupConfigsRequest
    val resource = Set[ResourceType](GROUP)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = false)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testDescribeGroupConfigsWithDescribeAcl(quorum: String): Unit = {
    addAndVerifyAcls(groupDescribeConfigsAcl(groupResource), groupResource)

    val request = describeGroupConfigsRequest
    val resource = Set[ResourceType](GROUP)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testDescribeGroupConfigsWithOperationAll(quorum: String): Unit = {
    val allowAllOpsAcl = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALL, ALLOW)
    addAndVerifyAcls(Set(allowAllOpsAcl), groupResource)

    val request = describeGroupConfigsRequest
    val resource = Set[ResourceType](GROUP)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testDescribeGroupConfigsWithoutDescribeAcl(quorum: String): Unit = {
    removeAllClientAcls()

    val request = describeGroupConfigsRequest
    val resource = Set[ResourceType](GROUP)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = false)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testUnauthorizedDeleteTopicsWithoutDescribe(quorum: String): Unit = {
    val deleteResponse = connectAndReceive[DeleteTopicsResponse](deleteTopicsRequest)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, deleteResponse.data.responses.find(topic).errorCode)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testUnauthorizedDeleteTopicsWithDescribe(quorum: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), topicResource)
    val deleteResponse = connectAndReceive[DeleteTopicsResponse](deleteTopicsRequest)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, deleteResponse.data.responses.find(topic).errorCode)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testDeleteTopicsWithWildCardAuth(quorum: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DELETE, ALLOW)), new ResourcePattern(TOPIC, "*", LITERAL))
    val deleteResponse = connectAndReceive[DeleteTopicsResponse](deleteTopicsRequest)
    assertEquals(Errors.NONE.code, deleteResponse.data.responses.find(topic).errorCode)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testUnauthorizedDeleteRecordsWithoutDescribe(quorum: String): Unit = {
    val deleteRecordsResponse = connectAndReceive[DeleteRecordsResponse](deleteRecordsRequest)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, deleteRecordsResponse.data.topics.asScala.head.
      partitions.asScala.head.errorCode)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testUnauthorizedDeleteRecordsWithDescribe(quorum: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), topicResource)
    val deleteRecordsResponse = connectAndReceive[DeleteRecordsResponse](deleteRecordsRequest)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, deleteRecordsResponse.data.topics.asScala.head.
      partitions.asScala.head.errorCode)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testDeleteRecordsWithWildCardAuth(quorum: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DELETE, ALLOW)), new ResourcePattern(TOPIC, "*", LITERAL))
    val deleteRecordsResponse = connectAndReceive[DeleteRecordsResponse](deleteRecordsRequest)
    assertEquals(Errors.NONE.code, deleteRecordsResponse.data.topics.asScala.head.
      partitions.asScala.head.errorCode)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testUnauthorizedCreatePartitions(quorum: String): Unit = {
    val createPartitionsResponse = connectAndReceive[CreatePartitionsResponse](createPartitionsRequest)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, createPartitionsResponse.data.results.asScala.head.errorCode)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testCreatePartitionsWithWildCardAuth(quorum: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALTER, ALLOW)), new ResourcePattern(TOPIC, "*", LITERAL))
    val createPartitionsResponse = connectAndReceive[CreatePartitionsResponse](createPartitionsRequest)
    assertEquals(Errors.NONE.code, createPartitionsResponse.data.results.asScala.head.errorCode)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testTransactionalProducerInitTransactionsNoWriteTransactionalIdAcl(quorum: String): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), transactionalIdResource)
    val producer = buildTransactionalProducer()
    assertThrows(classOf[TransactionalIdAuthorizationException], () => producer.initTransactions())
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testTransactionalProducerInitTransactionsNoDescribeTransactionalIdAcl(quorum: String): Unit = {
    val producer = buildTransactionalProducer()
    assertThrows(classOf[TransactionalIdAuthorizationException], () => producer.initTransactions())
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testSendOffsetsWithNoConsumerGroupDescribeAccess(quorum: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, CLUSTER_ACTION, ALLOW)), clusterResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), transactionalIdResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    producer.beginTransaction()

    assertThrows(classOf[GroupAuthorizationException],
      () => producer.sendOffsetsToTransaction(Map(tp -> new OffsetAndMetadata(0L)).asJava, new ConsumerGroupMetadata(group)))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testSendOffsetsWithNoConsumerGroupWriteAccess(quorum: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), transactionalIdResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), groupResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    producer.beginTransaction()

    assertThrows(classOf[GroupAuthorizationException],
      () => producer.sendOffsetsToTransaction(Map(tp -> new OffsetAndMetadata(0L)).asJava, new ConsumerGroupMetadata(group)))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testIdempotentProducerNoIdempotentWriteAclInInitProducerId(quorum: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)
    assertIdempotentSendAuthorizationFailure()
  }

  private def assertIdempotentSendSuccess(): Unit = {
    val producer = buildIdempotentProducer()
    producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, "hi".getBytes)).get()
  }

  private def assertIdempotentSendAuthorizationFailure(): Unit = {
    val producer = buildIdempotentProducer()

    def assertClusterAuthFailure(): Unit = {
      // the InitProducerId is sent asynchronously, so we expect the error either in the callback
      // or raised from send itself
      val exception = assertThrows(classOf[Exception], () => {
        val future = producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, "hi".getBytes))
        future.get()
      })

      exception match {
        case e@ (_: KafkaException | _: ExecutionException) =>
          assertTrue(exception.getCause.isInstanceOf[ClusterAuthorizationException])
        case _ =>
          fail(s"Unexpected exception type raised from send: ${exception.getClass}")
      }
    }

    assertClusterAuthFailure()

    // the second time, the call to send itself should fail (the producer becomes unusable
    // if no producerId can be obtained)
    assertClusterAuthFailure()
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testIdempotentProducerNoIdempotentWriteAclInProduce(quorum: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, IDEMPOTENT_WRITE, ALLOW)), clusterResource)
    idempotentProducerShouldFailInProduce(() => removeAllClientAcls())
  }

  def idempotentProducerShouldFailInProduce(removeAclIdempotenceRequired: () => Unit): Unit = {
    val producer = buildIdempotentProducer()

    // first send should be fine since we have permission to get a ProducerId
    producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, "hi".getBytes)).get()

    // revoke the IdempotentWrite permission
    removeAclIdempotenceRequired()
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), topicResource)

    // the send should now fail with a cluster auth error
    var e = assertThrows(classOf[ExecutionException], () => producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, "hi".getBytes)).get())
    assertTrue(e.getCause.isInstanceOf[TopicAuthorizationException])

    // the second time, the call to send itself should fail (the producer becomes unusable
    // if no producerId can be obtained)
    e = assertThrows(classOf[ExecutionException], () => producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, "hi".getBytes)).get())
    assertTrue(e.getCause.isInstanceOf[TopicAuthorizationException])
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def shouldInitTransactionsWhenAclSet(quorum: String): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), transactionalIdResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testTransactionalProducerTopicAuthorizationExceptionInSendCallback(quorum: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), transactionalIdResource)
    // add describe access so that we can fetch metadata
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), topicResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    producer.beginTransaction()

    val future = producer.send(new ProducerRecord(tp.topic, tp.partition, "1".getBytes, "1".getBytes))
    val e = JTestUtils.assertFutureThrows(future, classOf[TopicAuthorizationException])
    assertEquals(Set(topic), e.unauthorizedTopics.asScala)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testTransactionalProducerTopicAuthorizationExceptionInCommit(quorum: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), transactionalIdResource)
    // add describe access so that we can fetch metadata
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), topicResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    producer.beginTransaction()

    assertThrows(classOf[TopicAuthorizationException], () => {
      producer.send(new ProducerRecord(tp.topic, tp.partition, "1".getBytes, "1".getBytes))
      producer.commitTransaction()
    })
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def shouldThrowTransactionalIdAuthorizationExceptionWhenNoTransactionAccessDuringSend(quorum: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), transactionalIdResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    removeAllClientAcls()
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
    producer.beginTransaction()
    val future = producer.send(new ProducerRecord(tp.topic, tp.partition, "1".getBytes, "1".getBytes))
    JTestUtils.assertFutureThrows(future, classOf[TransactionalIdAuthorizationException])
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def shouldThrowTransactionalIdAuthorizationExceptionWhenNoTransactionAccessOnEndTransaction(quorum: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), transactionalIdResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    producer.beginTransaction()
    producer.send(new ProducerRecord(tp.topic, tp.partition, "1".getBytes, "1".getBytes)).get
    removeAllClientAcls()
    assertThrows(classOf[TransactionalIdAuthorizationException], () => producer.commitTransaction())
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testListTransactionsAuthorization(quorum: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), transactionalIdResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)

    // Start a transaction and write to a topic.
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    producer.beginTransaction()
    producer.send(new ProducerRecord(tp.topic, tp.partition, "1".getBytes, "1".getBytes)).get

    def assertListTransactionResult(
      expectedTransactionalIds: Set[String]
    ): Unit = {
      val listTransactionsRequest = new ListTransactionsRequest.Builder(new ListTransactionsRequestData()).build()
      val listTransactionsResponse = connectAndReceive[ListTransactionsResponse](listTransactionsRequest)
      assertEquals(Errors.NONE, Errors.forCode(listTransactionsResponse.data.errorCode))
      assertEquals(expectedTransactionalIds, listTransactionsResponse.data.transactionStates.asScala.map(_.transactionalId).toSet)
    }

    // First verify that we can list the transaction
    assertListTransactionResult(expectedTransactionalIds = Set(transactionalId))

    // Now revoke authorization and verify that the transaction is no longer listable
    removeAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), transactionalIdResource)
    assertListTransactionResult(expectedTransactionalIds = Set())

    // The minimum permission needed is `Describe`
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), transactionalIdResource)
    assertListTransactionResult(expectedTransactionalIds = Set(transactionalId))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def shouldNotIncludeUnauthorizedTopicsInDescribeTransactionsResponse(quorum: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), transactionalIdResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)

    // Start a transaction and write to a topic.
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    producer.beginTransaction()
    producer.send(new ProducerRecord(tp.topic, tp.partition, "1".getBytes, "1".getBytes)).get

    // Remove only topic authorization so that we can verify that the
    // topic does not get included in the response.
    removeAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
    val response = connectAndReceive[DescribeTransactionsResponse](describeTransactionsRequest)
    assertEquals(1, response.data.transactionStates.size)
    val transactionStateData = response.data.transactionStates.asScala.find(_.transactionalId == transactionalId).get
    assertEquals("Ongoing", transactionStateData.transactionState)
    assertEquals(List.empty, transactionStateData.topics.asScala.toList)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def shouldSuccessfullyAbortTransactionAfterTopicAuthorizationException(quorum: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), transactionalIdResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)),
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

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def shouldThrowTransactionalIdAuthorizationExceptionWhenNoTransactionAccessOnSendOffsetsToTxn(quorum: String): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), transactionalIdResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), groupResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    producer.beginTransaction()
    removeAllClientAcls()
    assertThrows(classOf[TransactionalIdAuthorizationException], () => {
      val offsets = Map(tp -> new OffsetAndMetadata(1L)).asJava
      producer.sendOffsetsToTransaction(offsets, new ConsumerGroupMetadata(group))
      producer.commitTransaction()
    })
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def shouldSendSuccessfullyWhenIdempotentAndHasCorrectACL(quorum: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, IDEMPOTENT_WRITE, ALLOW)), clusterResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
    val producer = buildIdempotentProducer()
    producer.send(new ProducerRecord(tp.topic, tp.partition, "1".getBytes, "1".getBytes)).get
  }

  // Verify that metadata request without topics works without any ACLs and returns cluster id
  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testClusterId(quorum: String): Unit = {
    val request = new requests.MetadataRequest.Builder(List.empty.asJava, false).build()
    val response = connectAndReceive[MetadataResponse](request)
    assertEquals(Collections.emptyMap, response.errorCounts)
    assertFalse(response.clusterId.isEmpty, "Cluster id not returned")
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testRetryProducerInitializationAfterPermissionFix(quorum: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    val wildcard = new ResourcePattern(TOPIC, ResourcePattern.WILDCARD_RESOURCE, LITERAL)
    val prefixed = new ResourcePattern(TOPIC, "t", PREFIXED)
    val literal = new ResourcePattern(TOPIC, topic, LITERAL)
    val allowWriteAce = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)
    val denyWriteAce = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, DENY)
    val producer = buildIdempotentProducer()

    addAndVerifyAcls(Set(denyWriteAce), wildcard)
    assertThrows(classOf[Exception], () => {
      val future = producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, "hi".getBytes))
      future.get()
    })
    removeAndVerifyAcls(Set(denyWriteAce), wildcard)
    addAndVerifyAcls(Set(allowWriteAce), prefixed)
    addAndVerifyAcls(Set(allowWriteAce), literal)
    val future = producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, "hi".getBytes))
    assertDoesNotThrow(() => future.get())
    producer.close()
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testAuthorizeByResourceTypeMultipleAddAndRemove(quorum: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    for (_ <- 1 to 3) {
      addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), topicResource)
      assertIdempotentSendAuthorizationFailure()

      addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
      assertIdempotentSendSuccess()

      removeAllClientAcls()
      addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), topicResource)
      assertIdempotentSendAuthorizationFailure()
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testAuthorizeByResourceTypeIsolationUnrelatedDenyWontDominateAllow(quorum: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    createTopicWithBrokerPrincipal("topic-2")
    createTopicWithBrokerPrincipal("to")

    val unrelatedPrincipalString = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "unrelated").toString
    val unrelatedTopicResource = new ResourcePattern(TOPIC, "topic-2", LITERAL)
    val unrelatedGroupResource = new ResourcePattern(GROUP, "to", PREFIXED)

    val acl1 = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, DENY)
    val acl2 = new AccessControlEntry(unrelatedPrincipalString, WILDCARD_HOST, READ, DENY)
    val acl3 = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, DENY)
    val acl4 = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)
    val acl5 = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)

    addAndVerifyAcls(Set(acl1, acl4, acl5), topicResource)
    addAndVerifyAcls(Set(acl2, acl3), unrelatedTopicResource)
    addAndVerifyAcls(Set(acl2, acl3), unrelatedGroupResource)
    assertIdempotentSendSuccess()
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testAuthorizeByResourceTypeDenyTakesPrecedence(quorum: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    val allowWriteAce = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)
    addAndVerifyAcls(Set(allowWriteAce), topicResource)
    assertIdempotentSendSuccess()

    val denyWriteAce = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, DENY)
    addAndVerifyAcls(Set(denyWriteAce), topicResource)
    assertIdempotentSendAuthorizationFailure()
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testAuthorizeByResourceTypeWildcardResourceDenyDominate(quorum: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    val wildcard = new ResourcePattern(TOPIC, ResourcePattern.WILDCARD_RESOURCE, LITERAL)
    val prefixed = new ResourcePattern(TOPIC, "t", PREFIXED)
    val literal = new ResourcePattern(TOPIC, topic, LITERAL)
    val allowWriteAce = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)
    val denyWriteAce = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, DENY)

    addAndVerifyAcls(Set(allowWriteAce), prefixed)
    addAndVerifyAcls(Set(allowWriteAce), literal)
    assertIdempotentSendSuccess()

    addAndVerifyAcls(Set(denyWriteAce), wildcard)
    assertIdempotentSendAuthorizationFailure()
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testAuthorizeByResourceTypePrefixedResourceDenyDominate(quorum: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    val prefixed = new ResourcePattern(TOPIC, topic.substring(0, 1), PREFIXED)
    val literal = new ResourcePattern(TOPIC, topic, LITERAL)
    val allowWriteAce = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)
    val denyWriteAce = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, DENY)

    addAndVerifyAcls(Set(denyWriteAce), prefixed)
    addAndVerifyAcls(Set(allowWriteAce), literal)
    assertIdempotentSendAuthorizationFailure()
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testMetadataClusterAuthorizedOperationsWithoutDescribeCluster(quorum: String): Unit = {
    removeAllClientAcls()

    // MetadataRequest versions older than 1 are not supported.
    for (version <- 1 to ApiKeys.METADATA.latestVersion) {
      testMetadataClusterClusterAuthorizedOperations(version.toShort, 0)
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testMetadataClusterAuthorizedOperationsWithDescribeAndAlterCluster(quorum: String): Unit = {
    removeAllClientAcls()

    val clusterResource = new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL)
    val acls = Set(
      new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW),
      new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALTER, ALLOW)
    )
    addAndVerifyAcls(acls, clusterResource)

    val expectedClusterAuthorizedOperations = Utils.to32BitField(
      acls.map(_.operation.code.asInstanceOf[JByte]).asJava)

    // MetadataRequest versions older than 1 are not supported.
    for (version <- 1 to ApiKeys.METADATA.latestVersion) {
      testMetadataClusterClusterAuthorizedOperations(version.toShort, expectedClusterAuthorizedOperations)
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testDescribeTopicAclWithOperationAll(quorum: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    removeAllClientAcls()

    val allowAllOpsAcl = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALL, ALLOW)
    addAndVerifyAcls(Set(allowAllOpsAcl), topicResource)

    val metadataRequestTopic = new MetadataRequestTopic()
      .setName(topic)

    val metadataRequest = new MetadataRequest.Builder(new MetadataRequestData()
      .setTopics(Collections.singletonList(metadataRequestTopic))
      .setAllowAutoTopicCreation(false)
    ).build()

    val metadataResponse = connectAndReceive[MetadataResponse](metadataRequest)
    val topicResponseOpt = metadataResponse.topicMetadata().asScala.find(_.topic == topic)
    assertTrue(topicResponseOpt.isDefined)

    val topicResponse = topicResponseOpt.get
    assertEquals(Errors.NONE, topicResponse.error)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testDescribeTopicConfigsAclWithOperationAll(quorum: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    removeAllClientAcls()

    val allowAllOpsAcl = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALL, ALLOW)
    addAndVerifyAcls(Set(allowAllOpsAcl), topicResource)

    val describeConfigsRequest = new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData()
      .setResources(Collections.singletonList(new DescribeConfigsRequestData.DescribeConfigsResource()
        .setResourceType(ConfigResource.Type.TOPIC.id)
        .setResourceName(tp.topic)))
    ).build()

    val describeConfigsResponse = connectAndReceive[DescribeConfigsResponse](describeConfigsRequest)
    val topicConfigResponse = describeConfigsResponse.data.results.get(0)
    assertEquals(Errors.NONE, Errors.forCode(topicConfigResponse.errorCode))
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

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testDescribeClusterClusterAuthorizedOperationsWithoutDescribeCluster(quorum: String): Unit = {
    removeAllClientAcls()

    for (version <- ApiKeys.DESCRIBE_CLUSTER.oldestVersion to ApiKeys.DESCRIBE_CLUSTER.latestVersion) {
      testDescribeClusterClusterAuthorizedOperations(version.toShort, 0)
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testDescribeClusterClusterAuthorizedOperationsWithDescribeAndAlterCluster(quorum: String): Unit = {
    removeAllClientAcls()

    val clusterResource = new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL)
    val acls = Set(
      new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW),
      new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALTER, ALLOW)
    )
    addAndVerifyAcls(acls, clusterResource)

    val expectedClusterAuthorizedOperations = Utils.to32BitField(
      acls.map(_.operation.code.asInstanceOf[JByte]).asJava)

    for (version <- ApiKeys.DESCRIBE_CLUSTER.oldestVersion to ApiKeys.DESCRIBE_CLUSTER.latestVersion) {
      testDescribeClusterClusterAuthorizedOperations(version.toShort, expectedClusterAuthorizedOperations)
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testHostAddressBasedAcls(quorum: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    removeAllClientAcls()

    val socket = connect(anySocketServer, listenerName)
    try {
      val acls = Set(
        new AccessControlEntry(clientPrincipalString, socket.getLocalAddress.getHostAddress, DESCRIBE, ALLOW)
      )

      addAndVerifyAcls(acls, topicResource)

      val metadataRequestTopic = new MetadataRequestTopic()
        .setName(topic)

      val metadataRequest = new MetadataRequest.Builder(new MetadataRequestData()
        .setTopics(Collections.singletonList(metadataRequestTopic))
        .setAllowAutoTopicCreation(false)
      ).build()

      val metadataResponse = sendAndReceive[MetadataResponse](metadataRequest, socket)
      val topicResponseOpt = metadataResponse.topicMetadata().asScala.find(_.topic == topic)
      assertTrue(topicResponseOpt.isDefined)

      val topicResponse = topicResponseOpt.get
      assertEquals(Errors.NONE, topicResponse.error)
    } finally {
      socket.close()
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testCreateAndCloseConsumerWithNoAccess(quorum: String, groupProtocol: String): Unit = {
    val consumer = createConsumer()
    val closeConsumer: Executable = () => consumer.close()
    // Close consumer without consuming anything. close() call should pass successfully and throw no exception.
    assertDoesNotThrow(closeConsumer, "Exception not expected on closing consumer")
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testConsumerGroupHeartbeatWithReadAcl(quorum: String): Unit = {
    addAndVerifyAcls(groupReadAcl(groupResource), groupResource)

    val request = consumerGroupHeartbeatRequest
    val resource = Set[ResourceType](GROUP)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testConsumerGroupHeartbeatWithOperationAll(quorum: String): Unit = {
    val allowAllOpsAcl = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALL, ALLOW)
    addAndVerifyAcls(Set(allowAllOpsAcl), groupResource)

    val request = consumerGroupHeartbeatRequest
    val resource = Set[ResourceType](GROUP)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testConsumerGroupHeartbeatWithoutReadAcl(quorum: String): Unit = {
    removeAllClientAcls()

    val request = consumerGroupHeartbeatRequest
    val resource = Set[ResourceType](GROUP)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = false)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testConsumerGroupDescribeWithDescribeAcl(quorum: String): Unit = {
    addAndVerifyAcls(groupDescribeAcl(groupResource), groupResource)

    val request = consumerGroupDescribeRequest
    val resource = Set[ResourceType](GROUP)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testConsumerGroupDescribeWithOperationAll(quorum: String): Unit = {
    val allowAllOpsAcl = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALL, ALLOW)
    addAndVerifyAcls(Set(allowAllOpsAcl), groupResource)

    val request = consumerGroupDescribeRequest
    val resource = Set[ResourceType](GROUP)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testConsumerGroupDescribeWithoutDescribeAcl(quorum: String): Unit = {
    removeAllClientAcls()

    val request = consumerGroupDescribeRequest
    val resource = Set[ResourceType](GROUP)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = false)
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
    val authorizerForWrite = TestUtils.pickAuthorizerForWrite(brokers, controllerServers)
    val aclEntryFilter = new AccessControlEntryFilter(clientPrincipalString, null, AclOperation.ANY, AclPermissionType.ANY)
    val aclFilter = new AclBindingFilter(ResourcePatternFilter.ANY, aclEntryFilter)

    authorizerForWrite.deleteAcls(TestUtils.anonymousAuthorizableContext, List(aclFilter).asJava).asScala.
      map(_.toCompletableFuture.get).flatMap { deletion =>
        deletion.aclBindingDeleteResults().asScala.map(_.aclBinding.pattern).toSet
      }.foreach { resource =>
        (brokers.map(_.authorizer.get) ++ controllerServers.map(_.authorizer.get)).foreach { authorizer =>
          TestUtils.waitAndVerifyAcls(Set.empty[AccessControlEntry], authorizer, resource, aclEntryFilter)
        }
      }
  }

  private def sendRequestAndVerifyResponseError(request: AbstractRequest,
                                                resources: Set[ResourceType],
                                                isAuthorized: Boolean,
                                                topicExists: Boolean = true,
                                                topicNames: Map[Uuid, String] = getTopicNames()): AbstractResponse = {
    val apiKey = request.apiKey
    val response = connectAndReceive[AbstractResponse](request)
    val error = requestKeyToError(topicNames, request.version())(apiKey).asInstanceOf[AbstractResponse => Errors](response)

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
        assertFalse(authorizationErrors.contains(error), s"$apiKey should be allowed. Found unexpected authorization error $error with $request")
      else
        assertTrue(authorizationErrors.contains(error), s"$apiKey should be forbidden. Found error $error but expected one of $authorizationErrors")
    else if (resources == Set(TOPIC))
      if (isAuthorized)
        if (apiKey.equals(ApiKeys.FETCH) && request.version() >= 13)
          assertEquals(Errors.UNKNOWN_TOPIC_ID, error, s"$apiKey had an unexpected error")
        else
          assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, error, s"$apiKey had an unexpected error")
      else {
        if (apiKey.equals(ApiKeys.FETCH) && request.version() >= 13)
          assertEquals(Errors.UNKNOWN_TOPIC_ID, error, s"$apiKey had an unexpected error")
        else
          assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED, error, s"$apiKey had an unexpected error")
      }

    response
  }

  private def sendRecords(producer: KafkaProducer[Array[Byte], Array[Byte]],
                          numRecords: Int,
                          tp: TopicPartition): Unit = {
    val futures = (0 until numRecords).map { i =>
      producer.send(new ProducerRecord(tp.topic, tp.partition, i.toString.getBytes, i.toString.getBytes))
    }
    try {
      futures.foreach(_.get)
    } catch {
      case e: ExecutionException => throw e.getCause
    }
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
    producerConfig.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    producerConfig.setProperty(ProducerConfig.ACKS_CONFIG, "all")
    createProducer()
  }

  private def buildIdempotentProducer(): KafkaProducer[Array[Byte], Array[Byte]] = {
    producerConfig.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    producerConfig.setProperty(ProducerConfig.ACKS_CONFIG, "all")
    createProducer()
  }

  private def createAdminClient(): Admin = {
    createAdminClient(listenerName)
  }

  private def createTopicWithBrokerPrincipal(
    topic: String,
    numPartitions: Int = 1
  ): Unit =  {
    // Note the principal builder implementation maps all connections on the
    // inter-broker listener to the broker principal.
    createTopic(
      topic,
      numPartitions = numPartitions,
      listenerName = interBrokerListenerName
    )
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testPrefixAcls(quorum: String): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, CREATE, ALLOW)),
      new ResourcePattern(TOPIC, "f", PREFIXED))
    addAndVerifyAcls(Set(new AccessControlEntry("User:otherPrincipal", WILDCARD_HOST, CREATE, DENY)),
      new ResourcePattern(TOPIC, "fooa", PREFIXED))
    addAndVerifyAcls(Set(new AccessControlEntry("User:otherPrincipal", WILDCARD_HOST, CREATE, ALLOW)),
      new ResourcePattern(TOPIC, "foob", PREFIXED))
    createAdminClient().createTopics(Collections.
      singletonList(new NewTopic("foobar", 1, 1.toShort))).all().get()
  }
}
