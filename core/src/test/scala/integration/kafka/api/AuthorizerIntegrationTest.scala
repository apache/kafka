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
import java.util.{Comparator, Optional, Properties, UUID}
import kafka.utils.{TestInfoUtils, TestUtils}
import kafka.utils.TestUtils.waitUntilTrue
import org.apache.kafka.clients.admin.{Admin, AlterConfigOp, ListGroupsOptions, NewTopic}
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.consumer.internals.{ShareAcquireMode, StreamsRebalanceData, StreamsRebalanceListener}
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
import org.apache.kafka.common.message.{AddOffsetsToTxnRequestData, AlterPartitionReassignmentsRequestData, AlterReplicaLogDirsRequestData, AlterShareGroupOffsetsRequestData, ConsumerGroupDescribeRequestData, ConsumerGroupHeartbeatRequestData, ConsumerGroupHeartbeatResponseData, CreateAclsRequestData, CreatePartitionsRequestData, CreateTopicsRequestData, DeleteAclsRequestData, DeleteGroupsRequestData, DeleteRecordsRequestData, DeleteShareGroupOffsetsRequestData, DeleteShareGroupStateRequestData, DeleteTopicsRequestData, DescribeClusterRequestData, DescribeConfigsRequestData, DescribeGroupsRequestData, DescribeLogDirsRequestData, DescribeProducersRequestData, DescribeShareGroupOffsetsRequestData, DescribeTransactionsRequestData, FetchResponseData, FindCoordinatorRequestData, HeartbeatRequestData, IncrementalAlterConfigsRequestData, InitializeShareGroupStateRequestData, JoinGroupRequestData, ListPartitionReassignmentsRequestData, ListTransactionsRequestData, MetadataRequestData, OffsetCommitRequestData, OffsetFetchRequestData, OffsetFetchResponseData, ProduceRequestData, ReadShareGroupStateRequestData, ReadShareGroupStateSummaryRequestData, ShareAcknowledgeRequestData, ShareFetchRequestData, ShareGroupDescribeRequestData, ShareGroupHeartbeatRequestData, StreamsGroupDescribeRequestData, StreamsGroupHeartbeatRequestData, StreamsGroupHeartbeatResponseData, SyncGroupRequestData, WriteShareGroupStateRequestData, WriteTxnMarkersRequestData}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.{MemoryRecords, RecordBatch, SimpleRecord}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.resource.PatternType.{LITERAL, PREFIXED}
import org.apache.kafka.common.resource.ResourceType._
import org.apache.kafka.common.resource.{PatternType, Resource, ResourcePattern, ResourcePatternFilter, ResourceType}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.{ElectionType, IsolationLevel, KafkaException, TopicIdPartition, TopicPartition, Uuid, requests}
import org.apache.kafka.test.{TestUtils => JTestUtils}
import org.apache.kafka.security.authorizer.AclEntry
import org.apache.kafka.security.authorizer.AclEntry.WILDCARD_HOST
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, MethodSource, ValueSource}
import org.apache.kafka.common.message.MetadataRequestData.MetadataRequestTopic
import org.apache.kafka.common.message.WriteTxnMarkersRequestData.{WritableTxnMarker, WritableTxnMarkerTopic}
import org.apache.kafka.coordinator.group.GroupConfig
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.jdk.javaapi.OptionConverters

class AuthorizerIntegrationTest extends AbstractAuthorizerIntegrationTest {
  val groupReadAcl = Map(groupResource -> Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)))
  val groupDescribeAcl = Map(groupResource -> Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)))
  val groupDeleteAcl = Map(groupResource -> Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DELETE, ALLOW)))
  val groupDescribeConfigsAcl = Map(groupResource -> Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE_CONFIGS, ALLOW)))
  val groupAlterConfigsAcl = Map(groupResource -> Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALTER_CONFIGS, ALLOW)))
  val shareGroupReadAcl = Map(shareGroupResource -> Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)))
  val shareGroupDescribeAcl = Map(shareGroupResource -> Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)))
  val shareGroupDeleteAcl = Map(shareGroupResource -> Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DELETE, ALLOW)))
  val shareGroupDescribeConfigsAcl = Map(shareGroupResource -> Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE_CONFIGS, ALLOW)))
  val shareGroupAlterConfigsAcl = Map(shareGroupResource -> Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALTER_CONFIGS, ALLOW)))
  val streamsGroupReadAcl = Map(streamsGroupResource -> Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)))
  val streamsGroupDescribeAcl = Map(streamsGroupResource -> Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)))
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
  val sourceTopicDescribeAcl = Map(sourceTopicResource -> Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)))

  val numRecords = 1

  val requestKeyToError = (topicNames: Map[Uuid, String], version: Short) => Map[ApiKeys, Nothing => Errors](
    ApiKeys.METADATA -> ((resp: requests.MetadataResponse) => resp.errors.asScala.find(_._1 == topic).getOrElse(("test", Errors.NONE))._2),
    ApiKeys.PRODUCE -> ((resp: requests.ProduceResponse) => {
      val topicId = topicNames.find { case (_, topicName) => topicName == topic}
        .map { case (topicId, _) => topicId }
        .getOrElse(Uuid.ZERO_UUID)
      Errors.forCode(
        resp.data
          .responses.find("", topicId) // version is always >= 13 no need to use topic name
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
    ApiKeys.OFFSET_FETCH -> ((resp: requests.OffsetFetchResponse) => Errors.forCode(resp.group(group).errorCode())),
    ApiKeys.FIND_COORDINATOR -> ((resp: FindCoordinatorResponse) => {
      Errors.forCode(resp.data.coordinators.asScala.find(g => group == g.key).head.errorCode)
    }),
    ApiKeys.JOIN_GROUP -> ((resp: JoinGroupResponse) => resp.error),
    ApiKeys.SYNC_GROUP -> ((resp: SyncGroupResponse) => Errors.forCode(resp.data.errorCode)),
    ApiKeys.DESCRIBE_GROUPS -> ((resp: DescribeGroupsResponse) => {
      Errors.forCode(resp.data.groups.asScala.find(g => group == g.groupId).head.errorCode)
    }),
    ApiKeys.HEARTBEAT -> ((resp: HeartbeatResponse) => resp.error),
    ApiKeys.LEAVE_GROUP -> ((resp: LeaveGroupResponse) => resp.error),
    ApiKeys.DELETE_GROUPS -> ((resp: DeleteGroupsResponse) => resp.get(group)),
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
      Errors.forCode(resp.data.groups.asScala.find(g => group == g.groupId).head.errorCode)),
    ApiKeys.SHARE_GROUP_HEARTBEAT -> ((resp: ShareGroupHeartbeatResponse) => Errors.forCode(resp.data.errorCode)),
    ApiKeys.SHARE_GROUP_DESCRIBE -> ((resp: ShareGroupDescribeResponse) =>
      Errors.forCode(resp.data.groups.asScala.find(g => shareGroup == g.groupId).head.errorCode)),
    ApiKeys.SHARE_FETCH -> ((resp: ShareFetchResponse) => Errors.forCode(resp.data.errorCode)),
    ApiKeys.SHARE_ACKNOWLEDGE -> ((resp: ShareAcknowledgeResponse) => Errors.forCode(resp.data.errorCode)),
    ApiKeys.INITIALIZE_SHARE_GROUP_STATE -> ((resp: InitializeShareGroupStateResponse) => Errors.forCode(
      resp.data.results.get(0).partitions.get(0).errorCode)),
    ApiKeys.READ_SHARE_GROUP_STATE -> ((resp: ReadShareGroupStateResponse) => Errors.forCode(
      resp.data.results.get(0).partitions.get(0).errorCode)),
    ApiKeys.WRITE_SHARE_GROUP_STATE -> ((resp: WriteShareGroupStateResponse) => Errors.forCode(
      resp.data.results.get(0).partitions.get(0).errorCode)),
    ApiKeys.DELETE_SHARE_GROUP_STATE -> ((resp: DeleteShareGroupStateResponse) => Errors.forCode(
      resp.data.results.get(0).partitions.get(0).errorCode)),
    ApiKeys.READ_SHARE_GROUP_STATE_SUMMARY -> ((resp: ReadShareGroupStateSummaryResponse) => Errors.forCode(
      resp.data.results.get(0).partitions.get(0).errorCode)),
    ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS -> ((resp: DescribeShareGroupOffsetsResponse) => Errors.forCode(
      resp.data.groups.asScala.find(g => shareGroup == g.groupId).head.errorCode)),
    ApiKeys.DELETE_SHARE_GROUP_OFFSETS -> ((resp: DeleteShareGroupOffsetsResponse) => Errors.forCode(
      resp.data.errorCode)),
    ApiKeys.ALTER_SHARE_GROUP_OFFSETS -> ((resp: AlterShareGroupOffsetsResponse) => Errors.forCode(
      resp.data.errorCode)),
    ApiKeys.STREAMS_GROUP_HEARTBEAT -> ((resp: StreamsGroupHeartbeatResponse) => Errors.forCode(resp.data.errorCode)),
    ApiKeys.STREAMS_GROUP_DESCRIBE -> ((resp: StreamsGroupDescribeResponse) =>
      Errors.forCode(resp.data.groups.asScala.find(g => streamsGroup == g.groupId).head.errorCode))
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
    ApiKeys.CONSUMER_GROUP_DESCRIBE -> groupDescribeAcl,
    ApiKeys.SHARE_GROUP_HEARTBEAT -> (shareGroupReadAcl ++ topicDescribeAcl),
    ApiKeys.SHARE_GROUP_DESCRIBE -> (shareGroupDescribeAcl ++ topicDescribeAcl),
    ApiKeys.SHARE_FETCH -> (shareGroupReadAcl ++ topicReadAcl),
    ApiKeys.SHARE_ACKNOWLEDGE -> (shareGroupReadAcl ++ topicReadAcl),
    ApiKeys.INITIALIZE_SHARE_GROUP_STATE -> clusterAcl,
    ApiKeys.READ_SHARE_GROUP_STATE -> clusterAcl,
    ApiKeys.WRITE_SHARE_GROUP_STATE -> clusterAcl,
    ApiKeys.DELETE_SHARE_GROUP_STATE -> clusterAcl,
    ApiKeys.READ_SHARE_GROUP_STATE_SUMMARY -> clusterAcl,
    ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS -> (shareGroupDescribeAcl ++ topicDescribeAcl),
    ApiKeys.DELETE_SHARE_GROUP_OFFSETS -> (shareGroupDeleteAcl ++ topicReadAcl),
    ApiKeys.ALTER_SHARE_GROUP_OFFSETS -> (shareGroupReadAcl ++ topicReadAcl),
    ApiKeys.STREAMS_GROUP_HEARTBEAT -> (streamsGroupReadAcl ++ topicDescribeAcl),
    ApiKeys.STREAMS_GROUP_DESCRIBE -> (streamsGroupDescribeAcl ++ topicDescribeAcl),
  )

  private def createMetadataRequest(allowAutoTopicCreation: Boolean) = {
    new requests.MetadataRequest.Builder(java.util.List.of(topic), allowAutoTopicCreation).build()
  }

  private def createProduceRequest(name: String, id: Uuid, version: Short) = {
    requests.ProduceRequest.builder(new ProduceRequestData()
        .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
          util.List.of(new ProduceRequestData.TopicProduceData()
              .setName(name)
              .setTopicId(id)
              .setPartitionData(util.List.of(
                new ProduceRequestData.PartitionProduceData()
                  .setIndex(tp.partition)
                  .setRecords(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("test".getBytes))))))
            .iterator))
        .setAcks(1.toShort)
        .setTimeoutMs(5000))
      .build(version)
  }

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

  private def createFetchRequestWithEmptyTopicNameAndZeroTopicId(version: Short) = {
    val partitionMap = new util.LinkedHashMap[TopicPartition, requests.FetchRequest.PartitionData]
    partitionMap.put(new TopicPartition("", part),
      new requests.FetchRequest.PartitionData(Uuid.ZERO_UUID, 0, 0, 100, Optional.of(27)))
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
      .setTargetTimes(java.util.List.of(new ListOffsetsTopic()
        .setName(tp.topic)
        .setPartitions(java.util.List.of(new ListOffsetsPartition()
          .setPartitionIndex(tp.partition)
          .setTimestamp(0L)
          .setCurrentLeaderEpoch(27))))
      ).
      build()
  }

  private def offsetsForLeaderEpochRequest: OffsetsForLeaderEpochRequest = {
    val epochs = new OffsetForLeaderTopicCollection()
    epochs.add(new OffsetForLeaderTopic()
      .setTopic(tp.topic)
      .setPartitions(java.util.List.of(new OffsetForLeaderPartition()
          .setPartition(tp.partition)
          .setLeaderEpoch(7)
          .setCurrentLeaderEpoch(27))))
    OffsetsForLeaderEpochRequest.Builder.forConsumer(epochs).build()
  }

  private def createOffsetFetchRequest: OffsetFetchRequest = {
    OffsetFetchRequest.Builder.forTopicNames(
      new OffsetFetchRequestData()
        .setRequireStable(false)
        .setGroups(util.List.of(
          new OffsetFetchRequestData.OffsetFetchRequestGroup()
            .setGroupId(group)
            .setTopics(util.List.of(
              new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setName(tp.topic)
                .setPartitionIndexes(util.List.of[Integer](tp.partition))
            ))
        )),
      false
    ).build()
  }

  private def createOffsetFetchRequestAllPartitions: OffsetFetchRequest = {
    OffsetFetchRequest.Builder.forTopicNames(
      new OffsetFetchRequestData()
        .setRequireStable(false)
        .setGroups(util.List.of(
          new OffsetFetchRequestData.OffsetFetchRequestGroup()
            .setGroupId(group)
            .setTopics(null)
        )),
      false
    ).build()
  }

  private def createOffsetFetchRequest(groupToPartitionMap: util.Map[String, util.List[TopicPartition]]): OffsetFetchRequest = {
    OffsetFetchRequest.Builder.forTopicNames(
      new OffsetFetchRequestData()
        .setGroups(groupToPartitionMap.asScala.map { case (groupId, partitions) =>
          new OffsetFetchRequestData.OffsetFetchRequestGroup()
            .setGroupId(groupId)
            .setTopics(
              if (partitions == null)
                null
              else
                partitions.asScala.groupBy(_.topic).map { case (topic, partitions) =>
                  new OffsetFetchRequestData.OffsetFetchRequestTopics()
                    .setName(topic)
                    .setPartitionIndexes(partitions.map(_.partition).map(Int.box).asJava)
                }.toList.asJava)
        }.toList.asJava),
      false
    ).build()
  }

  private def createFindCoordinatorRequest = {
    new FindCoordinatorRequest.Builder(
        new FindCoordinatorRequestData()
          .setKeyType(FindCoordinatorRequest.CoordinatorType.GROUP.id)
          .setCoordinatorKeys(util.List.of(group))).build()
  }

  private def createJoinGroupRequest = {
    val protocolSet = new JoinGroupRequestProtocolCollection(
      util.List.of(new JoinGroupRequestData.JoinGroupRequestProtocol()
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
        .setAssignments(util.List.of)
    ).build()
  }

  private def createDescribeGroupsRequest = {
    new DescribeGroupsRequest.Builder(new DescribeGroupsRequestData().setGroups(java.util.List.of(group))).build()
  }

  private def createOffsetCommitRequest = {
    requests.OffsetCommitRequest.Builder.forTopicNames(
        new OffsetCommitRequestData()
          .setGroupId(group)
          .setMemberId(JoinGroupRequest.UNKNOWN_MEMBER_ID)
          .setGenerationIdOrMemberEpoch(1)
          .setTopics(util.List.of(
            new OffsetCommitRequestData.OffsetCommitRequestTopic()
              .setName(topic)
              .setPartitions(util.List.of(
                new OffsetCommitRequestData.OffsetCommitRequestPartition()
                  .setPartitionIndex(part)
                  .setCommittedOffset(0)
                  .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
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
    group, util.List.of(
      new MemberIdentity()
        .setMemberId(JoinGroupRequest.UNKNOWN_MEMBER_ID)
    )).build()

  private def deleteGroupsRequest = new DeleteGroupsRequest.Builder(
    new DeleteGroupsRequestData()
      .setGroupsNames(util.List.of(group))
  ).build()

  private def createTopicsRequest: CreateTopicsRequest = {
    new CreateTopicsRequest.Builder(new CreateTopicsRequestData().setTopics(
      new CreatableTopicCollection(util.Set.of(new CreatableTopic().
        setName(topic).setNumPartitions(1).
        setReplicationFactor(1.toShort)).iterator))).build()
  }

  private def deleteTopicsRequest: DeleteTopicsRequest = {
    new DeleteTopicsRequest.Builder(
      new DeleteTopicsRequestData()
        .setTopicNames(util.List.of(topic))
        .setTimeoutMs(5000)).build()
  }

  private def deleteTopicsWithIdsRequest(topicId: Uuid): DeleteTopicsRequest = {
    new DeleteTopicsRequest.Builder(
      new DeleteTopicsRequestData()
        .setTopics(util.List.of(
          new DeleteTopicsRequestData.DeleteTopicState()
            .setTopicId(topicId)))
        .setTimeoutMs(5000)).build()
  }

  private def deleteRecordsRequest = new DeleteRecordsRequest.Builder(
    new DeleteRecordsRequestData()
      .setTimeoutMs(5000)
      .setTopics(util.List.of(new DeleteRecordsRequestData.DeleteRecordsTopic()
        .setName(tp.topic)
        .setPartitions(util.List.of(new DeleteRecordsRequestData.DeleteRecordsPartition()
          .setPartitionIndex(tp.partition)
          .setOffset(0L)))))).build()

  private def describeConfigsRequest =
    new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData().setResources(util.List.of(
      new DescribeConfigsRequestData.DescribeConfigsResource().setResourceType(ConfigResource.Type.TOPIC.id)
        .setResourceName(tp.topic)))).build()

  private def alterConfigsRequest =
    new AlterConfigsRequest.Builder(
      util.Map.of(new ConfigResource(ConfigResource.Type.TOPIC, tp.topic),
        new AlterConfigsRequest.Config(util.Set.of(
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
    new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData().setResources(util.List.of(
      new DescribeConfigsRequestData.DescribeConfigsResource().setResourceType(ConfigResource.Type.GROUP.id)
        .setResourceName(group)))).build()
  }

  private def describeAclsRequest = new DescribeAclsRequest.Builder(AclBindingFilter.ANY).build()

  private def createAclsRequest: CreateAclsRequest = new CreateAclsRequest.Builder(
    new CreateAclsRequestData().setCreations(util.List.of(
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
    new DeleteAclsRequestData().setFilters(util.List.of(
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
      .setPartitions(util.List.of(tp.partition)))
    val data = new AlterReplicaLogDirsRequestData()
    data.dirs.add(dir)
    new AlterReplicaLogDirsRequest.Builder(data).build()
  }

  private def describeLogDirsRequest = new DescribeLogDirsRequest.Builder(new DescribeLogDirsRequestData().setTopics(new DescribeLogDirsRequestData.DescribableLogDirTopicCollection(util.Set.of(
    new DescribeLogDirsRequestData.DescribableLogDirTopic().setTopic(tp.topic).setPartitions(util.List.of(tp.partition))).iterator()))).build()

  private def addPartitionsToTxnRequest = AddPartitionsToTxnRequest.Builder.forClient(transactionalId, 1, 1, util.List.of(tp)).build()

  private def addOffsetsToTxnRequest = new AddOffsetsToTxnRequest.Builder(
    new AddOffsetsToTxnRequestData()
      .setTransactionalId(transactionalId)
      .setProducerId(1)
      .setProducerEpoch(1)
      .setGroupId(group)
  ).build()

  private def electLeadersRequest = new ElectLeadersRequest.Builder(
    ElectionType.PREFERRED,
    util.Set.of(tp),
    10000
  ).build()

  private def describeProducersRequest: DescribeProducersRequest = new DescribeProducersRequest.Builder(
    new DescribeProducersRequestData()
      .setTopics(java.util.List.of(
        new DescribeProducersRequestData.TopicRequest()
          .setName(tp.topic)
          .setPartitionIndexes(java.util.List.of(Int.box(tp.partition)))
      ))
  ).build()

  private def describeTransactionsRequest: DescribeTransactionsRequest = new DescribeTransactionsRequest.Builder(
    new DescribeTransactionsRequestData().setTransactionalIds(java.util.List.of(transactionalId))
  ).build()

  private def alterPartitionReassignmentsRequest = new AlterPartitionReassignmentsRequest.Builder(
    new AlterPartitionReassignmentsRequestData().setTopics(
      java.util.List.of(new AlterPartitionReassignmentsRequestData.ReassignableTopic()
        .setName(topic)
        .setPartitions(
          java.util.List.of(new AlterPartitionReassignmentsRequestData.ReassignablePartition().setPartitionIndex(tp.partition))
        ))
    )
  ).build()

  private def listPartitionReassignmentsRequest = new ListPartitionReassignmentsRequest.Builder(
    new ListPartitionReassignmentsRequestData().setTopics(
      java.util.List.of(new ListPartitionReassignmentsRequestData.ListPartitionReassignmentsTopics()
        .setName(topic)
        .setPartitionIndexes(
          java.util.List.of(Integer.valueOf(tp.partition))
        ))
    )
  ).build()

  private def writeTxnMarkersRequest: WriteTxnMarkersRequest = new WriteTxnMarkersRequest.Builder(
    new WriteTxnMarkersRequestData()
      .setMarkers(
        java.util.List.of(new WritableTxnMarker()
          .setProducerId(producerId)
          .setProducerEpoch(1)
          .setTransactionResult(false)
          .setTopics(java.util.List.of(new WritableTxnMarkerTopic()
            .setName(tp.topic())
            .setPartitionIndexes(java.util.List.of(Integer.valueOf(tp.partition())))
          ))
          .setCoordinatorEpoch(1)
        )
      )
  ).build()

  private def consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
    new ConsumerGroupHeartbeatRequestData()
      .setGroupId(group)
      .setMemberEpoch(0)
      .setSubscribedTopicNames(java.util.List.of(topic))).build()

  private def consumerGroupDescribeRequest = new ConsumerGroupDescribeRequest.Builder(
    new ConsumerGroupDescribeRequestData()
      .setGroupIds(java.util.List.of(group))
      .setIncludeAuthorizedOperations(false)).build()

  private def shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
    new ShareGroupHeartbeatRequestData()
      .setGroupId(shareGroup)
      .setMemberId(Uuid.randomUuid().toString)
      .setMemberEpoch(0)
      .setSubscribedTopicNames(List(topic).asJava)).build(ApiKeys.SHARE_GROUP_HEARTBEAT.latestVersion)


  private def shareGroupDescribeRequest = new ShareGroupDescribeRequest.Builder(
    new ShareGroupDescribeRequestData()
      .setGroupIds(List(shareGroup).asJava)
      .setIncludeAuthorizedOperations(false)).build(ApiKeys.SHARE_GROUP_DESCRIBE.latestVersion)


  private def createShareFetchRequest = {
    val metadata: ShareRequestMetadata = new ShareRequestMetadata(Uuid.randomUuid(), ShareRequestMetadata.INITIAL_EPOCH)
    val send: Seq[TopicIdPartition] = Seq(
      new TopicIdPartition(getTopicIds().getOrElse(tp.topic, Uuid.ZERO_UUID), new TopicPartition(topic, part)))
    val ackMap = new util.HashMap[TopicIdPartition, util.List[ShareFetchRequestData.AcknowledgementBatch]]
    requests.ShareFetchRequest.Builder.forConsumer(shareGroup, metadata, 100, 0, Int.MaxValue, 500, 500, ShareAcquireMode.BATCH_OPTIMIZED.id(), false,
      send.asJava, Seq.empty.asJava, ackMap).build()
  }

  private def shareAcknowledgeRequest = {
    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData()
      .setGroupId(shareGroup)
      .setMemberId(Uuid.randomUuid().toString)
      .setShareSessionEpoch(1)
      .setTopics(new ShareAcknowledgeRequestData.AcknowledgeTopicCollection(util.List.of(new ShareAcknowledgeRequestData.AcknowledgeTopic()
        .setTopicId(getTopicIds().getOrElse(tp.topic, Uuid.ZERO_UUID))
        .setPartitions(new ShareAcknowledgeRequestData.AcknowledgePartitionCollection(util.List.of(
          new ShareAcknowledgeRequestData.AcknowledgePartition()
            .setPartitionIndex(part)
            .setAcknowledgementBatches(List(
              new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(1)
                .setAcknowledgeTypes(util.List.of(1.toByte))
            ).asJava)
        ).iterator))
      ).iterator))

    new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData).build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
  }

  private def initializeShareGroupStateRequest = new InitializeShareGroupStateRequest.Builder(
    new InitializeShareGroupStateRequestData()
      .setGroupId(shareGroup)
      .setTopics(List(new InitializeShareGroupStateRequestData.InitializeStateData()
        .setTopicId(getTopicIds().getOrElse(tp.topic, Uuid.ZERO_UUID))
        .setPartitions(List(new InitializeShareGroupStateRequestData.PartitionData()
          .setPartition(part)
        ).asJava)
      ).asJava)).build()

  private def readShareGroupStateRequest = new ReadShareGroupStateRequest.Builder(
    new ReadShareGroupStateRequestData()
      .setGroupId(shareGroup)
      .setTopics(List(new ReadShareGroupStateRequestData.ReadStateData()
        .setTopicId(getTopicIds().getOrElse(tp.topic, Uuid.ZERO_UUID))
        .setPartitions(List(new ReadShareGroupStateRequestData.PartitionData()
          .setPartition(part)
          .setLeaderEpoch(0)
        ).asJava)
      ).asJava)).build()

  private def writeShareGroupStateRequest = new WriteShareGroupStateRequest.Builder(
    new WriteShareGroupStateRequestData()
      .setGroupId(shareGroup)
      .setTopics(List(new WriteShareGroupStateRequestData.WriteStateData()
        .setTopicId(getTopicIds().getOrElse(tp.topic, Uuid.ZERO_UUID))
        .setPartitions(List(new WriteShareGroupStateRequestData.PartitionData()
          .setPartition(part)
        ).asJava)
      ).asJava)).build()

  private def deleteShareGroupStateRequest = new DeleteShareGroupStateRequest.Builder(
    new DeleteShareGroupStateRequestData()
      .setGroupId(shareGroup)
      .setTopics(List(new DeleteShareGroupStateRequestData.DeleteStateData()
        .setTopicId(getTopicIds().getOrElse(tp.topic, Uuid.ZERO_UUID))
        .setPartitions(List(new DeleteShareGroupStateRequestData.PartitionData()
          .setPartition(part)
        ).asJava)
      ).asJava)).build()

  private def readShareGroupStateSummaryRequest = new ReadShareGroupStateSummaryRequest.Builder(
    new ReadShareGroupStateSummaryRequestData()
      .setGroupId(shareGroup)
      .setTopics(List(new ReadShareGroupStateSummaryRequestData.ReadStateSummaryData()
        .setTopicId(getTopicIds().getOrElse(tp.topic, Uuid.ZERO_UUID))
        .setPartitions(List(new ReadShareGroupStateSummaryRequestData.PartitionData()
          .setPartition(part)
          .setLeaderEpoch(0)
        ).asJava)
      ).asJava)).build(ApiKeys.READ_SHARE_GROUP_STATE_SUMMARY.latestVersion)

  private def describeShareGroupOffsetsRequest = new DescribeShareGroupOffsetsRequest.Builder(
    new DescribeShareGroupOffsetsRequestData()
      .setGroups(List(new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup()
        .setGroupId(shareGroup)
        .setTopics(List(new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestTopic()
          .setTopicName(topic)
          .setPartitions(List(Integer.valueOf(part)
          ).asJava)
        ).asJava)
      ).asJava)).build(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS.latestVersion)

  private def deleteShareGroupOffsetsRequest = new DeleteShareGroupOffsetsRequest.Builder(
    new DeleteShareGroupOffsetsRequestData()
      .setGroupId(shareGroup)
      .setTopics(List(new DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic()
        .setTopicName(topic)
      ).asJava)).build(ApiKeys.DELETE_SHARE_GROUP_OFFSETS.latestVersion)

  private def alterShareGroupOffsetsRequest = {
    val data = new AlterShareGroupOffsetsRequestData
    val topicCollection = new AlterShareGroupOffsetsRequestData.AlterShareGroupOffsetsRequestTopicCollection()
    topicCollection.add(new AlterShareGroupOffsetsRequestData.AlterShareGroupOffsetsRequestTopic()
      .setTopicName(topic)
      .setPartitions(List(new AlterShareGroupOffsetsRequestData.AlterShareGroupOffsetsRequestPartition()
        .setPartitionIndex(part)
        .setStartOffset(0)
      ).asJava))
    data.setGroupId(shareGroup).setTopics(topicCollection)
    new AlterShareGroupOffsetsRequest.Builder(data).build(ApiKeys.ALTER_SHARE_GROUP_OFFSETS.latestVersion)
  }

  private def streamsGroupHeartbeatRequest = new StreamsGroupHeartbeatRequest.Builder(
    new StreamsGroupHeartbeatRequestData()
      .setGroupId(streamsGroup)
      .setMemberId("member-id")
      .setMemberEpoch(0)
      .setRebalanceTimeoutMs(1000)
      .setActiveTasks(List.empty.asJava)
      .setStandbyTasks(List.empty.asJava)
      .setWarmupTasks(List.empty.asJava)
      .setTopology(new StreamsGroupHeartbeatRequestData.Topology().setSubtopologies(
        List(new StreamsGroupHeartbeatRequestData.Subtopology()
          .setSourceTopics(List(topic).asJava)
        ).asJava
      ))).build(ApiKeys.STREAMS_GROUP_HEARTBEAT.latestVersion)

  private def streamsGroupHeartbeatRequest(
                                             topicAsSourceTopic: Boolean,
                                             topicAsRepartitionSinkTopic: Boolean,
                                             topicAsRepartitionSourceTopic: Boolean,
                                             topicAsStateChangelogTopics: Boolean
                                           ) = new StreamsGroupHeartbeatRequest.Builder(
    new StreamsGroupHeartbeatRequestData()
      .setGroupId(streamsGroup)
      .setMemberId("member-id")
      .setMemberEpoch(0)
      .setRebalanceTimeoutMs(1000)
      .setActiveTasks(List.empty.asJava)
      .setStandbyTasks(List.empty.asJava)
      .setWarmupTasks(List.empty.asJava)
      .setTopology(new StreamsGroupHeartbeatRequestData.Topology().setSubtopologies(
        List(new StreamsGroupHeartbeatRequestData.Subtopology()
          .setSourceTopics(
            (if (topicAsSourceTopic) List(sourceTopic, topic) else List(sourceTopic)).asJava)
          .setRepartitionSinkTopics(
            (if (topicAsRepartitionSinkTopic) List(topic) else List.empty).asJava)
          .setRepartitionSourceTopics(
            (if (topicAsRepartitionSourceTopic) List(new StreamsGroupHeartbeatRequestData.TopicInfo().setName(topic).setPartitions(3)) else List.empty).asJava)
          .setStateChangelogTopics(
            (if (topicAsStateChangelogTopics) List(new StreamsGroupHeartbeatRequestData.TopicInfo().setName(topic)) else List.empty).asJava)
        ).asJava
      ))).build(ApiKeys.STREAMS_GROUP_HEARTBEAT.latestVersion)

  private def streamsGroupDescribeRequest = new StreamsGroupDescribeRequest.Builder(
    new StreamsGroupDescribeRequestData()
      .setGroupIds(List(streamsGroup).asJava)
      .setIncludeAuthorizedOperations(false)).build(ApiKeys.STREAMS_GROUP_DESCRIBE.latestVersion)
  
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
        } else if (resourceToAcls.size > 1) {
          false
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

  @Test
  def testAuthorizationWithTopicExisting(): Unit = {
    //First create the topic so we have a valid topic ID
    createTopicWithBrokerPrincipal(topic)
    val topicId = getTopicIds()(topic)
    assertNotNull(topicId)

    val requestKeyToRequest = mutable.LinkedHashMap[ApiKeys, AbstractRequest](
      ApiKeys.METADATA -> createMetadataRequest(allowAutoTopicCreation = true),
      ApiKeys.PRODUCE -> createProduceRequest("", topicId, ApiKeys.PRODUCE.latestVersion()),
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
      ApiKeys.SHARE_GROUP_HEARTBEAT -> shareGroupHeartbeatRequest,
      ApiKeys.SHARE_GROUP_DESCRIBE -> shareGroupDescribeRequest,
      ApiKeys.SHARE_FETCH -> createShareFetchRequest,
      ApiKeys.SHARE_ACKNOWLEDGE -> shareAcknowledgeRequest,
      ApiKeys.INITIALIZE_SHARE_GROUP_STATE -> initializeShareGroupStateRequest,
      ApiKeys.READ_SHARE_GROUP_STATE -> readShareGroupStateRequest,
      ApiKeys.WRITE_SHARE_GROUP_STATE -> writeShareGroupStateRequest,
      ApiKeys.DELETE_SHARE_GROUP_STATE -> deleteShareGroupStateRequest,
      ApiKeys.READ_SHARE_GROUP_STATE_SUMMARY -> readShareGroupStateSummaryRequest,
      ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS -> describeShareGroupOffsetsRequest,
      ApiKeys.DELETE_SHARE_GROUP_OFFSETS -> deleteShareGroupOffsetsRequest,
      ApiKeys.ALTER_SHARE_GROUP_OFFSETS -> alterShareGroupOffsetsRequest,
      ApiKeys.STREAMS_GROUP_HEARTBEAT -> streamsGroupHeartbeatRequest,
      ApiKeys.STREAMS_GROUP_DESCRIBE -> streamsGroupDescribeRequest,

      // Delete the topic last
      ApiKeys.DELETE_TOPICS -> deleteTopicsRequest
    )

    sendRequests(requestKeyToRequest)
  }

  /*
   * even if the topic doesn't exist, request APIs should not leak the topic name
   */
  @Test
  def testAuthorizationWithTopicNotExisting(): Unit = {
    val id = Uuid.randomUuid()
    val topicNames = Map(id -> "topic")
    val requestKeyToRequest = mutable.LinkedHashMap[ApiKeys, AbstractRequest](
      ApiKeys.METADATA -> createMetadataRequest(allowAutoTopicCreation = false),
      ApiKeys.PRODUCE -> createProduceRequest("", id, ApiKeys.PRODUCE.latestVersion()),
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
      ApiKeys.ELECT_LEADERS -> electLeadersRequest,
      ApiKeys.SHARE_FETCH -> createShareFetchRequest,
      ApiKeys.SHARE_ACKNOWLEDGE -> shareAcknowledgeRequest,
      ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS -> describeShareGroupOffsetsRequest,
      ApiKeys.STREAMS_GROUP_HEARTBEAT -> streamsGroupHeartbeatRequest,
      ApiKeys.STREAMS_GROUP_DESCRIBE -> streamsGroupDescribeRequest
    )

    sendRequests(requestKeyToRequest, topicExists = false, topicNames)
  }

  /**
   * Test that the produce request fails with TOPIC_AUTHORIZATION_FAILED if the client doesn't have permission
   * and topic name is used in the request. Even if the topic doesn't exist, we return TOPIC_AUTHORIZATION_FAILED to
   * prevent leaking the topic name.
   * This case covers produce request version from oldest to 12.
   * The newer version is covered by testAuthorizationWithTopicNotExisting and testAuthorizationWithTopicExisting.
   */
  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testAuthorizationProduceVersionFromOldestTo12(withTopicExisting: Boolean): Unit = {
    if (withTopicExisting) {
      createTopicWithBrokerPrincipal(topic)
    }

    for (version <- ApiKeys.PRODUCE.oldestVersion to 12) {
      val request = createProduceRequest(topic, Uuid.ZERO_UUID, version.toShort)
      val response = connectAndReceive[AbstractResponse](request, listenerName = listenerName)
      val errorCode = response.asInstanceOf[ProduceResponse]
        .data()
        .responses()
        .find(topic, Uuid.ZERO_UUID)
        .partitionResponses.asScala.find(_.index == part).get
        .errorCode

      assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code(), errorCode, s"unexpected error for produce request version $version")
    }
  }

  /**
   * Test that the produce request fails with UNKNOWN_TOPIC_ID if topic id is zero when request version >= 13.
   * The produce request only supports topic id above version 13.
   */
  @Test
  def testZeroTopicIdForProduceVersionFrom13ToNewest(): Unit = {
    for (version <- 13 to ApiKeys.PRODUCE.latestVersion()) {
      val request = createProduceRequest("", Uuid.ZERO_UUID, version.toShort)
      val response = connectAndReceive[AbstractResponse](request, listenerName = listenerName)
      val errorCode = response.asInstanceOf[ProduceResponse]
        .data()
        .responses()
        .find("", Uuid.ZERO_UUID)
        .partitionResponses.asScala.find(_.index == part).get
        .errorCode

      assertEquals(Errors.UNKNOWN_TOPIC_ID.code(), errorCode, s"unexpected error for produce request version $version")
    }
  }

  /**
   * Test that the produce request fails with TOPIC_AUTHORIZATION_FAILED if topic name is empty when request version <= 12.
   * The produce request only supports topic name below version 12.
   */
  @Test
  def testEmptyTopicNameForProduceVersionFromOldestTo12(): Unit = {
    for (version <- ApiKeys.PRODUCE.oldestVersion() to 12) {
      val request = createProduceRequest("", Uuid.ZERO_UUID, version.toShort)
      val response = connectAndReceive[AbstractResponse](request, listenerName = listenerName)
      val errorCode = response.asInstanceOf[ProduceResponse]
        .data()
        .responses()
        .find("", Uuid.ZERO_UUID)
        .partitionResponses.asScala.find(_.index == part).get
        .errorCode

      assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code(), errorCode, s"unexpected error for produce request version $version")
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testTopicIdAuthorization(withTopicExisting: Boolean): Unit = {
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

  /**
   * Test that the fetch request fails with TOPIC_AUTHORIZATION_FAILED if the client doesn't have permission
   * and topic name is used in the request. Even if the topic doesn't exist, we return TOPIC_AUTHORIZATION_FAILED to
   * prevent leaking the topic name.
   * This case covers fetch request version from oldest to 12.
   * The newer version is covered by testAuthorizationWithTopicNotExisting and testAuthorizationWithTopicExisting.
   */
  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testAuthorizationFetchVersionFromOldestTo12(withTopicExisting: Boolean): Unit = {
    if (withTopicExisting) {
      createTopicWithBrokerPrincipal(topic)
    }

    val id = Uuid.ZERO_UUID
    val topicNames = Map(id -> topic)
    for (version <- ApiKeys.FETCH.oldestVersion to 12) {
      val requestKeyToRequest = mutable.LinkedHashMap[ApiKeys, AbstractRequest](
        ApiKeys.FETCH -> createFetchRequestWithUnknownTopic(id, version.toShort),
      )

      sendRequests(requestKeyToRequest, withTopicExisting, topicNames)
    }
  }

  /**
   * Test that the fetch request fails with UNKNOWN_TOPIC_ID if topic id is zero when request version >= 13.
   * The fetch request only supports topic id above version 13.
   */
  @Test
  def testZeroTopicIdForFetchVersionFrom13ToNewest(): Unit = {
    for (version <- 13 to ApiKeys.FETCH.latestVersion) {
      val request = createFetchRequestWithEmptyTopicNameAndZeroTopicId(version.toShort)
      val response = connectAndReceive[AbstractResponse](request, listenerName = listenerName)

      val errorCode = response.asInstanceOf[FetchResponse]
        .data()
        .responses()
        .get(0)
        .partitions()
        .get(0)
        .errorCode

      assertEquals(Errors.UNKNOWN_TOPIC_ID.code(), errorCode, s"unexpected error for fetch request version $version")
    }
  }

  /**
   * Test that the fetch request fails with TOPIC_AUTHORIZATION_FAILED if topic name is empty when request version <= 12.
   * The fetch request only supports topic name below version 12.
   */
  @Test
  def testEmptyTopicNameForFetchVersionFromOldestTo12(): Unit = {
    for (version <- ApiKeys.FETCH.oldestVersion to 12) {
      val request = createFetchRequestWithEmptyTopicNameAndZeroTopicId(version.toShort)
      val response = connectAndReceive[AbstractResponse](request, listenerName = listenerName)

      val errorCode = response.asInstanceOf[FetchResponse]
        .data()
        .responses()
        .get(0)
        .partitions()
        .get(0)
        .errorCode

      assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code(), errorCode, s"unexpected error for fetch request version $version")
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

  @Test
  def testFetchConsumerRequest(): Unit = {
    createTopicWithBrokerPrincipal(topic)

    val request = createFetchRequest
    val topicNames = getTopicNames().asJava

    def partitionDatas(response: AbstractResponse): Iterable[FetchResponseData.PartitionData] = {
      assertTrue(response.isInstanceOf[FetchResponse])
      response.asInstanceOf[FetchResponse].responseData(topicNames, ApiKeys.FETCH.latestVersion).values().asScala
    }

    removeAllClientAcls()
    val resources = Set(topicResource.resourceType, clusterResource.resourceType)
    val failedResponse = sendRequestAndVerifyResponseError(request, resources, isAuthorized = false)
    val failedPartitionDatas = partitionDatas(failedResponse)
    assertEquals(1, failedPartitionDatas.size)
    // Some clients (like librdkafka) always expect non-null records - even for the cases where an error is returned
    failedPartitionDatas.foreach(partitionData => assertEquals(MemoryRecords.EMPTY, partitionData.records))

    val readAcls = topicReadAcl(topicResource)
    addAndVerifyAcls(readAcls, topicResource)
    val succeededResponse = sendRequestAndVerifyResponseError(request, resources, isAuthorized = true)
    val succeededPartitionDatas = partitionDatas(succeededResponse)
    assertEquals(1, succeededPartitionDatas.size)
    succeededPartitionDatas.foreach(partitionData => assertEquals(MemoryRecords.EMPTY, partitionData.records))
  }

  @Test
  def testIncrementalAlterConfigsRequestRequiresClusterPermissionForBrokerLogger(): Unit = {
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

  @Test
  def testOffsetsForLeaderEpochClusterPermission(): Unit = {
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

  @Test
  def testProduceWithNoTopicAccess(): Unit = {
    createTopicWithBrokerPrincipal(topic)
    val producer = createProducer()
    assertThrows(classOf[TopicAuthorizationException], () => sendRecords(producer, numRecords, tp))
  }

  @Test
  def testProduceWithTopicDescribe(): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), topicResource)
    val producer = createProducer()
    assertThrows(classOf[TopicAuthorizationException], () => sendRecords(producer, numRecords, tp))
  }

  @Test
  def testProduceWithTopicRead(): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)
    val producer = createProducer()
    assertThrows(classOf[TopicAuthorizationException], () => sendRecords(producer, numRecords, tp))
  }

  @Test
  def testProduceWithTopicWrite(): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
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
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), newTopicResource)
    val producer = createProducer()
    val e = assertThrows(classOf[TopicAuthorizationException], () => sendRecords(producer, numRecords, tp))
    assertEquals(util.Set.of(tp.topic), e.unauthorizedTopics())

    val resource = if (resType == ResourceType.TOPIC) newTopicResource else clusterResource
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, CREATE, ALLOW)), resource)

    sendRecords(producer, numRecords, tp)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testConsumeUsingAssignWithNoAccess(groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllClientAcls()

    // Remove the group.id configuration since this self-assigning partitions.
    val consumer = createConsumer(configsToRemove = List(ConsumerConfig.GROUP_ID_CONFIG)) 
    consumer.assign(java.util.List.of(tp))
    assertThrows(classOf[TopicAuthorizationException], () => consumeRecords(consumer))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testSimpleConsumeWithOffsetLookupAndNoGroupAccess(groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllClientAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)

    // note this still depends on group access because we haven't set offsets explicitly, which means
    // they will first be fetched from the consumer coordinator (which requires group access)
    val consumer = createConsumer()
    consumer.assign(java.util.List.of(tp))
    val e = assertThrows(classOf[GroupAuthorizationException], () => consumeRecords(consumer))
    assertEquals(group, e.groupId())
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testSimpleConsumeWithExplicitSeekAndNoGroupAccess(groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllClientAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)

    // in this case, we do an explicit seek, so there should be no need to query the coordinator at all
    // remove the group.id config to avoid coordinator created
    val consumer = createConsumer(configsToRemove = List(ConsumerConfig.GROUP_ID_CONFIG))
    consumer.assign(java.util.List.of(tp))
    consumer.seekToBeginning(java.util.List.of(tp))
    consumeRecords(consumer)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testConsumeWithoutTopicDescribeAccess(groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllClientAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)
    val consumer = createConsumer()
    consumer.assign(java.util.List.of(tp))

    val e = assertThrows(classOf[TopicAuthorizationException], () => consumeRecords(consumer))
    assertEquals(util.Set.of(topic), e.unauthorizedTopics())
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testConsumeWithTopicDescribe(groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllClientAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)

    val consumer = createConsumer()
    consumer.assign(java.util.List.of(tp))
    val e = assertThrows(classOf[TopicAuthorizationException], () => consumeRecords(consumer))
    assertEquals(util.Set.of(topic), e.unauthorizedTopics())
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testConsumeWithTopicWrite(groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllClientAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)

    val consumer = createConsumer()
    consumer.assign(java.util.List.of(tp))
    val e = assertThrows(classOf[TopicAuthorizationException], () => consumeRecords(consumer))
    assertEquals(util.Set.of(topic), e.unauthorizedTopics())
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testConsumeWithTopicAndGroupRead(groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
    val producer = createProducer()
    sendRecords(producer, 1, tp)
    removeAllClientAcls()

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)

    val consumer = createConsumer()
    consumer.assign(java.util.List.of(tp))
    consumeRecords(consumer)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testPatternSubscriptionWithNoTopicAccess(groupProtocol: String): Unit = {
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

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testPatternSubscriptionWithTopicDescribeOnlyAndGroupRead(groupProtocol: String): Unit = {
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
    assertEquals(util.Set.of(topic), e.unauthorizedTopics())
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testPatternSubscriptionWithTopicAndGroupRead(groupProtocol: String): Unit = {
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

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testPatternSubscriptionMatchingInternalTopic(groupProtocol: String): Unit = {
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
    assertEquals(java.util.Set.of(topic), consumer.subscription)

    // now authorize the user for the internal topic and verify that we can subscribe
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), new ResourcePattern(TOPIC,
      GROUP_METADATA_TOPIC_NAME, LITERAL))
    consumer.subscribe(Pattern.compile(GROUP_METADATA_TOPIC_NAME))
    TestUtils.retry(60000) {
      consumer.poll(Duration.ofMillis(500))
      assertEquals(Set(GROUP_METADATA_TOPIC_NAME), consumer.subscription.asScala)
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testPatternSubscriptionMatchingInternalTopicWithDescribeOnlyPermission(groupProtocol: String): Unit = {
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
    assertEquals(util.Set.of(GROUP_METADATA_TOPIC_NAME), e.unauthorizedTopics())
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testPatternSubscriptionNotMatchingInternalTopic(groupProtocol: String): Unit = {
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

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testCreatePermissionOnTopicToReadFromNonExistentTopic(groupProtocol: String): Unit = {
    testCreatePermissionNeededToReadFromNonExistentTopic("newTopic",
      Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, CREATE, ALLOW)),
      TOPIC)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testCreatePermissionOnClusterToReadFromNonExistentTopic(groupProtocol: String): Unit = {
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
    consumer.assign(java.util.List.of(topicPartition))
    val unauthorizedTopics = assertThrows(classOf[TopicAuthorizationException],
      () => (0 until 10).foreach(_ => consumer.poll(Duration.ofMillis(50L)))).unauthorizedTopics
    assertEquals(util.Set.of(newTopic), unauthorizedTopics)

    val resource = if (resType == TOPIC) newTopicResource else clusterResource
    addAndVerifyAcls(acls, resource)

    waitUntilTrue(() => {
      consumer.poll(Duration.ofMillis(50L))
      brokers.forall { broker =>
        OptionConverters.toScala(broker.metadataCache.getLeaderAndIsr(newTopic, 0)) match {
          case Some(partitionState) => FetchRequest.isValidBrokerId(partitionState.leader)
          case _ => false
        }
      }
    }, "Partition metadata not propagated.")
  }

  @Test
  def testCreatePermissionMetadataRequestAutoCreate(): Unit = {
    val readAcls = topicReadAcl(topicResource)
    addAndVerifyAcls(readAcls, topicResource)
    brokers.foreach(b => assertEquals(Optional.empty, b.metadataCache.getLeaderAndIsr(topic, 0)))

    val metadataRequest = new MetadataRequest.Builder(java.util.List.of(topic), true).build()
    val metadataResponse = connectAndReceive[MetadataResponse](metadataRequest)

    assertEquals(java.util.Set.of(), metadataResponse.topicsByError(Errors.NONE))

    val createAcls = topicCreateAcl(topicResource)
    addAndVerifyAcls(createAcls, topicResource)

    // retry as topic being created can have MetadataResponse with Errors.LEADER_NOT_AVAILABLE
    TestUtils.retry(JTestUtils.DEFAULT_MAX_WAIT_MS) {
      val metadataResponse = connectAndReceive[MetadataResponse](metadataRequest)
      assertEquals(java.util.Set.of(topic), metadataResponse.topicsByError(Errors.NONE))
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testCommitWithNoAccess(groupProtocol: String): Unit = {
    val consumer = createConsumer()
    assertThrows(classOf[GroupAuthorizationException], () => consumer.commitSync(java.util.Map.of(tp, new OffsetAndMetadata(5))))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testCommitWithNoTopicAccess(groupProtocol: String): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)
    val consumer = createConsumer()
    assertThrows(classOf[TopicAuthorizationException], () => consumer.commitSync(java.util.Map.of(tp, new OffsetAndMetadata(5))))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testCommitWithTopicWrite(groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
    val consumer = createConsumer()
    assertThrows(classOf[TopicAuthorizationException], () => consumer.commitSync(java.util.Map.of(tp, new OffsetAndMetadata(5))))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testCommitWithTopicDescribe(groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), topicResource)
    val consumer = createConsumer()
    assertThrows(classOf[TopicAuthorizationException], () => consumer.commitSync(java.util.Map.of(tp, new OffsetAndMetadata(5))))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testCommitWithNoGroupAccess(groupProtocol: String): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)
    val consumer = createConsumer()
    assertThrows(classOf[GroupAuthorizationException], () => consumer.commitSync(java.util.Map.of(tp, new OffsetAndMetadata(5))))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testCommitWithTopicAndGroupRead(groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.commitSync(java.util.Map.of(tp, new OffsetAndMetadata(5)))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testOffsetFetchWithNoAccess(groupProtocol: String): Unit = {
    val consumer = createConsumer()
    consumer.assign(java.util.List.of(tp))
    assertThrows(classOf[TopicAuthorizationException], () => consumer.position(tp))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testOffsetFetchWithNoGroupAccess(groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.assign(java.util.List.of(tp))
    assertThrows(classOf[GroupAuthorizationException], () => consumer.position(tp))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testOffsetFetchWithNoTopicAccess(groupProtocol: String): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)
    val consumer = createConsumer()
    consumer.assign(java.util.List.of(tp))
    assertThrows(classOf[TopicAuthorizationException], () => consumer.position(tp))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testOffsetFetchAllTopicPartitionsAuthorization(groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    val offset = 15L
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.assign(java.util.List.of(tp))
    consumer.commitSync(java.util.Map.of(tp, new OffsetAndMetadata(offset)))

    removeAllClientAcls()
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)

    // send offset fetch requests directly since the consumer does not expose an API to do so
    // note there's only one broker, so no need to lookup the group coordinator

    // without describe permission on the topic, we shouldn't be able to fetch offsets
    val offsetFetchRequest = createOffsetFetchRequestAllPartitions
    var offsetFetchResponse = connectAndReceive[OffsetFetchResponse](offsetFetchRequest)
    assertEquals(Errors.NONE, Errors.forCode(offsetFetchResponse.group(group).errorCode()))
    assertTrue(offsetFetchResponse.group(group).topics.isEmpty)

    // now add describe permission on the topic and verify that the offset can be fetched
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), topicResource)
    offsetFetchResponse = connectAndReceive[OffsetFetchResponse](offsetFetchRequest)
    assertEquals(Errors.NONE, Errors.forCode(offsetFetchResponse.group(group).errorCode()))
    assertEquals(
      offset,
      offsetFetchResponse.group(group).topics.asScala
        .find(_.name == tp.topic)
        .flatMap(_.partitions.asScala.find(_.partitionIndex == tp.partition).map(_.committedOffset))
        .getOrElse(-1L)
    )
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testOffsetFetchMultipleGroupsAuthorization(groupProtocol: String): Unit = {
    val groups: Seq[String] = (1 to 5).map(i => s"group$i")
    val groupResources = groups.map(group => new ResourcePattern(GROUP, group, LITERAL))
    val topics: Seq[String] = (1 to 3).map(i => s"topic$i")
    val topicResources = topics.map(topic => new ResourcePattern(TOPIC, topic, LITERAL))

    val topic1List = util.List.of(new TopicPartition(topics(0), 0))
    val topic1And2List = util.List.of(
      new TopicPartition(topics(0), 0),
      new TopicPartition(topics(1), 0),
      new TopicPartition(topics(1), 1))
    val allTopicsList = util.List.of(
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
    groupResources.foreach { r =>
      addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), r)
    }
    topicResources.foreach { t =>
      addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), t)
    }

    val offset = 15L
    val leaderEpoch: Optional[Integer] = Optional.of(1)
    val metadata = "metadata"

    def assertResponse(
      expected: OffsetFetchResponseData.OffsetFetchResponseGroup,
      actual: OffsetFetchResponseData.OffsetFetchResponseGroup
    ): Unit = {
      actual.topics.sort((t1, t2) => t1.name.compareTo(t2.name))
      actual.topics.asScala.foreach { topic =>
        topic.partitions.sort(Comparator.comparingInt[OffsetFetchResponseData.OffsetFetchResponsePartitions](_.partitionIndex))
      }

      assertEquals(expected, actual)
    }

    def commitOffsets(tpList: util.List[TopicPartition]): Unit = {
      val consumer = createConsumer()
      consumer.assign(tpList)
      val offsets = tpList.asScala.map {
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

    // test handling partial errors, where one group is fully authorized, some groups don't have
    // the right topic authorizations, and some groups have no authorization
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResources(0))
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResources(1))
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResources(3))
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), topicResources(0))

    val offsetFetchRequest = createOffsetFetchRequest(groupToPartitionMap)
    var offsetFetchResponse = connectAndReceive[OffsetFetchResponse](offsetFetchRequest)

    offsetFetchResponse.data.groups.forEach { g =>
      g.groupId match {
        case "group1" =>
          assertResponse(
            new OffsetFetchResponseData.OffsetFetchResponseGroup()
              .setGroupId(g.groupId)
              .setTopics(List(
                new OffsetFetchResponseData.OffsetFetchResponseTopics()
                  .setName(topics(0))
                  .setPartitions(List(
                    new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                      .setPartitionIndex(0)
                      .setCommittedOffset(offset)
                      .setCommittedLeaderEpoch(leaderEpoch.get)
                      .setMetadata(metadata)
                  ).asJava)
              ).asJava),
            offsetFetchResponse.group(g.groupId)
          )

        case "group2" =>
          assertResponse(
            new OffsetFetchResponseData.OffsetFetchResponseGroup()
              .setGroupId(g.groupId)
              .setTopics(List(
                new OffsetFetchResponseData.OffsetFetchResponseTopics()
                  .setName(topics(0))
                  .setPartitions(List(
                    new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                      .setPartitionIndex(0)
                      .setCommittedOffset(offset)
                      .setCommittedLeaderEpoch(leaderEpoch.get)
                      .setMetadata(metadata)
                  ).asJava),
                new OffsetFetchResponseData.OffsetFetchResponseTopics()
                  .setName(topics(1))
                  .setPartitions(List(
                    new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                      .setPartitionIndex(0)
                      .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
                      .setCommittedOffset(OffsetFetchResponse.INVALID_OFFSET)
                      .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                      .setMetadata(OffsetFetchResponse.NO_METADATA),
                    new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                      .setPartitionIndex(1)
                      .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
                      .setCommittedOffset(OffsetFetchResponse.INVALID_OFFSET)
                      .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                      .setMetadata(OffsetFetchResponse.NO_METADATA)
                  ).asJava)
              ).asJava),
            offsetFetchResponse.group(g.groupId)
          )

        case "group3" =>
          assertResponse(
            new OffsetFetchResponseData.OffsetFetchResponseGroup()
              .setGroupId(g.groupId)
              .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code),
            offsetFetchResponse.group(g.groupId)
          )

        case "group4" =>
          assertResponse(
            new OffsetFetchResponseData.OffsetFetchResponseGroup()
              .setGroupId(g.groupId)
              .setTopics(List(
                new OffsetFetchResponseData.OffsetFetchResponseTopics()
                  .setName(topics(0))
                  .setPartitions(List(
                    new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                      .setPartitionIndex(0)
                      .setCommittedOffset(offset)
                      .setCommittedLeaderEpoch(leaderEpoch.get)
                      .setMetadata(metadata)
                  ).asJava)
              ).asJava),
            offsetFetchResponse.group(g.groupId)
          )

        case "group5" =>
          assertResponse(
            new OffsetFetchResponseData.OffsetFetchResponseGroup()
              .setGroupId(g.groupId)
              .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code),
            offsetFetchResponse.group(g.groupId)
          )
      }
    }

    // test that after adding some of the ACLs, we get no group level authorization errors, but
    // still get topic level authorization errors for topics we don't have ACLs for
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResources(2))
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResources(4))
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), topicResources(1))

    offsetFetchResponse = connectAndReceive[OffsetFetchResponse](offsetFetchRequest)

    offsetFetchResponse.data.groups.forEach { g =>
      g.groupId match {
        case "group1" =>
          assertResponse(
            new OffsetFetchResponseData.OffsetFetchResponseGroup()
              .setGroupId(g.groupId)
              .setTopics(List(
                new OffsetFetchResponseData.OffsetFetchResponseTopics()
                  .setName(topics(0))
                  .setPartitions(List(
                    new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                      .setPartitionIndex(0)
                      .setCommittedOffset(offset)
                      .setCommittedLeaderEpoch(leaderEpoch.get)
                      .setMetadata(metadata)
                  ).asJava)
              ).asJava),
            offsetFetchResponse.group(g.groupId)
          )

        case "group2" =>
          assertResponse(
            new OffsetFetchResponseData.OffsetFetchResponseGroup()
              .setGroupId(g.groupId)
              .setTopics(List(
                new OffsetFetchResponseData.OffsetFetchResponseTopics()
                  .setName(topics(0))
                  .setPartitions(List(
                    new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                      .setPartitionIndex(0)
                      .setCommittedOffset(offset)
                      .setCommittedLeaderEpoch(leaderEpoch.get)
                      .setMetadata(metadata)
                  ).asJava),
                new OffsetFetchResponseData.OffsetFetchResponseTopics()
                  .setName(topics(1))
                  .setPartitions(List(
                    new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                      .setPartitionIndex(0)
                      .setCommittedOffset(offset)
                      .setCommittedLeaderEpoch(leaderEpoch.get)
                      .setMetadata(metadata),
                    new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                      .setPartitionIndex(1)
                      .setCommittedOffset(offset)
                      .setCommittedLeaderEpoch(leaderEpoch.get)
                      .setMetadata(metadata)
                  ).asJava)
              ).asJava),
            offsetFetchResponse.group(g.groupId)
          )

        case "group3" =>
          assertResponse(
            new OffsetFetchResponseData.OffsetFetchResponseGroup()
              .setGroupId(g.groupId)
              .setTopics(List(
                new OffsetFetchResponseData.OffsetFetchResponseTopics()
                  .setName(topics(0))
                  .setPartitions(List(
                    new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                      .setPartitionIndex(0)
                      .setCommittedOffset(offset)
                      .setCommittedLeaderEpoch(leaderEpoch.get)
                      .setMetadata(metadata)
                  ).asJava),
                new OffsetFetchResponseData.OffsetFetchResponseTopics()
                  .setName(topics(1))
                  .setPartitions(List(
                    new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                      .setPartitionIndex(0)
                      .setCommittedOffset(offset)
                      .setCommittedLeaderEpoch(leaderEpoch.get)
                      .setMetadata(metadata),
                    new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                      .setPartitionIndex(1)
                      .setCommittedOffset(offset)
                      .setCommittedLeaderEpoch(leaderEpoch.get)
                      .setMetadata(metadata)
                  ).asJava),
                new OffsetFetchResponseData.OffsetFetchResponseTopics()
                  .setName(topics(2))
                  .setPartitions(List(
                    new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                      .setPartitionIndex(0)
                      .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
                      .setCommittedOffset(OffsetFetchResponse.INVALID_OFFSET)
                      .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                      .setMetadata(OffsetFetchResponse.NO_METADATA),
                    new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                      .setPartitionIndex(1)
                      .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
                      .setCommittedOffset(OffsetFetchResponse.INVALID_OFFSET)
                      .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                      .setMetadata(OffsetFetchResponse.NO_METADATA),
                    new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                      .setPartitionIndex(2)
                      .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
                      .setCommittedOffset(OffsetFetchResponse.INVALID_OFFSET)
                      .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                      .setMetadata(OffsetFetchResponse.NO_METADATA)
                  ).asJava)
              ).asJava),
            offsetFetchResponse.group(g.groupId)
          )

        case "group4" =>
          assertResponse(
            new OffsetFetchResponseData.OffsetFetchResponseGroup()
              .setGroupId(g.groupId)
              .setTopics(List(
                new OffsetFetchResponseData.OffsetFetchResponseTopics()
                  .setName(topics(0))
                  .setPartitions(List(
                    new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                      .setPartitionIndex(0)
                      .setCommittedOffset(offset)
                      .setCommittedLeaderEpoch(leaderEpoch.get)
                      .setMetadata(metadata)
                  ).asJava),
                new OffsetFetchResponseData.OffsetFetchResponseTopics()
                  .setName(topics(1))
                  .setPartitions(List(
                    new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                      .setPartitionIndex(0)
                      .setCommittedOffset(offset)
                      .setCommittedLeaderEpoch(leaderEpoch.get)
                      .setMetadata(metadata),
                    new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                      .setPartitionIndex(1)
                      .setCommittedOffset(offset)
                      .setCommittedLeaderEpoch(leaderEpoch.get)
                      .setMetadata(metadata)
                  ).asJava)
              ).asJava),
            offsetFetchResponse.group(g.groupId)
          )

        case "group5" =>
          assertResponse(
            new OffsetFetchResponseData.OffsetFetchResponseGroup()
              .setGroupId(g.groupId)
              .setTopics(List(
                new OffsetFetchResponseData.OffsetFetchResponseTopics()
                  .setName(topics(0))
                  .setPartitions(List(
                    new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                      .setPartitionIndex(0)
                      .setCommittedOffset(offset)
                      .setCommittedLeaderEpoch(leaderEpoch.get)
                      .setMetadata(metadata)
                  ).asJava),
                new OffsetFetchResponseData.OffsetFetchResponseTopics()
                  .setName(topics(1))
                  .setPartitions(List(
                    new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                      .setPartitionIndex(0)
                      .setCommittedOffset(offset)
                      .setCommittedLeaderEpoch(leaderEpoch.get)
                      .setMetadata(metadata),
                    new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                      .setPartitionIndex(1)
                      .setCommittedOffset(offset)
                      .setCommittedLeaderEpoch(leaderEpoch.get)
                      .setMetadata(metadata)
                  ).asJava)
              ).asJava),
            offsetFetchResponse.group(g.groupId)
          )
      }
    }

    // test that after adding all necessary ACLs, we get no partition level or group level errors
    // from the offsetFetch response
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), topicResources(2))
    offsetFetchResponse = connectAndReceive[OffsetFetchResponse](offsetFetchRequest)
    offsetFetchResponse.data.groups.forEach { group =>
      assertEquals(Errors.NONE.code, group.errorCode)
      group.topics.forEach { topic =>
        topic.partitions.forEach { partition =>
          assertEquals(Errors.NONE.code, partition.errorCode)
        }
      }
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testOffsetFetchTopicDescribe(groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.assign(java.util.List.of(tp))
    consumer.position(tp)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testOffsetFetchWithTopicAndGroupRead(groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.assign(java.util.List.of(tp))
    consumer.position(tp)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testMetadataWithNoTopicAccess(groupProtocol: String): Unit = {
    val consumer = createConsumer()
    assertThrows(classOf[TopicAuthorizationException], () => consumer.partitionsFor(topic))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testMetadataWithTopicDescribe(groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.partitionsFor(topic)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testListOffsetsWithNoTopicAccess(groupProtocol: String): Unit = {
    val consumer = createConsumer()
    assertThrows(classOf[TopicAuthorizationException], () => consumer.endOffsets(java.util.Set.of(tp)))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testListOffsetsWithTopicDescribe(groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.endOffsets(java.util.Set.of(tp))
  }

  @Test
  def testDescribeGroupApiWithNoGroupAcl(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), topicResource)
    val result = createAdminClient().describeConsumerGroups(java.util.List.of(group))
    JTestUtils.assertFutureThrows(classOf[GroupAuthorizationException], result.describedGroups().get(group))
  }

  @Test
  def testDescribeGroupApiWithGroupDescribe(): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), topicResource)
    val result = createAdminClient().describeConsumerGroups(java.util.List.of(group))
    JTestUtils.assertFutureThrows(classOf[GroupIdNotFoundException], result.describedGroups().get(group))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testListGroupApiWithAndWithoutListGroupAcls(groupProtocol: String): Unit = {
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
    consumer.subscribe(util.Set.of(topic))
    consumeRecords(consumer)

    val otherConsumerProps = new Properties
    otherConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, group2)
    val otherConsumer = createConsumer(configOverrides = otherConsumerProps)
    otherConsumer.subscribe(util.Set.of(topic))
    consumeRecords(otherConsumer)

    val adminClient = createAdminClient()

    // first use cluster describe permission
    removeAllClientAcls()
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), clusterResource)
    // it should list both groups (due to cluster describe permission)
    assertEquals(Set(group, group2), adminClient.listGroups(ListGroupsOptions.forConsumerGroups()).all().get().asScala.map(_.groupId()).toSet)

    // now replace cluster describe with group read permission
    removeAllClientAcls()
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)
    // it should list only one group now
    val groupList = adminClient.listGroups(ListGroupsOptions.forConsumerGroups()).all().get().asScala.toList
    assertEquals(1, groupList.length)
    assertEquals(group, groupList.head.groupId)

    // now remove all acls and verify describe group access is required to list any group
    removeAllClientAcls()
    val listGroupResult = adminClient.listGroups(ListGroupsOptions.forConsumerGroups())
    assertEquals(List(), listGroupResult.errors().get().asScala.toList)
    assertEquals(List(), listGroupResult.all().get().asScala.toList)
    otherConsumer.close()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testDeleteGroupApiWithDeleteGroupAcl(groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DELETE, ALLOW)), groupResource)
    val consumer = createConsumer()
    consumer.assign(java.util.List.of(tp))
    consumer.commitSync(java.util.Map.of(tp, new OffsetAndMetadata(5, "")))
    createAdminClient().deleteConsumerGroups(java.util.List.of(group)).deletedGroups().get(group).get()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testDeleteGroupApiWithNoDeleteGroupAcl(groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.assign(java.util.List.of(tp))
    consumer.commitSync(java.util.Map.of(tp, new OffsetAndMetadata(5, "")))
    val result = createAdminClient().deleteConsumerGroups(java.util.List.of(group))
    JTestUtils.assertFutureThrows(classOf[GroupAuthorizationException], result.deletedGroups().get(group))
  }

  @Test
  def testDeleteGroupApiWithNoDeleteGroupAcl2(): Unit = {
    val result = createAdminClient().deleteConsumerGroups(java.util.List.of(group))
    JTestUtils.assertFutureThrows(classOf[GroupAuthorizationException], result.deletedGroups().get(group))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testDeleteGroupOffsetsWithAcl(groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DELETE, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.assign(java.util.List.of(tp))
    consumer.commitSync(java.util.Map.of(tp, new OffsetAndMetadata(5, "")))
    consumer.close()
    val result = createAdminClient().deleteConsumerGroupOffsets(group, java.util.Set.of(tp))
    assertNull(result.partitionResult(tp).get())
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testDeleteGroupOffsetsWithoutDeleteAcl(groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.assign(java.util.List.of(tp))
    consumer.commitSync(java.util.Map.of(tp, new OffsetAndMetadata(5, "")))
    consumer.close()
    val result = createAdminClient().deleteConsumerGroupOffsets(group, java.util.Set.of(tp))
    JTestUtils.assertFutureThrows(classOf[GroupAuthorizationException], result.all())
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testDeleteGroupOffsetsWithDeleteAclWithoutTopicAcl(groupProtocol: String): Unit = {
    createTopicWithBrokerPrincipal(topic)
    // Create the consumer group
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)
    val consumer = createConsumer()
    consumer.assign(java.util.List.of(tp))
    consumer.commitSync(java.util.Map.of(tp, new OffsetAndMetadata(5, "")))
    consumer.close()

    // Remove the topic ACL & Check that it does not work without it
    removeAllClientAcls()
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DELETE, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)
    val result = createAdminClient().deleteConsumerGroupOffsets(group, java.util.Set.of(tp))
    JTestUtils.assertFutureThrows(classOf[TopicAuthorizationException], result.all())
    JTestUtils.assertFutureThrows(classOf[TopicAuthorizationException], result.partitionResult(tp))
  }

  @Test
  def testDeleteGroupOffsetsWithNoAcl(): Unit = {
    val result = createAdminClient().deleteConsumerGroupOffsets(group, java.util.Set.of(tp))
    JTestUtils.assertFutureThrows(classOf[GroupAuthorizationException], result.all())
  }

  @Test
  def testIncrementalAlterGroupConfigsWithAlterAcl(): Unit = {
    addAndVerifyAcls(groupAlterConfigsAcl(groupResource), groupResource)

    val request = incrementalAlterGroupConfigsRequest
    val resource = Set[ResourceType](GROUP)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @Test
  def testIncrementalAlterGroupConfigsWithOperationAll(): Unit = {
    val allowAllOpsAcl = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALL, ALLOW)
    addAndVerifyAcls(Set(allowAllOpsAcl), groupResource)

    val request = incrementalAlterGroupConfigsRequest
    val resource = Set[ResourceType](GROUP)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @Test
  def testIncrementalAlterGroupConfigsWithoutAlterAcl(): Unit = {
    removeAllClientAcls()

    val request = incrementalAlterGroupConfigsRequest
    val resource = Set[ResourceType](GROUP)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = false)
  }

  @Test
  def testDescribeGroupConfigsWithDescribeAcl(): Unit = {
    addAndVerifyAcls(groupDescribeConfigsAcl(groupResource), groupResource)

    val request = describeGroupConfigsRequest
    val resource = Set[ResourceType](GROUP)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @Test
  def testDescribeGroupConfigsWithOperationAll(): Unit = {
    val allowAllOpsAcl = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALL, ALLOW)
    addAndVerifyAcls(Set(allowAllOpsAcl), groupResource)

    val request = describeGroupConfigsRequest
    val resource = Set[ResourceType](GROUP)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @Test
  def testDescribeGroupConfigsWithoutDescribeAcl(): Unit = {
    removeAllClientAcls()

    val request = describeGroupConfigsRequest
    val resource = Set[ResourceType](GROUP)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = false)
  }

  @Test
  def testUnauthorizedDeleteTopicsWithoutDescribe(): Unit = {
    val deleteResponse = connectAndReceive[DeleteTopicsResponse](deleteTopicsRequest)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, deleteResponse.data.responses.find(topic).errorCode)
  }

  @Test
  def testUnauthorizedDeleteTopicsWithDescribe(): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), topicResource)
    val deleteResponse = connectAndReceive[DeleteTopicsResponse](deleteTopicsRequest)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, deleteResponse.data.responses.find(topic).errorCode)
  }

  @Test
  def testDeleteTopicsWithWildCardAuth(): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DELETE, ALLOW)), new ResourcePattern(TOPIC, "*", LITERAL))
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
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), topicResource)
    val deleteRecordsResponse = connectAndReceive[DeleteRecordsResponse](deleteRecordsRequest)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, deleteRecordsResponse.data.topics.asScala.head.
      partitions.asScala.head.errorCode)
  }

  @Test
  def testDeleteRecordsWithWildCardAuth(): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DELETE, ALLOW)), new ResourcePattern(TOPIC, "*", LITERAL))
    val deleteRecordsResponse = connectAndReceive[DeleteRecordsResponse](deleteRecordsRequest)
    assertEquals(Errors.NONE.code, deleteRecordsResponse.data.topics.asScala.head.
      partitions.asScala.head.errorCode)
  }

  @Test
  def testUnauthorizedCreatePartitions(): Unit = {
    val createPartitionsResponse = connectAndReceive[CreatePartitionsResponse](createPartitionsRequest)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, createPartitionsResponse.data.results.asScala.head.errorCode)
  }

  @Test
  def testCreatePartitionsWithWildCardAuth(): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALTER, ALLOW)), new ResourcePattern(TOPIC, "*", LITERAL))
    val createPartitionsResponse = connectAndReceive[CreatePartitionsResponse](createPartitionsRequest)
    assertEquals(Errors.NONE.code, createPartitionsResponse.data.results.asScala.head.errorCode)
  }

  @Test
  def testTransactionalProducerInitTransactionsNoWriteTransactionalIdAcl(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), transactionalIdResource)
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
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, CLUSTER_ACTION, ALLOW)), clusterResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), transactionalIdResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    producer.beginTransaction()

    assertThrows(classOf[GroupAuthorizationException],
      () => producer.sendOffsetsToTransaction(java.util.Map.of(tp, new OffsetAndMetadata(0L)), new ConsumerGroupMetadata(group)))
  }

  @Test
  def testSendOffsetsWithNoConsumerGroupWriteAccess(): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), transactionalIdResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), groupResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    producer.beginTransaction()

    assertThrows(classOf[GroupAuthorizationException],
      () => producer.sendOffsetsToTransaction(java.util.Map.of(tp, new OffsetAndMetadata(0L)), new ConsumerGroupMetadata(group)))
  }

  @Test
  def testIdempotentProducerNoIdempotentWriteAclInInitProducerId(): Unit = {
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

  @Test
  def testIdempotentProducerNoIdempotentWriteAclInProduce(): Unit = {
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

  @Test
  def shouldInitTransactionsWhenAclSet(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), transactionalIdResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
  }

  @Test
  def testTransactionalProducerTopicAuthorizationExceptionInSendCallback(): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), transactionalIdResource)
    // add describe access so that we can fetch metadata
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, DESCRIBE, ALLOW)), topicResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    producer.beginTransaction()

    val future = producer.send(new ProducerRecord(tp.topic, tp.partition, "1".getBytes, "1".getBytes))
    val e = JTestUtils.assertFutureThrows(classOf[TopicAuthorizationException], future)
    assertEquals(Set(topic), e.unauthorizedTopics.asScala)
  }

  @Test
  def testTransactionalProducerTopicAuthorizationExceptionInCommit(): Unit = {
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

  @Test
  def shouldThrowTransactionalIdAuthorizationExceptionWhenNoTransactionAccessDuringSend(): Unit = {
    createTopicWithBrokerPrincipal(topic)

    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), transactionalIdResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    removeAllClientAcls()
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
    producer.beginTransaction()
    val future = producer.send(new ProducerRecord(tp.topic, tp.partition, "1".getBytes, "1".getBytes))
    JTestUtils.assertFutureThrows(classOf[TransactionalIdAuthorizationException], future)
  }

  @Test
  def shouldThrowTransactionalIdAuthorizationExceptionWhenNoTransactionAccessOnEndTransaction(): Unit = {
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

  @Test
  def testListTransactionsAuthorization(): Unit = {
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

  @Test
  def shouldNotIncludeUnauthorizedTopicsInDescribeTransactionsResponse(): Unit = {
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

  @Test
  def shouldSuccessfullyAbortTransactionAfterTopicAuthorizationException(): Unit = {
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
    val e = JTestUtils.assertFutureThrows(classOf[TopicAuthorizationException], future)
    assertEquals(Set("otherTopic"), e.unauthorizedTopics.asScala)
    // now rollback
    producer.abortTransaction()
  }

  @Test
  def shouldThrowTransactionalIdAuthorizationExceptionWhenNoTransactionAccessOnSendOffsetsToTxn(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), transactionalIdResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), groupResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    producer.beginTransaction()
    removeAllClientAcls()
    // In transaction V2, the server receives the offset commit request first, so the error is GroupAuthorizationException
    // instead of TransactionalIdAuthorizationException.
    assertThrows(classOf[GroupAuthorizationException], () => {
      val offsets = java.util.Map.of(tp, new OffsetAndMetadata(1L))
      producer.sendOffsetsToTransaction(offsets, new ConsumerGroupMetadata(group))
      producer.commitTransaction()
    })
  }

  @Test
  def shouldSendSuccessfullyWhenIdempotentAndHasCorrectACL(): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, IDEMPOTENT_WRITE, ALLOW)), clusterResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)), topicResource)
    val producer = buildIdempotentProducer()
    producer.send(new ProducerRecord(tp.topic, tp.partition, "1".getBytes, "1".getBytes)).get
  }

  // Verify that metadata request without topics works without any ACLs and returns cluster id
  @Test
  def testClusterId(): Unit = {
    val request = new requests.MetadataRequest.Builder(java.util.List.of, false).build()
    val response = connectAndReceive[MetadataResponse](request)
    assertEquals(util.Map.of, response.errorCounts)
    assertFalse(response.clusterId.isEmpty, "Cluster id not returned")
  }

  @Test
  def testRetryProducerInitializationAfterPermissionFix(): Unit = {
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

  @Test
  def testAuthorizeByResourceTypeMultipleAddAndRemove(): Unit = {
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

  @Test
  def testAuthorizeByResourceTypeIsolationUnrelatedDenyWontDominateAllow(): Unit = {
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

  @Test
  def testAuthorizeByResourceTypeDenyTakesPrecedence(): Unit = {
    createTopicWithBrokerPrincipal(topic)
    val allowWriteAce = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)
    addAndVerifyAcls(Set(allowWriteAce), topicResource)
    assertIdempotentSendSuccess()

    val denyWriteAce = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, DENY)
    addAndVerifyAcls(Set(denyWriteAce), topicResource)
    assertIdempotentSendAuthorizationFailure()
  }

  @Test
  def testAuthorizeByResourceTypeWildcardResourceDenyDominate(): Unit = {
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

  @Test
  def testAuthorizeByResourceTypePrefixedResourceDenyDominate(): Unit = {
    createTopicWithBrokerPrincipal(topic)
    val prefixed = new ResourcePattern(TOPIC, topic.substring(0, 1), PREFIXED)
    val literal = new ResourcePattern(TOPIC, topic, LITERAL)
    val allowWriteAce = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, ALLOW)
    val denyWriteAce = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, WRITE, DENY)

    addAndVerifyAcls(Set(denyWriteAce), prefixed)
    addAndVerifyAcls(Set(allowWriteAce), literal)
    assertIdempotentSendAuthorizationFailure()
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

  @Test
  def testDescribeTopicAclWithOperationAll(): Unit = {
    createTopicWithBrokerPrincipal(topic)
    removeAllClientAcls()

    val allowAllOpsAcl = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALL, ALLOW)
    addAndVerifyAcls(Set(allowAllOpsAcl), topicResource)

    val metadataRequestTopic = new MetadataRequestTopic()
      .setName(topic)

    val metadataRequest = new MetadataRequest.Builder(new MetadataRequestData()
      .setTopics(util.List.of(metadataRequestTopic))
      .setAllowAutoTopicCreation(false)
    ).build()

    val metadataResponse = connectAndReceive[MetadataResponse](metadataRequest)
    val topicResponseOpt = metadataResponse.topicMetadata().asScala.find(_.topic == topic)
    assertTrue(topicResponseOpt.isDefined)

    val topicResponse = topicResponseOpt.get
    assertEquals(Errors.NONE, topicResponse.error)
  }

  @Test
  def testDescribeTopicConfigsAclWithOperationAll(): Unit = {
    createTopicWithBrokerPrincipal(topic)
    removeAllClientAcls()

    val allowAllOpsAcl = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALL, ALLOW)
    addAndVerifyAcls(Set(allowAllOpsAcl), topicResource)

    val describeConfigsRequest = new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData()
      .setResources(util.List.of(new DescribeConfigsRequestData.DescribeConfigsResource()
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
      .setTopics(util.List.of)
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

  @Test
  def testHostAddressBasedAcls(): Unit = {
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
        .setTopics(util.List.of(metadataRequestTopic))
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

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testCreateAndCloseConsumerWithNoAccess(groupProtocol: String): Unit = {
    val consumer = createConsumer()
    val closeConsumer: Executable = () => consumer.close()
    // Close consumer without consuming anything. close() call should pass successfully and throw no exception.
    assertDoesNotThrow(closeConsumer, "Exception not expected on closing consumer")
  }

  @Test
  def testConsumerGroupHeartbeatWithGroupReadAndTopicDescribeAcl(): Unit = {
    addAndVerifyAcls(groupReadAcl(groupResource), groupResource)
    addAndVerifyAcls(topicDescribeAcl(topicResource), topicResource)

    val request = consumerGroupHeartbeatRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @Test
  def testConsumerGroupHeartbeatWithOperationAll(): Unit = {
    val allowAllOpsAcl = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALL, ALLOW)
    addAndVerifyAcls(Set(allowAllOpsAcl), groupResource)
    addAndVerifyAcls(Set(allowAllOpsAcl), topicResource)

    val request = consumerGroupHeartbeatRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @Test
  def testConsumerGroupHeartbeatWithoutGroupReadOrTopicDescribeAcl(): Unit = {
    removeAllClientAcls()

    val request = consumerGroupHeartbeatRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = false)
  }

  @Test
  def testConsumerGroupHeartbeatWithoutGroupReadAcl(): Unit = {
    addAndVerifyAcls(topicDescribeAcl(topicResource), topicResource)

    val request = consumerGroupHeartbeatRequest

    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = false)
  }

  @Test
  def testConsumerGroupHeartbeatWithoutTopicDescribeAcl(): Unit = {
    addAndVerifyAcls(groupReadAcl(groupResource), groupResource)

    val request = consumerGroupHeartbeatRequest

    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = false)
  }

  @Test
  def testConsumerGroupHeartbeatWithRegex(): Unit = {
    createTopicWithBrokerPrincipal(topic)
    val allowAllOpsAcl = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALL, ALLOW)
    addAndVerifyAcls(Set(allowAllOpsAcl), groupResource)
    addAndVerifyAcls(Set(allowAllOpsAcl), topicResource)

    var response = sendAndReceiveFirstRegexHeartbeat(Uuid.randomUuid.toString, listenerName)
    TestUtils.tryUntilNoAssertionError() {
      response = sendAndReceiveRegexHeartbeat(response, listenerName, Some(1))
    }
  }

  @Test
  def testConsumerGroupHeartbeatWithRegexWithoutTopicDescribeAcl(): Unit = {
    createTopicWithBrokerPrincipal(topic)
    val allowAllOpsAcl = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALL, ALLOW)
    addAndVerifyAcls(Set(allowAllOpsAcl), groupResource)

    val response = sendAndReceiveFirstRegexHeartbeat(Uuid.randomUuid.toString, listenerName)
    sendAndReceiveRegexHeartbeat(response, listenerName, None)
  }

  @Test
  def testConsumerGroupHeartbeatWithRegexWithTopicDescribeAclAddedAndRemoved(): Unit = {
    createTopicWithBrokerPrincipal(topic)
    val allowAllOpsAcl = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALL, ALLOW)
    addAndVerifyAcls(Set(allowAllOpsAcl), groupResource)

    val memberId = Uuid.randomUuid.toString;
    var response = sendAndReceiveFirstRegexHeartbeat(memberId, listenerName)
    TestUtils.tryUntilNoAssertionError() {
      response = sendAndReceiveRegexHeartbeat(response, listenerName, Some(0), true)
    }

    addAndVerifyAcls(topicDescribeAcl(topicResource), topicResource)
    TestUtils.tryUntilNoAssertionError(waitTime = 25000) {
      response = sendAndReceiveRegexHeartbeat(response, listenerName, Some(1))
    }

    removeAndVerifyAcls(topicDescribeAcl(topicResource), topicResource)
    TestUtils.tryUntilNoAssertionError(waitTime = 25000) {
      response = sendAndReceiveRegexHeartbeat(response, listenerName, Some(0))
    }
  }

  @Test
  def testConsumerGroupHeartbeatWithRegexWithDifferentMemberAcls(): Unit = {
    createTopicWithBrokerPrincipal(topic, numPartitions = 2)
    val allowAllOpsAcl = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALL, ALLOW)
    addAndVerifyAcls(Set(allowAllOpsAcl), groupResource)

    // Member on inter-broker listener has all access and is assigned the matching topic
    var member1Response = sendAndReceiveFirstRegexHeartbeat("memberWithAllAccess", interBrokerListenerName)
    member1Response = sendAndReceiveRegexHeartbeat(member1Response, interBrokerListenerName, Some(2))

    // Member on client listener has no topic describe access, but is assigned a partition of the
    // unauthorized topic. This is leaking unauthorized topic metadata to member2. Simply filtering out
    // the topic from the assignment in the response is not sufficient since different assignment states
    // in the broker and client can lead to other issues. This needs to be fixed properly by using
    // member permissions while computing assignments.
    var member2Response = sendAndReceiveFirstRegexHeartbeat("memberWithLimitedAccess", listenerName)
    member1Response = sendAndReceiveRegexHeartbeat(member1Response, interBrokerListenerName, Some(1))
    member1Response = sendAndReceiveRegexHeartbeat(member1Response, interBrokerListenerName, Some(1), fullRequest = true)
    member2Response = sendAndReceiveRegexHeartbeat(member2Response, listenerName, Some(1))

    // Create another topic and send heartbeats on member1 to trigger regex refresh
    createTopicWithBrokerPrincipal("topic2", numPartitions = 2)
    TestUtils.retry(15000) {
      member1Response = sendAndReceiveRegexHeartbeat(member1Response, interBrokerListenerName, Some(2))
    }
    // This is leaking unauthorized topic metadata to member2.
    member2Response = sendAndReceiveRegexHeartbeat(member2Response, listenerName, Some(2))

    // Create another topic and send heartbeats on member2 to trigger regex refresh
    createTopicWithBrokerPrincipal("topic3", numPartitions = 2)
    TestUtils.retry(15000) {
      member2Response = sendAndReceiveRegexHeartbeat(member2Response, listenerName, Some(0), fullRequest = true)
    }
    // This removes all topics from member1 since member2's permissions were used to refresh regex.
    sendAndReceiveRegexHeartbeat(member1Response, interBrokerListenerName, Some(0), fullRequest = true)
  }

  @Test
  def testShareGroupHeartbeatWithGroupReadAndTopicDescribeAcl(): Unit = {
    addAndVerifyAcls(shareGroupReadAcl(shareGroupResource), shareGroupResource)
    addAndVerifyAcls(topicDescribeAcl(topicResource), topicResource)

    val request = shareGroupHeartbeatRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @Test
  def testShareGroupHeartbeatWithOperationAll(): Unit = {
    val allowAllOpsAcl = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALL, ALLOW)
    addAndVerifyAcls(Set(allowAllOpsAcl), shareGroupResource)
    addAndVerifyAcls(Set(allowAllOpsAcl), topicResource)

    val request = shareGroupHeartbeatRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @Test
  def testShareGroupHeartbeatWithoutGroupReadOrTopicDescribeAcl(): Unit = {
    removeAllClientAcls()

    val request = shareGroupHeartbeatRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = false)
  }

  @Test
  def testShareGroupHeartbeatWithoutGroupReadAcl(): Unit = {
    addAndVerifyAcls(topicDescribeAcl(topicResource), topicResource)

    val request = shareGroupHeartbeatRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = false)
  }

  @Test
  def testShareGroupHeartbeatWithoutTopicDescribeAcl(): Unit = {
    addAndVerifyAcls(shareGroupReadAcl(shareGroupResource), shareGroupResource)

    val request = shareGroupHeartbeatRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = false)
  }

  private def createShareGroupToDescribe(): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), shareGroupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)
    shareConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, shareGroup)
    val consumer = createShareConsumer()
    consumer.subscribe(util.Set.of(topic))
    consumer.poll(Duration.ofMillis(500L))
    removeAllClientAcls()
  }

  private def createEmptyShareGroup(): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), shareGroupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)
    shareConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, shareGroup)
    val consumer = createShareConsumer()
    consumer.subscribe(util.Set.of(topic))
    consumer.poll(Duration.ofMillis(500L))
    consumer.close()
    removeAllClientAcls()
  }

  @Test
  def testShareGroupDescribeWithGroupDescribeAndTopicDescribeAcl(): Unit = {
    createShareGroupToDescribe()
    addAndVerifyAcls(shareGroupDescribeAcl(shareGroupResource), shareGroupResource)
    addAndVerifyAcls(topicDescribeAcl(topicResource), topicResource)

    val request = shareGroupDescribeRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @Test
  def testShareGroupDescribeWithOperationAll(): Unit = {
    createShareGroupToDescribe()

    val allowAllOpsAcl = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALL, ALLOW)
    addAndVerifyAcls(Set(allowAllOpsAcl), shareGroupResource)
    addAndVerifyAcls(Set(allowAllOpsAcl), topicResource)

    val request = shareGroupDescribeRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @Test
  def testShareGroupDescribeWithoutGroupDescribeAcl(): Unit = {
    createShareGroupToDescribe()
    addAndVerifyAcls(topicDescribeAcl(topicResource), topicResource)

    val request = shareGroupDescribeRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = false)
  }

  @Test
  def testShareGroupDescribeWithoutGroupDescribeOrTopicDescribeAcl(): Unit = {
    createShareGroupToDescribe()

    val request = shareGroupDescribeRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = false)
  }

  @Test
  def testShareFetchWithGroupReadAndTopicReadAcl(): Unit = {
    addAndVerifyAcls(shareGroupReadAcl(shareGroupResource), shareGroupResource)
    addAndVerifyAcls(topicReadAcl(topicResource), topicResource)

    val request = createShareFetchRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @Test
  def testShareFetchWithOperationAll(): Unit = {
    val allowAllOpsAcl = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALL, ALLOW)
    addAndVerifyAcls(Set(allowAllOpsAcl), shareGroupResource)
    addAndVerifyAcls(Set(allowAllOpsAcl), topicResource)

    val request = createShareFetchRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @Test
  def testShareFetchWithoutGroupReadOrTopicReadAcl(): Unit = {
    removeAllClientAcls()

    val request = createShareFetchRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = false)
  }

  @Test
  def testShareFetchWithoutGroupReadAcl(): Unit = {
    addAndVerifyAcls(topicReadAcl(topicResource), topicResource)

    val request = createShareFetchRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = false)
  }

  @Test
  def testShareFetchWithoutTopicReadAcl(): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(shareGroupReadAcl(shareGroupResource), shareGroupResource)

    val request = createShareFetchRequest
    val response = connectAndReceive[ShareFetchResponse](request, listenerName = listenerName)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED, Errors.forCode(response.data.responses.stream().findFirst().get().partitions.get(0).errorCode))
  }

  @Test
  def testShareAcknowledgeWithGroupReadAndTopicReadAcl(): Unit = {
    addAndVerifyAcls(shareGroupReadAcl(shareGroupResource), shareGroupResource)
    addAndVerifyAcls(topicReadAcl(topicResource), topicResource)

    val request = shareAcknowledgeRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @Test
  def testShareAcknowledgeWithOperationAll(): Unit = {
    val allowAllOpsAcl = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALL, ALLOW)
    addAndVerifyAcls(Set(allowAllOpsAcl), shareGroupResource)
    addAndVerifyAcls(Set(allowAllOpsAcl), topicResource)

    val request = shareAcknowledgeRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @Test
  def testShareAcknowledgeWithoutGroupReadOrTopicReadAcl(): Unit = {
    removeAllClientAcls()

    val request = shareAcknowledgeRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = false)
  }

  @Test
  def testShareAcknowledgeFetchWithoutGroupReadAcl(): Unit = {
    addAndVerifyAcls(topicReadAcl(topicResource), topicResource)

    val request = shareAcknowledgeRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = false)
  }

  @Test
  def testInitializeShareGroupStateWithClusterAcl(): Unit = {
    addAndVerifyAcls(clusterAcl(clusterResource), clusterResource)

    val request = initializeShareGroupStateRequest
    val resource = Set[ResourceType](CLUSTER)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @Test
  def testInitializeShareGroupStateWithOperationAll(): Unit = {
    val allowAllOpsAcl = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALL, ALLOW)
    addAndVerifyAcls(Set(allowAllOpsAcl), clusterResource)

    val request = initializeShareGroupStateRequest
    val resource = Set[ResourceType](CLUSTER)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @Test
  def testInitializeShareGroupStateWithoutClusterAcl(): Unit = {
    removeAllClientAcls()

    val request = initializeShareGroupStateRequest
    val resource = Set[ResourceType](CLUSTER)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = false)
  }

  @Test
  def testReadShareGroupStateWithClusterAcl(): Unit = {
    addAndVerifyAcls(clusterAcl(clusterResource), clusterResource)

    val request = readShareGroupStateRequest
    val resource = Set[ResourceType](CLUSTER)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @Test
  def testReadShareGroupStateWithOperationAll(): Unit = {
    val allowAllOpsAcl = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALL, ALLOW)
    addAndVerifyAcls(Set(allowAllOpsAcl), clusterResource)

    val request = readShareGroupStateRequest
    val resource = Set[ResourceType](CLUSTER)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @Test
  def testReadShareGroupStateWithoutClusterAcl(): Unit = {
    removeAllClientAcls()

    val request = readShareGroupStateRequest
    val resource = Set[ResourceType](CLUSTER)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = false)
  }

  @Test
  def testWriteShareGroupStateWithClusterAcl(): Unit = {
    addAndVerifyAcls(clusterAcl(clusterResource), clusterResource)

    val request = writeShareGroupStateRequest
    val resource = Set[ResourceType](CLUSTER)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @Test
  def testWriteShareGroupStateWithOperationAll(): Unit = {
    val allowAllOpsAcl = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALL, ALLOW)
    addAndVerifyAcls(Set(allowAllOpsAcl), clusterResource)

    val request = writeShareGroupStateRequest
    val resource = Set[ResourceType](CLUSTER)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @Test
  def testWriteShareGroupStateWithoutClusterAcl(): Unit = {
    removeAllClientAcls()

    val request = writeShareGroupStateRequest
    val resource = Set[ResourceType](CLUSTER)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = false)
  }

  @Test
  def testDeleteShareGroupStateWithClusterAcl(): Unit = {
    addAndVerifyAcls(clusterAcl(clusterResource), clusterResource)

    val request = deleteShareGroupStateRequest
    val resource = Set[ResourceType](CLUSTER)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @Test
  def testDeleteShareGroupStateWithOperationAll(): Unit = {
    val allowAllOpsAcl = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALL, ALLOW)
    addAndVerifyAcls(Set(allowAllOpsAcl), clusterResource)

    val request = deleteShareGroupStateRequest
    val resource = Set[ResourceType](CLUSTER)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @Test
  def testDeleteShareGroupStateWithoutClusterAcl(): Unit = {
    removeAllClientAcls()

    val request = deleteShareGroupStateRequest
    val resource = Set[ResourceType](CLUSTER)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = false)
  }

  @Test
  def testReadShareGroupStateSummaryWithClusterAcl(): Unit = {
    addAndVerifyAcls(clusterAcl(clusterResource), clusterResource)

    val request = readShareGroupStateSummaryRequest
    val resource = Set[ResourceType](CLUSTER)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @Test
  def testReadShareGroupStateSummaryWithOperationAll(): Unit = {
    val allowAllOpsAcl = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALL, ALLOW)
    addAndVerifyAcls(Set(allowAllOpsAcl), clusterResource)

    val request = readShareGroupStateRequest
    val resource = Set[ResourceType](CLUSTER)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @Test
  def testReadShareGroupStateSummaryWithoutClusterAcl(): Unit = {
    removeAllClientAcls()

    val request = readShareGroupStateRequest
    val resource = Set[ResourceType](CLUSTER)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = false)
  }

  @Test
  def testDescribeShareGroupOffsetsWithGroupDescribeAndTopicDescribeAcl(): Unit = {
    addAndVerifyAcls(shareGroupDescribeAcl(shareGroupResource), shareGroupResource)
    addAndVerifyAcls(topicDescribeAcl(topicResource), topicResource)

    val request = describeShareGroupOffsetsRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @Test
  def testDescribeShareGroupOffsetsWithOperationAll(): Unit = {
    val allowAllOpsAcl = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALL, ALLOW)
    addAndVerifyAcls(Set(allowAllOpsAcl), shareGroupResource)
    addAndVerifyAcls(Set(allowAllOpsAcl), topicResource)

    val request = describeShareGroupOffsetsRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @Test
  def testDescribeShareGroupOffsetsWithoutGroupDescribeOrTopicDescribeAcl(): Unit = {
    removeAllClientAcls()

    val request = describeShareGroupOffsetsRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = false)
  }

  @Test
  def testDescribeShareGroupOffsetsWithoutGroupDescribeAcl(): Unit = {
    addAndVerifyAcls(topicDescribeAcl(topicResource), topicResource)

    val request = describeShareGroupOffsetsRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = false)
  }

  @Test
  def testDescribeShareGroupOffsetsWithoutTopicDescribeAcl(): Unit = {
    addAndVerifyAcls(shareGroupDescribeAcl(shareGroupResource), shareGroupResource)

    val request = describeShareGroupOffsetsRequest
    val response = connectAndReceive[DescribeShareGroupOffsetsResponse](request, listenerName = listenerName)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED, Errors.forCode(response.data.groups.get(0).topics.get(0).partitions.get(0).errorCode))
  }

  @Test
  def testDeleteShareGroupOffsetsWithGroupDeleteAndTopicReadAcl(): Unit = {
    addAndVerifyAcls(shareGroupDeleteAcl(shareGroupResource), shareGroupResource)
    addAndVerifyAcls(topicReadAcl(topicResource), topicResource)

    val request = deleteShareGroupOffsetsRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @Test
  def testDeleteShareGroupOffsetsWithOperationAll(): Unit = {
    val allowAllOpsAcl = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALL, ALLOW)
    addAndVerifyAcls(Set(allowAllOpsAcl), shareGroupResource)
    addAndVerifyAcls(Set(allowAllOpsAcl), topicResource)

    val request = deleteShareGroupOffsetsRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @Test
  def testDeleteShareGroupOffsetsWithoutGroupDeleteOrTopicReadAcl(): Unit = {
    removeAllClientAcls()

    val request = deleteShareGroupOffsetsRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = false)
  }

  @Test
  def testDeleteShareGroupOffsetsWithoutGroupDeleteAcl(): Unit = {
    addAndVerifyAcls(topicReadAcl(topicResource), topicResource)

    val request = deleteShareGroupOffsetsRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = false)
  }

  @Test
  def testDeleteShareGroupOffsetsWithoutTopicReadAcl(): Unit = {
    createEmptyShareGroup()
    addAndVerifyAcls(shareGroupDeleteAcl(shareGroupResource), shareGroupResource)

    val request = deleteShareGroupOffsetsRequest
    val response = connectAndReceive[DeleteShareGroupOffsetsResponse](request, listenerName = listenerName)
    assertEquals(1, response.data.responses.size)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, response.data.responses.get(0).errorCode, s"Unexpected response $response")
  }

  @Test
  def testAlterShareGroupOffsetsWithGroupReadAndTopicReadAcl(): Unit = {
    addAndVerifyAcls(shareGroupReadAcl(shareGroupResource), shareGroupResource)
    addAndVerifyAcls(topicReadAcl(topicResource), topicResource)

    val request = alterShareGroupOffsetsRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @Test
  def testAlterShareGroupOffsetsWithOperationAll(): Unit = {
    val allowAllOpsAcl = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALL, ALLOW)
    addAndVerifyAcls(Set(allowAllOpsAcl), shareGroupResource)
    addAndVerifyAcls(Set(allowAllOpsAcl), topicResource)

    val request = alterShareGroupOffsetsRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @Test
  def testAlterShareGroupOffsetsWithoutGroupReadOrTopicReadAcl(): Unit = {
    removeAllClientAcls()

    val request = alterShareGroupOffsetsRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = false)
  }

  @Test
  def testAlterShareGroupOffsetsWithoutGroupReadAcl(): Unit = {
    addAndVerifyAcls(topicReadAcl(topicResource), topicResource)

    val request = alterShareGroupOffsetsRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = false)
  }

  @Test
  def testAlterShareGroupOffsetsWithoutTopicReadAcl(): Unit = {
    createEmptyShareGroup()
    addAndVerifyAcls(shareGroupReadAcl(shareGroupResource), shareGroupResource)

    val request = alterShareGroupOffsetsRequest
    val response = connectAndReceive[AlterShareGroupOffsetsResponse](request, listenerName = listenerName)
    assertEquals(1, response.data.responses.size)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, response.data.responses.stream().findFirst().get().partitions.get(0).errorCode, s"Unexpected response $response")
  }

  @ParameterizedTest
  @CsvSource(Array(
    "true,  false, false, false",
    "false, true,  false, false",
    "false, false, true,  false",
    "false, false, false, true"
  ))
  def testStreamsGroupHeartbeatWithGroupReadAndTopicDescribeAcl(
                                                                 topicAsSourceTopic: Boolean,
                                                                 topicAsRepartitionSinkTopic: Boolean,
                                                                 topicAsRepartitionSourceTopic: Boolean,
                                                                 topicAsStateChangelogTopics: Boolean
                                                               ): Unit = {
    addAndVerifyAcls(streamsGroupReadAcl(streamsGroupResource), streamsGroupResource)
    addAndVerifyAcls(sourceTopicDescribeAcl(sourceTopicResource), sourceTopicResource) // Always added, since we need a source topic
    addAndVerifyAcls(topicDescribeAcl(topicResource), topicResource)

    val request = streamsGroupHeartbeatRequest(
      topicAsSourceTopic,
      topicAsRepartitionSinkTopic,
      topicAsRepartitionSourceTopic,
      topicAsStateChangelogTopics
    )
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @ParameterizedTest
  @CsvSource(Array(
    "true,  false, false, false",
    "false, true,  false, false",
    "false, false, true,  false",
    "false, false, false, true"
  ))
  def testStreamsGroupHeartbeatWithOperationAll(
                                                 topicAsSourceTopic: Boolean,
                                                 topicAsRepartitionSinkTopic: Boolean,
                                                 topicAsRepartitionSourceTopic: Boolean,
                                                 topicAsStateChangelogTopics: Boolean
                                               ): Unit = {
    val allowAllOpsAcl = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALL, ALLOW)
    addAndVerifyAcls(Set(allowAllOpsAcl), streamsGroupResource)
    addAndVerifyAcls(Set(allowAllOpsAcl), topicResource)
    addAndVerifyAcls(Set(allowAllOpsAcl), sourceTopicResource)

    val request = streamsGroupHeartbeatRequest(
      topicAsSourceTopic,
      topicAsRepartitionSinkTopic,
      topicAsRepartitionSourceTopic,
      topicAsStateChangelogTopics
    )
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @ParameterizedTest
  @CsvSource(Array(
    "true,  false, false, false",
    "false, true,  false, false",
    "false, false, true,  false",
    "false, false, false, true"
  ))
  def testStreamsGroupHeartbeatWithoutGroupReadOrTopicDescribeAcl(
                                                                   topicAsSourceTopic: Boolean,
                                                                   topicAsRepartitionSinkTopic: Boolean,
                                                                   topicAsRepartitionSourceTopic: Boolean,
                                                                   topicAsStateChangelogTopics: Boolean
                                                                 ): Unit = {
    removeAllClientAcls()

    val request = streamsGroupHeartbeatRequest(
      topicAsSourceTopic,
      topicAsRepartitionSinkTopic,
      topicAsRepartitionSourceTopic,
      topicAsStateChangelogTopics
    )
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = false)
  }

  @ParameterizedTest
  @CsvSource(Array(
    "true,  false, false, false",
    "false, true,  false, false",
    "false, false, true,  false",
    "false, false, false, true"
  ))
  def testStreamsGroupHeartbeatWithoutGroupReadAcl(
                                                    topicAsSourceTopic: Boolean,
                                                    topicAsRepartitionSinkTopic: Boolean,
                                                    topicAsRepartitionSourceTopic: Boolean,
                                                    topicAsStateChangelogTopics: Boolean
                                                  ): Unit = {
    addAndVerifyAcls(topicDescribeAcl(topicResource), topicResource)
    addAndVerifyAcls(sourceTopicDescribeAcl(sourceTopicResource), sourceTopicResource) // Always added, since we need a source topic

    val request = streamsGroupHeartbeatRequest(
      topicAsSourceTopic,
      topicAsRepartitionSinkTopic,
      topicAsRepartitionSourceTopic,
      topicAsStateChangelogTopics
    )
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = false)
  }

  @ParameterizedTest
  @CsvSource(Array(
    "true,  false, false, false",
    "false, true,  false, false",
    "false, false, true,  false",
    "false, false, false, true"
  ))
  def testStreamsGroupHeartbeatWithoutTopicDescribeAcl(
                                                        topicAsSourceTopic: Boolean,
                                                        topicAsRepartitionSinkTopic: Boolean,
                                                        topicAsRepartitionSourceTopic: Boolean,
                                                        topicAsStateChangelogTopics: Boolean
                                                      ): Unit = {
    addAndVerifyAcls(streamsGroupReadAcl(streamsGroupResource), streamsGroupResource)
    addAndVerifyAcls(sourceTopicDescribeAcl(sourceTopicResource), sourceTopicResource) // Always added, since we need a source topic

    val request = streamsGroupHeartbeatRequest(
      topicAsSourceTopic,
      topicAsRepartitionSinkTopic,
      topicAsRepartitionSourceTopic,
      topicAsStateChangelogTopics
    )
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = false)
  }

  @ParameterizedTest
  @CsvSource(Array(
    "true,  false",
    "false, true"
  ))
  def testStreamsGroupHeartbeatWithoutInternalTopicCreateAcl(
                                                              topicAsRepartitionSourceTopic: Boolean,
                                                              topicAsStateChangelogTopics: Boolean
                                                            ): Unit = {
    createTopicWithBrokerPrincipal(sourceTopic)
    addAndVerifyAcls(streamsGroupReadAcl(streamsGroupResource), streamsGroupResource)
    addAndVerifyAcls(sourceTopicDescribeAcl(sourceTopicResource), sourceTopicResource) // Always added, since we need a source topic
    addAndVerifyAcls(topicDescribeAcl(topicResource), topicResource)

    val request = streamsGroupHeartbeatRequest(
      topicAsSourceTopic = false,
      topicAsRepartitionSinkTopic = false,
      topicAsRepartitionSourceTopic = topicAsRepartitionSourceTopic,
      topicAsStateChangelogTopics = topicAsStateChangelogTopics
    )
    val resource = Set[ResourceType](GROUP, TOPIC)

    // Request successful, but internal topic not created.
    val response = sendRequestAndVerifyResponseError(request, resource, isAuthorized = true).asInstanceOf[StreamsGroupHeartbeatResponse]
    assertEquals(
      util.List.of(new StreamsGroupHeartbeatResponseData.Status()
        .setStatusCode(StreamsGroupHeartbeatResponse.Status.ASSIGNMENT_DELAYED.code())
        .setStatusDetail("Assignment delayed due to the configured initial rebalance delay."),
        new StreamsGroupHeartbeatResponseData.Status()
        .setStatusCode(StreamsGroupHeartbeatResponse.Status.MISSING_INTERNAL_TOPICS.code())
        .setStatusDetail("Internal topics are missing: [topic]; Unauthorized to CREATE on topics topic.")),
    response.data().status())
  }

  @ParameterizedTest
  @CsvSource(Array(
    "true,  false",
    "false, true"
  ))
  def testStreamsGroupHeartbeatWithInternalTopicCreateAcl(
                                                           topicAsRepartitionSourceTopic: Boolean,
                                                           topicAsStateChangelogTopics: Boolean
                                                         ): Unit = {
    createTopicWithBrokerPrincipal(sourceTopic)
    addAndVerifyAcls(streamsGroupReadAcl(streamsGroupResource), streamsGroupResource)
    addAndVerifyAcls(sourceTopicDescribeAcl(sourceTopicResource), sourceTopicResource) // Always added, since we need a source topic
    addAndVerifyAcls(topicDescribeAcl(topicResource), topicResource)
    addAndVerifyAcls(topicCreateAcl(topicResource), topicResource)

    val request = streamsGroupHeartbeatRequest(
      topicAsSourceTopic = false,
      topicAsRepartitionSinkTopic = false,
      topicAsRepartitionSourceTopic = topicAsRepartitionSourceTopic,
      topicAsStateChangelogTopics = topicAsStateChangelogTopics
    )
    val resource = Set[ResourceType](GROUP, TOPIC)
    val response = sendRequestAndVerifyResponseError(request, resource, isAuthorized = true).asInstanceOf[StreamsGroupHeartbeatResponse]
    // Request successful, and no internal topic creation error.
    assertEquals(
      util.List.of(new StreamsGroupHeartbeatResponseData.Status()
        .setStatusCode(StreamsGroupHeartbeatResponse.Status.ASSIGNMENT_DELAYED.code())
        .setStatusDetail("Assignment delayed due to the configured initial rebalance delay."),
        new StreamsGroupHeartbeatResponseData.Status()
        .setStatusCode(StreamsGroupHeartbeatResponse.Status.MISSING_INTERNAL_TOPICS.code())
        .setStatusDetail("Internal topics are missing: [topic]")),
      response.data().status())
  }

  private def createStreamsGroupToDescribe(
                                            topicAsSourceTopic: Boolean,
                                            topicAsRepartitionSinkTopic: Boolean,
                                            topicAsRepartitionSourceTopic: Boolean,
                                            topicAsStateChangelogTopics: Boolean
                                          ): Unit = {
    createTopicWithBrokerPrincipal(sourceTopic)
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), streamsGroupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), sourceTopicResource)
    streamsConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, streamsGroup)
    streamsConsumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    val consumer = createStreamsConsumer(streamsRebalanceData = new StreamsRebalanceData(
      UUID.randomUUID(),
      Optional.empty(),
      util.Map.of(
        "subtopology-0", new StreamsRebalanceData.Subtopology(
          if (topicAsSourceTopic) util.Set.of(sourceTopic, topic) else util.Set.of(sourceTopic),
          if (topicAsRepartitionSinkTopic) util.Set.of(topic) else util.Set.of(),
          if (topicAsRepartitionSourceTopic)
            util.Map.of(topic, new StreamsRebalanceData.TopicInfo(Optional.of(1), Optional.empty(), util.Map.of()))
          else util.Map.of(),
          if (topicAsStateChangelogTopics)
            util.Map.of(topic, new StreamsRebalanceData.TopicInfo(Optional.of(1), Optional.empty(), util.Map.of()))
          else util.Map.of(),
          util.Set.of()
        )),
      Map.empty[String, String].asJava
    ))
    consumer.subscribe(
      if (topicAsSourceTopic || topicAsRepartitionSourceTopic) util.Set.of(sourceTopic, topic) else util.Set.of(sourceTopic),
      new StreamsRebalanceListener {
        override def onTasksRevoked(tasks: util.Set[StreamsRebalanceData.TaskId]): Unit = ()
        override def onTasksAssigned(assignment: StreamsRebalanceData.Assignment): Unit = ()
        override def onAllTasksLost(): Unit = ()
      }
    )
    consumer.poll(Duration.ofMillis(500L))
    removeAllClientAcls()
  }

  @ParameterizedTest
  @CsvSource(Array(
    "true,  false, false, false",
    "false, true,  false, false",
    "false, false, true,  false",
    "false, false, false, true"
  ))
  def testStreamsGroupDescribeWithGroupDescribeAndTopicDescribeAcl(
                                                                    topicAsSourceTopic: Boolean,
                                                                    topicAsRepartitionSinkTopic: Boolean,
                                                                    topicAsRepartitionSourceTopic: Boolean,
                                                                    topicAsStateChangelogTopics: Boolean
                                                                  ): Unit = {
    createStreamsGroupToDescribe(
      topicAsSourceTopic,
      topicAsRepartitionSinkTopic,
      topicAsRepartitionSourceTopic,
      topicAsStateChangelogTopics
    )
    addAndVerifyAcls(streamsGroupDescribeAcl(streamsGroupResource), streamsGroupResource)
    addAndVerifyAcls(sourceTopicDescribeAcl(sourceTopicResource), sourceTopicResource) // Always added, since we need a source topic
    addAndVerifyAcls(topicDescribeAcl(topicResource), topicResource)

    val request = streamsGroupDescribeRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @ParameterizedTest
  @CsvSource(Array(
    "true,  false, false, false",
    "false, true,  false, false",
    "false, false, true,  false",
    "false, false, false, true"
  ))
  def testStreamsGroupDescribeWithOperationAll(
                                                topicAsSourceTopic: Boolean,
                                                topicAsRepartitionSinkTopic: Boolean,
                                                topicAsRepartitionSourceTopic: Boolean,
                                                topicAsStateChangelogTopics: Boolean
                                              ): Unit = {
    createStreamsGroupToDescribe(
      topicAsSourceTopic,
      topicAsRepartitionSinkTopic,
      topicAsRepartitionSourceTopic,
      topicAsStateChangelogTopics
    )

    val allowAllOpsAcl = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALL, ALLOW)
    addAndVerifyAcls(Set(allowAllOpsAcl), streamsGroupResource)
    addAndVerifyAcls(sourceTopicDescribeAcl(sourceTopicResource), sourceTopicResource) // Always added, since we need a source topic
    addAndVerifyAcls(Set(allowAllOpsAcl), topicResource)

    val request = streamsGroupDescribeRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @ParameterizedTest
  @CsvSource(Array(
    "true,  false, false, false",
    "false, true,  false, false",
    "false, false, true,  false",
    "false, false, false, true"
  ))
  def testStreamsGroupDescribeWithoutGroupDescribeAcl(
                                                       topicAsSourceTopic: Boolean,
                                                       topicAsRepartitionSinkTopic: Boolean,
                                                       topicAsRepartitionSourceTopic: Boolean,
                                                       topicAsStateChangelogTopics: Boolean
                                                     ): Unit = {
    createStreamsGroupToDescribe(
      topicAsSourceTopic,
      topicAsRepartitionSinkTopic,
      topicAsRepartitionSourceTopic,
      topicAsStateChangelogTopics
    )
    addAndVerifyAcls(sourceTopicDescribeAcl(sourceTopicResource), sourceTopicResource) // Always added, since we need a source topic
    addAndVerifyAcls(topicDescribeAcl(topicResource), topicResource)

    val request = streamsGroupDescribeRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = false)
  }

  @ParameterizedTest
  @CsvSource(Array(
    "true,  false, false, false",
    "false, true,  false, false",
    "false, false, true,  false",
    "false, false, false, true"
  ))
  def testStreamsGroupDescribeWithoutGroupDescribeOrTopicDescribeAcl(
                                                                      topicAsSourceTopic: Boolean,
                                                                      topicAsRepartitionSinkTopic: Boolean,
                                                                      topicAsRepartitionSourceTopic: Boolean,
                                                                      topicAsStateChangelogTopics: Boolean
                                                                    ): Unit = {
    createStreamsGroupToDescribe(
      topicAsSourceTopic,
      topicAsRepartitionSinkTopic,
      topicAsRepartitionSourceTopic,
      topicAsStateChangelogTopics
    )

    val request = streamsGroupDescribeRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    addAndVerifyAcls(sourceTopicDescribeAcl(sourceTopicResource), sourceTopicResource) // Always added, since we need a source topic

    sendRequestAndVerifyResponseError(request, resource, isAuthorized = false)
  }
  
  private def sendAndReceiveFirstRegexHeartbeat(memberId: String,
                                                listenerName: ListenerName): ConsumerGroupHeartbeatResponseData = {
    val request = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId(group)
        .setMemberId(memberId)
        .setMemberEpoch(0)
        .setRebalanceTimeoutMs(5 * 60 * 1000)
        .setTopicPartitions(util.List.of())
        .setSubscribedTopicRegex("^top.*")).build()
    val resource = Set[ResourceType](GROUP, TOPIC)
    val response = sendRequestAndVerifyResponseError(request, resource, isAuthorized = true, listenerName = listenerName)
      .data.asInstanceOf[ConsumerGroupHeartbeatResponseData]
    assertEquals(Errors.NONE.code, response.errorCode, s"Unexpected response $response")
    assertEquals(0, response.assignment.topicPartitions.size, s"Unexpected assignment $response")
    response
  }

  private def sendAndReceiveRegexHeartbeat(lastResponse: ConsumerGroupHeartbeatResponseData,
                                           listenerName: ListenerName,
                                           expectedAssignmentSize: Option[Int],
                                           fullRequest: Boolean = false): ConsumerGroupHeartbeatResponseData = {
    var data = new ConsumerGroupHeartbeatRequestData()
      .setGroupId(group)
      .setMemberId(lastResponse.memberId)
      .setMemberEpoch(lastResponse.memberEpoch)
    if (fullRequest) {
      val partitions = Option(lastResponse.assignment).map(_.topicPartitions.asScala.map(p =>
        new ConsumerGroupHeartbeatRequestData.TopicPartitions()
          .setTopicId(p.topicId)
          .setPartitions(p.partitions)
      )).getOrElse(List())
      data = data
        .setTopicPartitions(partitions.asJava)
        .setSubscribedTopicRegex("^top.*")
        .setRebalanceTimeoutMs(5 * 60 * 1000)
    }
    val request = new ConsumerGroupHeartbeatRequest.Builder(data).build()
    val resource = Set[ResourceType](GROUP, TOPIC)
    val response = sendRequestAndVerifyResponseError(request, resource, isAuthorized = true, listenerName = listenerName)
      .data.asInstanceOf[ConsumerGroupHeartbeatResponseData]
    assertEquals(Errors.NONE.code, response.errorCode, s"Unexpected response $response")
    expectedAssignmentSize match {
      case Some(size) =>
        assertNotNull(response.assignment, s"Unexpected assignment $response")
        assertEquals(size, response.assignment.topicPartitions.asScala.map(_.partitions.size).sum, s"Unexpected assignment $response")
      case None =>
        assertNull(response.assignment, s"Unexpected assignment $response")
    }
    response
  }

  private def createConsumerGroupToDescribe(): Unit = {
    createTopicWithBrokerPrincipal(topic)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), groupResource)
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, READ, ALLOW)), topicResource)
    consumerConfig.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer")
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, group)
    val consumer = createConsumer()
    consumer.subscribe(util.Set.of(topic))
    consumer.poll(Duration.ofMillis(500L))
    removeAllClientAcls()
  }

  @Test
  def testConsumerGroupDescribeWithGroupDescribeAndTopicDescribeAcl(): Unit = {
    createConsumerGroupToDescribe()

    addAndVerifyAcls(groupDescribeAcl(groupResource), groupResource)
    addAndVerifyAcls(topicDescribeAcl(topicResource), topicResource)

    val request = consumerGroupDescribeRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @Test
  def testConsumerGroupDescribeWithOperationAll(): Unit = {
    createConsumerGroupToDescribe()

    val allowAllOpsAcl = new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, ALL, ALLOW)
    addAndVerifyAcls(Set(allowAllOpsAcl), groupResource)
    addAndVerifyAcls(Set(allowAllOpsAcl), topicResource)

    val request = consumerGroupDescribeRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = true)
  }

  @Test
  def testConsumerGroupDescribeWithoutGroupDescribeAcl(): Unit = {
    createConsumerGroupToDescribe()

    addAndVerifyAcls(topicDescribeAcl(topicResource), topicResource)

    val request = consumerGroupDescribeRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = false)
  }

  @Test
  def testConsumerGroupDescribeWithoutTopicDescribeAcl(): Unit = {
    createConsumerGroupToDescribe()

    addAndVerifyAcls(groupDescribeAcl(groupResource), groupResource)

    val request = consumerGroupDescribeRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
    sendRequestAndVerifyResponseError(request, resource, isAuthorized = false)
  }

  @Test
  def testConsumerGroupDescribeWithoutGroupDescribeOrTopicDescribeAcl(): Unit = {
    createConsumerGroupToDescribe()

    val request = consumerGroupDescribeRequest
    val resource = Set[ResourceType](GROUP, TOPIC)
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

    authorizerForWrite.deleteAcls(TestUtils.anonymousAuthorizableContext, java.util.List.of(aclFilter)).asScala.
      map(_.toCompletableFuture.get).flatMap { deletion =>
        deletion.aclBindingDeleteResults().asScala.map(_.aclBinding.pattern).toSet
      }.foreach { resource =>
        (brokers.map(_.authorizerPlugin.get) ++ controllerServers.map(_.authorizerPlugin.get)).foreach { authorizer =>
          TestUtils.waitAndVerifyAcls(Set.empty[AccessControlEntry], authorizer.get, resource, aclEntryFilter)
        }
      }
  }

  private def sendRequestAndVerifyResponseError(request: AbstractRequest,
                                                resources: Set[ResourceType],
                                                isAuthorized: Boolean,
                                                topicExists: Boolean = true,
                                                topicNames: Map[Uuid, String] = getTopicNames(),
                                                listenerName: ListenerName = listenerName): AbstractResponse = {
    val apiKey = request.apiKey
    val response = connectAndReceive[AbstractResponse](request, listenerName = listenerName)
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

  @Test
  def testPrefixAcls(): Unit = {
    addAndVerifyAcls(Set(new AccessControlEntry(clientPrincipalString, WILDCARD_HOST, CREATE, ALLOW)),
      new ResourcePattern(TOPIC, "f", PREFIXED))
    addAndVerifyAcls(Set(new AccessControlEntry("User:otherPrincipal", WILDCARD_HOST, CREATE, DENY)),
      new ResourcePattern(TOPIC, "fooa", PREFIXED))
    addAndVerifyAcls(Set(new AccessControlEntry("User:otherPrincipal", WILDCARD_HOST, CREATE, ALLOW)),
      new ResourcePattern(TOPIC, "foob", PREFIXED))
    createAdminClient().createTopics(util.List.of(new NewTopic("foobar", 1, 1.toShort))).all().get()
  }
}
