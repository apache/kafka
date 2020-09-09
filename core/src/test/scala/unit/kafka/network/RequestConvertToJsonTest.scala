/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.network

import java.util.HashMap

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message._
import org.junit.Test
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests._

import scala.collection.mutable.ArrayBuffer

class RequestConvertToJsonTest {

  def createRequestsFromApiKey(apiKey: ApiKeys, version: Short): AbstractRequest = apiKey match {
    case ApiKeys.PRODUCE => ProduceRequest.Builder.forCurrentMagic(0.toShort, 10000, new HashMap[TopicPartition, MemoryRecords]()).build()
    case ApiKeys.FETCH => new FetchRequest(new FetchRequestData(), version)
    case ApiKeys.LIST_OFFSETS => new ListOffsetRequest(new ListOffsetRequestData().toStruct(version), version)
    case ApiKeys.METADATA => new MetadataRequest(new MetadataRequestData(), version)
    case ApiKeys.OFFSET_COMMIT => new OffsetCommitRequest(new OffsetCommitRequestData(), version)
    case ApiKeys.OFFSET_FETCH => new OffsetFetchRequest(new OffsetFetchRequestData().toStruct(version), version)
    case ApiKeys.FIND_COORDINATOR => new FindCoordinatorRequest(new FindCoordinatorRequestData().toStruct(version), version)
    case ApiKeys.JOIN_GROUP => new JoinGroupRequest(new JoinGroupRequestData(), version)
    case ApiKeys.HEARTBEAT => new HeartbeatRequest(new HeartbeatRequestData().toStruct(version), version)
    case ApiKeys.LEAVE_GROUP => new LeaveGroupRequest(new LeaveGroupRequestData().toStruct(version), version)
    case ApiKeys.SYNC_GROUP => new SyncGroupRequest(new SyncGroupRequestData(), version)
    case ApiKeys.STOP_REPLICA => new StopReplicaRequest(new StopReplicaRequestData().toStruct(version), version)
    case ApiKeys.CONTROLLED_SHUTDOWN => new ControlledShutdownRequest(new ControlledShutdownRequestData().toStruct(version), version)
    case ApiKeys.UPDATE_METADATA => new UpdateMetadataRequest(new UpdateMetadataRequestData().toStruct(version), version)
    case ApiKeys.LEADER_AND_ISR => new LeaderAndIsrRequest(new LeaderAndIsrRequestData().toStruct(version), version)
    case ApiKeys.DESCRIBE_GROUPS => new DescribeGroupsRequest(new DescribeGroupsRequestData().toStruct(version), version)
    case ApiKeys.LIST_GROUPS => new ListGroupsRequest(new ListGroupsRequestData(), version)
    case ApiKeys.SASL_HANDSHAKE => new SaslHandshakeRequest(new SaslHandshakeRequestData())
    case ApiKeys.API_VERSIONS => new ApiVersionsRequest(new ApiVersionsRequestData(), version)
    case ApiKeys.CREATE_TOPICS => new CreateTopicsRequest(new CreateTopicsRequestData().toStruct(version), version)
    case ApiKeys.DELETE_TOPICS => new DeleteTopicsRequest(new DeleteTopicsRequestData().toStruct(version), version)
    case ApiKeys.DELETE_RECORDS => new DeleteRecordsRequest(new DeleteRecordsRequestData().toStruct(version), version)
    case ApiKeys.INIT_PRODUCER_ID => new InitProducerIdRequest(new InitProducerIdRequestData().toStruct(version), version)
    case ApiKeys.OFFSET_FOR_LEADER_EPOCH => new OffsetsForLeaderEpochRequest(new OffsetForLeaderEpochResponseData().toStruct(version), version)
    case ApiKeys.ADD_PARTITIONS_TO_TXN => new AddPartitionsToTxnRequest(new AddPartitionsToTxnRequestData(), version)
    case ApiKeys.ADD_OFFSETS_TO_TXN => new AddOffsetsToTxnRequest(new AddOffsetsToTxnRequestData(), version)
    case ApiKeys.END_TXN =>  new EndTxnRequest(new EndTxnRequestData().toStruct(version), version)
    case ApiKeys.WRITE_TXN_MARKERS =>  new WriteTxnMarkersRequest(new WriteTxnMarkersRequestData().toStruct(version), version)
    case ApiKeys.TXN_OFFSET_COMMIT => new TxnOffsetCommitRequest(new TxnOffsetCommitRequestData(), version)
    case ApiKeys.DESCRIBE_ACLS =>
      val data = new DescribeAclsRequestData().setResourceTypeFilter(1).setOperation(2).setPermissionType(2)
      new DescribeAclsRequest(data.toStruct(version), version)
    case ApiKeys.CREATE_ACLS =>  new CreateAclsRequest(new CreateAclsRequestData().toStruct(version), version)
    case ApiKeys.DELETE_ACLS => new DeleteAclsRequest(new DeleteAclsRequestData().toStruct(version), version)
    case ApiKeys.DESCRIBE_CONFIGS => new DescribeConfigsRequest(new DescribeConfigsRequestData(), version)
    case ApiKeys.ALTER_CONFIGS => new AlterConfigsRequest(new AlterConfigsRequestData(), version)
    case ApiKeys.ALTER_REPLICA_LOG_DIRS =>  new AlterReplicaLogDirsRequest(new AlterReplicaLogDirsRequestData(), version)
    case ApiKeys.DESCRIBE_LOG_DIRS => new DescribeLogDirsRequest(new DescribeLogDirsRequestData(), version)
    case ApiKeys.SASL_AUTHENTICATE => new SaslAuthenticateRequest(new SaslAuthenticateRequestData(), version)
    case ApiKeys.CREATE_PARTITIONS => new CreatePartitionsRequest(new CreatePartitionsRequestData().toStruct(version), version)
    case ApiKeys.CREATE_DELEGATION_TOKEN => new CreateDelegationTokenRequest(new CreateDelegationTokenRequestData().toStruct(version), version)
    case ApiKeys.RENEW_DELEGATION_TOKEN => new RenewDelegationTokenRequest(new RenewDelegationTokenRequestData(), version)
    case ApiKeys.EXPIRE_DELEGATION_TOKEN => new ExpireDelegationTokenRequest(new ExpireDelegationTokenRequestData().toStruct(version), version)
    case ApiKeys.DESCRIBE_DELEGATION_TOKEN => new DescribeDelegationTokenRequest(new DescribeDelegationTokenRequestData(), version)
    case ApiKeys.DELETE_GROUPS => new DeleteGroupsRequest(new DeleteGroupsRequestData(), version)
    case ApiKeys.ELECT_LEADERS => new ElectLeadersRequest(new ElectLeadersRequestData().toStruct(version), version)
    case ApiKeys.INCREMENTAL_ALTER_CONFIGS => new IncrementalAlterConfigsRequest.Builder(new IncrementalAlterConfigsRequestData()).build(version)
    case ApiKeys.ALTER_PARTITION_REASSIGNMENTS => new AlterPartitionReassignmentsRequest.Builder(new AlterPartitionReassignmentsRequestData()).build(version)
    case ApiKeys.LIST_PARTITION_REASSIGNMENTS => new ListPartitionReassignmentsRequest.Builder(new ListPartitionReassignmentsRequestData()).build(version)
    case ApiKeys.OFFSET_DELETE => new OffsetDeleteRequest(new OffsetDeleteRequestData(), version)
    case ApiKeys.DESCRIBE_CLIENT_QUOTAS => new DescribeClientQuotasRequest(new DescribeClientQuotasRequestData(), version)
    case ApiKeys.ALTER_CLIENT_QUOTAS => new AlterClientQuotasRequest(new AlterClientQuotasRequestData(), version)
    case ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS => new DescribeUserScramCredentialsRequest.Builder(new DescribeUserScramCredentialsRequestData()).build(version)
    case ApiKeys.ALTER_USER_SCRAM_CREDENTIALS => new AlterUserScramCredentialsRequest.Builder(new AlterUserScramCredentialsRequestData()).build(version)
    case ApiKeys.VOTE => new VoteRequest.Builder(new VoteRequestData()).build(version)
    case ApiKeys.BEGIN_QUORUM_EPOCH => new BeginQuorumEpochRequest.Builder(new BeginQuorumEpochRequestData()).build(version)
    case ApiKeys.END_QUORUM_EPOCH => new EndQuorumEpochRequest.Builder(new EndQuorumEpochRequestData()).build(version)
    case ApiKeys.DESCRIBE_QUORUM => new DescribeQuorumRequest.Builder(new DescribeQuorumRequestData()).build(version)
    case ApiKeys.ALTER_ISR => new AlterIsrRequest.Builder(new AlterIsrRequestData()).build(version)
    case ApiKeys.UPDATE_FEATURES => new UpdateFeaturesRequest.Builder(new UpdateFeaturesRequestData()).build(version)
    case _ => throw new AssertionError(String.format("Request type %s is not tested in `RequestConvertToJsonTest`", apiKey))
  }

  def createResponseFromApiKey(apiKey: ApiKeys, version: Short): AbstractResponse = apiKey match {
    case ApiKeys.PRODUCE => new ProduceResponse(new ProduceResponseData().toStruct(version))
    case ApiKeys.FETCH => new FetchResponse(new FetchResponseData())
    case ApiKeys.LIST_OFFSETS => new ListOffsetResponse(new ListOffsetResponseData())
    case ApiKeys.METADATA => new MetadataResponse(new MetadataResponseData())
    case ApiKeys.OFFSET_COMMIT => new OffsetCommitResponse(new OffsetCommitResponseData())
    case ApiKeys.OFFSET_FETCH => new OffsetFetchResponse(new OffsetFetchResponseData().toStruct(version), version)
    case ApiKeys.FIND_COORDINATOR => new FindCoordinatorResponse(new FindCoordinatorResponseData())
    case ApiKeys.JOIN_GROUP => new JoinGroupResponse(new JoinGroupResponseData())
    case ApiKeys.HEARTBEAT => new HeartbeatResponse(new HeartbeatResponseData())
    case ApiKeys.LEAVE_GROUP => new LeaveGroupResponse(new LeaveGroupResponseData())
    case ApiKeys.SYNC_GROUP => new SyncGroupResponse(new SyncGroupResponseData())
    case ApiKeys.STOP_REPLICA => new StopReplicaResponse(new StopReplicaResponseData())
    case ApiKeys.CONTROLLED_SHUTDOWN => new ControlledShutdownResponse(new ControlledShutdownResponseData())
    case ApiKeys.UPDATE_METADATA => new UpdateMetadataResponse(new UpdateMetadataResponseData())
    case ApiKeys.LEADER_AND_ISR => new LeaderAndIsrResponse(new LeaderAndIsrResponseData())
    case ApiKeys.DESCRIBE_GROUPS => new DescribeGroupsResponse(new DescribeGroupsResponseData())
    case ApiKeys.LIST_GROUPS => new ListGroupsResponse(new ListGroupsResponseData())
    case ApiKeys.SASL_HANDSHAKE => new SaslHandshakeResponse(new SaslHandshakeResponseData())
    case ApiKeys.API_VERSIONS => new ApiVersionsResponse(new ApiVersionsResponseData())
    case ApiKeys.CREATE_TOPICS => new CreateTopicsResponse(new CreateTopicsResponseData())
    case ApiKeys.DELETE_TOPICS => new DeleteTopicsResponse(new DeleteTopicsResponseData())
    case ApiKeys.DELETE_RECORDS => new DeleteRecordsResponse(new DeleteRecordsResponseData())
    case ApiKeys.INIT_PRODUCER_ID => new InitProducerIdResponse(new InitProducerIdResponseData())
    case ApiKeys.OFFSET_FOR_LEADER_EPOCH => new OffsetsForLeaderEpochResponse(new OffsetForLeaderEpochResponseData().toStruct(version))
    case ApiKeys.ADD_PARTITIONS_TO_TXN => new AddPartitionsToTxnResponse(new AddPartitionsToTxnResponseData().toStruct(version), version)
    case ApiKeys.ADD_OFFSETS_TO_TXN => new AddOffsetsToTxnResponse(new AddOffsetsToTxnResponseData())
    case ApiKeys.END_TXN => new EndTxnResponse(new EndTxnResponseData())
    case ApiKeys.WRITE_TXN_MARKERS => new WriteTxnMarkersResponse(new WriteTxnMarkersResponseData().toStruct(version), version)
    case ApiKeys.TXN_OFFSET_COMMIT => new TxnOffsetCommitResponse(new TxnOffsetCommitResponseData())
    case ApiKeys.DESCRIBE_ACLS => new DescribeAclsResponse(new DescribeAclsResponseData())
    case ApiKeys.CREATE_ACLS => new CreateAclsResponse(new CreateAclsResponseData())
    case ApiKeys.DELETE_ACLS => new DeleteAclsResponse(new DeleteAclsResponseData())
    case ApiKeys.DESCRIBE_CONFIGS => new DescribeConfigsResponse(new DescribeConfigsResponseData())
    case ApiKeys.ALTER_CONFIGS => new AlterConfigsResponse(new AlterConfigsResponseData())
    case ApiKeys.ALTER_REPLICA_LOG_DIRS => new AlterReplicaLogDirsResponse(new AlterReplicaLogDirsResponseData())
    case ApiKeys.DESCRIBE_LOG_DIRS => new DescribeLogDirsResponse(new DescribeLogDirsResponseData())
    case ApiKeys.SASL_AUTHENTICATE => new SaslAuthenticateResponse(new SaslAuthenticateResponseData())
    case ApiKeys.CREATE_PARTITIONS => new CreatePartitionsResponse(new CreatePartitionsResponseData())
    case ApiKeys.CREATE_DELEGATION_TOKEN => new CreateDelegationTokenResponse(new CreateDelegationTokenResponseData())
    case ApiKeys.RENEW_DELEGATION_TOKEN => new RenewDelegationTokenResponse(new RenewDelegationTokenResponseData())
    case ApiKeys.EXPIRE_DELEGATION_TOKEN => new ExpireDelegationTokenResponse(new ExpireDelegationTokenResponseData())
    case ApiKeys.DESCRIBE_DELEGATION_TOKEN => new DescribeDelegationTokenResponse(new DescribeDelegationTokenResponseData().toStruct(version), version)
    case ApiKeys.DELETE_GROUPS => new DeleteGroupsResponse(new DeleteGroupsResponseData())
    case ApiKeys.ELECT_LEADERS => new ElectLeadersResponse(new ElectLeadersResponseData().toStruct(version), version)
    case ApiKeys.INCREMENTAL_ALTER_CONFIGS => new IncrementalAlterConfigsResponse(new IncrementalAlterConfigsResponseData())
    case ApiKeys.ALTER_PARTITION_REASSIGNMENTS => new AlterPartitionReassignmentsResponse(new AlterPartitionReassignmentsResponseData())
    case ApiKeys.LIST_PARTITION_REASSIGNMENTS => new ListPartitionReassignmentsResponse(new ListPartitionReassignmentsResponseData())
    case ApiKeys.OFFSET_DELETE => new OffsetDeleteResponse(new OffsetDeleteResponseData())
    case ApiKeys.DESCRIBE_CLIENT_QUOTAS => new DescribeClientQuotasResponse(new DescribeClientQuotasResponseData().toStruct(version), version)
    case ApiKeys.ALTER_CLIENT_QUOTAS => new AlterClientQuotasResponse(new AlterClientQuotasResponseData().toStruct(version), version)
    case ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS => new DescribeUserScramCredentialsResponse(new DescribeUserScramCredentialsResponseData())
    case ApiKeys.ALTER_USER_SCRAM_CREDENTIALS => new AlterUserScramCredentialsResponse(new AlterUserScramCredentialsResponseData())
    case ApiKeys.VOTE => new VoteResponse(new VoteResponseData())
    case ApiKeys.BEGIN_QUORUM_EPOCH => new BeginQuorumEpochResponse(new BeginQuorumEpochResponseData())
    case ApiKeys.END_QUORUM_EPOCH => new EndQuorumEpochResponse(new EndQuorumEpochResponseData())
    case ApiKeys.DESCRIBE_QUORUM => new DescribeQuorumResponse(new DescribeQuorumResponseData())
    case ApiKeys.ALTER_ISR => new AlterIsrResponse(new AlterIsrResponseData())
    case ApiKeys.UPDATE_FEATURES => new UpdateFeaturesResponse(new UpdateFeaturesResponseData())
    case _ => throw new AssertionError(String.format("Response type %s not tested in `RequestConvertToJsonTest`", apiKey))
  }

  @Test
  def testAllRequestTypesHandled(): Unit = {
    val unhandledKeys = ArrayBuffer[String]()
    ApiKeys.values().foreach(key => {
      val version: Short = 0
      val req = createRequestsFromApiKey(key, version)
      try {
        RequestConvertToJson.request(req, false)
      } catch {
        case _ : Throwable => unhandledKeys += key.toString
      }
    })
    assert(unhandledKeys.isEmpty, String.format("%s request keys not handled in RequestConvertToJson", unhandledKeys))
  }

  @Test
  def testAllResponseTypesHandled(): Unit = {
    val unhandledKeys = ArrayBuffer[String]()
    ApiKeys.values().foreach(key => {
      val version: Short = 0
      val res = createResponseFromApiKey(key, version)
      try {
        RequestConvertToJson.response(res, version)
      } catch {
        case _ : Throwable => unhandledKeys += key.toString
      }
    })
    assert(unhandledKeys.isEmpty, String.format("%s response keys not handled in RequestConvertToJson", unhandledKeys))
  }
}
