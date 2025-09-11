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
package org.apache.kafka.network;

import org.apache.kafka.common.message.AddOffsetsToTxnRequestDataJsonConverter;
import org.apache.kafka.common.message.AddOffsetsToTxnResponseDataJsonConverter;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestDataJsonConverter;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseDataJsonConverter;
import org.apache.kafka.common.message.AddRaftVoterRequestDataJsonConverter;
import org.apache.kafka.common.message.AddRaftVoterResponseDataJsonConverter;
import org.apache.kafka.common.message.AllocateProducerIdsRequestDataJsonConverter;
import org.apache.kafka.common.message.AllocateProducerIdsResponseDataJsonConverter;
import org.apache.kafka.common.message.AlterClientQuotasRequestDataJsonConverter;
import org.apache.kafka.common.message.AlterClientQuotasResponseDataJsonConverter;
import org.apache.kafka.common.message.AlterConfigsRequestDataJsonConverter;
import org.apache.kafka.common.message.AlterConfigsResponseDataJsonConverter;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestDataJsonConverter;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseDataJsonConverter;
import org.apache.kafka.common.message.AlterPartitionRequestDataJsonConverter;
import org.apache.kafka.common.message.AlterPartitionResponseDataJsonConverter;
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestDataJsonConverter;
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseDataJsonConverter;
import org.apache.kafka.common.message.AlterShareGroupOffsetsRequestDataJsonConverter;
import org.apache.kafka.common.message.AlterShareGroupOffsetsResponseDataJsonConverter;
import org.apache.kafka.common.message.AlterUserScramCredentialsRequestDataJsonConverter;
import org.apache.kafka.common.message.AlterUserScramCredentialsResponseDataJsonConverter;
import org.apache.kafka.common.message.ApiVersionsRequestDataJsonConverter;
import org.apache.kafka.common.message.ApiVersionsResponseDataJsonConverter;
import org.apache.kafka.common.message.AssignReplicasToDirsRequestDataJsonConverter;
import org.apache.kafka.common.message.AssignReplicasToDirsResponseDataJsonConverter;
import org.apache.kafka.common.message.BeginQuorumEpochRequestDataJsonConverter;
import org.apache.kafka.common.message.BeginQuorumEpochResponseDataJsonConverter;
import org.apache.kafka.common.message.BrokerHeartbeatRequestDataJsonConverter;
import org.apache.kafka.common.message.BrokerHeartbeatResponseDataJsonConverter;
import org.apache.kafka.common.message.BrokerRegistrationRequestDataJsonConverter;
import org.apache.kafka.common.message.BrokerRegistrationResponseDataJsonConverter;
import org.apache.kafka.common.message.ConsumerGroupDescribeRequestDataJsonConverter;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseDataJsonConverter;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestDataJsonConverter;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseDataJsonConverter;
import org.apache.kafka.common.message.ControllerRegistrationRequestDataJsonConverter;
import org.apache.kafka.common.message.ControllerRegistrationResponseDataJsonConverter;
import org.apache.kafka.common.message.CreateAclsRequestDataJsonConverter;
import org.apache.kafka.common.message.CreateAclsResponseDataJsonConverter;
import org.apache.kafka.common.message.CreateDelegationTokenRequestDataJsonConverter;
import org.apache.kafka.common.message.CreateDelegationTokenResponseDataJsonConverter;
import org.apache.kafka.common.message.CreatePartitionsRequestDataJsonConverter;
import org.apache.kafka.common.message.CreatePartitionsResponseDataJsonConverter;
import org.apache.kafka.common.message.CreateTopicsRequestDataJsonConverter;
import org.apache.kafka.common.message.CreateTopicsResponseDataJsonConverter;
import org.apache.kafka.common.message.DeleteAclsRequestDataJsonConverter;
import org.apache.kafka.common.message.DeleteAclsResponseDataJsonConverter;
import org.apache.kafka.common.message.DeleteGroupsRequestDataJsonConverter;
import org.apache.kafka.common.message.DeleteGroupsResponseDataJsonConverter;
import org.apache.kafka.common.message.DeleteRecordsRequestDataJsonConverter;
import org.apache.kafka.common.message.DeleteRecordsResponseDataJsonConverter;
import org.apache.kafka.common.message.DeleteShareGroupOffsetsRequestDataJsonConverter;
import org.apache.kafka.common.message.DeleteShareGroupOffsetsResponseDataJsonConverter;
import org.apache.kafka.common.message.DeleteShareGroupStateRequestDataJsonConverter;
import org.apache.kafka.common.message.DeleteShareGroupStateResponseDataJsonConverter;
import org.apache.kafka.common.message.DeleteTopicsRequestDataJsonConverter;
import org.apache.kafka.common.message.DeleteTopicsResponseDataJsonConverter;
import org.apache.kafka.common.message.DescribeAclsRequestDataJsonConverter;
import org.apache.kafka.common.message.DescribeAclsResponseDataJsonConverter;
import org.apache.kafka.common.message.DescribeClientQuotasRequestDataJsonConverter;
import org.apache.kafka.common.message.DescribeClientQuotasResponseDataJsonConverter;
import org.apache.kafka.common.message.DescribeClusterRequestDataJsonConverter;
import org.apache.kafka.common.message.DescribeClusterResponseDataJsonConverter;
import org.apache.kafka.common.message.DescribeConfigsRequestDataJsonConverter;
import org.apache.kafka.common.message.DescribeConfigsResponseDataJsonConverter;
import org.apache.kafka.common.message.DescribeDelegationTokenRequestDataJsonConverter;
import org.apache.kafka.common.message.DescribeDelegationTokenResponseDataJsonConverter;
import org.apache.kafka.common.message.DescribeGroupsRequestDataJsonConverter;
import org.apache.kafka.common.message.DescribeGroupsResponseDataJsonConverter;
import org.apache.kafka.common.message.DescribeLogDirsRequestDataJsonConverter;
import org.apache.kafka.common.message.DescribeLogDirsResponseDataJsonConverter;
import org.apache.kafka.common.message.DescribeProducersRequestDataJsonConverter;
import org.apache.kafka.common.message.DescribeProducersResponseDataJsonConverter;
import org.apache.kafka.common.message.DescribeQuorumRequestDataJsonConverter;
import org.apache.kafka.common.message.DescribeQuorumResponseDataJsonConverter;
import org.apache.kafka.common.message.DescribeShareGroupOffsetsRequestDataJsonConverter;
import org.apache.kafka.common.message.DescribeShareGroupOffsetsResponseDataJsonConverter;
import org.apache.kafka.common.message.DescribeTopicPartitionsRequestDataJsonConverter;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseDataJsonConverter;
import org.apache.kafka.common.message.DescribeTransactionsRequestDataJsonConverter;
import org.apache.kafka.common.message.DescribeTransactionsResponseDataJsonConverter;
import org.apache.kafka.common.message.DescribeUserScramCredentialsRequestDataJsonConverter;
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseDataJsonConverter;
import org.apache.kafka.common.message.ElectLeadersRequestDataJsonConverter;
import org.apache.kafka.common.message.ElectLeadersResponseDataJsonConverter;
import org.apache.kafka.common.message.EndQuorumEpochRequestDataJsonConverter;
import org.apache.kafka.common.message.EndQuorumEpochResponseDataJsonConverter;
import org.apache.kafka.common.message.EndTxnRequestDataJsonConverter;
import org.apache.kafka.common.message.EndTxnResponseDataJsonConverter;
import org.apache.kafka.common.message.EnvelopeRequestDataJsonConverter;
import org.apache.kafka.common.message.EnvelopeResponseDataJsonConverter;
import org.apache.kafka.common.message.ExpireDelegationTokenRequestDataJsonConverter;
import org.apache.kafka.common.message.ExpireDelegationTokenResponseDataJsonConverter;
import org.apache.kafka.common.message.FetchRequestDataJsonConverter;
import org.apache.kafka.common.message.FetchResponseDataJsonConverter;
import org.apache.kafka.common.message.FetchSnapshotRequestDataJsonConverter;
import org.apache.kafka.common.message.FetchSnapshotResponseDataJsonConverter;
import org.apache.kafka.common.message.FindCoordinatorRequestDataJsonConverter;
import org.apache.kafka.common.message.FindCoordinatorResponseDataJsonConverter;
import org.apache.kafka.common.message.GetTelemetrySubscriptionsRequestDataJsonConverter;
import org.apache.kafka.common.message.GetTelemetrySubscriptionsResponseDataJsonConverter;
import org.apache.kafka.common.message.HeartbeatRequestDataJsonConverter;
import org.apache.kafka.common.message.HeartbeatResponseDataJsonConverter;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestDataJsonConverter;
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseDataJsonConverter;
import org.apache.kafka.common.message.InitProducerIdRequestDataJsonConverter;
import org.apache.kafka.common.message.InitProducerIdResponseDataJsonConverter;
import org.apache.kafka.common.message.InitializeShareGroupStateRequestDataJsonConverter;
import org.apache.kafka.common.message.InitializeShareGroupStateResponseDataJsonConverter;
import org.apache.kafka.common.message.JoinGroupRequestDataJsonConverter;
import org.apache.kafka.common.message.JoinGroupResponseDataJsonConverter;
import org.apache.kafka.common.message.LeaveGroupRequestDataJsonConverter;
import org.apache.kafka.common.message.LeaveGroupResponseDataJsonConverter;
import org.apache.kafka.common.message.ListConfigResourcesRequestDataJsonConverter;
import org.apache.kafka.common.message.ListConfigResourcesResponseDataJsonConverter;
import org.apache.kafka.common.message.ListGroupsRequestDataJsonConverter;
import org.apache.kafka.common.message.ListGroupsResponseDataJsonConverter;
import org.apache.kafka.common.message.ListOffsetsRequestDataJsonConverter;
import org.apache.kafka.common.message.ListOffsetsResponseDataJsonConverter;
import org.apache.kafka.common.message.ListPartitionReassignmentsRequestDataJsonConverter;
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseDataJsonConverter;
import org.apache.kafka.common.message.ListTransactionsRequestDataJsonConverter;
import org.apache.kafka.common.message.ListTransactionsResponseDataJsonConverter;
import org.apache.kafka.common.message.MetadataRequestDataJsonConverter;
import org.apache.kafka.common.message.MetadataResponseDataJsonConverter;
import org.apache.kafka.common.message.OffsetCommitRequestDataJsonConverter;
import org.apache.kafka.common.message.OffsetCommitResponseDataJsonConverter;
import org.apache.kafka.common.message.OffsetDeleteRequestDataJsonConverter;
import org.apache.kafka.common.message.OffsetDeleteResponseDataJsonConverter;
import org.apache.kafka.common.message.OffsetFetchRequestDataJsonConverter;
import org.apache.kafka.common.message.OffsetFetchResponseDataJsonConverter;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestDataJsonConverter;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseDataJsonConverter;
import org.apache.kafka.common.message.ProduceRequestDataJsonConverter;
import org.apache.kafka.common.message.ProduceResponseDataJsonConverter;
import org.apache.kafka.common.message.PushTelemetryRequestDataJsonConverter;
import org.apache.kafka.common.message.PushTelemetryResponseDataJsonConverter;
import org.apache.kafka.common.message.ReadShareGroupStateRequestDataJsonConverter;
import org.apache.kafka.common.message.ReadShareGroupStateResponseDataJsonConverter;
import org.apache.kafka.common.message.ReadShareGroupStateSummaryRequestDataJsonConverter;
import org.apache.kafka.common.message.ReadShareGroupStateSummaryResponseDataJsonConverter;
import org.apache.kafka.common.message.RemoveRaftVoterRequestDataJsonConverter;
import org.apache.kafka.common.message.RemoveRaftVoterResponseDataJsonConverter;
import org.apache.kafka.common.message.RenewDelegationTokenRequestDataJsonConverter;
import org.apache.kafka.common.message.RenewDelegationTokenResponseDataJsonConverter;
import org.apache.kafka.common.message.RequestHeaderDataJsonConverter;
import org.apache.kafka.common.message.SaslAuthenticateRequestDataJsonConverter;
import org.apache.kafka.common.message.SaslAuthenticateResponseDataJsonConverter;
import org.apache.kafka.common.message.SaslHandshakeRequestDataJsonConverter;
import org.apache.kafka.common.message.SaslHandshakeResponseDataJsonConverter;
import org.apache.kafka.common.message.ShareAcknowledgeRequestDataJsonConverter;
import org.apache.kafka.common.message.ShareAcknowledgeResponseDataJsonConverter;
import org.apache.kafka.common.message.ShareFetchRequestDataJsonConverter;
import org.apache.kafka.common.message.ShareFetchResponseDataJsonConverter;
import org.apache.kafka.common.message.ShareGroupDescribeRequestDataJsonConverter;
import org.apache.kafka.common.message.ShareGroupDescribeResponseDataJsonConverter;
import org.apache.kafka.common.message.ShareGroupHeartbeatRequestDataJsonConverter;
import org.apache.kafka.common.message.ShareGroupHeartbeatResponseDataJsonConverter;
import org.apache.kafka.common.message.StreamsGroupDescribeRequestDataJsonConverter;
import org.apache.kafka.common.message.StreamsGroupDescribeResponseDataJsonConverter;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestDataJsonConverter;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseDataJsonConverter;
import org.apache.kafka.common.message.SyncGroupRequestDataJsonConverter;
import org.apache.kafka.common.message.SyncGroupResponseDataJsonConverter;
import org.apache.kafka.common.message.TxnOffsetCommitRequestDataJsonConverter;
import org.apache.kafka.common.message.TxnOffsetCommitResponseDataJsonConverter;
import org.apache.kafka.common.message.UnregisterBrokerRequestDataJsonConverter;
import org.apache.kafka.common.message.UnregisterBrokerResponseDataJsonConverter;
import org.apache.kafka.common.message.UpdateFeaturesRequestDataJsonConverter;
import org.apache.kafka.common.message.UpdateFeaturesResponseDataJsonConverter;
import org.apache.kafka.common.message.UpdateRaftVoterRequestDataJsonConverter;
import org.apache.kafka.common.message.UpdateRaftVoterResponseDataJsonConverter;
import org.apache.kafka.common.message.VoteRequestDataJsonConverter;
import org.apache.kafka.common.message.VoteResponseDataJsonConverter;
import org.apache.kafka.common.message.WriteShareGroupStateRequestDataJsonConverter;
import org.apache.kafka.common.message.WriteShareGroupStateResponseDataJsonConverter;
import org.apache.kafka.common.message.WriteTxnMarkersRequestDataJsonConverter;
import org.apache.kafka.common.message.WriteTxnMarkersResponseDataJsonConverter;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.AddOffsetsToTxnRequest;
import org.apache.kafka.common.requests.AddOffsetsToTxnResponse;
import org.apache.kafka.common.requests.AddPartitionsToTxnRequest;
import org.apache.kafka.common.requests.AddPartitionsToTxnResponse;
import org.apache.kafka.common.requests.AddRaftVoterRequest;
import org.apache.kafka.common.requests.AddRaftVoterResponse;
import org.apache.kafka.common.requests.AllocateProducerIdsRequest;
import org.apache.kafka.common.requests.AllocateProducerIdsResponse;
import org.apache.kafka.common.requests.AlterClientQuotasRequest;
import org.apache.kafka.common.requests.AlterClientQuotasResponse;
import org.apache.kafka.common.requests.AlterConfigsRequest;
import org.apache.kafka.common.requests.AlterConfigsResponse;
import org.apache.kafka.common.requests.AlterPartitionReassignmentsRequest;
import org.apache.kafka.common.requests.AlterPartitionReassignmentsResponse;
import org.apache.kafka.common.requests.AlterPartitionRequest;
import org.apache.kafka.common.requests.AlterPartitionResponse;
import org.apache.kafka.common.requests.AlterReplicaLogDirsRequest;
import org.apache.kafka.common.requests.AlterReplicaLogDirsResponse;
import org.apache.kafka.common.requests.AlterShareGroupOffsetsRequest;
import org.apache.kafka.common.requests.AlterShareGroupOffsetsResponse;
import org.apache.kafka.common.requests.AlterUserScramCredentialsRequest;
import org.apache.kafka.common.requests.AlterUserScramCredentialsResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.AssignReplicasToDirsRequest;
import org.apache.kafka.common.requests.AssignReplicasToDirsResponse;
import org.apache.kafka.common.requests.BeginQuorumEpochRequest;
import org.apache.kafka.common.requests.BeginQuorumEpochResponse;
import org.apache.kafka.common.requests.BrokerHeartbeatRequest;
import org.apache.kafka.common.requests.BrokerHeartbeatResponse;
import org.apache.kafka.common.requests.BrokerRegistrationRequest;
import org.apache.kafka.common.requests.BrokerRegistrationResponse;
import org.apache.kafka.common.requests.ConsumerGroupDescribeRequest;
import org.apache.kafka.common.requests.ConsumerGroupDescribeResponse;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse;
import org.apache.kafka.common.requests.ControllerRegistrationRequest;
import org.apache.kafka.common.requests.ControllerRegistrationResponse;
import org.apache.kafka.common.requests.CreateAclsRequest;
import org.apache.kafka.common.requests.CreateAclsResponse;
import org.apache.kafka.common.requests.CreateDelegationTokenRequest;
import org.apache.kafka.common.requests.CreateDelegationTokenResponse;
import org.apache.kafka.common.requests.CreatePartitionsRequest;
import org.apache.kafka.common.requests.CreatePartitionsResponse;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.DeleteAclsRequest;
import org.apache.kafka.common.requests.DeleteAclsResponse;
import org.apache.kafka.common.requests.DeleteGroupsRequest;
import org.apache.kafka.common.requests.DeleteGroupsResponse;
import org.apache.kafka.common.requests.DeleteRecordsRequest;
import org.apache.kafka.common.requests.DeleteRecordsResponse;
import org.apache.kafka.common.requests.DeleteShareGroupOffsetsRequest;
import org.apache.kafka.common.requests.DeleteShareGroupOffsetsResponse;
import org.apache.kafka.common.requests.DeleteShareGroupStateRequest;
import org.apache.kafka.common.requests.DeleteShareGroupStateResponse;
import org.apache.kafka.common.requests.DeleteTopicsRequest;
import org.apache.kafka.common.requests.DeleteTopicsResponse;
import org.apache.kafka.common.requests.DescribeAclsRequest;
import org.apache.kafka.common.requests.DescribeAclsResponse;
import org.apache.kafka.common.requests.DescribeClientQuotasRequest;
import org.apache.kafka.common.requests.DescribeClientQuotasResponse;
import org.apache.kafka.common.requests.DescribeClusterRequest;
import org.apache.kafka.common.requests.DescribeClusterResponse;
import org.apache.kafka.common.requests.DescribeConfigsRequest;
import org.apache.kafka.common.requests.DescribeConfigsResponse;
import org.apache.kafka.common.requests.DescribeDelegationTokenRequest;
import org.apache.kafka.common.requests.DescribeDelegationTokenResponse;
import org.apache.kafka.common.requests.DescribeGroupsRequest;
import org.apache.kafka.common.requests.DescribeGroupsResponse;
import org.apache.kafka.common.requests.DescribeLogDirsRequest;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;
import org.apache.kafka.common.requests.DescribeProducersRequest;
import org.apache.kafka.common.requests.DescribeProducersResponse;
import org.apache.kafka.common.requests.DescribeQuorumRequest;
import org.apache.kafka.common.requests.DescribeQuorumResponse;
import org.apache.kafka.common.requests.DescribeShareGroupOffsetsRequest;
import org.apache.kafka.common.requests.DescribeShareGroupOffsetsResponse;
import org.apache.kafka.common.requests.DescribeTopicPartitionsRequest;
import org.apache.kafka.common.requests.DescribeTopicPartitionsResponse;
import org.apache.kafka.common.requests.DescribeTransactionsRequest;
import org.apache.kafka.common.requests.DescribeTransactionsResponse;
import org.apache.kafka.common.requests.DescribeUserScramCredentialsRequest;
import org.apache.kafka.common.requests.DescribeUserScramCredentialsResponse;
import org.apache.kafka.common.requests.ElectLeadersRequest;
import org.apache.kafka.common.requests.ElectLeadersResponse;
import org.apache.kafka.common.requests.EndQuorumEpochRequest;
import org.apache.kafka.common.requests.EndQuorumEpochResponse;
import org.apache.kafka.common.requests.EndTxnRequest;
import org.apache.kafka.common.requests.EndTxnResponse;
import org.apache.kafka.common.requests.EnvelopeRequest;
import org.apache.kafka.common.requests.EnvelopeResponse;
import org.apache.kafka.common.requests.ExpireDelegationTokenRequest;
import org.apache.kafka.common.requests.ExpireDelegationTokenResponse;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.FetchSnapshotRequest;
import org.apache.kafka.common.requests.FetchSnapshotResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.GetTelemetrySubscriptionsRequest;
import org.apache.kafka.common.requests.GetTelemetrySubscriptionsResponse;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.IncrementalAlterConfigsRequest;
import org.apache.kafka.common.requests.IncrementalAlterConfigsResponse;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.requests.InitializeShareGroupStateRequest;
import org.apache.kafka.common.requests.InitializeShareGroupStateResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupResponse;
import org.apache.kafka.common.requests.ListConfigResourcesRequest;
import org.apache.kafka.common.requests.ListConfigResourcesResponse;
import org.apache.kafka.common.requests.ListGroupsRequest;
import org.apache.kafka.common.requests.ListGroupsResponse;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.ListPartitionReassignmentsRequest;
import org.apache.kafka.common.requests.ListPartitionReassignmentsResponse;
import org.apache.kafka.common.requests.ListTransactionsRequest;
import org.apache.kafka.common.requests.ListTransactionsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetDeleteRequest;
import org.apache.kafka.common.requests.OffsetDeleteResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.PushTelemetryRequest;
import org.apache.kafka.common.requests.PushTelemetryResponse;
import org.apache.kafka.common.requests.ReadShareGroupStateRequest;
import org.apache.kafka.common.requests.ReadShareGroupStateResponse;
import org.apache.kafka.common.requests.ReadShareGroupStateSummaryRequest;
import org.apache.kafka.common.requests.ReadShareGroupStateSummaryResponse;
import org.apache.kafka.common.requests.RemoveRaftVoterRequest;
import org.apache.kafka.common.requests.RemoveRaftVoterResponse;
import org.apache.kafka.common.requests.RenewDelegationTokenRequest;
import org.apache.kafka.common.requests.RenewDelegationTokenResponse;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.SaslAuthenticateRequest;
import org.apache.kafka.common.requests.SaslAuthenticateResponse;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.requests.SaslHandshakeResponse;
import org.apache.kafka.common.requests.ShareAcknowledgeRequest;
import org.apache.kafka.common.requests.ShareAcknowledgeResponse;
import org.apache.kafka.common.requests.ShareFetchRequest;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.requests.ShareGroupDescribeRequest;
import org.apache.kafka.common.requests.ShareGroupDescribeResponse;
import org.apache.kafka.common.requests.ShareGroupHeartbeatRequest;
import org.apache.kafka.common.requests.ShareGroupHeartbeatResponse;
import org.apache.kafka.common.requests.StreamsGroupDescribeRequest;
import org.apache.kafka.common.requests.StreamsGroupDescribeResponse;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatRequest;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatResponse;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest;
import org.apache.kafka.common.requests.TxnOffsetCommitResponse;
import org.apache.kafka.common.requests.UnregisterBrokerRequest;
import org.apache.kafka.common.requests.UnregisterBrokerResponse;
import org.apache.kafka.common.requests.UpdateFeaturesRequest;
import org.apache.kafka.common.requests.UpdateFeaturesResponse;
import org.apache.kafka.common.requests.UpdateRaftVoterRequest;
import org.apache.kafka.common.requests.UpdateRaftVoterResponse;
import org.apache.kafka.common.requests.VoteRequest;
import org.apache.kafka.common.requests.VoteResponse;
import org.apache.kafka.common.requests.WriteShareGroupStateRequest;
import org.apache.kafka.common.requests.WriteShareGroupStateResponse;
import org.apache.kafka.common.requests.WriteTxnMarkersRequest;
import org.apache.kafka.common.requests.WriteTxnMarkersResponse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import java.util.Optional;

public class RequestConvertToJson {

    public static JsonNode request(AbstractRequest request) {
        return switch (request.apiKey()) {
            case ADD_OFFSETS_TO_TXN ->
                AddOffsetsToTxnRequestDataJsonConverter.write(((AddOffsetsToTxnRequest) request).data(), request.version());
            case ADD_PARTITIONS_TO_TXN ->
                AddPartitionsToTxnRequestDataJsonConverter.write(((AddPartitionsToTxnRequest) request).data(), request.version());
            case ADD_RAFT_VOTER ->
                AddRaftVoterRequestDataJsonConverter.write(((AddRaftVoterRequest) request).data(), request.version());
            case ALLOCATE_PRODUCER_IDS ->
                AllocateProducerIdsRequestDataJsonConverter.write(((AllocateProducerIdsRequest) request).data(), request.version());
            case ALTER_CLIENT_QUOTAS ->
                AlterClientQuotasRequestDataJsonConverter.write(((AlterClientQuotasRequest) request).data(), request.version());
            case ALTER_CONFIGS ->
                AlterConfigsRequestDataJsonConverter.write(((AlterConfigsRequest) request).data(), request.version());
            case ALTER_PARTITION_REASSIGNMENTS ->
                AlterPartitionReassignmentsRequestDataJsonConverter.write(((AlterPartitionReassignmentsRequest) request).data(), request.version());
            case ALTER_PARTITION ->
                AlterPartitionRequestDataJsonConverter.write(((AlterPartitionRequest) request).data(), request.version());
            case ALTER_REPLICA_LOG_DIRS ->
                AlterReplicaLogDirsRequestDataJsonConverter.write(((AlterReplicaLogDirsRequest) request).data(), request.version());
            case ALTER_SHARE_GROUP_OFFSETS ->
                AlterShareGroupOffsetsRequestDataJsonConverter.write(((AlterShareGroupOffsetsRequest) request).data(), request.version());
            case ALTER_USER_SCRAM_CREDENTIALS ->
                AlterUserScramCredentialsRequestDataJsonConverter.write(((AlterUserScramCredentialsRequest) request).data(), request.version());
            case API_VERSIONS ->
                ApiVersionsRequestDataJsonConverter.write(((ApiVersionsRequest) request).data(), request.version());
            case ASSIGN_REPLICAS_TO_DIRS ->
                AssignReplicasToDirsRequestDataJsonConverter.write(((AssignReplicasToDirsRequest) request).data(), request.version());
            case BEGIN_QUORUM_EPOCH ->
                BeginQuorumEpochRequestDataJsonConverter.write(((BeginQuorumEpochRequest) request).data(), request.version());
            case BROKER_HEARTBEAT ->
                BrokerHeartbeatRequestDataJsonConverter.write(((BrokerHeartbeatRequest) request).data(), request.version());
            case BROKER_REGISTRATION ->
                BrokerRegistrationRequestDataJsonConverter.write(((BrokerRegistrationRequest) request).data(), request.version());
            case CONSUMER_GROUP_DESCRIBE ->
                ConsumerGroupDescribeRequestDataJsonConverter.write(((ConsumerGroupDescribeRequest) request).data(), request.version());
            case CONSUMER_GROUP_HEARTBEAT ->
                ConsumerGroupHeartbeatRequestDataJsonConverter.write(((ConsumerGroupHeartbeatRequest) request).data(), request.version());
            case CONTROLLER_REGISTRATION ->
                ControllerRegistrationRequestDataJsonConverter.write(((ControllerRegistrationRequest) request).data(), request.version());
            case CREATE_ACLS ->
                CreateAclsRequestDataJsonConverter.write(((CreateAclsRequest) request).data(), request.version());
            case CREATE_DELEGATION_TOKEN ->
                CreateDelegationTokenRequestDataJsonConverter.write(((CreateDelegationTokenRequest) request).data(), request.version());
            case CREATE_PARTITIONS ->
                CreatePartitionsRequestDataJsonConverter.write(((CreatePartitionsRequest) request).data(), request.version());
            case CREATE_TOPICS ->
                CreateTopicsRequestDataJsonConverter.write(((CreateTopicsRequest) request).data(), request.version());
            case DELETE_ACLS ->
                DeleteAclsRequestDataJsonConverter.write(((DeleteAclsRequest) request).data(), request.version());
            case DELETE_GROUPS ->
                DeleteGroupsRequestDataJsonConverter.write(((DeleteGroupsRequest) request).data(), request.version());
            case DELETE_RECORDS ->
                DeleteRecordsRequestDataJsonConverter.write(((DeleteRecordsRequest) request).data(), request.version());
            case DELETE_SHARE_GROUP_OFFSETS ->
                DeleteShareGroupOffsetsRequestDataJsonConverter.write(((DeleteShareGroupOffsetsRequest) request).data(), request.version());
            case DELETE_SHARE_GROUP_STATE ->
                DeleteShareGroupStateRequestDataJsonConverter.write(((DeleteShareGroupStateRequest) request).data(), request.version());
            case DELETE_TOPICS ->
                DeleteTopicsRequestDataJsonConverter.write(((DeleteTopicsRequest) request).data(), request.version());
            case DESCRIBE_ACLS ->
                DescribeAclsRequestDataJsonConverter.write(((DescribeAclsRequest) request).data(), request.version());
            case DESCRIBE_CLIENT_QUOTAS ->
                DescribeClientQuotasRequestDataJsonConverter.write(((DescribeClientQuotasRequest) request).data(), request.version());
            case DESCRIBE_CLUSTER ->
                DescribeClusterRequestDataJsonConverter.write(((DescribeClusterRequest) request).data(), request.version());
            case DESCRIBE_CONFIGS ->
                DescribeConfigsRequestDataJsonConverter.write(((DescribeConfigsRequest) request).data(), request.version());
            case DESCRIBE_DELEGATION_TOKEN ->
                DescribeDelegationTokenRequestDataJsonConverter.write(((DescribeDelegationTokenRequest) request).data(), request.version());
            case DESCRIBE_GROUPS ->
                DescribeGroupsRequestDataJsonConverter.write(((DescribeGroupsRequest) request).data(), request.version());
            case DESCRIBE_LOG_DIRS ->
                DescribeLogDirsRequestDataJsonConverter.write(((DescribeLogDirsRequest) request).data(), request.version());
            case DESCRIBE_PRODUCERS ->
                DescribeProducersRequestDataJsonConverter.write(((DescribeProducersRequest) request).data(), request.version());
            case DESCRIBE_QUORUM ->
                DescribeQuorumRequestDataJsonConverter.write(((DescribeQuorumRequest) request).data(), request.version());
            case DESCRIBE_SHARE_GROUP_OFFSETS ->
                DescribeShareGroupOffsetsRequestDataJsonConverter.write(((DescribeShareGroupOffsetsRequest) request).data(), request.version());
            case DESCRIBE_TOPIC_PARTITIONS ->
                DescribeTopicPartitionsRequestDataJsonConverter.write(((DescribeTopicPartitionsRequest) request).data(), request.version());
            case DESCRIBE_TRANSACTIONS ->
                DescribeTransactionsRequestDataJsonConverter.write(((DescribeTransactionsRequest) request).data(), request.version());
            case DESCRIBE_USER_SCRAM_CREDENTIALS ->
                DescribeUserScramCredentialsRequestDataJsonConverter.write(((DescribeUserScramCredentialsRequest) request).data(), request.version());
            case ELECT_LEADERS ->
                ElectLeadersRequestDataJsonConverter.write(((ElectLeadersRequest) request).data(), request.version());
            case END_QUORUM_EPOCH ->
                EndQuorumEpochRequestDataJsonConverter.write(((EndQuorumEpochRequest) request).data(), request.version());
            case END_TXN -> EndTxnRequestDataJsonConverter.write(((EndTxnRequest) request).data(), request.version());
            case ENVELOPE ->
                EnvelopeRequestDataJsonConverter.write(((EnvelopeRequest) request).data(), request.version());
            case EXPIRE_DELEGATION_TOKEN ->
                ExpireDelegationTokenRequestDataJsonConverter.write(((ExpireDelegationTokenRequest) request).data(), request.version());
            case FETCH -> FetchRequestDataJsonConverter.write(((FetchRequest) request).data(), request.version());
            case FETCH_SNAPSHOT ->
                FetchSnapshotRequestDataJsonConverter.write(((FetchSnapshotRequest) request).data(), request.version());
            case FIND_COORDINATOR ->
                FindCoordinatorRequestDataJsonConverter.write(((FindCoordinatorRequest) request).data(), request.version());
            case GET_TELEMETRY_SUBSCRIPTIONS ->
                GetTelemetrySubscriptionsRequestDataJsonConverter.write(((GetTelemetrySubscriptionsRequest) request).data(), request.version());
            case HEARTBEAT ->
                HeartbeatRequestDataJsonConverter.write(((HeartbeatRequest) request).data(), request.version());
            case INCREMENTAL_ALTER_CONFIGS ->
                IncrementalAlterConfigsRequestDataJsonConverter.write(((IncrementalAlterConfigsRequest) request).data(), request.version());
            case INITIALIZE_SHARE_GROUP_STATE ->
                InitializeShareGroupStateRequestDataJsonConverter.write(((InitializeShareGroupStateRequest) request).data(), request.version());
            case INIT_PRODUCER_ID ->
                InitProducerIdRequestDataJsonConverter.write(((InitProducerIdRequest) request).data(), request.version());
            case JOIN_GROUP ->
                JoinGroupRequestDataJsonConverter.write(((JoinGroupRequest) request).data(), request.version());
            case LEAVE_GROUP ->
                LeaveGroupRequestDataJsonConverter.write(((LeaveGroupRequest) request).data(), request.version());
            case LIST_CONFIG_RESOURCES ->
                ListConfigResourcesRequestDataJsonConverter.write(((ListConfigResourcesRequest) request).data(), request.version());
            case LIST_GROUPS ->
                ListGroupsRequestDataJsonConverter.write(((ListGroupsRequest) request).data(), request.version());
            case LIST_OFFSETS ->
                ListOffsetsRequestDataJsonConverter.write(((ListOffsetsRequest) request).data(), request.version());
            case LIST_PARTITION_REASSIGNMENTS ->
                ListPartitionReassignmentsRequestDataJsonConverter.write(((ListPartitionReassignmentsRequest) request).data(), request.version());
            case LIST_TRANSACTIONS ->
                ListTransactionsRequestDataJsonConverter.write(((ListTransactionsRequest) request).data(), request.version());
            case METADATA ->
                MetadataRequestDataJsonConverter.write(((MetadataRequest) request).data(), request.version());
            case OFFSET_COMMIT ->
                OffsetCommitRequestDataJsonConverter.write(((OffsetCommitRequest) request).data(), request.version());
            case OFFSET_DELETE ->
                OffsetDeleteRequestDataJsonConverter.write(((OffsetDeleteRequest) request).data(), request.version());
            case OFFSET_FETCH ->
                OffsetFetchRequestDataJsonConverter.write(((OffsetFetchRequest) request).data(), request.version());
            case OFFSET_FOR_LEADER_EPOCH ->
                OffsetForLeaderEpochRequestDataJsonConverter.write(((OffsetsForLeaderEpochRequest) request).data(), request.version());
            case PRODUCE ->
                ProduceRequestDataJsonConverter.write(((ProduceRequest) request).data(), request.version(), false);
            case PUSH_TELEMETRY ->
                PushTelemetryRequestDataJsonConverter.write(((PushTelemetryRequest) request).data(), request.version());
            case READ_SHARE_GROUP_STATE ->
                ReadShareGroupStateRequestDataJsonConverter.write(((ReadShareGroupStateRequest) request).data(), request.version());
            case READ_SHARE_GROUP_STATE_SUMMARY ->
                ReadShareGroupStateSummaryRequestDataJsonConverter.write(((ReadShareGroupStateSummaryRequest) request).data(), request.version());
            case REMOVE_RAFT_VOTER ->
                RemoveRaftVoterRequestDataJsonConverter.write(((RemoveRaftVoterRequest) request).data(), request.version());
            case RENEW_DELEGATION_TOKEN ->
                RenewDelegationTokenRequestDataJsonConverter.write(((RenewDelegationTokenRequest) request).data(), request.version());
            case SASL_AUTHENTICATE ->
                SaslAuthenticateRequestDataJsonConverter.write(((SaslAuthenticateRequest) request).data(), request.version());
            case SASL_HANDSHAKE ->
                SaslHandshakeRequestDataJsonConverter.write(((SaslHandshakeRequest) request).data(), request.version());
            case SHARE_ACKNOWLEDGE ->
                ShareAcknowledgeRequestDataJsonConverter.write(((ShareAcknowledgeRequest) request).data(), request.version());
            case SHARE_FETCH ->
                ShareFetchRequestDataJsonConverter.write(((ShareFetchRequest) request).data(), request.version());
            case SHARE_GROUP_DESCRIBE ->
                ShareGroupDescribeRequestDataJsonConverter.write(((ShareGroupDescribeRequest) request).data(), request.version());
            case SHARE_GROUP_HEARTBEAT ->
                ShareGroupHeartbeatRequestDataJsonConverter.write(((ShareGroupHeartbeatRequest) request).data(), request.version());
            case STREAMS_GROUP_DESCRIBE ->
                StreamsGroupDescribeRequestDataJsonConverter.write(((StreamsGroupDescribeRequest) request).data(), request.version());
            case STREAMS_GROUP_HEARTBEAT ->
                StreamsGroupHeartbeatRequestDataJsonConverter.write(((StreamsGroupHeartbeatRequest) request).data(), request.version());
            case SYNC_GROUP ->
                SyncGroupRequestDataJsonConverter.write(((SyncGroupRequest) request).data(), request.version());
            case TXN_OFFSET_COMMIT ->
                TxnOffsetCommitRequestDataJsonConverter.write(((TxnOffsetCommitRequest) request).data(), request.version());
            case UNREGISTER_BROKER ->
                UnregisterBrokerRequestDataJsonConverter.write(((UnregisterBrokerRequest) request).data(), request.version());
            case UPDATE_FEATURES ->
                UpdateFeaturesRequestDataJsonConverter.write(((UpdateFeaturesRequest) request).data(), request.version());
            case UPDATE_RAFT_VOTER ->
                UpdateRaftVoterRequestDataJsonConverter.write(((UpdateRaftVoterRequest) request).data(), request.version());
            case VOTE -> VoteRequestDataJsonConverter.write(((VoteRequest) request).data(), request.version());
            case WRITE_SHARE_GROUP_STATE ->
                WriteShareGroupStateRequestDataJsonConverter.write(((WriteShareGroupStateRequest) request).data(), request.version());
            case WRITE_TXN_MARKERS ->
                WriteTxnMarkersRequestDataJsonConverter.write(((WriteTxnMarkersRequest) request).data(), request.version());
            default ->
                throw new IllegalStateException("ApiKey " + request.apiKey() + " is not currently handled in `request`, the " +
                    "code should be updated to do so.");
        };
    }

    public static JsonNode response(AbstractResponse response, short version) {
        return switch (response.apiKey()) {
            case ADD_OFFSETS_TO_TXN ->
                AddOffsetsToTxnResponseDataJsonConverter.write(((AddOffsetsToTxnResponse) response).data(), version);
            case ADD_PARTITIONS_TO_TXN ->
                AddPartitionsToTxnResponseDataJsonConverter.write(((AddPartitionsToTxnResponse) response).data(), version);
            case ADD_RAFT_VOTER ->
                AddRaftVoterResponseDataJsonConverter.write(((AddRaftVoterResponse) response).data(), version);
            case ALLOCATE_PRODUCER_IDS ->
                AllocateProducerIdsResponseDataJsonConverter.write(((AllocateProducerIdsResponse) response).data(), version);
            case ALTER_CLIENT_QUOTAS ->
                AlterClientQuotasResponseDataJsonConverter.write(((AlterClientQuotasResponse) response).data(), version);
            case ALTER_CONFIGS ->
                AlterConfigsResponseDataJsonConverter.write(((AlterConfigsResponse) response).data(), version);
            case ALTER_PARTITION_REASSIGNMENTS ->
                AlterPartitionReassignmentsResponseDataJsonConverter.write(((AlterPartitionReassignmentsResponse) response).data(), version);
            case ALTER_PARTITION ->
                AlterPartitionResponseDataJsonConverter.write(((AlterPartitionResponse) response).data(), version);
            case ALTER_REPLICA_LOG_DIRS ->
                AlterReplicaLogDirsResponseDataJsonConverter.write(((AlterReplicaLogDirsResponse) response).data(), version);
            case ALTER_SHARE_GROUP_OFFSETS ->
                AlterShareGroupOffsetsResponseDataJsonConverter.write(((AlterShareGroupOffsetsResponse) response).data(), version);
            case ALTER_USER_SCRAM_CREDENTIALS ->
                AlterUserScramCredentialsResponseDataJsonConverter.write(((AlterUserScramCredentialsResponse) response).data(), version);
            case API_VERSIONS ->
                ApiVersionsResponseDataJsonConverter.write(((ApiVersionsResponse) response).data(), version);
            case ASSIGN_REPLICAS_TO_DIRS ->
                AssignReplicasToDirsResponseDataJsonConverter.write(((AssignReplicasToDirsResponse) response).data(), version);
            case BEGIN_QUORUM_EPOCH ->
                BeginQuorumEpochResponseDataJsonConverter.write(((BeginQuorumEpochResponse) response).data(), version);
            case BROKER_HEARTBEAT ->
                BrokerHeartbeatResponseDataJsonConverter.write(((BrokerHeartbeatResponse) response).data(), version);
            case BROKER_REGISTRATION ->
                BrokerRegistrationResponseDataJsonConverter.write(((BrokerRegistrationResponse) response).data(), version);
            case CONSUMER_GROUP_DESCRIBE ->
                ConsumerGroupDescribeResponseDataJsonConverter.write(((ConsumerGroupDescribeResponse) response).data(), version);
            case CONSUMER_GROUP_HEARTBEAT ->
                ConsumerGroupHeartbeatResponseDataJsonConverter.write(((ConsumerGroupHeartbeatResponse) response).data(), version);
            case CONTROLLER_REGISTRATION ->
                ControllerRegistrationResponseDataJsonConverter.write(((ControllerRegistrationResponse) response).data(), version);
            case CREATE_ACLS ->
                CreateAclsResponseDataJsonConverter.write(((CreateAclsResponse) response).data(), version);
            case CREATE_DELEGATION_TOKEN ->
                CreateDelegationTokenResponseDataJsonConverter.write(((CreateDelegationTokenResponse) response).data(), version);
            case CREATE_PARTITIONS ->
                CreatePartitionsResponseDataJsonConverter.write(((CreatePartitionsResponse) response).data(), version);
            case CREATE_TOPICS ->
                CreateTopicsResponseDataJsonConverter.write(((CreateTopicsResponse) response).data(), version);
            case DELETE_ACLS ->
                DeleteAclsResponseDataJsonConverter.write(((DeleteAclsResponse) response).data(), version);
            case DELETE_GROUPS ->
                DeleteGroupsResponseDataJsonConverter.write(((DeleteGroupsResponse) response).data(), version);
            case DELETE_RECORDS ->
                DeleteRecordsResponseDataJsonConverter.write(((DeleteRecordsResponse) response).data(), version);
            case DELETE_SHARE_GROUP_OFFSETS ->
                DeleteShareGroupOffsetsResponseDataJsonConverter.write(((DeleteShareGroupOffsetsResponse) response).data(), version);
            case DELETE_SHARE_GROUP_STATE ->
                DeleteShareGroupStateResponseDataJsonConverter.write(((DeleteShareGroupStateResponse) response).data(), version);
            case DELETE_TOPICS ->
                DeleteTopicsResponseDataJsonConverter.write(((DeleteTopicsResponse) response).data(), version);
            case DESCRIBE_ACLS ->
                DescribeAclsResponseDataJsonConverter.write(((DescribeAclsResponse) response).data(), version);
            case DESCRIBE_CLIENT_QUOTAS ->
                DescribeClientQuotasResponseDataJsonConverter.write(((DescribeClientQuotasResponse) response).data(), version);
            case DESCRIBE_CLUSTER ->
                DescribeClusterResponseDataJsonConverter.write(((DescribeClusterResponse) response).data(), version);
            case DESCRIBE_CONFIGS ->
                DescribeConfigsResponseDataJsonConverter.write(((DescribeConfigsResponse) response).data(), version);
            case DESCRIBE_DELEGATION_TOKEN ->
                DescribeDelegationTokenResponseDataJsonConverter.write(((DescribeDelegationTokenResponse) response).data(), version);
            case DESCRIBE_GROUPS ->
                DescribeGroupsResponseDataJsonConverter.write(((DescribeGroupsResponse) response).data(), version);
            case DESCRIBE_LOG_DIRS ->
                DescribeLogDirsResponseDataJsonConverter.write(((DescribeLogDirsResponse) response).data(), version);
            case DESCRIBE_PRODUCERS ->
                DescribeProducersResponseDataJsonConverter.write(((DescribeProducersResponse) response).data(), version);
            case DESCRIBE_QUORUM ->
                DescribeQuorumResponseDataJsonConverter.write(((DescribeQuorumResponse) response).data(), version);
            case DESCRIBE_SHARE_GROUP_OFFSETS ->
                DescribeShareGroupOffsetsResponseDataJsonConverter.write(((DescribeShareGroupOffsetsResponse) response).data(), version);
            case DESCRIBE_TOPIC_PARTITIONS ->
                DescribeTopicPartitionsResponseDataJsonConverter.write(((DescribeTopicPartitionsResponse) response).data(), version);
            case DESCRIBE_TRANSACTIONS ->
                DescribeTransactionsResponseDataJsonConverter.write(((DescribeTransactionsResponse) response).data(), version);
            case DESCRIBE_USER_SCRAM_CREDENTIALS ->
                DescribeUserScramCredentialsResponseDataJsonConverter.write(((DescribeUserScramCredentialsResponse) response).data(), version);
            case ELECT_LEADERS ->
                ElectLeadersResponseDataJsonConverter.write(((ElectLeadersResponse) response).data(), version);
            case END_QUORUM_EPOCH ->
                EndQuorumEpochResponseDataJsonConverter.write(((EndQuorumEpochResponse) response).data(), version);
            case END_TXN -> EndTxnResponseDataJsonConverter.write(((EndTxnResponse) response).data(), version);
            case ENVELOPE -> EnvelopeResponseDataJsonConverter.write(((EnvelopeResponse) response).data(), version);
            case EXPIRE_DELEGATION_TOKEN ->
                ExpireDelegationTokenResponseDataJsonConverter.write(((ExpireDelegationTokenResponse) response).data(), version);
            case FETCH -> FetchResponseDataJsonConverter.write(((FetchResponse) response).data(), version, false);
            case FETCH_SNAPSHOT ->
                FetchSnapshotResponseDataJsonConverter.write(((FetchSnapshotResponse) response).data(), version);
            case FIND_COORDINATOR ->
                FindCoordinatorResponseDataJsonConverter.write(((FindCoordinatorResponse) response).data(), version);
            case GET_TELEMETRY_SUBSCRIPTIONS ->
                GetTelemetrySubscriptionsResponseDataJsonConverter.write(((GetTelemetrySubscriptionsResponse) response).data(), version);
            case HEARTBEAT -> HeartbeatResponseDataJsonConverter.write(((HeartbeatResponse) response).data(), version);
            case INCREMENTAL_ALTER_CONFIGS ->
                IncrementalAlterConfigsResponseDataJsonConverter.write(((IncrementalAlterConfigsResponse) response).data(), version);
            case INITIALIZE_SHARE_GROUP_STATE ->
                InitializeShareGroupStateResponseDataJsonConverter.write(((InitializeShareGroupStateResponse) response).data(), version);
            case INIT_PRODUCER_ID ->
                InitProducerIdResponseDataJsonConverter.write(((InitProducerIdResponse) response).data(), version);
            case JOIN_GROUP -> JoinGroupResponseDataJsonConverter.write(((JoinGroupResponse) response).data(), version);
            case LEAVE_GROUP ->
                LeaveGroupResponseDataJsonConverter.write(((LeaveGroupResponse) response).data(), version);
            case LIST_CONFIG_RESOURCES ->
                ListConfigResourcesResponseDataJsonConverter.write(((ListConfigResourcesResponse) response).data(), version);
            case LIST_GROUPS ->
                ListGroupsResponseDataJsonConverter.write(((ListGroupsResponse) response).data(), version);
            case LIST_OFFSETS ->
                ListOffsetsResponseDataJsonConverter.write(((ListOffsetsResponse) response).data(), version);
            case LIST_PARTITION_REASSIGNMENTS ->
                ListPartitionReassignmentsResponseDataJsonConverter.write(((ListPartitionReassignmentsResponse) response).data(), version);
            case LIST_TRANSACTIONS ->
                ListTransactionsResponseDataJsonConverter.write(((ListTransactionsResponse) response).data(), version);
            case METADATA -> MetadataResponseDataJsonConverter.write(((MetadataResponse) response).data(), version);
            case OFFSET_COMMIT ->
                OffsetCommitResponseDataJsonConverter.write(((OffsetCommitResponse) response).data(), version);
            case OFFSET_DELETE ->
                OffsetDeleteResponseDataJsonConverter.write(((OffsetDeleteResponse) response).data(), version);
            case OFFSET_FETCH ->
                OffsetFetchResponseDataJsonConverter.write(((OffsetFetchResponse) response).data(), version);
            case OFFSET_FOR_LEADER_EPOCH ->
                OffsetForLeaderEpochResponseDataJsonConverter.write(((OffsetsForLeaderEpochResponse) response).data(), version);
            case PRODUCE -> ProduceResponseDataJsonConverter.write(((ProduceResponse) response).data(), version);
            case PUSH_TELEMETRY ->
                PushTelemetryResponseDataJsonConverter.write(((PushTelemetryResponse) response).data(), version);
            case READ_SHARE_GROUP_STATE ->
                ReadShareGroupStateResponseDataJsonConverter.write(((ReadShareGroupStateResponse) response).data(), version);
            case READ_SHARE_GROUP_STATE_SUMMARY ->
                ReadShareGroupStateSummaryResponseDataJsonConverter.write(((ReadShareGroupStateSummaryResponse) response).data(), version);
            case REMOVE_RAFT_VOTER ->
                RemoveRaftVoterResponseDataJsonConverter.write(((RemoveRaftVoterResponse) response).data(), version);
            case RENEW_DELEGATION_TOKEN ->
                RenewDelegationTokenResponseDataJsonConverter.write(((RenewDelegationTokenResponse) response).data(), version);
            case SASL_AUTHENTICATE ->
                SaslAuthenticateResponseDataJsonConverter.write(((SaslAuthenticateResponse) response).data(), version);
            case SASL_HANDSHAKE ->
                SaslHandshakeResponseDataJsonConverter.write(((SaslHandshakeResponse) response).data(), version);
            case SHARE_ACKNOWLEDGE ->
                ShareAcknowledgeResponseDataJsonConverter.write(((ShareAcknowledgeResponse) response).data(), version);
            case SHARE_FETCH ->
                ShareFetchResponseDataJsonConverter.write(((ShareFetchResponse) response).data(), version);
            case SHARE_GROUP_DESCRIBE ->
                ShareGroupDescribeResponseDataJsonConverter.write(((ShareGroupDescribeResponse) response).data(), version);
            case SHARE_GROUP_HEARTBEAT ->
                ShareGroupHeartbeatResponseDataJsonConverter.write(((ShareGroupHeartbeatResponse) response).data(), version);
            case STREAMS_GROUP_DESCRIBE ->
                StreamsGroupDescribeResponseDataJsonConverter.write(((StreamsGroupDescribeResponse) response).data(), version);
            case STREAMS_GROUP_HEARTBEAT ->
                StreamsGroupHeartbeatResponseDataJsonConverter.write(((StreamsGroupHeartbeatResponse) response).data(), version);
            case SYNC_GROUP -> SyncGroupResponseDataJsonConverter.write(((SyncGroupResponse) response).data(), version);
            case TXN_OFFSET_COMMIT ->
                TxnOffsetCommitResponseDataJsonConverter.write(((TxnOffsetCommitResponse) response).data(), version);
            case UNREGISTER_BROKER ->
                UnregisterBrokerResponseDataJsonConverter.write(((UnregisterBrokerResponse) response).data(), version);
            case UPDATE_FEATURES ->
                UpdateFeaturesResponseDataJsonConverter.write(((UpdateFeaturesResponse) response).data(), version);
            case UPDATE_RAFT_VOTER ->
                UpdateRaftVoterResponseDataJsonConverter.write(((UpdateRaftVoterResponse) response).data(), version);
            case VOTE -> VoteResponseDataJsonConverter.write(((VoteResponse) response).data(), version);
            case WRITE_SHARE_GROUP_STATE ->
                WriteShareGroupStateResponseDataJsonConverter.write(((WriteShareGroupStateResponse) response).data(), version);
            case WRITE_TXN_MARKERS ->
                WriteTxnMarkersResponseDataJsonConverter.write(((WriteTxnMarkersResponse) response).data(), version);
            default ->
                throw new IllegalStateException("ApiKey " + response.apiKey() + " is not currently handled in `response`, the " +
                    "code should be updated to do so.");
        };
    }

    public static JsonNode requestHeaderNode(RequestHeader header) {
        ObjectNode node = (ObjectNode) RequestHeaderDataJsonConverter.write(
            header.data(), header.headerVersion(), false
        );
        node.set("requestApiKeyName", new TextNode(header.apiKey().toString()));
        if (header.isApiVersionDeprecated()) {
            node.set("requestApiVersionDeprecated", BooleanNode.TRUE);
        }
        return node;
    }

    public static JsonNode requestDesc(RequestHeader header, Optional<JsonNode> requestNode, boolean isForwarded) {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.set("isForwarded", isForwarded ? BooleanNode.TRUE : BooleanNode.FALSE);
        node.set("requestHeader", requestHeaderNode(header));
        node.set("request", requestNode.orElse(new TextNode("")));
        return node;
    }

    public static JsonNode clientInfoNode(ClientInformation clientInfo) {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.set("softwareName", new TextNode(clientInfo.softwareName()));
        node.set("softwareVersion", new TextNode(clientInfo.softwareVersion()));
        return node;
    }

    public static JsonNode requestDescMetrics(RequestHeader header, Optional<JsonNode> requestNode, Optional<JsonNode> responseNode,
                                              RequestContext context, Session session, boolean isForwarded,
                                              double totalTimeMs, double requestQueueTimeMs, double apiLocalTimeMs,
                                              double apiRemoteTimeMs, long apiThrottleTimeMs, double responseQueueTimeMs,
                                              double responseSendTimeMs, long temporaryMemoryBytes,
                                              double messageConversionsTimeMs) {
        ObjectNode node = (ObjectNode) requestDesc(header, requestNode, isForwarded);
        node.set("response", responseNode.orElse(new TextNode("")));
        node.set("connection", new TextNode(context.connectionId));
        node.set("totalTimeMs", new DoubleNode(totalTimeMs));
        node.set("requestQueueTimeMs", new DoubleNode(requestQueueTimeMs));
        node.set("localTimeMs", new DoubleNode(apiLocalTimeMs));
        node.set("remoteTimeMs", new DoubleNode(apiRemoteTimeMs));
        node.set("throttleTimeMs", new LongNode(apiThrottleTimeMs));
        node.set("responseQueueTimeMs", new DoubleNode(responseQueueTimeMs));
        node.set("sendTimeMs", new DoubleNode(responseSendTimeMs));
        node.set("securityProtocol", new TextNode(context.securityProtocol.toString()));
        node.set("principal", new TextNode(session.principal.toString()));
        node.set("listener", new TextNode(context.listenerName.value()));
        node.set("clientInformation", clientInfoNode(context.clientInformation));
        if (temporaryMemoryBytes > 0) {
            node.set("temporaryMemoryBytes", new LongNode(temporaryMemoryBytes));
        }
        if (messageConversionsTimeMs > 0) {
            node.set("messageConversionsTime", new DoubleNode(messageConversionsTimeMs));
        }
        return node;
    }
}
