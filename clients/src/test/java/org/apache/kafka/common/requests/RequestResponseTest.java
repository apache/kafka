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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.NotCoordinatorException;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.AddOffsetsToTxnRequestData;
import org.apache.kafka.common.message.AddOffsetsToTxnResponseData;
import org.apache.kafka.common.message.AllocateProducerIdsRequestData;
import org.apache.kafka.common.message.AllocateProducerIdsResponseData;
import org.apache.kafka.common.message.AlterClientQuotasResponseData;
import org.apache.kafka.common.message.AlterConfigsResponseData;
import org.apache.kafka.common.message.AlterPartitionRequestData;
import org.apache.kafka.common.message.AlterPartitionResponseData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData;
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData;
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData.AlterReplicaLogDirTopic;
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData.AlterReplicaLogDirTopicCollection;
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData;
import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData;
import org.apache.kafka.common.message.AlterUserScramCredentialsResponseData;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionCollection;
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.BrokerHeartbeatResponseData;
import org.apache.kafka.common.message.BrokerRegistrationRequestData;
import org.apache.kafka.common.message.BrokerRegistrationResponseData;
import org.apache.kafka.common.message.ControlledShutdownRequestData;
import org.apache.kafka.common.message.ControlledShutdownResponseData;
import org.apache.kafka.common.message.ControlledShutdownResponseData.RemainingPartition;
import org.apache.kafka.common.message.ControlledShutdownResponseData.RemainingPartitionCollection;
import org.apache.kafka.common.message.CreateAclsRequestData;
import org.apache.kafka.common.message.CreateAclsResponseData;
import org.apache.kafka.common.message.CreateDelegationTokenRequestData;
import org.apache.kafka.common.message.CreateDelegationTokenRequestData.CreatableRenewers;
import org.apache.kafka.common.message.CreateDelegationTokenResponseData;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsAssignment;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopicCollection;
import org.apache.kafka.common.message.CreatePartitionsResponseData;
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableReplicaAssignment;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicCollection;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreateableTopicConfig;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicConfigs;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.message.DeleteAclsRequestData;
import org.apache.kafka.common.message.DeleteAclsResponseData;
import org.apache.kafka.common.message.DeleteGroupsRequestData;
import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.message.DeleteGroupsResponseData.DeletableGroupResult;
import org.apache.kafka.common.message.DeleteGroupsResponseData.DeletableGroupResultCollection;
import org.apache.kafka.common.message.DeleteRecordsRequestData;
import org.apache.kafka.common.message.DeleteRecordsResponseData;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.DeleteTopicsResponseData.DeletableTopicResult;
import org.apache.kafka.common.message.DescribeAclsResponseData;
import org.apache.kafka.common.message.DescribeAclsResponseData.AclDescription;
import org.apache.kafka.common.message.DescribeAclsResponseData.DescribeAclsResource;
import org.apache.kafka.common.message.DescribeClientQuotasResponseData;
import org.apache.kafka.common.message.DescribeClusterRequestData;
import org.apache.kafka.common.message.DescribeClusterResponseData;
import org.apache.kafka.common.message.DescribeClusterResponseData.DescribeClusterBroker;
import org.apache.kafka.common.message.DescribeClusterResponseData.DescribeClusterBrokerCollection;
import org.apache.kafka.common.message.DescribeConfigsRequestData;
import org.apache.kafka.common.message.DescribeConfigsResponseData;
import org.apache.kafka.common.message.DescribeConfigsResponseData.DescribeConfigsResourceResult;
import org.apache.kafka.common.message.DescribeConfigsResponseData.DescribeConfigsResult;
import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroup;
import org.apache.kafka.common.message.DescribeLogDirsRequestData;
import org.apache.kafka.common.message.DescribeLogDirsResponseData;
import org.apache.kafka.common.message.DescribeProducersRequestData;
import org.apache.kafka.common.message.DescribeProducersResponseData;
import org.apache.kafka.common.message.DescribeQuorumRequestData;
import org.apache.kafka.common.message.DescribeQuorumResponseData;
import org.apache.kafka.common.message.DescribeTransactionsRequestData;
import org.apache.kafka.common.message.DescribeTransactionsResponseData;
import org.apache.kafka.common.message.DescribeUserScramCredentialsRequestData;
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseData;
import org.apache.kafka.common.message.ElectLeadersResponseData.PartitionResult;
import org.apache.kafka.common.message.ElectLeadersResponseData.ReplicaElectionResult;
import org.apache.kafka.common.message.EndQuorumEpochRequestData;
import org.apache.kafka.common.message.EndQuorumEpochResponseData;
import org.apache.kafka.common.message.EndTxnRequestData;
import org.apache.kafka.common.message.EndTxnResponseData;
import org.apache.kafka.common.message.EnvelopeResponseData;
import org.apache.kafka.common.message.ExpireDelegationTokenRequestData;
import org.apache.kafka.common.message.ExpireDelegationTokenResponseData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchSnapshotRequestData;
import org.apache.kafka.common.message.FetchSnapshotResponseData;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.AlterConfigsResource;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.AlterableConfig;
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData;
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember;
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState;
import org.apache.kafka.common.message.LeaderAndIsrResponseData;
import org.apache.kafka.common.message.LeaderAndIsrResponseData.LeaderAndIsrTopicErrorCollection;
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsTopic;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse;
import org.apache.kafka.common.message.ListPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData;
import org.apache.kafka.common.message.ListTransactionsRequestData;
import org.apache.kafka.common.message.ListTransactionsResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetDeleteRequestData;
import org.apache.kafka.common.message.OffsetDeleteRequestData.OffsetDeleteRequestPartition;
import org.apache.kafka.common.message.OffsetDeleteRequestData.OffsetDeleteRequestTopic;
import org.apache.kafka.common.message.OffsetDeleteRequestData.OffsetDeleteRequestTopicCollection;
import org.apache.kafka.common.message.OffsetDeleteResponseData;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponsePartition;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponsePartitionCollection;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponseTopic;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponseTopicCollection;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderPartition;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopic;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopicCollection;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.RenewDelegationTokenRequestData;
import org.apache.kafka.common.message.RenewDelegationTokenResponseData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaPartitionState;
import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaTopicState;
import org.apache.kafka.common.message.StopReplicaResponseData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupRequestData.SyncGroupRequestAssignment;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.message.UnregisterBrokerRequestData;
import org.apache.kafka.common.message.UnregisterBrokerResponseData;
import org.apache.kafka.common.message.UpdateFeaturesRequestData;
import org.apache.kafka.common.message.UpdateFeaturesResponseData;
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataBroker;
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataEndpoint;
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataPartitionState;
import org.apache.kafka.common.message.UpdateMetadataResponseData;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.CreateTopicsRequest.Builder;
import org.apache.kafka.common.requests.DescribeConfigsResponse.ConfigType;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.requests.FindCoordinatorRequest.NoBatchedFindCoordinatorsException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.token.delegation.DelegationToken;
import org.apache.kafka.common.security.token.delegation.TokenInformation;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.kafka.common.protocol.ApiKeys.API_VERSIONS;
import static org.apache.kafka.common.protocol.ApiKeys.CONTROLLED_SHUTDOWN;
import static org.apache.kafka.common.protocol.ApiKeys.CREATE_PARTITIONS;
import static org.apache.kafka.common.protocol.ApiKeys.CREATE_TOPICS;
import static org.apache.kafka.common.protocol.ApiKeys.DELETE_ACLS;
import static org.apache.kafka.common.protocol.ApiKeys.DELETE_TOPICS;
import static org.apache.kafka.common.protocol.ApiKeys.DESCRIBE_CONFIGS;
import static org.apache.kafka.common.protocol.ApiKeys.DESCRIBE_LOG_DIRS;
import static org.apache.kafka.common.protocol.ApiKeys.ELECT_LEADERS;
import static org.apache.kafka.common.protocol.ApiKeys.FETCH;
import static org.apache.kafka.common.protocol.ApiKeys.FIND_COORDINATOR;
import static org.apache.kafka.common.protocol.ApiKeys.JOIN_GROUP;
import static org.apache.kafka.common.protocol.ApiKeys.LIST_GROUPS;
import static org.apache.kafka.common.protocol.ApiKeys.LIST_OFFSETS;
import static org.apache.kafka.common.protocol.ApiKeys.METADATA;
import static org.apache.kafka.common.protocol.ApiKeys.OFFSET_FETCH;
import static org.apache.kafka.common.protocol.ApiKeys.PRODUCE;
import static org.apache.kafka.common.protocol.ApiKeys.STOP_REPLICA;
import static org.apache.kafka.common.protocol.ApiKeys.SYNC_GROUP;
import static org.apache.kafka.common.protocol.ApiKeys.UPDATE_METADATA;
import static org.apache.kafka.common.protocol.ApiKeys.WRITE_TXN_MARKERS;
import static org.apache.kafka.common.requests.FetchMetadata.INVALID_SESSION_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

// This class performs tests requests and responses for all API keys
public class RequestResponseTest {

    // Exception includes a message that we verify is not included in error responses
    private final UnknownServerException unknownServerException = new UnknownServerException("secret");

    @Test
    public void testSerialization() {
        Map<ApiKeys, List<Short>> toSkip = new HashMap<>();
        // It's not possible to create a MetadataRequest v0 via the builder
        toSkip.put(METADATA, singletonList((short) 0));
        // DescribeLogDirsResponse v0, v1 and v2 don't have a top level error field
        toSkip.put(DESCRIBE_LOG_DIRS, Arrays.asList((short) 0, (short) 1, (short) 2));
        // ElectLeaders v0 does not have a top level error field, when accessing it, it defaults to NONE
        toSkip.put(ELECT_LEADERS, singletonList((short) 0));

        for (ApiKeys apikey : ApiKeys.values()) {
            for (short version : apikey.allVersions()) {
                if (toSkip.containsKey(apikey) && toSkip.get(apikey).contains(version)) continue;
                AbstractRequest request = getRequest(apikey, version);
                checkRequest(request);
                checkErrorResponse(request, unknownServerException);
                checkResponse(getResponse(apikey, version), version);
            }
        }
    }

    // This test validates special cases that are not checked in testSerialization
    @Test
    public void testSerializationSpecialCases() {
        // Produce
        checkResponse(createProduceResponseWithErrorMessage(), (short) 8);
        // Fetch
        checkResponse(createFetchResponse(true), (short) 4);
        List<TopicIdPartition> toForgetTopics = new ArrayList<>();
        toForgetTopics.add(new TopicIdPartition(Uuid.ZERO_UUID, new TopicPartition("foo", 0)));
        toForgetTopics.add(new TopicIdPartition(Uuid.ZERO_UUID, new TopicPartition("foo", 2)));
        toForgetTopics.add(new TopicIdPartition(Uuid.ZERO_UUID, new TopicPartition("bar", 0)));
        checkRequest(createFetchRequest((short) 7, new FetchMetadata(123, 456), toForgetTopics));
        checkResponse(createFetchResponse(123), (short) 7);
        checkResponse(createFetchResponse(Errors.FETCH_SESSION_ID_NOT_FOUND, 123), (short) 7);
        checkOlderFetchVersions();
        // Metadata
        checkRequest(MetadataRequest.Builder.allTopics().build((short) 2));
        // OffsetFetch
        checkRequest(createOffsetFetchRequestWithMultipleGroups((short) 8, true));
        checkRequest(createOffsetFetchRequestWithMultipleGroups((short) 8, false));
        checkRequest(createOffsetFetchRequestForAllPartition((short) 7, true));
        checkRequest(createOffsetFetchRequestForAllPartition((short) 8, true));
        checkErrorResponse(createOffsetFetchRequestWithMultipleGroups((short) 8, true), unknownServerException);
        checkErrorResponse(createOffsetFetchRequestForAllPartition((short) 7, true),
            new NotCoordinatorException("Not Coordinator"));
        checkErrorResponse(createOffsetFetchRequestForAllPartition((short) 8, true),
            new NotCoordinatorException("Not Coordinator"));
        checkErrorResponse(createOffsetFetchRequestWithMultipleGroups((short) 8, true),
            new NotCoordinatorException("Not Coordinator"));
        // StopReplica
        for (short version : STOP_REPLICA.allVersions()) {
            checkRequest(createStopReplicaRequest(version, false));
            checkErrorResponse(createStopReplicaRequest(version, false), unknownServerException);
        }
        // CreatePartitions
        for (short version : CREATE_PARTITIONS.allVersions()) {
            checkRequest(createCreatePartitionsRequestWithAssignments(version));
        }
        // UpdateMetadata
        for (short version : UPDATE_METADATA.allVersions()) {
            checkRequest(createUpdateMetadataRequest(version, null));
            checkErrorResponse(createUpdateMetadataRequest(version, null), unknownServerException);
        }
        // LeaderForEpoch
        checkRequest(createLeaderEpochRequestForConsumer());
        checkErrorResponse(createLeaderEpochRequestForConsumer(), unknownServerException);
        // TxnOffsetCommit
        checkRequest(createTxnOffsetCommitRequestWithAutoDowngrade());
        checkErrorResponse(createTxnOffsetCommitRequestWithAutoDowngrade(), unknownServerException);
        // DescribeAcls
        checkErrorResponse(createDescribeAclsRequest((short) 0), new SecurityDisabledException("Security is not enabled."));
        checkErrorResponse(createCreateAclsRequest((short) 0), new SecurityDisabledException("Security is not enabled."));
        // DeleteAcls
        checkErrorResponse(createDeleteAclsRequest((short) 0), new SecurityDisabledException("Security is not enabled."));
        // DescribeConfigs
        checkRequest(createDescribeConfigsRequestWithConfigEntries((short) 0));
        checkRequest(createDescribeConfigsRequestWithConfigEntries((short) 1));
        checkRequest(createDescribeConfigsRequestWithDocumentation((short) 1));
        checkRequest(createDescribeConfigsRequestWithDocumentation((short) 2));
        checkRequest(createDescribeConfigsRequestWithDocumentation((short) 3));
        checkDescribeConfigsResponseVersions();
        // ElectLeaders
        checkRequest(createElectLeadersRequestNullPartitions());
    }

    @Test
    public void testApiVersionsSerialization() {
        for (short version : API_VERSIONS.allVersions()) {
            checkErrorResponse(createApiVersionRequest(version), new UnsupportedVersionException("Not Supported"));
            checkResponse(ApiVersionsResponse.defaultApiVersionsResponse(ApiMessageType.ListenerType.ZK_BROKER), version);
        }
    }

    @Test
    public void testBatchedFindCoordinatorRequestSerialization() {
        for (short version : FIND_COORDINATOR.allVersions()) {
            checkRequest(createBatchedFindCoordinatorRequest(singletonList("group1"), version));
            if (version < FindCoordinatorRequest.MIN_BATCHED_VERSION) {
                assertThrows(NoBatchedFindCoordinatorsException.class, () ->
                        createBatchedFindCoordinatorRequest(asList("group1", "group2"), version));
            } else {
                checkRequest(createBatchedFindCoordinatorRequest(asList("group1", "group2"), version));
            }
        }
    }

    @Test
    public void testResponseHeader() {
        ResponseHeader header = new ResponseHeader(10, (short) 1);
        ObjectSerializationCache serializationCache = new ObjectSerializationCache();
        ByteBuffer buffer = ByteBuffer.allocate(header.size(serializationCache));
        header.write(buffer, serializationCache);
        buffer.flip();
        ResponseHeader deserialized = ResponseHeader.parse(buffer, header.headerVersion());
        assertEquals(header.correlationId(), deserialized.correlationId());
    }

    @Test
    public void cannotUseFindCoordinatorV0ToFindTransactionCoordinator() {
        FindCoordinatorRequest.Builder builder = new FindCoordinatorRequest.Builder(
                new FindCoordinatorRequestData()
                    .setKeyType(CoordinatorType.TRANSACTION.id)
                    .setKey("foobar"));
        assertThrows(UnsupportedVersionException.class, () -> builder.build((short) 0));
    }

    @Test
    public void testProduceRequestPartitionSize() {
        TopicPartition tp0 = new TopicPartition("test", 0);
        TopicPartition tp1 = new TopicPartition("test", 1);
        MemoryRecords records0 = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V2,
            CompressionType.NONE, new SimpleRecord("woot".getBytes()));
        MemoryRecords records1 = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V2,
            CompressionType.NONE, new SimpleRecord("woot".getBytes()), new SimpleRecord("woot".getBytes()));
        ProduceRequest request = ProduceRequest.forMagic(RecordBatch.MAGIC_VALUE_V2,
                new ProduceRequestData()
                        .setTopicData(new ProduceRequestData.TopicProduceDataCollection(asList(
                                new ProduceRequestData.TopicProduceData().setName(tp0.topic()).setPartitionData(
                                        singletonList(new ProduceRequestData.PartitionProduceData().setIndex(tp0.partition()).setRecords(records0))),
                                new ProduceRequestData.TopicProduceData().setName(tp1.topic()).setPartitionData(
                                        singletonList(new ProduceRequestData.PartitionProduceData().setIndex(tp1.partition()).setRecords(records1))))
                                .iterator()))
                        .setAcks((short) 1)
                        .setTimeoutMs(5000)
                        .setTransactionalId("transactionalId"))
            .build((short) 3);
        assertEquals(2, request.partitionSizes().size());
        assertEquals(records0.sizeInBytes(), (int) request.partitionSizes().get(tp0));
        assertEquals(records1.sizeInBytes(), (int) request.partitionSizes().get(tp1));
    }

    @Test
    public void produceRequestToStringTest() {
        ProduceRequest request = createProduceRequest(PRODUCE.latestVersion());
        assertEquals(1, request.data().topicData().size());
        assertFalse(request.toString(false).contains("partitionSizes"));
        assertTrue(request.toString(false).contains("numPartitions=1"));
        assertTrue(request.toString(true).contains("partitionSizes"));
        assertFalse(request.toString(true).contains("numPartitions"));

        request.clearPartitionRecords();
        try {
            request.data();
            fail("dataOrException should fail after clearPartitionRecords()");
        } catch (IllegalStateException e) {
            // OK
        }

        // `toString` should behave the same after `clearPartitionRecords`
        assertFalse(request.toString(false).contains("partitionSizes"));
        assertTrue(request.toString(false).contains("numPartitions=1"));
        assertTrue(request.toString(true).contains("partitionSizes"));
        assertFalse(request.toString(true).contains("numPartitions"));
    }

    @Test
    public void produceRequestGetErrorResponseTest() {
        ProduceRequest request = createProduceRequest(PRODUCE.latestVersion());

        ProduceResponse errorResponse = (ProduceResponse) request.getErrorResponse(new NotEnoughReplicasException());
        ProduceResponseData.TopicProduceResponse topicProduceResponse = errorResponse.data().responses().iterator().next();
        ProduceResponseData.PartitionProduceResponse partitionProduceResponse = topicProduceResponse.partitionResponses().iterator().next();

        assertEquals(Errors.NOT_ENOUGH_REPLICAS, Errors.forCode(partitionProduceResponse.errorCode()));
        assertEquals(ProduceResponse.INVALID_OFFSET, partitionProduceResponse.baseOffset());
        assertEquals(RecordBatch.NO_TIMESTAMP, partitionProduceResponse.logAppendTimeMs());

        request.clearPartitionRecords();

        // `getErrorResponse` should behave the same after `clearPartitionRecords`
        errorResponse = (ProduceResponse) request.getErrorResponse(new NotEnoughReplicasException());
        topicProduceResponse = errorResponse.data().responses().iterator().next();
        partitionProduceResponse = topicProduceResponse.partitionResponses().iterator().next();

        assertEquals(Errors.NOT_ENOUGH_REPLICAS, Errors.forCode(partitionProduceResponse.errorCode()));
        assertEquals(ProduceResponse.INVALID_OFFSET, partitionProduceResponse.baseOffset());
        assertEquals(RecordBatch.NO_TIMESTAMP, partitionProduceResponse.logAppendTimeMs());
    }

    @Test
    public void fetchResponseVersionTest() {
        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> responseData = new LinkedHashMap<>();
        Uuid id = Uuid.randomUuid();
        Map<Uuid, String> topicNames = Collections.singletonMap(id, "test");
        TopicPartition tp = new TopicPartition("test", 0);

        MemoryRecords records = MemoryRecords.readableRecords(ByteBuffer.allocate(10));
        FetchResponseData.PartitionData partitionData = new FetchResponseData.PartitionData()
                .setPartitionIndex(0)
                .setHighWatermark(1000000)
                .setLogStartOffset(-1)
                .setRecords(records);

        // Use zero UUID since we are comparing with old request versions
        responseData.put(new TopicIdPartition(Uuid.ZERO_UUID, tp), partitionData);

        LinkedHashMap<TopicPartition, FetchResponseData.PartitionData> tpResponseData = new LinkedHashMap<>();
        tpResponseData.put(tp, partitionData);

        FetchResponse v0Response = FetchResponse.of(Errors.NONE, 0, INVALID_SESSION_ID, responseData);
        FetchResponse v1Response = FetchResponse.of(Errors.NONE, 10, INVALID_SESSION_ID, responseData);
        FetchResponse v0Deserialized = FetchResponse.parse(v0Response.serialize((short) 0), (short) 0);
        FetchResponse v1Deserialized = FetchResponse.parse(v1Response.serialize((short) 1), (short) 1);
        assertEquals(0, v0Deserialized.throttleTimeMs(), "Throttle time must be zero");
        assertEquals(10, v1Deserialized.throttleTimeMs(), "Throttle time must be 10");
        assertEquals(tpResponseData, v0Deserialized.responseData(topicNames, (short) 0), "Response data does not match");
        assertEquals(tpResponseData, v1Deserialized.responseData(topicNames, (short) 1), "Response data does not match");

        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> idResponseData = new LinkedHashMap<>();
        idResponseData.put(new TopicIdPartition(id, new TopicPartition("test", 0)),
                new FetchResponseData.PartitionData()
                        .setPartitionIndex(0)
                        .setHighWatermark(1000000)
                        .setLogStartOffset(-1)
                        .setRecords(records));
        FetchResponse idTestResponse = FetchResponse.of(Errors.NONE, 0, INVALID_SESSION_ID, idResponseData);
        FetchResponse v12Deserialized = FetchResponse.parse(idTestResponse.serialize((short) 12), (short) 12);
        FetchResponse newestDeserialized = FetchResponse.parse(idTestResponse.serialize(FETCH.latestVersion()), FETCH.latestVersion());
        assertTrue(v12Deserialized.topicIds().isEmpty());
        assertEquals(1, newestDeserialized.topicIds().size());
        assertTrue(newestDeserialized.topicIds().contains(id));
    }

    @Test
    public void testFetchResponseV4() {
        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> responseData = new LinkedHashMap<>();
        Map<Uuid, String> topicNames = new HashMap<>();
        topicNames.put(Uuid.randomUuid(), "bar");
        topicNames.put(Uuid.randomUuid(), "foo");
        MemoryRecords records = MemoryRecords.readableRecords(ByteBuffer.allocate(10));

        List<FetchResponseData.AbortedTransaction> abortedTransactions = asList(
                new FetchResponseData.AbortedTransaction().setProducerId(10).setFirstOffset(100),
                new FetchResponseData.AbortedTransaction().setProducerId(15).setFirstOffset(50)
        );

        // Use zero UUID since this is an old request version.
        responseData.put(new TopicIdPartition(Uuid.ZERO_UUID, new TopicPartition("bar", 0)),
                new FetchResponseData.PartitionData()
                        .setPartitionIndex(0)
                        .setHighWatermark(1000000)
                        .setAbortedTransactions(abortedTransactions)
                        .setRecords(records));
        responseData.put(new TopicIdPartition(Uuid.ZERO_UUID, new TopicPartition("bar", 1)),
                new FetchResponseData.PartitionData()
                        .setPartitionIndex(1)
                        .setHighWatermark(900000)
                        .setLastStableOffset(5)
                        .setRecords(records));
        responseData.put(new TopicIdPartition(Uuid.ZERO_UUID, new TopicPartition("foo", 0)),
                new FetchResponseData.PartitionData()
                        .setPartitionIndex(0)
                        .setHighWatermark(70000)
                        .setLastStableOffset(6)
                        .setRecords(records));

        FetchResponse response = FetchResponse.of(Errors.NONE, 10, INVALID_SESSION_ID, responseData);
        FetchResponse deserialized = FetchResponse.parse(response.serialize((short) 4), (short) 4);
        assertEquals(responseData.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().topicPartition(), Map.Entry::getValue)),
                deserialized.responseData(topicNames, (short) 4));
    }

    @Test
    public void verifyFetchResponseFullWrites() throws Exception {
        verifyFetchResponseFullWrite(FETCH.latestVersion(), createFetchResponse(123));
        verifyFetchResponseFullWrite(FETCH.latestVersion(),
            createFetchResponse(Errors.FETCH_SESSION_ID_NOT_FOUND, 123));
        for (short version : FETCH.allVersions()) {
            verifyFetchResponseFullWrite(version, createFetchResponse(version >= 4));
        }
    }

    private void verifyFetchResponseFullWrite(short version, FetchResponse fetchResponse) throws Exception {
        int correlationId = 15;

        short responseHeaderVersion = FETCH.responseHeaderVersion(version);
        Send send = fetchResponse.toSend(new ResponseHeader(correlationId, responseHeaderVersion), version);
        ByteBufferChannel channel = new ByteBufferChannel(send.size());
        send.writeTo(channel);
        channel.close();

        ByteBuffer buf = channel.buffer();

        // read the size
        int size = buf.getInt();
        assertTrue(size > 0);

        // read the header
        ResponseHeader responseHeader = ResponseHeader.parse(channel.buffer(), responseHeaderVersion);
        assertEquals(correlationId, responseHeader.correlationId());

        assertEquals(fetchResponse.serialize(version), buf);
        FetchResponseData deserialized = new FetchResponseData(new ByteBufferAccessor(buf), version);
        ObjectSerializationCache serializationCache = new ObjectSerializationCache();
        assertEquals(size, responseHeader.size(serializationCache) + deserialized.size(serializationCache, version));
    }

    @Test
    public void testCreateTopicRequestV0FailsIfValidateOnly() {
        assertThrows(UnsupportedVersionException.class,
            () -> createCreateTopicRequest((short) 0, true));
    }

    @Test
    public void testCreateTopicRequestV3FailsIfNoPartitionsOrReplicas() {
        final UnsupportedVersionException exception = assertThrows(
            UnsupportedVersionException.class, () -> {
                CreateTopicsRequestData data = new CreateTopicsRequestData()
                    .setTimeoutMs(123)
                    .setValidateOnly(false);
                data.topics().add(new CreatableTopic().
                    setName("foo").
                    setNumPartitions(CreateTopicsRequest.NO_NUM_PARTITIONS).
                    setReplicationFactor((short) 1));
                data.topics().add(new CreatableTopic().
                    setName("bar").
                    setNumPartitions(1).
                    setReplicationFactor(CreateTopicsRequest.NO_REPLICATION_FACTOR));

                new Builder(data).build((short) 3);
            });
        assertTrue(exception.getMessage().contains("supported in CreateTopicRequest version 4+"));
        assertTrue(exception.getMessage().contains("[foo, bar]"));
    }

    @Test
    public void testFetchRequestMaxBytesOldVersions() {
        final short version = 1;
        FetchRequest fr = createFetchRequest(version);
        FetchRequest fr2 = FetchRequest.parse(fr.serialize(), version);
        assertEquals(fr2.maxBytes(), fr.maxBytes());
    }

    @Test
    public void testFetchRequestIsolationLevel() {
        FetchRequest request = createFetchRequest((short) 4, IsolationLevel.READ_COMMITTED);
        FetchRequest deserialized = (FetchRequest) AbstractRequest.parseRequest(request.apiKey(), request.version(),
                request.serialize()).request;
        assertEquals(request.isolationLevel(), deserialized.isolationLevel());

        request = createFetchRequest((short) 4, IsolationLevel.READ_UNCOMMITTED);
        deserialized = (FetchRequest) AbstractRequest.parseRequest(request.apiKey(), request.version(),
                request.serialize()).request;
        assertEquals(request.isolationLevel(), deserialized.isolationLevel());
    }

    @Test
    public void testFetchRequestWithMetadata() {
        FetchRequest request = createFetchRequest((short) 4, IsolationLevel.READ_COMMITTED);
        FetchRequest deserialized = (FetchRequest) AbstractRequest.parseRequest(FETCH, request.version(),
                request.serialize()).request;
        assertEquals(request.isolationLevel(), deserialized.isolationLevel());

        request = createFetchRequest((short) 4, IsolationLevel.READ_UNCOMMITTED);
        deserialized = (FetchRequest) AbstractRequest.parseRequest(FETCH, request.version(),
                request.serialize()).request;
        assertEquals(request.isolationLevel(), deserialized.isolationLevel());
    }

    @Test
    public void testFetchRequestCompat() {
        Map<TopicPartition, FetchRequest.PartitionData> fetchData = new HashMap<>();
        fetchData.put(new TopicPartition("test", 0), new FetchRequest.PartitionData(Uuid.ZERO_UUID, 100, 2, 100, Optional.of(42)));
        FetchRequest req = FetchRequest.Builder
                .forConsumer((short) 2, 100, 100, fetchData)
                .metadata(new FetchMetadata(10, 20))
                .isolationLevel(IsolationLevel.READ_COMMITTED)
                .build((short) 2);

        FetchRequestData data = req.data();
        ObjectSerializationCache cache = new ObjectSerializationCache();
        int size = data.size(cache, (short) 2);

        ByteBufferAccessor writer = new ByteBufferAccessor(ByteBuffer.allocate(size));
        data.write(writer, cache, (short) 2);
    }

    @Test
    public void testSerializeWithHeader() {
        CreatableTopicCollection topicsToCreate = new CreatableTopicCollection(1);
        topicsToCreate.add(new CreatableTopic()
                               .setName("topic")
                               .setNumPartitions(3)
                               .setReplicationFactor((short) 2));

        CreateTopicsRequest createTopicsRequest = new CreateTopicsRequest.Builder(
            new CreateTopicsRequestData()
                .setTimeoutMs(10)
                .setTopics(topicsToCreate)
        ).build();

        short requestVersion = CREATE_TOPICS.latestVersion();
        RequestHeader requestHeader = new RequestHeader(CREATE_TOPICS, requestVersion, "client", 2);
        ByteBuffer serializedRequest = createTopicsRequest.serializeWithHeader(requestHeader);

        RequestHeader parsedHeader = RequestHeader.parse(serializedRequest);
        assertEquals(requestHeader, parsedHeader);

        RequestAndSize parsedRequest = AbstractRequest.parseRequest(
            CREATE_TOPICS, requestVersion, serializedRequest);

        assertEquals(createTopicsRequest.data(), parsedRequest.request.data());
    }

    @Test
    public void testSerializeWithInconsistentHeaderApiKey() {
        CreateTopicsRequest createTopicsRequest = new CreateTopicsRequest.Builder(
            new CreateTopicsRequestData()
        ).build();
        short requestVersion = CREATE_TOPICS.latestVersion();
        RequestHeader requestHeader = new RequestHeader(DELETE_TOPICS, requestVersion, "client", 2);
        assertThrows(IllegalArgumentException.class, () -> createTopicsRequest.serializeWithHeader(requestHeader));
    }

    @Test
    public void testSerializeWithInconsistentHeaderVersion() {
        CreateTopicsRequest createTopicsRequest = new CreateTopicsRequest.Builder(
            new CreateTopicsRequestData()
        ).build((short) 2);
        RequestHeader requestHeader = new RequestHeader(CREATE_TOPICS, (short) 1, "client", 2);
        assertThrows(IllegalArgumentException.class, () -> createTopicsRequest.serializeWithHeader(requestHeader));
    }

    @Test
    public void testJoinGroupRequestV0RebalanceTimeout() {
        final short version = 0;
        JoinGroupRequest jgr = createJoinGroupRequest(version);
        JoinGroupRequest jgr2 = JoinGroupRequest.parse(jgr.serialize(), version);
        assertEquals(jgr2.data().rebalanceTimeoutMs(), jgr.data().rebalanceTimeoutMs());
    }

    @Test
    public void testOffsetFetchRequestBuilderToStringV0ToV7() {
        List<Boolean> stableFlags = asList(true, false);
        for (Boolean requireStable : stableFlags) {
            String allTopicPartitionsString = new OffsetFetchRequest.Builder("someGroup",
                requireStable,
                null,
                false)
                .toString();

            assertTrue(allTopicPartitionsString.contains("groupId='someGroup', topics=null,"
                + " groups=[], requireStable=" + requireStable));
            String string = new OffsetFetchRequest.Builder("group1",
                requireStable,
                singletonList(
                    new TopicPartition("test11", 1)),
                false)
                .toString();
            assertTrue(string.contains("test11"));
            assertTrue(string.contains("group1"));
            assertTrue(string.contains("requireStable=" + requireStable));
        }
    }

    @Test
    public void testOffsetFetchRequestBuilderToStringV8AndAbove() {
        List<Boolean> stableFlags = asList(true, false);
        for (Boolean requireStable : stableFlags) {
            String allTopicPartitionsString = new OffsetFetchRequest.Builder(
                Collections.singletonMap("someGroup", null),
                requireStable,
                false)
                .toString();
            assertTrue(allTopicPartitionsString.contains("groups=[OffsetFetchRequestGroup"
                + "(groupId='someGroup', topics=null)], requireStable=" + requireStable));

            String subsetTopicPartitionsString = new OffsetFetchRequest.Builder(
                Collections.singletonMap(
                    "group1",
                    singletonList(new TopicPartition("test11", 1))),
                requireStable,
                false)
                .toString();
            assertTrue(subsetTopicPartitionsString.contains("test11"));
            assertTrue(subsetTopicPartitionsString.contains("group1"));
            assertTrue(subsetTopicPartitionsString.contains("requireStable=" + requireStable));
        }
    }

    @Test
    public void testApiVersionsRequestBeforeV3Validation() {
        for (short version = 0; version < 3; version++) {
            ApiVersionsRequest request = new ApiVersionsRequest(new ApiVersionsRequestData(), version);
            assertTrue(request.isValid());
        }
    }

    @Test
    public void testValidApiVersionsRequest() {
        ApiVersionsRequest request;

        request = new ApiVersionsRequest.Builder().build();
        assertTrue(request.isValid());

        request = new ApiVersionsRequest(new ApiVersionsRequestData()
            .setClientSoftwareName("apache-kafka.java")
            .setClientSoftwareVersion("0.0.0-SNAPSHOT"),
            API_VERSIONS.latestVersion()
        );
        assertTrue(request.isValid());
    }

    @Test
    public void testListGroupRequestV3FailsWithStates() {
        ListGroupsRequestData data = new ListGroupsRequestData()
                .setStatesFilter(singletonList(ConsumerGroupState.STABLE.name()));
        assertThrows(UnsupportedVersionException.class, () -> new ListGroupsRequest.Builder(data).build((short) 3));
    }

    @Test
    public void testInvalidApiVersionsRequest() {
        testInvalidCase("java@apache_kafka", "0.0.0-SNAPSHOT");
        testInvalidCase("apache-kafka-java", "0.0.0@java");
        testInvalidCase("-apache-kafka-java", "0.0.0");
        testInvalidCase("apache-kafka-java.", "0.0.0");
    }

    private void testInvalidCase(String name, String version) {
        ApiVersionsRequest request = new ApiVersionsRequest(new ApiVersionsRequestData()
            .setClientSoftwareName(name)
            .setClientSoftwareVersion(version),
            API_VERSIONS.latestVersion()
        );
        assertFalse(request.isValid());
    }

    @Test
    public void testApiVersionResponseWithUnsupportedError() {
        for (short version : API_VERSIONS.allVersions()) {
            ApiVersionsRequest request = new ApiVersionsRequest.Builder().build(version);
            ApiVersionsResponse response = request.getErrorResponse(0, Errors.UNSUPPORTED_VERSION.exception());
            assertEquals(Errors.UNSUPPORTED_VERSION.code(), response.data().errorCode());

            ApiVersion apiVersion = response.data().apiKeys().find(API_VERSIONS.id);
            assertNotNull(apiVersion);
            assertEquals(API_VERSIONS.id, apiVersion.apiKey());
            assertEquals(API_VERSIONS.oldestVersion(), apiVersion.minVersion());
            assertEquals(API_VERSIONS.latestVersion(), apiVersion.maxVersion());
        }
    }

    @Test
    public void testApiVersionResponseWithNotUnsupportedError() {
        for (short version : API_VERSIONS.allVersions()) {
            ApiVersionsRequest request = new ApiVersionsRequest.Builder().build(version);
            ApiVersionsResponse response = request.getErrorResponse(0, Errors.INVALID_REQUEST.exception());
            assertEquals(response.data().errorCode(), Errors.INVALID_REQUEST.code());
            assertTrue(response.data().apiKeys().isEmpty());
        }
    }

    private ApiVersionsResponse defaultApiVersionsResponse() {
        return ApiVersionsResponse.defaultApiVersionsResponse(ApiMessageType.ListenerType.ZK_BROKER);
    }

    @Test
    public void testApiVersionResponseParsingFallback() {
        for (short version : API_VERSIONS.allVersions()) {
            ByteBuffer buffer = defaultApiVersionsResponse().serialize((short) 0);
            ApiVersionsResponse response = ApiVersionsResponse.parse(buffer, version);
            assertEquals(Errors.NONE.code(), response.data().errorCode());
        }
    }

    @Test
    public void testApiVersionResponseParsingFallbackException() {
        for (final short version : API_VERSIONS.allVersions()) {
            assertThrows(BufferUnderflowException.class, () -> ApiVersionsResponse.parse(ByteBuffer.allocate(0), version));
        }
    }

    @Test
    public void testApiVersionResponseParsing() {
        for (short version : API_VERSIONS.allVersions()) {
            ByteBuffer buffer = defaultApiVersionsResponse().serialize(version);
            ApiVersionsResponse response = ApiVersionsResponse.parse(buffer, version);
            assertEquals(Errors.NONE.code(), response.data().errorCode());
        }
    }

    @Test
    public void testInitProducerIdRequestVersions() {
        InitProducerIdRequest.Builder bld = new InitProducerIdRequest.Builder(
            new InitProducerIdRequestData().setTransactionTimeoutMs(1000).
                setTransactionalId("abracadabra").
                setProducerId(123));
        final UnsupportedVersionException exception = assertThrows(
            UnsupportedVersionException.class, () -> bld.build((short) 2).serialize());
        assertTrue(exception.getMessage().contains("Attempted to write a non-default producerId at version 2"));
        bld.build((short) 3);
    }

    @Test
    public void testDeletableTopicResultErrorMessageIsNullByDefault() {
        DeletableTopicResult result = new DeletableTopicResult()
            .setName("topic")
            .setErrorCode(Errors.THROTTLING_QUOTA_EXCEEDED.code());

        assertEquals("topic", result.name());
        assertEquals(Errors.THROTTLING_QUOTA_EXCEEDED.code(), result.errorCode());
        assertNull(result.errorMessage());
    }

    /**
     * Check that all error codes in the response get included in {@link AbstractResponse#errorCounts()}.
     */
    @Test
    public void testErrorCountsIncludesNone() {
        assertEquals(1, createAddOffsetsToTxnResponse().errorCounts().get(Errors.NONE));
        assertEquals(1, createAddPartitionsToTxnResponse().errorCounts().get(Errors.NONE));
        assertEquals(1, createAlterClientQuotasResponse().errorCounts().get(Errors.NONE));
        assertEquals(1, createAlterConfigsResponse().errorCounts().get(Errors.NONE));
        assertEquals(2, createAlterPartitionReassignmentsResponse().errorCounts().get(Errors.NONE));
        assertEquals(1, createAlterReplicaLogDirsResponse().errorCounts().get(Errors.NONE));
        assertEquals(1, createApiVersionResponse().errorCounts().get(Errors.NONE));
        assertEquals(1, createBrokerHeartbeatResponse().errorCounts().get(Errors.NONE));
        assertEquals(1, createBrokerRegistrationResponse().errorCounts().get(Errors.NONE));
        assertEquals(1, createControlledShutdownResponse().errorCounts().get(Errors.NONE));
        assertEquals(2, createCreateAclsResponse().errorCounts().get(Errors.NONE));
        assertEquals(1, createCreatePartitionsResponse().errorCounts().get(Errors.NONE));
        assertEquals(1, createCreateTokenResponse().errorCounts().get(Errors.NONE));
        assertEquals(1, createCreateTopicResponse().errorCounts().get(Errors.NONE));
        assertEquals(1, createDeleteAclsResponse(DELETE_ACLS.latestVersion()).errorCounts().get(Errors.NONE));
        assertEquals(1, createDeleteGroupsResponse().errorCounts().get(Errors.NONE));
        assertEquals(1, createDeleteTopicsResponse().errorCounts().get(Errors.NONE));
        assertEquals(1, createDescribeAclsResponse().errorCounts().get(Errors.NONE));
        assertEquals(1, createDescribeClientQuotasResponse().errorCounts().get(Errors.NONE));
        assertEquals(2, createDescribeConfigsResponse(DESCRIBE_CONFIGS.latestVersion()).errorCounts().get(Errors.NONE));
        assertEquals(1, createDescribeGroupResponse().errorCounts().get(Errors.NONE));
        assertEquals(2, createDescribeLogDirsResponse().errorCounts().get(Errors.NONE));
        assertEquals(1, createDescribeTokenResponse().errorCounts().get(Errors.NONE));
        assertEquals(2, createElectLeadersResponse().errorCounts().get(Errors.NONE));
        assertEquals(1, createEndTxnResponse().errorCounts().get(Errors.NONE));
        assertEquals(1, createExpireTokenResponse().errorCounts().get(Errors.NONE));
        assertEquals(3, createFetchResponse(123).errorCounts().get(Errors.NONE));
        assertEquals(1, createFindCoordinatorResponse(FIND_COORDINATOR.oldestVersion()).errorCounts().get(Errors.NONE));
        assertEquals(1, createFindCoordinatorResponse(FIND_COORDINATOR.latestVersion()).errorCounts().get(Errors.NONE));
        assertEquals(1, createHeartBeatResponse().errorCounts().get(Errors.NONE));
        assertEquals(1, createIncrementalAlterConfigsResponse().errorCounts().get(Errors.NONE));
        assertEquals(1, createJoinGroupResponse(JOIN_GROUP.latestVersion()).errorCounts().get(Errors.NONE));
        assertEquals(2, createLeaderAndIsrResponse((short) 4).errorCounts().get(Errors.NONE));
        assertEquals(2, createLeaderAndIsrResponse((short) 5).errorCounts().get(Errors.NONE));
        assertEquals(3, createLeaderEpochResponse().errorCounts().get(Errors.NONE));
        assertEquals(1, createLeaveGroupResponse().errorCounts().get(Errors.NONE));
        assertEquals(1, createListGroupsResponse(LIST_GROUPS.latestVersion()).errorCounts().get(Errors.NONE));
        assertEquals(1, createListOffsetResponse(LIST_OFFSETS.latestVersion()).errorCounts().get(Errors.NONE));
        assertEquals(1, createListPartitionReassignmentsResponse().errorCounts().get(Errors.NONE));
        assertEquals(3, createMetadataResponse().errorCounts().get(Errors.NONE));
        assertEquals(1, createOffsetCommitResponse().errorCounts().get(Errors.NONE));
        assertEquals(2, createOffsetDeleteResponse().errorCounts().get(Errors.NONE));
        assertEquals(3, createOffsetFetchResponse(OFFSET_FETCH.latestVersion()).errorCounts().get(Errors.NONE));
        assertEquals(1, createProduceResponse().errorCounts().get(Errors.NONE));
        assertEquals(1, createRenewTokenResponse().errorCounts().get(Errors.NONE));
        assertEquals(1, createSaslAuthenticateResponse().errorCounts().get(Errors.NONE));
        assertEquals(1, createSaslHandshakeResponse().errorCounts().get(Errors.NONE));
        assertEquals(2, createStopReplicaResponse().errorCounts().get(Errors.NONE));
        assertEquals(1, createSyncGroupResponse(SYNC_GROUP.latestVersion()).errorCounts().get(Errors.NONE));
        assertEquals(1, createTxnOffsetCommitResponse().errorCounts().get(Errors.NONE));
        assertEquals(1, createUpdateMetadataResponse().errorCounts().get(Errors.NONE));
        assertEquals(1, createWriteTxnMarkersResponse().errorCounts().get(Errors.NONE));
    }

    private AbstractRequest getRequest(ApiKeys apikey, short version) {
        switch (apikey) {
            case PRODUCE: return createProduceRequest(version);
            case FETCH: return createFetchRequest(version);
            case LIST_OFFSETS: return createListOffsetRequest(version);
            case METADATA: return createMetadataRequest(version, singletonList("topic1"));
            case LEADER_AND_ISR: return createLeaderAndIsrRequest(version);
            case STOP_REPLICA: return createStopReplicaRequest(version, true);
            case UPDATE_METADATA: return createUpdateMetadataRequest(version, "rack1");
            case CONTROLLED_SHUTDOWN: return createControlledShutdownRequest(version);
            case OFFSET_COMMIT: return createOffsetCommitRequest(version);
            case OFFSET_FETCH: return createOffsetFetchRequest(version, true);
            case FIND_COORDINATOR: return createFindCoordinatorRequest(version);
            case JOIN_GROUP: return createJoinGroupRequest(version);
            case HEARTBEAT: return createHeartBeatRequest(version);
            case LEAVE_GROUP: return createLeaveGroupRequest(version);
            case SYNC_GROUP: return createSyncGroupRequest(version);
            case DESCRIBE_GROUPS: return createDescribeGroupRequest(version);
            case LIST_GROUPS: return createListGroupsRequest(version);
            case SASL_HANDSHAKE: return createSaslHandshakeRequest(version);
            case API_VERSIONS: return createApiVersionRequest(version);
            case CREATE_TOPICS: return createCreateTopicRequest(version);
            case DELETE_TOPICS: return createDeleteTopicsRequest(version);
            case DELETE_RECORDS: return createDeleteRecordsRequest(version);
            case INIT_PRODUCER_ID: return createInitPidRequest(version);
            case OFFSET_FOR_LEADER_EPOCH: return createLeaderEpochRequestForReplica(version, 1);
            case ADD_PARTITIONS_TO_TXN: return createAddPartitionsToTxnRequest(version);
            case ADD_OFFSETS_TO_TXN: return createAddOffsetsToTxnRequest(version);
            case END_TXN: return createEndTxnRequest(version);
            case WRITE_TXN_MARKERS: return createWriteTxnMarkersRequest(version);
            case TXN_OFFSET_COMMIT: return createTxnOffsetCommitRequest(version);
            case DESCRIBE_ACLS: return createDescribeAclsRequest(version);
            case CREATE_ACLS: return createCreateAclsRequest(version);
            case DELETE_ACLS: return createDeleteAclsRequest(version);
            case DESCRIBE_CONFIGS: return createDescribeConfigsRequest(version);
            case ALTER_CONFIGS: return createAlterConfigsRequest(version);
            case ALTER_REPLICA_LOG_DIRS: return createAlterReplicaLogDirsRequest(version);
            case DESCRIBE_LOG_DIRS: return createDescribeLogDirsRequest(version);
            case SASL_AUTHENTICATE: return createSaslAuthenticateRequest(version);
            case CREATE_PARTITIONS: return createCreatePartitionsRequest(version);
            case CREATE_DELEGATION_TOKEN: return createCreateTokenRequest(version);
            case RENEW_DELEGATION_TOKEN: return createRenewTokenRequest(version);
            case EXPIRE_DELEGATION_TOKEN: return createExpireTokenRequest(version);
            case DESCRIBE_DELEGATION_TOKEN: return createDescribeTokenRequest(version);
            case DELETE_GROUPS: return createDeleteGroupsRequest(version);
            case ELECT_LEADERS: return createElectLeadersRequest(version);
            case INCREMENTAL_ALTER_CONFIGS: return createIncrementalAlterConfigsRequest(version);
            case ALTER_PARTITION_REASSIGNMENTS: return createAlterPartitionReassignmentsRequest(version);
            case LIST_PARTITION_REASSIGNMENTS: return createListPartitionReassignmentsRequest(version);
            case OFFSET_DELETE: return createOffsetDeleteRequest(version);
            case DESCRIBE_CLIENT_QUOTAS: return createDescribeClientQuotasRequest(version);
            case ALTER_CLIENT_QUOTAS: return createAlterClientQuotasRequest(version);
            case DESCRIBE_USER_SCRAM_CREDENTIALS: return createDescribeUserScramCredentialsRequest(version);
            case ALTER_USER_SCRAM_CREDENTIALS: return createAlterUserScramCredentialsRequest(version);
            case VOTE: return createVoteRequest(version);
            case BEGIN_QUORUM_EPOCH: return createBeginQuorumEpochRequest(version);
            case END_QUORUM_EPOCH: return createEndQuorumEpochRequest(version);
            case DESCRIBE_QUORUM: return createDescribeQuorumRequest(version);
            case ALTER_PARTITION: return createAlterPartitionRequest(version);
            case UPDATE_FEATURES: return createUpdateFeaturesRequest(version);
            case ENVELOPE: return createEnvelopeRequest(version);
            case FETCH_SNAPSHOT: return createFetchSnapshotRequest(version);
            case DESCRIBE_CLUSTER: return createDescribeClusterRequest(version);
            case DESCRIBE_PRODUCERS: return createDescribeProducersRequest(version);
            case BROKER_REGISTRATION: return createBrokerRegistrationRequest(version);
            case BROKER_HEARTBEAT: return createBrokerHeartbeatRequest(version);
            case UNREGISTER_BROKER: return createUnregisterBrokerRequest(version);
            case DESCRIBE_TRANSACTIONS: return createDescribeTransactionsRequest(version);
            case LIST_TRANSACTIONS: return createListTransactionsRequest(version);
            case ALLOCATE_PRODUCER_IDS: return createAllocateProducerIdsRequest(version);
            default: throw new IllegalArgumentException("Unknown API key " + apikey);
        }
    }

    private AbstractResponse getResponse(ApiKeys apikey, short version) {
        switch (apikey) {
            case PRODUCE: return createProduceResponse();
            case FETCH: return createFetchResponse(version);
            case LIST_OFFSETS: return createListOffsetResponse(version);
            case METADATA: return createMetadataResponse();
            case LEADER_AND_ISR: return createLeaderAndIsrResponse(version);
            case STOP_REPLICA: return createStopReplicaResponse();
            case UPDATE_METADATA: return createUpdateMetadataResponse();
            case CONTROLLED_SHUTDOWN: return createControlledShutdownResponse();
            case OFFSET_COMMIT: return createOffsetCommitResponse();
            case OFFSET_FETCH: return createOffsetFetchResponse(version);
            case FIND_COORDINATOR: return createFindCoordinatorResponse(version);
            case JOIN_GROUP: return createJoinGroupResponse(version);
            case HEARTBEAT: return createHeartBeatResponse();
            case LEAVE_GROUP: return createLeaveGroupResponse();
            case SYNC_GROUP: return createSyncGroupResponse(version);
            case DESCRIBE_GROUPS: return createDescribeGroupResponse();
            case LIST_GROUPS: return createListGroupsResponse(version);
            case SASL_HANDSHAKE: return createSaslHandshakeResponse();
            case API_VERSIONS: return createApiVersionResponse();
            case CREATE_TOPICS: return createCreateTopicResponse();
            case DELETE_TOPICS: return createDeleteTopicsResponse();
            case DELETE_RECORDS: return createDeleteRecordsResponse();
            case INIT_PRODUCER_ID: return createInitPidResponse();
            case OFFSET_FOR_LEADER_EPOCH: return createLeaderEpochResponse();
            case ADD_PARTITIONS_TO_TXN: return createAddPartitionsToTxnResponse();
            case ADD_OFFSETS_TO_TXN: return createAddOffsetsToTxnResponse();
            case END_TXN: return createEndTxnResponse();
            case WRITE_TXN_MARKERS: return createWriteTxnMarkersResponse();
            case TXN_OFFSET_COMMIT: return createTxnOffsetCommitResponse();
            case DESCRIBE_ACLS: return createDescribeAclsResponse();
            case CREATE_ACLS: return createCreateAclsResponse();
            case DELETE_ACLS: return createDeleteAclsResponse(version);
            case DESCRIBE_CONFIGS: return createDescribeConfigsResponse(version);
            case ALTER_CONFIGS: return createAlterConfigsResponse();
            case ALTER_REPLICA_LOG_DIRS: return createAlterReplicaLogDirsResponse();
            case DESCRIBE_LOG_DIRS: return createDescribeLogDirsResponse();
            case SASL_AUTHENTICATE: return createSaslAuthenticateResponse();
            case CREATE_PARTITIONS: return createCreatePartitionsResponse();
            case CREATE_DELEGATION_TOKEN: return createCreateTokenResponse();
            case RENEW_DELEGATION_TOKEN: return createRenewTokenResponse();
            case EXPIRE_DELEGATION_TOKEN: return createExpireTokenResponse();
            case DESCRIBE_DELEGATION_TOKEN: return createDescribeTokenResponse();
            case DELETE_GROUPS: return createDeleteGroupsResponse();
            case ELECT_LEADERS: return createElectLeadersResponse();
            case INCREMENTAL_ALTER_CONFIGS: return createIncrementalAlterConfigsResponse();
            case ALTER_PARTITION_REASSIGNMENTS: return createAlterPartitionReassignmentsResponse();
            case LIST_PARTITION_REASSIGNMENTS: return createListPartitionReassignmentsResponse();
            case OFFSET_DELETE: return createOffsetDeleteResponse();
            case DESCRIBE_CLIENT_QUOTAS: return createDescribeClientQuotasResponse();
            case ALTER_CLIENT_QUOTAS: return createAlterClientQuotasResponse();
            case DESCRIBE_USER_SCRAM_CREDENTIALS: return createDescribeUserScramCredentialsResponse();
            case ALTER_USER_SCRAM_CREDENTIALS: return createAlterUserScramCredentialsResponse();
            case VOTE: return createVoteResponse();
            case BEGIN_QUORUM_EPOCH: return createBeginQuorumEpochResponse();
            case END_QUORUM_EPOCH: return createEndQuorumEpochResponse();
            case DESCRIBE_QUORUM: return createDescribeQuorumResponse();
            case ALTER_PARTITION: return createAlterPartitionResponse(version);
            case UPDATE_FEATURES: return createUpdateFeaturesResponse();
            case ENVELOPE: return createEnvelopeResponse();
            case FETCH_SNAPSHOT: return createFetchSnapshotResponse();
            case DESCRIBE_CLUSTER: return createDescribeClusterResponse();
            case DESCRIBE_PRODUCERS: return createDescribeProducersResponse();
            case BROKER_REGISTRATION: return createBrokerRegistrationResponse();
            case BROKER_HEARTBEAT: return createBrokerHeartbeatResponse();
            case UNREGISTER_BROKER: return createUnregisterBrokerResponse();
            case DESCRIBE_TRANSACTIONS: return createDescribeTransactionsResponse();
            case LIST_TRANSACTIONS: return createListTransactionsResponse();
            case ALLOCATE_PRODUCER_IDS: return createAllocateProducerIdsResponse();
            default: throw new IllegalArgumentException("Unknown API key " + apikey);
        }
    }

    private FetchSnapshotRequest createFetchSnapshotRequest(short version) {
        FetchSnapshotRequestData data = new FetchSnapshotRequestData()
                .setClusterId("clusterId")
                .setTopics(singletonList(new FetchSnapshotRequestData.TopicSnapshot()
                        .setName("topic1")
                        .setPartitions(singletonList(new FetchSnapshotRequestData.PartitionSnapshot()
                                .setSnapshotId(new FetchSnapshotRequestData.SnapshotId()
                                        .setEndOffset(123L)
                                        .setEpoch(0))
                                .setPosition(123L)
                                .setPartition(0)
                                .setCurrentLeaderEpoch(1)))))
                .setMaxBytes(1000)
                .setReplicaId(2);
        return new FetchSnapshotRequest.Builder(data).build(version);
    }

    private FetchSnapshotResponse createFetchSnapshotResponse() {
        FetchSnapshotResponseData data = new FetchSnapshotResponseData()
                .setErrorCode(Errors.NONE.code())
                .setTopics(singletonList(new FetchSnapshotResponseData.TopicSnapshot()
                        .setName("topic1")
                        .setPartitions(singletonList(new FetchSnapshotResponseData.PartitionSnapshot()
                                .setErrorCode(Errors.NONE.code())
                                .setIndex(0)
                                .setCurrentLeader(new FetchSnapshotResponseData.LeaderIdAndEpoch()
                                        .setLeaderEpoch(0)
                                        .setLeaderId(1))
                                .setSnapshotId(new FetchSnapshotResponseData.SnapshotId()
                                        .setEndOffset(123L)
                                        .setEpoch(0))
                                .setPosition(234L)
                                .setSize(345L)
                                .setUnalignedRecords(MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord("blah".getBytes())))))))
                .setThrottleTimeMs(123);
        return new FetchSnapshotResponse(data);
    }

    private EnvelopeRequest createEnvelopeRequest(short version) {
        return new EnvelopeRequest.Builder(
                ByteBuffer.wrap("data".getBytes(StandardCharsets.UTF_8)),
                "principal".getBytes(StandardCharsets.UTF_8),
                "address".getBytes(StandardCharsets.UTF_8))
                .build(version);
    }

    private EnvelopeResponse createEnvelopeResponse() {
        EnvelopeResponseData data = new EnvelopeResponseData()
                .setResponseData(ByteBuffer.wrap("data".getBytes(StandardCharsets.UTF_8)))
                .setErrorCode(Errors.NONE.code());
        return new EnvelopeResponse(data);
    }

    private DescribeQuorumRequest createDescribeQuorumRequest(short version) {
        DescribeQuorumRequestData data = new DescribeQuorumRequestData()
                .setTopics(singletonList(new DescribeQuorumRequestData.TopicData()
                        .setPartitions(singletonList(new DescribeQuorumRequestData.PartitionData()
                                .setPartitionIndex(0)))
                        .setTopicName("topic1")));
        return new DescribeQuorumRequest.Builder(data).build(version);
    }

    private DescribeQuorumResponse createDescribeQuorumResponse() {
        DescribeQuorumResponseData data = new DescribeQuorumResponseData()
                .setErrorCode(Errors.NONE.code());
        return new DescribeQuorumResponse(data);
    }

    private EndQuorumEpochRequest createEndQuorumEpochRequest(short version) {
        EndQuorumEpochRequestData data = new EndQuorumEpochRequestData()
                .setClusterId("clusterId")
                .setTopics(singletonList(new EndQuorumEpochRequestData.TopicData()
                        .setPartitions(singletonList(new EndQuorumEpochRequestData.PartitionData()
                                .setLeaderEpoch(0)
                                .setLeaderId(1)
                                .setPartitionIndex(2)
                                .setPreferredSuccessors(asList(0, 1, 2))))
                        .setTopicName("topic1")));
        return new EndQuorumEpochRequest.Builder(data).build(version);
    }

    private EndQuorumEpochResponse createEndQuorumEpochResponse() {
        EndQuorumEpochResponseData data = new EndQuorumEpochResponseData()
                .setErrorCode(Errors.NONE.code())
                .setTopics(singletonList(new EndQuorumEpochResponseData.TopicData()
                        .setPartitions(singletonList(new EndQuorumEpochResponseData.PartitionData()
                                .setErrorCode(Errors.NONE.code())
                                .setLeaderEpoch(1)))
                        .setTopicName("topic1")));
        return new EndQuorumEpochResponse(data);
    }

    private BeginQuorumEpochRequest createBeginQuorumEpochRequest(short version) {
        BeginQuorumEpochRequestData data = new BeginQuorumEpochRequestData()
                .setClusterId("clusterId")
                .setTopics(singletonList(new BeginQuorumEpochRequestData.TopicData()
                        .setPartitions(singletonList(new BeginQuorumEpochRequestData.PartitionData()
                                .setLeaderEpoch(0)
                                .setLeaderId(1)
                                .setPartitionIndex(2)))));
        return new BeginQuorumEpochRequest.Builder(data).build(version);
    }

    private BeginQuorumEpochResponse createBeginQuorumEpochResponse() {
        BeginQuorumEpochResponseData data = new BeginQuorumEpochResponseData()
                .setErrorCode(Errors.NONE.code())
                .setTopics(singletonList(new BeginQuorumEpochResponseData.TopicData()
                        .setPartitions(singletonList(new BeginQuorumEpochResponseData.PartitionData()
                                .setErrorCode(Errors.NONE.code())
                                .setLeaderEpoch(0)
                                .setLeaderId(1)
                                .setPartitionIndex(2)))));
        return new BeginQuorumEpochResponse(data);
    }

    private VoteRequest createVoteRequest(short version) {
        VoteRequestData data = new VoteRequestData()
                .setClusterId("clusterId")
                .setTopics(singletonList(new VoteRequestData.TopicData()
                        .setPartitions(singletonList(new VoteRequestData.PartitionData()
                                .setPartitionIndex(0)
                                .setCandidateEpoch(1)
                                .setCandidateId(2)
                                .setLastOffset(3L)
                                .setLastOffsetEpoch(4)))
                        .setTopicName("topic1")));
        return new VoteRequest.Builder(data).build(version);
    }

    private VoteResponse createVoteResponse() {
        VoteResponseData data = new VoteResponseData()
                .setErrorCode(Errors.NONE.code())
                .setTopics(singletonList(new VoteResponseData.TopicData()
                        .setPartitions(singletonList(new VoteResponseData.PartitionData()
                                .setErrorCode(Errors.NONE.code())
                                .setLeaderEpoch(0)
                                .setPartitionIndex(1)
                                .setLeaderId(2)
                                .setVoteGranted(false)))));
        return new VoteResponse(data);
    }

    private AlterUserScramCredentialsRequest createAlterUserScramCredentialsRequest(short version) {
        AlterUserScramCredentialsRequestData data = new AlterUserScramCredentialsRequestData()
                .setDeletions(singletonList(new AlterUserScramCredentialsRequestData.ScramCredentialDeletion()
                        .setName("user1")
                        .setMechanism((byte) 0)))
                .setUpsertions(singletonList(new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion()
                        .setName("user2")
                        .setIterations(1024)
                        .setMechanism((byte) 1)
                        .setSalt("salt".getBytes())));
        return new AlterUserScramCredentialsRequest.Builder(data).build(version);
    }

    private AlterUserScramCredentialsResponse createAlterUserScramCredentialsResponse() {
        AlterUserScramCredentialsResponseData data = new AlterUserScramCredentialsResponseData()
                .setResults(singletonList(new AlterUserScramCredentialsResponseData.AlterUserScramCredentialsResult()
                        .setErrorCode(Errors.NONE.code())
                        .setUser("user1")
                        .setErrorMessage("error message")));
        return new AlterUserScramCredentialsResponse(data);
    }

    private DescribeUserScramCredentialsRequest createDescribeUserScramCredentialsRequest(short version) {
        DescribeUserScramCredentialsRequestData data = new DescribeUserScramCredentialsRequestData()
                .setUsers(singletonList(new DescribeUserScramCredentialsRequestData.UserName()
                        .setName("user1")));
        return new DescribeUserScramCredentialsRequest.Builder(data).build(version);
    }

    private DescribeUserScramCredentialsResponse createDescribeUserScramCredentialsResponse() {
        DescribeUserScramCredentialsResponseData data = new DescribeUserScramCredentialsResponseData()
                .setResults(singletonList(new DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult()
                        .setUser("user1")
                        .setErrorCode(Errors.NONE.code())
                        .setErrorMessage("error message")
                        .setCredentialInfos(singletonList(new DescribeUserScramCredentialsResponseData.CredentialInfo()
                                .setIterations(1024)
                                .setMechanism((byte) 0)))))
                .setErrorCode(Errors.NONE.code())
                .setErrorMessage("error message")
                .setThrottleTimeMs(123);
        return new DescribeUserScramCredentialsResponse(data);
    }

    private AlterPartitionRequest createAlterPartitionRequest(short version) {
        AlterPartitionRequestData.PartitionData partitionData = new AlterPartitionRequestData.PartitionData()
            .setPartitionIndex(1)
            .setPartitionEpoch(2)
            .setLeaderEpoch(3)
            .setNewIsr(asList(1, 2));

        if (version >= 1) {
            // Use the none default value; 1 - RECOVERING
            partitionData.setLeaderRecoveryState((byte) 1);
        }

        AlterPartitionRequestData data = new AlterPartitionRequestData()
            .setBrokerEpoch(123L)
            .setBrokerId(1)
            .setTopics(singletonList(new AlterPartitionRequestData.TopicData()
                .setName("topic1")
                .setPartitions(singletonList(partitionData))));
        return new AlterPartitionRequest.Builder(data).build(version);
    }

    private AlterPartitionResponse createAlterPartitionResponse(int version) {
        AlterPartitionResponseData.PartitionData partitionData = new AlterPartitionResponseData.PartitionData()
            .setPartitionEpoch(1)
            .setIsr(asList(0, 1, 2))
            .setErrorCode(Errors.NONE.code())
            .setLeaderEpoch(2)
            .setLeaderId(3);

        if (version >= 1) {
            // Use the none default value; 1 - RECOVERING
            partitionData.setLeaderRecoveryState((byte) 1);
        }

        AlterPartitionResponseData data = new AlterPartitionResponseData()
                .setErrorCode(Errors.NONE.code())
                .setThrottleTimeMs(123)
                .setTopics(singletonList(new AlterPartitionResponseData.TopicData()
                        .setName("topic1")
                        .setPartitions(singletonList(partitionData))));
        return new AlterPartitionResponse(data);
    }

    private UpdateFeaturesRequest createUpdateFeaturesRequest(short version) {
        UpdateFeaturesRequestData.FeatureUpdateKeyCollection features = new UpdateFeaturesRequestData.FeatureUpdateKeyCollection();
        features.add(new UpdateFeaturesRequestData.FeatureUpdateKey()
                .setFeature("feature1")
                .setAllowDowngrade(false)
                .setMaxVersionLevel((short) 1));
        UpdateFeaturesRequestData data = new UpdateFeaturesRequestData()
                .setFeatureUpdates(features)
                .setTimeoutMs(123);
        return new UpdateFeaturesRequest.Builder(data).build(version);
    }

    private UpdateFeaturesResponse createUpdateFeaturesResponse() {
        UpdateFeaturesResponseData.UpdatableFeatureResultCollection results = new UpdateFeaturesResponseData.UpdatableFeatureResultCollection();
        results.add(new UpdateFeaturesResponseData.UpdatableFeatureResult()
                .setFeature("feature1")
                .setErrorCode(Errors.NONE.code())
                .setErrorMessage("error message"));
        UpdateFeaturesResponseData data = new UpdateFeaturesResponseData()
                .setErrorCode(Errors.NONE.code())
                .setThrottleTimeMs(123)
                .setResults(results)
                .setErrorMessage("error message");
        return new UpdateFeaturesResponse(data);
    }

    private AllocateProducerIdsRequest createAllocateProducerIdsRequest(short version) {
        AllocateProducerIdsRequestData data = new AllocateProducerIdsRequestData()
                .setBrokerEpoch(123L)
                .setBrokerId(2);
        return new AllocateProducerIdsRequest.Builder(data).build(version);
    }

    private AllocateProducerIdsResponse createAllocateProducerIdsResponse() {
        AllocateProducerIdsResponseData data = new AllocateProducerIdsResponseData()
                .setErrorCode(Errors.NONE.code())
                .setThrottleTimeMs(123)
                .setProducerIdLen(234)
                .setProducerIdStart(345L);
        return new AllocateProducerIdsResponse(data);
    }

    private DescribeLogDirsRequest createDescribeLogDirsRequest(short version) {
        DescribeLogDirsRequestData.DescribableLogDirTopicCollection topics = new DescribeLogDirsRequestData.DescribableLogDirTopicCollection();
        topics.add(new DescribeLogDirsRequestData.DescribableLogDirTopic()
                .setPartitions(asList(0, 1, 2))
                .setTopic("topic1"));
        DescribeLogDirsRequestData data = new DescribeLogDirsRequestData()
                .setTopics(topics);
        return new DescribeLogDirsRequest.Builder(data).build(version);
    }

    private DescribeLogDirsResponse createDescribeLogDirsResponse() {
        DescribeLogDirsResponseData data = new DescribeLogDirsResponseData()
                .setResults(singletonList(new DescribeLogDirsResponseData.DescribeLogDirsResult()
                        .setErrorCode(Errors.NONE.code())
                        .setLogDir("logdir")
                        .setTopics(singletonList(new DescribeLogDirsResponseData.DescribeLogDirsTopic()
                                .setName("topic1")
                                .setPartitions(singletonList(new DescribeLogDirsResponseData.DescribeLogDirsPartition()
                                        .setPartitionIndex(0)
                                        .setIsFutureKey(false)
                                        .setOffsetLag(123L)
                                        .setPartitionSize(234L)))))))
                .setThrottleTimeMs(123);
        return new DescribeLogDirsResponse(data);
    }

    private DeleteRecordsRequest createDeleteRecordsRequest(short version) {
        DeleteRecordsRequestData.DeleteRecordsTopic topic = new DeleteRecordsRequestData.DeleteRecordsTopic()
                .setName("topic1")
                .setPartitions(singletonList(new DeleteRecordsRequestData.DeleteRecordsPartition()
                        .setPartitionIndex(1)
                        .setOffset(123L)));
        DeleteRecordsRequestData data = new DeleteRecordsRequestData()
                .setTopics(singletonList(topic))
                .setTimeoutMs(123);
        return new DeleteRecordsRequest.Builder(data).build(version);
    }

    private DeleteRecordsResponse createDeleteRecordsResponse() {
        DeleteRecordsResponseData.DeleteRecordsTopicResultCollection topics = new DeleteRecordsResponseData.DeleteRecordsTopicResultCollection();
        DeleteRecordsResponseData.DeleteRecordsPartitionResultCollection partitions = new DeleteRecordsResponseData.DeleteRecordsPartitionResultCollection();
        partitions.add(new DeleteRecordsResponseData.DeleteRecordsPartitionResult()
                .setErrorCode(Errors.NONE.code())
                .setLowWatermark(123L)
                .setPartitionIndex(0));
        topics.add(new DeleteRecordsResponseData.DeleteRecordsTopicResult()
                .setName("topic1")
                .setPartitions(partitions));
        DeleteRecordsResponseData data = new DeleteRecordsResponseData()
                .setThrottleTimeMs(123)
                .setTopics(topics);
        return new DeleteRecordsResponse(data);
    }

    private DescribeClusterRequest createDescribeClusterRequest(short version) {
        return new DescribeClusterRequest.Builder(
                new DescribeClusterRequestData()
                        .setIncludeClusterAuthorizedOperations(true))
                .build(version);
    }

    private DescribeClusterResponse createDescribeClusterResponse() {
        return new DescribeClusterResponse(
                new DescribeClusterResponseData()
                        .setBrokers(new DescribeClusterBrokerCollection(
                                singletonList(new DescribeClusterBroker()
                                        .setBrokerId(1)
                                        .setHost("localhost")
                                        .setPort(9092)
                                        .setRack("rack1")).iterator()))
                        .setClusterId("clusterId")
                        .setControllerId(1)
                        .setClusterAuthorizedOperations(10));
    }

    private void checkOlderFetchVersions() {
        for (short version : FETCH.allVersions()) {
            if (version > 7) {
                checkErrorResponse(createFetchRequest(version), unknownServerException);
            }
            checkRequest(createFetchRequest(version));
            checkResponse(createFetchResponse(version >= 4), version);
        }
    }

    private void verifyDescribeConfigsResponse(DescribeConfigsResponse expected, DescribeConfigsResponse actual,
                                               short version) {
        for (Map.Entry<ConfigResource, DescribeConfigsResult> resource : expected.resultMap().entrySet()) {
            List<DescribeConfigsResourceResult> actualEntries = actual.resultMap().get(resource.getKey()).configs();
            List<DescribeConfigsResourceResult> expectedEntries = expected.resultMap().get(resource.getKey()).configs();
            assertEquals(expectedEntries.size(), actualEntries.size());
            for (int i = 0; i < actualEntries.size(); ++i) {
                DescribeConfigsResourceResult actualEntry = actualEntries.get(i);
                DescribeConfigsResourceResult expectedEntry = expectedEntries.get(i);
                assertEquals(expectedEntry.name(), actualEntry.name());
                assertEquals(expectedEntry.value(), actualEntry.value(),
                        "Non-matching values for " + actualEntry.name() + " in version " + version);
                assertEquals(expectedEntry.readOnly(), actualEntry.readOnly(),
                        "Non-matching readonly for " + actualEntry.name() + " in version " + version);
                assertEquals(expectedEntry.isSensitive(), actualEntry.isSensitive(),
                        "Non-matching isSensitive for " + actualEntry.name() + " in version " + version);
                if (version < 3) {
                    assertEquals(ConfigType.UNKNOWN.id(), actualEntry.configType(),
                            "Non-matching configType for " + actualEntry.name() + " in version " + version);
                } else {
                    assertEquals(expectedEntry.configType(), actualEntry.configType(),
                            "Non-matching configType for " + actualEntry.name() + " in version " + version);
                }
                if (version == 0) {
                    assertEquals(DescribeConfigsResponse.ConfigSource.STATIC_BROKER_CONFIG.id(), actualEntry.configSource(),
                            "Non matching configSource for " + actualEntry.name() + " in version " + version);
                } else {
                    assertEquals(expectedEntry.configSource(), actualEntry.configSource(),
                            "Non-matching configSource for " + actualEntry.name() + " in version " + version);
                }
            }
        }
    }

    private void checkDescribeConfigsResponseVersions() {
        for (short version : DESCRIBE_CONFIGS.allVersions()) {
            DescribeConfigsResponse response = createDescribeConfigsResponse(version);
            DescribeConfigsResponse deserialized = (DescribeConfigsResponse) AbstractResponse.parseResponse(DESCRIBE_CONFIGS,
                    response.serialize(version), version);
            verifyDescribeConfigsResponse(response, deserialized, version);
        }
    }

    private void checkErrorResponse(AbstractRequest req, Throwable e) {
        AbstractResponse response = req.getErrorResponse(e);
        checkResponse(response, req.version());
        Errors error = Errors.forException(e);
        Map<Errors, Integer> errorCounts = response.errorCounts();
        assertEquals(Collections.singleton(error), errorCounts.keySet(),
                "API Key " + req.apiKey().name + " v" + req.version() + " failed errorCounts test");
        assertTrue(errorCounts.get(error) > 0);
        if (e instanceof UnknownServerException) {
            String responseStr = response.toString();
            assertFalse(responseStr.contains(e.getMessage()),
                    String.format("Unknown message included in response for %s: %s ", req.apiKey(), responseStr));
        }
    }

    private void checkRequest(AbstractRequest req) {
        // Check that we can serialize, deserialize and serialize again
        // Check for equality of the ByteBuffer only if indicated (it is likely to fail if any of the fields
        // in the request is a HashMap with multiple elements since ordering of the elements may vary)
        try {
            ByteBuffer serializedBytes = req.serialize();
            AbstractRequest deserialized = AbstractRequest.parseRequest(req.apiKey(), req.version(), serializedBytes).request;
            ByteBuffer serializedBytes2 = deserialized.serialize();
            serializedBytes.rewind();
            assertEquals(serializedBytes, serializedBytes2, "Request " + req + "failed equality test");
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize request " + req + " with type " + req.getClass(), e);
        }
    }

    private void checkResponse(AbstractResponse response, short version) {
        // Check that we can serialize, deserialize and serialize again
        // Check for equality and hashCode of the Struct only if indicated (it is likely to fail if any of the fields
        // in the response is a HashMap with multiple elements since ordering of the elements may vary)
        try {
            ByteBuffer serializedBytes = response.serialize(version);
            AbstractResponse deserialized = AbstractResponse.parseResponse(response.apiKey(), serializedBytes, version);
            ByteBuffer serializedBytes2 = deserialized.serialize(version);
            serializedBytes.rewind();
            assertEquals(serializedBytes, serializedBytes2, "Response " + response + "failed equality test");
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize response " + response + " with type " + response.getClass(), e);
        }
    }

    private FindCoordinatorRequest createFindCoordinatorRequest(short version) {
        return new FindCoordinatorRequest.Builder(
                new FindCoordinatorRequestData()
                    .setKeyType(CoordinatorType.GROUP.id())
                    .setKey("test-group"))
                .build(version);
    }

    private FindCoordinatorRequest createBatchedFindCoordinatorRequest(List<String> coordinatorKeys, short version) {
        return new FindCoordinatorRequest.Builder(
                new FindCoordinatorRequestData()
                        .setKeyType(CoordinatorType.GROUP.id())
                        .setCoordinatorKeys(coordinatorKeys))
                .build(version);
    }

    private FindCoordinatorResponse createFindCoordinatorResponse(short version) {
        Node node = new Node(10, "host1", 2014);
        if (version < FindCoordinatorRequest.MIN_BATCHED_VERSION)
            return FindCoordinatorResponse.prepareOldResponse(Errors.NONE, node);
        else
            return FindCoordinatorResponse.prepareResponse(Errors.NONE, "group", node);
    }

    private FetchRequest createFetchRequest(short version, FetchMetadata metadata, List<TopicIdPartition> toForget) {
        LinkedHashMap<TopicPartition, FetchRequest.PartitionData> fetchData = new LinkedHashMap<>();
        fetchData.put(new TopicPartition("test1", 0),
                new FetchRequest.PartitionData(Uuid.randomUuid(), 100, -1L, 1000000, Optional.empty()));
        fetchData.put(new TopicPartition("test2", 0),
                new FetchRequest.PartitionData(Uuid.randomUuid(), 200, -1L, 1000000, Optional.empty()));
        return FetchRequest.Builder.forConsumer(version, 100, 100000, fetchData).
            metadata(metadata).setMaxBytes(1000).removed(toForget).build(version);
    }

    private FetchRequest createFetchRequest(short version, IsolationLevel isolationLevel) {
        LinkedHashMap<TopicPartition, FetchRequest.PartitionData> fetchData = new LinkedHashMap<>();
        fetchData.put(new TopicPartition("test1", 0),
                new FetchRequest.PartitionData(Uuid.randomUuid(), 100, -1L, 1000000, Optional.empty()));
        fetchData.put(new TopicPartition("test2", 0),
                new FetchRequest.PartitionData(Uuid.randomUuid(), 200, -1L, 1000000, Optional.empty()));
        return FetchRequest.Builder.forConsumer(version, 100, 100000, fetchData).
            isolationLevel(isolationLevel).setMaxBytes(1000).build(version);
    }

    private FetchRequest createFetchRequest(short version) {
        LinkedHashMap<TopicPartition, FetchRequest.PartitionData> fetchData = new LinkedHashMap<>();
        fetchData.put(new TopicPartition("test1", 0),
                new FetchRequest.PartitionData(Uuid.randomUuid(), 100, -1L, 1000000, Optional.empty()));
        fetchData.put(new TopicPartition("test2", 0),
                new FetchRequest.PartitionData(Uuid.randomUuid(), 200, -1L, 1000000, Optional.empty()));
        return FetchRequest.Builder.forConsumer(version, 100, 100000, fetchData).setMaxBytes(1000).build(version);
    }

    private FetchResponse createFetchResponse(Errors error, int sessionId) {
        return FetchResponse.parse(
            FetchResponse.of(error, 25, sessionId, new LinkedHashMap<>()).serialize(FETCH.latestVersion()), FETCH.latestVersion());
    }

    private FetchResponse createFetchResponse(int sessionId) {
        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> responseData = new LinkedHashMap<>();
        Map<String, Uuid> topicIds = new HashMap<>();
        topicIds.put("test", Uuid.randomUuid());
        MemoryRecords records = MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord("blah".getBytes()));
        responseData.put(new TopicIdPartition(topicIds.get("test"), new TopicPartition("test", 0)), new FetchResponseData.PartitionData()
                        .setPartitionIndex(0)
                        .setHighWatermark(1000000)
                        .setLogStartOffset(0)
                        .setRecords(records));
        List<FetchResponseData.AbortedTransaction> abortedTransactions = singletonList(
            new FetchResponseData.AbortedTransaction().setProducerId(234L).setFirstOffset(999L));
        responseData.put(new TopicIdPartition(topicIds.get("test"), new TopicPartition("test", 1)), new FetchResponseData.PartitionData()
                        .setPartitionIndex(1)
                        .setHighWatermark(1000000)
                        .setLogStartOffset(0)
                        .setAbortedTransactions(abortedTransactions));
        return FetchResponse.parse(FetchResponse.of(Errors.NONE, 25, sessionId,
            responseData).serialize(FETCH.latestVersion()), FETCH.latestVersion());
    }

    private FetchResponse createFetchResponse(boolean includeAborted) {
        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> responseData = new LinkedHashMap<>();
        Uuid topicId = Uuid.randomUuid();
        MemoryRecords records = MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord("blah".getBytes()));
        responseData.put(new TopicIdPartition(topicId, new TopicPartition("test", 0)), new FetchResponseData.PartitionData()
                        .setPartitionIndex(0)
                        .setHighWatermark(1000000)
                        .setLogStartOffset(0)
                        .setRecords(records));

        List<FetchResponseData.AbortedTransaction> abortedTransactions = emptyList();
        if (includeAborted) {
            abortedTransactions = singletonList(
                    new FetchResponseData.AbortedTransaction().setProducerId(234L).setFirstOffset(999L));
        }
        responseData.put(new TopicIdPartition(topicId, new TopicPartition("test", 1)), new FetchResponseData.PartitionData()
                        .setPartitionIndex(1)
                        .setHighWatermark(1000000)
                        .setLogStartOffset(0)
                        .setAbortedTransactions(abortedTransactions));
        return FetchResponse.parse(FetchResponse.of(Errors.NONE, 25, INVALID_SESSION_ID,
            responseData).serialize(FETCH.latestVersion()), FETCH.latestVersion());
    }

    private FetchResponse createFetchResponse(short version) {
        FetchResponseData data = new FetchResponseData();
        if (version > 0) {
            data.setThrottleTimeMs(345);
        }
        if (version > 6) {
            data.setErrorCode(Errors.NONE.code())
                    .setSessionId(123);
        }
        MemoryRecords records = MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord("blah".getBytes()));
        FetchResponseData.PartitionData partition = new FetchResponseData.PartitionData()
                .setPartitionIndex(0)
                .setErrorCode(Errors.NONE.code())
                .setHighWatermark(123L)
                .setRecords(records);
        if (version > 3) {
            partition.setLastStableOffset(234L);
        }
        if (version > 4) {
            partition.setLogStartOffset(456L);
        }
        if (version > 10) {
            partition.setPreferredReadReplica(1);
        }
        if (version > 11) {
            partition.setDivergingEpoch(new FetchResponseData.EpochEndOffset().setEndOffset(1L).setEpoch(2))
                    .setSnapshotId(new FetchResponseData.SnapshotId().setEndOffset(1L).setEndOffset(2))
                    .setCurrentLeader(new FetchResponseData.LeaderIdAndEpoch().setLeaderEpoch(1).setLeaderId(2));
        }
        FetchResponseData.FetchableTopicResponse response = new FetchResponseData.FetchableTopicResponse()
                .setTopic("topic")
                .setPartitions(singletonList(partition));
        if (version > 12) {
            response.setTopicId(Uuid.randomUuid());
        }
        data.setResponses(singletonList(response));
        return new FetchResponse(data);
    }

    private HeartbeatRequest createHeartBeatRequest(short version) {
        return new HeartbeatRequest.Builder(new HeartbeatRequestData()
                .setGroupId("group1")
                .setGenerationId(1)
                .setMemberId("consumer1")).build(version);
    }

    private HeartbeatResponse createHeartBeatResponse() {
        return new HeartbeatResponse(new HeartbeatResponseData().setErrorCode(Errors.NONE.code()));
    }

    private JoinGroupRequest createJoinGroupRequest(short version) {
        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols =
            new JoinGroupRequestData.JoinGroupRequestProtocolCollection(
                Collections.singleton(
                new JoinGroupRequestData.JoinGroupRequestProtocol()
                        .setName("consumer-range")
                        .setMetadata(new byte[0])).iterator()
        );

        JoinGroupRequestData data = new JoinGroupRequestData()
            .setGroupId("group1")
            .setSessionTimeoutMs(30000)
            .setMemberId("consumer1")
            .setProtocolType("consumer")
            .setProtocols(protocols)
            .setReason("reason: test");

        // v1 and above contains rebalance timeout
        if (version >= 1)
            data.setRebalanceTimeoutMs(60000);

        // v5 and above could set group instance id
        if (version >= 5)
            data.setGroupInstanceId("groupInstanceId");

        return new JoinGroupRequest.Builder(data).build(version);
    }

    private JoinGroupResponse createJoinGroupResponse(short version) {
        List<JoinGroupResponseData.JoinGroupResponseMember> members = new ArrayList<>();

        for (int i = 0; i < 2; i++) {
            JoinGroupResponseMember member = new JoinGroupResponseData.JoinGroupResponseMember()
                .setMemberId("consumer" + i)
                .setMetadata(new byte[0]);

            if (version >= 5)
                member.setGroupInstanceId("instance" + i);

            members.add(member);
        }

        JoinGroupResponseData data = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(1)
            .setProtocolType("consumer") // Added in v7 but ignorable
            .setProtocolName("range")
            .setLeader("leader")
            .setMemberId("consumer1")
            .setMembers(members);

        // v1 and above could set throttle time
        if (version >= 1)
            data.setThrottleTimeMs(1000);

        return new JoinGroupResponse(data);
    }

    private SyncGroupRequest createSyncGroupRequest(short version) {
        List<SyncGroupRequestAssignment> assignments = singletonList(
            new SyncGroupRequestAssignment()
                .setMemberId("member")
                .setAssignment(new byte[0])
        );

        SyncGroupRequestData data = new SyncGroupRequestData()
            .setGroupId("group1")
            .setGenerationId(1)
            .setMemberId("member")
            .setProtocolType("consumer") // Added in v5 but ignorable
            .setProtocolName("range")    // Added in v5 but ignorable
            .setAssignments(assignments);

        // v3 and above could set group instance id
        if (version >= 3)
            data.setGroupInstanceId("groupInstanceId");

        return new SyncGroupRequest.Builder(data).build(version);
    }

    private SyncGroupResponse createSyncGroupResponse(short version) {
        SyncGroupResponseData data = new SyncGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setProtocolType("consumer") // Added in v5 but ignorable
            .setProtocolName("range")    // Added in v5 but ignorable
            .setAssignment(new byte[0]);

        // v1 and above could set throttle time
        if (version >= 1)
            data.setThrottleTimeMs(1000);

        return new SyncGroupResponse(data);
    }

    private ListGroupsRequest createListGroupsRequest(short version) {
        ListGroupsRequestData data = new ListGroupsRequestData();
        if (version >= 4)
            data.setStatesFilter(singletonList("Stable"));
        return new ListGroupsRequest.Builder(data).build(version);
    }

    private ListGroupsResponse createListGroupsResponse(short version) {
        ListGroupsResponseData.ListedGroup group = new ListGroupsResponseData.ListedGroup()
                .setGroupId("test-group")
                .setProtocolType("consumer");
        if (version >= 4)
            group.setGroupState("Stable");
        ListGroupsResponseData data = new ListGroupsResponseData()
                .setErrorCode(Errors.NONE.code())
                .setGroups(singletonList(group));
        return new ListGroupsResponse(data);
    }

    private DescribeGroupsRequest createDescribeGroupRequest(short version) {
        return new DescribeGroupsRequest.Builder(
            new DescribeGroupsRequestData()
                .setGroups(singletonList("test-group"))).build(version);
    }

    private DescribeGroupsResponse createDescribeGroupResponse() {
        String clientId = "consumer-1";
        String clientHost = "localhost";
        DescribeGroupsResponseData describeGroupsResponseData = new DescribeGroupsResponseData();
        DescribeGroupsResponseData.DescribedGroupMember member = DescribeGroupsResponse.groupMember("memberId", null,
                clientId, clientHost, new byte[0], new byte[0]);
        DescribedGroup metadata = DescribeGroupsResponse.groupMetadata("test-group",
                Errors.NONE,
                "STABLE",
                "consumer",
                "roundrobin",
                singletonList(member),
                DescribeGroupsResponse.AUTHORIZED_OPERATIONS_OMITTED);
        describeGroupsResponseData.groups().add(metadata);
        return new DescribeGroupsResponse(describeGroupsResponseData);
    }

    private LeaveGroupRequest createLeaveGroupRequest(short version) {
        MemberIdentity member = new MemberIdentity().setMemberId("consumer1").setReason("reason: test");
        return new LeaveGroupRequest.Builder("group1", Collections.singletonList(member))
                .build(version);
    }

    private LeaveGroupResponse createLeaveGroupResponse() {
        return new LeaveGroupResponse(new LeaveGroupResponseData().setErrorCode(Errors.NONE.code()));
    }

    private DeleteGroupsRequest createDeleteGroupsRequest(short version) {
        return new DeleteGroupsRequest.Builder(
            new DeleteGroupsRequestData()
                .setGroupsNames(singletonList("test-group"))
        ).build(version);
    }

    private DeleteGroupsResponse createDeleteGroupsResponse() {
        DeletableGroupResultCollection result = new DeletableGroupResultCollection();
        result.add(new DeletableGroupResult()
                       .setGroupId("test-group")
                       .setErrorCode(Errors.NONE.code()));
        return new DeleteGroupsResponse(
            new DeleteGroupsResponseData()
                .setResults(result)
        );
    }

    private ListOffsetsRequest createListOffsetRequest(short version) {
        if (version == 0) {
            ListOffsetsTopic topic = new ListOffsetsTopic()
                    .setName("test")
                    .setPartitions(singletonList(new ListOffsetsPartition()
                            .setPartitionIndex(0)
                            .setTimestamp(1000000L)
                            .setMaxNumOffsets(10)
                            .setCurrentLeaderEpoch(5)));
            return ListOffsetsRequest.Builder
                    .forConsumer(false, IsolationLevel.READ_UNCOMMITTED, false)
                    .setTargetTimes(singletonList(topic))
                    .build(version);
        } else if (version == 1) {
            ListOffsetsTopic topic = new ListOffsetsTopic()
                    .setName("test")
                    .setPartitions(singletonList(new ListOffsetsPartition()
                            .setPartitionIndex(0)
                            .setTimestamp(1000000L)
                            .setCurrentLeaderEpoch(5)));
            return ListOffsetsRequest.Builder
                    .forConsumer(true, IsolationLevel.READ_UNCOMMITTED, false)
                    .setTargetTimes(singletonList(topic))
                    .build(version);
        } else if (version >= 2 && version <= LIST_OFFSETS.latestVersion()) {
            ListOffsetsPartition partition = new ListOffsetsPartition()
                    .setPartitionIndex(0)
                    .setTimestamp(1000000L)
                    .setCurrentLeaderEpoch(5);

            ListOffsetsTopic topic = new ListOffsetsTopic()
                    .setName("test")
                    .setPartitions(singletonList(partition));
            return ListOffsetsRequest.Builder
                    .forConsumer(true, IsolationLevel.READ_COMMITTED, false)
                    .setTargetTimes(singletonList(topic))
                    .build(version);
        } else {
            throw new IllegalArgumentException("Illegal ListOffsetRequest version " + version);
        }
    }

    private ListOffsetsResponse createListOffsetResponse(short version) {
        if (version == 0) {
            ListOffsetsResponseData data = new ListOffsetsResponseData()
                    .setTopics(singletonList(new ListOffsetsTopicResponse()
                            .setName("test")
                            .setPartitions(singletonList(new ListOffsetsPartitionResponse()
                                    .setPartitionIndex(0)
                                    .setErrorCode(Errors.NONE.code())
                                    .setOldStyleOffsets(singletonList(100L))))));
            return new ListOffsetsResponse(data);
        } else if (version >= 1 && version <= LIST_OFFSETS.latestVersion()) {
            ListOffsetsPartitionResponse partition = new ListOffsetsPartitionResponse()
                    .setPartitionIndex(0)
                    .setErrorCode(Errors.NONE.code())
                    .setTimestamp(10000L)
                    .setOffset(100L);
            if (version >= 4) {
                partition.setLeaderEpoch(27);
            }
            ListOffsetsResponseData data = new ListOffsetsResponseData()
                    .setTopics(singletonList(new ListOffsetsTopicResponse()
                            .setName("test")
                            .setPartitions(singletonList(partition))));
            return new ListOffsetsResponse(data);
        } else {
            throw new IllegalArgumentException("Illegal ListOffsetResponse version " + version);
        }
    }

    private MetadataRequest createMetadataRequest(short version, List<String> topics) {
        return new MetadataRequest.Builder(topics, true).build(version);
    }

    private MetadataResponse createMetadataResponse() {
        Node node = new Node(1, "host1", 1001);
        List<Integer> replicas = singletonList(node.id());
        List<Integer> isr = singletonList(node.id());
        List<Integer> offlineReplicas = emptyList();

        List<MetadataResponse.TopicMetadata> allTopicMetadata = new ArrayList<>();
        allTopicMetadata.add(new MetadataResponse.TopicMetadata(Errors.NONE, "__consumer_offsets", true,
                singletonList(new MetadataResponse.PartitionMetadata(Errors.NONE,
                        new TopicPartition("__consumer_offsets", 1),
                        Optional.of(node.id()), Optional.of(5), replicas, isr, offlineReplicas))));
        allTopicMetadata.add(new MetadataResponse.TopicMetadata(Errors.LEADER_NOT_AVAILABLE, "topic2", false,
                emptyList()));
        allTopicMetadata.add(new MetadataResponse.TopicMetadata(Errors.NONE, "topic3", false,
                singletonList(new MetadataResponse.PartitionMetadata(Errors.LEADER_NOT_AVAILABLE,
                    new TopicPartition("topic3", 0), Optional.empty(),
                    Optional.empty(), replicas, isr, offlineReplicas))));

        return RequestTestUtils.metadataResponse(singletonList(node), null, MetadataResponse.NO_CONTROLLER_ID, allTopicMetadata);
    }

    private OffsetCommitRequest createOffsetCommitRequest(short version) {
        return new OffsetCommitRequest.Builder(new OffsetCommitRequestData()
                .setGroupId("group1")
                .setMemberId("consumer1")
                .setGroupInstanceId(null)
                .setGenerationId(100)
                .setTopics(singletonList(
                        new OffsetCommitRequestData.OffsetCommitRequestTopic()
                                .setName("test")
                                .setPartitions(asList(
                                        new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                                .setPartitionIndex(0)
                                                .setCommittedOffset(100)
                                                .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                                                .setCommittedMetadata(""),
                                        new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                                .setPartitionIndex(1)
                                                .setCommittedOffset(200)
                                                .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                                                .setCommittedMetadata(null)
                                ))
                ))
        ).build(version);
    }

    private OffsetCommitResponse createOffsetCommitResponse() {
        return new OffsetCommitResponse(new OffsetCommitResponseData()
                .setTopics(singletonList(
                        new OffsetCommitResponseData.OffsetCommitResponseTopic()
                                .setName("test")
                                .setPartitions(singletonList(
                                        new OffsetCommitResponseData.OffsetCommitResponsePartition()
                                                .setPartitionIndex(0)
                                                .setErrorCode(Errors.NONE.code())
                                ))
                ))
        );
    }

    private OffsetFetchRequest createOffsetFetchRequest(short version, boolean requireStable) {
        if (version < 8) {
            return new OffsetFetchRequest.Builder(
                "group1",
                requireStable,
                singletonList(new TopicPartition("test11", 1)),
                false)
                .build(version);
        }
        return new OffsetFetchRequest.Builder(
                Collections.singletonMap(
                "group1",
                singletonList(new TopicPartition("test11", 1))),
            requireStable,
            false)
            .build(version);
    }

    private OffsetFetchRequest createOffsetFetchRequestWithMultipleGroups(short version, boolean requireStable) {
        Map<String, List<TopicPartition>> groupToPartitionMap = new HashMap<>();
        List<TopicPartition> topic1 = singletonList(
            new TopicPartition("topic1", 0));
        List<TopicPartition> topic2 = asList(
            new TopicPartition("topic1", 0),
            new TopicPartition("topic2", 0),
            new TopicPartition("topic2", 1));
        List<TopicPartition> topic3 = asList(
            new TopicPartition("topic1", 0),
            new TopicPartition("topic2", 0),
            new TopicPartition("topic2", 1),
            new TopicPartition("topic3", 0),
            new TopicPartition("topic3", 1),
            new TopicPartition("topic3", 2));
        groupToPartitionMap.put("group1", topic1);
        groupToPartitionMap.put("group2", topic2);
        groupToPartitionMap.put("group3", topic3);
        groupToPartitionMap.put("group4", null);
        groupToPartitionMap.put("group5", null);

        return new OffsetFetchRequest.Builder(
            groupToPartitionMap,
            requireStable,
            false
        ).build(version);
    }

    private OffsetFetchRequest createOffsetFetchRequestForAllPartition(short version, boolean requireStable) {
        if (version < 8) {
            return new OffsetFetchRequest.Builder(
                "group1",
                requireStable,
                null,
                false)
                .build(version);
        }
        return new OffsetFetchRequest.Builder(
            Collections.singletonMap(
                "group1", null),
            requireStable,
            false)
            .build(version);
    }

    private OffsetFetchResponse createOffsetFetchResponse(short version) {
        Map<TopicPartition, OffsetFetchResponse.PartitionData> responseData = new HashMap<>();
        responseData.put(new TopicPartition("test", 0), new OffsetFetchResponse.PartitionData(
            100L, Optional.empty(), "", Errors.NONE));
        responseData.put(new TopicPartition("test", 1), new OffsetFetchResponse.PartitionData(
            100L, Optional.of(10), null, Errors.NONE));
        if (version < 8) {
            return new OffsetFetchResponse(Errors.NONE, responseData);
        }
        int throttleMs = 10;
        return new OffsetFetchResponse(throttleMs, Collections.singletonMap("group1", Errors.NONE),
            Collections.singletonMap("group1", responseData));
    }

    private ProduceRequest createProduceRequest(short version) {
        if (version < 2) {
            MemoryRecords records = MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord("blah".getBytes()));
            ProduceRequestData data = new ProduceRequestData()
                    .setAcks((short) -1)
                    .setTimeoutMs(123)
                    .setTopicData(new ProduceRequestData.TopicProduceDataCollection(singletonList(
                            new ProduceRequestData.TopicProduceData()
                                    .setName("topic1")
                                    .setPartitionData(singletonList(new ProduceRequestData.PartitionProduceData()
                                            .setIndex(1)
                                            .setRecords(records)))).iterator()));
            return new ProduceRequest.Builder(version, version, data).build(version);
        }
        byte magic = version == 2 ? RecordBatch.MAGIC_VALUE_V1 : RecordBatch.MAGIC_VALUE_V2;
        MemoryRecords records = MemoryRecords.withRecords(magic, CompressionType.NONE, new SimpleRecord("woot".getBytes()));
        return ProduceRequest.forMagic(magic,
                new ProduceRequestData()
                        .setTopicData(new ProduceRequestData.TopicProduceDataCollection(singletonList(
                                new ProduceRequestData.TopicProduceData()
                                        .setName("test")
                                        .setPartitionData(singletonList(new ProduceRequestData.PartitionProduceData()
                                                .setIndex(0)
                                                .setRecords(records)))).iterator()))
                        .setAcks((short) 1)
                        .setTimeoutMs(5000)
                        .setTransactionalId(version >= 3 ? "transactionalId" : null))
                .build(version);
    }

    @SuppressWarnings("deprecation")
    private ProduceResponse createProduceResponse() {
        Map<TopicPartition, ProduceResponse.PartitionResponse> responseData = new HashMap<>();
        responseData.put(new TopicPartition("test", 0), new ProduceResponse.PartitionResponse(Errors.NONE,
                10000, RecordBatch.NO_TIMESTAMP, 100));
        return new ProduceResponse(responseData, 0);
    }

    @SuppressWarnings("deprecation")
    private ProduceResponse createProduceResponseWithErrorMessage() {
        Map<TopicPartition, ProduceResponse.PartitionResponse> responseData = new HashMap<>();
        responseData.put(new TopicPartition("test", 0), new ProduceResponse.PartitionResponse(Errors.NONE,
                10000, RecordBatch.NO_TIMESTAMP, 100, singletonList(new ProduceResponse.RecordError(0, "error message")),
                "global error message"));
        return new ProduceResponse(responseData, 0);
    }

    private StopReplicaRequest createStopReplicaRequest(short version, boolean deletePartitions) {
        List<StopReplicaTopicState> topicStates = new ArrayList<>();
        StopReplicaTopicState topic1 = new StopReplicaTopicState()
            .setTopicName("topic1")
            .setPartitionStates(singletonList(new StopReplicaPartitionState()
                .setPartitionIndex(0)
                .setLeaderEpoch(1)
                .setDeletePartition(deletePartitions)));
        topicStates.add(topic1);
        StopReplicaTopicState topic2 = new StopReplicaTopicState()
            .setTopicName("topic2")
            .setPartitionStates(singletonList(new StopReplicaPartitionState()
                .setPartitionIndex(1)
                .setLeaderEpoch(2)
                .setDeletePartition(deletePartitions)));
        topicStates.add(topic2);

        return new StopReplicaRequest.Builder(version, 0, 1, 0,
            deletePartitions, topicStates).build(version);
    }

    private StopReplicaResponse createStopReplicaResponse() {
        List<StopReplicaResponseData.StopReplicaPartitionError> partitions = new ArrayList<>();
        partitions.add(new StopReplicaResponseData.StopReplicaPartitionError()
            .setTopicName("test")
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code()));
        return new StopReplicaResponse(new StopReplicaResponseData()
            .setErrorCode(Errors.NONE.code())
            .setPartitionErrors(partitions));
    }

    private ControlledShutdownRequest createControlledShutdownRequest(short version) {
        ControlledShutdownRequestData data = new ControlledShutdownRequestData()
                .setBrokerId(10)
                .setBrokerEpoch(0L);
        return new ControlledShutdownRequest.Builder(
                data,
                CONTROLLED_SHUTDOWN.latestVersion()).build(version);
    }

    private ControlledShutdownResponse createControlledShutdownResponse() {
        RemainingPartition p1 = new RemainingPartition()
                .setTopicName("test2")
                .setPartitionIndex(5);
        RemainingPartition p2 = new RemainingPartition()
                .setTopicName("test1")
                .setPartitionIndex(10);
        RemainingPartitionCollection pSet = new RemainingPartitionCollection();
        pSet.add(p1);
        pSet.add(p2);
        ControlledShutdownResponseData data = new ControlledShutdownResponseData()
                .setErrorCode(Errors.NONE.code())
                .setRemainingPartitions(pSet);
        return new ControlledShutdownResponse(data);
    }

    private LeaderAndIsrRequest createLeaderAndIsrRequest(short version) {
        List<LeaderAndIsrPartitionState> partitionStates = new ArrayList<>();
        List<Integer> isr = asList(1, 2);
        List<Integer> replicas = asList(1, 2, 3, 4);
        partitionStates.add(new LeaderAndIsrPartitionState()
            .setTopicName("topic5")
            .setPartitionIndex(105)
            .setControllerEpoch(0)
            .setLeader(2)
            .setLeaderEpoch(1)
            .setIsr(isr)
            .setZkVersion(2)
            .setReplicas(replicas)
            .setIsNew(false));
        partitionStates.add(new LeaderAndIsrPartitionState()
            .setTopicName("topic5")
            .setPartitionIndex(1)
            .setControllerEpoch(1)
            .setLeader(1)
            .setLeaderEpoch(1)
            .setIsr(isr)
            .setZkVersion(2)
            .setReplicas(replicas)
            .setIsNew(false));
        partitionStates.add(new LeaderAndIsrPartitionState()
            .setTopicName("topic20")
            .setPartitionIndex(1)
            .setControllerEpoch(1)
            .setLeader(0)
            .setLeaderEpoch(1)
            .setIsr(isr)
            .setZkVersion(2)
            .setReplicas(replicas)
            .setIsNew(false));

        Set<Node> leaders = Utils.mkSet(
                new Node(0, "test0", 1223),
                new Node(1, "test1", 1223)
        );

        Map<String, Uuid> topicIds = new HashMap<>();
        topicIds.put("topic5", Uuid.randomUuid());
        topicIds.put("topic20", Uuid.randomUuid());

        return new LeaderAndIsrRequest.Builder(version, 1, 10, 0,
                partitionStates, topicIds, leaders).build();
    }

    private LeaderAndIsrResponse createLeaderAndIsrResponse(short version) {
        if (version < 5) {
            List<LeaderAndIsrResponseData.LeaderAndIsrPartitionError> partitions = new ArrayList<>();
            partitions.add(new LeaderAndIsrResponseData.LeaderAndIsrPartitionError()
                    .setTopicName("test")
                    .setPartitionIndex(0)
                    .setErrorCode(Errors.NONE.code()));
            return new LeaderAndIsrResponse(new LeaderAndIsrResponseData()
                    .setErrorCode(Errors.NONE.code())
                    .setPartitionErrors(partitions), version);
        } else {
            List<LeaderAndIsrResponseData.LeaderAndIsrPartitionError> partition = singletonList(
                    new LeaderAndIsrResponseData.LeaderAndIsrPartitionError()
                    .setPartitionIndex(0)
                    .setErrorCode(Errors.NONE.code()));
            LeaderAndIsrTopicErrorCollection topics = new LeaderAndIsrTopicErrorCollection();
            topics.add(new LeaderAndIsrResponseData.LeaderAndIsrTopicError()
                    .setTopicId(Uuid.randomUuid())
                    .setPartitionErrors(partition));
            return new LeaderAndIsrResponse(new LeaderAndIsrResponseData()
                    .setTopics(topics), version);
        }
    }

    private UpdateMetadataRequest createUpdateMetadataRequest(short version, String rack) {
        List<UpdateMetadataPartitionState> partitionStates = new ArrayList<>();
        List<Integer> isr = asList(1, 2);
        List<Integer> replicas = asList(1, 2, 3, 4);
        List<Integer> offlineReplicas = emptyList();
        partitionStates.add(new UpdateMetadataPartitionState()
            .setTopicName("topic5")
            .setPartitionIndex(105)
            .setControllerEpoch(0)
            .setLeader(2)
            .setLeaderEpoch(1)
            .setIsr(isr)
            .setZkVersion(2)
            .setReplicas(replicas)
            .setOfflineReplicas(offlineReplicas));
        partitionStates.add(new UpdateMetadataPartitionState()
                .setTopicName("topic5")
                .setPartitionIndex(1)
                .setControllerEpoch(1)
                .setLeader(1)
                .setLeaderEpoch(1)
                .setIsr(isr)
                .setZkVersion(2)
                .setReplicas(replicas)
                .setOfflineReplicas(offlineReplicas));
        partitionStates.add(new UpdateMetadataPartitionState()
                .setTopicName("topic20")
                .setPartitionIndex(1)
                .setControllerEpoch(1)
                .setLeader(0)
                .setLeaderEpoch(1)
                .setIsr(isr)
                .setZkVersion(2)
                .setReplicas(replicas)
                .setOfflineReplicas(offlineReplicas));

        Map<String, Uuid> topicIds = new HashMap<>();
        if (version > 6) {
            topicIds.put("topic5", Uuid.randomUuid());
            topicIds.put("topic20", Uuid.randomUuid());
        }

        SecurityProtocol plaintext = SecurityProtocol.PLAINTEXT;
        List<UpdateMetadataEndpoint> endpoints1 = new ArrayList<>();
        endpoints1.add(new UpdateMetadataEndpoint()
            .setHost("host1")
            .setPort(1223)
            .setSecurityProtocol(plaintext.id)
            .setListener(ListenerName.forSecurityProtocol(plaintext).value()));

        List<UpdateMetadataEndpoint> endpoints2 = new ArrayList<>();
        endpoints2.add(new UpdateMetadataEndpoint()
            .setHost("host1")
            .setPort(1244)
            .setSecurityProtocol(plaintext.id)
            .setListener(ListenerName.forSecurityProtocol(plaintext).value()));
        if (version > 0) {
            SecurityProtocol ssl = SecurityProtocol.SSL;
            endpoints2.add(new UpdateMetadataEndpoint()
                .setHost("host2")
                .setPort(1234)
                .setSecurityProtocol(ssl.id)
                .setListener(ListenerName.forSecurityProtocol(ssl).value()));
            endpoints2.add(new UpdateMetadataEndpoint()
                .setHost("host2")
                .setPort(1334)
                .setSecurityProtocol(ssl.id));
            if (version >= 3)
                endpoints2.get(1).setListener("CLIENT");
        }

        List<UpdateMetadataBroker> liveBrokers = asList(
            new UpdateMetadataBroker()
                .setId(0)
                .setEndpoints(endpoints1)
                .setRack(rack),
            new UpdateMetadataBroker()
                .setId(1)
                .setEndpoints(endpoints2)
                .setRack(rack)
        );
        return new UpdateMetadataRequest.Builder(version, 1, 10, 0, partitionStates,
            liveBrokers, topicIds).build();
    }

    private UpdateMetadataResponse createUpdateMetadataResponse() {
        return new UpdateMetadataResponse(new UpdateMetadataResponseData().setErrorCode(Errors.NONE.code()));
    }

    private SaslHandshakeRequest createSaslHandshakeRequest(short version) {
        return new SaslHandshakeRequest.Builder(
                new SaslHandshakeRequestData().setMechanism("PLAIN")).build(version);
    }

    private SaslHandshakeResponse createSaslHandshakeResponse() {
        return new SaslHandshakeResponse(
                new SaslHandshakeResponseData()
                    .setErrorCode(Errors.NONE.code()).setMechanisms(singletonList("GSSAPI")));
    }

    private SaslAuthenticateRequest createSaslAuthenticateRequest(short version) {
        SaslAuthenticateRequestData data = new SaslAuthenticateRequestData().setAuthBytes(new byte[0]);
        return new SaslAuthenticateRequest(data, version);
    }

    private SaslAuthenticateResponse createSaslAuthenticateResponse() {
        SaslAuthenticateResponseData data = new SaslAuthenticateResponseData()
                .setErrorCode(Errors.NONE.code())
                .setAuthBytes(new byte[0])
                .setSessionLifetimeMs(Long.MAX_VALUE);
        return new SaslAuthenticateResponse(data);
    }

    private ApiVersionsRequest createApiVersionRequest(short version) {
        return new ApiVersionsRequest.Builder().build(version);
    }

    private ApiVersionsResponse createApiVersionResponse() {
        ApiVersionCollection apiVersions = new ApiVersionCollection();
        apiVersions.add(new ApiVersion()
            .setApiKey((short) 0)
            .setMinVersion((short) 0)
            .setMaxVersion((short) 2));

        return new ApiVersionsResponse(new ApiVersionsResponseData()
            .setErrorCode(Errors.NONE.code())
            .setThrottleTimeMs(0)
            .setApiKeys(apiVersions));
    }

    private CreateTopicsRequest createCreateTopicRequest(short version) {
        return createCreateTopicRequest(version, version >= 1);
    }

    private CreateTopicsRequest createCreateTopicRequest(short version, boolean validateOnly) {
        CreateTopicsRequestData data = new CreateTopicsRequestData()
            .setTimeoutMs(123)
            .setValidateOnly(validateOnly);
        data.topics().add(new CreatableTopic()
            .setNumPartitions(3)
            .setReplicationFactor((short) 5));

        CreatableTopic topic2 = new CreatableTopic();
        data.topics().add(topic2);
        topic2.assignments().add(new CreatableReplicaAssignment()
            .setPartitionIndex(0)
            .setBrokerIds(asList(1, 2, 3)));
        topic2.assignments().add(new CreatableReplicaAssignment()
            .setPartitionIndex(1)
            .setBrokerIds(asList(2, 3, 4)));
        topic2.configs().add(new CreateableTopicConfig()
            .setName("config1").setValue("value1"));

        return new CreateTopicsRequest.Builder(data).build(version);
    }

    private CreateTopicsResponse createCreateTopicResponse() {
        CreateTopicsResponseData data = new CreateTopicsResponseData();
        data.topics().add(new CreatableTopicResult()
            .setName("t1")
            .setErrorCode(Errors.INVALID_TOPIC_EXCEPTION.code())
            .setErrorMessage(null));
        data.topics().add(new CreatableTopicResult()
            .setName("t2")
            .setErrorCode(Errors.LEADER_NOT_AVAILABLE.code())
            .setErrorMessage("Leader with id 5 is not available."));
        data.topics().add(new CreatableTopicResult()
            .setName("t3")
            .setErrorCode(Errors.NONE.code())
            .setNumPartitions(1)
            .setReplicationFactor((short) 2)
            .setConfigs(singletonList(new CreatableTopicConfigs()
                .setName("min.insync.replicas")
                .setValue("2"))));
        return new CreateTopicsResponse(data);
    }

    private DeleteTopicsRequest createDeleteTopicsRequest(short version) {
        return new DeleteTopicsRequest.Builder(new DeleteTopicsRequestData()
            .setTopicNames(asList("my_t1", "my_t2"))
            .setTimeoutMs(1000)
        ).build(version);
    }

    private DeleteTopicsResponse createDeleteTopicsResponse() {
        DeleteTopicsResponseData data = new DeleteTopicsResponseData();
        data.responses().add(new DeletableTopicResult()
            .setName("t1")
            .setErrorCode(Errors.INVALID_TOPIC_EXCEPTION.code())
            .setErrorMessage("Error Message"));
        data.responses().add(new DeletableTopicResult()
            .setName("t2")
            .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code())
            .setErrorMessage("Error Message"));
        data.responses().add(new DeletableTopicResult()
            .setName("t3")
            .setErrorCode(Errors.NOT_CONTROLLER.code()));
        data.responses().add(new DeletableTopicResult()
                .setName("t4")
                .setErrorCode(Errors.NONE.code()));
        return new DeleteTopicsResponse(data);
    }

    private InitProducerIdRequest createInitPidRequest(short version) {
        InitProducerIdRequestData requestData = new InitProducerIdRequestData()
                .setTransactionalId(null)
                .setTransactionTimeoutMs(100);
        return new InitProducerIdRequest.Builder(requestData).build(version);
    }

    private InitProducerIdResponse createInitPidResponse() {
        InitProducerIdResponseData responseData = new InitProducerIdResponseData()
                .setErrorCode(Errors.NONE.code())
                .setProducerEpoch((short) 3)
                .setProducerId(3332)
                .setThrottleTimeMs(0);
        return new InitProducerIdResponse(responseData);
    }

    private OffsetForLeaderTopicCollection createOffsetForLeaderTopicCollection() {
        OffsetForLeaderTopicCollection topics = new OffsetForLeaderTopicCollection();
        topics.add(new OffsetForLeaderTopic()
            .setTopic("topic1")
            .setPartitions(asList(
                new OffsetForLeaderPartition()
                    .setPartition(0)
                    .setLeaderEpoch(1)
                    .setCurrentLeaderEpoch(0),
                new OffsetForLeaderPartition()
                    .setPartition(1)
                    .setLeaderEpoch(1)
                    .setCurrentLeaderEpoch(0))));
        topics.add(new OffsetForLeaderTopic()
            .setTopic("topic2")
            .setPartitions(singletonList(
                new OffsetForLeaderPartition()
                    .setPartition(2)
                    .setLeaderEpoch(3)
                    .setCurrentLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH))));
        return topics;
    }

    private OffsetsForLeaderEpochRequest createLeaderEpochRequestForConsumer() {
        OffsetForLeaderTopicCollection epochs = createOffsetForLeaderTopicCollection();
        return OffsetsForLeaderEpochRequest.Builder.forConsumer(epochs).build();
    }

    private OffsetsForLeaderEpochRequest createLeaderEpochRequestForReplica(short version, int replicaId) {
        OffsetForLeaderTopicCollection epochs = createOffsetForLeaderTopicCollection();
        return OffsetsForLeaderEpochRequest.Builder.forFollower(version, epochs, replicaId).build();
    }

    private OffsetsForLeaderEpochResponse createLeaderEpochResponse() {
        OffsetForLeaderEpochResponseData data = new OffsetForLeaderEpochResponseData();
        data.topics().add(new OffsetForLeaderTopicResult()
            .setTopic("topic1")
            .setPartitions(asList(
                new EpochEndOffset()
                    .setPartition(0)
                    .setErrorCode(Errors.NONE.code())
                    .setLeaderEpoch(1)
                    .setEndOffset(0),
                new EpochEndOffset()
                    .setPartition(1)
                    .setErrorCode(Errors.NONE.code())
                    .setLeaderEpoch(1)
                    .setEndOffset(1))));
        data.topics().add(new OffsetForLeaderTopicResult()
            .setTopic("topic2")
            .setPartitions(singletonList(
                new EpochEndOffset()
                    .setPartition(2)
                    .setErrorCode(Errors.NONE.code())
                    .setLeaderEpoch(1)
                    .setEndOffset(1))));

        return new OffsetsForLeaderEpochResponse(data);
    }

    private AddPartitionsToTxnRequest createAddPartitionsToTxnRequest(short version) {
        return new AddPartitionsToTxnRequest.Builder("tid", 21L, (short) 42,
            singletonList(new TopicPartition("topic", 73))).build(version);
    }

    private AddPartitionsToTxnResponse createAddPartitionsToTxnResponse() {
        return new AddPartitionsToTxnResponse(0, Collections.singletonMap(new TopicPartition("t", 0), Errors.NONE));
    }

    private AddOffsetsToTxnRequest createAddOffsetsToTxnRequest(short version) {
        return new AddOffsetsToTxnRequest.Builder(
            new AddOffsetsToTxnRequestData()
                .setTransactionalId("tid")
                .setProducerId(21L)
                .setProducerEpoch((short) 42)
                .setGroupId("gid")
        ).build(version);
    }

    private AddOffsetsToTxnResponse createAddOffsetsToTxnResponse() {
        return new AddOffsetsToTxnResponse(new AddOffsetsToTxnResponseData()
                                               .setErrorCode(Errors.NONE.code())
                                               .setThrottleTimeMs(0));
    }

    private EndTxnRequest createEndTxnRequest(short version) {
        return new EndTxnRequest.Builder(
            new EndTxnRequestData()
                .setTransactionalId("tid")
                .setProducerId(21L)
                .setProducerEpoch((short) 42)
                .setCommitted(TransactionResult.COMMIT.id)
            ).build(version);
    }

    private EndTxnResponse createEndTxnResponse() {
        return new EndTxnResponse(
            new EndTxnResponseData()
                .setErrorCode(Errors.NONE.code())
                .setThrottleTimeMs(0)
        );
    }

    private WriteTxnMarkersRequest createWriteTxnMarkersRequest(short version) {
        List<TopicPartition> partitions = singletonList(new TopicPartition("topic", 73));
        WriteTxnMarkersRequest.TxnMarkerEntry txnMarkerEntry = new WriteTxnMarkersRequest.TxnMarkerEntry(21L, (short) 42, 73, TransactionResult.ABORT, partitions);
        return new WriteTxnMarkersRequest.Builder(WRITE_TXN_MARKERS.latestVersion(), singletonList(txnMarkerEntry)).build(version);
    }

    private WriteTxnMarkersResponse createWriteTxnMarkersResponse() {
        final Map<TopicPartition, Errors> errorPerPartitions = new HashMap<>();
        errorPerPartitions.put(new TopicPartition("topic", 73), Errors.NONE);
        final Map<Long, Map<TopicPartition, Errors>> response = new HashMap<>();
        response.put(21L, errorPerPartitions);
        return new WriteTxnMarkersResponse(response);
    }

    private TxnOffsetCommitRequest createTxnOffsetCommitRequest(short version) {
        final Map<TopicPartition, TxnOffsetCommitRequest.CommittedOffset> offsets = new HashMap<>();
        offsets.put(new TopicPartition("topic", 73),
                    new TxnOffsetCommitRequest.CommittedOffset(100, null, Optional.empty()));
        offsets.put(new TopicPartition("topic", 74),
                new TxnOffsetCommitRequest.CommittedOffset(100, "blah", Optional.of(27)));

        if (version < 3) {
            return new TxnOffsetCommitRequest.Builder("transactionalId",
                "groupId",
                21L,
                (short) 42,
                offsets).build();
        } else {
            return new TxnOffsetCommitRequest.Builder("transactionalId",
                "groupId",
                21L,
                (short) 42,
                offsets,
                "member",
                2,
                Optional.of("instance")).build(version);
        }
    }

    private TxnOffsetCommitRequest createTxnOffsetCommitRequestWithAutoDowngrade() {
        final Map<TopicPartition, TxnOffsetCommitRequest.CommittedOffset> offsets = new HashMap<>();
        offsets.put(new TopicPartition("topic", 73),
            new TxnOffsetCommitRequest.CommittedOffset(100, null, Optional.empty()));
        offsets.put(new TopicPartition("topic", 74),
            new TxnOffsetCommitRequest.CommittedOffset(100, "blah", Optional.of(27)));

        return new TxnOffsetCommitRequest.Builder("transactionalId",
            "groupId",
            21L,
            (short) 42,
            offsets,
            "member",
            2,
            Optional.of("instance")).build();
    }

    private TxnOffsetCommitResponse createTxnOffsetCommitResponse() {
        final Map<TopicPartition, Errors> errorPerPartitions = new HashMap<>();
        errorPerPartitions.put(new TopicPartition("topic", 73), Errors.NONE);
        return new TxnOffsetCommitResponse(0, errorPerPartitions);
    }

    private DescribeAclsRequest createDescribeAclsRequest(short version) {
        return new DescribeAclsRequest.Builder(new AclBindingFilter(
                new ResourcePatternFilter(ResourceType.TOPIC, "mytopic", PatternType.LITERAL),
                new AccessControlEntryFilter(null, null, AclOperation.ANY, AclPermissionType.ANY))).build(version);
    }

    private DescribeAclsResponse createDescribeAclsResponse() {
        DescribeAclsResponseData data = new DescribeAclsResponseData()
                .setErrorCode(Errors.NONE.code())
                .setErrorMessage(Errors.NONE.message())
                .setThrottleTimeMs(0)
                .setResources(singletonList(new DescribeAclsResource()
                        .setResourceType(ResourceType.TOPIC.code())
                        .setResourceName("mytopic")
                        .setPatternType(PatternType.LITERAL.code())
                        .setAcls(singletonList(new AclDescription()
                                .setHost("*")
                                .setOperation(AclOperation.WRITE.code())
                                .setPermissionType(AclPermissionType.ALLOW.code())
                                .setPrincipal("User:ANONYMOUS")))));
        return new DescribeAclsResponse(data);
    }

    private CreateAclsRequest createCreateAclsRequest(short version) {
        List<CreateAclsRequestData.AclCreation> creations = new ArrayList<>();
        creations.add(CreateAclsRequest.aclCreation(new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, "mytopic", PatternType.LITERAL),
            new AccessControlEntry("User:ANONYMOUS", "127.0.0.1", AclOperation.READ, AclPermissionType.ALLOW))));
        creations.add(CreateAclsRequest.aclCreation(new AclBinding(
            new ResourcePattern(ResourceType.GROUP, "mygroup", PatternType.LITERAL),
            new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.WRITE, AclPermissionType.DENY))));
        CreateAclsRequestData data = new CreateAclsRequestData().setCreations(creations);
        return new CreateAclsRequest.Builder(data).build(version);
    }

    private CreateAclsResponse createCreateAclsResponse() {
        return new CreateAclsResponse(new CreateAclsResponseData().setResults(asList(
            new CreateAclsResponseData.AclCreationResult(),
            new CreateAclsResponseData.AclCreationResult()
                .setErrorCode(Errors.NONE.code())
                .setErrorMessage("Foo bar"))));
    }

    private DeleteAclsRequest createDeleteAclsRequest(short version) {
        DeleteAclsRequestData data = new DeleteAclsRequestData().setFilters(asList(
            new DeleteAclsRequestData.DeleteAclsFilter()
                .setResourceTypeFilter(ResourceType.ANY.code())
                .setResourceNameFilter(null)
                .setPatternTypeFilter(PatternType.LITERAL.code())
                .setPrincipalFilter("User:ANONYMOUS")
                .setHostFilter(null)
                .setOperation(AclOperation.ANY.code())
                .setPermissionType(AclPermissionType.ANY.code()),
            new DeleteAclsRequestData.DeleteAclsFilter()
                .setResourceTypeFilter(ResourceType.ANY.code())
                .setResourceNameFilter(null)
                .setPatternTypeFilter(PatternType.LITERAL.code())
                .setPrincipalFilter("User:bob")
                .setHostFilter(null)
                .setOperation(AclOperation.ANY.code())
                .setPermissionType(AclPermissionType.ANY.code())
        ));
        return new DeleteAclsRequest.Builder(data).build(version);
    }

    private DeleteAclsResponse createDeleteAclsResponse(short version) {
        List<DeleteAclsResponseData.DeleteAclsFilterResult> filterResults = new ArrayList<>();
        filterResults.add(new DeleteAclsResponseData.DeleteAclsFilterResult().setMatchingAcls(asList(
                new DeleteAclsResponseData.DeleteAclsMatchingAcl()
                    .setResourceType(ResourceType.TOPIC.code())
                    .setResourceName("mytopic3")
                    .setPatternType(PatternType.LITERAL.code())
                    .setPrincipal("User:ANONYMOUS")
                    .setHost("*")
                    .setOperation(AclOperation.DESCRIBE.code())
                    .setPermissionType(AclPermissionType.ALLOW.code()),
                new DeleteAclsResponseData.DeleteAclsMatchingAcl()
                    .setResourceType(ResourceType.TOPIC.code())
                    .setResourceName("mytopic4")
                    .setPatternType(PatternType.LITERAL.code())
                    .setPrincipal("User:ANONYMOUS")
                    .setHost("*")
                    .setOperation(AclOperation.DESCRIBE.code())
                    .setPermissionType(AclPermissionType.DENY.code()))));
        filterResults.add(new DeleteAclsResponseData.DeleteAclsFilterResult()
            .setErrorCode(Errors.SECURITY_DISABLED.code())
            .setErrorMessage("No security"));
        return new DeleteAclsResponse(new DeleteAclsResponseData()
            .setThrottleTimeMs(0)
            .setFilterResults(filterResults), version);
    }

    private DescribeConfigsRequest createDescribeConfigsRequest(short version) {
        return new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData()
                .setResources(asList(
                        new DescribeConfigsRequestData.DescribeConfigsResource()
                                .setResourceType(ConfigResource.Type.BROKER.id())
                                .setResourceName("0"),
                        new DescribeConfigsRequestData.DescribeConfigsResource()
                                .setResourceType(ConfigResource.Type.TOPIC.id())
                                .setResourceName("topic"))))
                .build(version);
    }

    private DescribeConfigsRequest createDescribeConfigsRequestWithConfigEntries(short version) {
        return new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData()
            .setResources(asList(
                new DescribeConfigsRequestData.DescribeConfigsResource()
                    .setResourceType(ConfigResource.Type.BROKER.id())
                    .setResourceName("0")
                    .setConfigurationKeys(asList("foo", "bar")),
                new DescribeConfigsRequestData.DescribeConfigsResource()
                    .setResourceType(ConfigResource.Type.TOPIC.id())
                    .setResourceName("topic")
                    .setConfigurationKeys(null),
                new DescribeConfigsRequestData.DescribeConfigsResource()
                    .setResourceType(ConfigResource.Type.TOPIC.id())
                    .setResourceName("topic a")
                    .setConfigurationKeys(emptyList())))).build(version);
    }

    private DescribeConfigsRequest createDescribeConfigsRequestWithDocumentation(short version) {
        DescribeConfigsRequestData data = new DescribeConfigsRequestData()
                .setResources(singletonList(
                        new DescribeConfigsRequestData.DescribeConfigsResource()
                                .setResourceType(ConfigResource.Type.BROKER.id())
                                .setResourceName("0")
                                .setConfigurationKeys(asList("foo", "bar"))));
        if (version == 3) {
            data.setIncludeDocumentation(true);
        }
        return new DescribeConfigsRequest.Builder(data).build(version);
    }

    private DescribeConfigsResponse createDescribeConfigsResponse(short version) {
        return new DescribeConfigsResponse(new DescribeConfigsResponseData().setResults(asList(
                new DescribeConfigsResult()
                        .setErrorCode(Errors.NONE.code())
                        .setResourceType(ConfigResource.Type.BROKER.id())
                        .setResourceName("0")
                        .setConfigs(asList(
                                new DescribeConfigsResourceResult()
                                        .setName("config_name")
                                        .setValue("config_value")
                                        // Note: the v0 default for this field that should be exposed to callers is
                                        // context-dependent. For example, if the resource is a broker, this should default to 4.
                                        // -1 is just a placeholder value.
                                        .setConfigSource(version == 0 ? DescribeConfigsResponse.ConfigSource.STATIC_BROKER_CONFIG.id() : DescribeConfigsResponse.ConfigSource.DYNAMIC_BROKER_CONFIG.id)
                                        .setIsSensitive(true).setReadOnly(false)
                                        .setSynonyms(emptyList()),
                                new DescribeConfigsResourceResult()
                                        .setName("yet_another_name")
                                        .setValue("yet another value")
                                        .setConfigSource(version == 0 ? DescribeConfigsResponse.ConfigSource.STATIC_BROKER_CONFIG.id() : DescribeConfigsResponse.ConfigSource.DEFAULT_CONFIG.id)
                                        .setIsSensitive(false).setReadOnly(true)
                                        .setSynonyms(emptyList())
                                        .setConfigType(ConfigType.BOOLEAN.id())
                                        .setDocumentation("some description"),
                                new DescribeConfigsResourceResult()
                                        .setName("another_name")
                                        .setValue("another value")
                                        .setConfigSource(version == 0 ? DescribeConfigsResponse.ConfigSource.STATIC_BROKER_CONFIG.id() : DescribeConfigsResponse.ConfigSource.DEFAULT_CONFIG.id)
                                        .setIsSensitive(false).setReadOnly(true)
                                        .setSynonyms(emptyList())
                        )),
                new DescribeConfigsResult()
                        .setErrorCode(Errors.NONE.code())
                        .setResourceType(ConfigResource.Type.TOPIC.id())
                        .setResourceName("topic")
                        .setConfigs(emptyList())
        )));

    }

    private AlterConfigsRequest createAlterConfigsRequest(short version) {
        Map<ConfigResource, AlterConfigsRequest.Config> configs = new HashMap<>();
        List<AlterConfigsRequest.ConfigEntry> configEntries = asList(
                new AlterConfigsRequest.ConfigEntry("config_name", "config_value"),
                new AlterConfigsRequest.ConfigEntry("another_name", "another value")
        );
        configs.put(new ConfigResource(ConfigResource.Type.BROKER, "0"), new AlterConfigsRequest.Config(configEntries));
        configs.put(new ConfigResource(ConfigResource.Type.TOPIC, "topic"),
                new AlterConfigsRequest.Config(emptyList()));
        return new AlterConfigsRequest.Builder(configs, false).build(version);
    }

    private AlterConfigsResponse createAlterConfigsResponse() {
        AlterConfigsResponseData data = new AlterConfigsResponseData()
                .setThrottleTimeMs(20);
        data.responses().add(new AlterConfigsResponseData.AlterConfigsResourceResponse()
                .setErrorCode(Errors.NONE.code())
                .setErrorMessage(null)
                .setResourceName("0")
                .setResourceType(ConfigResource.Type.BROKER.id()));
        data.responses().add(new AlterConfigsResponseData.AlterConfigsResourceResponse()
                .setErrorCode(Errors.INVALID_REQUEST.code())
                .setErrorMessage("This request is invalid")
                .setResourceName("topic")
                .setResourceType(ConfigResource.Type.TOPIC.id()));
        return new AlterConfigsResponse(data);
    }

    private CreatePartitionsRequest createCreatePartitionsRequest(short version) {
        CreatePartitionsTopicCollection topics = new CreatePartitionsTopicCollection();
        topics.add(new CreatePartitionsTopic()
                .setName("my_topic")
                .setCount(3)
        );
        topics.add(new CreatePartitionsTopic()
                .setName("my_other_topic")
                .setCount(3)
        );

        CreatePartitionsRequestData data = new CreatePartitionsRequestData()
                .setTimeoutMs(0)
                .setValidateOnly(false)
                .setTopics(topics);

        return new CreatePartitionsRequest(data, version);
    }

    private CreatePartitionsRequest createCreatePartitionsRequestWithAssignments(short version) {
        CreatePartitionsTopicCollection topics = new CreatePartitionsTopicCollection();
        CreatePartitionsAssignment myTopicAssignment = new CreatePartitionsAssignment()
                .setBrokerIds(singletonList(2));
        topics.add(new CreatePartitionsTopic()
                .setName("my_topic")
                .setCount(3)
                .setAssignments(singletonList(myTopicAssignment))
        );

        topics.add(new CreatePartitionsTopic()
                .setName("my_other_topic")
                .setCount(3)
                .setAssignments(asList(
                    new CreatePartitionsAssignment().setBrokerIds(asList(2, 3)),
                    new CreatePartitionsAssignment().setBrokerIds(asList(3, 1))
                ))
        );

        CreatePartitionsRequestData data = new CreatePartitionsRequestData()
                .setTimeoutMs(0)
                .setValidateOnly(false)
                .setTopics(topics);

        return new CreatePartitionsRequest(data, version);
    }

    private CreatePartitionsResponse createCreatePartitionsResponse() {
        List<CreatePartitionsTopicResult> results = new LinkedList<>();
        results.add(new CreatePartitionsTopicResult()
                .setName("my_topic")
                .setErrorCode(Errors.INVALID_REPLICA_ASSIGNMENT.code()));
        results.add(new CreatePartitionsTopicResult()
                .setName("my_topic")
                .setErrorCode(Errors.NONE.code()));
        CreatePartitionsResponseData data = new CreatePartitionsResponseData()
                .setThrottleTimeMs(42)
                .setResults(results);
        return new CreatePartitionsResponse(data);
    }

    private CreateDelegationTokenRequest createCreateTokenRequest(short version) {
        List<CreatableRenewers> renewers = new ArrayList<>();
        renewers.add(new CreatableRenewers()
                .setPrincipalType("User")
                .setPrincipalName("user1"));
        renewers.add(new CreatableRenewers()
                .setPrincipalType("User")
                .setPrincipalName("user2"));
        return new CreateDelegationTokenRequest.Builder(new CreateDelegationTokenRequestData()
                .setRenewers(renewers)
                .setMaxLifetimeMs(System.currentTimeMillis())).build(version);
    }

    private CreateDelegationTokenResponse createCreateTokenResponse() {
        CreateDelegationTokenResponseData data = new CreateDelegationTokenResponseData()
                .setThrottleTimeMs(20)
                .setErrorCode(Errors.NONE.code())
                .setPrincipalType("User")
                .setPrincipalName("user1")
                .setIssueTimestampMs(System.currentTimeMillis())
                .setExpiryTimestampMs(System.currentTimeMillis())
                .setMaxTimestampMs(System.currentTimeMillis())
                .setTokenId("token1")
                .setHmac("test".getBytes());
        return new CreateDelegationTokenResponse(data);
    }

    private RenewDelegationTokenRequest createRenewTokenRequest(short version) {
        RenewDelegationTokenRequestData data = new RenewDelegationTokenRequestData()
                .setHmac("test".getBytes())
                .setRenewPeriodMs(System.currentTimeMillis());
        return new RenewDelegationTokenRequest.Builder(data).build(version);
    }

    private RenewDelegationTokenResponse createRenewTokenResponse() {
        RenewDelegationTokenResponseData data = new RenewDelegationTokenResponseData()
                .setThrottleTimeMs(20)
                .setErrorCode(Errors.NONE.code())
                .setExpiryTimestampMs(System.currentTimeMillis());
        return new RenewDelegationTokenResponse(data);
    }

    private ExpireDelegationTokenRequest createExpireTokenRequest(short version) {
        ExpireDelegationTokenRequestData data = new ExpireDelegationTokenRequestData()
                .setHmac("test".getBytes())
                .setExpiryTimePeriodMs(System.currentTimeMillis());
        return new ExpireDelegationTokenRequest.Builder(data).build(version);
    }

    private ExpireDelegationTokenResponse createExpireTokenResponse() {
        ExpireDelegationTokenResponseData data = new ExpireDelegationTokenResponseData()
                .setThrottleTimeMs(20)
                .setErrorCode(Errors.NONE.code())
                .setExpiryTimestampMs(System.currentTimeMillis());
        return new ExpireDelegationTokenResponse(data);
    }

    private DescribeDelegationTokenRequest createDescribeTokenRequest(short version) {
        List<KafkaPrincipal> owners = new ArrayList<>();
        owners.add(SecurityUtils.parseKafkaPrincipal("User:user1"));
        owners.add(SecurityUtils.parseKafkaPrincipal("User:user2"));
        return new DescribeDelegationTokenRequest.Builder(owners).build(version);
    }

    private DescribeDelegationTokenResponse createDescribeTokenResponse() {
        List<KafkaPrincipal> renewers = new ArrayList<>();
        renewers.add(SecurityUtils.parseKafkaPrincipal("User:user1"));
        renewers.add(SecurityUtils.parseKafkaPrincipal("User:user2"));

        List<DelegationToken> tokenList = new LinkedList<>();

        TokenInformation tokenInfo1 = new TokenInformation("1", SecurityUtils.parseKafkaPrincipal("User:owner"), renewers,
            System.currentTimeMillis(), System.currentTimeMillis(), System.currentTimeMillis());

        TokenInformation tokenInfo2 = new TokenInformation("2", SecurityUtils.parseKafkaPrincipal("User:owner1"), renewers,
            System.currentTimeMillis(), System.currentTimeMillis(), System.currentTimeMillis());

        tokenList.add(new DelegationToken(tokenInfo1, "test".getBytes()));
        tokenList.add(new DelegationToken(tokenInfo2, "test".getBytes()));

        return new DescribeDelegationTokenResponse(20, Errors.NONE, tokenList);
    }

    private ElectLeadersRequest createElectLeadersRequestNullPartitions() {
        return new ElectLeadersRequest.Builder(ElectionType.PREFERRED, null, 100).build((short) 1);
    }

    private ElectLeadersRequest createElectLeadersRequest(short version) {
        List<TopicPartition> partitions = asList(new TopicPartition("data", 1), new TopicPartition("data", 2));

        return new ElectLeadersRequest.Builder(ElectionType.PREFERRED, partitions, 100).build(version);
    }

    private ElectLeadersResponse createElectLeadersResponse() {
        String topic = "myTopic";
        List<ReplicaElectionResult> electionResults = new ArrayList<>();
        ReplicaElectionResult electionResult = new ReplicaElectionResult();
        electionResults.add(electionResult);
        electionResult.setTopic(topic);
        // Add partition 1 result
        PartitionResult partitionResult = new PartitionResult();
        partitionResult.setPartitionId(0);
        partitionResult.setErrorCode(ApiError.NONE.error().code());
        partitionResult.setErrorMessage(ApiError.NONE.message());
        electionResult.partitionResult().add(partitionResult);

        // Add partition 2 result
        partitionResult = new PartitionResult();
        partitionResult.setPartitionId(1);
        partitionResult.setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
        partitionResult.setErrorMessage(Errors.UNKNOWN_TOPIC_OR_PARTITION.message());
        electionResult.partitionResult().add(partitionResult);

        return new ElectLeadersResponse(200, Errors.NONE.code(), electionResults, ELECT_LEADERS.latestVersion());
    }

    private IncrementalAlterConfigsRequest createIncrementalAlterConfigsRequest(short version) {
        IncrementalAlterConfigsRequestData data = new IncrementalAlterConfigsRequestData();
        AlterableConfig alterableConfig = new AlterableConfig()
                .setName("retention.ms")
                .setConfigOperation((byte) 0)
                .setValue("100");
        IncrementalAlterConfigsRequestData.AlterableConfigCollection alterableConfigs = new IncrementalAlterConfigsRequestData.AlterableConfigCollection();
        alterableConfigs.add(alterableConfig);

        data.resources().add(new AlterConfigsResource()
                .setResourceName("testtopic")
                .setResourceType(ResourceType.TOPIC.code())
                .setConfigs(alterableConfigs));
        return new IncrementalAlterConfigsRequest.Builder(data).build(version);
    }

    private IncrementalAlterConfigsResponse createIncrementalAlterConfigsResponse() {
        IncrementalAlterConfigsResponseData data = new IncrementalAlterConfigsResponseData();

        data.responses().add(new AlterConfigsResourceResponse()
                .setResourceName("testtopic")
                .setResourceType(ResourceType.TOPIC.code())
                .setErrorCode(Errors.NONE.code())
                .setErrorMessage("Duplicate Keys"));
        return new IncrementalAlterConfigsResponse(data);
    }

    private AlterPartitionReassignmentsRequest createAlterPartitionReassignmentsRequest(short version) {
        AlterPartitionReassignmentsRequestData data = new AlterPartitionReassignmentsRequestData();
        data.topics().add(
                new AlterPartitionReassignmentsRequestData.ReassignableTopic().setName("topic").setPartitions(
                        singletonList(
                                new AlterPartitionReassignmentsRequestData.ReassignablePartition().setPartitionIndex(0).setReplicas(null)
                        )
                )
        );
        return new AlterPartitionReassignmentsRequest.Builder(data).build(version);
    }

    private AlterPartitionReassignmentsResponse createAlterPartitionReassignmentsResponse() {
        AlterPartitionReassignmentsResponseData data = new AlterPartitionReassignmentsResponseData();
        data.responses().add(
                new AlterPartitionReassignmentsResponseData.ReassignableTopicResponse()
                        .setName("topic")
                        .setPartitions(singletonList(
                                new AlterPartitionReassignmentsResponseData.ReassignablePartitionResponse()
                                        .setPartitionIndex(0)
                                        .setErrorCode(Errors.NONE.code())
                                        .setErrorMessage("No reassignment is in progress for topic topic partition 0")
                                )
                        )
        );
        return new AlterPartitionReassignmentsResponse(data);
    }

    private ListPartitionReassignmentsRequest createListPartitionReassignmentsRequest(short version) {
        ListPartitionReassignmentsRequestData data = new ListPartitionReassignmentsRequestData();
        data.setTopics(
            singletonList(
                new ListPartitionReassignmentsRequestData.ListPartitionReassignmentsTopics()
                    .setName("topic")
                    .setPartitionIndexes(singletonList(1))
            )
        );
        return new ListPartitionReassignmentsRequest.Builder(data).build(version);
    }

    private ListPartitionReassignmentsResponse createListPartitionReassignmentsResponse() {
        ListPartitionReassignmentsResponseData data = new ListPartitionReassignmentsResponseData();
        data.setTopics(singletonList(
            new ListPartitionReassignmentsResponseData.OngoingTopicReassignment()
                        .setName("topic")
                        .setPartitions(singletonList(
                                new ListPartitionReassignmentsResponseData.OngoingPartitionReassignment()
                                        .setPartitionIndex(0)
                                        .setReplicas(asList(1, 2))
                                        .setAddingReplicas(singletonList(2))
                                        .setRemovingReplicas(singletonList(1))
                                )
                        )
        ));
        return new ListPartitionReassignmentsResponse(data);
    }

    private OffsetDeleteRequest createOffsetDeleteRequest(short version) {
        OffsetDeleteRequestTopicCollection topics = new OffsetDeleteRequestTopicCollection();
        topics.add(new OffsetDeleteRequestTopic()
            .setName("topic1")
            .setPartitions(singletonList(
                new OffsetDeleteRequestPartition()
                    .setPartitionIndex(0)
                )
            )
        );

        OffsetDeleteRequestData data = new OffsetDeleteRequestData();
        data.setGroupId("group1");
        data.setTopics(topics);

        return new OffsetDeleteRequest.Builder(data).build(version);
    }

    private OffsetDeleteResponse createOffsetDeleteResponse() {
        OffsetDeleteResponsePartitionCollection partitions = new OffsetDeleteResponsePartitionCollection();
        partitions.add(new OffsetDeleteResponsePartition()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code())
        );

        OffsetDeleteResponseTopicCollection topics = new OffsetDeleteResponseTopicCollection();
        topics.add(new OffsetDeleteResponseTopic()
            .setName("topic1")
            .setPartitions(partitions)
        );

        OffsetDeleteResponseData data = new OffsetDeleteResponseData();
        data.setErrorCode(Errors.NONE.code());
        data.setTopics(topics);

        return new OffsetDeleteResponse(data);
    }

    private AlterReplicaLogDirsRequest createAlterReplicaLogDirsRequest(short version) {
        AlterReplicaLogDirsRequestData data = new AlterReplicaLogDirsRequestData();
        data.dirs().add(
                new AlterReplicaLogDirsRequestData.AlterReplicaLogDir()
                        .setPath("/data0")
                        .setTopics(new AlterReplicaLogDirTopicCollection(singletonList(
                                new AlterReplicaLogDirTopic()
                                        .setPartitions(singletonList(0))
                                        .setName("topic")
                        ).iterator())
                )
        );
        return new AlterReplicaLogDirsRequest.Builder(data).build(version);
    }

    private AlterReplicaLogDirsResponse createAlterReplicaLogDirsResponse() {
        AlterReplicaLogDirsResponseData data = new AlterReplicaLogDirsResponseData();
        data.results().add(
                new AlterReplicaLogDirsResponseData.AlterReplicaLogDirTopicResult()
                        .setTopicName("topic")
                        .setPartitions(singletonList(
                                new AlterReplicaLogDirsResponseData.AlterReplicaLogDirPartitionResult()
                                        .setPartitionIndex(0)
                                        .setErrorCode(Errors.NONE.code())
                                )
                        )
        );
        return new AlterReplicaLogDirsResponse(data);
    }

    private DescribeClientQuotasRequest createDescribeClientQuotasRequest(short version) {
        ClientQuotaFilter filter = ClientQuotaFilter.all();
        return new DescribeClientQuotasRequest.Builder(filter).build(version);
    }

    private DescribeClientQuotasResponse createDescribeClientQuotasResponse() {
        DescribeClientQuotasResponseData data = new DescribeClientQuotasResponseData().setEntries(singletonList(
                new DescribeClientQuotasResponseData.EntryData()
                        .setEntity(singletonList(new DescribeClientQuotasResponseData.EntityData()
                            .setEntityType(ClientQuotaEntity.USER)
                            .setEntityName("user")))
                        .setValues(singletonList(new DescribeClientQuotasResponseData.ValueData()
                            .setKey("request_percentage")
                            .setValue(1.0)))));
        return new DescribeClientQuotasResponse(data);
    }

    private AlterClientQuotasRequest createAlterClientQuotasRequest(short version) {
        ClientQuotaEntity entity = new ClientQuotaEntity(Collections.singletonMap(ClientQuotaEntity.USER, "user"));
        ClientQuotaAlteration.Op op = new ClientQuotaAlteration.Op("request_percentage", 2.0);
        ClientQuotaAlteration alteration = new ClientQuotaAlteration(entity, Collections.singleton(op));
        return new AlterClientQuotasRequest.Builder(Collections.singleton(alteration), false).build(version);
    }

    private AlterClientQuotasResponse createAlterClientQuotasResponse() {
        AlterClientQuotasResponseData data = new AlterClientQuotasResponseData()
            .setEntries(singletonList(new AlterClientQuotasResponseData.EntryData()
                .setEntity(singletonList(new AlterClientQuotasResponseData.EntityData()
                    .setEntityType(ClientQuotaEntity.USER)
                    .setEntityName("user")))));
        return new AlterClientQuotasResponse(data);
    }

    private DescribeProducersRequest createDescribeProducersRequest(short version) {
        DescribeProducersRequestData data = new DescribeProducersRequestData();
        DescribeProducersRequestData.TopicRequest topicRequest = new DescribeProducersRequestData.TopicRequest();
        topicRequest.setName("test");
        topicRequest.partitionIndexes().add(0);
        topicRequest.partitionIndexes().add(1);
        data.topics().add(topicRequest);
        return new DescribeProducersRequest.Builder(data).build(version);
    }

    private DescribeProducersResponse createDescribeProducersResponse() {
        DescribeProducersResponseData data = new DescribeProducersResponseData();
        DescribeProducersResponseData.TopicResponse topicResponse = new DescribeProducersResponseData.TopicResponse();
        topicResponse.partitions().add(new DescribeProducersResponseData.PartitionResponse()
            .setErrorCode(Errors.NONE.code())
            .setPartitionIndex(0)
            .setActiveProducers(asList(
                new DescribeProducersResponseData.ProducerState()
                    .setProducerId(1234L)
                    .setProducerEpoch(15)
                    .setLastTimestamp(13490218304L)
                    .setCurrentTxnStartOffset(5000),
                new DescribeProducersResponseData.ProducerState()
                    .setProducerId(9876L)
                    .setProducerEpoch(32)
                    .setLastTimestamp(13490218399L)
            ))
        );
        data.topics().add(topicResponse);
        return new DescribeProducersResponse(data);
    }

    private BrokerHeartbeatRequest createBrokerHeartbeatRequest(short v) {
        BrokerHeartbeatRequestData data = new BrokerHeartbeatRequestData()
                .setBrokerId(1)
                .setBrokerEpoch(1)
                .setCurrentMetadataOffset(1)
                .setWantFence(false)
                .setWantShutDown(false);
        return new BrokerHeartbeatRequest.Builder(data).build(v);
    }

    private BrokerHeartbeatResponse createBrokerHeartbeatResponse() {
        BrokerHeartbeatResponseData data = new BrokerHeartbeatResponseData()
                .setIsFenced(false)
                .setShouldShutDown(false)
                .setThrottleTimeMs(0);
        return new BrokerHeartbeatResponse(data);
    }

    private BrokerRegistrationRequest createBrokerRegistrationRequest(short v) {
        BrokerRegistrationRequestData data = new BrokerRegistrationRequestData()
                .setBrokerId(1)
                .setClusterId(Uuid.randomUuid().toString())
                .setRack("1")
                .setFeatures(new BrokerRegistrationRequestData.FeatureCollection(singletonList(
                        new BrokerRegistrationRequestData.Feature()).iterator()))
                .setListeners(new BrokerRegistrationRequestData.ListenerCollection(singletonList(
                        new BrokerRegistrationRequestData.Listener()).iterator()))
                .setIncarnationId(Uuid.randomUuid());
        return new BrokerRegistrationRequest.Builder(data).build(v);
    }

    private BrokerRegistrationResponse createBrokerRegistrationResponse() {
        BrokerRegistrationResponseData data = new BrokerRegistrationResponseData()
                .setBrokerEpoch(1)
                .setThrottleTimeMs(0);
        return new BrokerRegistrationResponse(data);
    }

    private UnregisterBrokerRequest createUnregisterBrokerRequest(short version) {
        UnregisterBrokerRequestData data = new UnregisterBrokerRequestData().setBrokerId(1);
        return new UnregisterBrokerRequest.Builder(data).build(version);
    }

    private UnregisterBrokerResponse createUnregisterBrokerResponse() {
        return new UnregisterBrokerResponse(new UnregisterBrokerResponseData());
    }

    private DescribeTransactionsRequest createDescribeTransactionsRequest(short version) {
        DescribeTransactionsRequestData data = new DescribeTransactionsRequestData()
            .setTransactionalIds(asList("t1", "t2", "t3"));
        return new DescribeTransactionsRequest.Builder(data).build(version);
    }

    private DescribeTransactionsResponse createDescribeTransactionsResponse() {
        DescribeTransactionsResponseData data = new DescribeTransactionsResponseData();
        data.setTransactionStates(asList(
            new DescribeTransactionsResponseData.TransactionState()
                .setErrorCode(Errors.NONE.code())
                .setTransactionalId("t1")
                .setProducerId(12345L)
                .setProducerEpoch((short) 15)
                .setTransactionStartTimeMs(13490218304L)
                .setTransactionState("Empty"),
            new DescribeTransactionsResponseData.TransactionState()
                .setErrorCode(Errors.NONE.code())
                .setTransactionalId("t2")
                .setProducerId(98765L)
                .setProducerEpoch((short) 30)
                .setTransactionStartTimeMs(13490218304L)
                .setTransactionState("Ongoing")
                .setTopics(new DescribeTransactionsResponseData.TopicDataCollection(
                    asList(
                        new DescribeTransactionsResponseData.TopicData()
                            .setTopic("foo")
                            .setPartitions(asList(1, 3, 5, 7)),
                        new DescribeTransactionsResponseData.TopicData()
                            .setTopic("bar")
                            .setPartitions(asList(1, 3))
                    ).iterator()
                )),
            new DescribeTransactionsResponseData.TransactionState()
                .setErrorCode(Errors.NOT_COORDINATOR.code())
                .setTransactionalId("t3")
        ));
        return new DescribeTransactionsResponse(data);
    }

    private ListTransactionsRequest createListTransactionsRequest(short version) {
        return new ListTransactionsRequest.Builder(new ListTransactionsRequestData()
            .setStateFilters(singletonList("Ongoing"))
            .setProducerIdFilters(asList(1L, 2L, 15L))
        ).build(version);
    }

    private ListTransactionsResponse createListTransactionsResponse() {
        ListTransactionsResponseData response = new ListTransactionsResponseData();
        response.setErrorCode(Errors.NONE.code());
        response.setTransactionStates(asList(
            new ListTransactionsResponseData.TransactionState()
                .setTransactionalId("foo")
                .setProducerId(12345L)
                .setTransactionState("Ongoing"),
            new ListTransactionsResponseData.TransactionState()
                .setTransactionalId("bar")
                .setProducerId(98765L)
                .setTransactionState("PrepareAbort")
        ));
        return new ListTransactionsResponse(response);
    }

}
