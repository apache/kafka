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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidReplicaAssignmentException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.NotCoordinatorException;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ControlledShutdownRequestData;
import org.apache.kafka.common.message.ControlledShutdownResponseData;
import org.apache.kafka.common.message.ControlledShutdownResponseData.RemainingPartition;
import org.apache.kafka.common.message.ControlledShutdownResponseData.RemainingPartitionCollection;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableReplicaAssignment;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreateableTopicConfig;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.DeleteTopicsResponseData.DeletableTopicResult;
import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.ElectPreferredLeadersRequestData;
import org.apache.kafka.common.message.ElectPreferredLeadersRequestData.TopicPartitions;
import org.apache.kafka.common.message.ElectPreferredLeadersResponseData;
import org.apache.kafka.common.message.ElectPreferredLeadersResponseData.PartitionResult;
import org.apache.kafka.common.message.ElectPreferredLeadersResponseData.ReplicaElectionResult;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.AlterConfigsResource;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.AlterableConfig;
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData;
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData.AlterConfigsResourceResult;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.CreateAclsRequest.AclCreation;
import org.apache.kafka.common.requests.CreateAclsResponse.AclCreationResponse;
import org.apache.kafka.common.requests.CreatePartitionsRequest.PartitionDetails;
import org.apache.kafka.common.requests.DeleteAclsResponse.AclDeletionResult;
import org.apache.kafka.common.requests.DeleteAclsResponse.AclFilterResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
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
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.kafka.common.requests.FetchMetadata.INVALID_SESSION_ID;
import static org.apache.kafka.test.TestUtils.toBuffer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RequestResponseTest {

    @Test
    public void testSerialization() throws Exception {
        checkRequest(createFindCoordinatorRequest(0), true);
        checkRequest(createFindCoordinatorRequest(1), true);
        checkErrorResponse(createFindCoordinatorRequest(0), new UnknownServerException(), true);
        checkErrorResponse(createFindCoordinatorRequest(1), new UnknownServerException(), true);
        checkResponse(createFindCoordinatorResponse(), 0, true);
        checkResponse(createFindCoordinatorResponse(), 1, true);
        checkRequest(createControlledShutdownRequest(), true);
        checkResponse(createControlledShutdownResponse(), 1, true);
        checkErrorResponse(createControlledShutdownRequest(), new UnknownServerException(), true);
        checkErrorResponse(createControlledShutdownRequest(0), new UnknownServerException(), true);
        checkRequest(createFetchRequest(4), true);
        checkResponse(createFetchResponse(), 4, true);
        List<TopicPartition> toForgetTopics = new ArrayList<>();
        toForgetTopics.add(new TopicPartition("foo", 0));
        toForgetTopics.add(new TopicPartition("foo", 2));
        toForgetTopics.add(new TopicPartition("bar", 0));
        checkRequest(createFetchRequest(7, new FetchMetadata(123, 456), toForgetTopics), true);
        checkResponse(createFetchResponse(123), 7, true);
        checkResponse(createFetchResponse(Errors.FETCH_SESSION_ID_NOT_FOUND, 123), 7, true);
        checkErrorResponse(createFetchRequest(4), new UnknownServerException(), true);
        checkRequest(createHeartBeatRequest(), true);
        checkErrorResponse(createHeartBeatRequest(), new UnknownServerException(), true);
        checkResponse(createHeartBeatResponse(), 0, true);
        checkRequest(createJoinGroupRequest(1), true);
        checkErrorResponse(createJoinGroupRequest(0), new UnknownServerException(), true);
        checkErrorResponse(createJoinGroupRequest(1), new UnknownServerException(), true);
        checkResponse(createJoinGroupResponse(), 0, true);
        checkRequest(createLeaveGroupRequest(), true);
        checkErrorResponse(createLeaveGroupRequest(), new UnknownServerException(), true);
        checkResponse(createLeaveGroupResponse(), 0, true);
        checkRequest(createListGroupsRequest(), true);
        checkErrorResponse(createListGroupsRequest(), new UnknownServerException(), true);
        checkResponse(createListGroupsResponse(), 0, true);
        checkRequest(createDescribeGroupRequest(), true);
        checkErrorResponse(createDescribeGroupRequest(), new UnknownServerException(), true);
        checkResponse(createDescribeGroupResponse(), 0, true);
        checkRequest(createDeleteGroupsRequest(), true);
        checkErrorResponse(createDeleteGroupsRequest(), new UnknownServerException(), true);
        checkResponse(createDeleteGroupsResponse(), 0, true);
        checkRequest(createListOffsetRequest(1), true);
        checkErrorResponse(createListOffsetRequest(1), new UnknownServerException(), true);
        checkResponse(createListOffsetResponse(1), 1, true);
        checkRequest(createListOffsetRequest(2), true);
        checkErrorResponse(createListOffsetRequest(2), new UnknownServerException(), true);
        checkResponse(createListOffsetResponse(2), 2, true);
        checkRequest(MetadataRequest.Builder.allTopics().build((short) 2), true);
        checkRequest(createMetadataRequest(1, singletonList("topic1")), true);
        checkErrorResponse(createMetadataRequest(1, singletonList("topic1")), new UnknownServerException(), true);
        checkResponse(createMetadataResponse(), 2, true);
        checkErrorResponse(createMetadataRequest(2, singletonList("topic1")), new UnknownServerException(), true);
        checkResponse(createMetadataResponse(), 3, true);
        checkErrorResponse(createMetadataRequest(3, singletonList("topic1")), new UnknownServerException(), true);
        checkResponse(createMetadataResponse(), 4, true);
        checkErrorResponse(createMetadataRequest(4, singletonList("topic1")), new UnknownServerException(), true);
        checkRequest(OffsetFetchRequest.forAllPartitions("group1"), true);
        checkErrorResponse(OffsetFetchRequest.forAllPartitions("group1"), new NotCoordinatorException("Not Coordinator"), true);
        checkRequest(createOffsetFetchRequest(0), true);
        checkRequest(createOffsetFetchRequest(1), true);
        checkRequest(createOffsetFetchRequest(2), true);
        checkRequest(OffsetFetchRequest.forAllPartitions("group1"), true);
        checkErrorResponse(createOffsetFetchRequest(0), new UnknownServerException(), true);
        checkErrorResponse(createOffsetFetchRequest(1), new UnknownServerException(), true);
        checkErrorResponse(createOffsetFetchRequest(2), new UnknownServerException(), true);
        checkResponse(createOffsetFetchResponse(), 0, true);
        checkRequest(createProduceRequest(2), true);
        checkErrorResponse(createProduceRequest(2), new UnknownServerException(), true);
        checkRequest(createProduceRequest(3), true);
        checkErrorResponse(createProduceRequest(3), new UnknownServerException(), true);
        checkResponse(createProduceResponse(), 2, true);
        checkRequest(createStopReplicaRequest(0, true), true);
        checkRequest(createStopReplicaRequest(0, false), true);
        checkErrorResponse(createStopReplicaRequest(0, true), new UnknownServerException(), true);
        checkRequest(createStopReplicaRequest(1, true), true);
        checkRequest(createStopReplicaRequest(1, false), true);
        checkErrorResponse(createStopReplicaRequest(1, true), new UnknownServerException(), true);
        checkResponse(createStopReplicaResponse(), 0, true);
        checkRequest(createLeaderAndIsrRequest(0), true);
        checkErrorResponse(createLeaderAndIsrRequest(0), new UnknownServerException(), false);
        checkRequest(createLeaderAndIsrRequest(1), true);
        checkErrorResponse(createLeaderAndIsrRequest(1), new UnknownServerException(), false);
        checkResponse(createLeaderAndIsrResponse(), 0, true);
        checkRequest(createSaslHandshakeRequest(), true);
        checkErrorResponse(createSaslHandshakeRequest(), new UnknownServerException(), true);
        checkResponse(createSaslHandshakeResponse(), 0, true);
        checkRequest(createSaslAuthenticateRequest(), true);
        checkErrorResponse(createSaslAuthenticateRequest(), new UnknownServerException(), true);
        checkResponse(createSaslAuthenticateResponse(), 0, true);
        checkResponse(createSaslAuthenticateResponse(), 1, true);
        checkRequest(createApiVersionRequest(), true);
        checkErrorResponse(createApiVersionRequest(), new UnknownServerException(), true);
        checkResponse(createApiVersionResponse(), 0, true);
        checkRequest(createCreateTopicRequest(0), true);
        checkErrorResponse(createCreateTopicRequest(0), new UnknownServerException(), true);
        checkResponse(createCreateTopicResponse(), 0, true);
        checkRequest(createCreateTopicRequest(1), true);
        checkErrorResponse(createCreateTopicRequest(1), new UnknownServerException(), true);
        checkResponse(createCreateTopicResponse(), 1, true);
        checkRequest(createDeleteTopicsRequest(), true);
        checkErrorResponse(createDeleteTopicsRequest(), new UnknownServerException(), true);
        checkResponse(createDeleteTopicsResponse(), 0, true);

        checkRequest(createInitPidRequest(), true);
        checkErrorResponse(createInitPidRequest(), new UnknownServerException(), true);
        checkResponse(createInitPidResponse(), 0, true);

        checkRequest(createAddPartitionsToTxnRequest(), true);
        checkResponse(createAddPartitionsToTxnResponse(), 0, true);
        checkErrorResponse(createAddPartitionsToTxnRequest(), new UnknownServerException(), true);
        checkRequest(createAddOffsetsToTxnRequest(), true);
        checkResponse(createAddOffsetsToTxnResponse(), 0, true);
        checkErrorResponse(createAddOffsetsToTxnRequest(), new UnknownServerException(), true);
        checkRequest(createEndTxnRequest(), true);
        checkResponse(createEndTxnResponse(), 0, true);
        checkErrorResponse(createEndTxnRequest(), new UnknownServerException(), true);
        checkRequest(createWriteTxnMarkersRequest(), true);
        checkResponse(createWriteTxnMarkersResponse(), 0, true);
        checkErrorResponse(createWriteTxnMarkersRequest(), new UnknownServerException(), true);
        checkRequest(createTxnOffsetCommitRequest(), true);
        checkResponse(createTxnOffsetCommitResponse(), 0, true);
        checkErrorResponse(createTxnOffsetCommitRequest(), new UnknownServerException(), true);

        checkOlderFetchVersions();
        checkResponse(createMetadataResponse(), 0, true);
        checkResponse(createMetadataResponse(), 1, true);
        checkErrorResponse(createMetadataRequest(1, singletonList("topic1")), new UnknownServerException(), true);
        checkRequest(createOffsetCommitRequest(0), true);
        checkErrorResponse(createOffsetCommitRequest(0), new UnknownServerException(), true);
        checkRequest(createOffsetCommitRequest(1), true);
        checkErrorResponse(createOffsetCommitRequest(1), new UnknownServerException(), true);
        checkRequest(createOffsetCommitRequest(2), true);
        checkErrorResponse(createOffsetCommitRequest(2), new UnknownServerException(), true);
        checkRequest(createOffsetCommitRequest(3), true);
        checkErrorResponse(createOffsetCommitRequest(3), new UnknownServerException(), true);
        checkRequest(createOffsetCommitRequest(4), true);
        checkErrorResponse(createOffsetCommitRequest(4), new UnknownServerException(), true);
        checkResponse(createOffsetCommitResponse(), 4, true);
        checkRequest(createOffsetCommitRequest(5), true);
        checkErrorResponse(createOffsetCommitRequest(5), new UnknownServerException(), true);
        checkResponse(createOffsetCommitResponse(), 5, true);
        checkRequest(createJoinGroupRequest(0), true);
        checkRequest(createUpdateMetadataRequest(0, null), false);
        checkErrorResponse(createUpdateMetadataRequest(0, null), new UnknownServerException(), true);
        checkRequest(createUpdateMetadataRequest(1, null), false);
        checkRequest(createUpdateMetadataRequest(1, "rack1"), false);
        checkErrorResponse(createUpdateMetadataRequest(1, null), new UnknownServerException(), true);
        checkRequest(createUpdateMetadataRequest(2, "rack1"), false);
        checkRequest(createUpdateMetadataRequest(2, null), false);
        checkErrorResponse(createUpdateMetadataRequest(2, "rack1"), new UnknownServerException(), true);
        checkRequest(createUpdateMetadataRequest(3, "rack1"), false);
        checkRequest(createUpdateMetadataRequest(3, null), false);
        checkErrorResponse(createUpdateMetadataRequest(3, "rack1"), new UnknownServerException(), true);
        checkRequest(createUpdateMetadataRequest(4, "rack1"), false);
        checkRequest(createUpdateMetadataRequest(4, null), false);
        checkErrorResponse(createUpdateMetadataRequest(4, "rack1"), new UnknownServerException(), true);
        checkRequest(createUpdateMetadataRequest(5, "rack1"), false);
        checkRequest(createUpdateMetadataRequest(5, null), false);
        checkErrorResponse(createUpdateMetadataRequest(5, "rack1"), new UnknownServerException(), true);
        checkResponse(createUpdateMetadataResponse(), 0, true);
        checkRequest(createListOffsetRequest(0), true);
        checkErrorResponse(createListOffsetRequest(0), new UnknownServerException(), true);
        checkResponse(createListOffsetResponse(0), 0, true);
        checkRequest(createLeaderEpochRequestForReplica(0, 1), true);
        checkRequest(createLeaderEpochRequestForConsumer(), true);
        checkResponse(createLeaderEpochResponse(), 0, true);
        checkErrorResponse(createLeaderEpochRequestForConsumer(), new UnknownServerException(), true);
        checkRequest(createAddPartitionsToTxnRequest(), true);
        checkErrorResponse(createAddPartitionsToTxnRequest(), new UnknownServerException(), true);
        checkResponse(createAddPartitionsToTxnResponse(), 0, true);
        checkRequest(createAddOffsetsToTxnRequest(), true);
        checkErrorResponse(createAddOffsetsToTxnRequest(), new UnknownServerException(), true);
        checkResponse(createAddOffsetsToTxnResponse(), 0, true);
        checkRequest(createEndTxnRequest(), true);
        checkErrorResponse(createEndTxnRequest(), new UnknownServerException(), true);
        checkResponse(createEndTxnResponse(), 0, true);
        checkRequest(createWriteTxnMarkersRequest(), true);
        checkErrorResponse(createWriteTxnMarkersRequest(), new UnknownServerException(), true);
        checkResponse(createWriteTxnMarkersResponse(), 0, true);
        checkRequest(createTxnOffsetCommitRequest(), true);
        checkErrorResponse(createTxnOffsetCommitRequest(), new UnknownServerException(), true);
        checkResponse(createTxnOffsetCommitResponse(), 0, true);
        checkRequest(createListAclsRequest(), true);
        checkErrorResponse(createListAclsRequest(), new SecurityDisabledException("Security is not enabled."), true);
        checkResponse(createDescribeAclsResponse(), ApiKeys.DESCRIBE_ACLS.latestVersion(), true);
        checkRequest(createCreateAclsRequest(), true);
        checkErrorResponse(createCreateAclsRequest(), new SecurityDisabledException("Security is not enabled."), true);
        checkResponse(createCreateAclsResponse(), ApiKeys.CREATE_ACLS.latestVersion(), true);
        checkRequest(createDeleteAclsRequest(), true);
        checkErrorResponse(createDeleteAclsRequest(), new SecurityDisabledException("Security is not enabled."), true);
        checkResponse(createDeleteAclsResponse(), ApiKeys.DELETE_ACLS.latestVersion(), true);
        checkRequest(createAlterConfigsRequest(), false);
        checkErrorResponse(createAlterConfigsRequest(), new UnknownServerException(), true);
        checkResponse(createAlterConfigsResponse(), 0, false);
        checkRequest(createDescribeConfigsRequest(0), true);
        checkRequest(createDescribeConfigsRequestWithConfigEntries(0), false);
        checkErrorResponse(createDescribeConfigsRequest(0), new UnknownServerException(), true);
        checkResponse(createDescribeConfigsResponse(), 0, false);
        checkRequest(createDescribeConfigsRequest(1), true);
        checkRequest(createDescribeConfigsRequestWithConfigEntries(1), false);
        checkErrorResponse(createDescribeConfigsRequest(1), new UnknownServerException(), true);
        checkResponse(createDescribeConfigsResponse(), 1, false);
        checkDescribeConfigsResponseVersions();
        checkRequest(createCreatePartitionsRequest(), true);
        checkRequest(createCreatePartitionsRequestWithAssignments(), false);
        checkErrorResponse(createCreatePartitionsRequest(), new InvalidTopicException(), true);
        checkResponse(createCreatePartitionsResponse(), 0, true);
        checkRequest(createCreateTokenRequest(), true);
        checkErrorResponse(createCreateTokenRequest(), new UnknownServerException(), true);
        checkResponse(createCreateTokenResponse(), 0, true);
        checkRequest(createDescribeTokenRequest(), true);
        checkErrorResponse(createDescribeTokenRequest(), new UnknownServerException(), true);
        checkResponse(createDescribeTokenResponse(), 0, true);
        checkRequest(createExpireTokenRequest(), true);
        checkErrorResponse(createExpireTokenRequest(), new UnknownServerException(), true);
        checkResponse(createExpireTokenResponse(), 0, true);
        checkRequest(createRenewTokenRequest(), true);
        checkErrorResponse(createRenewTokenRequest(), new UnknownServerException(), true);
        checkResponse(createRenewTokenResponse(), 0, true);
        checkRequest(createElectPreferredLeadersRequest(), true);
        checkRequest(createElectPreferredLeadersRequestNullPartitions(), true);
        checkErrorResponse(createElectPreferredLeadersRequest(), new UnknownServerException(), true);
        checkResponse(createElectPreferredLeadersResponse(), 0, true);
        checkRequest(createIncrementalAlterConfigsRequest(), true);
        checkErrorResponse(createIncrementalAlterConfigsRequest(), new UnknownServerException(), true);
        checkResponse(createIncrementalAlterConfigsResponse(), 0, true);
    }

    @Test
    public void testResponseHeader() {
        ResponseHeader header = createResponseHeader();
        ByteBuffer buffer = toBuffer(header.toStruct());
        ResponseHeader deserialized = ResponseHeader.parse(buffer);
        assertEquals(header.correlationId(), deserialized.correlationId());
    }

    private void checkOlderFetchVersions() throws Exception {
        int latestVersion = ApiKeys.FETCH.latestVersion();
        for (int i = 0; i < latestVersion; ++i) {
            checkErrorResponse(createFetchRequest(i), new UnknownServerException(), true);
            checkRequest(createFetchRequest(i), true);
            checkResponse(createFetchResponse(), i, true);
        }
    }

    private void verifyDescribeConfigsResponse(DescribeConfigsResponse expected, DescribeConfigsResponse actual, int version) throws Exception {
        for (ConfigResource resource : expected.configs().keySet()) {
            Collection<DescribeConfigsResponse.ConfigEntry> deserializedEntries1 = actual.config(resource).entries();
            Iterator<DescribeConfigsResponse.ConfigEntry> expectedEntries = expected.config(resource).entries().iterator();
            for (DescribeConfigsResponse.ConfigEntry entry : deserializedEntries1) {
                DescribeConfigsResponse.ConfigEntry expectedEntry = expectedEntries.next();
                assertEquals(expectedEntry.name(), entry.name());
                assertEquals(expectedEntry.value(), entry.value());
                assertEquals(expectedEntry.isReadOnly(), entry.isReadOnly());
                assertEquals(expectedEntry.isSensitive(), entry.isSensitive());
                if (version == 1 || (expectedEntry.source() != DescribeConfigsResponse.ConfigSource.DYNAMIC_BROKER_CONFIG &&
                        expectedEntry.source() != DescribeConfigsResponse.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG))
                    assertEquals(expectedEntry.source(), entry.source());
                else
                    assertEquals(DescribeConfigsResponse.ConfigSource.STATIC_BROKER_CONFIG, entry.source());
            }
        }
    }

    private void checkDescribeConfigsResponseVersions() throws Exception {
        DescribeConfigsResponse response = createDescribeConfigsResponse();
        DescribeConfigsResponse deserialized0 = (DescribeConfigsResponse) deserialize(response,
                response.toStruct((short) 0), (short) 0);
        verifyDescribeConfigsResponse(response, deserialized0, 0);

        DescribeConfigsResponse deserialized1 = (DescribeConfigsResponse) deserialize(response,
                response.toStruct((short) 1), (short) 1);
        verifyDescribeConfigsResponse(response, deserialized1, 1);
    }

    private void checkErrorResponse(AbstractRequest req, Throwable e, boolean checkEqualityAndHashCode) {
        checkResponse(req.getErrorResponse(e), req.version(), checkEqualityAndHashCode);
    }

    private void checkRequest(AbstractRequest req, boolean checkEqualityAndHashCode) {
        // Check that we can serialize, deserialize and serialize again
        // Check for equality and hashCode of the Struct only if indicated (it is likely to fail if any of the fields
        // in the request is a HashMap with multiple elements since ordering of the elements may vary)
        try {
            Struct struct = req.toStruct();
            AbstractRequest deserialized = AbstractRequest.parseRequest(req.api, req.version(), struct);
            Struct struct2 = deserialized.toStruct();
            if (checkEqualityAndHashCode) {
                assertEquals(struct, struct2);
                assertEquals(struct.hashCode(), struct2.hashCode());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize request " + req + " with type " + req.getClass(), e);
        }
    }

    private void checkResponse(AbstractResponse response, int version, boolean checkEqualityAndHashCode) {
        // Check that we can serialize, deserialize and serialize again
        // Check for equality and hashCode of the Struct only if indicated (it is likely to fail if any of the fields
        // in the response is a HashMap with multiple elements since ordering of the elements may vary)
        try {
            Struct struct = response.toStruct((short) version);
            AbstractResponse deserialized = (AbstractResponse) deserialize(response, struct, (short) version);
            Struct struct2 = deserialized.toStruct((short) version);
            if (checkEqualityAndHashCode) {
                assertEquals(struct, struct2);
                assertEquals(struct.hashCode(), struct2.hashCode());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize response " + response + " with type " + response.getClass(), e);
        }
    }

    private AbstractRequestResponse deserialize(AbstractRequestResponse req, Struct struct, short version) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        ByteBuffer buffer = toBuffer(struct);
        Method deserializer = req.getClass().getDeclaredMethod("parse", ByteBuffer.class, Short.TYPE);
        return (AbstractRequestResponse) deserializer.invoke(null, buffer, version);
    }

    @Test(expected = UnsupportedVersionException.class)
    public void cannotUseFindCoordinatorV0ToFindTransactionCoordinator() {
        FindCoordinatorRequest.Builder builder = new FindCoordinatorRequest.Builder(
                new FindCoordinatorRequestData()
                    .setKeyType(CoordinatorType.TRANSACTION.id)
                    .setKey("foobar"));
        builder.build((short) 0);
    }

    @Test
    public void produceRequestToStringTest() {
        ProduceRequest request = createProduceRequest(ApiKeys.PRODUCE.latestVersion());
        assertEquals(1, request.partitionRecordsOrFail().size());
        assertFalse(request.toString(false).contains("partitionSizes"));
        assertTrue(request.toString(false).contains("numPartitions=1"));
        assertTrue(request.toString(true).contains("partitionSizes"));
        assertFalse(request.toString(true).contains("numPartitions"));

        request.clearPartitionRecords();
        try {
            request.partitionRecordsOrFail();
            fail("partitionRecordsOrFail should fail after clearPartitionRecords()");
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
        ProduceRequest request = createProduceRequest(ApiKeys.PRODUCE.latestVersion());
        Set<TopicPartition> partitions = new HashSet<>(request.partitionRecordsOrFail().keySet());

        ProduceResponse errorResponse = (ProduceResponse) request.getErrorResponse(new NotEnoughReplicasException());
        assertEquals(partitions, errorResponse.responses().keySet());
        ProduceResponse.PartitionResponse partitionResponse = errorResponse.responses().values().iterator().next();
        assertEquals(Errors.NOT_ENOUGH_REPLICAS, partitionResponse.error);
        assertEquals(ProduceResponse.INVALID_OFFSET, partitionResponse.baseOffset);
        assertEquals(RecordBatch.NO_TIMESTAMP, partitionResponse.logAppendTime);

        request.clearPartitionRecords();

        // `getErrorResponse` should behave the same after `clearPartitionRecords`
        errorResponse = (ProduceResponse) request.getErrorResponse(new NotEnoughReplicasException());
        assertEquals(partitions, errorResponse.responses().keySet());
        partitionResponse = errorResponse.responses().values().iterator().next();
        assertEquals(Errors.NOT_ENOUGH_REPLICAS, partitionResponse.error);
        assertEquals(ProduceResponse.INVALID_OFFSET, partitionResponse.baseOffset);
        assertEquals(RecordBatch.NO_TIMESTAMP, partitionResponse.logAppendTime);
    }

    @Test
    public void produceResponseV5Test() {
        Map<TopicPartition, ProduceResponse.PartitionResponse> responseData = new HashMap<>();
        TopicPartition tp0 = new TopicPartition("test", 0);
        responseData.put(tp0, new ProduceResponse.PartitionResponse(Errors.NONE,
                10000, RecordBatch.NO_TIMESTAMP, 100));

        ProduceResponse v5Response = new ProduceResponse(responseData, 10);
        short version = 5;

        ByteBuffer buffer = v5Response.serialize(version, new ResponseHeader(0));
        buffer.rewind();

        ResponseHeader.parse(buffer); // throw away.

        Struct deserializedStruct = ApiKeys.PRODUCE.parseResponse(version, buffer);

        ProduceResponse v5FromBytes = (ProduceResponse) AbstractResponse.parseResponse(ApiKeys.PRODUCE,
                deserializedStruct, version);

        assertEquals(1, v5FromBytes.responses().size());
        assertTrue(v5FromBytes.responses().containsKey(tp0));
        ProduceResponse.PartitionResponse partitionResponse = v5FromBytes.responses().get(tp0);
        assertEquals(100, partitionResponse.logStartOffset);
        assertEquals(10000, partitionResponse.baseOffset);
        assertEquals(10, v5FromBytes.throttleTimeMs());
        assertEquals(responseData, v5Response.responses());
    }

    @Test
    public void produceResponseVersionTest() {
        Map<TopicPartition, ProduceResponse.PartitionResponse> responseData = new HashMap<>();
        responseData.put(new TopicPartition("test", 0), new ProduceResponse.PartitionResponse(Errors.NONE,
                10000, RecordBatch.NO_TIMESTAMP, 100));
        ProduceResponse v0Response = new ProduceResponse(responseData);
        ProduceResponse v1Response = new ProduceResponse(responseData, 10);
        ProduceResponse v2Response = new ProduceResponse(responseData, 10);
        assertEquals("Throttle time must be zero", 0, v0Response.throttleTimeMs());
        assertEquals("Throttle time must be 10", 10, v1Response.throttleTimeMs());
        assertEquals("Throttle time must be 10", 10, v2Response.throttleTimeMs());
        assertEquals("Should use schema version 0", ApiKeys.PRODUCE.responseSchema((short) 0),
                v0Response.toStruct((short) 0).schema());
        assertEquals("Should use schema version 1", ApiKeys.PRODUCE.responseSchema((short) 1),
                v1Response.toStruct((short) 1).schema());
        assertEquals("Should use schema version 2", ApiKeys.PRODUCE.responseSchema((short) 2),
                v2Response.toStruct((short) 2).schema());
        assertEquals("Response data does not match", responseData, v0Response.responses());
        assertEquals("Response data does not match", responseData, v1Response.responses());
        assertEquals("Response data does not match", responseData, v2Response.responses());
    }

    @Test
    public void fetchResponseVersionTest() {
        LinkedHashMap<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> responseData = new LinkedHashMap<>();

        MemoryRecords records = MemoryRecords.readableRecords(ByteBuffer.allocate(10));
        responseData.put(new TopicPartition("test", 0), new FetchResponse.PartitionData<>(
                Errors.NONE, 1000000, FetchResponse.INVALID_LAST_STABLE_OFFSET,
                0L, Optional.empty(), null, records));

        FetchResponse<MemoryRecords> v0Response = new FetchResponse<>(Errors.NONE, responseData, 0, INVALID_SESSION_ID);
        FetchResponse<MemoryRecords> v1Response = new FetchResponse<>(Errors.NONE, responseData, 10, INVALID_SESSION_ID);
        assertEquals("Throttle time must be zero", 0, v0Response.throttleTimeMs());
        assertEquals("Throttle time must be 10", 10, v1Response.throttleTimeMs());
        assertEquals("Should use schema version 0", ApiKeys.FETCH.responseSchema((short) 0),
                v0Response.toStruct((short) 0).schema());
        assertEquals("Should use schema version 1", ApiKeys.FETCH.responseSchema((short) 1),
                v1Response.toStruct((short) 1).schema());
        assertEquals("Response data does not match", responseData, v0Response.responseData());
        assertEquals("Response data does not match", responseData, v1Response.responseData());
    }

    @Test
    public void testFetchResponseV4() {
        LinkedHashMap<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> responseData = new LinkedHashMap<>();
        MemoryRecords records = MemoryRecords.readableRecords(ByteBuffer.allocate(10));

        List<FetchResponse.AbortedTransaction> abortedTransactions = asList(
                new FetchResponse.AbortedTransaction(10, 100),
                new FetchResponse.AbortedTransaction(15, 50)
        );
        responseData.put(new TopicPartition("bar", 0), new FetchResponse.PartitionData<>(Errors.NONE, 100000,
                FetchResponse.INVALID_LAST_STABLE_OFFSET, FetchResponse.INVALID_LOG_START_OFFSET, Optional.empty(), abortedTransactions, records));
        responseData.put(new TopicPartition("bar", 1), new FetchResponse.PartitionData<>(Errors.NONE, 900000,
                5, FetchResponse.INVALID_LOG_START_OFFSET, Optional.empty(), null, records));
        responseData.put(new TopicPartition("foo", 0), new FetchResponse.PartitionData<>(Errors.NONE, 70000,
                6, FetchResponse.INVALID_LOG_START_OFFSET, Optional.empty(), Collections.emptyList(), records));

        FetchResponse<MemoryRecords> response = new FetchResponse<>(Errors.NONE, responseData, 10, INVALID_SESSION_ID);
        FetchResponse deserialized = FetchResponse.parse(toBuffer(response.toStruct((short) 4)), (short) 4);
        assertEquals(responseData, deserialized.responseData());
    }

    @Test
    public void verifyFetchResponseFullWrites() throws Exception {
        verifyFetchResponseFullWrite(ApiKeys.FETCH.latestVersion(), createFetchResponse(123));
        verifyFetchResponseFullWrite(ApiKeys.FETCH.latestVersion(),
            createFetchResponse(Errors.FETCH_SESSION_ID_NOT_FOUND, 123));
        for (short version = 0; version <= ApiKeys.FETCH.latestVersion(); version++) {
            verifyFetchResponseFullWrite(version, createFetchResponse());
        }
    }

    private void verifyFetchResponseFullWrite(short apiVersion, FetchResponse fetchResponse) throws Exception {
        int correlationId = 15;

        Send send = fetchResponse.toSend("1", new ResponseHeader(correlationId), apiVersion);
        ByteBufferChannel channel = new ByteBufferChannel(send.size());
        send.writeTo(channel);
        channel.close();

        ByteBuffer buf = channel.buffer();

        // read the size
        int size = buf.getInt();
        assertTrue(size > 0);

        // read the header
        ResponseHeader responseHeader = ResponseHeader.parse(channel.buffer());
        assertEquals(correlationId, responseHeader.correlationId());

        // read the body
        Struct responseBody = ApiKeys.FETCH.responseSchema(apiVersion).read(buf);
        assertEquals(fetchResponse.toStruct(apiVersion), responseBody);

        assertEquals(size, responseHeader.sizeOf() + responseBody.sizeOf());
    }

    @Test
    public void testControlledShutdownResponse() {
        ControlledShutdownResponse response = createControlledShutdownResponse();
        short version = ApiKeys.CONTROLLED_SHUTDOWN.latestVersion();
        Struct struct = response.toStruct(version);
        ByteBuffer buffer = toBuffer(struct);
        ControlledShutdownResponse deserialized = ControlledShutdownResponse.parse(buffer, version);
        assertEquals(response.error(), deserialized.error());
        assertEquals(response.data().remainingPartitions(), deserialized.data().remainingPartitions());
    }

    @Test(expected = UnsupportedVersionException.class)
    public void testCreateTopicRequestV0FailsIfValidateOnly() {
        createCreateTopicRequest(0, true);
    }

    @Test
    public void testFetchRequestMaxBytesOldVersions() throws Exception {
        final short version = 1;
        FetchRequest fr = createFetchRequest(version);
        FetchRequest fr2 = new FetchRequest(fr.toStruct(), version);
        assertEquals(fr2.maxBytes(), fr.maxBytes());
    }

    @Test
    public void testFetchRequestIsolationLevel() throws Exception {
        FetchRequest request = createFetchRequest(4, IsolationLevel.READ_COMMITTED);
        Struct struct = request.toStruct();
        FetchRequest deserialized = (FetchRequest) deserialize(request, struct, request.version());
        assertEquals(request.isolationLevel(), deserialized.isolationLevel());

        request = createFetchRequest(4, IsolationLevel.READ_UNCOMMITTED);
        struct = request.toStruct();
        deserialized = (FetchRequest) deserialize(request, struct, request.version());
        assertEquals(request.isolationLevel(), deserialized.isolationLevel());
    }

    @Test
    public void testFetchRequestWithMetadata() throws Exception {
        FetchRequest request = createFetchRequest(4, IsolationLevel.READ_COMMITTED);
        Struct struct = request.toStruct();
        FetchRequest deserialized = (FetchRequest) deserialize(request, struct, request.version());
        assertEquals(request.isolationLevel(), deserialized.isolationLevel());

        request = createFetchRequest(4, IsolationLevel.READ_UNCOMMITTED);
        struct = request.toStruct();
        deserialized = (FetchRequest) deserialize(request, struct, request.version());
        assertEquals(request.isolationLevel(), deserialized.isolationLevel());
    }

    @Test
    public void testJoinGroupRequestVersion0RebalanceTimeout() {
        final short version = 0;
        JoinGroupRequest jgr = createJoinGroupRequest(version);
        JoinGroupRequest jgr2 = new JoinGroupRequest(jgr.toStruct(), version);
        assertEquals(jgr2.data().rebalanceTimeoutMs(), jgr.data().rebalanceTimeoutMs());
    }

    @Test
    public void testOffsetFetchRequestBuilderToString() {
        String allTopicPartitionsString = OffsetFetchRequest.Builder.allTopicPartitions("someGroup").toString();
        assertTrue(allTopicPartitionsString.contains("<ALL>"));
        String string = new OffsetFetchRequest.Builder("group1",
                singletonList(new TopicPartition("test11", 1))).toString();
        assertTrue(string.contains("test11"));
        assertTrue(string.contains("group1"));
    }

    private ResponseHeader createResponseHeader() {
        return new ResponseHeader(10);
    }

    private FindCoordinatorRequest createFindCoordinatorRequest(int version) {
        return new FindCoordinatorRequest.Builder(
                new FindCoordinatorRequestData()
                    .setKeyType(CoordinatorType.GROUP.id())
                    .setKey("test-group"))
                .build((short) version);
    }

    private FindCoordinatorResponse createFindCoordinatorResponse() {
        Node node = new Node(10, "host1", 2014);
        return FindCoordinatorResponse.prepareResponse(Errors.NONE, node);
    }

    private FetchRequest createFetchRequest(int version, FetchMetadata metadata, List<TopicPartition> toForget) {
        LinkedHashMap<TopicPartition, FetchRequest.PartitionData> fetchData = new LinkedHashMap<>();
        fetchData.put(new TopicPartition("test1", 0), new FetchRequest.PartitionData(100, 0L,
                1000000, Optional.of(15)));
        fetchData.put(new TopicPartition("test2", 0), new FetchRequest.PartitionData(200, 0L,
                1000000, Optional.of(25)));
        return FetchRequest.Builder.forConsumer(100, 100000, fetchData).
            metadata(metadata).setMaxBytes(1000).toForget(toForget).build((short) version);
    }

    private FetchRequest createFetchRequest(int version, IsolationLevel isolationLevel) {
        LinkedHashMap<TopicPartition, FetchRequest.PartitionData> fetchData = new LinkedHashMap<>();
        fetchData.put(new TopicPartition("test1", 0), new FetchRequest.PartitionData(100, 0L,
                1000000, Optional.of(15)));
        fetchData.put(new TopicPartition("test2", 0), new FetchRequest.PartitionData(200, 0L,
                1000000, Optional.of(25)));
        return FetchRequest.Builder.forConsumer(100, 100000, fetchData).
            isolationLevel(isolationLevel).setMaxBytes(1000).build((short) version);
    }

    private FetchRequest createFetchRequest(int version) {
        LinkedHashMap<TopicPartition, FetchRequest.PartitionData> fetchData = new LinkedHashMap<>();
        fetchData.put(new TopicPartition("test1", 0), new FetchRequest.PartitionData(100, 0L,
                1000000, Optional.of(15)));
        fetchData.put(new TopicPartition("test2", 0), new FetchRequest.PartitionData(200, 0L,
                1000000, Optional.of(25)));
        return FetchRequest.Builder.forConsumer(100, 100000, fetchData).setMaxBytes(1000).build((short) version);
    }

    private FetchResponse<MemoryRecords> createFetchResponse(Errors error, int sessionId) {
        return new FetchResponse<>(error, new LinkedHashMap<>(), 25, sessionId);
    }

    private FetchResponse<MemoryRecords> createFetchResponse(int sessionId) {
        LinkedHashMap<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> responseData = new LinkedHashMap<>();
        MemoryRecords records = MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord("blah".getBytes()));
        responseData.put(new TopicPartition("test", 0), new FetchResponse.PartitionData<>(Errors.NONE,
            1000000, FetchResponse.INVALID_LAST_STABLE_OFFSET, 0L, Optional.empty(), null, records));
        List<FetchResponse.AbortedTransaction> abortedTransactions = Collections.singletonList(
            new FetchResponse.AbortedTransaction(234L, 999L));
        responseData.put(new TopicPartition("test", 1), new FetchResponse.PartitionData<>(Errors.NONE,
            1000000, FetchResponse.INVALID_LAST_STABLE_OFFSET, 0L, Optional.empty(), abortedTransactions, MemoryRecords.EMPTY));
        return new FetchResponse<>(Errors.NONE, responseData, 25, sessionId);
    }

    private FetchResponse<MemoryRecords> createFetchResponse() {
        LinkedHashMap<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> responseData = new LinkedHashMap<>();
        MemoryRecords records = MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord("blah".getBytes()));
        responseData.put(new TopicPartition("test", 0), new FetchResponse.PartitionData<>(Errors.NONE,
                1000000, FetchResponse.INVALID_LAST_STABLE_OFFSET, 0L, Optional.empty(), null, records));

        List<FetchResponse.AbortedTransaction> abortedTransactions = Collections.singletonList(
                new FetchResponse.AbortedTransaction(234L, 999L));
        responseData.put(new TopicPartition("test", 1), new FetchResponse.PartitionData<>(Errors.NONE,
                1000000, FetchResponse.INVALID_LAST_STABLE_OFFSET, 0L, Optional.empty(), abortedTransactions, MemoryRecords.EMPTY));

        return new FetchResponse<>(Errors.NONE, responseData, 25, INVALID_SESSION_ID);
    }

    private HeartbeatRequest createHeartBeatRequest() {
        return new HeartbeatRequest.Builder(new HeartbeatRequestData()
                .setGroupId("group1")
                .setGenerationId(1)
                .setMemberId("consumer1")).build();
    }

    private HeartbeatResponse createHeartBeatResponse() {
        return new HeartbeatResponse(new HeartbeatResponseData().setErrorCode(Errors.NONE.code()));
    }

    private JoinGroupRequest createJoinGroupRequest(int version) {
        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestData.JoinGroupRequestProtocolCollection(
                Collections.singleton(
                new JoinGroupRequestData.JoinGroupRequestProtocol()
                        .setName("consumer-range")
                        .setMetadata(new byte[0])).iterator()
        );
        if (version == 0) {
            return new JoinGroupRequest.Builder(
                    new JoinGroupRequestData()
                            .setGroupId("group1")
                            .setSessionTimeoutMs(30000)
                            .setMemberId("consumer1")
                            .setGroupInstanceId(null)
                            .setProtocolType("consumer")
                            .setProtocols(protocols))
                    .build((short) version);
        } else if (version <= 4) {
            return new JoinGroupRequest.Builder(
                    new JoinGroupRequestData()
                            .setGroupId("group1")
                            .setSessionTimeoutMs(30000)
                            .setMemberId("consumer1")
                            .setGroupInstanceId(null)
                            .setProtocolType("consumer")
                            .setProtocols(protocols)
                            .setRebalanceTimeoutMs(60000)) // v1 and above contains rebalance timeout
                    .build((short) version);
        } else {
            return new JoinGroupRequest.Builder(
                    new JoinGroupRequestData()
                            .setGroupId("group1")
                            .setSessionTimeoutMs(30000)
                            .setMemberId("consumer1")
                            .setGroupInstanceId("groupInstanceId") // v5 and above could set group instance id
                            .setProtocolType("consumer")
                            .setProtocols(protocols)
                            .setRebalanceTimeoutMs(60000)) // v1 and above contains rebalance timeout
                    .build((short) version);
        }
    }

    private JoinGroupResponse createJoinGroupResponse() {
        List<JoinGroupResponseData.JoinGroupResponseMember> members = Arrays.asList(
                new JoinGroupResponseData.JoinGroupResponseMember()
                        .setMemberId("consumer1")
                        .setMetadata(new byte[0]),
                new JoinGroupResponseData.JoinGroupResponseMember()
                        .setMemberId("consumer2")
                        .setMetadata(new byte[0])
        );

        return new JoinGroupResponse(
                new JoinGroupResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setGenerationId(1)
                        .setProtocolName("range")
                        .setLeader("leader")
                        .setMemberId("consumer1")
                        .setMembers(members)
        );
    }

    private ListGroupsRequest createListGroupsRequest() {
        return new ListGroupsRequest.Builder().build();
    }

    private ListGroupsResponse createListGroupsResponse() {
        List<ListGroupsResponse.Group> groups = Collections.singletonList(new ListGroupsResponse.Group("test-group", "consumer"));
        return new ListGroupsResponse(Errors.NONE, groups);
    }

    private DescribeGroupsRequest createDescribeGroupRequest() {
        return new DescribeGroupsRequest.Builder(
            new DescribeGroupsRequestData().
                setGroups(Collections.singletonList("test-group"))).build();
    }

    private DescribeGroupsResponse createDescribeGroupResponse() {
        String clientId = "consumer-1";
        String clientHost = "localhost";
        DescribeGroupsResponseData describeGroupsResponseData = new DescribeGroupsResponseData();
        DescribeGroupsResponseData.DescribedGroupMember member = DescribeGroupsResponse.groupMember("memberId",
                clientId, clientHost, new byte[0], new byte[0]);
        DescribeGroupsResponseData.DescribedGroup metadata = DescribeGroupsResponse.groupMetadata("test-group", Errors.NONE,
                "STABLE", "consumer", "roundrobin", asList(member), Collections.emptySet());
        describeGroupsResponseData.groups().add(metadata);
        return new DescribeGroupsResponse(describeGroupsResponseData);
    }

    private LeaveGroupRequest createLeaveGroupRequest() {
        return new LeaveGroupRequest.Builder(new LeaveGroupRequestData().setGroupId("group1").setMemberId("consumer1")).build();
    }

    private LeaveGroupResponse createLeaveGroupResponse() {
        return new LeaveGroupResponse(new LeaveGroupResponseData().setErrorCode(Errors.NONE.code()));
    }

    private DeleteGroupsRequest createDeleteGroupsRequest() {
        return new DeleteGroupsRequest.Builder(Collections.singleton("test-group")).build();
    }

    private DeleteGroupsResponse createDeleteGroupsResponse() {
        Map<String, Errors> result = new HashMap<>();
        result.put("test-group", Errors.NONE);
        return new DeleteGroupsResponse(result);
    }

    @SuppressWarnings("deprecation")
    private ListOffsetRequest createListOffsetRequest(int version) {
        if (version == 0) {
            Map<TopicPartition, ListOffsetRequest.PartitionData> offsetData = Collections.singletonMap(
                    new TopicPartition("test", 0),
                    new ListOffsetRequest.PartitionData(1000000L, 10));
            return ListOffsetRequest.Builder
                    .forConsumer(false, IsolationLevel.READ_UNCOMMITTED)
                    .setTargetTimes(offsetData)
                    .build((short) version);
        } else if (version == 1) {
            Map<TopicPartition, ListOffsetRequest.PartitionData> offsetData = Collections.singletonMap(
                    new TopicPartition("test", 0),
                    new ListOffsetRequest.PartitionData(1000000L, Optional.empty()));
            return ListOffsetRequest.Builder
                    .forConsumer(true, IsolationLevel.READ_UNCOMMITTED)
                    .setTargetTimes(offsetData)
                    .build((short) version);
        } else if (version == 2) {
            Map<TopicPartition, ListOffsetRequest.PartitionData> offsetData = Collections.singletonMap(
                    new TopicPartition("test", 0),
                    new ListOffsetRequest.PartitionData(1000000L, Optional.of(5)));
            return ListOffsetRequest.Builder
                    .forConsumer(true, IsolationLevel.READ_COMMITTED)
                    .setTargetTimes(offsetData)
                    .build((short) version);
        } else {
            throw new IllegalArgumentException("Illegal ListOffsetRequest version " + version);
        }
    }

    @SuppressWarnings("deprecation")
    private ListOffsetResponse createListOffsetResponse(int version) {
        if (version == 0) {
            Map<TopicPartition, ListOffsetResponse.PartitionData> responseData = new HashMap<>();
            responseData.put(new TopicPartition("test", 0),
                    new ListOffsetResponse.PartitionData(Errors.NONE, asList(100L)));
            return new ListOffsetResponse(responseData);
        } else if (version == 1 || version == 2) {
            Map<TopicPartition, ListOffsetResponse.PartitionData> responseData = new HashMap<>();
            responseData.put(new TopicPartition("test", 0),
                    new ListOffsetResponse.PartitionData(Errors.NONE, 10000L, 100L, Optional.of(27)));
            return new ListOffsetResponse(responseData);
        } else {
            throw new IllegalArgumentException("Illegal ListOffsetResponse version " + version);
        }
    }

    private MetadataRequest createMetadataRequest(int version, List<String> topics) {
        return new MetadataRequest.Builder(topics, true).build((short) version);
    }

    private MetadataResponse createMetadataResponse() {
        Node node = new Node(1, "host1", 1001);
        List<Node> replicas = asList(node);
        List<Node> isr = asList(node);
        List<Node> offlineReplicas = asList();

        List<MetadataResponse.TopicMetadata> allTopicMetadata = new ArrayList<>();
        allTopicMetadata.add(new MetadataResponse.TopicMetadata(Errors.NONE, "__consumer_offsets", true,
                asList(new MetadataResponse.PartitionMetadata(Errors.NONE, 1, node,
                        Optional.of(5), replicas, isr, offlineReplicas))));
        allTopicMetadata.add(new MetadataResponse.TopicMetadata(Errors.LEADER_NOT_AVAILABLE, "topic2", false,
                Collections.emptyList()));
        allTopicMetadata.add(new MetadataResponse.TopicMetadata(Errors.NONE, "topic3", false,
            asList(new MetadataResponse.PartitionMetadata(Errors.LEADER_NOT_AVAILABLE, 0, null,
                Optional.empty(), replicas, isr, offlineReplicas))));

        return MetadataResponse.prepareResponse(asList(node), null, MetadataResponse.NO_CONTROLLER_ID, allTopicMetadata);
    }

    @SuppressWarnings("deprecation")
    private OffsetCommitRequest createOffsetCommitRequest(int version) {
        return new OffsetCommitRequest.Builder(new OffsetCommitRequestData()
                .setGroupId("group1")
                .setMemberId("consumer1")
                .setGroupInstanceId(null)
                .setGenerationId(100)
                .setTopics(Collections.singletonList(
                        new OffsetCommitRequestData.OffsetCommitRequestTopic()
                                .setName("test")
                                .setPartitions(Arrays.asList(
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
        ).build((short) version);
    }

    private OffsetCommitResponse createOffsetCommitResponse() {
        return new OffsetCommitResponse(new OffsetCommitResponseData()
                .setTopics(Collections.singletonList(
                        new OffsetCommitResponseData.OffsetCommitResponseTopic()
                                .setName("test")
                                .setPartitions(Collections.singletonList(
                                        new OffsetCommitResponseData.OffsetCommitResponsePartition()
                                                .setPartitionIndex(0)
                                                .setErrorCode(Errors.NONE.code())
                                ))
                ))
        );
    }

    private OffsetFetchRequest createOffsetFetchRequest(int version) {
        return new OffsetFetchRequest.Builder("group1", singletonList(new TopicPartition("test11", 1)))
                .build((short) version);
    }

    private OffsetFetchResponse createOffsetFetchResponse() {
        Map<TopicPartition, OffsetFetchResponse.PartitionData> responseData = new HashMap<>();
        responseData.put(new TopicPartition("test", 0), new OffsetFetchResponse.PartitionData(
                100L, Optional.empty(), "", Errors.NONE));
        responseData.put(new TopicPartition("test", 1), new OffsetFetchResponse.PartitionData(
                100L, Optional.of(10), null, Errors.NONE));
        return new OffsetFetchResponse(Errors.NONE, responseData);
    }

    private ProduceRequest createProduceRequest(int version) {
        if (version < 2)
            throw new IllegalArgumentException("Produce request version 2 is not supported");

        byte magic = version == 2 ? RecordBatch.MAGIC_VALUE_V1 : RecordBatch.MAGIC_VALUE_V2;
        MemoryRecords records = MemoryRecords.withRecords(magic, CompressionType.NONE, new SimpleRecord("woot".getBytes()));
        Map<TopicPartition, MemoryRecords> produceData = Collections.singletonMap(new TopicPartition("test", 0), records);
        return ProduceRequest.Builder.forMagic(magic, (short) 1, 5000, produceData, "transactionalId")
                .build((short) version);
    }

    private ProduceResponse createProduceResponse() {
        Map<TopicPartition, ProduceResponse.PartitionResponse> responseData = new HashMap<>();
        responseData.put(new TopicPartition("test", 0), new ProduceResponse.PartitionResponse(Errors.NONE,
                10000, RecordBatch.NO_TIMESTAMP, 100));
        return new ProduceResponse(responseData, 0);
    }

    private StopReplicaRequest createStopReplicaRequest(int version, boolean deletePartitions) {
        Set<TopicPartition> partitions = Utils.mkSet(new TopicPartition("test", 0));
        return new StopReplicaRequest.Builder((short) version, 0, 1, 0, deletePartitions, partitions).build();
    }

    private StopReplicaResponse createStopReplicaResponse() {
        Map<TopicPartition, Errors> responses = new HashMap<>();
        responses.put(new TopicPartition("test", 0), Errors.NONE);
        return new StopReplicaResponse(Errors.NONE, responses);
    }

    private ControlledShutdownRequest createControlledShutdownRequest() {
        ControlledShutdownRequestData data = new ControlledShutdownRequestData()
                .setBrokerId(10)
                .setBrokerEpoch(0L);
        return new ControlledShutdownRequest.Builder(
                data,
                ApiKeys.CONTROLLED_SHUTDOWN.latestVersion()).build();
    }

    private ControlledShutdownRequest createControlledShutdownRequest(int version) {
        ControlledShutdownRequestData data = new ControlledShutdownRequestData()
                .setBrokerId(10)
                .setBrokerEpoch(0L);
        return new ControlledShutdownRequest.Builder(
                data,
                ApiKeys.CONTROLLED_SHUTDOWN.latestVersion()).build((short) version);
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

    private LeaderAndIsrRequest createLeaderAndIsrRequest(int version) {
        Map<TopicPartition, LeaderAndIsrRequest.PartitionState> partitionStates = new HashMap<>();
        List<Integer> isr = asList(1, 2);
        List<Integer> replicas = asList(1, 2, 3, 4);
        partitionStates.put(new TopicPartition("topic5", 105),
                new LeaderAndIsrRequest.PartitionState(0, 2, 1, new ArrayList<>(isr), 2, replicas, false));
        partitionStates.put(new TopicPartition("topic5", 1),
                new LeaderAndIsrRequest.PartitionState(1, 1, 1, new ArrayList<>(isr), 2, replicas, false));
        partitionStates.put(new TopicPartition("topic20", 1),
                new LeaderAndIsrRequest.PartitionState(1, 0, 1, new ArrayList<>(isr), 2, replicas, false));

        Set<Node> leaders = Utils.mkSet(
                new Node(0, "test0", 1223),
                new Node(1, "test1", 1223)
        );
        return new LeaderAndIsrRequest.Builder((short) version, 1, 10, 0, partitionStates, leaders).build();
    }

    private LeaderAndIsrResponse createLeaderAndIsrResponse() {
        Map<TopicPartition, Errors> responses = new HashMap<>();
        responses.put(new TopicPartition("test", 0), Errors.NONE);
        return new LeaderAndIsrResponse(Errors.NONE, responses);
    }

    private UpdateMetadataRequest createUpdateMetadataRequest(int version, String rack) {
        Map<TopicPartition, UpdateMetadataRequest.PartitionState> partitionStates = new HashMap<>();
        List<Integer> isr = asList(1, 2);
        List<Integer> replicas = asList(1, 2, 3, 4);
        List<Integer> offlineReplicas = asList();
        partitionStates.put(new TopicPartition("topic5", 105),
            new UpdateMetadataRequest.PartitionState(0, 2, 1, isr, 2, replicas, offlineReplicas));
        partitionStates.put(new TopicPartition("topic5", 1),
            new UpdateMetadataRequest.PartitionState(1, 1, 1, isr, 2, replicas, offlineReplicas));
        partitionStates.put(new TopicPartition("topic20", 1),
            new UpdateMetadataRequest.PartitionState(1, 0, 1, isr, 2, replicas, offlineReplicas));

        SecurityProtocol plaintext = SecurityProtocol.PLAINTEXT;
        List<UpdateMetadataRequest.EndPoint> endPoints1 = new ArrayList<>();
        endPoints1.add(new UpdateMetadataRequest.EndPoint("host1", 1223, plaintext,
                ListenerName.forSecurityProtocol(plaintext)));

        List<UpdateMetadataRequest.EndPoint> endPoints2 = new ArrayList<>();
        endPoints2.add(new UpdateMetadataRequest.EndPoint("host1", 1244, plaintext,
                ListenerName.forSecurityProtocol(plaintext)));
        if (version > 0) {
            SecurityProtocol ssl = SecurityProtocol.SSL;
            endPoints2.add(new UpdateMetadataRequest.EndPoint("host2", 1234, ssl,
                    ListenerName.forSecurityProtocol(ssl)));
            endPoints2.add(new UpdateMetadataRequest.EndPoint("host2", 1334, ssl,
                    new ListenerName("CLIENT")));
        }

        Set<UpdateMetadataRequest.Broker> liveBrokers = Utils.mkSet(
                new UpdateMetadataRequest.Broker(0, endPoints1, rack),
                new UpdateMetadataRequest.Broker(1, endPoints2, rack)
        );
        return new UpdateMetadataRequest.Builder((short) version, 1, 10, 0, partitionStates,
                liveBrokers).build();
    }

    private UpdateMetadataResponse createUpdateMetadataResponse() {
        return new UpdateMetadataResponse(Errors.NONE);
    }

    private SaslHandshakeRequest createSaslHandshakeRequest() {
        return new SaslHandshakeRequest.Builder(
                new SaslHandshakeRequestData().setMechanism("PLAIN")).build();
    }

    private SaslHandshakeResponse createSaslHandshakeResponse() {
        return new SaslHandshakeResponse(
                new SaslHandshakeResponseData()
                .setErrorCode(Errors.NONE.code()).setMechanisms(singletonList("GSSAPI")));
    }

    private SaslAuthenticateRequest createSaslAuthenticateRequest() {
        SaslAuthenticateRequestData data = new SaslAuthenticateRequestData().setAuthBytes(new byte[0]);
        return new SaslAuthenticateRequest(data);
    }

    private SaslAuthenticateResponse createSaslAuthenticateResponse() {
        SaslAuthenticateResponseData data = new SaslAuthenticateResponseData()
                .setErrorCode(Errors.NONE.code())
                .setAuthBytes(new byte[0])
                .setSessionLifetimeMs(Long.MAX_VALUE);
        return new SaslAuthenticateResponse(data);
    }

    private ApiVersionsRequest createApiVersionRequest() {
        return new ApiVersionsRequest.Builder().build();
    }

    private ApiVersionsResponse createApiVersionResponse() {
        List<ApiVersionsResponse.ApiVersion> apiVersions = asList(new ApiVersionsResponse.ApiVersion((short) 0, (short) 0, (short) 2));
        return new ApiVersionsResponse(Errors.NONE, apiVersions);
    }

    private CreateTopicsRequest createCreateTopicRequest(int version) {
        return createCreateTopicRequest(version, version >= 1);
    }

    private CreateTopicsRequest createCreateTopicRequest(int version, boolean validateOnly) {
        CreateTopicsRequestData data = new CreateTopicsRequestData().
            setTimeoutMs(123).
            setValidateOnly(validateOnly);
        data.topics().add(new CreatableTopic().
            setNumPartitions(3).
            setReplicationFactor((short) 5));

        CreatableTopic topic2 = new CreatableTopic();
        data.topics().add(topic2);
        topic2.assignments().add(new CreatableReplicaAssignment().
            setPartitionIndex(0).
            setBrokerIds(Arrays.asList(1, 2, 3)));
        topic2.assignments().add(new CreatableReplicaAssignment().
            setPartitionIndex(1).
            setBrokerIds(Arrays.asList(2, 3, 4)));
        topic2.configs().add(new CreateableTopicConfig().
            setName("config1").setValue("value1"));

        return new CreateTopicsRequest.Builder(data).build((short) version);
    }

    private CreateTopicsResponse createCreateTopicResponse() {
        CreateTopicsResponseData data = new CreateTopicsResponseData();
        data.topics().add(new CreatableTopicResult().
            setName("t1").
            setErrorCode(Errors.INVALID_TOPIC_EXCEPTION.code()).
            setErrorMessage(null));
        data.topics().add(new CreatableTopicResult().
            setName("t2").
            setErrorCode(Errors.LEADER_NOT_AVAILABLE.code()).
            setErrorMessage("Leader with id 5 is not available."));
        return new CreateTopicsResponse(data);
    }

    private DeleteTopicsRequest createDeleteTopicsRequest() {
        return new DeleteTopicsRequest.Builder(
                new DeleteTopicsRequestData()
                .setTopicNames(Arrays.asList("my_t1", "my_t2"))
                .setTimeoutMs(1000)).build();
    }

    private DeleteTopicsResponse createDeleteTopicsResponse() {
        DeleteTopicsResponseData data = new DeleteTopicsResponseData();
        data.responses().add(new DeletableTopicResult()
                .setName("t1")
                .setErrorCode(Errors.INVALID_TOPIC_EXCEPTION.code()));
        data.responses().add(new DeletableTopicResult()
                .setName("t2")
                .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code()));
        return new DeleteTopicsResponse(data);
    }

    private InitProducerIdRequest createInitPidRequest() {
        InitProducerIdRequestData requestData = new InitProducerIdRequestData()
                .setTransactionalId(null)
                .setTransactionTimeoutMs(100);
        return new InitProducerIdRequest.Builder(requestData).build();
    }

    private InitProducerIdResponse createInitPidResponse() {
        InitProducerIdResponseData responseData = new InitProducerIdResponseData()
                .setErrorCode(Errors.NONE.code())
                .setProducerEpoch((short) 3)
                .setProducerId(3332)
                .setThrottleTimeMs(0);
        return new InitProducerIdResponse(responseData);
    }

    private Map<TopicPartition, OffsetsForLeaderEpochRequest.PartitionData> createOffsetForLeaderEpochPartitionData() {
        Map<TopicPartition, OffsetsForLeaderEpochRequest.PartitionData> epochs = new HashMap<>();
        epochs.put(new TopicPartition("topic1", 0),
                new OffsetsForLeaderEpochRequest.PartitionData(Optional.of(0), 1));
        epochs.put(new TopicPartition("topic1", 1),
                new OffsetsForLeaderEpochRequest.PartitionData(Optional.of(0), 1));
        epochs.put(new TopicPartition("topic2", 2),
                new OffsetsForLeaderEpochRequest.PartitionData(Optional.empty(), 3));
        return epochs;
    }

    private OffsetsForLeaderEpochRequest createLeaderEpochRequestForConsumer() {
        Map<TopicPartition, OffsetsForLeaderEpochRequest.PartitionData> epochs = createOffsetForLeaderEpochPartitionData();
        return OffsetsForLeaderEpochRequest.Builder.forConsumer(epochs).build();
    }

    private OffsetsForLeaderEpochRequest createLeaderEpochRequestForReplica(int version, int replicaId) {
        Map<TopicPartition, OffsetsForLeaderEpochRequest.PartitionData> epochs = createOffsetForLeaderEpochPartitionData();
        return OffsetsForLeaderEpochRequest.Builder.forFollower((short) version, epochs, replicaId).build();
    }

    private OffsetsForLeaderEpochResponse createLeaderEpochResponse() {
        Map<TopicPartition, EpochEndOffset> epochs = new HashMap<>();

        epochs.put(new TopicPartition("topic1", 0), new EpochEndOffset(Errors.NONE, 1, 0));
        epochs.put(new TopicPartition("topic1", 1), new EpochEndOffset(Errors.NONE, 1, 1));
        epochs.put(new TopicPartition("topic2", 2), new EpochEndOffset(Errors.NONE, 1, 2));

        return new OffsetsForLeaderEpochResponse(0, epochs);
    }

    private AddPartitionsToTxnRequest createAddPartitionsToTxnRequest() {
        return new AddPartitionsToTxnRequest.Builder("tid", 21L, (short) 42,
            Collections.singletonList(new TopicPartition("topic", 73))).build();
    }

    private AddPartitionsToTxnResponse createAddPartitionsToTxnResponse() {
        return new AddPartitionsToTxnResponse(0, Collections.singletonMap(new TopicPartition("t", 0), Errors.NONE));
    }

    private AddOffsetsToTxnRequest createAddOffsetsToTxnRequest() {
        return new AddOffsetsToTxnRequest.Builder("tid", 21L, (short) 42, "gid").build();
    }

    private AddOffsetsToTxnResponse createAddOffsetsToTxnResponse() {
        return new AddOffsetsToTxnResponse(0, Errors.NONE);
    }

    private EndTxnRequest createEndTxnRequest() {
        return new EndTxnRequest.Builder("tid", 21L, (short) 42, TransactionResult.COMMIT).build();
    }

    private EndTxnResponse createEndTxnResponse() {
        return new EndTxnResponse(0, Errors.NONE);
    }

    private WriteTxnMarkersRequest createWriteTxnMarkersRequest() {
        return new WriteTxnMarkersRequest.Builder(
            Collections.singletonList(new WriteTxnMarkersRequest.TxnMarkerEntry(21L, (short) 42, 73, TransactionResult.ABORT,
                                                                                Collections.singletonList(new TopicPartition("topic", 73))))).build();
    }

    private WriteTxnMarkersResponse createWriteTxnMarkersResponse() {
        final Map<TopicPartition, Errors> errorPerPartitions = new HashMap<>();
        errorPerPartitions.put(new TopicPartition("topic", 73), Errors.NONE);
        final Map<Long, Map<TopicPartition, Errors>> response = new HashMap<>();
        response.put(21L, errorPerPartitions);
        return new WriteTxnMarkersResponse(response);
    }

    private TxnOffsetCommitRequest createTxnOffsetCommitRequest() {
        final Map<TopicPartition, TxnOffsetCommitRequest.CommittedOffset> offsets = new HashMap<>();
        offsets.put(new TopicPartition("topic", 73),
                    new TxnOffsetCommitRequest.CommittedOffset(100, null, Optional.empty()));
        offsets.put(new TopicPartition("topic", 74),
                new TxnOffsetCommitRequest.CommittedOffset(100, "blah", Optional.of(27)));
        return new TxnOffsetCommitRequest.Builder("transactionalId", "groupId", 21L, (short) 42, offsets).build();
    }

    private TxnOffsetCommitResponse createTxnOffsetCommitResponse() {
        final Map<TopicPartition, Errors> errorPerPartitions = new HashMap<>();
        errorPerPartitions.put(new TopicPartition("topic", 73), Errors.NONE);
        return new TxnOffsetCommitResponse(0, errorPerPartitions);
    }

    private DescribeAclsRequest createListAclsRequest() {
        return new DescribeAclsRequest.Builder(new AclBindingFilter(
                new ResourcePatternFilter(ResourceType.TOPIC, "mytopic", PatternType.LITERAL),
                new AccessControlEntryFilter(null, null, AclOperation.ANY, AclPermissionType.ANY))).build();
    }

    private DescribeAclsResponse createDescribeAclsResponse() {
        return new DescribeAclsResponse(0, ApiError.NONE, Collections.singleton(new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, "mytopic", PatternType.LITERAL),
            new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.WRITE, AclPermissionType.ALLOW))));
    }

    private CreateAclsRequest createCreateAclsRequest() {
        List<AclCreation> creations = new ArrayList<>();
        creations.add(new AclCreation(new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, "mytopic", PatternType.LITERAL),
            new AccessControlEntry("User:ANONYMOUS", "127.0.0.1", AclOperation.READ, AclPermissionType.ALLOW))));
        creations.add(new AclCreation(new AclBinding(
            new ResourcePattern(ResourceType.GROUP, "mygroup", PatternType.LITERAL),
            new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.WRITE, AclPermissionType.DENY))));
        return new CreateAclsRequest.Builder(creations).build();
    }

    private CreateAclsResponse createCreateAclsResponse() {
        return new CreateAclsResponse(0, Arrays.asList(new AclCreationResponse(ApiError.NONE),
            new AclCreationResponse(new ApiError(Errors.INVALID_REQUEST, "Foo bar"))));
    }

    private DeleteAclsRequest createDeleteAclsRequest() {
        List<AclBindingFilter> filters = new ArrayList<>();
        filters.add(new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.ANY, null, PatternType.LITERAL),
            new AccessControlEntryFilter("User:ANONYMOUS", null, AclOperation.ANY, AclPermissionType.ANY)));
        filters.add(new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.ANY, null, PatternType.LITERAL),
            new AccessControlEntryFilter("User:bob", null, AclOperation.ANY, AclPermissionType.ANY)));
        return new DeleteAclsRequest.Builder(filters).build();
    }

    private DeleteAclsResponse createDeleteAclsResponse() {
        List<AclFilterResponse> responses = new ArrayList<>();
        responses.add(new AclFilterResponse(Utils.mkSet(
                new AclDeletionResult(new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, "mytopic3", PatternType.LITERAL),
                        new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW))),
                new AclDeletionResult(new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, "mytopic4", PatternType.LITERAL),
                        new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.DESCRIBE, AclPermissionType.DENY))))));
        responses.add(new AclFilterResponse(new ApiError(Errors.SECURITY_DISABLED, "No security"),
            Collections.<AclDeletionResult>emptySet()));
        return new DeleteAclsResponse(0, responses);
    }

    private DescribeConfigsRequest createDescribeConfigsRequest(int version) {
        return new DescribeConfigsRequest.Builder(asList(
                new ConfigResource(ConfigResource.Type.BROKER, "0"),
                new ConfigResource(ConfigResource.Type.TOPIC, "topic")))
                .build((short) version);
    }

    private DescribeConfigsRequest createDescribeConfigsRequestWithConfigEntries(int version) {
        Map<ConfigResource, Collection<String>> resources = new HashMap<>();
        resources.put(new ConfigResource(ConfigResource.Type.BROKER, "0"), asList("foo", "bar"));
        resources.put(new ConfigResource(ConfigResource.Type.TOPIC, "topic"), null);
        resources.put(new ConfigResource(ConfigResource.Type.TOPIC, "topic a"), Collections.emptyList());
        return new DescribeConfigsRequest.Builder(resources).build((short) version);
    }

    private DescribeConfigsResponse createDescribeConfigsResponse() {
        Map<ConfigResource, DescribeConfigsResponse.Config> configs = new HashMap<>();
        List<DescribeConfigsResponse.ConfigSynonym> synonyms = Collections.emptyList();
        List<DescribeConfigsResponse.ConfigEntry> configEntries = asList(
                new DescribeConfigsResponse.ConfigEntry("config_name", "config_value",
                        DescribeConfigsResponse.ConfigSource.DYNAMIC_BROKER_CONFIG, true, false, synonyms),
                new DescribeConfigsResponse.ConfigEntry("another_name", "another value",
                        DescribeConfigsResponse.ConfigSource.DEFAULT_CONFIG, false, true, synonyms)
        );
        configs.put(new ConfigResource(ConfigResource.Type.BROKER, "0"), new DescribeConfigsResponse.Config(
                ApiError.NONE, configEntries));
        configs.put(new ConfigResource(ConfigResource.Type.TOPIC, "topic"), new DescribeConfigsResponse.Config(
                ApiError.NONE, Collections.<DescribeConfigsResponse.ConfigEntry>emptyList()));
        return new DescribeConfigsResponse(200, configs);
    }

    private AlterConfigsRequest createAlterConfigsRequest() {
        Map<ConfigResource, AlterConfigsRequest.Config> configs = new HashMap<>();
        List<AlterConfigsRequest.ConfigEntry> configEntries = asList(
                new AlterConfigsRequest.ConfigEntry("config_name", "config_value"),
                new AlterConfigsRequest.ConfigEntry("another_name", "another value")
        );
        configs.put(new ConfigResource(ConfigResource.Type.BROKER, "0"), new AlterConfigsRequest.Config(configEntries));
        configs.put(new ConfigResource(ConfigResource.Type.TOPIC, "topic"),
                new AlterConfigsRequest.Config(Collections.<AlterConfigsRequest.ConfigEntry>emptyList()));
        return new AlterConfigsRequest((short) 0, configs, false);
    }

    private AlterConfigsResponse createAlterConfigsResponse() {
        Map<ConfigResource, ApiError> errors = new HashMap<>();
        errors.put(new ConfigResource(ConfigResource.Type.BROKER, "0"), ApiError.NONE);
        errors.put(new ConfigResource(ConfigResource.Type.TOPIC, "topic"), new ApiError(Errors.INVALID_REQUEST, "This request is invalid"));
        return new AlterConfigsResponse(20, errors);
    }

    private CreatePartitionsRequest createCreatePartitionsRequest() {
        Map<String, PartitionDetails> assignments = new HashMap<>();
        assignments.put("my_topic", new PartitionDetails(3));
        assignments.put("my_other_topic", new PartitionDetails(3));
        return new CreatePartitionsRequest(assignments, 0, false, (short) 0);
    }

    private CreatePartitionsRequest createCreatePartitionsRequestWithAssignments() {
        Map<String, PartitionDetails> assignments = new HashMap<>();
        assignments.put("my_topic", new PartitionDetails(3, asList(asList(2))));
        assignments.put("my_other_topic", new PartitionDetails(3, asList(asList(2, 3), asList(3, 1))));
        return new CreatePartitionsRequest(assignments, 0, false, (short) 0);
    }

    private CreatePartitionsResponse createCreatePartitionsResponse() {
        Map<String, ApiError> results = new HashMap<>();
        results.put("my_topic", ApiError.fromThrowable(
                new InvalidReplicaAssignmentException("The assigned brokers included an unknown broker")));
        results.put("my_topic", ApiError.NONE);
        return new CreatePartitionsResponse(42, results);
    }

    private CreateDelegationTokenRequest createCreateTokenRequest() {
        List<KafkaPrincipal> renewers = new ArrayList<>();
        renewers.add(SecurityUtils.parseKafkaPrincipal("User:user1"));
        renewers.add(SecurityUtils.parseKafkaPrincipal("User:user2"));
        return new CreateDelegationTokenRequest.Builder(renewers, System.currentTimeMillis()).build();
    }

    private CreateDelegationTokenResponse createCreateTokenResponse() {
        return new CreateDelegationTokenResponse(20, Errors.NONE, SecurityUtils.parseKafkaPrincipal("User:user1"), System.currentTimeMillis(),
            System.currentTimeMillis(), System.currentTimeMillis(), "token1", ByteBuffer.wrap("test".getBytes()));
    }

    private RenewDelegationTokenRequest createRenewTokenRequest() {
        return new RenewDelegationTokenRequest.Builder("test".getBytes(), System.currentTimeMillis()).build();
    }

    private RenewDelegationTokenResponse createRenewTokenResponse() {
        return new RenewDelegationTokenResponse(20, Errors.NONE, System.currentTimeMillis());
    }

    private ExpireDelegationTokenRequest createExpireTokenRequest() {
        return new ExpireDelegationTokenRequest.Builder("test".getBytes(), System.currentTimeMillis()).build();
    }

    private ExpireDelegationTokenResponse createExpireTokenResponse() {
        return new ExpireDelegationTokenResponse(20, Errors.NONE, System.currentTimeMillis());
    }

    private DescribeDelegationTokenRequest createDescribeTokenRequest() {
        List<KafkaPrincipal> owners = new ArrayList<>();
        owners.add(SecurityUtils.parseKafkaPrincipal("User:user1"));
        owners.add(SecurityUtils.parseKafkaPrincipal("User:user2"));
        return new DescribeDelegationTokenRequest.Builder(owners).build();
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

    private ElectPreferredLeadersRequest createElectPreferredLeadersRequestNullPartitions() {
        return new ElectPreferredLeadersRequest.Builder(
                new ElectPreferredLeadersRequestData()
                        .setTimeoutMs(100)
                        .setTopicPartitions(null))
                .build((short) 0);
    }

    private ElectPreferredLeadersRequest createElectPreferredLeadersRequest() {
        ElectPreferredLeadersRequestData data = new ElectPreferredLeadersRequestData()
                .setTimeoutMs(100);
        data.topicPartitions().add(new TopicPartitions().setTopic("data").setPartitionId(asList(1, 2)));
        return new ElectPreferredLeadersRequest.Builder(data).build((short) 0);
    }

    private ElectPreferredLeadersResponse createElectPreferredLeadersResponse() {
        ElectPreferredLeadersResponseData data = new ElectPreferredLeadersResponseData().setThrottleTimeMs(200);
        ReplicaElectionResult resultsByTopic = new ReplicaElectionResult().setTopic("myTopic");
        resultsByTopic.partitionResult().add(new PartitionResult().setPartitionId(0)
                .setErrorCode(Errors.NONE.code())
                .setErrorMessage(Errors.NONE.message()));
        resultsByTopic.partitionResult().add(new PartitionResult().setPartitionId(1)
                .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                .setErrorMessage(Errors.UNKNOWN_TOPIC_OR_PARTITION.message()));
        data.replicaElectionResults().add(resultsByTopic);
        return new ElectPreferredLeadersResponse(data);
    }

    private IncrementalAlterConfigsRequest createIncrementalAlterConfigsRequest() {
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
        return new IncrementalAlterConfigsRequest.Builder(data).build((short) 0);
    }

    private IncrementalAlterConfigsResponse createIncrementalAlterConfigsResponse() {
        IncrementalAlterConfigsResponseData data = new IncrementalAlterConfigsResponseData();

        data.responses().add(new AlterConfigsResourceResult()
                .setResourceName("testtopic")
                .setResourceType(ResourceType.TOPIC.code())
                .setErrorCode(Errors.INVALID_REQUEST.code())
                .setErrorMessage("Duplicate Keys"));
        return new IncrementalAlterConfigsResponse(data);
    }
}
