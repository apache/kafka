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

import org.apache.kafka.common.message.EnvelopeResponseData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.AlterIsrResponseData;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class AbstractResponse implements AbstractRequestResponse {
    public static final int DEFAULT_THROTTLE_TIME = 0;

    protected Send toSend(String destination, ResponseHeader header, short apiVersion) {
        return new NetworkSend(destination, RequestUtils.serialize(header.toStruct(), toStruct(apiVersion)));
    }

    /**
     * Visible for testing, typically {@link #toSend(String, ResponseHeader, short)} should be used instead.
     */
    public ByteBuffer serialize(ApiKeys apiKey, short version, int correlationId) {
        ResponseHeader header =
            new ResponseHeader(correlationId, apiKey.responseHeaderVersion(version));
        return RequestUtils.serialize(header.toStruct(), toStruct(version));
    }

    public abstract Map<Errors, Integer> errorCounts();

    protected Map<Errors, Integer> errorCounts(Errors error) {
        return Collections.singletonMap(error, 1);
    }

    protected Map<Errors, Integer> errorCounts(Stream<Errors> errors) {
        return errors.collect(Collectors.groupingBy(e -> e, Collectors.summingInt(e -> 1)));
    }

    protected Map<Errors, Integer> errorCounts(Collection<Errors> errors) {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        for (Errors error : errors)
            updateErrorCounts(errorCounts, error);
        return errorCounts;
    }

    protected Map<Errors, Integer> apiErrorCounts(Map<?, ApiError> errors) {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        for (ApiError apiError : errors.values())
            updateErrorCounts(errorCounts, apiError.error());
        return errorCounts;
    }

    protected void updateErrorCounts(Map<Errors, Integer> errorCounts, Errors error) {
        Integer count = errorCounts.getOrDefault(error, 0);
        errorCounts.put(error, count + 1);
    }

    protected abstract Struct toStruct(short version);

    /**
     * Parse a response from the provided buffer. The buffer is expected to hold both
     * the {@link ResponseHeader} as well as the response payload.
     */
    public static AbstractResponse parseResponse(ByteBuffer byteBuffer, RequestHeader header) {
        ApiKeys apiKey = header.apiKey();
        short apiVersion = header.apiVersion();

        ResponseHeader.parse(byteBuffer, apiKey.responseHeaderVersion(apiVersion));
        Struct struct = apiKey.parseResponse(apiVersion, byteBuffer);
        return AbstractResponse.parseResponse(apiKey, struct, apiVersion);
    }

    public static AbstractResponse parseResponse(ApiKeys apiKey, Struct struct, short version) {
        switch (apiKey) {
            case PRODUCE:
                return new ProduceResponse(struct);
            case FETCH:
                return new FetchResponse<>(new FetchResponseData(struct, version));
            case LIST_OFFSETS:
                return new ListOffsetResponse(struct, version);
            case METADATA:
                return new MetadataResponse(struct, version);
            case OFFSET_COMMIT:
                return new OffsetCommitResponse(struct, version);
            case OFFSET_FETCH:
                return new OffsetFetchResponse(struct, version);
            case FIND_COORDINATOR:
                return new FindCoordinatorResponse(struct, version);
            case JOIN_GROUP:
                return new JoinGroupResponse(struct, version);
            case HEARTBEAT:
                return new HeartbeatResponse(struct, version);
            case LEAVE_GROUP:
                return new LeaveGroupResponse(struct, version);
            case SYNC_GROUP:
                return new SyncGroupResponse(struct, version);
            case STOP_REPLICA:
                return new StopReplicaResponse(struct, version);
            case CONTROLLED_SHUTDOWN:
                return new ControlledShutdownResponse(struct, version);
            case UPDATE_METADATA:
                return new UpdateMetadataResponse(struct, version);
            case LEADER_AND_ISR:
                return new LeaderAndIsrResponse(struct, version);
            case DESCRIBE_GROUPS:
                return new DescribeGroupsResponse(struct, version);
            case LIST_GROUPS:
                return new ListGroupsResponse(struct, version);
            case SASL_HANDSHAKE:
                return new SaslHandshakeResponse(struct, version);
            case API_VERSIONS:
                return ApiVersionsResponse.fromStruct(struct, version);
            case CREATE_TOPICS:
                return new CreateTopicsResponse(struct, version);
            case DELETE_TOPICS:
                return new DeleteTopicsResponse(struct, version);
            case DELETE_RECORDS:
                return new DeleteRecordsResponse(struct, version);
            case INIT_PRODUCER_ID:
                return new InitProducerIdResponse(struct, version);
            case OFFSET_FOR_LEADER_EPOCH:
                return new OffsetsForLeaderEpochResponse(struct);
            case ADD_PARTITIONS_TO_TXN:
                return new AddPartitionsToTxnResponse(struct, version);
            case ADD_OFFSETS_TO_TXN:
                return new AddOffsetsToTxnResponse(struct, version);
            case END_TXN:
                return new EndTxnResponse(struct, version);
            case WRITE_TXN_MARKERS:
                return new WriteTxnMarkersResponse(struct, version);
            case TXN_OFFSET_COMMIT:
                return new TxnOffsetCommitResponse(struct, version);
            case DESCRIBE_ACLS:
                return new DescribeAclsResponse(struct, version);
            case CREATE_ACLS:
                return new CreateAclsResponse(struct, version);
            case DELETE_ACLS:
                return new DeleteAclsResponse(struct, version);
            case DESCRIBE_CONFIGS:
                return new DescribeConfigsResponse(struct, version);
            case ALTER_CONFIGS:
                return new AlterConfigsResponse(struct, version);
            case ALTER_REPLICA_LOG_DIRS:
                return new AlterReplicaLogDirsResponse(struct);
            case DESCRIBE_LOG_DIRS:
                return new DescribeLogDirsResponse(struct, version);
            case SASL_AUTHENTICATE:
                return new SaslAuthenticateResponse(struct, version);
            case CREATE_PARTITIONS:
                return new CreatePartitionsResponse(struct, version);
            case CREATE_DELEGATION_TOKEN:
                return new CreateDelegationTokenResponse(struct, version);
            case RENEW_DELEGATION_TOKEN:
                return new RenewDelegationTokenResponse(struct, version);
            case EXPIRE_DELEGATION_TOKEN:
                return new ExpireDelegationTokenResponse(struct, version);
            case DESCRIBE_DELEGATION_TOKEN:
                return new DescribeDelegationTokenResponse(struct, version);
            case DELETE_GROUPS:
                return new DeleteGroupsResponse(struct, version);
            case ELECT_LEADERS:
                return new ElectLeadersResponse(struct, version);
            case INCREMENTAL_ALTER_CONFIGS:
                return new IncrementalAlterConfigsResponse(struct, version);
            case ALTER_PARTITION_REASSIGNMENTS:
                return new AlterPartitionReassignmentsResponse(struct, version);
            case LIST_PARTITION_REASSIGNMENTS:
                return new ListPartitionReassignmentsResponse(struct, version);
            case OFFSET_DELETE:
                return new OffsetDeleteResponse(struct, version);
            case DESCRIBE_CLIENT_QUOTAS:
                return new DescribeClientQuotasResponse(struct, version);
            case ALTER_CLIENT_QUOTAS:
                return new AlterClientQuotasResponse(struct, version);
            case DESCRIBE_USER_SCRAM_CREDENTIALS:
                return new DescribeUserScramCredentialsResponse(struct, version);
            case ALTER_USER_SCRAM_CREDENTIALS:
                return new AlterUserScramCredentialsResponse(struct, version);
            case VOTE:
                return new VoteResponse(struct, version);
            case BEGIN_QUORUM_EPOCH:
                return new BeginQuorumEpochResponse(struct, version);
            case END_QUORUM_EPOCH:
                return new EndQuorumEpochResponse(struct, version);
            case DESCRIBE_QUORUM:
                return new DescribeQuorumResponse(struct, version);
            case ALTER_ISR:
                return new AlterIsrResponse(new AlterIsrResponseData(struct, version));
            case UPDATE_FEATURES:
                return new UpdateFeaturesResponse(struct, version);
            case ENVELOPE:
                return new EnvelopeResponse(new EnvelopeResponseData(struct, version));
            default:
                throw new AssertionError(String.format("ApiKey %s is not currently handled in `parseResponse`, the " +
                        "code should be updated to do so.", apiKey));
        }
    }

    /**
     * Returns whether or not client should throttle upon receiving a response of the specified version with a non-zero
     * throttle time. Client-side throttling is needed when communicating with a newer version of broker which, on
     * quota violation, sends out responses before throttling.
     */
    public boolean shouldClientThrottle(short version) {
        return false;
    }

    public int throttleTimeMs() {
        return DEFAULT_THROTTLE_TIME;
    }

    public String toString(short version) {
        return toStruct(version).toString();
    }
}
