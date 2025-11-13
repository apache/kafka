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

import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.protocol.SendBuilder;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class AbstractResponse implements AbstractRequestResponse {
    public static final int DEFAULT_THROTTLE_TIME = 0;

    private final ApiKeys apiKey;

    protected AbstractResponse(ApiKeys apiKey) {
        this.apiKey = apiKey;
    }

    public final Send toSend(ResponseHeader header, short version) {
        return SendBuilder.buildResponseSend(header, data(), version);
    }

    /**
     * Serializes header and body without prefixing with size (unlike `toSend`, which does include a size prefix).
     */
    final ByteBuffer serializeWithHeader(ResponseHeader header, short version) {
        return RequestUtils.serialize(header.data(), header.headerVersion(), data(), version);
    }

    // Visible for testing
    final ByteBufferAccessor serialize(short version) {
        return MessageUtil.toByteBufferAccessor(data(), version);
    }

    /**
     * The number of each type of error in the response, including {@link Errors#NONE} and top-level errors as well as
     * more specifically scoped errors (such as topic or partition-level errors).
     * @return A count of errors.
     */
    public abstract Map<Errors, Integer> errorCounts();

    protected static Map<Errors, Integer> errorCounts(Errors error) {
        return Collections.singletonMap(error, 1);
    }

    protected static Map<Errors, Integer> errorCounts(Stream<Errors> errors) {
        return errors.collect(Collectors.groupingBy(e -> e, Collectors.summingInt(e -> 1)));
    }

    protected static Map<Errors, Integer> errorCounts(Collection<Errors> errors) {
        Map<Errors, Integer> errorCounts = new EnumMap<>(Errors.class);
        for (Errors error : errors)
            updateErrorCounts(errorCounts, error);
        return errorCounts;
    }

    protected static Map<Errors, Integer> apiErrorCounts(Map<?, ApiError> errors) {
        Map<Errors, Integer> errorCounts = new EnumMap<>(Errors.class);
        for (ApiError apiError : errors.values())
            updateErrorCounts(errorCounts, apiError.error());
        return errorCounts;
    }

    protected static void updateErrorCounts(Map<Errors, Integer> errorCounts, Errors error) {
        Integer count = errorCounts.getOrDefault(error, 0);
        errorCounts.put(error, count + 1);
    }

    /**
     * Parse a response from the provided buffer. The buffer is expected to hold both
     * the {@link ResponseHeader} as well as the response payload.
     */
    public static AbstractResponse parseResponse(ByteBuffer buffer, RequestHeader requestHeader) {
        ApiKeys apiKey = requestHeader.apiKey();
        short apiVersion = requestHeader.apiVersion();

        ResponseHeader responseHeader = ResponseHeader.parse(buffer, apiKey.responseHeaderVersion(apiVersion));

        if (requestHeader.correlationId() != responseHeader.correlationId()) {
            throw new CorrelationIdMismatchException("Correlation id for response ("
                + responseHeader.correlationId() + ") does not match request ("
                + requestHeader.correlationId() + "), request header: " + requestHeader,
                requestHeader.correlationId(), responseHeader.correlationId());
        }

        return AbstractResponse.parseResponse(apiKey, new ByteBufferAccessor(buffer), apiVersion);
    }

    public static AbstractResponse parseResponse(ApiKeys apiKey, Readable readable, short version) {
        switch (apiKey) {
            case PRODUCE:
                return ProduceResponse.parse(readable, version);
            case FETCH:
                return FetchResponse.parse(readable, version);
            case LIST_OFFSETS:
                return ListOffsetsResponse.parse(readable, version);
            case METADATA:
                return MetadataResponse.parse(readable, version);
            case OFFSET_COMMIT:
                return OffsetCommitResponse.parse(readable, version);
            case OFFSET_FETCH:
                return OffsetFetchResponse.parse(readable, version);
            case FIND_COORDINATOR:
                return FindCoordinatorResponse.parse(readable, version);
            case JOIN_GROUP:
                return JoinGroupResponse.parse(readable, version);
            case HEARTBEAT:
                return HeartbeatResponse.parse(readable, version);
            case LEAVE_GROUP:
                return LeaveGroupResponse.parse(readable, version);
            case SYNC_GROUP:
                return SyncGroupResponse.parse(readable, version);
            case DESCRIBE_GROUPS:
                return DescribeGroupsResponse.parse(readable, version);
            case LIST_GROUPS:
                return ListGroupsResponse.parse(readable, version);
            case SASL_HANDSHAKE:
                return SaslHandshakeResponse.parse(readable, version);
            case API_VERSIONS:
                return ApiVersionsResponse.parse(readable, version);
            case CREATE_TOPICS:
                return CreateTopicsResponse.parse(readable, version);
            case DELETE_TOPICS:
                return DeleteTopicsResponse.parse(readable, version);
            case DELETE_RECORDS:
                return DeleteRecordsResponse.parse(readable, version);
            case INIT_PRODUCER_ID:
                return InitProducerIdResponse.parse(readable, version);
            case OFFSET_FOR_LEADER_EPOCH:
                return OffsetsForLeaderEpochResponse.parse(readable, version);
            case ADD_PARTITIONS_TO_TXN:
                return AddPartitionsToTxnResponse.parse(readable, version);
            case ADD_OFFSETS_TO_TXN:
                return AddOffsetsToTxnResponse.parse(readable, version);
            case END_TXN:
                return EndTxnResponse.parse(readable, version);
            case WRITE_TXN_MARKERS:
                return WriteTxnMarkersResponse.parse(readable, version);
            case TXN_OFFSET_COMMIT:
                return TxnOffsetCommitResponse.parse(readable, version);
            case DESCRIBE_ACLS:
                return DescribeAclsResponse.parse(readable, version);
            case CREATE_ACLS:
                return CreateAclsResponse.parse(readable, version);
            case DELETE_ACLS:
                return DeleteAclsResponse.parse(readable, version);
            case DESCRIBE_CONFIGS:
                return DescribeConfigsResponse.parse(readable, version);
            case ALTER_CONFIGS:
                return AlterConfigsResponse.parse(readable, version);
            case ALTER_REPLICA_LOG_DIRS:
                return AlterReplicaLogDirsResponse.parse(readable, version);
            case DESCRIBE_LOG_DIRS:
                return DescribeLogDirsResponse.parse(readable, version);
            case SASL_AUTHENTICATE:
                return SaslAuthenticateResponse.parse(readable, version);
            case CREATE_PARTITIONS:
                return CreatePartitionsResponse.parse(readable, version);
            case CREATE_DELEGATION_TOKEN:
                return CreateDelegationTokenResponse.parse(readable, version);
            case RENEW_DELEGATION_TOKEN:
                return RenewDelegationTokenResponse.parse(readable, version);
            case EXPIRE_DELEGATION_TOKEN:
                return ExpireDelegationTokenResponse.parse(readable, version);
            case DESCRIBE_DELEGATION_TOKEN:
                return DescribeDelegationTokenResponse.parse(readable, version);
            case DELETE_GROUPS:
                return DeleteGroupsResponse.parse(readable, version);
            case ELECT_LEADERS:
                return ElectLeadersResponse.parse(readable, version);
            case INCREMENTAL_ALTER_CONFIGS:
                return IncrementalAlterConfigsResponse.parse(readable, version);
            case ALTER_PARTITION_REASSIGNMENTS:
                return AlterPartitionReassignmentsResponse.parse(readable, version);
            case LIST_PARTITION_REASSIGNMENTS:
                return ListPartitionReassignmentsResponse.parse(readable, version);
            case OFFSET_DELETE:
                return OffsetDeleteResponse.parse(readable, version);
            case DESCRIBE_CLIENT_QUOTAS:
                return DescribeClientQuotasResponse.parse(readable, version);
            case ALTER_CLIENT_QUOTAS:
                return AlterClientQuotasResponse.parse(readable, version);
            case DESCRIBE_USER_SCRAM_CREDENTIALS:
                return DescribeUserScramCredentialsResponse.parse(readable, version);
            case ALTER_USER_SCRAM_CREDENTIALS:
                return AlterUserScramCredentialsResponse.parse(readable, version);
            case VOTE:
                return VoteResponse.parse(readable, version);
            case BEGIN_QUORUM_EPOCH:
                return BeginQuorumEpochResponse.parse(readable, version);
            case END_QUORUM_EPOCH:
                return EndQuorumEpochResponse.parse(readable, version);
            case DESCRIBE_QUORUM:
                return DescribeQuorumResponse.parse(readable, version);
            case ALTER_PARTITION:
                return AlterPartitionResponse.parse(readable, version);
            case UPDATE_FEATURES:
                return UpdateFeaturesResponse.parse(readable, version);
            case ENVELOPE:
                return EnvelopeResponse.parse(readable, version);
            case FETCH_SNAPSHOT:
                return FetchSnapshotResponse.parse(readable, version);
            case DESCRIBE_CLUSTER:
                return DescribeClusterResponse.parse(readable, version);
            case DESCRIBE_PRODUCERS:
                return DescribeProducersResponse.parse(readable, version);
            case BROKER_REGISTRATION:
                return BrokerRegistrationResponse.parse(readable, version);
            case BROKER_HEARTBEAT:
                return BrokerHeartbeatResponse.parse(readable, version);
            case UNREGISTER_BROKER:
                return UnregisterBrokerResponse.parse(readable, version);
            case DESCRIBE_TRANSACTIONS:
                return DescribeTransactionsResponse.parse(readable, version);
            case LIST_TRANSACTIONS:
                return ListTransactionsResponse.parse(readable, version);
            case ALLOCATE_PRODUCER_IDS:
                return AllocateProducerIdsResponse.parse(readable, version);
            case CONSUMER_GROUP_HEARTBEAT:
                return ConsumerGroupHeartbeatResponse.parse(readable, version);
            case CONSUMER_GROUP_DESCRIBE:
                return ConsumerGroupDescribeResponse.parse(readable, version);
            case CONTROLLER_REGISTRATION:
                return ControllerRegistrationResponse.parse(readable, version);
            case GET_TELEMETRY_SUBSCRIPTIONS:
                return GetTelemetrySubscriptionsResponse.parse(readable, version);
            case PUSH_TELEMETRY:
                return PushTelemetryResponse.parse(readable, version);
            case ASSIGN_REPLICAS_TO_DIRS:
                return AssignReplicasToDirsResponse.parse(readable, version);
            case LIST_CONFIG_RESOURCES:
                return ListConfigResourcesResponse.parse(readable, version);
            case DESCRIBE_TOPIC_PARTITIONS:
                return DescribeTopicPartitionsResponse.parse(readable, version);
            case SHARE_GROUP_HEARTBEAT:
                return ShareGroupHeartbeatResponse.parse(readable, version);
            case SHARE_GROUP_DESCRIBE:
                return ShareGroupDescribeResponse.parse(readable, version);
            case SHARE_FETCH:
                return ShareFetchResponse.parse(readable, version);
            case SHARE_ACKNOWLEDGE:
                return ShareAcknowledgeResponse.parse(readable, version);
            case ADD_RAFT_VOTER:
                return AddRaftVoterResponse.parse(readable, version);
            case REMOVE_RAFT_VOTER:
                return RemoveRaftVoterResponse.parse(readable, version);
            case UPDATE_RAFT_VOTER:
                return UpdateRaftVoterResponse.parse(readable, version);
            case INITIALIZE_SHARE_GROUP_STATE:
                return InitializeShareGroupStateResponse.parse(readable, version);
            case READ_SHARE_GROUP_STATE:
                return ReadShareGroupStateResponse.parse(readable, version);
            case WRITE_SHARE_GROUP_STATE:
                return WriteShareGroupStateResponse.parse(readable, version);
            case DELETE_SHARE_GROUP_STATE:
                return DeleteShareGroupStateResponse.parse(readable, version);
            case READ_SHARE_GROUP_STATE_SUMMARY:
                return ReadShareGroupStateSummaryResponse.parse(readable, version);
            case STREAMS_GROUP_HEARTBEAT:
                return StreamsGroupHeartbeatResponse.parse(readable, version);
            case STREAMS_GROUP_DESCRIBE:
                return StreamsGroupDescribeResponse.parse(readable, version);
            case DESCRIBE_SHARE_GROUP_OFFSETS:
                return DescribeShareGroupOffsetsResponse.parse(readable, version);
            case ALTER_SHARE_GROUP_OFFSETS:
                return AlterShareGroupOffsetsResponse.parse(readable, version);
            case DELETE_SHARE_GROUP_OFFSETS:
                return DeleteShareGroupOffsetsResponse.parse(readable, version);
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

    public ApiKeys apiKey() {
        return apiKey;
    }

    /**
     * Get the throttle time in milliseconds. If the response schema does not
     * support this field, then 0 will be returned.
     */
    public abstract int throttleTimeMs();

    /**
     * Set the throttle time in the response if the schema supports it. Otherwise,
     * this is a no-op.
     *
     * @param throttleTimeMs The throttle time in milliseconds
     */
    public abstract void maybeSetThrottleTimeMs(int throttleTimeMs);

    public String toString() {
        return data().toString();
    }
}
