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

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.protocol.SendBuilder;

import java.nio.ByteBuffer;
import java.util.Map;

public abstract class AbstractRequest implements AbstractRequestResponse {

    public abstract static class Builder<T extends AbstractRequest> {
        private final ApiKeys apiKey;
        private final short oldestAllowedVersion;
        private final short latestAllowedVersion;

        /**
         * Construct a new builder which allows any supported version
         */
        public Builder(ApiKeys apiKey, boolean enableUnstableLastVersion) {
            this(apiKey, apiKey.oldestVersion(), apiKey.latestVersion(enableUnstableLastVersion));
        }

        /**
         * Construct a new builder which allows any supported and released version
         */
        public Builder(ApiKeys apiKey) {
            this(apiKey, false);
        }

        /**
         * Construct a new builder which allows only a specific version
         */
        public Builder(ApiKeys apiKey, short allowedVersion) {
            this(apiKey, allowedVersion, allowedVersion);
        }

        /**
         * Construct a new builder which allows an inclusive range of versions
         */
        public Builder(ApiKeys apiKey, short oldestAllowedVersion, short latestAllowedVersion) {
            this.apiKey = apiKey;
            this.oldestAllowedVersion = oldestAllowedVersion;
            this.latestAllowedVersion = latestAllowedVersion;
        }

        public ApiKeys apiKey() {
            return apiKey;
        }

        public short oldestAllowedVersion() {
            return oldestAllowedVersion;
        }

        public short latestAllowedVersion() {
            return latestAllowedVersion;
        }

        public T build() {
            return build(latestAllowedVersion());
        }

        public abstract T build(short version);
    }

    private final short version;
    private final ApiKeys apiKey;

    public AbstractRequest(ApiKeys apiKey, short version) {
        if (!apiKey.isVersionSupported(version))
            throw new UnsupportedVersionException("The " + apiKey + " protocol does not support version " + version);
        this.version = version;
        this.apiKey = apiKey;
    }

    /**
     * Get the version of this AbstractRequest object.
     */
    public short version() {
        return version;
    }

    public ApiKeys apiKey() {
        return apiKey;
    }

    public final Send toSend(RequestHeader header) {
        return SendBuilder.buildRequestSend(header, data());
    }

    /**
     * Serializes header and body without prefixing with size (unlike `toSend`, which does include a size prefix).
     */
    public final ByteBuffer serializeWithHeader(RequestHeader header) {
        if (header.apiKey() != apiKey) {
            throw new IllegalArgumentException("Could not build request " + apiKey + " with header api key " + header.apiKey());
        }
        if (header.apiVersion() != version) {
            throw new IllegalArgumentException("Could not build request version " + version + " with header version " + header.apiVersion());
        }
        return RequestUtils.serialize(header.data(), header.headerVersion(), data(), version);
    }

    // Visible for testing
    public final ByteBufferAccessor serialize() {
        return MessageUtil.toByteBufferAccessor(data(), version);
    }

    // Visible for testing
    final int sizeInBytes() {
        return data().size(new ObjectSerializationCache(), version);
    }

    public String toString(boolean verbose) {
        return data().toString();
    }

    @Override
    public String toString() {
        return toString(true);
    }

    /**
     * Get an error response for a request
     */
    public AbstractResponse getErrorResponse(Throwable e) {
        return getErrorResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, e);
    }

    /**
     * Get an error response for a request with specified throttle time in the response if applicable
     */
    public abstract AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e);

    /**
     * Get the error counts corresponding to an error response. This is overridden for requests
     * where response may be null (e.g produce with acks=0).
     */
    public Map<Errors, Integer> errorCounts(Throwable e) {
        AbstractResponse response = getErrorResponse(0, e);
        if (response == null)
            throw new IllegalStateException("Error counts could not be obtained for request " + this);
        else
            return response.errorCounts();
    }

    /**
     * Factory method for getting a request object based on ApiKey ID and a version
     */
    public static RequestAndSize parseRequest(ApiKeys apiKey, short apiVersion, Readable readable) {
        int bufferSize = readable.remaining();
        return new RequestAndSize(doParseRequest(apiKey, apiVersion, readable), bufferSize);
    }

    private static AbstractRequest doParseRequest(ApiKeys apiKey, short apiVersion, Readable readable) {
        switch (apiKey) {
            case PRODUCE:
                return ProduceRequest.parse(readable, apiVersion);
            case FETCH:
                return FetchRequest.parse(readable, apiVersion);
            case LIST_OFFSETS:
                return ListOffsetsRequest.parse(readable, apiVersion);
            case METADATA:
                return MetadataRequest.parse(readable, apiVersion);
            case OFFSET_COMMIT:
                return OffsetCommitRequest.parse(readable, apiVersion);
            case OFFSET_FETCH:
                return OffsetFetchRequest.parse(readable, apiVersion);
            case FIND_COORDINATOR:
                return FindCoordinatorRequest.parse(readable, apiVersion);
            case JOIN_GROUP:
                return JoinGroupRequest.parse(readable, apiVersion);
            case HEARTBEAT:
                return HeartbeatRequest.parse(readable, apiVersion);
            case LEAVE_GROUP:
                return LeaveGroupRequest.parse(readable, apiVersion);
            case SYNC_GROUP:
                return SyncGroupRequest.parse(readable, apiVersion);
            case DESCRIBE_GROUPS:
                return DescribeGroupsRequest.parse(readable, apiVersion);
            case LIST_GROUPS:
                return ListGroupsRequest.parse(readable, apiVersion);
            case SASL_HANDSHAKE:
                return SaslHandshakeRequest.parse(readable, apiVersion);
            case API_VERSIONS:
                return ApiVersionsRequest.parse(readable, apiVersion);
            case CREATE_TOPICS:
                return CreateTopicsRequest.parse(readable, apiVersion);
            case DELETE_TOPICS:
                return DeleteTopicsRequest.parse(readable, apiVersion);
            case DELETE_RECORDS:
                return DeleteRecordsRequest.parse(readable, apiVersion);
            case INIT_PRODUCER_ID:
                return InitProducerIdRequest.parse(readable, apiVersion);
            case OFFSET_FOR_LEADER_EPOCH:
                return OffsetsForLeaderEpochRequest.parse(readable, apiVersion);
            case ADD_PARTITIONS_TO_TXN:
                return AddPartitionsToTxnRequest.parse(readable, apiVersion);
            case ADD_OFFSETS_TO_TXN:
                return AddOffsetsToTxnRequest.parse(readable, apiVersion);
            case END_TXN:
                return EndTxnRequest.parse(readable, apiVersion);
            case WRITE_TXN_MARKERS:
                return WriteTxnMarkersRequest.parse(readable, apiVersion);
            case TXN_OFFSET_COMMIT:
                return TxnOffsetCommitRequest.parse(readable, apiVersion);
            case DESCRIBE_ACLS:
                return DescribeAclsRequest.parse(readable, apiVersion);
            case CREATE_ACLS:
                return CreateAclsRequest.parse(readable, apiVersion);
            case DELETE_ACLS:
                return DeleteAclsRequest.parse(readable, apiVersion);
            case DESCRIBE_CONFIGS:
                return DescribeConfigsRequest.parse(readable, apiVersion);
            case ALTER_CONFIGS:
                return AlterConfigsRequest.parse(readable, apiVersion);
            case ALTER_REPLICA_LOG_DIRS:
                return AlterReplicaLogDirsRequest.parse(readable, apiVersion);
            case DESCRIBE_LOG_DIRS:
                return DescribeLogDirsRequest.parse(readable, apiVersion);
            case SASL_AUTHENTICATE:
                return SaslAuthenticateRequest.parse(readable, apiVersion);
            case CREATE_PARTITIONS:
                return CreatePartitionsRequest.parse(readable, apiVersion);
            case CREATE_DELEGATION_TOKEN:
                return CreateDelegationTokenRequest.parse(readable, apiVersion);
            case RENEW_DELEGATION_TOKEN:
                return RenewDelegationTokenRequest.parse(readable, apiVersion);
            case EXPIRE_DELEGATION_TOKEN:
                return ExpireDelegationTokenRequest.parse(readable, apiVersion);
            case DESCRIBE_DELEGATION_TOKEN:
                return DescribeDelegationTokenRequest.parse(readable, apiVersion);
            case DELETE_GROUPS:
                return DeleteGroupsRequest.parse(readable, apiVersion);
            case ELECT_LEADERS:
                return ElectLeadersRequest.parse(readable, apiVersion);
            case INCREMENTAL_ALTER_CONFIGS:
                return IncrementalAlterConfigsRequest.parse(readable, apiVersion);
            case ALTER_PARTITION_REASSIGNMENTS:
                return AlterPartitionReassignmentsRequest.parse(readable, apiVersion);
            case LIST_PARTITION_REASSIGNMENTS:
                return ListPartitionReassignmentsRequest.parse(readable, apiVersion);
            case OFFSET_DELETE:
                return OffsetDeleteRequest.parse(readable, apiVersion);
            case DESCRIBE_CLIENT_QUOTAS:
                return DescribeClientQuotasRequest.parse(readable, apiVersion);
            case ALTER_CLIENT_QUOTAS:
                return AlterClientQuotasRequest.parse(readable, apiVersion);
            case DESCRIBE_USER_SCRAM_CREDENTIALS:
                return DescribeUserScramCredentialsRequest.parse(readable, apiVersion);
            case ALTER_USER_SCRAM_CREDENTIALS:
                return AlterUserScramCredentialsRequest.parse(readable, apiVersion);
            case VOTE:
                return VoteRequest.parse(readable, apiVersion);
            case BEGIN_QUORUM_EPOCH:
                return BeginQuorumEpochRequest.parse(readable, apiVersion);
            case END_QUORUM_EPOCH:
                return EndQuorumEpochRequest.parse(readable, apiVersion);
            case DESCRIBE_QUORUM:
                return DescribeQuorumRequest.parse(readable, apiVersion);
            case ALTER_PARTITION:
                return AlterPartitionRequest.parse(readable, apiVersion);
            case UPDATE_FEATURES:
                return UpdateFeaturesRequest.parse(readable, apiVersion);
            case ENVELOPE:
                return EnvelopeRequest.parse(readable, apiVersion);
            case FETCH_SNAPSHOT:
                return FetchSnapshotRequest.parse(readable, apiVersion);
            case DESCRIBE_CLUSTER:
                return DescribeClusterRequest.parse(readable, apiVersion);
            case DESCRIBE_PRODUCERS:
                return DescribeProducersRequest.parse(readable, apiVersion);
            case BROKER_REGISTRATION:
                return BrokerRegistrationRequest.parse(readable, apiVersion);
            case BROKER_HEARTBEAT:
                return BrokerHeartbeatRequest.parse(readable, apiVersion);
            case UNREGISTER_BROKER:
                return UnregisterBrokerRequest.parse(readable, apiVersion);
            case DESCRIBE_TRANSACTIONS:
                return DescribeTransactionsRequest.parse(readable, apiVersion);
            case LIST_TRANSACTIONS:
                return ListTransactionsRequest.parse(readable, apiVersion);
            case ALLOCATE_PRODUCER_IDS:
                return AllocateProducerIdsRequest.parse(readable, apiVersion);
            case CONSUMER_GROUP_HEARTBEAT:
                return ConsumerGroupHeartbeatRequest.parse(readable, apiVersion);
            case CONSUMER_GROUP_DESCRIBE:
                return ConsumerGroupDescribeRequest.parse(readable, apiVersion);
            case CONTROLLER_REGISTRATION:
                return ControllerRegistrationRequest.parse(readable, apiVersion);
            case GET_TELEMETRY_SUBSCRIPTIONS:
                return GetTelemetrySubscriptionsRequest.parse(readable, apiVersion);
            case PUSH_TELEMETRY:
                return PushTelemetryRequest.parse(readable, apiVersion);
            case ASSIGN_REPLICAS_TO_DIRS:
                return AssignReplicasToDirsRequest.parse(readable, apiVersion);
            case LIST_CONFIG_RESOURCES:
                return ListConfigResourcesRequest.parse(readable, apiVersion);
            case DESCRIBE_TOPIC_PARTITIONS:
                return DescribeTopicPartitionsRequest.parse(readable, apiVersion);
            case SHARE_GROUP_HEARTBEAT:
                return ShareGroupHeartbeatRequest.parse(readable, apiVersion);
            case SHARE_GROUP_DESCRIBE:
                return ShareGroupDescribeRequest.parse(readable, apiVersion);
            case SHARE_FETCH:
                return ShareFetchRequest.parse(readable, apiVersion);
            case SHARE_ACKNOWLEDGE:
                return ShareAcknowledgeRequest.parse(readable, apiVersion);
            case ADD_RAFT_VOTER:
                return AddRaftVoterRequest.parse(readable, apiVersion);
            case REMOVE_RAFT_VOTER:
                return RemoveRaftVoterRequest.parse(readable, apiVersion);
            case UPDATE_RAFT_VOTER:
                return UpdateRaftVoterRequest.parse(readable, apiVersion);
            case INITIALIZE_SHARE_GROUP_STATE:
                return InitializeShareGroupStateRequest.parse(readable, apiVersion);
            case READ_SHARE_GROUP_STATE:
                return ReadShareGroupStateRequest.parse(readable, apiVersion);
            case WRITE_SHARE_GROUP_STATE:
                return WriteShareGroupStateRequest.parse(readable, apiVersion);
            case DELETE_SHARE_GROUP_STATE:
                return DeleteShareGroupStateRequest.parse(readable, apiVersion);
            case READ_SHARE_GROUP_STATE_SUMMARY:
                return ReadShareGroupStateSummaryRequest.parse(readable, apiVersion);
            case STREAMS_GROUP_HEARTBEAT:
                return StreamsGroupHeartbeatRequest.parse(readable, apiVersion);
            case STREAMS_GROUP_DESCRIBE:
                return StreamsGroupDescribeRequest.parse(readable, apiVersion);
            case DESCRIBE_SHARE_GROUP_OFFSETS:
                return DescribeShareGroupOffsetsRequest.parse(readable, apiVersion);
            case ALTER_SHARE_GROUP_OFFSETS:
                return AlterShareGroupOffsetsRequest.parse(readable, apiVersion);
            case DELETE_SHARE_GROUP_OFFSETS:
                return DeleteShareGroupOffsetsRequest.parse(readable, apiVersion);
            default:
                throw new AssertionError(String.format("ApiKey %s is not currently handled in `parseRequest`, the " +
                        "code should be updated to do so.", apiKey));
        }
    }
}
