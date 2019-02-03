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
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Map;

public abstract class AbstractRequest extends AbstractRequestResponse {

    public static abstract class Builder<T extends AbstractRequest> {
        private final ApiKeys apiKey;
        private final short oldestAllowedVersion;
        private final short latestAllowedVersion;

        /**
         * Construct a new builder which allows any supported version
         */
        public Builder(ApiKeys apiKey) {
            this(apiKey, apiKey.oldestVersion(), apiKey.latestVersion());
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

    public AbstractRequest(ApiKeys api, short version) {
        if (!api.isVersionSupported(version))
            throw new UnsupportedVersionException("The " + api + " protocol does not support version " + version);
        this.version = version;
    }

    /**
     * Get the version of this AbstractRequest object.
     */
    public short version() {
        return version;
    }

    public Send toSend(String destination, RequestHeader header) {
        return new NetworkSend(destination, serialize(header));
    }

    /**
     * Use with care, typically {@link #toSend(String, RequestHeader)} should be used instead.
     */
    public ByteBuffer serialize(RequestHeader header) {
        return serialize(header.toStruct(), toStruct());
    }

    protected abstract Struct toStruct();

    public String toString(boolean verbose) {
        return toStruct().toString();
    }

    @Override
    public final String toString() {
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
    public static AbstractRequest parseRequest(ApiKeys apiKey, short apiVersion, Struct struct) {
        switch (apiKey) {
            case PRODUCE:
                return new ProduceRequest(struct, apiVersion);
            case FETCH:
                return new FetchRequest(struct, apiVersion);
            case LIST_OFFSETS:
                return new ListOffsetRequest(struct, apiVersion);
            case METADATA:
                return new MetadataRequest(struct, apiVersion);
            case OFFSET_COMMIT:
                return new OffsetCommitRequest(struct, apiVersion);
            case OFFSET_FETCH:
                return new OffsetFetchRequest(struct, apiVersion);
            case FIND_COORDINATOR:
                return new FindCoordinatorRequest(struct, apiVersion);
            case JOIN_GROUP:
                return new JoinGroupRequest(struct, apiVersion);
            case HEARTBEAT:
                return new HeartbeatRequest(struct, apiVersion);
            case LEAVE_GROUP:
                return new LeaveGroupRequest(struct, apiVersion);
            case SYNC_GROUP:
                return new SyncGroupRequest(struct, apiVersion);
            case STOP_REPLICA:
                return new StopReplicaRequest(struct, apiVersion);
            case CONTROLLED_SHUTDOWN:
                return new ControlledShutdownRequest(struct, apiVersion);
            case UPDATE_METADATA:
                return new UpdateMetadataRequest(struct, apiVersion);
            case LEADER_AND_ISR:
                return new LeaderAndIsrRequest(struct, apiVersion);
            case DESCRIBE_GROUPS:
                return new DescribeGroupsRequest(struct, apiVersion);
            case LIST_GROUPS:
                return new ListGroupsRequest(struct, apiVersion);
            case SASL_HANDSHAKE:
                return new SaslHandshakeRequest(struct, apiVersion);
            case API_VERSIONS:
                return new ApiVersionsRequest(struct, apiVersion);
            case CREATE_TOPICS:
                return new CreateTopicsRequest(struct, apiVersion);
            case DELETE_TOPICS:
                return new DeleteTopicsRequest(struct, apiVersion);
            case DELETE_RECORDS:
                return new DeleteRecordsRequest(struct, apiVersion);
            case INIT_PRODUCER_ID:
                return new InitProducerIdRequest(struct, apiVersion);
            case OFFSET_FOR_LEADER_EPOCH:
                return new OffsetsForLeaderEpochRequest(struct, apiVersion);
            case ADD_PARTITIONS_TO_TXN:
                return new AddPartitionsToTxnRequest(struct, apiVersion);
            case ADD_OFFSETS_TO_TXN:
                return new AddOffsetsToTxnRequest(struct, apiVersion);
            case END_TXN:
                return new EndTxnRequest(struct, apiVersion);
            case WRITE_TXN_MARKERS:
                return new WriteTxnMarkersRequest(struct, apiVersion);
            case TXN_OFFSET_COMMIT:
                return new TxnOffsetCommitRequest(struct, apiVersion);
            case DESCRIBE_ACLS:
                return new DescribeAclsRequest(struct, apiVersion);
            case CREATE_ACLS:
                return new CreateAclsRequest(struct, apiVersion);
            case DELETE_ACLS:
                return new DeleteAclsRequest(struct, apiVersion);
            case DESCRIBE_CONFIGS:
                return new DescribeConfigsRequest(struct, apiVersion);
            case ALTER_CONFIGS:
                return new AlterConfigsRequest(struct, apiVersion);
            case ALTER_REPLICA_LOG_DIRS:
                return new AlterReplicaLogDirsRequest(struct, apiVersion);
            case DESCRIBE_LOG_DIRS:
                return new DescribeLogDirsRequest(struct, apiVersion);
            case SASL_AUTHENTICATE:
                return new SaslAuthenticateRequest(struct, apiVersion);
            case CREATE_PARTITIONS:
                return new CreatePartitionsRequest(struct, apiVersion);
            case CREATE_DELEGATION_TOKEN:
                return new CreateDelegationTokenRequest(struct, apiVersion);
            case RENEW_DELEGATION_TOKEN:
                return new RenewDelegationTokenRequest(struct, apiVersion);
            case EXPIRE_DELEGATION_TOKEN:
                return new ExpireDelegationTokenRequest(struct, apiVersion);
            case DESCRIBE_DELEGATION_TOKEN:
                return new DescribeDelegationTokenRequest(struct, apiVersion);
            case DELETE_GROUPS:
                return new DeleteGroupsRequest(struct, apiVersion);
            case ELECT_PREFERRED_LEADERS:
                return new ElectPreferredLeadersRequest(struct, apiVersion);
            default:
                throw new AssertionError(String.format("ApiKey %s is not currently handled in `parseRequest`, the " +
                        "code should be updated to do so.", apiKey));
        }
    }
}
