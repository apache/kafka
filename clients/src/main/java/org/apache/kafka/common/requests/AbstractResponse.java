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

import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractResponse extends AbstractRequestResponse {
    public static final int DEFAULT_THROTTLE_TIME = 0;

    protected Send toSend(String destination, ResponseHeader header, short apiVersion) {
        return new NetworkSend(destination, serialize(apiVersion, header));
    }

    /**
     * Visible for testing, typically {@link #toSend(String, ResponseHeader, short)} should be used instead.
     */
    public ByteBuffer serialize(short version, ResponseHeader responseHeader) {
        return serialize(responseHeader.toStruct(), toStruct(version));
    }

    public abstract Map<Errors, Integer> errorCounts();

    protected Map<Errors, Integer> errorCounts(Errors error) {
        return Collections.singletonMap(error, 1);
    }

    protected Map<Errors, Integer> errorCounts(Map<?, Errors> errors) {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        for (Errors error : errors.values())
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
        Integer count = errorCounts.get(error);
        errorCounts.put(error, count == null ? 1 : count + 1);
    }

    protected abstract Struct toStruct(short version);

    public static AbstractResponse parseResponse(ApiKeys apiKey, Struct struct, short version) {
        switch (apiKey) {
            case PRODUCE:
                return new ProduceResponse(struct);
            case FETCH:
                return FetchResponse.parse(struct);
            case LIST_OFFSETS:
                return new ListOffsetResponse(struct);
            case METADATA:
                return new MetadataResponse(struct, version);
            case OFFSET_COMMIT:
                return new OffsetCommitResponse(struct, version);
            case OFFSET_FETCH:
                return new OffsetFetchResponse(struct);
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
                return new StopReplicaResponse(struct);
            case CONTROLLED_SHUTDOWN:
                return new ControlledShutdownResponse(struct, version);
            case UPDATE_METADATA:
                return new UpdateMetadataResponse(struct);
            case LEADER_AND_ISR:
                return new LeaderAndIsrResponse(struct);
            case DESCRIBE_GROUPS:
                return new DescribeGroupsResponse(struct, version);
            case LIST_GROUPS:
                return new ListGroupsResponse(struct);
            case SASL_HANDSHAKE:
                return new SaslHandshakeResponse(struct, version);
            case API_VERSIONS:
                return new ApiVersionsResponse(struct);
            case CREATE_TOPICS:
                return new CreateTopicsResponse(struct, version);
            case DELETE_TOPICS:
                return new DeleteTopicsResponse(struct, version);
            case DELETE_RECORDS:
                return new DeleteRecordsResponse(struct);
            case INIT_PRODUCER_ID:
                return new InitProducerIdResponse(struct, version);
            case OFFSET_FOR_LEADER_EPOCH:
                return new OffsetsForLeaderEpochResponse(struct);
            case ADD_PARTITIONS_TO_TXN:
                return new AddPartitionsToTxnResponse(struct);
            case ADD_OFFSETS_TO_TXN:
                return new AddOffsetsToTxnResponse(struct);
            case END_TXN:
                return new EndTxnResponse(struct);
            case WRITE_TXN_MARKERS:
                return new WriteTxnMarkersResponse(struct);
            case TXN_OFFSET_COMMIT:
                return new TxnOffsetCommitResponse(struct);
            case DESCRIBE_ACLS:
                return new DescribeAclsResponse(struct);
            case CREATE_ACLS:
                return new CreateAclsResponse(struct);
            case DELETE_ACLS:
                return new DeleteAclsResponse(struct);
            case DESCRIBE_CONFIGS:
                return new DescribeConfigsResponse(struct);
            case ALTER_CONFIGS:
                return new AlterConfigsResponse(struct);
            case ALTER_REPLICA_LOG_DIRS:
                return new AlterReplicaLogDirsResponse(struct);
            case DESCRIBE_LOG_DIRS:
                return new DescribeLogDirsResponse(struct);
            case SASL_AUTHENTICATE:
                return new SaslAuthenticateResponse(struct, version);
            case CREATE_PARTITIONS:
                return new CreatePartitionsResponse(struct);
            case CREATE_DELEGATION_TOKEN:
                return new CreateDelegationTokenResponse(struct);
            case RENEW_DELEGATION_TOKEN:
                return new RenewDelegationTokenResponse(struct);
            case EXPIRE_DELEGATION_TOKEN:
                return new ExpireDelegationTokenResponse(struct);
            case DESCRIBE_DELEGATION_TOKEN:
                return new DescribeDelegationTokenResponse(struct);
            case DELETE_GROUPS:
                return new DeleteGroupsResponse(struct);
            case ELECT_PREFERRED_LEADERS:
                return new ElectPreferredLeadersResponse(struct, version);
            case INCREMENTAL_ALTER_CONFIGS:
                return new IncrementalAlterConfigsResponse(struct, version);
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
