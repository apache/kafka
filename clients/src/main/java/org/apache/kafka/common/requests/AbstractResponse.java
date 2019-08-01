/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package org.apache.kafka.common.requests;

import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;

public abstract class AbstractResponse extends AbstractRequestResponse {

    public AbstractResponse(Struct struct) {
        super(struct);
    }

    public Send toSend(String destination, RequestHeader request) {
        ResponseHeader responseHeader = new ResponseHeader(request.correlationId());
        return new NetworkSend(destination, serialize(responseHeader, this));
    }

    public static AbstractResponse getResponse(int requestId, Struct struct) {
        ApiKeys apiKey = ApiKeys.forId(requestId);
        switch (apiKey) {
            case PRODUCE:
                return new ProduceResponse(struct);
            case FETCH:
                return new FetchResponse(struct);
            case LIST_OFFSETS:
                return new ListOffsetResponse(struct);
            case METADATA:
                return new MetadataResponse(struct);
            case OFFSET_COMMIT:
                return new OffsetCommitResponse(struct);
            case OFFSET_FETCH:
                return new OffsetFetchResponse(struct);
            case GROUP_COORDINATOR:
                return new GroupCoordinatorResponse(struct);
            case JOIN_GROUP:
                return new JoinGroupResponse(struct);
            case HEARTBEAT:
                return new HeartbeatResponse(struct);
            case LEAVE_GROUP:
                return new LeaveGroupResponse(struct);
            case SYNC_GROUP:
                return new SyncGroupResponse(struct);
            case STOP_REPLICA:
                return new StopReplicaResponse(struct);
            case CONTROLLED_SHUTDOWN_KEY:
                return new ControlledShutdownResponse(struct);
            case UPDATE_METADATA_KEY:
                return new UpdateMetadataResponse(struct);
            case LEADER_AND_ISR:
                return new LeaderAndIsrResponse(struct);
            case DESCRIBE_GROUPS:
                return new DescribeGroupsResponse(struct);
            case LIST_GROUPS:
                return new ListGroupsResponse(struct);
            case SASL_HANDSHAKE:
                return new SaslHandshakeResponse(struct);
            case API_VERSIONS:
                return new ApiVersionsResponse(struct);
            case CREATE_TOPICS:
                return new CreateTopicsResponse(struct);
            case DELETE_TOPICS:
                return new DeleteTopicsResponse(struct);
            case GET_START_OFFSET:
                return new GetStartOffsetResponse(struct);
            default:
                throw new AssertionError(String.format("ApiKey %s is not currently handled in `getResponse`, the " +
                        "code should be updated to do so.", apiKey));
        }
    }

}
