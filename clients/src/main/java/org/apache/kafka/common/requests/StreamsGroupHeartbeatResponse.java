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

import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

/**
 * Possible error codes.
 *
 * - {@link Errors#GROUP_AUTHORIZATION_FAILED}
 * - {@link Errors#NOT_COORDINATOR}
 * - {@link Errors#COORDINATOR_NOT_AVAILABLE}
 * - {@link Errors#COORDINATOR_LOAD_IN_PROGRESS}
 * - {@link Errors#INVALID_REQUEST}
 * - {@link Errors#UNKNOWN_MEMBER_ID}
 * - {@link Errors#FENCED_MEMBER_EPOCH}
 * - {@link Errors#UNRELEASED_INSTANCE_ID}
 * - {@link Errors#GROUP_MAX_SIZE_REACHED}
 * - {@link Errors#GROUP_ID_NOT_FOUND}
 * - {@link Errors#TOPIC_AUTHORIZATION_FAILED}
 * - {@link Errors#CLUSTER_AUTHORIZATION_FAILED}
 * - {@link Errors#STREAMS_INVALID_TOPOLOGY}
 * - {@link Errors#STREAMS_INVALID_TOPOLOGY_EPOCH}
 * - {@link Errors#STREAMS_TOPOLOGY_FENCED}
 */
public class StreamsGroupHeartbeatResponse extends AbstractResponse {
    private final StreamsGroupHeartbeatResponseData data;

    public StreamsGroupHeartbeatResponse(StreamsGroupHeartbeatResponseData data) {
        super(ApiKeys.STREAMS_GROUP_HEARTBEAT);
        this.data = data;
    }

    @Override
    public StreamsGroupHeartbeatResponseData data() {
        return data;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return Collections.singletonMap(Errors.forCode(data.errorCode()), 1);
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    public static StreamsGroupHeartbeatResponse parse(ByteBuffer buffer, short version) {
        return new StreamsGroupHeartbeatResponse(new StreamsGroupHeartbeatResponseData(
            new ByteBufferAccessor(buffer), version));
    }

    public enum Status {
        STALE_TOPOLOGY((byte) 0, "The topology epoch supplied is inconsistent with the topology for this streams group."),
        MISSING_SOURCE_TOPICS((byte) 1, "One or more source topics are missing or a source topic regex resolves to zero topics."),
        INCORRECTLY_PARTITIONED_TOPICS((byte) 2, "One or more topics expected to be copartitioned are not copartitioned."),
        MISSING_INTERNAL_TOPICS((byte) 3, "One or more internal topics are missing."),
        SHUTDOWN_APPLICATION((byte) 4, "A client requested the shutdown of the whole application.");

        private final byte code;
        private final String message;

        Status(final byte code, final String message) {
            this.code = code;
            this.message = message;
        }

        public byte code() {
            return code;
        }

        public String message() {
            return message;
        }
    }
}
