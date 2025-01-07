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

import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
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
 * - {@link Errors#UNSUPPORTED_ASSIGNOR}
 * - {@link Errors#UNRELEASED_INSTANCE_ID}
 * - {@link Errors#GROUP_MAX_SIZE_REACHED}
 * - {@link Errors#INVALID_REGULAR_EXPRESSION}
 */
public class ConsumerGroupHeartbeatResponse extends AbstractResponse {
    private final ConsumerGroupHeartbeatResponseData data;

    public ConsumerGroupHeartbeatResponse(ConsumerGroupHeartbeatResponseData data) {
        super(ApiKeys.CONSUMER_GROUP_HEARTBEAT);
        this.data = data;
    }

    @Override
    public ConsumerGroupHeartbeatResponseData data() {
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

    public static ConsumerGroupHeartbeatResponse parse(ByteBuffer buffer, short version) {
        return new ConsumerGroupHeartbeatResponse(new ConsumerGroupHeartbeatResponseData(
            new ByteBufferAccessor(buffer), version));
    }
}
