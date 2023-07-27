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

import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.Map;

public class HeartbeatResponse extends AbstractResponse {

    /**
     * Possible error codes:
     *
     * GROUP_COORDINATOR_NOT_AVAILABLE (15)
     * NOT_COORDINATOR (16)
     * ILLEGAL_GENERATION (22)
     * UNKNOWN_MEMBER_ID (25)
     * REBALANCE_IN_PROGRESS (27)
     * GROUP_AUTHORIZATION_FAILED (30)
     */
    private final HeartbeatResponseData data;

    public HeartbeatResponse(HeartbeatResponseData data) {
        super(ApiKeys.HEARTBEAT);
        this.data = data;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    public Errors error() {
        return Errors.forCode(data.errorCode());
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(error());
    }

    @Override
    public HeartbeatResponseData data() {
        return data;
    }

    public static HeartbeatResponse parse(ByteBuffer buffer, short version) {
        return new HeartbeatResponse(new HeartbeatResponseData(new ByteBufferAccessor(buffer), version));
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 2;
    }
}
