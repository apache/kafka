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

import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.Map;

public class JoinGroupResponse extends AbstractResponse {

    private final JoinGroupResponseData data;

    public JoinGroupResponse(JoinGroupResponseData data, short version) {
        super(ApiKeys.JOIN_GROUP);
        this.data = data;

        // All versions prior to version 7 do not support nullable
        // string for the protocol name. Empty string should be used.
        if (version < 7 && data.protocolName() == null) {
            data.setProtocolName("");
        }

        // If nullable string for the protocol name is supported,
        // we set empty string to be null to ensure compliance.
        if (version >= 7 && data.protocolName() != null && data.protocolName().isEmpty()) {
            data.setProtocolName(null);
        }
    }

    @Override
    public JoinGroupResponseData data() {
        return data;
    }

    public boolean isLeader() {
        return data.memberId().equals(data.leader());
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
        return errorCounts(Errors.forCode(data.errorCode()));
    }

    public static JoinGroupResponse parse(ByteBuffer buffer, short version) {
        return new JoinGroupResponse(new JoinGroupResponseData(new ByteBufferAccessor(buffer), version), version);
    }

    @Override
    public String toString() {
        return data.toString();
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 3;
    }
}
