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

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Map;

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;

public class LeaveGroupResponse extends AbstractResponse {

    private static final Schema LEAVE_GROUP_RESPONSE_V0 = new Schema(
            ERROR_CODE);
    private static final Schema LEAVE_GROUP_RESPONSE_V1 = new Schema(
            THROTTLE_TIME_MS,
            ERROR_CODE);

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema LEAVE_GROUP_RESPONSE_V2 = LEAVE_GROUP_RESPONSE_V1;

    public static Schema[] schemaVersions() {
        return new Schema[] {LEAVE_GROUP_RESPONSE_V0, LEAVE_GROUP_RESPONSE_V1,
            LEAVE_GROUP_RESPONSE_V2};
    }

    /**
     * Possible error code:
     *
     * GROUP_LOAD_IN_PROGRESS (14)
     * CONSUMER_COORDINATOR_NOT_AVAILABLE (15)
     * NOT_COORDINATOR_FOR_CONSUMER (16)
     * UNKNOWN_CONSUMER_ID (25)
     * GROUP_AUTHORIZATION_FAILED (30)
     */
    private final Errors error;
    private final int throttleTimeMs;

    public LeaveGroupResponse(Errors error) {
        this(DEFAULT_THROTTLE_TIME, error);
    }

    public LeaveGroupResponse(int throttleTimeMs, Errors error) {
        this.throttleTimeMs = throttleTimeMs;
        this.error = error;
    }

    public LeaveGroupResponse(Struct struct) {
        this.throttleTimeMs = struct.getOrElse(THROTTLE_TIME_MS, DEFAULT_THROTTLE_TIME);
        this.error = Errors.forCode(struct.get(ERROR_CODE));
    }

    @Override
    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public Errors error() {
        return error;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(error);
    }

    @Override
    public Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.LEAVE_GROUP.responseSchema(version));
        struct.setIfExists(THROTTLE_TIME_MS, throttleTimeMs);
        struct.set(ERROR_CODE, error.code());
        return struct;
    }

    public static LeaveGroupResponse parse(ByteBuffer buffer, short versionId) {
        return new LeaveGroupResponse(ApiKeys.LEAVE_GROUP.parseResponse(versionId, buffer));
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 2;
    }
}
