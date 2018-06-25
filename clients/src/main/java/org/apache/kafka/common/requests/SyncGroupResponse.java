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
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Map;

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.apache.kafka.common.protocol.types.Type.BYTES;

public class SyncGroupResponse extends AbstractResponse {
    private static final String MEMBER_ASSIGNMENT_KEY_NAME = "member_assignment";

    private static final Schema SYNC_GROUP_RESPONSE_V0 = new Schema(
            ERROR_CODE,
            new Field(MEMBER_ASSIGNMENT_KEY_NAME, BYTES));
    private static final Schema SYNC_GROUP_RESPONSE_V1 = new Schema(
            THROTTLE_TIME_MS,
            ERROR_CODE,
            new Field(MEMBER_ASSIGNMENT_KEY_NAME, BYTES));

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema SYNC_GROUP_RESPONSE_V2 = SYNC_GROUP_RESPONSE_V1;

    public static Schema[] schemaVersions() {
        return new Schema[] {SYNC_GROUP_RESPONSE_V0, SYNC_GROUP_RESPONSE_V1,
            SYNC_GROUP_RESPONSE_V2};
    }

    /**
     * Possible error codes:
     *
     * COORDINATOR_NOT_AVAILABLE (15)
     * NOT_COORDINATOR (16)
     * ILLEGAL_GENERATION (22)
     * UNKNOWN_MEMBER_ID (25)
     * REBALANCE_IN_PROGRESS (27)
     * GROUP_AUTHORIZATION_FAILED (30)
     *
     * NOTE: Currently the coordinator returns REBALANCE_IN_PROGRESS while the coordinator is
     * loading. On the next protocol bump, we should consider using COORDINATOR_LOAD_IN_PROGRESS
     * to be consistent with the other APIs.
     */

    private final Errors error;
    private final int throttleTimeMs;
    private final ByteBuffer memberState;

    public SyncGroupResponse(Errors error, ByteBuffer memberState) {
        this(DEFAULT_THROTTLE_TIME, error, memberState);
    }

    public SyncGroupResponse(int throttleTimeMs, Errors error, ByteBuffer memberState) {
        this.throttleTimeMs = throttleTimeMs;
        this.error = error;
        this.memberState = memberState;
    }

    public SyncGroupResponse(Struct struct) {
        this.throttleTimeMs = struct.getOrElse(THROTTLE_TIME_MS, DEFAULT_THROTTLE_TIME);
        this.error = Errors.forCode(struct.get(ERROR_CODE));
        this.memberState = struct.getBytes(MEMBER_ASSIGNMENT_KEY_NAME);
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

    public ByteBuffer memberAssignment() {
        return memberState;
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.SYNC_GROUP.responseSchema(version));
        struct.setIfExists(THROTTLE_TIME_MS, throttleTimeMs);
        struct.set(ERROR_CODE, error.code());
        struct.set(MEMBER_ASSIGNMENT_KEY_NAME, memberState);
        return struct;
    }

    public static SyncGroupResponse parse(ByteBuffer buffer, short version) {
        return new SyncGroupResponse(ApiKeys.SYNC_GROUP.parseResponse(version, buffer));
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 2;
    }
}
