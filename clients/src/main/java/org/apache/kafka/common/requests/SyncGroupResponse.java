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
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

public class SyncGroupResponse extends AbstractResponse {

    public static final String ERROR_CODE_KEY_NAME = "error_code";
    public static final String MEMBER_ASSIGNMENT_KEY_NAME = "member_assignment";

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

    private final Errors error;
    private final ByteBuffer memberState;

    public SyncGroupResponse(Errors error, ByteBuffer memberState) {
        this.error = error;
        this.memberState = memberState;
    }

    public SyncGroupResponse(Struct struct) {
        this.error = Errors.forCode(struct.getShort(ERROR_CODE_KEY_NAME));
        this.memberState = struct.getBytes(MEMBER_ASSIGNMENT_KEY_NAME);
    }

    public Errors error() {
        return error;
    }

    public ByteBuffer memberAssignment() {
        return memberState;
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.SYNC_GROUP.responseSchema(version));
        struct.set(ERROR_CODE_KEY_NAME, error.code());
        struct.set(MEMBER_ASSIGNMENT_KEY_NAME, memberState);
        return struct;
    }

    public static SyncGroupResponse parse(ByteBuffer buffer, short version) {
        return new SyncGroupResponse(ApiKeys.SYNC_GROUP.parseResponse(version, buffer));
    }

}
