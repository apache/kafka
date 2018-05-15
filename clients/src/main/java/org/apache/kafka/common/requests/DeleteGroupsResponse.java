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
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.GROUP_ID;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;

public class DeleteGroupsResponse extends AbstractResponse {
    private static final String GROUP_ERROR_CODES_KEY_NAME = "group_error_codes";

    private static final Schema GROUP_ERROR_CODE = new Schema(
            GROUP_ID,
            ERROR_CODE);

    private static final Schema DELETE_GROUPS_RESPONSE_V0 = new Schema(
            THROTTLE_TIME_MS,
            new Field(GROUP_ERROR_CODES_KEY_NAME, new ArrayOf(GROUP_ERROR_CODE), "An array of per group error codes."));

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema DELETE_GROUPS_RESPONSE_V1 = DELETE_GROUPS_RESPONSE_V0;

    public static Schema[] schemaVersions() {
        return new Schema[]{DELETE_GROUPS_RESPONSE_V0, DELETE_GROUPS_RESPONSE_V1};
    }


    /**
     * Possible error codes:
     *
     * COORDINATOR_LOAD_IN_PROGRESS (14)
     * COORDINATOR_NOT_AVAILABLE(15)
     * NOT_COORDINATOR (16)
     * INVALID_GROUP_ID(24)
     * GROUP_AUTHORIZATION_FAILED(30)
     * NON_EMPTY_GROUP(68)
     * GROUP_ID_NOT_FOUND(69)
     */

    private final Map<String, Errors> errors;
    private final int throttleTimeMs;

    public DeleteGroupsResponse(Map<String, Errors> errors) {
        this(DEFAULT_THROTTLE_TIME, errors);
    }

    public DeleteGroupsResponse(int throttleTimeMs, Map<String, Errors> errors) {
        this.throttleTimeMs = throttleTimeMs;
        this.errors = errors;
    }

    public DeleteGroupsResponse(Struct struct) {
        this.throttleTimeMs = struct.getOrElse(THROTTLE_TIME_MS, DEFAULT_THROTTLE_TIME);
        Object[] groupErrorCodesStructs = struct.getArray(GROUP_ERROR_CODES_KEY_NAME);
        Map<String, Errors> errors = new HashMap<>();
        for (Object groupErrorCodeStructObj : groupErrorCodesStructs) {
            Struct groupErrorCodeStruct = (Struct) groupErrorCodeStructObj;
            String group = groupErrorCodeStruct.get(GROUP_ID);
            Errors error = Errors.forCode(groupErrorCodeStruct.get(ERROR_CODE));
            errors.put(group, error);
        }

        this.errors = errors;
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.DELETE_GROUPS.responseSchema(version));
        struct.setIfExists(THROTTLE_TIME_MS, throttleTimeMs);
        List<Struct> groupErrorCodeStructs = new ArrayList<>(errors.size());
        for (Map.Entry<String, Errors> groupError : errors.entrySet()) {
            Struct groupErrorCodeStruct = struct.instance(GROUP_ERROR_CODES_KEY_NAME);
            groupErrorCodeStruct.set(GROUP_ID, groupError.getKey());
            groupErrorCodeStruct.set(ERROR_CODE, groupError.getValue().code());
            groupErrorCodeStructs.add(groupErrorCodeStruct);
        }
        struct.set(GROUP_ERROR_CODES_KEY_NAME, groupErrorCodeStructs.toArray());
        return struct;
    }

    @Override
    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public Map<String, Errors> errors() {
        return errors;
    }

    public boolean hasError(String group) {
        return errors.containsKey(group) && errors.get(group) != Errors.NONE;
    }

    public Errors get(String group) {
        return errors.get(group);
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(errors);
    }

    public static DeleteGroupsResponse parse(ByteBuffer buffer, short version) {
        return new DeleteGroupsResponse(ApiKeys.DELETE_GROUPS.responseSchema(version).read(buffer));
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }
}
