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
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.ERROR_MESSAGE;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.apache.kafka.common.protocol.types.Type.INT8;
import static org.apache.kafka.common.protocol.types.Type.STRING;

public class AlterConfigsResponse extends AbstractResponse {

    private static final String RESOURCES_KEY_NAME = "resources";
    private static final String RESOURCE_TYPE_KEY_NAME = "resource_type";
    private static final String RESOURCE_NAME_KEY_NAME = "resource_name";

    private static final Schema ALTER_CONFIGS_RESPONSE_ENTITY_V0 = new Schema(
            ERROR_CODE,
            ERROR_MESSAGE,
            new Field(RESOURCE_TYPE_KEY_NAME, INT8),
            new Field(RESOURCE_NAME_KEY_NAME, STRING));

    private static final Schema ALTER_CONFIGS_RESPONSE_V0 = new Schema(
            THROTTLE_TIME_MS,
            new Field(RESOURCES_KEY_NAME, new ArrayOf(ALTER_CONFIGS_RESPONSE_ENTITY_V0)));

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema ALTER_CONFIGS_RESPONSE_V1 = ALTER_CONFIGS_RESPONSE_V0;

    public static Schema[] schemaVersions() {
        return new Schema[]{ALTER_CONFIGS_RESPONSE_V0, ALTER_CONFIGS_RESPONSE_V1};
    }

    private final int throttleTimeMs;
    private final Map<Resource, ApiError> errors;

    public AlterConfigsResponse(int throttleTimeMs, Map<Resource, ApiError> errors) {
        this.throttleTimeMs = throttleTimeMs;
        this.errors = errors;

    }

    public AlterConfigsResponse(Struct struct) {
        throttleTimeMs = struct.get(THROTTLE_TIME_MS);
        Object[] resourcesArray = struct.getArray(RESOURCES_KEY_NAME);
        errors = new HashMap<>(resourcesArray.length);
        for (Object resourceObj : resourcesArray) {
            Struct resourceStruct = (Struct) resourceObj;
            ApiError error = new ApiError(resourceStruct);
            ResourceType resourceType = ResourceType.forId(resourceStruct.getByte(RESOURCE_TYPE_KEY_NAME));
            String resourceName = resourceStruct.getString(RESOURCE_NAME_KEY_NAME);
            errors.put(new Resource(resourceType, resourceName), error);
        }
    }

    public Map<Resource, ApiError> errors() {
        return errors;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return apiErrorCounts(errors);
    }

    @Override
    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.ALTER_CONFIGS.responseSchema(version));
        struct.set(THROTTLE_TIME_MS, throttleTimeMs);
        List<Struct> resourceStructs = new ArrayList<>(errors.size());
        for (Map.Entry<Resource, ApiError> entry : errors.entrySet()) {
            Struct resourceStruct = struct.instance(RESOURCES_KEY_NAME);
            Resource resource = entry.getKey();
            entry.getValue().write(resourceStruct);
            resourceStruct.set(RESOURCE_TYPE_KEY_NAME, resource.type().id());
            resourceStruct.set(RESOURCE_NAME_KEY_NAME, resource.name());
            resourceStructs.add(resourceStruct);
        }
        struct.set(RESOURCES_KEY_NAME, resourceStructs.toArray(new Struct[0]));
        return struct;
    }

    public static AlterConfigsResponse parse(ByteBuffer buffer, short version) {
        return new AlterConfigsResponse(ApiKeys.ALTER_CONFIGS.parseResponse(version, buffer));
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }
}
