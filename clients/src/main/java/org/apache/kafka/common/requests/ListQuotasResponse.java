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
import org.apache.kafka.common.protocol.types.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.ERROR_MESSAGE;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;

public class ListQuotasResponse extends AbstractResponse {

    private static final String QUOTA_CONFIG_ENTITY_NAME_KEY_NAME = "quota_config_entity_name";

    private static final Schema LIST_QUOTAS_REQUEST_SCHEMA_V0 = new Schema(
            THROTTLE_TIME_MS,
            ERROR_CODE,
            ERROR_MESSAGE,
            new Field(QUOTA_CONFIG_ENTITY_NAME_KEY_NAME, new ArrayOf(Type.STRING))
    );

    public static Schema[] schemaVersions() {
        return new Schema[]{LIST_QUOTAS_REQUEST_SCHEMA_V0};
    }

    private final int throttleTimeMs;
    private final ApiError apiError;
    private final List<String> quotaEntityNames;

    public ListQuotasResponse(int throttleTimeMs, List<String> quotaEntityNames) {
        this.throttleTimeMs = throttleTimeMs;
        this.apiError = ApiError.NONE;
        this.quotaEntityNames = quotaEntityNames;
    }

    public ListQuotasResponse(int throttleTimeMs, ApiError error) {
        this.throttleTimeMs = throttleTimeMs;
        this.apiError = error;
        this.quotaEntityNames = new ArrayList<>(0);
    }

    public ListQuotasResponse(Struct struct) {
        throttleTimeMs = struct.get(THROTTLE_TIME_MS);
        apiError = new ApiError(struct);
        Object[] entityNames = struct.getArray(QUOTA_CONFIG_ENTITY_NAME_KEY_NAME);
        quotaEntityNames = new ArrayList<>(entityNames.length);
        for (Object entityName : entityNames) {
            quotaEntityNames.add((String) entityName);
        }
    }

    public List<String> quotaEntityNames() {
        return Collections.unmodifiableList(quotaEntityNames);
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        updateErrorCounts(errorCounts, apiError.error());
        return errorCounts;
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.LIST_QUOTAS.responseSchema(version));
        struct.set(THROTTLE_TIME_MS, throttleTimeMs);
        apiError.write(struct);
        struct.set(QUOTA_CONFIG_ENTITY_NAME_KEY_NAME, quotaEntityNames.toArray(new String[quotaEntityNames.size()]));
        return struct;
    }
}
