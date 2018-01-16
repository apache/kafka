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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.ERROR_MESSAGE;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.apache.kafka.common.protocol.types.Type.BOOLEAN;
import static org.apache.kafka.common.protocol.types.Type.INT8;
import static org.apache.kafka.common.protocol.types.Type.NULLABLE_STRING;
import static org.apache.kafka.common.protocol.types.Type.STRING;
import static org.apache.kafka.common.requests.DescribeQuotasRequest.unwrapQuotaConfigResource;
import static org.apache.kafka.common.requests.DescribeQuotasRequest.wrapQuotaConfigResource;

public class DescribeQuotasResponse extends AbstractResponse {

    private static final String QUOTA_CONFIG_RESOURCE = "quota_config_resource";
    private static final String CHILD_QUOTA_CONFIG_RESOURCE = "child_quota_config_resource";
    private static final String QUOTA_TYPE_KEY_NAME = "type";
    private static final String QUOTA_NAME_KEY_NAME = "name";

    private static final String RESOURCE_KEY_NAME = "resource";
    private static final String CONFIG_KEY_NAME = "config";

    private static final String CONFIG_ENTRY_KEY_NAME = "config_entry";
    private static final String CONFIG_NAME_KEY_NAME = "config_name";
    private static final String CONFIG_VALUE_KEY_NAME = "config_value";
    private static final String IS_SENSITIVE_KEY_NAME = "is_sensitive";
    private static final String IS_DEFAULT_KEY_NAME = "is_default";
    private static final String READ_ONLY_KEY_NAME = "read_only";

    private static final Schema QUOTA_SCHEMA_V0 = new Schema(
            new Field(QUOTA_TYPE_KEY_NAME, INT8),
            new Field(QUOTA_NAME_KEY_NAME, NULLABLE_STRING)
    );

    private static final Schema CONFIG_ENTRY_SCHEMA_V0 = new Schema(
            new Field(CONFIG_NAME_KEY_NAME, STRING),
            new Field(CONFIG_VALUE_KEY_NAME, NULLABLE_STRING),
            new Field(READ_ONLY_KEY_NAME, BOOLEAN),
            new Field(IS_DEFAULT_KEY_NAME, BOOLEAN),
            new Field(IS_SENSITIVE_KEY_NAME, BOOLEAN));

    private static final Schema CONFIG_SCHEMA_V0 = new Schema(
            ERROR_CODE,
            ERROR_MESSAGE,
            new Field(CONFIG_ENTRY_KEY_NAME, new ArrayOf(CONFIG_ENTRY_SCHEMA_V0))
    );

    private static final Schema RESOURCE_SCHEMA_V0 = new Schema(
            new Field(QUOTA_CONFIG_RESOURCE, QUOTA_SCHEMA_V0),
            new Field(CHILD_QUOTA_CONFIG_RESOURCE, QUOTA_SCHEMA_V0),
            new Field(CONFIG_KEY_NAME, CONFIG_SCHEMA_V0)
    );

    private static final Schema DESCRIBE_QUOTAS_RESPONSE_SCHEMA_V0 = new Schema(
            THROTTLE_TIME_MS,
            new Field(RESOURCE_KEY_NAME, new ArrayOf(RESOURCE_SCHEMA_V0)));


    public static Schema[] schemaVersions() {
        return new Schema[]{DESCRIBE_QUOTAS_RESPONSE_SCHEMA_V0};
    }

    private final int throttleTimeMs;
    private final Map<QuotaConfigResourceTuple, DescribeConfigsResponse.Config> configs;

    public DescribeQuotasResponse(int throttleTimeMs, Map<QuotaConfigResourceTuple, DescribeConfigsResponse.Config> configs) {
        this.throttleTimeMs = throttleTimeMs;
        this.configs = configs;
    }

    public DescribeQuotasResponse(Struct struct) {
        throttleTimeMs = struct.get(THROTTLE_TIME_MS);
        Object[] resourcesArray = struct.getArray(RESOURCE_KEY_NAME);
        configs = new HashMap<>(resourcesArray.length);
        for (Object resourceObj : resourcesArray) {
            Struct resourceStruct = (Struct) resourceObj;
            QuotaConfigResourceTuple tuple =
                    new QuotaConfigResourceTuple(
                            unwrapQuotaConfigResource(resourceStruct.getStruct(QUOTA_CONFIG_RESOURCE)),
                            unwrapQuotaConfigResource(resourceStruct.getStruct(CHILD_QUOTA_CONFIG_RESOURCE)));
            configs.put(tuple, unwrapConfig(resourceStruct.getStruct(CONFIG_KEY_NAME)));
        }
    }

    public Map<QuotaConfigResourceTuple, DescribeConfigsResponse.Config> quotaConfigSettings() {
        return Collections.unmodifiableMap(configs);
    }

    private DescribeConfigsResponse.Config unwrapConfig(Struct struct) {
        ApiError error = new ApiError(struct);
        Collection<DescribeConfigsResponse.ConfigEntry> entries = new ArrayList<>();
        Object[] configEntriesStructArray = struct.getArray(CONFIG_ENTRY_KEY_NAME);
        for (Object configEntryStructObj : configEntriesStructArray) {
            Struct configEntryStruct = (Struct) configEntryStructObj;
            String configName = configEntryStruct.getString(CONFIG_NAME_KEY_NAME);
            String configValue = configEntryStruct.getString(CONFIG_VALUE_KEY_NAME);
            boolean isSensitive = configEntryStruct.getBoolean(IS_SENSITIVE_KEY_NAME);
            boolean isDefault = configEntryStruct.getBoolean(IS_DEFAULT_KEY_NAME);
            boolean readOnly = configEntryStruct.getBoolean(READ_ONLY_KEY_NAME);
            entries.add(new DescribeConfigsResponse.ConfigEntry(configName, configValue, isSensitive, isDefault, readOnly));
        }
        return new DescribeConfigsResponse.Config(error, entries);
    }

    private Struct wrapConfig(DescribeConfigsResponse.Config config, Struct parent) {
        Struct struct = parent.instance(CONFIG_KEY_NAME);
        config.error().write(struct);
        List<Struct> entryStructs = new ArrayList<>(config.entries().size());
        for (DescribeConfigsResponse.ConfigEntry entry : config.entries()) {
            Struct entryStruct = struct.instance(CONFIG_ENTRY_KEY_NAME);
            entryStruct.set(CONFIG_NAME_KEY_NAME, entry.name());
            entryStruct.set(CONFIG_VALUE_KEY_NAME, entry.value());
            entryStruct.set(IS_SENSITIVE_KEY_NAME, entry.isSensitive());
            entryStruct.set(IS_DEFAULT_KEY_NAME, entry.isDefault());
            entryStruct.set(READ_ONLY_KEY_NAME, entry.isReadOnly());
            entryStructs.add(entryStruct);
        }
        struct.set(CONFIG_ENTRY_KEY_NAME, entryStructs.toArray(new Struct[0]));
        return struct;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        for (DescribeConfigsResponse.Config response : configs.values())
            updateErrorCounts(errorCounts, response.error().error());
        return errorCounts;
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.DESCRIBE_QUOTAS.responseSchema(version));
        struct.set(THROTTLE_TIME_MS, throttleTimeMs);
        List<Struct> resourceStructList = new ArrayList<>(configs.entrySet().size());
        for (Map.Entry<QuotaConfigResourceTuple, DescribeConfigsResponse.Config> entry : configs.entrySet()) {
            Struct resource = struct.instance(RESOURCE_KEY_NAME);
            resource.set(QUOTA_CONFIG_RESOURCE, wrapQuotaConfigResource(entry.getKey().quotaConfigResource(), resource, QUOTA_CONFIG_RESOURCE));
            resource.set(CHILD_QUOTA_CONFIG_RESOURCE, wrapQuotaConfigResource(entry.getKey().childQuotaConfigResource(), resource, CHILD_QUOTA_CONFIG_RESOURCE));
            resource.set(CONFIG_KEY_NAME, wrapConfig(entry.getValue(), resource));
            resourceStructList.add(resource);
        }
        struct.set(RESOURCE_KEY_NAME, resourceStructList.toArray(new Struct[0]));
        return struct;
    }
}
