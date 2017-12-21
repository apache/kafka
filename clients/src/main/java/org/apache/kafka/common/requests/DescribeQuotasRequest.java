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
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.protocol.types.Type.INT8;
import static org.apache.kafka.common.protocol.types.Type.NULLABLE_STRING;
import static org.apache.kafka.common.protocol.types.Type.STRING;

public class DescribeQuotasRequest extends AbstractRequest {

    private static final String QUOTA_CONFIG_RESOURCE = "quota_config_resource";
    private static final String CHILD_QUOTA_CONFIG_RESOURCE = "child_quota_config_resource";
    private static final String QUOTA_TYPE_KEY_NAME = "type";
    private static final String QUOTA_NAME_KEY_NAME = "name";
    private static final String CONFIG_NAME_KEY_NAME = "config_name";
    private static final String RESOURCE = "resource";

    private static final Schema QUOTA_SCHEMA_V0 = new Schema(
            new Field(QUOTA_TYPE_KEY_NAME, INT8),
            new Field(QUOTA_NAME_KEY_NAME, NULLABLE_STRING)
    );

    private static final Schema RESOURCE_SCHEMA = new Schema(
            new Field(QUOTA_CONFIG_RESOURCE, QUOTA_SCHEMA_V0),
            new Field(CHILD_QUOTA_CONFIG_RESOURCE, QUOTA_SCHEMA_V0),
            new Field(CONFIG_NAME_KEY_NAME, new ArrayOf(STRING)));

    private static final Schema DESCRIBE_QUOTAS_REQUEST_SCHEMA_V0 = new Schema(
            new Field(RESOURCE, new ArrayOf(RESOURCE_SCHEMA))
    );

    public static Schema[] schemaVersions() {
        return new Schema[]{DESCRIBE_QUOTAS_REQUEST_SCHEMA_V0};
    }

    public static class Builder extends AbstractRequest.Builder {
        private final Map<QuotaConfigResourceTuple, Collection<String>> quotaConfigSettings;

        public Builder(Map<QuotaConfigResourceTuple, Collection<String>> quotaConfigSettings) {
            super(ApiKeys.DESCRIBE_QUOTAS);
            this.quotaConfigSettings = quotaConfigSettings;
        }

        @Override
        public DescribeQuotasRequest build(short version) {
            return new DescribeQuotasRequest(version, quotaConfigSettings);
        }
    }

    private final Map<QuotaConfigResourceTuple, Collection<String>> quotaConfigSettings;

    public DescribeQuotasRequest(short version, Map<QuotaConfigResourceTuple, Collection<String>> quotaConfigSettings) {
        super(version);
        this.quotaConfigSettings = quotaConfigSettings;
    }

    public DescribeQuotasRequest(Struct struct, short version) {
        super(version);
        Object[] resourcesObjectArray = struct.getArray(RESOURCE);
        quotaConfigSettings = new HashMap<>(resourcesObjectArray.length);
        for (Object resourceObj : resourcesObjectArray) {
            Struct resource = (Struct) resourceObj;
            QuotaConfigResourceTuple tuple = new QuotaConfigResourceTuple(
                    unwrapQuotaConfigResource(resource.getStruct(QUOTA_CONFIG_RESOURCE)),
                    unwrapQuotaConfigResource(resource.getStruct(CHILD_QUOTA_CONFIG_RESOURCE))
            );
            Object[] configNamesArray = resource.getArray(CONFIG_NAME_KEY_NAME);
            Collection<String> configNames;
            if (configNamesArray != null) {
                configNames = new ArrayList<>(configNamesArray.length);
                for (Object configNameObj : configNamesArray)
                    configNames.add((String) configNameObj);
            } else {
                configNames = new ArrayList<>(0);
            }
            quotaConfigSettings.put(tuple, configNames);
        }
    }

    public Map<QuotaConfigResourceTuple, Collection<String>> quotaConfigSettings() {
        return Collections.unmodifiableMap(quotaConfigSettings);
    }

    private Struct wrapQuotaConfigResource(Resource quota, Struct parent, String field) {
        Struct struct = parent.instance(field);
        struct.set(QUOTA_TYPE_KEY_NAME, quota.type().id());
        struct.set(QUOTA_NAME_KEY_NAME, quota.name());
        return struct;
    }

    private Resource unwrapQuotaConfigResource(Struct quotaStruct) {
        ResourceType type = ResourceType.forId(quotaStruct.getByte(QUOTA_TYPE_KEY_NAME));
        return new Resource(type, quotaStruct.getString(QUOTA_NAME_KEY_NAME));
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.DESCRIBE_QUOTAS.requestSchema(version()));
        List<Struct> resourceStruct = new LinkedList<>();
        for (Map.Entry<QuotaConfigResourceTuple, Collection<String>> entry : quotaConfigSettings.entrySet()) {
            Struct resource = struct.instance(RESOURCE);

            QuotaConfigResourceTuple tuple = entry.getKey();
            resource.set(QUOTA_CONFIG_RESOURCE, wrapQuotaConfigResource(tuple.quotaConfigResource(), resource, QUOTA_CONFIG_RESOURCE));
            resource.set(CHILD_QUOTA_CONFIG_RESOURCE, wrapQuotaConfigResource(tuple.childQuotaConfigResource(), resource, CHILD_QUOTA_CONFIG_RESOURCE));

            String[] configNames = entry.getValue() == null ? null : entry.getValue().toArray(new String[0]);
            resource.set(CONFIG_NAME_KEY_NAME, configNames);
            resourceStruct.add(resource);
        }
        struct.set(RESOURCE, resourceStruct.toArray(new Struct[0]));
        return struct;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
//        short version = version();
//        switch (version) {
//            case 0:
//                ApiError error = ApiError.fromThrowable(e);
//                Map<Resource, DescribeConfigsResponse.Config> errors = new HashMap<>(resources().size());
//                DescribeConfigsResponse.Config config = new DescribeConfigsResponse.Config(error,
//                        Collections.<DescribeConfigsResponse.ConfigEntry>emptyList());
//                for (Resource resource : resources())
//                    errors.put(resource, config);
//                return new DescribeConfigsResponse(throttleTimeMs, errors);
//            default:
//                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
//                        version, this.getClass().getSimpleName(), ApiKeys.DESCRIBE_CONFIGS.latestVersion()));
//        }
        return null;
    }

    public static DescribeQuotasRequest parse(ByteBuffer buffer, short version) {
        return new DescribeQuotasRequest(ApiKeys.DESCRIBE_QUOTAS.parseRequest(version, buffer), version);
    }
}
