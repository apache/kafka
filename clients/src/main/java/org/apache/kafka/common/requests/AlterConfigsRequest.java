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

import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.common.protocol.types.Type.BOOLEAN;
import static org.apache.kafka.common.protocol.types.Type.INT8;
import static org.apache.kafka.common.protocol.types.Type.NULLABLE_STRING;
import static org.apache.kafka.common.protocol.types.Type.STRING;

public class AlterConfigsRequest extends AbstractRequest {

    private static final String RESOURCES_KEY_NAME = "resources";
    private static final String RESOURCE_TYPE_KEY_NAME = "resource_type";
    private static final String RESOURCE_NAME_KEY_NAME = "resource_name";
    private static final String VALIDATE_ONLY_KEY_NAME = "validate_only";

    private static final String CONFIG_ENTRIES_KEY_NAME = "config_entries";
    private static final String CONFIG_NAME = "config_name";
    private static final String CONFIG_VALUE = "config_value";

    private static final Schema CONFIG_ENTRY = new Schema(
            new Field(CONFIG_NAME, STRING, "Configuration name"),
            new Field(CONFIG_VALUE, NULLABLE_STRING, "Configuration value"));

    private static final Schema ALTER_CONFIGS_REQUEST_RESOURCE_V0 = new Schema(
            new Field(RESOURCE_TYPE_KEY_NAME, INT8),
            new Field(RESOURCE_NAME_KEY_NAME, STRING),
            new Field(CONFIG_ENTRIES_KEY_NAME, new ArrayOf(CONFIG_ENTRY)));

    private static final Schema ALTER_CONFIGS_REQUEST_V0 = new Schema(
            new Field(RESOURCES_KEY_NAME, new ArrayOf(ALTER_CONFIGS_REQUEST_RESOURCE_V0),
                    "An array of resources to update with the provided configs."),
            new Field(VALIDATE_ONLY_KEY_NAME, BOOLEAN));

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema ALTER_CONFIGS_REQUEST_V1 = ALTER_CONFIGS_REQUEST_V0;

    public static Schema[] schemaVersions() {
        return new Schema[] {ALTER_CONFIGS_REQUEST_V0, ALTER_CONFIGS_REQUEST_V1};
    }

    public static class Config {
        private final Collection<ConfigEntry> entries;

        public Config(Collection<ConfigEntry> entries) {
            this.entries = Objects.requireNonNull(entries, "entries");
        }

        public Collection<ConfigEntry> entries() {
            return entries;
        }
    }

    public static class ConfigEntry {
        private final String name;
        private final String value;

        public ConfigEntry(String name, String value) {
            this.name = Objects.requireNonNull(name, "name");
            this.value = Objects.requireNonNull(value, "value");
        }

        public String name() {
            return name;
        }

        public String value() {
            return value;
        }

    }

    public static class Builder extends AbstractRequest.Builder {

        private final Map<ConfigResource, Config> configs;
        private final boolean validateOnly;

        public Builder(Map<ConfigResource, Config> configs, boolean validateOnly) {
            super(ApiKeys.ALTER_CONFIGS);
            this.configs = Objects.requireNonNull(configs, "configs");
            this.validateOnly = validateOnly;
        }

        @Override
        public AlterConfigsRequest build(short version) {
            return new AlterConfigsRequest(version, configs, validateOnly);
        }
    }

    private final Map<ConfigResource, Config> configs;
    private final boolean validateOnly;

    public AlterConfigsRequest(short version, Map<ConfigResource, Config> configs, boolean validateOnly) {
        super(ApiKeys.ALTER_CONFIGS, version);
        this.configs = Objects.requireNonNull(configs, "configs");
        this.validateOnly = validateOnly;
    }

    public AlterConfigsRequest(Struct struct, short version) {
        super(ApiKeys.ALTER_CONFIGS, version);
        validateOnly = struct.getBoolean(VALIDATE_ONLY_KEY_NAME);
        Object[] resourcesArray = struct.getArray(RESOURCES_KEY_NAME);
        configs = new HashMap<>(resourcesArray.length);
        for (Object resourcesObj : resourcesArray) {
            Struct resourcesStruct = (Struct) resourcesObj;

            ConfigResource.Type resourceType = ConfigResource.Type.forId(resourcesStruct.getByte(RESOURCE_TYPE_KEY_NAME));
            String resourceName = resourcesStruct.getString(RESOURCE_NAME_KEY_NAME);
            ConfigResource resource = new ConfigResource(resourceType, resourceName);

            Object[] configEntriesArray = resourcesStruct.getArray(CONFIG_ENTRIES_KEY_NAME);
            List<ConfigEntry> configEntries = new ArrayList<>(configEntriesArray.length);
            for (Object configEntriesObj: configEntriesArray) {
                Struct configEntriesStruct = (Struct) configEntriesObj;
                String configName = configEntriesStruct.getString(CONFIG_NAME);
                String configValue = configEntriesStruct.getString(CONFIG_VALUE);
                configEntries.add(new ConfigEntry(configName, configValue));
            }
            Config config = new Config(configEntries);
            configs.put(resource, config);
        }
    }

    public Map<ConfigResource, Config> configs() {
        return configs;
    }

    public boolean validateOnly() {
        return validateOnly;
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.ALTER_CONFIGS.requestSchema(version()));
        struct.set(VALIDATE_ONLY_KEY_NAME, validateOnly);
        List<Struct> resourceStructs = new ArrayList<>(configs.size());
        for (Map.Entry<ConfigResource, Config> entry : configs.entrySet()) {
            Struct resourceStruct = struct.instance(RESOURCES_KEY_NAME);

            ConfigResource resource = entry.getKey();
            resourceStruct.set(RESOURCE_TYPE_KEY_NAME, resource.type().id());
            resourceStruct.set(RESOURCE_NAME_KEY_NAME, resource.name());

            Config config = entry.getValue();
            List<Struct> configEntryStructs = new ArrayList<>(config.entries.size());
            for (ConfigEntry configEntry : config.entries) {
                Struct configEntriesStruct = resourceStruct.instance(CONFIG_ENTRIES_KEY_NAME);
                configEntriesStruct.set(CONFIG_NAME, configEntry.name);
                configEntriesStruct.set(CONFIG_VALUE, configEntry.value);
                configEntryStructs.add(configEntriesStruct);
            }
            resourceStruct.set(CONFIG_ENTRIES_KEY_NAME, configEntryStructs.toArray(new Struct[0]));

            resourceStructs.add(resourceStruct);
        }
        struct.set(RESOURCES_KEY_NAME, resourceStructs.toArray(new Struct[0]));
        return struct;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        short version = version();
        switch (version) {
            case 0:
            case 1:
                ApiError error = ApiError.fromThrowable(e);
                Map<ConfigResource, ApiError> errors = new HashMap<>(configs.size());
                for (ConfigResource resource : configs.keySet())
                    errors.put(resource, error);
                return new AlterConfigsResponse(throttleTimeMs, errors);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        version, this.getClass().getSimpleName(), ApiKeys.ALTER_CONFIGS.latestVersion()));
        }
    }

    public static AlterConfigsRequest parse(ByteBuffer buffer, short version) {
        return new AlterConfigsRequest(ApiKeys.ALTER_CONFIGS.parseRequest(version, buffer), version);
    }
}
