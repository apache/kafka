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
import org.apache.kafka.common.message.DescribeConfigsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DescribeConfigsResponse extends AbstractResponse {

    public static class Config {
        private final ApiError error;
        private final Collection<ConfigEntry> entries;

        public Config(ApiError error, Collection<ConfigEntry> entries) {
            this.error = Objects.requireNonNull(error, "error");
            this.entries = Objects.requireNonNull(entries, "entries");
        }

        public ApiError error() {
            return error;
        }

        public Collection<ConfigEntry> entries() {
            return entries;
        }
    }

    public static class ConfigEntry {
        private final String name;
        private final String value;
        private final boolean isSensitive;
        private final ConfigSource source;
        private final boolean readOnly;
        private final Collection<ConfigSynonym> synonyms;
        private final ConfigType type;
        private final String documentation;

        public ConfigEntry(String name, String value, ConfigSource source, boolean isSensitive, boolean readOnly,
            Collection<ConfigSynonym> synonyms) {
            this(name, value, source, isSensitive, readOnly, synonyms, ConfigType.UNKNOWN, null);
        }

        public ConfigEntry(String name, String value, ConfigSource source, boolean isSensitive, boolean readOnly,
                           Collection<ConfigSynonym> synonyms, ConfigType type, String documentation) {

            this.name = Objects.requireNonNull(name, "name");
            this.value = value;
            this.source = Objects.requireNonNull(source, "source");
            this.isSensitive = isSensitive;
            this.readOnly = readOnly;
            this.synonyms = Objects.requireNonNull(synonyms, "synonyms");
            this.type = type;
            this.documentation = documentation;
        }

        public String name() {
            return name;
        }

        public String value() {
            return value;
        }

        public boolean isSensitive() {
            return isSensitive;
        }

        public ConfigSource source() {
            return source;
        }

        public boolean isReadOnly() {
            return readOnly;
        }

        public Collection<ConfigSynonym> synonyms() {
            return synonyms;
        }

        public ConfigType type() {
            return type;
        }

        public String documentation() {
            return documentation;
        }
    }

    public enum ConfigSource {
        UNKNOWN((byte) 0, org.apache.kafka.clients.admin.ConfigEntry.ConfigSource.UNKNOWN),
        TOPIC_CONFIG((byte) 1, org.apache.kafka.clients.admin.ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG),
        DYNAMIC_BROKER_CONFIG((byte) 2, org.apache.kafka.clients.admin.ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG),
        DYNAMIC_DEFAULT_BROKER_CONFIG((byte) 3, org.apache.kafka.clients.admin.ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG),
        STATIC_BROKER_CONFIG((byte) 4, org.apache.kafka.clients.admin.ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG),
        DEFAULT_CONFIG((byte) 5, org.apache.kafka.clients.admin.ConfigEntry.ConfigSource.DEFAULT_CONFIG),
        DYNAMIC_BROKER_LOGGER_CONFIG((byte) 6, org.apache.kafka.clients.admin.ConfigEntry.ConfigSource.DYNAMIC_BROKER_LOGGER_CONFIG);

        final byte id;
        private final org.apache.kafka.clients.admin.ConfigEntry.ConfigSource source;
        private static final ConfigSource[] VALUES = values();

        ConfigSource(byte id, org.apache.kafka.clients.admin.ConfigEntry.ConfigSource source) {
            this.id = id;
            this.source = source;
        }

        public byte id() {
            return id;
        }

        public static ConfigSource forId(byte id) {
            if (id < 0)
                throw new IllegalArgumentException("id should be positive, id: " + id);
            if (id >= VALUES.length)
                return UNKNOWN;
            return VALUES[id];
        }

        public org.apache.kafka.clients.admin.ConfigEntry.ConfigSource source() {
            return source;
        }
    }

    public enum ConfigType {
        UNKNOWN((byte) 0, org.apache.kafka.clients.admin.ConfigEntry.ConfigType.UNKNOWN),
        BOOLEAN((byte) 1, org.apache.kafka.clients.admin.ConfigEntry.ConfigType.BOOLEAN),
        STRING((byte) 2, org.apache.kafka.clients.admin.ConfigEntry.ConfigType.STRING),
        INT((byte) 3, org.apache.kafka.clients.admin.ConfigEntry.ConfigType.INT),
        SHORT((byte) 4, org.apache.kafka.clients.admin.ConfigEntry.ConfigType.SHORT),
        LONG((byte) 5, org.apache.kafka.clients.admin.ConfigEntry.ConfigType.LONG),
        DOUBLE((byte) 6, org.apache.kafka.clients.admin.ConfigEntry.ConfigType.DOUBLE),
        LIST((byte) 7, org.apache.kafka.clients.admin.ConfigEntry.ConfigType.LIST),
        CLASS((byte) 8, org.apache.kafka.clients.admin.ConfigEntry.ConfigType.CLASS),
        PASSWORD((byte) 9, org.apache.kafka.clients.admin.ConfigEntry.ConfigType.PASSWORD);

        final byte id;
        final org.apache.kafka.clients.admin.ConfigEntry.ConfigType type;
        private static final ConfigType[] VALUES = values();

        ConfigType(byte id, org.apache.kafka.clients.admin.ConfigEntry.ConfigType type) {
            this.id = id;
            this.type = type;
        }

        public byte id() {
            return id;
        }

        public static ConfigType forId(byte id) {
            if (id < 0)
                throw new IllegalArgumentException("id should be positive, id: " + id);
            if (id >= VALUES.length)
                return UNKNOWN;
            return VALUES[id];
        }

        public org.apache.kafka.clients.admin.ConfigEntry.ConfigType type() {
            return type;
        }
    }

    public static class ConfigSynonym {
        private final String name;
        private final String value;
        private final ConfigSource source;

        public ConfigSynonym(String name, String value, ConfigSource source) {
            this.name = Objects.requireNonNull(name, "name");
            this.value = value;
            this.source = Objects.requireNonNull(source, "source");
        }

        public String name() {
            return name;
        }
        public String value() {
            return value;
        }
        public ConfigSource source() {
            return source;
        }
    }

    public Map<ConfigResource, DescribeConfigsResponseData.DescribeConfigsResult> resultMap() {
        return data().results().stream().collect(Collectors.toMap(
            configsResult ->
                    new ConfigResource(ConfigResource.Type.forId(configsResult.resourceType()),
                            configsResult.resourceName()),
            Function.identity()));
    }

    private final DescribeConfigsResponseData data;

    public DescribeConfigsResponse(DescribeConfigsResponseData data) {
        super(ApiKeys.DESCRIBE_CONFIGS);
        this.data = data;
    }

    // This constructor should only be used after deserialization, it has special handling for version 0
    private DescribeConfigsResponse(DescribeConfigsResponseData data, short version) {
        super(ApiKeys.DESCRIBE_CONFIGS);
        this.data = data;
        if (version == 0) {
            for (DescribeConfigsResponseData.DescribeConfigsResult result : data.results()) {
                for (DescribeConfigsResponseData.DescribeConfigsResourceResult config : result.configs()) {
                    if (config.isDefault()) {
                        config.setConfigSource(ConfigSource.DEFAULT_CONFIG.id);
                    } else {
                        if (result.resourceType() == ConfigResource.Type.BROKER.id()) {
                            config.setConfigSource(ConfigSource.STATIC_BROKER_CONFIG.id);
                        } else if (result.resourceType() == ConfigResource.Type.TOPIC.id()) {
                            config.setConfigSource(ConfigSource.TOPIC_CONFIG.id);
                        } else {
                            config.setConfigSource(ConfigSource.UNKNOWN.id);
                        }
                    }
                }
            }
        }
    }

    @Override
    public DescribeConfigsResponseData data() {
        return data;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        data.results().forEach(response ->
            updateErrorCounts(errorCounts, Errors.forCode(response.errorCode()))
        );
        return errorCounts;
    }

    public static DescribeConfigsResponse parse(ByteBuffer buffer, short version) {
        return new DescribeConfigsResponse(new DescribeConfigsResponseData(new ByteBufferAccessor(buffer), version), version);
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 2;
    }
}
