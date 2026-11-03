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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.util.ConnectUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

final class ShareConsumerSettings {
    static final String RUNTIME_UNAVAILABLE_MESSAGE = "Share sink runtime and offset APIs are not supported yet; "
            + "ShareSinkTask requires a durable-write share runtime";
    private static final ConfigDef CONSUMER_CONFIG_DEF = ConsumerConfig.configDef();
    private static final List<String> UNSUPPORTED_CONFIGS = List.of(
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
            ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,
            ConsumerConfig.ISOLATION_LEVEL_CONFIG,
            ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
            ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
            ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,
            ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,
            ConsumerConfig.GROUP_PROTOCOL_CONFIG,
            ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG
    );

    private ShareConsumerSettings() {
    }

    static String resolveGroupId(String connectorName, Map<String, ?> connectorProperties) {
        Objects.requireNonNull(connectorProperties, "connectorProperties");
        if (!connectorProperties.containsKey(SinkConnectorConfig.SHARE_GROUP_ID_CONFIG)) {
            if (connectorName == null || connectorName.trim().isEmpty()) {
                throw new ConfigException(ConnectorConfig.NAME_CONFIG, connectorName,
                        "A connector name is required to derive share.group.id");
            }
            return "connect-share-" + connectorName.trim();
        }
        Object value = connectorProperties.get(SinkConnectorConfig.SHARE_GROUP_ID_CONFIG);
        if (!(value instanceof String) || ((String) value).trim().isEmpty()) {
            throw new ConfigException(SinkConnectorConfig.SHARE_GROUP_ID_CONFIG, value,
                    "Must be a non-blank string");
        }
        return ((String) value).trim();
    }

    // A null resolved group permits collecting other errors after an invalid share.group.id.
    static Map<String, String> validationErrors(Map<String, ?> overrides, String resolvedGroupId) {
        Objects.requireNonNull(overrides, "overrides");
        Map<String, String> errors = new LinkedHashMap<>();
        Map<String, ConfigDef.ConfigKey> nativeConfigs = CONSUMER_CONFIG_DEF.configKeys();
        for (String key : UNSUPPORTED_CONFIGS) {
            if (overrides.containsKey(key)) {
                errors.put(key, "Cannot be set when using a share group");
            }
        }
        validateMode(overrides, nativeConfigs.get(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG), "explicit", errors);
        validateMode(overrides, nativeConfigs.get(ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG), "record_limit", errors);
        validateGroupId(overrides, resolvedGroupId, errors);
        for (Map.Entry<String, ?> entry : overrides.entrySet()) {
            String key = entry.getKey();
            ConfigDef.ConfigKey config = nativeConfigs.get(key);
            if (config == null || errors.containsKey(key) || ConsumerConfig.GROUP_ID_CONFIG.equals(key)
                    || ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG.equals(key)
                    || ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG.equals(key)) {
                continue;
            }
            try {
                Object value = ConfigDef.parseType(key, entry.getValue(), config.type);
                if (config.validator != null) {
                    config.validator.ensureValid(key, value);
                }
            } catch (ConfigException e) {
                errors.put(key, e.getMessage());
            }
        }
        return Collections.unmodifiableMap(errors);
    }

    private static void validateGroupId(Map<String, ?> overrides, String resolvedGroupId, Map<String, String> errors) {
        if (!overrides.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
            return;
        }
        Object value = overrides.get(ConsumerConfig.GROUP_ID_CONFIG);
        if (!(value instanceof String) || ((String) value).trim().isEmpty()) {
            errors.put(ConsumerConfig.GROUP_ID_CONFIG, "Must be a non-blank string");
        } else if (resolvedGroupId != null && !resolvedGroupId.equals(((String) value).trim())) {
            errors.put(ConsumerConfig.GROUP_ID_CONFIG,
                    "Must match the resolved " + SinkConnectorConfig.SHARE_GROUP_ID_CONFIG);
        }
    }

    // Common properties must omit ordinary consumer defaults; connector overrides must be policy-validated.
    static Map<String, Object> create(String connectorName,
                                      Map<String, ?> connectorProperties,
                                      Map<String, ?> commonClientProperties,
                                      Map<String, ?> workerConsumerOverrides,
                                      Map<String, ?> validatedConnectorOverrides) {
        String groupId = resolveGroupId(connectorName, connectorProperties);
        requireValid(commonClientProperties, groupId);
        requireValid(workerConsumerOverrides, groupId);
        requireValid(validatedConnectorOverrides, groupId);
        Map<String, Object> result = new HashMap<>(commonClientProperties);
        result.putAll(workerConsumerOverrides);
        result.putAll(validatedConnectorOverrides);
        result.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        result.put(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "explicit");
        result.put(ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG, "record_limit");
        ConnectUtils.enforceSynchronousBootstrapResolution(result);
        return result;
    }

    private static void requireValid(Map<String, ?> properties, String groupId) {
        Map<String, String> errors = validationErrors(properties, groupId);
        if (!errors.isEmpty()) {
            Map.Entry<String, String> error = errors.entrySet().iterator().next();
            throw new ConfigException(error.getKey(), properties.get(error.getKey()), error.getValue());
        }
    }

    private static void validateMode(Map<String, ?> properties, ConfigDef.ConfigKey config, String required,
                                     Map<String, String> errors) {
        String key = config.name;
        if (!properties.containsKey(key)) {
            return;
        }
        try {
            String mode = (String) ConfigDef.parseType(key, properties.get(key), ConfigDef.Type.STRING);
            config.validator.ensureValid(key, mode);
            if (required.equalsIgnoreCase(mode)) {
                return;
            }
        } catch (ConfigException e) {
            // Report the runtime requirement for invalid native values as well.
        }
        errors.put(key, "Share sinks require " + key + "=" + required);
    }
}
