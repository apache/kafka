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

package org.apache.kafka.controller;

import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.protocol.ApiMessageAndVersion;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.APPEND;

public class ConfigurationControlManager {
    private final SnapshotRegistry snapshotRegistry;
    private final Map<ConfigResource.Type, ConfigDef> configDefs;
    private final TimelineHashMap<ConfigResource, TimelineHashMap<String, String>> configData;

    ConfigurationControlManager(SnapshotRegistry snapshotRegistry,
                                Map<ConfigResource.Type, ConfigDef> configDefs) {
        this.snapshotRegistry = snapshotRegistry;
        this.configDefs = configDefs;
        this.configData = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    /**
     * Determine the result of applying a batch of incremental configuration changes.  Note
     * that this method does not change the contents of memory.  It just generates a
     * result, that you can replay later if you wish using replay().
     *
     * Note that there can only be one result per ConfigResource.  So if you try to modify
     * several keys and one modification fails, the whole ConfigKey fails and nothing gets
     * changed.
     *
     * @param configChanges     Maps each resource to a map from config keys to
     *                          operation data.
     * @return                  The result.
     */
    ControllerResult<Map<ConfigResource, ApiError>> incrementalAlterConfigs(
            Map<ConfigResource, Map<String, Entry<OpType, String>>> configChanges) {
        List<ApiMessageAndVersion> outputRecords = new ArrayList<>();
        Map<ConfigResource, ApiError> outputResults = new HashMap<>();
        for (Entry<ConfigResource, Map<String, Entry<OpType, String>>> resourceEntry :
                configChanges.entrySet()) {
            incrementalAlterConfigResource(resourceEntry.getKey(),
                resourceEntry.getValue(),
                outputRecords,
                outputResults);
        }
        return new ControllerResult<>(outputRecords, outputResults);
    }

    private void incrementalAlterConfigResource(ConfigResource configResource,
                                                Map<String, Entry<OpType, String>> keysToOps,
                                                List<ApiMessageAndVersion> outputRecords,
                                                Map<ConfigResource, ApiError> outputResults) {
        ApiError error = configResourceError(configResource);
        if (error.isFailure()) {
            outputResults.put(configResource, error);
            return;
        }
        List<ApiMessageAndVersion> newRecords = new ArrayList<>();
        for (Entry<String, Entry<OpType, String>> keysToOpsEntry : keysToOps.entrySet()) {
            String key = keysToOpsEntry.getKey();
            String currentValue = null;
            TimelineHashMap<String, String> currentConfigs = configData.get(configResource);
            if (currentConfigs != null) {
                currentValue = currentConfigs.get(key);
            }
            String newValue = currentValue;
            Entry<OpType, String> opTypeAndNewValue = keysToOpsEntry.getValue();
            OpType opType = opTypeAndNewValue.getKey();
            String opValue = opTypeAndNewValue.getValue();
            switch (opType) {
                case SET:
                    newValue = opValue;
                    break;
                case DELETE:
                    if (opValue != null) {
                        outputResults.put(configResource, new ApiError(
                            Errors.INVALID_REQUEST, "A DELETE op was given with a " +
                            "non-null value."));
                        return;
                    }
                    newValue = null;
                    break;
                case APPEND:
                case SUBTRACT:
                    if (!isSplittable(configResource.type(), key)) {
                        outputResults.put(configResource, new ApiError(
                            Errors.INVALID_CONFIG, "Can't " + opType + " to " +
                            "key " + key + " because its type is not LIST."));
                        return;
                    }
                    List<String> newValueParts = getParts(newValue, key, configResource);
                    if (opType == APPEND) {
                        if (!newValueParts.contains(opValue)) {
                            newValueParts.add(opValue);
                        }
                        newValue = String.join(",", newValueParts);
                    } else if (newValueParts.remove(opValue)) {
                        newValue = String.join(",", newValueParts);
                    }
                    break;
            }
            if (!Objects.equals(currentValue, newValue)) {
                newRecords.add(new ApiMessageAndVersion(new ConfigRecord().
                    setResourceType(configResource.type().id()).
                    setResourceName(configResource.name()).
                    setName(key).
                    setValue(newValue), (short) 0));
            }
        }
        outputRecords.addAll(newRecords);
        outputResults.put(configResource, ApiError.NONE);
    }

    private List<String> getParts(String value, String key, ConfigResource configResource) {
        if (value == null) {
            value = getConfigValueDefault(configResource.type(), key);
        }
        List<String> parts = new ArrayList<>();
        String[] splitValues = value.split(",");
        for (String splitValue : splitValues) {
            if (!splitValue.isEmpty()) {
                parts.add(splitValue);
            }
        }
        return parts;
    }

    private static ApiError configResourceError(ConfigResource configResource) {
        switch (configResource.type()) {
            case BROKER_LOGGER:
                // TODO: are there any rules for the names of these?
                return ApiError.NONE;
            case BROKER:
                if (!configResource.name().isEmpty()) {
                    try {
                        int brokerId = Integer.parseInt(configResource.name());
                        if (brokerId < 0) {
                            return new ApiError(Errors.INVALID_REQUEST, "Cannot perform " +
                                "operations on a BROKER resource with a negative integer name.");
                        }
                    } catch (NumberFormatException e) {
                        return new ApiError(Errors.INVALID_REQUEST, "Cannot perform " +
                            "operations on a BROKER resource with a non-integer name.");
                    }
                }
                return ApiError.NONE;
            case TOPIC:
                if (!configResource.name().isEmpty()) {
                    try {
                        Topic.validate(configResource.name());
                    } catch (Exception e) {
                        return new ApiError(Errors.INVALID_REQUEST, "Cannot perform " +
                            "operations on a TOPIC resource with an illegal topic name.");
                    }
                }
                return ApiError.NONE;
            case UNKNOWN:
                return new ApiError(Errors.INVALID_REQUEST, "Cannot perform operations " +
                    "on a configuration resource with type UNKNOWN.");
            default:
                return new ApiError(Errors.INVALID_REQUEST, "Cannot perform operations " +
                    "on a configuration resource with an unexpected type.");
        }
    }

    private boolean isSplittable(ConfigResource.Type type, String key) {
        ConfigDef configDef = configDefs.get(type);
        if (configDef == null) {
            return false;
        }
        ConfigKey configKey = configDef.configKeys().get(key);
        if (configKey == null) {
            return false;
        }
        return configKey.type == ConfigDef.Type.LIST;
    }

    private String getConfigValueDefault(ConfigResource.Type type, String key) {
        ConfigDef configDef = configDefs.get(type);
        if (configDef == null) {
            return null;
        }
        ConfigKey configKey = configDef.configKeys().get(key);
        if (configKey == null) {
            return null;
        }
        if (configKey.defaultValue == null) {
            return null;
        }
        return ConfigDef.convertToString(configKey.defaultValue, configKey.type);
    }

    /**
     * Apply a configuration record to the in-memory state.
     *
     * @param record            The ConfigRecord.
     */
    void replay(ConfigRecord record) {
        Type type = Type.forId(record.resourceType());
        ConfigResource configResource = new ConfigResource(type, record.resourceName());
        TimelineHashMap<String, String> configs = configData.get(configResource);
        if (configs == null) {
            configs = new TimelineHashMap<>(snapshotRegistry, 0);
            configData.put(configResource, configs);
        }
        configs.put(record.name(), record.value());
    }
}
