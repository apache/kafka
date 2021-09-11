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
import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;

import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.APPEND;
import static org.apache.kafka.common.metadata.MetadataRecordType.CONFIG_RECORD;


public class ConfigurationControlManager {
    private final Logger log;
    private final SnapshotRegistry snapshotRegistry;
    private final Map<ConfigResource.Type, ConfigDef> configDefs;
    private final TimelineHashMap<ConfigResource, TimelineHashMap<String, String>> configData;

    ConfigurationControlManager(LogContext logContext,
                                SnapshotRegistry snapshotRegistry,
                                Map<ConfigResource.Type, ConfigDef> configDefs) {
        this.log = logContext.logger(ConfigurationControlManager.class);
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
        return ControllerResult.atomicOf(outputRecords, outputResults);
    }

    private void incrementalAlterConfigResource(ConfigResource configResource,
                                                Map<String, Entry<OpType, String>> keysToOps,
                                                List<ApiMessageAndVersion> outputRecords,
                                                Map<ConfigResource, ApiError> outputResults) {
        ApiError error = checkConfigResource(configResource);
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
                    setValue(newValue), CONFIG_RECORD.highestSupportedVersion()));
            }
        }
        outputRecords.addAll(newRecords);
        outputResults.put(configResource, ApiError.NONE);
    }

    /**
     * Determine the result of applying a batch of legacy configuration changes.  Note
     * that this method does not change the contents of memory.  It just generates a
     * result, that you can replay later if you wish using replay().
     *
     * @param newConfigs        The new configurations to install for each resource.
     *                          All existing configurations will be overwritten.
     * @return                  The result.
     */
    ControllerResult<Map<ConfigResource, ApiError>> legacyAlterConfigs(
        Map<ConfigResource, Map<String, String>> newConfigs) {
        List<ApiMessageAndVersion> outputRecords = new ArrayList<>();
        Map<ConfigResource, ApiError> outputResults = new HashMap<>();
        for (Entry<ConfigResource, Map<String, String>> resourceEntry :
            newConfigs.entrySet()) {
            legacyAlterConfigResource(resourceEntry.getKey(),
                resourceEntry.getValue(),
                outputRecords,
                outputResults);
        }
        return ControllerResult.atomicOf(outputRecords, outputResults);
    }

    private void legacyAlterConfigResource(ConfigResource configResource,
                                           Map<String, String> newConfigs,
                                           List<ApiMessageAndVersion> outputRecords,
                                           Map<ConfigResource, ApiError> outputResults) {
        ApiError error = checkConfigResource(configResource);
        if (error.isFailure()) {
            outputResults.put(configResource, error);
            return;
        }

        Map<String, String> currentConfigs = configData.get(configResource);
        if (currentConfigs == null) {
            currentConfigs = Collections.emptyMap();
        }

        List<ApiMessageAndVersion> newRecords = new ArrayList<>();
        for (Entry<String, String> entry : newConfigs.entrySet()) {
            String key = entry.getKey();
            String newValue = entry.getValue();
            String currentValue = currentConfigs.get(key);
            if (!Objects.equals(newValue, currentValue)) {
                newRecords.add(new ApiMessageAndVersion(new ConfigRecord().
                    setResourceType(configResource.type().id()).
                    setResourceName(configResource.name()).
                    setName(key).
                    setValue(newValue), CONFIG_RECORD.highestSupportedVersion()));
            }
        }
        for (String key : currentConfigs.keySet()) {
            if (!newConfigs.containsKey(key)) {
                newRecords.add(new ApiMessageAndVersion(new ConfigRecord().
                    setResourceType(configResource.type().id()).
                    setResourceName(configResource.name()).
                    setName(key).
                    setValue(null), CONFIG_RECORD.highestSupportedVersion()));
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
        if (value == null) {
            return parts;
        }
        String[] splitValues = value.split(",");
        for (String splitValue : splitValues) {
            if (!splitValue.isEmpty()) {
                parts.add(splitValue);
            }
        }
        return parts;
    }

    static ApiError checkConfigResource(ConfigResource configResource) {
        switch (configResource.type()) {
            case BROKER_LOGGER:
                // We do not handle resources of type BROKER_LOGGER in
                // ConfigurationControlManager, since they are not persisted to the
                // metadata log.
                //
                // When using incrementalAlterConfigs, we handle changes to BROKER_LOGGER
                // in ControllerApis.scala.  When using the legacy alterConfigs,
                // BROKER_LOGGER is not supported at all.
                return new ApiError(Errors.INVALID_REQUEST, "Unsupported " +
                    "configuration resource type BROKER_LOGGER ");
            case BROKER:
                // Note: A Resource with type BROKER and an empty name represents a
                // cluster configuration that applies to all brokers.
                if (!configResource.name().isEmpty()) {
                    try {
                        int brokerId = Integer.parseInt(configResource.name());
                        if (brokerId < 0) {
                            return new ApiError(Errors.INVALID_REQUEST, "Illegal " +
                                "negative broker ID in BROKER resource.");
                        }
                    } catch (NumberFormatException e) {
                        return new ApiError(Errors.INVALID_REQUEST, "Illegal " +
                            "non-integral BROKER resource type name.");
                    }
                }
                return ApiError.NONE;
            case TOPIC:
                try {
                    Topic.validate(configResource.name());
                } catch (Exception e) {
                    return new ApiError(Errors.INVALID_REQUEST, "Illegal topic name.");
                }
                return ApiError.NONE;
            case UNKNOWN:
                return new ApiError(Errors.INVALID_REQUEST, "Unsupported configuration " +
                    "resource type UNKNOWN.");
            default:
                return new ApiError(Errors.INVALID_REQUEST, "Unsupported unexpected " +
                    "resource type");
        }
    }

    boolean isSplittable(ConfigResource.Type type, String key) {
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

    String getConfigValueDefault(ConfigResource.Type type, String key) {
        ConfigDef configDef = configDefs.get(type);
        if (configDef == null) {
            return null;
        }
        ConfigKey configKey = configDef.configKeys().get(key);
        if (configKey == null || !configKey.hasDefault()) {
            return null;
        }
        return ConfigDef.convertToString(configKey.defaultValue, configKey.type);
    }

    /**
     * Apply a configuration record to the in-memory state.
     *
     * @param record            The ConfigRecord.
     */
    public void replay(ConfigRecord record) {
        Type type = Type.forId(record.resourceType());
        ConfigResource configResource = new ConfigResource(type, record.resourceName());
        TimelineHashMap<String, String> configs = configData.get(configResource);
        if (configs == null) {
            configs = new TimelineHashMap<>(snapshotRegistry, 0);
            configData.put(configResource, configs);
        }
        if (record.value() == null) {
            configs.remove(record.name());
        } else {
            configs.put(record.name(), record.value());
        }
        if (configs.isEmpty()) {
            configData.remove(configResource);
        }
        log.info("{}: set configuration {} to {}", configResource, record.name(), record.value());
    }

    // VisibleForTesting
    Map<String, String> getConfigs(ConfigResource configResource) {
        Map<String, String> map = configData.get(configResource);
        if (map == null) {
            return Collections.emptyMap();
        } else {
            return Collections.unmodifiableMap(new HashMap<>(map));
        }
    }

    public Map<ConfigResource, ResultOrError<Map<String, String>>> describeConfigs(
            long lastCommittedOffset, Map<ConfigResource, Collection<String>> resources) {
        Map<ConfigResource, ResultOrError<Map<String, String>>> results = new HashMap<>();
        for (Entry<ConfigResource, Collection<String>> resourceEntry : resources.entrySet()) {
            ConfigResource resource = resourceEntry.getKey();
            ApiError error = checkConfigResource(resource);
            if (error.isFailure()) {
                results.put(resource, new ResultOrError<>(error));
                continue;
            }
            Map<String, String> foundConfigs = new HashMap<>();
            TimelineHashMap<String, String> configs =
                configData.get(resource, lastCommittedOffset);
            if (configs != null) {
                Collection<String> targetConfigs = resourceEntry.getValue();
                if (targetConfigs.isEmpty()) {
                    Iterator<Entry<String, String>> iter =
                        configs.entrySet(lastCommittedOffset).iterator();
                    while (iter.hasNext()) {
                        Entry<String, String> entry = iter.next();
                        foundConfigs.put(entry.getKey(), entry.getValue());
                    }
                } else {
                    for (String key : targetConfigs) {
                        String value = configs.get(key, lastCommittedOffset);
                        if (value != null) {
                            foundConfigs.put(key, value);
                        }
                    }
                }
            }
            results.put(resource, new ResultOrError<>(foundConfigs));
        }
        return results;
    }

    void deleteTopicConfigs(String name) {
        configData.remove(new ConfigResource(Type.TOPIC, name));
    }

    boolean uncleanLeaderElectionEnabledForTopic(String name) {
        return false; // TODO: support configuring unclean leader election.
    }

    class ConfigurationControlIterator implements Iterator<List<ApiMessageAndVersion>> {
        private final long epoch;
        private final Iterator<Entry<ConfigResource, TimelineHashMap<String, String>>> iterator;

        ConfigurationControlIterator(long epoch) {
            this.epoch = epoch;
            this.iterator = configData.entrySet(epoch).iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public List<ApiMessageAndVersion> next() {
            if (!hasNext()) throw new NoSuchElementException();
            List<ApiMessageAndVersion> records = new ArrayList<>();
            Entry<ConfigResource, TimelineHashMap<String, String>> entry = iterator.next();
            ConfigResource resource = entry.getKey();
            for (Entry<String, String> configEntry : entry.getValue().entrySet(epoch)) {
                records.add(new ApiMessageAndVersion(new ConfigRecord().
                    setResourceName(resource.name()).
                    setResourceType(resource.type().id()).
                    setName(configEntry.getKey()).
                    setValue(configEntry.getValue()), CONFIG_RECORD.highestSupportedVersion()));
            }
            return records;
        }
    }

    ConfigurationControlIterator iterator(long epoch) {
        return new ConfigurationControlIterator(epoch);
    }
}
