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
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.metadata.KafkaConfigSchema;
import org.apache.kafka.metadata.KafkaConfigSchema.OrderedConfigResolver;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.mutable.BoundedList;
import org.apache.kafka.server.policy.AlterConfigPolicy;
import org.apache.kafka.server.policy.AlterConfigPolicy.RequestMetadata;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.APPEND;
import static org.apache.kafka.common.protocol.Errors.INVALID_CONFIG;
import static org.apache.kafka.controller.QuorumController.MAX_RECORDS_PER_USER_OP;


public class ConfigurationControlManager {
    public static final ConfigResource DEFAULT_NODE = new ConfigResource(Type.BROKER, "");

    private final Logger log;
    private final SnapshotRegistry snapshotRegistry;
    private final KafkaConfigSchema configSchema;
    private final Consumer<ConfigResource> existenceChecker;
    private final Optional<AlterConfigPolicy> alterConfigPolicy;
    private final ConfigurationValidator validator;
    private final TimelineHashMap<ConfigResource, TimelineHashMap<String, String>> configData;
    private final Map<String, Object> staticConfig;
    private final ConfigResource currentController;
    private final MinIsrConfigUpdatePartitionHandler minIsrConfigUpdatePartitionHandler;

    static class Builder {
        private LogContext logContext = null;
        private SnapshotRegistry snapshotRegistry = null;
        private KafkaConfigSchema configSchema = KafkaConfigSchema.EMPTY;
        private Consumer<ConfigResource> existenceChecker = __ -> { };
        private Optional<AlterConfigPolicy> alterConfigPolicy = Optional.empty();
        private ConfigurationValidator validator = ConfigurationValidator.NO_OP;
        private Map<String, Object> staticConfig = Collections.emptyMap();
        private MinIsrConfigUpdatePartitionHandler minIsrConfigUpdatePartitionHandler;
        private int nodeId = 0;

        Builder setLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        Builder setSnapshotRegistry(SnapshotRegistry snapshotRegistry) {
            this.snapshotRegistry = snapshotRegistry;
            return this;
        }

        Builder setKafkaConfigSchema(KafkaConfigSchema configSchema) {
            this.configSchema = configSchema;
            return this;
        }

        Builder setExistenceChecker(Consumer<ConfigResource> existenceChecker) {
            this.existenceChecker = existenceChecker;
            return this;
        }

        Builder setAlterConfigPolicy(Optional<AlterConfigPolicy> alterConfigPolicy) {
            this.alterConfigPolicy = alterConfigPolicy;
            return this;
        }

        Builder setValidator(ConfigurationValidator validator) {
            this.validator = validator;
            return this;
        }

        Builder setStaticConfig(Map<String, Object> staticConfig) {
            this.staticConfig = staticConfig;
            return this;
        }

        Builder setNodeId(int nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        Builder setMinIsrConfigUpdatePartitionHandler(
            MinIsrConfigUpdatePartitionHandler minIsrConfigUpdatePartitionHandler
        ) {
            this.minIsrConfigUpdatePartitionHandler = minIsrConfigUpdatePartitionHandler;
            return this;
        }

        ConfigurationControlManager build() {
            if (logContext == null) logContext = new LogContext();
            if (snapshotRegistry == null) snapshotRegistry = new SnapshotRegistry(logContext);
            if (minIsrConfigUpdatePartitionHandler == null) {
                throw new RuntimeException("You must specify MinIsrConfigUpdatePartitionHandler");
            }
            return new ConfigurationControlManager(
                logContext,
                snapshotRegistry,
                configSchema,
                existenceChecker,
                alterConfigPolicy,
                validator,
                staticConfig,
                nodeId,
                minIsrConfigUpdatePartitionHandler);
        }
    }

    private ConfigurationControlManager(LogContext logContext,
            SnapshotRegistry snapshotRegistry,
            KafkaConfigSchema configSchema,
            Consumer<ConfigResource> existenceChecker,
            Optional<AlterConfigPolicy> alterConfigPolicy,
            ConfigurationValidator validator,
            Map<String, Object> staticConfig,
            int nodeId,
            MinIsrConfigUpdatePartitionHandler minIsrConfigUpdatePartitionHandler
    ) {
        this.log = logContext.logger(ConfigurationControlManager.class);
        this.snapshotRegistry = snapshotRegistry;
        this.configSchema = configSchema;
        this.existenceChecker = existenceChecker;
        this.alterConfigPolicy = alterConfigPolicy;
        this.validator = validator;
        this.configData = new TimelineHashMap<>(snapshotRegistry, 0);
        this.staticConfig = Collections.unmodifiableMap(new HashMap<>(staticConfig));
        this.currentController = new ConfigResource(Type.BROKER, Integer.toString(nodeId));
        this.minIsrConfigUpdatePartitionHandler = minIsrConfigUpdatePartitionHandler;
    }

    SnapshotRegistry snapshotRegistry() {
        return snapshotRegistry;
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
        Map<ConfigResource, Map<String, Entry<OpType, String>>> configChanges,
        boolean newlyCreatedResource
    ) {
        List<ApiMessageAndVersion> outputRecords =
                BoundedList.newArrayBacked(MAX_RECORDS_PER_USER_OP);
        Map<ConfigResource, ApiError> outputResults = new HashMap<>();
        for (Entry<ConfigResource, Map<String, Entry<OpType, String>>> resourceEntry :
                configChanges.entrySet()) {
            ApiError apiError = incrementalAlterConfigResource(resourceEntry.getKey(),
                resourceEntry.getValue(),
                newlyCreatedResource,
                outputRecords);
            outputResults.put(resourceEntry.getKey(), apiError);
        }
        return ControllerResult.atomicOf(outputRecords, outputResults);
    }

    ControllerResult<ApiError> incrementalAlterConfig(
        ConfigResource configResource,
        Map<String, Entry<OpType, String>> keyToOps,
        boolean newlyCreatedResource
    ) {
        List<ApiMessageAndVersion> outputRecords =
                BoundedList.newArrayBacked(MAX_RECORDS_PER_USER_OP);
        ApiError apiError = incrementalAlterConfigResource(configResource,
            keyToOps,
            newlyCreatedResource,
            outputRecords);
        return ControllerResult.atomicOf(outputRecords, apiError);
    }

    private ApiError incrementalAlterConfigResource(
        ConfigResource configResource,
        Map<String, Entry<OpType, String>> keysToOps,
        boolean newlyCreatedResource,
        List<ApiMessageAndVersion> outputRecords
    ) {
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
                    newValue = null;
                    break;
                case APPEND:
                case SUBTRACT:
                    if (!configSchema.isSplittable(configResource.type(), key)) {
                        return new ApiError(
                            INVALID_CONFIG, "Can't " + opType + " to " +
                            "key " + key + " because its type is not LIST.");
                    }
                    List<String> oldValueList = getParts(newValue, key, configResource);
                    if (opType == APPEND) {
                        for (String value : opValue.split(",")) {
                            if (!oldValueList.contains(value)) {
                                oldValueList.add(value);
                            }
                        }
                    } else {
                        for (String value : opValue.split(",")) {
                            oldValueList.remove(value);
                        }
                    }
                    newValue = String.join(",", oldValueList);
                    break;
            }
            if (!Objects.equals(currentValue, newValue) || configResource.type().equals(Type.BROKER)) {
                // KAFKA-14136 We need to generate records even if the value is unchanged to trigger reloads on the brokers
                newRecords.add(new ApiMessageAndVersion(new ConfigRecord().
                    setResourceType(configResource.type().id()).
                    setResourceName(configResource.name()).
                    setName(key).
                    setValue(newValue), (short) 0));
            }
        }
        ApiError error = validateAlterConfig(configResource, newRecords, Collections.emptyList(), newlyCreatedResource);
        if (error.isFailure()) {
            return error;
        }
        maybeTriggerPartitionUpdateOnMinIsrChange(newRecords);
        outputRecords.addAll(newRecords);
        return ApiError.NONE;
    }

    private ApiError validateAlterConfig(ConfigResource configResource,
                                         List<ApiMessageAndVersion> recordsExplicitlyAltered,
                                         List<ApiMessageAndVersion> recordsImplicitlyDeleted,
                                         boolean newlyCreatedResource) {
        Map<String, String> allConfigs = new HashMap<>();
        Map<String, String> alteredConfigsForAlterConfigPolicyCheck = new HashMap<>();
        TimelineHashMap<String, String> existingConfigs = configData.get(configResource);
        if (existingConfigs != null) allConfigs.putAll(existingConfigs);
        for (ApiMessageAndVersion newRecord : recordsExplicitlyAltered) {
            ConfigRecord configRecord = (ConfigRecord) newRecord.message();
            if (configRecord.value() == null) {
                allConfigs.remove(configRecord.name());
            } else {
                allConfigs.put(configRecord.name(), configRecord.value());
            }
            alteredConfigsForAlterConfigPolicyCheck.put(configRecord.name(), configRecord.value());
        }
        for (ApiMessageAndVersion recordImplicitlyDeleted : recordsImplicitlyDeleted) {
            ConfigRecord configRecord = (ConfigRecord) recordImplicitlyDeleted.message();
            allConfigs.remove(configRecord.name());
            // As per KAFKA-14195, do not include implicit deletions caused by using the legacy AlterConfigs API
            // in the list passed to the policy in order to maintain backwards compatibility
        }
        try {
            validator.validate(configResource, allConfigs);
            if (!newlyCreatedResource) {
                existenceChecker.accept(configResource);
            }
            if (alterConfigPolicy.isPresent()) {
                alterConfigPolicy.get().validate(new RequestMetadata(configResource, alteredConfigsForAlterConfigPolicyCheck));
            }
        } catch (ConfigException e) {
            return new ApiError(INVALID_CONFIG, e.getMessage());
        } catch (Throwable e) {
            // return the corresponding API error, but emit the stack trace first if it is an unknown server error
            ApiError apiError = ApiError.fromThrowable(e);
            if (apiError.error() == Errors.UNKNOWN_SERVER_ERROR) {
                log.error("Unknown server error validating Alter Configs", e);
            }
            return apiError;
        }
        return ApiError.NONE;
    }

    void maybeTriggerPartitionUpdateOnMinIsrChange(List<ApiMessageAndVersion> records) {
        List<ConfigRecord> minIsrRecords = new ArrayList<>();
        Map<String, String> topicToMinIsrValueMap = new HashMap<>();
        Map<String, String> configRemovedTopicMap = new HashMap<>();
        records.forEach(record -> {
            if (MetadataRecordType.fromId(record.message().apiKey()) == MetadataRecordType.CONFIG_RECORD) {
                ConfigRecord configRecord = (ConfigRecord) record.message();
                if (configRecord.name().equals(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG)) {
                    minIsrRecords.add(configRecord);
                    if (Type.forId(configRecord.resourceType()) == Type.TOPIC) {
                        if (configRecord.value() != null) topicToMinIsrValueMap.put(configRecord.resourceName(), configRecord.value());
                        else configRemovedTopicMap.put(configRecord.resourceName(), configRecord.value());
                    }
                }
            }
        });

        if (minIsrRecords.isEmpty()) return;
        if (topicToMinIsrValueMap.size() == minIsrRecords.size()) {
            // If all the min isr config updates are on the topic level, we can trigger a simpler update just on the
            // updated topics.
            records.addAll(minIsrConfigUpdatePartitionHandler.addRecordsForMinIsrUpdate(
                new ArrayList<>(topicToMinIsrValueMap.keySet()),
                topicName -> topicToMinIsrValueMap.get(topicName))
            );
            return;
        }

        // Because it may require multiple layer look up for the min ISR config value. Build a config data copy and apply
        // the config updates to it. Use this config copy for the min ISR look up.
        Map<ConfigResource, Map<String, String>> pendingConfigData = new HashMap<>();

        for (ConfigRecord record : minIsrRecords) {
            replayForPendingConfig(record, pendingConfigData);
        }

        ArrayList<String> topicList = new ArrayList<>();
        // If all the updates are on the Topic level, we can avoid perform a full scan of the partitions.
        if (configRemovedTopicMap.size() + topicToMinIsrValueMap.size() == minIsrRecords.size()) {
            topicList.addAll(configRemovedTopicMap.keySet());
            topicList.addAll(topicToMinIsrValueMap.keySet());
        }
        records.addAll(minIsrConfigUpdatePartitionHandler.addRecordsForMinIsrUpdate(
            topicList,
            topicName -> getTopicConfigWithPendingChange(topicName, TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, pendingConfigData))
        );
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
        Map<ConfigResource, Map<String, String>> newConfigs,
        boolean newlyCreatedResource
    ) {
        List<ApiMessageAndVersion> outputRecords =
                BoundedList.newArrayBacked(MAX_RECORDS_PER_USER_OP);
        Map<ConfigResource, ApiError> outputResults = new HashMap<>();
        for (Entry<ConfigResource, Map<String, String>> resourceEntry :
            newConfigs.entrySet()) {
            legacyAlterConfigResource(resourceEntry.getKey(),
                resourceEntry.getValue(),
                newlyCreatedResource,
                outputRecords,
                outputResults);
        }
        return ControllerResult.atomicOf(outputRecords, outputResults);
    }

    private void legacyAlterConfigResource(ConfigResource configResource,
                                           Map<String, String> newConfigs,
                                           boolean newlyCreatedResource,
                                           List<ApiMessageAndVersion> outputRecords,
                                           Map<ConfigResource, ApiError> outputResults) {
        List<ApiMessageAndVersion> recordsExplicitlyAltered = new ArrayList<>();
        Map<String, String> currentConfigs = configData.get(configResource);
        if (currentConfigs == null) {
            currentConfigs = Collections.emptyMap();
        }
        for (Entry<String, String> entry : newConfigs.entrySet()) {
            String key = entry.getKey();
            String newValue = entry.getValue();
            String currentValue = currentConfigs.get(key);
            if (!Objects.equals(currentValue, newValue) || configResource.type().equals(Type.BROKER)) {
                // KAFKA-14136 We need to generate records even if the value is unchanged to trigger reloads on the brokers
                recordsExplicitlyAltered.add(new ApiMessageAndVersion(new ConfigRecord().
                    setResourceType(configResource.type().id()).
                    setResourceName(configResource.name()).
                    setName(key).
                    setValue(newValue), (short) 0));
            }
        }
        List<ApiMessageAndVersion> recordsImplicitlyDeleted = new ArrayList<>();
        for (String key : currentConfigs.keySet()) {
            if (!newConfigs.containsKey(key)) {
                recordsImplicitlyDeleted.add(new ApiMessageAndVersion(new ConfigRecord().
                    setResourceType(configResource.type().id()).
                    setResourceName(configResource.name()).
                    setName(key).
                    setValue(null), (short) 0));
            }
        }
        ApiError error = validateAlterConfig(configResource, recordsExplicitlyAltered, recordsImplicitlyDeleted, newlyCreatedResource);
        if (error.isFailure()) {
            outputResults.put(configResource, error);
            return;
        }
        outputRecords.addAll(recordsExplicitlyAltered);
        outputRecords.addAll(recordsImplicitlyDeleted);
        maybeTriggerPartitionUpdateOnMinIsrChange(outputRecords);
        outputResults.put(configResource, ApiError.NONE);
    }

    private List<String> getParts(String value, String key, ConfigResource configResource) {
        if (value == null) {
            value = configSchema.getDefault(configResource.type(), key);
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
        if (configSchema.isSensitive(record)) {
            log.info("Replayed ConfigRecord for {} which set configuration {} to {}",
                    configResource, record.name(), Password.HIDDEN);
        } else {
            log.info("Replayed ConfigRecord for {} which set configuration {} to {}",
                    configResource, record.name(), record.value());
        }
    }

    /**
     * Apply a configuration record to the given config data. Note that, it will store null for the config to be removed.
     *
     * @param record                 The ConfigRecord.
     * @param localConfigData        The config data is going to be updated.
     */
    public void replayForPendingConfig(
        ConfigRecord record,
        Map<ConfigResource, Map<String, String>> localConfigData
    ) {
        Type type = Type.forId(record.resourceType());
        ConfigResource configResource = new ConfigResource(type, record.resourceName());
        Map<String, String> configs = localConfigData.get(configResource);
        if (configs == null) {
            configs = new HashMap<>();
            localConfigData.put(configResource, configs);
        }

        // If the record removes a config, we keep an empty value here to indicate the value is removed.
        if (record.value() == null) {
            configs.put(record.name(), null);
        } else {
            configs.put(record.name(), record.value());
        }
        if (configs.isEmpty()) {
            localConfigData.remove(configResource);
        }
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

    /**
     * Get the config value for the give topic and give config key.
     * If the config value is not found, return null.
     *
     * @param topicName            The topic name for the config.
     * @param configKey            The key for the config.
     */
    String getTopicConfig(String topicName, String configKey) throws NoSuchElementException {
        Map<String, String> map = configData.get(new ConfigResource(Type.TOPIC, topicName));
        if (map == null || !map.containsKey(configKey)) {
            Map<String, ConfigEntry> effectiveConfigMap = computeEffectiveTopicConfigs(Collections.emptyMap());
            if (!effectiveConfigMap.containsKey(configKey)) {
                return null;
            }
            return effectiveConfigMap.get(configKey).value();
        }
        return map.get(configKey);
    }

    /**
     * Get the config value for the give topic and give config key. Also, it will search the configs in the pending
     * config data first.
     * If the config value is not found, return null.
     *
     * @param topicName            The topic name for the config.
     * @param configKey            The key for the config.
     * @param pendingConfigData    The configs which is going to be applied. It should have the higher priority than
     *                             the current configs.
     */
    String getTopicConfigWithPendingChange(
        String topicName,
        String configKey,
        Map<ConfigResource, Map<String, String>> pendingConfigData
    ) throws NoSuchElementException {
        Map<String, String> pendingTopicConfigs =
            pendingConfigData.getOrDefault(new ConfigResource(Type.TOPIC, topicName), Collections.emptyMap());
        Map<String, String> currentTopicConfigs = configData.get(new ConfigResource(Type.TOPIC, topicName));
        if (currentTopicConfigs == null) currentTopicConfigs = Collections.emptyMap();
        OrderedConfigResolver configResolver = new OrderedConfigResolver(Arrays.asList(pendingTopicConfigs, currentTopicConfigs));

        if (!configResolver.containsKey(configKey)) {
            Map<String, ConfigEntry> effectiveConfigMap = computeEffectiveTopicConfigsWithPendingChange(pendingConfigData);
            if (!effectiveConfigMap.containsKey(configKey)) {
                return null;
            }
            return effectiveConfigMap.get(configKey).value();
        }
        return (String) configResolver.get(configKey);
    }

    public Map<ConfigResource, ResultOrError<Map<String, String>>> describeConfigs(
            long lastCommittedOffset, Map<ConfigResource, Collection<String>> resources) {
        Map<ConfigResource, ResultOrError<Map<String, String>>> results = new HashMap<>();
        for (Entry<ConfigResource, Collection<String>> resourceEntry : resources.entrySet()) {
            ConfigResource resource = resourceEntry.getKey();
            try {
                validator.validate(resource);
            } catch (Throwable e) {
                results.put(resource, new ResultOrError<>(ApiError.fromThrowable(e)));
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

    Map<String, ConfigEntry> computeEffectiveTopicConfigsWithPendingChange(
        Map<ConfigResource, Map<String, String>> pendingConfigData
    ) {
        Map<String, String> pendingClusterConfig =
            pendingConfigData.containsKey(DEFAULT_NODE) ? pendingConfigData.get(DEFAULT_NODE) : Collections.emptyMap();
        Map<String, String> pendingControllerConfig =
            pendingConfigData.containsKey(currentController) ? pendingConfigData.get(currentController) : Collections.emptyMap();
        return configSchema.resolveEffectiveTopicConfigs(
            new OrderedConfigResolver(staticConfig),
            new OrderedConfigResolver(Arrays.asList(pendingClusterConfig, clusterConfig())),
            new OrderedConfigResolver(Arrays.asList(pendingControllerConfig, currentControllerConfig())),
            new OrderedConfigResolver(Collections.emptyMap()));
    }

    Map<String, ConfigEntry> computeEffectiveTopicConfigs(Map<String, String> creationConfigs) {
        return configSchema.resolveEffectiveTopicConfigs(
            new OrderedConfigResolver(staticConfig),
            new OrderedConfigResolver(clusterConfig()),
            new OrderedConfigResolver(currentControllerConfig()),
            new OrderedConfigResolver(creationConfigs));
    }

    Map<String, String> clusterConfig() {
        Map<String, String> result = configData.get(DEFAULT_NODE);
        return (result == null) ? Collections.emptyMap() : result;
    }

    Map<String, String> currentControllerConfig() {
        Map<String, String> result = configData.get(currentController);
        return (result == null) ? Collections.emptyMap() : result;
    }

    @FunctionalInterface
    interface MinIsrConfigUpdatePartitionHandler {
        List<ApiMessageAndVersion> addRecordsForMinIsrUpdate(List<String> topicNames, Function<String, String> getTopicMinIsrConfig);
    }
}
