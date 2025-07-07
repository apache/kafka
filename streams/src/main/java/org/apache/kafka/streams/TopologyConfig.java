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
package org.apache.kafka.streams;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ProcessingExceptionHandler;
import org.apache.kafka.streams.internals.StreamsConfigUtils;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.internals.MaterializedInternal;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.internals.NoOpProcessorWrapper;
import org.apache.kafka.streams.processor.internals.namedtopology.KafkaStreamsNamedTopologyWrapper;
import org.apache.kafka.streams.state.DslStoreSuppliers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Properties;
import java.util.function.Supplier;

import static org.apache.kafka.common.config.ConfigDef.ValidString.in;
import static org.apache.kafka.common.utils.Utils.mkObjectProperties;
import static org.apache.kafka.streams.StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_DOC;
import static org.apache.kafka.streams.StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.CACHE_MAX_BYTES_BUFFERING_DOC;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_DOC;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_DSL_STORE_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_DSL_STORE_DOC;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_DOC;
import static org.apache.kafka.streams.StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_DOC;
import static org.apache.kafka.streams.StreamsConfig.DSL_STORE_SUPPLIERS_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DSL_STORE_SUPPLIERS_CLASS_DEFAULT;
import static org.apache.kafka.streams.StreamsConfig.DSL_STORE_SUPPLIERS_CLASS_DOC;
import static org.apache.kafka.streams.StreamsConfig.ENSURE_EXPLICIT_INTERNAL_RESOURCE_NAMING_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.ENSURE_EXPLICIT_INTERNAL_RESOURCE_NAMING_DOC;
import static org.apache.kafka.streams.StreamsConfig.IN_MEMORY;
import static org.apache.kafka.streams.StreamsConfig.MAX_TASK_IDLE_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.MAX_TASK_IDLE_MS_DOC;
import static org.apache.kafka.streams.StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.PROCESSOR_WRAPPER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.PROCESSOR_WRAPPER_CLASS_DOC;
import static org.apache.kafka.streams.StreamsConfig.ROCKS_DB;
import static org.apache.kafka.streams.StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATESTORE_CACHE_MAX_BYTES_DOC;
import static org.apache.kafka.streams.StreamsConfig.TASK_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.TASK_TIMEOUT_MS_DOC;
import static org.apache.kafka.streams.internals.StreamsConfigUtils.totalCacheSize;

/**
 * Streams configs that apply at the topology level. The values in the {@link StreamsConfig} parameter of the
 * {@link org.apache.kafka.streams.KafkaStreams} constructor or the {@link KafkaStreamsNamedTopologyWrapper} constructor (deprecated)
 * will determine the defaults, which can then be overridden for specific topologies by passing them in when creating the
 * topology builders via the {@link StreamsBuilder#StreamsBuilder(TopologyConfig)} constructor for DSL applications,
 * or the {@link Topology#Topology(TopologyConfig)} for PAPI applications.
 * <p>
 * Note that some configs, such as the {@code processor.wrapper.class} config, can only take effect while the
 * topology is being built, which means they have to be passed in as a TopologyConfig to the
 * {@link Topology#Topology(TopologyConfig)} constructor (PAPI) or the
 * {@link StreamsBuilder#StreamsBuilder(TopologyConfig)} constructor (DSL).
 * If they are only set in the configs passed in to the KafkaStreams constructor, it will be too late for them
 * to be applied and the config will be ignored.
 */
@SuppressWarnings("deprecation")
public final class TopologyConfig extends AbstractConfig {
    private static final ConfigDef CONFIG;
    static {
        CONFIG = new ConfigDef()
            .define(PROCESSOR_WRAPPER_CLASS_CONFIG,
                    Type.CLASS,
                    NoOpProcessorWrapper.class.getName(),
                    Importance.LOW,
                    PROCESSOR_WRAPPER_CLASS_DOC)
            .define(BUFFERED_RECORDS_PER_PARTITION_CONFIG,
                Type.INT,
                null,
                Importance.LOW,
                BUFFERED_RECORDS_PER_PARTITION_DOC)
            .define(CACHE_MAX_BYTES_BUFFERING_CONFIG,
                    Type.LONG,
                    null,
                    Importance.MEDIUM,
                    CACHE_MAX_BYTES_BUFFERING_DOC)
            .define(STATESTORE_CACHE_MAX_BYTES_CONFIG,
                Type.LONG,
                null,
                Importance.MEDIUM,
                STATESTORE_CACHE_MAX_BYTES_DOC)
            .define(DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                Type.CLASS,
                null,
                Importance.MEDIUM,
                DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_DOC)
            .define(DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
                Type.CLASS,
                null,
                Importance.MEDIUM,
                DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_DOC)
            .define(DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                Type.CLASS,
                null,
                Importance.MEDIUM,
                DESERIALIZATION_EXCEPTION_HANDLER_CLASS_DOC)
            .define(MAX_TASK_IDLE_MS_CONFIG,
                Type.LONG,
                null,
                Importance.MEDIUM,
                MAX_TASK_IDLE_MS_DOC)
            .define(TASK_TIMEOUT_MS_CONFIG,
                Type.LONG,
                null,
                Importance.MEDIUM,
                TASK_TIMEOUT_MS_DOC)
            .define(DEFAULT_DSL_STORE_CONFIG,
                Type.STRING,
                ROCKS_DB,
                in(ROCKS_DB, IN_MEMORY),
                Importance.LOW,
                DEFAULT_DSL_STORE_DOC)
            .define(DSL_STORE_SUPPLIERS_CLASS_CONFIG,
                Type.CLASS,
                DSL_STORE_SUPPLIERS_CLASS_DEFAULT,
                Importance.LOW,
                DSL_STORE_SUPPLIERS_CLASS_DOC)
            .define(ENSURE_EXPLICIT_INTERNAL_RESOURCE_NAMING_CONFIG,
                Type.BOOLEAN,
                false,
                Importance.HIGH,
                ENSURE_EXPLICIT_INTERNAL_RESOURCE_NAMING_DOC);
    }
    private static final Logger log = LoggerFactory.getLogger(TopologyConfig.class);

    private final StreamsConfig globalAppConfigs;

    public final String topologyName;
    public final boolean eosEnabled;

    public final StreamsConfig applicationConfigs;
    public final Properties topologyOverrides;

    public final int maxBufferedSize;
    public final long cacheSize;
    public final long maxTaskIdleMs;
    public final long taskTimeoutMs;
    public final String storeType;
    public final Class<?> dslStoreSuppliers;
    public final Supplier<TimestampExtractor> timestampExtractorSupplier;
    public final Supplier<DeserializationExceptionHandler> deserializationExceptionHandlerSupplier;
    public final Supplier<ProcessingExceptionHandler> processingExceptionHandlerSupplier;

    public final boolean ensureExplicitInternalResourceNaming;

    public TopologyConfig(final StreamsConfig configs) {
        this(null, configs, mkObjectProperties(configs.originals()));
    }

    public TopologyConfig(final String topologyName, final StreamsConfig globalAppConfigs, final Properties topologyOverrides) {
        super(CONFIG, topologyOverrides, false);

        this.globalAppConfigs = globalAppConfigs;
        this.topologyName = topologyName;
        this.eosEnabled = StreamsConfigUtils.eosEnabled(globalAppConfigs);

        this.applicationConfigs = globalAppConfigs;
        this.topologyOverrides = topologyOverrides;
        this.processingExceptionHandlerSupplier = () -> globalAppConfigs.getConfiguredInstance(PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG, ProcessingExceptionHandler.class);

        if (isTopologyOverride(BUFFERED_RECORDS_PER_PARTITION_CONFIG, topologyOverrides)) {
            maxBufferedSize = getInt(BUFFERED_RECORDS_PER_PARTITION_CONFIG);
            log.info("Topology {} is overriding {} to {}", topologyName, BUFFERED_RECORDS_PER_PARTITION_CONFIG, maxBufferedSize);
        } else {
            maxBufferedSize = globalAppConfigs.getInt(BUFFERED_RECORDS_PER_PARTITION_CONFIG);
        }

        final boolean stateStoreCacheMaxBytesOverridden = isTopologyOverride(STATESTORE_CACHE_MAX_BYTES_CONFIG, topologyOverrides);
        final boolean cacheMaxBytesBufferingOverridden = isTopologyOverride(CACHE_MAX_BYTES_BUFFERING_CONFIG, topologyOverrides);

        if (!stateStoreCacheMaxBytesOverridden && !cacheMaxBytesBufferingOverridden) {
            cacheSize = totalCacheSize(globalAppConfigs);
        } else {
            if (stateStoreCacheMaxBytesOverridden && cacheMaxBytesBufferingOverridden) {
                cacheSize = getLong(STATESTORE_CACHE_MAX_BYTES_CONFIG);
                log.info("Topology {} is using both deprecated config {} and new config {}, hence {} is ignored and the new config {} (value {}) is used",
                        topologyName,
                        CACHE_MAX_BYTES_BUFFERING_CONFIG,
                        STATESTORE_CACHE_MAX_BYTES_CONFIG,
                        CACHE_MAX_BYTES_BUFFERING_CONFIG,
                        STATESTORE_CACHE_MAX_BYTES_CONFIG,
                        cacheSize);
            } else if (cacheMaxBytesBufferingOverridden) {
                cacheSize = getLong(CACHE_MAX_BYTES_BUFFERING_CONFIG);
                log.info("Topology {} is using only deprecated config {}, and will be used to set cache size to {}; " +
                                "we suggest setting the new config {} instead as deprecated {} would be removed in the future.",
                        topologyName,
                        CACHE_MAX_BYTES_BUFFERING_CONFIG,
                        cacheSize,
                        STATESTORE_CACHE_MAX_BYTES_CONFIG,
                        CACHE_MAX_BYTES_BUFFERING_CONFIG);
            } else {
                cacheSize = getLong(STATESTORE_CACHE_MAX_BYTES_CONFIG);
            }

            if (cacheSize != 0) {
                log.warn("Topology {} is overriding cache size to {} but this will not have any effect as the "
                                + "topology-level cache size config only controls whether record buffering is enabled "
                                + "or disabled, thus the only valid override value is 0",
                        topologyName, cacheSize);
            } else {
                log.info("Topology {} is overriding cache size to {}, record buffering will be disabled",
                        topologyName, cacheSize);
            }
        }

        if (isTopologyOverride(MAX_TASK_IDLE_MS_CONFIG, topologyOverrides)) {
            maxTaskIdleMs = getLong(MAX_TASK_IDLE_MS_CONFIG);
            log.info("Topology {} is overriding {} to {}", topologyName, MAX_TASK_IDLE_MS_CONFIG, maxTaskIdleMs);
        } else {
            maxTaskIdleMs = globalAppConfigs.getLong(MAX_TASK_IDLE_MS_CONFIG);
        }

        if (isTopologyOverride(TASK_TIMEOUT_MS_CONFIG, topologyOverrides)) {
            taskTimeoutMs = getLong(TASK_TIMEOUT_MS_CONFIG);
            log.info("Topology {} is overriding {} to {}", topologyName, TASK_TIMEOUT_MS_CONFIG, taskTimeoutMs);
        } else {
            taskTimeoutMs = globalAppConfigs.getLong(TASK_TIMEOUT_MS_CONFIG);
        }

        if (isTopologyOverride(DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, topologyOverrides)) {
            timestampExtractorSupplier = () -> getConfiguredInstance(DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TimestampExtractor.class);
            log.info("Topology {} is overriding {} to {}", topologyName, DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, getClass(DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG));
        } else {
            timestampExtractorSupplier = () -> globalAppConfigs.getConfiguredInstance(DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TimestampExtractor.class);
        }


        final String deserializationExceptionHandlerKey = (globalAppConfigs.originals().containsKey(DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG)
            || originals().containsKey(DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG)) ?
            DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG :
            DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG;

        if (isTopologyOverride(deserializationExceptionHandlerKey, topologyOverrides)) {
            deserializationExceptionHandlerSupplier = () -> getConfiguredInstance(deserializationExceptionHandlerKey, DeserializationExceptionHandler.class);
            log.info("Topology {} is overriding {} to {}", topologyName, deserializationExceptionHandlerKey, getClass(deserializationExceptionHandlerKey));
        } else {
            deserializationExceptionHandlerSupplier = () -> globalAppConfigs.getConfiguredInstance(deserializationExceptionHandlerKey, DeserializationExceptionHandler.class);
        }

        if (isTopologyOverride(DEFAULT_DSL_STORE_CONFIG, topologyOverrides)) {
            storeType = getString(DEFAULT_DSL_STORE_CONFIG);
            log.info("Topology {} is overriding {} to {}", topologyName, DEFAULT_DSL_STORE_CONFIG, storeType);
        } else {
            storeType = globalAppConfigs.getString(DEFAULT_DSL_STORE_CONFIG);
        }

        if (isTopologyOverride(DSL_STORE_SUPPLIERS_CLASS_CONFIG, topologyOverrides)) {
            dslStoreSuppliers = getClass(DSL_STORE_SUPPLIERS_CLASS_CONFIG);
            log.info("Topology {} is overriding {} to {}", topologyName, DSL_STORE_SUPPLIERS_CLASS_CONFIG, dslStoreSuppliers);
        } else {
            dslStoreSuppliers = globalAppConfigs.getClass(DSL_STORE_SUPPLIERS_CLASS_CONFIG);
        }

        ensureExplicitInternalResourceNaming = globalAppConfigs.getBoolean(ENSURE_EXPLICIT_INTERNAL_RESOURCE_NAMING_CONFIG);
    }

    @Deprecated
    public Materialized.StoreType parseStoreType() {
        return MaterializedInternal.parse(storeType);
    }

    /**
     * @return the DslStoreSuppliers if the value was explicitly configured (either by
     *         {@link StreamsConfig#DEFAULT_DSL_STORE} or {@link StreamsConfig#DSL_STORE_SUPPLIERS_CLASS_CONFIG})
     */
    public Optional<DslStoreSuppliers> resolveDslStoreSuppliers() {
        if (isTopologyOverride(DSL_STORE_SUPPLIERS_CLASS_CONFIG, topologyOverrides) || globalAppConfigs.originals().containsKey(DSL_STORE_SUPPLIERS_CLASS_CONFIG)) {
            return Optional.of(Utils.newInstance(dslStoreSuppliers, DslStoreSuppliers.class));
        } else if (isTopologyOverride(DEFAULT_DSL_STORE_CONFIG, topologyOverrides) || globalAppConfigs.originals().containsKey(DEFAULT_DSL_STORE_CONFIG)) {
            return Optional.of(MaterializedInternal.parse(storeType));
        } else {
            return Optional.empty();
        }
    }

    public boolean isNamedTopology() {
        return topologyName != null;
    }

    /**
     * @return true if there is an override for this config in the properties of this NamedTopology. Applications that
     *         don't use named topologies will just refer to the global defaults regardless of the topology properties
     */
    private boolean isTopologyOverride(final String config, final Properties topologyOverrides) {
        // TODO KAFKA-13283: eventually we should always have the topology props override the global ones regardless
        // of whether it's a named topology or not
        return topologyName != null && topologyOverrides.containsKey(config);
    }

    public TaskConfig getTaskConfig() {
        return new TaskConfig(
            maxTaskIdleMs,
            taskTimeoutMs,
            maxBufferedSize,
            timestampExtractorSupplier.get(),
            deserializationExceptionHandlerSupplier.get(),
            processingExceptionHandlerSupplier.get(),
            eosEnabled
        );
    }

    public static class TaskConfig {
        public final long maxTaskIdleMs;
        public final long taskTimeoutMs;
        public final int maxBufferedSize;
        public final TimestampExtractor timestampExtractor;
        public final DeserializationExceptionHandler deserializationExceptionHandler;
        public final ProcessingExceptionHandler processingExceptionHandler;
        public final boolean eosEnabled;

        private TaskConfig(final long maxTaskIdleMs,
                           final long taskTimeoutMs,
                           final int maxBufferedSize,
                           final TimestampExtractor timestampExtractor,
                           final DeserializationExceptionHandler deserializationExceptionHandler,
                           final ProcessingExceptionHandler processingExceptionHandler,
                           final boolean eosEnabled) {
            this.maxTaskIdleMs = maxTaskIdleMs;
            this.taskTimeoutMs = taskTimeoutMs;
            this.maxBufferedSize = maxBufferedSize;
            this.timestampExtractor = timestampExtractor;
            this.deserializationExceptionHandler = deserializationExceptionHandler;
            this.processingExceptionHandler = processingExceptionHandler;
            this.eosEnabled = eosEnabled;
        }
    }
}
