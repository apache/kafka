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
package org.apache.kafka.streams.processor.internals.namedtopology;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.internals.StreamThread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import java.util.function.Supplier;

/**
 * Streams configs that apply at the topology level. The values in the {@link StreamsConfig} parameter of the
 * {@link org.apache.kafka.streams.KafkaStreams} or {@link KafkaStreamsNamedTopologyWrapper} constructors will
 * determine the defaults, which can then be overridden for specific topologies by passing them in when creating the
 * topology via the {@link org.apache.kafka.streams.StreamsBuilder#build(Properties)} or
 * {@link NamedTopologyStreamsBuilder#buildNamedTopology(Properties)} methods.
 */
public class TopologyConfig extends AbstractConfig {
    private final Logger log = LoggerFactory.getLogger(TopologyConfig.class);

    public final String topologyName;
    public final boolean eosEnabled;

    final long maxTaskIdleMs;
    final long taskTimeoutMs;
    final int maxBufferedSize;
    final Supplier<TimestampExtractor> timestampExtractorSupplier;
    final Supplier<DeserializationExceptionHandler> deserializationExceptionHandlerSupplier;

    public TopologyConfig(final String topologyName, final StreamsConfig globalAppConfigs, final Properties topologyOverrides) {
        super(new ConfigDef(), topologyOverrides, false);

        this.topologyName = topologyName;
        this.eosEnabled = StreamThread.eosEnabled(globalAppConfigs);
        
        if (isTopologyOverride(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, topologyOverrides)) {
            maxTaskIdleMs = getLong(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG);
            log.info("Topology {} is overriding {} to {}", topologyName, StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, maxTaskIdleMs);
        } else {
            maxTaskIdleMs = globalAppConfigs.getLong(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG);
        }

        if (isTopologyOverride(StreamsConfig.TASK_TIMEOUT_MS_CONFIG, topologyOverrides)) {
            taskTimeoutMs = getLong(StreamsConfig.TASK_TIMEOUT_MS_CONFIG);
            log.info("Topology {} is overriding {} to {}", topologyName, StreamsConfig.TASK_TIMEOUT_MS_CONFIG, taskTimeoutMs);
        } else {
            taskTimeoutMs = globalAppConfigs.getLong(StreamsConfig.TASK_TIMEOUT_MS_CONFIG);
        }

        if (isTopologyOverride(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, topologyOverrides)) {
            maxBufferedSize = getInt(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG);
            log.info("Topology {} is overriding {} to {}", topologyName, StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, maxBufferedSize);
        } else {
            maxBufferedSize = globalAppConfigs.getInt(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG);
        }

        if (isTopologyOverride(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, topologyOverrides)) {
            timestampExtractorSupplier = () -> getConfiguredInstance(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TimestampExtractor.class);
            log.info("Topology {} is overriding {} to {}", topologyName, StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, getClass(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG));
        } else {
            timestampExtractorSupplier = () -> globalAppConfigs.getConfiguredInstance(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TimestampExtractor.class);
        }

        if (isTopologyOverride(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, topologyOverrides)) {
            deserializationExceptionHandlerSupplier = () -> getConfiguredInstance(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, DeserializationExceptionHandler.class);
            log.info("Topology {} is overriding {} to {}", topologyName, StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, getClass(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG));
        } else {
            deserializationExceptionHandlerSupplier = () -> globalAppConfigs.getConfiguredInstance(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, DeserializationExceptionHandler.class);

        }
    }

    /**
     * @return true if there is an override for this config in the properties of this NamedTopology. Applications that
     *         don't use named topologies will just refer to the global defaults regardless of the topology properties
     */
    private boolean isTopologyOverride(final String config, final Properties topologyOverrides) {
        return topologyName != null && topologyOverrides.containsKey(config);
    }

    public TaskConfig getTaskConfig() {
        return new TaskConfig(
            maxTaskIdleMs,
            taskTimeoutMs,
            maxBufferedSize,
            timestampExtractorSupplier.get(),
            deserializationExceptionHandlerSupplier.get(),
            eosEnabled
        );
    }

    public static class TaskConfig {
        public final long maxTaskIdleMs;
        public final long taskTimeoutMs;
        public final int maxBufferedSize;
        public final TimestampExtractor timestampExtractor;
        public final DeserializationExceptionHandler deserializationExceptionHandler;
        public final boolean eosEnabled;

        private TaskConfig(final long maxTaskIdleMs,
                           final long taskTimeoutMs,
                           final int maxBufferedSize,
                           final TimestampExtractor timestampExtractor,
                           final DeserializationExceptionHandler deserializationExceptionHandler,
                           final boolean eosEnabled) {
            this.maxTaskIdleMs = maxTaskIdleMs;
            this.taskTimeoutMs = taskTimeoutMs;
            this.maxBufferedSize = maxBufferedSize;
            this.timestampExtractor = timestampExtractor;
            this.deserializationExceptionHandler = deserializationExceptionHandler;
            this.eosEnabled = eosEnabled;
        }
    }
}
