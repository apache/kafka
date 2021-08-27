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

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.internals.StreamThread;

import java.util.Properties;
import java.util.function.Function;

/**
 * Streams configs that apply at the topology level. The values in the {@link StreamsConfig} parameter of the
 * {@link org.apache.kafka.streams.KafkaStreams} or {@link KafkaStreamsNamedTopologyWrapper} constructors will
 * determine the defaults, which can then be overridden for specific topologies by passing them in when creating the
 * topology via the {@link org.apache.kafka.streams.StreamsBuilder#build(Properties)} or
 * {@link NamedTopologyStreamsBuilder#buildNamedTopology(Properties)} methods.
 */
public class TopologyConfig extends StreamsConfig {
    final StreamsConfig applicationConfig;
    final Properties topologyOverrides;

    public TopologyConfig(final StreamsConfig applicationConfig, final Properties topologyProps) {
        super(topologyProps, containsOverrides(topologyProps)); // skip logging if there aren't any topology overrides
        this.applicationConfig = applicationConfig;
        this.topologyOverrides = topologyProps;
    }

    public TaskConfig getTaskConfig() {
        return new TaskConfig(
            getConfig(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, config -> config.getLong(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG)),
            getConfig(StreamsConfig.TASK_TIMEOUT_MS_CONFIG, config -> config.getLong(StreamsConfig.TASK_TIMEOUT_MS_CONFIG)),
            getConfig(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, config -> config.getInt(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG)),
            getConfig(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, StreamsConfig::defaultTimestampExtractor),
            getConfig(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, StreamsConfig::defaultDeserializationExceptionHandler),
            StreamThread.eosEnabled(applicationConfig)
        );
    }

    /**
     * @return the value of this config passed in to the topology if it exists, otherwise default application-wide config
     */
    <V> V getConfig(final String config, final Function<StreamsConfig, V> configGetter) {
        return topologyOverrides.containsKey(config) ?
            configGetter.apply(this) :
            configGetter.apply(applicationConfig);
    }

    private static boolean containsOverrides(final Properties props) {
        return props.containsKey(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG) ||
               props.containsKey(StreamsConfig.TASK_TIMEOUT_MS_CONFIG) ||
               props.containsKey(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG) ||
               props.containsKey(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG) ||
               props.containsKey(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG);
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
