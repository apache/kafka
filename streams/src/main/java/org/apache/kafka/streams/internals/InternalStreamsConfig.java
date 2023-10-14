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
package org.apache.kafka.streams.internals;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class InternalStreamsConfig extends StreamsConfig {
    private static final Logger log = LoggerFactory.getLogger(StreamsConfig.class);

    // This is settable in the main Streams config, but it's a private API for now
    public static final String TASK_ASSIGNOR_CLASS = "__internal.task.assignor.class__";
    // These are not settable in the main Streams config; they are set by the StreamThread to pass internal
    // state into the assignor.
    public static final String REFERENCE_CONTAINER_PARTITION_ASSIGNOR = "__reference.container.instance__";
    // This is settable in the main Streams config, but it's a private API for testing
    public static final String ASSIGNMENT_LISTENER = "__assignment.listener__";
    // Private API used to control the emit latency for left/outer join results (https://issues.apache.org/jira/browse/KAFKA-10847)
    public static final String EMIT_INTERVAL_MS_KSTREAMS_OUTER_JOIN_SPURIOUS_RESULTS_FIX = "__emit.interval.ms.kstreams.outer.join.spurious.results.fix__";
    // Private API used to control the emit latency for windowed aggregation results for ON_WINDOW_CLOSE emit strategy
    public static final String EMIT_INTERVAL_MS_KSTREAMS_WINDOWED_AGGREGATION = "__emit.interval.ms.kstreams.windowed.aggregation__";
    // Private API used to control the usage of consistency offset vectors
    public static final String IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED = "__iq.consistency.offset.vector.enabled__";
    // Private API used to control the prefix of the auto created topics
    public static final String TOPIC_PREFIX_ALTERNATIVE = "__internal.override.topic.prefix__";
    // Private API to enable the state updater (i.e. state updating on a dedicated thread)
    public static final String STATE_UPDATER_ENABLED = "__state.updater.enabled__";



    public InternalStreamsConfig(final StreamsConfig streamsConfig) {
        super(streamsConfig.originals(), false);
    }

    public InternalStreamsConfig(final Map<?, ?> configs) {
        super(configs, false);
    }



    /**
     * Return a copy of the config definition.
     *
     * @return a copy of the config definition
     */
    @SuppressWarnings({"deprecation", "unused"})
    public static ConfigDef configDef() {
        return new ConfigDef(CONFIG);
    }

    public static boolean stateUpdaterEnabled(final Map<String, Object> configs) {
        return getBoolean(configs, STATE_UPDATER_ENABLED, true);
    }

    public static boolean getBoolean(final Map<String, Object> configs, final String key, final boolean defaultValue) {
        final Object value = configs.getOrDefault(key, defaultValue);
        if (value instanceof Boolean) {
            return (boolean) value;
        } else if (value instanceof String) {
            return Boolean.parseBoolean((String) value);
        } else {
            log.warn("Invalid value (" + value + ") on internal configuration '" + key + "'. Please specify a true/false value.");
            return defaultValue;
        }
    }

    public static long getLong(final Map<String, Object> configs, final String key, final long defaultValue) {
        final Object value = configs.getOrDefault(key, defaultValue);
        if (value instanceof Number) {
            return ((Number) value).longValue();
        } else if (value instanceof String) {
            return Long.parseLong((String) value);
        } else {
            log.warn("Invalid value (" + value + ") on internal configuration '" + key + "'. Please specify a numeric value.");
            return defaultValue;
        }
    }

    public static String getString(final Map<String, Object> configs, final String key, final String defaultValue) {
        final Object value = configs.getOrDefault(key, defaultValue);
        if (value instanceof String) {
            return (String) value;
        } else {
            log.warn("Invalid value (" + value + ") on internal configuration '" + key + "'. Please specify a String value.");
            return defaultValue;
        }
    }

    @SuppressWarnings("deprecation")
    public Map<String, Object> mainConsumerConfigs(final String groupId, final String clientId, final int threadIdx) {
        return super.getMainConsumerConfigs(groupId, clientId, threadIdx);
    }

    @SuppressWarnings("deprecation")
    public Map<String, Object> restoreConsumerConfigs(final String clientId) {
        return super.getRestoreConsumerConfigs(clientId);
    }

    @SuppressWarnings("deprecation")
    public Map<String, Object> globalConsumerConfigs(final String clientId) {
        return super.getGlobalConsumerConfigs(clientId);
    }

    @SuppressWarnings("deprecation")
    public Map<String, Object> producerConfigs(final String clientId) {
        return super.getProducerConfigs(clientId);
    }

    @SuppressWarnings("deprecation")
    public Map<String, Object> adminConfigs(final String clientId) {
        return super.getAdminConfigs(clientId);
    }

    @SuppressWarnings("deprecation")
    public Map<String, String> clientTags() {
        return super.getClientTags();
    }

    @SuppressWarnings("deprecation")
    public Serde<?> defaultKeySerde() {
        return super.defaultKeySerde();
    }

    @SuppressWarnings("deprecation")
    public Serde<?> defaultValueSerde() {
        return super.defaultValueSerde();
    }

    @SuppressWarnings("deprecation")
    public TimestampExtractor defaultTimestampExtractor() {
        return super.defaultTimestampExtractor();
    }

    @SuppressWarnings("deprecation")
    public DeserializationExceptionHandler defaultDeserializationExceptionHandler() {
        return super.defaultDeserializationExceptionHandler();
    }

    @SuppressWarnings("deprecation")
    public ProductionExceptionHandler defaultProductionExceptionHandler() {
        return super.defaultProductionExceptionHandler();
    }

    @SuppressWarnings("deprecation")
    public KafkaClientSupplier kafkaClientSupplier() {
        return super.getKafkaClientSupplier();
    }

    @Override
    protected Map<String, Object> postProcessParsedConfig(final Map<String, Object> parsedValues) {
        final Map<String, Object> configUpdates =
                CommonClientConfigs.postProcessReconnectBackoffConfigs(this, parsedValues);

        if (StreamsConfigUtils.eosEnabled(this) && !originals().containsKey(COMMIT_INTERVAL_MS_CONFIG)) {
            log.debug("Using {} default value of {} as exactly once is enabled.",
                    COMMIT_INTERVAL_MS_CONFIG, EOS_DEFAULT_COMMIT_INTERVAL_MS);
            configUpdates.put(COMMIT_INTERVAL_MS_CONFIG, EOS_DEFAULT_COMMIT_INTERVAL_MS);
        }

        validateRackAwarenessConfiguration();

        return configUpdates;
    }

    private void validateRackAwarenessConfiguration() {
        final List<String> rackAwareAssignmentTags = getList(RACK_AWARE_ASSIGNMENT_TAGS_CONFIG);
        final Map<String, String> clientTags = clientTags();

        if (clientTags.size() > MAX_RACK_AWARE_ASSIGNMENT_TAG_LIST_SIZE) {
            throw new ConfigException("At most " + MAX_RACK_AWARE_ASSIGNMENT_TAG_LIST_SIZE + " client tags " +
                    "can be specified using " + CLIENT_TAG_PREFIX + " prefix.");
        }

        for (final String rackAwareAssignmentTag : rackAwareAssignmentTags) {
            if (!clientTags.containsKey(rackAwareAssignmentTag)) {
                throw new ConfigException(RACK_AWARE_ASSIGNMENT_TAGS_CONFIG,
                        rackAwareAssignmentTags,
                        "Contains invalid value [" + rackAwareAssignmentTag + "] " +
                                "which doesn't have corresponding tag set via [" + CLIENT_TAG_PREFIX + "] prefix.");
            }
        }

        clientTags.forEach((tagKey, tagValue) -> {
            if (tagKey.length() > MAX_RACK_AWARE_ASSIGNMENT_TAG_KEY_LENGTH) {
                throw new ConfigException(CLIENT_TAG_PREFIX,
                        tagKey,
                        "Tag key exceeds maximum length of " + MAX_RACK_AWARE_ASSIGNMENT_TAG_KEY_LENGTH + ".");
            }
            if (tagValue.length() > MAX_RACK_AWARE_ASSIGNMENT_TAG_VALUE_LENGTH) {
                throw new ConfigException(CLIENT_TAG_PREFIX,
                        tagValue,
                        "Tag value exceeds maximum length of " + MAX_RACK_AWARE_ASSIGNMENT_TAG_VALUE_LENGTH + ".");
            }
        });
    }

    @SuppressWarnings({"deprecation"})
    public static void main(final String[] args) {
        System.out.println(CONFIG.toHtml(4, config -> "streamsconfigs_" + config));
    }
}
