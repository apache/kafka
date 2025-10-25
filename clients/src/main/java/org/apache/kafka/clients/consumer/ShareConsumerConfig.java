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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.MetadataRecoveryStrategy;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.ShareAcknowledgementMode;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

/**
 * The consumer configuration behavior specific to share groups.
 */
public class ShareConsumerConfig extends AbstractConfig {

    private static final ConfigDef CONFIG;

    /**
     * A list of configuration keys not supported for SHARE consumer.
     */
    private static final List<String> SHARE_GROUP_UNSUPPORTED_CONFIGS = List.of(
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

    static {
        CONFIG = new ConfigDef().define(CommonConsumerConfigs.BOOTSTRAP_SERVERS_CONFIG,
                        ConfigDef.Type.LIST,
                        ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.ValidList.anyNonDuplicateValues(false, false),
                        ConfigDef.Importance.HIGH,
                        CommonClientConfigs.BOOTSTRAP_SERVERS_DOC)
                .define(CommonConsumerConfigs.CLIENT_DNS_LOOKUP_CONFIG,
                        ConfigDef.Type.STRING,
                        ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
                        in(ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
                                ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY.toString()),
                        ConfigDef.Importance.MEDIUM,
                        CommonClientConfigs.CLIENT_DNS_LOOKUP_DOC)
                .define(CommonConsumerConfigs.GROUP_ID_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, CommonConsumerConfigs.GROUP_ID_DOC)
                .define(CommonConsumerConfigs.GROUP_INSTANCE_ID_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        new ConfigDef.NonEmptyString(),
                        ConfigDef.Importance.MEDIUM,
                        CommonConsumerConfigs.GROUP_INSTANCE_ID_DOC)
                .define(CommonConsumerConfigs.SESSION_TIMEOUT_MS_CONFIG,
                        ConfigDef.Type.INT,
                        45000,
                        ConfigDef.Importance.HIGH,
                        CommonConsumerConfigs.SESSION_TIMEOUT_MS_DOC)
                .define(CommonConsumerConfigs.HEARTBEAT_INTERVAL_MS_CONFIG,
                        ConfigDef.Type.INT,
                        3000,
                        ConfigDef.Importance.HIGH,
                        CommonConsumerConfigs.HEARTBEAT_INTERVAL_MS_DOC)
                .define(CommonConsumerConfigs.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                        ConfigDef.Type.LIST,
                        List.of(RangeAssignor.class, CooperativeStickyAssignor.class),
                        ConfigDef.ValidList.anyNonDuplicateValues(true, false),
                        ConfigDef.Importance.MEDIUM,
                        CommonConsumerConfigs.PARTITION_ASSIGNMENT_STRATEGY_DOC)
                .define(CommonConsumerConfigs.METADATA_MAX_AGE_CONFIG,
                        ConfigDef.Type.LONG,
                        5 * 60 * 1000,
                        atLeast(0),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.METADATA_MAX_AGE_DOC)
                .define(CommonConsumerConfigs.ENABLE_AUTO_COMMIT_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        true,
                        ConfigDef.Importance.MEDIUM,
                        CommonConsumerConfigs.ENABLE_AUTO_COMMIT_DOC)
                .define(CommonConsumerConfigs.AUTO_COMMIT_INTERVAL_MS_CONFIG,
                        ConfigDef.Type.INT,
                        5000,
                        atLeast(0),
                        ConfigDef.Importance.LOW,
                        CommonConsumerConfigs.AUTO_COMMIT_INTERVAL_MS_DOC)
                .define(CommonConsumerConfigs.CLIENT_ID_CONFIG,
                        ConfigDef.Type.STRING,
                        "",
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.CLIENT_ID_DOC)
                .define(CommonConsumerConfigs.CLIENT_RACK_CONFIG,
                        ConfigDef.Type.STRING,
                        CommonConsumerConfigs.DEFAULT_CLIENT_RACK,
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.CLIENT_RACK_DOC)
                .define(CommonConsumerConfigs.MAX_PARTITION_FETCH_BYTES_CONFIG,
                        ConfigDef.Type.INT,
                        CommonConsumerConfigs.DEFAULT_MAX_PARTITION_FETCH_BYTES,
                        atLeast(0),
                        ConfigDef.Importance.HIGH,
                        CommonConsumerConfigs.MAX_PARTITION_FETCH_BYTES_DOC)
                .define(CommonConsumerConfigs.SEND_BUFFER_CONFIG,
                        ConfigDef.Type.INT,
                        128 * 1024,
                        atLeast(CommonClientConfigs.SEND_BUFFER_LOWER_BOUND),
                        ConfigDef.Importance.MEDIUM,
                        CommonClientConfigs.SEND_BUFFER_DOC)
                .define(CommonConsumerConfigs.RECEIVE_BUFFER_CONFIG,
                        ConfigDef.Type.INT,
                        64 * 1024,
                        atLeast(CommonClientConfigs.RECEIVE_BUFFER_LOWER_BOUND),
                        ConfigDef.Importance.MEDIUM,
                        CommonClientConfigs.RECEIVE_BUFFER_DOC)
                .define(CommonConsumerConfigs.FETCH_MIN_BYTES_CONFIG,
                        ConfigDef.Type.INT,
                        CommonConsumerConfigs.DEFAULT_FETCH_MIN_BYTES,
                        atLeast(0),
                        ConfigDef.Importance.HIGH,
                        CommonConsumerConfigs.FETCH_MIN_BYTES_DOC)
                .define(CommonConsumerConfigs.FETCH_MAX_BYTES_CONFIG,
                        ConfigDef.Type.INT,
                        CommonConsumerConfigs.DEFAULT_FETCH_MAX_BYTES,
                        atLeast(0),
                        ConfigDef.Importance.MEDIUM,
                        CommonConsumerConfigs.FETCH_MAX_BYTES_DOC)
                .define(CommonConsumerConfigs.FETCH_MAX_WAIT_MS_CONFIG,
                        ConfigDef.Type.INT,
                        CommonConsumerConfigs.DEFAULT_FETCH_MAX_WAIT_MS,
                        atLeast(0),
                        ConfigDef.Importance.LOW,
                        CommonConsumerConfigs.FETCH_MAX_WAIT_MS_DOC)
                .define(CommonConsumerConfigs.RECONNECT_BACKOFF_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        50L,
                        atLeast(0L),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.RECONNECT_BACKOFF_MS_DOC)
                .define(CommonConsumerConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        1000L,
                        atLeast(0L),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_DOC)
                .define(CommonConsumerConfigs.RETRY_BACKOFF_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        CommonClientConfigs.DEFAULT_RETRY_BACKOFF_MS,
                        atLeast(0L),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.RETRY_BACKOFF_MS_DOC)
                .define(CommonConsumerConfigs.RETRY_BACKOFF_MAX_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        CommonClientConfigs.DEFAULT_RETRY_BACKOFF_MAX_MS,
                        atLeast(0L),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.RETRY_BACKOFF_MAX_MS_DOC)
                .define(CommonConsumerConfigs.ENABLE_METRICS_PUSH_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        true,
                        ConfigDef.Importance.LOW,
                        CommonConsumerConfigs.ENABLE_METRICS_PUSH_DOC)
                .define(CommonConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                        ConfigDef.Type.STRING,
                        AutoOffsetResetStrategy.LATEST.name(),
                        new AutoOffsetResetStrategy.Validator(),
                        ConfigDef.Importance.MEDIUM,
                        CommonConsumerConfigs.AUTO_OFFSET_RESET_DOC)
                .define(CommonConsumerConfigs.CHECK_CRCS_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        true,
                        ConfigDef.Importance.LOW,
                        CommonConsumerConfigs.CHECK_CRCS_DOC)
                .define(CommonConsumerConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        30000,
                        atLeast(0),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_DOC)
                .define(CommonConsumerConfigs.METRICS_NUM_SAMPLES_CONFIG,
                        ConfigDef.Type.INT,
                        2,
                        atLeast(1),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.METRICS_NUM_SAMPLES_DOC)
                .define(CommonConsumerConfigs.METRICS_RECORDING_LEVEL_CONFIG,
                        ConfigDef.Type.STRING,
                        Sensor.RecordingLevel.INFO.toString(),
                        in(Sensor.RecordingLevel.INFO.toString(), Sensor.RecordingLevel.DEBUG.toString(), Sensor.RecordingLevel.TRACE.toString()),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.METRICS_RECORDING_LEVEL_DOC)
                .define(CommonConsumerConfigs.METRIC_REPORTER_CLASSES_CONFIG,
                        ConfigDef.Type.LIST,
                        JmxReporter.class.getName(),
                        ConfigDef.ValidList.anyNonDuplicateValues(true, false),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC)
                .define(CommonConsumerConfigs.KEY_DESERIALIZER_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        ConfigDef.Importance.HIGH,
                        CommonConsumerConfigs.KEY_DESERIALIZER_CLASS_DOC)
                .define(CommonConsumerConfigs.VALUE_DESERIALIZER_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        ConfigDef.Importance.HIGH,
                        CommonConsumerConfigs.VALUE_DESERIALIZER_CLASS_DOC)
                .define(CommonConsumerConfigs.REQUEST_TIMEOUT_MS_CONFIG,
                        ConfigDef.Type.INT,
                        30000,
                        atLeast(0),
                        ConfigDef.Importance.MEDIUM,
                        CommonConsumerConfigs.REQUEST_TIMEOUT_MS_DOC)
                .define(CommonConsumerConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG,
                        ConfigDef.Type.INT,
                        60 * 1000,
                        atLeast(0),
                        ConfigDef.Importance.MEDIUM,
                        CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_DOC)
                .define(CommonConsumerConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS,
                        ConfigDef.Importance.MEDIUM,
                        CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC)
                .define(CommonConsumerConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS,
                        ConfigDef.Importance.MEDIUM,
                        CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC)
                /* default is set to be a bit lower than the server default (10 min), to avoid both client and server closing connection at same time */
                .define(CommonConsumerConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        9 * 60 * 1000,
                        ConfigDef.Importance.MEDIUM,
                        CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_DOC)
                .define(CommonConsumerConfigs.INTERCEPTOR_CLASSES_CONFIG,
                        ConfigDef.Type.LIST,
                        List.of(),
                        ConfigDef.ValidList.anyNonDuplicateValues(true, false),
                        ConfigDef.Importance.LOW,
                        CommonConsumerConfigs.INTERCEPTOR_CLASSES_DOC)
                .define(CommonConsumerConfigs.MAX_POLL_RECORDS_CONFIG,
                        ConfigDef.Type.INT,
                        CommonConsumerConfigs.DEFAULT_MAX_POLL_RECORDS,
                        atLeast(1),
                        ConfigDef.Importance.MEDIUM,
                        CommonConsumerConfigs.MAX_POLL_RECORDS_DOC)
                .define(CommonConsumerConfigs.MAX_POLL_INTERVAL_MS_CONFIG,
                        ConfigDef.Type.INT,
                        300000,
                        atLeast(1),
                        ConfigDef.Importance.MEDIUM,
                        CommonConsumerConfigs.MAX_POLL_INTERVAL_MS_DOC)
                .define(CommonConsumerConfigs.EXCLUDE_INTERNAL_TOPICS_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        CommonConsumerConfigs.DEFAULT_EXCLUDE_INTERNAL_TOPICS,
                        ConfigDef.Importance.MEDIUM,
                        CommonConsumerConfigs.EXCLUDE_INTERNAL_TOPICS_DOC)
                .defineInternal(CommonConsumerConfigs.THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.LOW)
                .define(CommonConsumerConfigs.ISOLATION_LEVEL_CONFIG,
                        ConfigDef.Type.STRING,
                        CommonConsumerConfigs.DEFAULT_ISOLATION_LEVEL,
                        in(IsolationLevel.READ_COMMITTED.toString(), IsolationLevel.READ_UNCOMMITTED.toString()),
                        ConfigDef.Importance.MEDIUM,
                        CommonConsumerConfigs.ISOLATION_LEVEL_DOC)
                .define(CommonConsumerConfigs.ALLOW_AUTO_CREATE_TOPICS_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        CommonConsumerConfigs.DEFAULT_ALLOW_AUTO_CREATE_TOPICS,
                        ConfigDef.Importance.MEDIUM,
                        CommonConsumerConfigs.ALLOW_AUTO_CREATE_TOPICS_DOC)
                .define(CommonConsumerConfigs.GROUP_PROTOCOL_CONFIG,
                        ConfigDef.Type.STRING,
                        CommonConsumerConfigs.DEFAULT_GROUP_PROTOCOL,
                        ConfigDef.CaseInsensitiveValidString.in(Utils.enumOptions(GroupProtocol.class)),
                        ConfigDef.Importance.HIGH,
                        CommonConsumerConfigs.GROUP_PROTOCOL_DOC)
                .define(CommonConsumerConfigs.GROUP_REMOTE_ASSIGNOR_CONFIG,
                        ConfigDef.Type.STRING,
                        CommonConsumerConfigs.DEFAULT_GROUP_REMOTE_ASSIGNOR,
                        ConfigDef.Importance.MEDIUM,
                        CommonConsumerConfigs.GROUP_REMOTE_ASSIGNOR_DOC)
                // security support
                .define(CommonConsumerConfigs.SECURITY_PROVIDERS_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.LOW,
                        CommonConsumerConfigs.SECURITY_PROVIDERS_DOC)
                .define(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                        ConfigDef.Type.STRING,
                        CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
                        ConfigDef.CaseInsensitiveValidString
                                .in(Utils.enumOptions(SecurityProtocol.class)),
                        ConfigDef.Importance.MEDIUM,
                        CommonClientConfigs.SECURITY_PROTOCOL_DOC)
                .withClientSslSupport()
                .withClientSaslSupport()
                .define(CommonClientConfigs.METADATA_RECOVERY_STRATEGY_CONFIG,
                        ConfigDef.Type.STRING,
                        CommonClientConfigs.DEFAULT_METADATA_RECOVERY_STRATEGY,
                        ConfigDef.CaseInsensitiveValidString
                                .in(Utils.enumOptions(MetadataRecoveryStrategy.class)),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.METADATA_RECOVERY_STRATEGY_DOC)
                .define(CommonClientConfigs.METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        CommonClientConfigs.DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS,
                        atLeast(0),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_DOC)
                .define(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG,
                        ConfigDef.Type.STRING,
                        ShareAcknowledgementMode.IMPLICIT.name(),
                        new ShareAcknowledgementMode.Validator(),
                        ConfigDef.Importance.MEDIUM,
                        CommonConsumerConfigs.SHARE_ACKNOWLEDGEMENT_MODE_DOC)
                .define(CONFIG_PROVIDERS_CONFIG,
                        ConfigDef.Type.LIST,
                        List.of(),
                        ConfigDef.ValidList.anyNonDuplicateValues(true, false),
                        ConfigDef.Importance.LOW,
                        CONFIG_PROVIDERS_DOC);
    }

    public static Map<String, Object> appendDeserializerToConfig(Map<String, Object> configs,
                                                                 Deserializer<?> keyDeserializer,
                                                                 Deserializer<?> valueDeserializer) {
        // validate deserializer configuration, if the passed deserializer instance is null, the user must explicitly set a valid deserializer configuration value
        Map<String, Object> newConfigs = new HashMap<>(configs);
        if (keyDeserializer != null)
            newConfigs.put(CommonConsumerConfigs.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getClass());
        else if (newConfigs.get(CommonConsumerConfigs.KEY_DESERIALIZER_CLASS_CONFIG) == null)
            throw new ConfigException(CommonConsumerConfigs.KEY_DESERIALIZER_CLASS_CONFIG, null, "must be non-null.");
        if (valueDeserializer != null)
            newConfigs.put(CommonConsumerConfigs.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getClass());
        else if (newConfigs.get(CommonConsumerConfigs.VALUE_DESERIALIZER_CLASS_CONFIG) == null)
            throw new ConfigException(CommonConsumerConfigs.VALUE_DESERIALIZER_CLASS_CONFIG, null, "must be non-null.");
        return newConfigs;
    }


    public ShareConsumerConfig(Properties props) {
        super(CONFIG, props);
    }

    ShareConsumerConfig(Map<String, Object> props) {
        super(CONFIG, props);
    }

    protected ShareConsumerConfig(Map<?, ?> props, boolean doLog) {
        super(CONFIG, props, doLog);
    }

    @Override
    protected Map<String, Object> preProcessParsedConfig(final Map<String, Object> parsedValues) {
        checkUnsupportedConfigsPreProcess(parsedValues);
        return parsedValues;
    }

    private void checkUnsupportedConfigsPreProcess(Map<String, Object> parsedValues) {
        List<String> invalidConfigs = new ArrayList<>();
        SHARE_GROUP_UNSUPPORTED_CONFIGS.forEach(configName -> {
            if (parsedValues.containsKey(configName)) {
                invalidConfigs.add(configName);
            }
        });
        if (!invalidConfigs.isEmpty()) {
            throw new ConfigException(String.join(", ", invalidConfigs) +
                    " cannot be set when using a share group.");
        }
    }
}
