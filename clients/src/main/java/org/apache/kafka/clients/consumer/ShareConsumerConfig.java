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
import org.apache.kafka.clients.consumer.internals.ShareAcknowledgementMode;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SecurityConfig;
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
import java.util.concurrent.atomic.AtomicInteger;

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

    /*
     * NOTE: DO NOT CHANGE EITHER CONFIG STRINGS OR THEIR JAVA VARIABLE NAMES AS
     * THESE ARE PART OF THE PUBLIC API AND CHANGE WILL BREAK USER CODE.
     */

    /**
     * <code>group.id</code>
     */
    public static final String GROUP_ID_CONFIG = CommonConsumerConfigs.GROUP_ID_CONFIG;
    private static final String GROUP_ID_DOC = CommonConsumerConfigs.GROUP_ID_DOC;


    /** <code>max.poll.records</code> */
    public static final String MAX_POLL_RECORDS_CONFIG = CommonConsumerConfigs.MAX_POLL_RECORDS_CONFIG;
    private static final String MAX_POLL_RECORDS_DOC = CommonConsumerConfigs.MAX_POLL_RECORDS_DOC;
    public static final int DEFAULT_MAX_POLL_RECORDS = 500;

    /** <code>max.poll.interval.ms</code> */
    public static final String MAX_POLL_INTERVAL_MS_CONFIG = CommonConsumerConfigs.MAX_POLL_INTERVAL_MS_CONFIG;
    private static final String MAX_POLL_INTERVAL_MS_DOC = CommonConsumerConfigs.MAX_POLL_INTERVAL_MS_DOC;

    /**
     * <code>bootstrap.servers</code>
     */
    public static final String BOOTSTRAP_SERVERS_CONFIG = CommonConsumerConfigs.BOOTSTRAP_SERVERS_CONFIG;

    /** <code>client.dns.lookup</code> */
    public static final String CLIENT_DNS_LOOKUP_CONFIG = CommonConsumerConfigs.CLIENT_DNS_LOOKUP_CONFIG;

    /**
     * <code>fetch.min.bytes</code>
     */
    public static final String FETCH_MIN_BYTES_CONFIG = CommonConsumerConfigs.FETCH_MIN_BYTES_CONFIG;
    public static final int DEFAULT_FETCH_MIN_BYTES = 1;
    private static final String FETCH_MIN_BYTES_DOC = CommonConsumerConfigs.FETCH_MIN_BYTES_DOC;

    /**
     * <code>fetch.max.bytes</code>
     */
    public static final String FETCH_MAX_BYTES_CONFIG = CommonConsumerConfigs.FETCH_MAX_BYTES_CONFIG;
    private static final String FETCH_MAX_BYTES_DOC = CommonConsumerConfigs.FETCH_MAX_BYTES_DOC;
    public static final int DEFAULT_FETCH_MAX_BYTES = 50 * 1024 * 1024;

    /**
     * <code>fetch.max.wait.ms</code>
     */
    public static final String FETCH_MAX_WAIT_MS_CONFIG = CommonConsumerConfigs.FETCH_MAX_WAIT_MS_CONFIG;
    private static final String FETCH_MAX_WAIT_MS_DOC = CommonConsumerConfigs.FETCH_MAX_WAIT_MS_DOC;
    public static final int DEFAULT_FETCH_MAX_WAIT_MS = 500;

    /** <code>metadata.max.age.ms</code> */
    public static final String METADATA_MAX_AGE_CONFIG = CommonConsumerConfigs.METADATA_MAX_AGE_CONFIG;

    /**
     * <code>max.partition.fetch.bytes</code>
     */
    public static final String MAX_PARTITION_FETCH_BYTES_CONFIG = CommonConsumerConfigs.MAX_PARTITION_FETCH_BYTES_CONFIG;
    private static final String MAX_PARTITION_FETCH_BYTES_DOC = CommonConsumerConfigs.MAX_PARTITION_FETCH_BYTES_DOC;
    public static final int DEFAULT_MAX_PARTITION_FETCH_BYTES = 1 * 1024 * 1024;

    /** <code>send.buffer.bytes</code> */
    public static final String SEND_BUFFER_CONFIG = CommonConsumerConfigs.SEND_BUFFER_CONFIG;

    /** <code>receive.buffer.bytes</code> */
    public static final String RECEIVE_BUFFER_CONFIG = CommonConsumerConfigs.RECEIVE_BUFFER_CONFIG;

    /**
     * <code>client.id</code>
     */
    public static final String CLIENT_ID_CONFIG = CommonConsumerConfigs.CLIENT_ID_CONFIG;

    /**
     * <code>client.rack</code>
     */
    public static final String CLIENT_RACK_CONFIG = CommonConsumerConfigs.CLIENT_RACK_CONFIG;
    public static final String DEFAULT_CLIENT_RACK = CommonConsumerConfigs.DEFAULT_CLIENT_RACK;

    /**
     * <code>reconnect.backoff.ms</code>
     */
    public static final String RECONNECT_BACKOFF_MS_CONFIG = CommonConsumerConfigs.RECONNECT_BACKOFF_MS_CONFIG;

    /**
     * <code>reconnect.backoff.max.ms</code>
     */
    public static final String RECONNECT_BACKOFF_MAX_MS_CONFIG = CommonConsumerConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG;

    /**
     * <code>retry.backoff.ms</code>
     */
    public static final String RETRY_BACKOFF_MS_CONFIG = CommonConsumerConfigs.RETRY_BACKOFF_MS_CONFIG;

    /**
     * <code>enable.metrics.push</code>
     */
    public static final String ENABLE_METRICS_PUSH_CONFIG = CommonConsumerConfigs.ENABLE_METRICS_PUSH_CONFIG;
    public static final String ENABLE_METRICS_PUSH_DOC = CommonConsumerConfigs.ENABLE_METRICS_PUSH_DOC;

    /**
     * <code>retry.backoff.max.ms</code>
     */
    public static final String RETRY_BACKOFF_MAX_MS_CONFIG = CommonConsumerConfigs.RETRY_BACKOFF_MAX_MS_CONFIG;

    /**
     * <code>metrics.sample.window.ms</code>
     */
    public static final String METRICS_SAMPLE_WINDOW_MS_CONFIG = CommonConsumerConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG;

    /**
     * <code>metrics.num.samples</code>
     */
    public static final String METRICS_NUM_SAMPLES_CONFIG = CommonConsumerConfigs.METRICS_NUM_SAMPLES_CONFIG;

    /**
     * <code>metrics.log.level</code>
     */
    public static final String METRICS_RECORDING_LEVEL_CONFIG = CommonConsumerConfigs.METRICS_RECORDING_LEVEL_CONFIG;

    /**
     * <code>metric.reporters</code>
     */
    public static final String METRIC_REPORTER_CLASSES_CONFIG = CommonConsumerConfigs.METRIC_REPORTER_CLASSES_CONFIG;

    /**
     * <code>check.crcs</code>
     */
    public static final String CHECK_CRCS_CONFIG = CommonConsumerConfigs.CHECK_CRCS_CONFIG;
    private static final String CHECK_CRCS_DOC = CommonConsumerConfigs.CHECK_CRCS_DOC;

    /** <code>key.deserializer</code> */
    public static final String KEY_DESERIALIZER_CLASS_CONFIG = CommonConsumerConfigs.KEY_DESERIALIZER_CLASS_CONFIG;
    public static final String KEY_DESERIALIZER_CLASS_DOC = CommonConsumerConfigs.KEY_DESERIALIZER_CLASS_DOC;

    /** <code>value.deserializer</code> */
    public static final String VALUE_DESERIALIZER_CLASS_CONFIG = CommonConsumerConfigs.VALUE_DESERIALIZER_CLASS_CONFIG;
    public static final String VALUE_DESERIALIZER_CLASS_DOC = CommonConsumerConfigs.VALUE_DESERIALIZER_CLASS_DOC;

    /** <code>socket.connection.setup.timeout.ms</code> */
    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG = CommonConsumerConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG;

    /** <code>socket.connection.setup.timeout.max.ms</code> */
    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG = CommonConsumerConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG;

    /** <code>connections.max.idle.ms</code> */
    public static final String CONNECTIONS_MAX_IDLE_MS_CONFIG = CommonConsumerConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG;

    /** <code>request.timeout.ms</code> */
    public static final String REQUEST_TIMEOUT_MS_CONFIG = CommonConsumerConfigs.REQUEST_TIMEOUT_MS_CONFIG;
    private static final String REQUEST_TIMEOUT_MS_DOC = CommonConsumerConfigs.REQUEST_TIMEOUT_MS_DOC;

    /** <code>default.api.timeout.ms</code> */
    public static final String DEFAULT_API_TIMEOUT_MS_CONFIG = CommonConsumerConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG;

    /** <code>exclude.internal.topics</code> */
    public static final String EXCLUDE_INTERNAL_TOPICS_CONFIG = CommonConsumerConfigs.EXCLUDE_INTERNAL_TOPICS_CONFIG;
    private static final String EXCLUDE_INTERNAL_TOPICS_DOC = CommonConsumerConfigs.EXCLUDE_INTERNAL_TOPICS_DOC;
    public static final boolean DEFAULT_EXCLUDE_INTERNAL_TOPICS = true;

    /**
     * <code>internal.throw.on.fetch.stable.offset.unsupported</code>
     * Whether or not the consumer should throw when the new stable offset feature is supported.
     * If set to <code>true</code> then the client shall crash upon hitting it.
     * The purpose of this flag is to prevent unexpected broker downgrade which makes
     * the offset fetch protection against pending commit invalid. The safest approach
     * is to fail fast to avoid introducing correctness issue.
     *
     * <p>
     * Note: this is an internal configuration and could be changed in the future in a backward incompatible way
     *
     */
    static final String THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED = CommonConsumerConfigs.THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED;

    public static final String DEFAULT_ISOLATION_LEVEL = IsolationLevel.READ_UNCOMMITTED.toString();

    /** <code>allow.auto.create.topics</code> */
    public static final String ALLOW_AUTO_CREATE_TOPICS_CONFIG = CommonConsumerConfigs.ALLOW_AUTO_CREATE_TOPICS_CONFIG;
    private static final String ALLOW_AUTO_CREATE_TOPICS_DOC = CommonConsumerConfigs.ALLOW_AUTO_CREATE_TOPICS_DOC;
    public static final boolean DEFAULT_ALLOW_AUTO_CREATE_TOPICS = true;

    /**
     * <code>security.providers</code>
     */
    public static final String SECURITY_PROVIDERS_CONFIG = SecurityConfig.SECURITY_PROVIDERS_CONFIG;
    private static final String SECURITY_PROVIDERS_DOC = SecurityConfig.SECURITY_PROVIDERS_DOC;

    /**
     * <code>share.acknowledgement.mode</code>
     */
    public static final String SHARE_ACKNOWLEDGEMENT_MODE_CONFIG = CommonConsumerConfigs.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG;
    private static final String SHARE_ACKNOWLEDGEMENT_MODE_DOC = CommonConsumerConfigs.SHARE_ACKNOWLEDGEMENT_MODE_DOC;

    private static final AtomicInteger CONSUMER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);


    static {
        CONFIG = new ConfigDef().define(BOOTSTRAP_SERVERS_CONFIG,
                        ConfigDef.Type.LIST,
                        ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.ValidList.anyNonDuplicateValues(false, false),
                        ConfigDef.Importance.HIGH,
                        CommonClientConfigs.BOOTSTRAP_SERVERS_DOC)
                .define(CLIENT_DNS_LOOKUP_CONFIG,
                        ConfigDef.Type.STRING,
                        ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
                        in(ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
                                ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY.toString()),
                        ConfigDef.Importance.MEDIUM,
                        CommonClientConfigs.CLIENT_DNS_LOOKUP_DOC)
                .define(GROUP_ID_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.HIGH,
                        CommonConsumerConfigs.GROUP_ID_DOC)
                .define(METADATA_MAX_AGE_CONFIG,
                        ConfigDef.Type.LONG,
                        5 * 60 * 1000,
                        atLeast(0),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.METADATA_MAX_AGE_DOC)
                .define(CLIENT_ID_CONFIG,
                        ConfigDef.Type.STRING,
                        "",
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.CLIENT_ID_DOC)
                .define(CLIENT_RACK_CONFIG,
                        ConfigDef.Type.STRING,
                        DEFAULT_CLIENT_RACK,
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.CLIENT_RACK_DOC)
                .define(MAX_PARTITION_FETCH_BYTES_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_MAX_PARTITION_FETCH_BYTES,
                        atLeast(0),
                        ConfigDef.Importance.HIGH,
                        MAX_PARTITION_FETCH_BYTES_DOC)
                .define(SEND_BUFFER_CONFIG,
                        ConfigDef.Type.INT,
                        128 * 1024,
                        atLeast(CommonClientConfigs.SEND_BUFFER_LOWER_BOUND),
                        ConfigDef.Importance.MEDIUM,
                        CommonClientConfigs.SEND_BUFFER_DOC)
                .define(RECEIVE_BUFFER_CONFIG,
                        ConfigDef.Type.INT,
                        64 * 1024,
                        atLeast(CommonClientConfigs.RECEIVE_BUFFER_LOWER_BOUND),
                        ConfigDef.Importance.MEDIUM,
                        CommonClientConfigs.RECEIVE_BUFFER_DOC)
                .define(FETCH_MIN_BYTES_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_FETCH_MIN_BYTES,
                        atLeast(0),
                        ConfigDef.Importance.HIGH,
                        FETCH_MIN_BYTES_DOC)
                .define(FETCH_MAX_BYTES_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_FETCH_MAX_BYTES,
                        atLeast(0),
                        ConfigDef.Importance.MEDIUM,
                        FETCH_MAX_BYTES_DOC)
                .define(FETCH_MAX_WAIT_MS_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_FETCH_MAX_WAIT_MS,
                        atLeast(0),
                        ConfigDef.Importance.LOW,
                        FETCH_MAX_WAIT_MS_DOC)
                .define(RECONNECT_BACKOFF_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        50L,
                        atLeast(0L),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.RECONNECT_BACKOFF_MS_DOC)
                .define(RECONNECT_BACKOFF_MAX_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        1000L,
                        atLeast(0L),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_DOC)
                .define(RETRY_BACKOFF_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        CommonClientConfigs.DEFAULT_RETRY_BACKOFF_MS,
                        atLeast(0L),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.RETRY_BACKOFF_MS_DOC)
                .define(RETRY_BACKOFF_MAX_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        CommonClientConfigs.DEFAULT_RETRY_BACKOFF_MAX_MS,
                        atLeast(0L),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.RETRY_BACKOFF_MAX_MS_DOC)
                .define(ENABLE_METRICS_PUSH_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        true,
                        ConfigDef.Importance.LOW,
                        ENABLE_METRICS_PUSH_DOC)
                .define(CHECK_CRCS_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        true,
                        ConfigDef.Importance.LOW,
                        CHECK_CRCS_DOC)
                .define(METRICS_SAMPLE_WINDOW_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        30000,
                        atLeast(0),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_DOC)
                .define(METRICS_NUM_SAMPLES_CONFIG,
                        ConfigDef.Type.INT,
                        2,
                        atLeast(1),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.METRICS_NUM_SAMPLES_DOC)
                .define(METRICS_RECORDING_LEVEL_CONFIG,
                        ConfigDef.Type.STRING,
                        Sensor.RecordingLevel.INFO.toString(),
                        in(Sensor.RecordingLevel.INFO.toString(), Sensor.RecordingLevel.DEBUG.toString(), Sensor.RecordingLevel.TRACE.toString()),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.METRICS_RECORDING_LEVEL_DOC)
                .define(METRIC_REPORTER_CLASSES_CONFIG,
                        ConfigDef.Type.LIST,
                        JmxReporter.class.getName(),
                        ConfigDef.ValidList.anyNonDuplicateValues(true, false),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC)
                .define(KEY_DESERIALIZER_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        ConfigDef.Importance.HIGH,
                        KEY_DESERIALIZER_CLASS_DOC)
                .define(VALUE_DESERIALIZER_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        ConfigDef.Importance.HIGH,
                        VALUE_DESERIALIZER_CLASS_DOC)
                .define(REQUEST_TIMEOUT_MS_CONFIG,
                        ConfigDef.Type.INT,
                        30000,
                        atLeast(0),
                        ConfigDef.Importance.MEDIUM,
                        REQUEST_TIMEOUT_MS_DOC)
                .define(DEFAULT_API_TIMEOUT_MS_CONFIG,
                        ConfigDef.Type.INT,
                        60 * 1000,
                        atLeast(0),
                        ConfigDef.Importance.MEDIUM,
                        CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_DOC)
                .define(SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS,
                        ConfigDef.Importance.MEDIUM,
                        CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC)
                .define(SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS,
                        ConfigDef.Importance.MEDIUM,
                        CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC)
                /* default is set to be a bit lower than the server default (10 min), to avoid both client and server closing connection at same time */
                .define(CONNECTIONS_MAX_IDLE_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        9 * 60 * 1000,
                        ConfigDef.Importance.MEDIUM,
                        CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_DOC)
                .define(MAX_POLL_RECORDS_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_MAX_POLL_RECORDS,
                        atLeast(1),
                        ConfigDef.Importance.MEDIUM,
                        MAX_POLL_RECORDS_DOC)
                .define(MAX_POLL_INTERVAL_MS_CONFIG,
                        ConfigDef.Type.INT,
                        300000,
                        atLeast(1),
                        ConfigDef.Importance.MEDIUM,
                        MAX_POLL_INTERVAL_MS_DOC)
                .define(EXCLUDE_INTERNAL_TOPICS_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        DEFAULT_EXCLUDE_INTERNAL_TOPICS,
                        ConfigDef.Importance.MEDIUM,
                        EXCLUDE_INTERNAL_TOPICS_DOC)
                .defineInternal(THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.LOW)
                .define(ALLOW_AUTO_CREATE_TOPICS_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        DEFAULT_ALLOW_AUTO_CREATE_TOPICS,
                        ConfigDef.Importance.MEDIUM,
                        ALLOW_AUTO_CREATE_TOPICS_DOC)
                // security support
                .define(SECURITY_PROVIDERS_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.LOW,
                        SECURITY_PROVIDERS_DOC)
                .define(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                        ConfigDef.Type.STRING,
                        CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
                        ConfigDef.CaseInsensitiveValidString
                                .in(Utils.enumOptions(SecurityProtocol.class)),
                        ConfigDef.Importance.MEDIUM,
                        CommonClientConfigs.SECURITY_PROTOCOL_DOC)
                .withClientSslSupport()
                .withClientSaslSupport()
                .define(CommonConsumerConfigs.METADATA_RECOVERY_STRATEGY_CONFIG,
                        ConfigDef.Type.STRING,
                        CommonClientConfigs.DEFAULT_METADATA_RECOVERY_STRATEGY,
                        ConfigDef.CaseInsensitiveValidString
                                .in(Utils.enumOptions(MetadataRecoveryStrategy.class)),
                        ConfigDef.Importance.LOW,
                        CommonConsumerConfigs.METADATA_RECOVERY_STRATEGY_DOC)
                .define(CommonClientConfigs.METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        CommonClientConfigs.DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS,
                        atLeast(0),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_DOC)
                .define(SHARE_ACKNOWLEDGEMENT_MODE_CONFIG,
                        ConfigDef.Type.STRING,
                        ShareAcknowledgementMode.IMPLICIT.name(),
                        new ShareAcknowledgementMode.Validator(),
                        ConfigDef.Importance.MEDIUM,
                        SHARE_ACKNOWLEDGEMENT_MODE_DOC)
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
            newConfigs.put(KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getClass());
        else if (newConfigs.get(KEY_DESERIALIZER_CLASS_CONFIG) == null)
            throw new ConfigException(KEY_DESERIALIZER_CLASS_CONFIG, null, "must be non-null.");
        if (valueDeserializer != null)
            newConfigs.put(VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getClass());
        else if (newConfigs.get(VALUE_DESERIALIZER_CLASS_CONFIG) == null)
            throw new ConfigException(VALUE_DESERIALIZER_CLASS_CONFIG, null, "must be non-null.");
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
