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
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

/**
 * The consumer configuration keys
 */
public class ConsumerConfig extends AbstractConfig {
    private static final ConfigDef CONFIG;

    /*
     * NOTE: DO NOT CHANGE EITHER CONFIG STRINGS OR THEIR JAVA VARIABLE NAMES AS
     * THESE ARE PART OF THE PUBLIC API AND CHANGE WILL BREAK USER CODE.
     */

    /**
     * <code>group.id</code>
     */
    public static final String GROUP_ID_CONFIG = "group.id";
    private static final String GROUP_ID_DOC = "A unique string that identifies the consumer group this consumer belongs to. This property is required if the consumer uses either the group management functionality by using <code>subscribe(topic)</code> or the Kafka-based offset management strategy.";

    /**
     * <code>group.instance.id</code>
     */
    public static final String GROUP_INSTANCE_ID_CONFIG = "group.instance.id";
    private static final String GROUP_INSTANCE_ID_DOC = "A unique identifier of the consumer instance provided by end user. " +
            "Only non-empty strings are permitted. If set, the consumer is treated as a static member, " +
            "which means that only one instance with this ID is allowed in the consumer group at any time. " +
            "This can be used in combination with a larger session timeout to avoid group rebalances caused by transient unavailability " +
            "(e.g. process restarts). If not set, the consumer will join the group as a dynamic member, which is the traditional behavior.";

    /** <code>max.poll.records</code> */
    public static final String MAX_POLL_RECORDS_CONFIG = "max.poll.records";
    private static final String MAX_POLL_RECORDS_DOC = "The maximum number of records returned in a single call to poll().";

    /** <code>max.poll.interval.ms</code> */
    public static final String MAX_POLL_INTERVAL_MS_CONFIG = "max.poll.interval.ms";
    private static final String MAX_POLL_INTERVAL_MS_DOC = "The maximum delay between invocations of poll() when using " +
            "consumer group management. This places an upper bound on the amount of time that the consumer can be idle " +
            "before fetching more records. If poll() is not called before expiration of this timeout, then the consumer " +
            "is considered failed and the group will rebalance in order to reassign the partitions to another member. ";

    /**
     * <code>session.timeout.ms</code>
     */
    public static final String SESSION_TIMEOUT_MS_CONFIG = "session.timeout.ms";
    private static final String SESSION_TIMEOUT_MS_DOC = "The timeout used to detect consumer failures when using " +
            "Kafka's group management facility. The consumer sends periodic heartbeats to indicate its liveness " +
            "to the broker. If no heartbeats are received by the broker before the expiration of this session timeout, " +
            "then the broker will remove this consumer from the group and initiate a rebalance. Note that the value " +
            "must be in the allowable range as configured in the broker configuration by <code>group.min.session.timeout.ms</code> " +
            "and <code>group.max.session.timeout.ms</code>.";

    /**
     * <code>heartbeat.interval.ms</code>
     */
    public static final String HEARTBEAT_INTERVAL_MS_CONFIG = "heartbeat.interval.ms";
    private static final String HEARTBEAT_INTERVAL_MS_DOC = "The expected time between heartbeats to the consumer " +
            "coordinator when using Kafka's group management facilities. Heartbeats are used to ensure that the " +
            "consumer's session stays active and to facilitate rebalancing when new consumers join or leave the group. " +
            "The value must be set lower than <code>session.timeout.ms</code>, but typically should be set no higher " +
            "than 1/3 of that value. It can be adjusted even lower to control the expected time for normal rebalances.";

    /**
     * <code>bootstrap.servers</code>
     */
    public static final String BOOTSTRAP_SERVERS_CONFIG = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

    /** <code>client.dns.lookup</code> */
    public static final String CLIENT_DNS_LOOKUP_CONFIG = CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG;

    /**
     * <code>enable.auto.commit</code>
     */
    public static final String ENABLE_AUTO_COMMIT_CONFIG = "enable.auto.commit";
    private static final String ENABLE_AUTO_COMMIT_DOC = "If true the consumer's offset will be periodically committed in the background.";

    /**
     * <code>auto.commit.interval.ms</code>
     */
    public static final String AUTO_COMMIT_INTERVAL_MS_CONFIG = "auto.commit.interval.ms";
    private static final String AUTO_COMMIT_INTERVAL_MS_DOC = "The frequency in milliseconds that the consumer offsets are auto-committed to Kafka if <code>enable.auto.commit</code> is set to <code>true</code>.";

    /**
     * <code>partition.assignment.strategy</code>
     */
    public static final String PARTITION_ASSIGNMENT_STRATEGY_CONFIG = "partition.assignment.strategy";
    private static final String PARTITION_ASSIGNMENT_STRATEGY_DOC = "The class name of the partition assignment strategy that the client will use to distribute partition ownership amongst consumer instances when group management is used";

    /**
     * <code>auto.offset.reset</code>
     */
    public static final String AUTO_OFFSET_RESET_CONFIG = "auto.offset.reset";
    public static final String AUTO_OFFSET_RESET_DOC = "What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server (e.g. because that data has been deleted): <ul><li>earliest: automatically reset the offset to the earliest offset<li>latest: automatically reset the offset to the latest offset</li><li>none: throw exception to the consumer if no previous offset is found for the consumer's group</li><li>anything else: throw exception to the consumer.</li></ul>";

    /**
     * <code>fetch.min.bytes</code>
     */
    public static final String FETCH_MIN_BYTES_CONFIG = "fetch.min.bytes";
    private static final String FETCH_MIN_BYTES_DOC = "The minimum amount of data the server should return for a fetch request. If insufficient data is available the request will wait for that much data to accumulate before answering the request. The default setting of 1 byte means that fetch requests are answered as soon as a single byte of data is available or the fetch request times out waiting for data to arrive. Setting this to something greater than 1 will cause the server to wait for larger amounts of data to accumulate which can improve server throughput a bit at the cost of some additional latency.";

    /**
     * <code>fetch.max.bytes</code>
     */
    public static final String FETCH_MAX_BYTES_CONFIG = "fetch.max.bytes";
    private static final String FETCH_MAX_BYTES_DOC = "The maximum amount of data the server should return for a fetch request. " +
            "Records are fetched in batches by the consumer, and if the first record batch in the first non-empty partition of the fetch is larger than " +
            "this value, the record batch will still be returned to ensure that the consumer can make progress. As such, this is not a absolute maximum. " +
            "The maximum record batch size accepted by the broker is defined via <code>message.max.bytes</code> (broker config) or " +
            "<code>max.message.bytes</code> (topic config). Note that the consumer performs multiple fetches in parallel.";
    public static final int DEFAULT_FETCH_MAX_BYTES = 50 * 1024 * 1024;

    /**
     * <code>fetch.max.wait.ms</code>
     */
    public static final String FETCH_MAX_WAIT_MS_CONFIG = "fetch.max.wait.ms";
    private static final String FETCH_MAX_WAIT_MS_DOC = "The maximum amount of time the server will block before answering the fetch request if there isn't sufficient data to immediately satisfy the requirement given by fetch.min.bytes.";

    /** <code>metadata.max.age.ms</code> */
    public static final String METADATA_MAX_AGE_CONFIG = CommonClientConfigs.METADATA_MAX_AGE_CONFIG;

    /**
     * <code>max.partition.fetch.bytes</code>
     */
    public static final String MAX_PARTITION_FETCH_BYTES_CONFIG = "max.partition.fetch.bytes";
    private static final String MAX_PARTITION_FETCH_BYTES_DOC = "The maximum amount of data per-partition the server " +
            "will return. Records are fetched in batches by the consumer. If the first record batch in the first non-empty " +
            "partition of the fetch is larger than this limit, the " +
            "batch will still be returned to ensure that the consumer can make progress. The maximum record batch size " +
            "accepted by the broker is defined via <code>message.max.bytes</code> (broker config) or " +
            "<code>max.message.bytes</code> (topic config). See " + FETCH_MAX_BYTES_CONFIG + " for limiting the consumer request size.";
    public static final int DEFAULT_MAX_PARTITION_FETCH_BYTES = 1 * 1024 * 1024;

    /** <code>send.buffer.bytes</code> */
    public static final String SEND_BUFFER_CONFIG = CommonClientConfigs.SEND_BUFFER_CONFIG;

    /** <code>receive.buffer.bytes</code> */
    public static final String RECEIVE_BUFFER_CONFIG = CommonClientConfigs.RECEIVE_BUFFER_CONFIG;

    /**
     * <code>client.id</code>
     */
    public static final String CLIENT_ID_CONFIG = CommonClientConfigs.CLIENT_ID_CONFIG;

    /**
     * <code>client.rack</code>
     */
    public static final String CLIENT_RACK_CONFIG = CommonClientConfigs.CLIENT_RACK_CONFIG;

    /**
     * <code>reconnect.backoff.ms</code>
     */
    public static final String RECONNECT_BACKOFF_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG;

    /**
     * <code>reconnect.backoff.max.ms</code>
     */
    public static final String RECONNECT_BACKOFF_MAX_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG;

    /**
     * <code>retry.backoff.ms</code>
     */
    public static final String RETRY_BACKOFF_MS_CONFIG = CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG;

    /**
     * <code>metrics.sample.window.ms</code>
     */
    public static final String METRICS_SAMPLE_WINDOW_MS_CONFIG = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG;

    /**
     * <code>metrics.num.samples</code>
     */
    public static final String METRICS_NUM_SAMPLES_CONFIG = CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG;

    /**
     * <code>metrics.log.level</code>
     */
    public static final String METRICS_RECORDING_LEVEL_CONFIG = CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG;

    /**
     * <code>metric.reporters</code>
     */
    public static final String METRIC_REPORTER_CLASSES_CONFIG = CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG;

    /**
     * <code>check.crcs</code>
     */
    public static final String CHECK_CRCS_CONFIG = "check.crcs";
    private static final String CHECK_CRCS_DOC = "Automatically check the CRC32 of the records consumed. This ensures no on-the-wire or on-disk corruption to the messages occurred. This check adds some overhead, so it may be disabled in cases seeking extreme performance.";

    /** <code>key.deserializer</code> */
    public static final String KEY_DESERIALIZER_CLASS_CONFIG = "key.deserializer";
    public static final String KEY_DESERIALIZER_CLASS_DOC = "Deserializer class for key that implements the <code>org.apache.kafka.common.serialization.Deserializer</code> interface.";

    /** <code>value.deserializer</code> */
    public static final String VALUE_DESERIALIZER_CLASS_CONFIG = "value.deserializer";
    public static final String VALUE_DESERIALIZER_CLASS_DOC = "Deserializer class for value that implements the <code>org.apache.kafka.common.serialization.Deserializer</code> interface.";

    /** <code>connections.max.idle.ms</code> */
    public static final String CONNECTIONS_MAX_IDLE_MS_CONFIG = CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG;

    /** <code>request.timeout.ms</code> */
    public static final String REQUEST_TIMEOUT_MS_CONFIG = CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;
    private static final String REQUEST_TIMEOUT_MS_DOC = CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC;

    /** <code>default.api.timeout.ms</code> */
    public static final String DEFAULT_API_TIMEOUT_MS_CONFIG = "default.api.timeout.ms";
    public static final String DEFAULT_API_TIMEOUT_MS_DOC = "Specifies the timeout (in milliseconds) for consumer APIs that could block. This configuration is used as the default timeout for all consumer operations that do not explicitly accept a <code>timeout</code> parameter.";

    /** <code>interceptor.classes</code> */
    public static final String INTERCEPTOR_CLASSES_CONFIG = "interceptor.classes";
    public static final String INTERCEPTOR_CLASSES_DOC = "A list of classes to use as interceptors. "
                                                        + "Implementing the <code>org.apache.kafka.clients.consumer.ConsumerInterceptor</code> interface allows you to intercept (and possibly mutate) records "
                                                        + "received by the consumer. By default, there are no interceptors.";


    /** <code>exclude.internal.topics</code> */
    public static final String EXCLUDE_INTERNAL_TOPICS_CONFIG = "exclude.internal.topics";
    private static final String EXCLUDE_INTERNAL_TOPICS_DOC = "Whether internal topics matching a subscribed pattern should " +
            "be excluded from the subscription. It is always possible to explicitly subscribe to an internal topic.";
    public static final boolean DEFAULT_EXCLUDE_INTERNAL_TOPICS = true;

    /**
     * <code>internal.leave.group.on.close</code>
     * Whether or not the consumer should leave the group on close. If set to <code>false</code> then a rebalance
     * won't occur until <code>session.timeout.ms</code> expires.
     *
     * <p>
     * Note: this is an internal configuration and could be changed in the future in a backward incompatible way
     *
     */
    static final String LEAVE_GROUP_ON_CLOSE_CONFIG = "internal.leave.group.on.close";

    /** <code>isolation.level</code> */
    public static final String ISOLATION_LEVEL_CONFIG = "isolation.level";
    public static final String ISOLATION_LEVEL_DOC = "<p>Controls how to read messages written transactionally. If set to <code>read_committed</code>, consumer.poll() will only return" +
            " transactional messages which have been committed. If set to <code>read_uncommitted</code>' (the default), consumer.poll() will return all messages, even transactional messages" +
            " which have been aborted. Non-transactional messages will be returned unconditionally in either mode.</p> <p>Messages will always be returned in offset order. Hence, in " +
            " <code>read_committed</code> mode, consumer.poll() will only return messages up to the last stable offset (LSO), which is the one less than the offset of the first open transaction." +
            " In particular any messages appearing after messages belonging to ongoing transactions will be withheld until the relevant transaction has been completed. As a result, <code>read_committed</code>" +
            " consumers will not be able to read up to the high watermark when there are in flight transactions.</p><p> Further, when in <code>read_committed</code> the seekToEnd method will" +
            " return the LSO";

    public static final String DEFAULT_ISOLATION_LEVEL = IsolationLevel.READ_UNCOMMITTED.toString().toLowerCase(Locale.ROOT);

    /** <code>allow.auto.create.topics</code> */
    public static final String ALLOW_AUTO_CREATE_TOPICS_CONFIG = "allow.auto.create.topics";
    private static final String ALLOW_AUTO_CREATE_TOPICS_DOC = "Allow automatic topic creation on the broker when" +
            " subscribing to or assigning a topic. A topic being subscribed to will be automatically created only if the" +
            " broker allows for it using `auto.create.topics.enable` broker configuration. This configuration must" +
            " be set to `false` when using brokers older than 0.11.0";
    public static final boolean DEFAULT_ALLOW_AUTO_CREATE_TOPICS = true;
    
    static {
        CONFIG = new ConfigDef().define(BOOTSTRAP_SERVERS_CONFIG,
                                        Type.LIST,
                                        Collections.emptyList(),
                                        new ConfigDef.NonNullValidator(),
                                        Importance.HIGH,
                                        CommonClientConfigs.BOOTSTRAP_SERVERS_DOC)
                                .define(CLIENT_DNS_LOOKUP_CONFIG,
                                        Type.STRING,
                                        ClientDnsLookup.DEFAULT.toString(),
                                        in(ClientDnsLookup.DEFAULT.toString(),
                                           ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
                                           ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY.toString()),
                                        Importance.MEDIUM,
                                        CommonClientConfigs.CLIENT_DNS_LOOKUP_DOC)
                                .define(GROUP_ID_CONFIG, Type.STRING, null, Importance.HIGH, GROUP_ID_DOC)
                                .define(GROUP_INSTANCE_ID_CONFIG,
                                        Type.STRING,
                                        null,
                                        Importance.MEDIUM,
                                        GROUP_INSTANCE_ID_DOC)
                                .define(SESSION_TIMEOUT_MS_CONFIG,
                                        Type.INT,
                                        10000,
                                        Importance.HIGH,
                                        SESSION_TIMEOUT_MS_DOC)
                                .define(HEARTBEAT_INTERVAL_MS_CONFIG,
                                        Type.INT,
                                        3000,
                                        Importance.HIGH,
                                        HEARTBEAT_INTERVAL_MS_DOC)
                                .define(PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                                        Type.LIST,
                                        Collections.singletonList(RangeAssignor.class),
                                        new ConfigDef.NonNullValidator(),
                                        Importance.MEDIUM,
                                        PARTITION_ASSIGNMENT_STRATEGY_DOC)
                                .define(METADATA_MAX_AGE_CONFIG,
                                        Type.LONG,
                                        5 * 60 * 1000,
                                        atLeast(0),
                                        Importance.LOW,
                                        CommonClientConfigs.METADATA_MAX_AGE_DOC)
                                .define(ENABLE_AUTO_COMMIT_CONFIG,
                                        Type.BOOLEAN,
                                        true,
                                        Importance.MEDIUM,
                                        ENABLE_AUTO_COMMIT_DOC)
                                .define(AUTO_COMMIT_INTERVAL_MS_CONFIG,
                                        Type.INT,
                                        5000,
                                        atLeast(0),
                                        Importance.LOW,
                                        AUTO_COMMIT_INTERVAL_MS_DOC)
                                .define(CLIENT_ID_CONFIG,
                                        Type.STRING,
                                        "",
                                        Importance.LOW,
                                        CommonClientConfigs.CLIENT_ID_DOC)
                                .define(CLIENT_RACK_CONFIG,
                                        Type.STRING,
                                        "",
                                        Importance.LOW,
                                        CommonClientConfigs.CLIENT_RACK_DOC)
                                .define(MAX_PARTITION_FETCH_BYTES_CONFIG,
                                        Type.INT,
                                        DEFAULT_MAX_PARTITION_FETCH_BYTES,
                                        atLeast(0),
                                        Importance.HIGH,
                                        MAX_PARTITION_FETCH_BYTES_DOC)
                                .define(SEND_BUFFER_CONFIG,
                                        Type.INT,
                                        128 * 1024,
                                        atLeast(CommonClientConfigs.SEND_BUFFER_LOWER_BOUND),
                                        Importance.MEDIUM,
                                        CommonClientConfigs.SEND_BUFFER_DOC)
                                .define(RECEIVE_BUFFER_CONFIG,
                                        Type.INT,
                                        64 * 1024,
                                        atLeast(CommonClientConfigs.RECEIVE_BUFFER_LOWER_BOUND),
                                        Importance.MEDIUM,
                                        CommonClientConfigs.RECEIVE_BUFFER_DOC)
                                .define(FETCH_MIN_BYTES_CONFIG,
                                        Type.INT,
                                        1,
                                        atLeast(0),
                                        Importance.HIGH,
                                        FETCH_MIN_BYTES_DOC)
                                .define(FETCH_MAX_BYTES_CONFIG,
                                        Type.INT,
                                        DEFAULT_FETCH_MAX_BYTES,
                                        atLeast(0),
                                        Importance.MEDIUM,
                                        FETCH_MAX_BYTES_DOC)
                                .define(FETCH_MAX_WAIT_MS_CONFIG,
                                        Type.INT,
                                        500,
                                        atLeast(0),
                                        Importance.LOW,
                                        FETCH_MAX_WAIT_MS_DOC)
                                .define(RECONNECT_BACKOFF_MS_CONFIG,
                                        Type.LONG,
                                        50L,
                                        atLeast(0L),
                                        Importance.LOW,
                                        CommonClientConfigs.RECONNECT_BACKOFF_MS_DOC)
                                .define(RECONNECT_BACKOFF_MAX_MS_CONFIG,
                                        Type.LONG,
                                        1000L,
                                        atLeast(0L),
                                        Importance.LOW,
                                        CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_DOC)
                                .define(RETRY_BACKOFF_MS_CONFIG,
                                        Type.LONG,
                                        100L,
                                        atLeast(0L),
                                        Importance.LOW,
                                        CommonClientConfigs.RETRY_BACKOFF_MS_DOC)
                                .define(AUTO_OFFSET_RESET_CONFIG,
                                        Type.STRING,
                                        "latest",
                                        in("latest", "earliest", "none"),
                                        Importance.MEDIUM,
                                        AUTO_OFFSET_RESET_DOC)
                                .define(CHECK_CRCS_CONFIG,
                                        Type.BOOLEAN,
                                        true,
                                        Importance.LOW,
                                        CHECK_CRCS_DOC)
                                .define(METRICS_SAMPLE_WINDOW_MS_CONFIG,
                                        Type.LONG,
                                        30000,
                                        atLeast(0),
                                        Importance.LOW,
                                        CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_DOC)
                                .define(METRICS_NUM_SAMPLES_CONFIG,
                                        Type.INT,
                                        2,
                                        atLeast(1),
                                        Importance.LOW,
                                        CommonClientConfigs.METRICS_NUM_SAMPLES_DOC)
                                .define(METRICS_RECORDING_LEVEL_CONFIG,
                                        Type.STRING,
                                        Sensor.RecordingLevel.INFO.toString(),
                                        in(Sensor.RecordingLevel.INFO.toString(), Sensor.RecordingLevel.DEBUG.toString()),
                                        Importance.LOW,
                                        CommonClientConfigs.METRICS_RECORDING_LEVEL_DOC)
                                .define(METRIC_REPORTER_CLASSES_CONFIG,
                                        Type.LIST,
                                        Collections.emptyList(),
                                        new ConfigDef.NonNullValidator(),
                                        Importance.LOW,
                                        CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC)
                                .define(KEY_DESERIALIZER_CLASS_CONFIG,
                                        Type.CLASS,
                                        Importance.HIGH,
                                        KEY_DESERIALIZER_CLASS_DOC)
                                .define(VALUE_DESERIALIZER_CLASS_CONFIG,
                                        Type.CLASS,
                                        Importance.HIGH,
                                        VALUE_DESERIALIZER_CLASS_DOC)
                                .define(REQUEST_TIMEOUT_MS_CONFIG,
                                        Type.INT,
                                        30000,
                                        atLeast(0),
                                        Importance.MEDIUM,
                                        REQUEST_TIMEOUT_MS_DOC)
                                .define(DEFAULT_API_TIMEOUT_MS_CONFIG,
                                        Type.INT,
                                        60 * 1000,
                                        atLeast(0),
                                        Importance.MEDIUM,
                                        DEFAULT_API_TIMEOUT_MS_DOC)
                                /* default is set to be a bit lower than the server default (10 min), to avoid both client and server closing connection at same time */
                                .define(CONNECTIONS_MAX_IDLE_MS_CONFIG,
                                        Type.LONG,
                                        9 * 60 * 1000,
                                        Importance.MEDIUM,
                                        CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_DOC)
                                .define(INTERCEPTOR_CLASSES_CONFIG,
                                        Type.LIST,
                                        Collections.emptyList(),
                                        new ConfigDef.NonNullValidator(),
                                        Importance.LOW,
                                        INTERCEPTOR_CLASSES_DOC)
                                .define(MAX_POLL_RECORDS_CONFIG,
                                        Type.INT,
                                        500,
                                        atLeast(1),
                                        Importance.MEDIUM,
                                        MAX_POLL_RECORDS_DOC)
                                .define(MAX_POLL_INTERVAL_MS_CONFIG,
                                        Type.INT,
                                        300000,
                                        atLeast(1),
                                        Importance.MEDIUM,
                                        MAX_POLL_INTERVAL_MS_DOC)
                                .define(EXCLUDE_INTERNAL_TOPICS_CONFIG,
                                        Type.BOOLEAN,
                                        DEFAULT_EXCLUDE_INTERNAL_TOPICS,
                                        Importance.MEDIUM,
                                        EXCLUDE_INTERNAL_TOPICS_DOC)
                                .defineInternal(LEAVE_GROUP_ON_CLOSE_CONFIG,
                                        Type.BOOLEAN,
                                        true,
                                        Importance.LOW)
                                .define(ISOLATION_LEVEL_CONFIG,
                                        Type.STRING,
                                        DEFAULT_ISOLATION_LEVEL,
                                        in(IsolationLevel.READ_COMMITTED.toString().toLowerCase(Locale.ROOT), IsolationLevel.READ_UNCOMMITTED.toString().toLowerCase(Locale.ROOT)),
                                        Importance.MEDIUM,
                                        ISOLATION_LEVEL_DOC)
                                .define(ALLOW_AUTO_CREATE_TOPICS_CONFIG,
                                        Type.BOOLEAN,
                                        DEFAULT_ALLOW_AUTO_CREATE_TOPICS,
                                        Importance.MEDIUM,
                                        ALLOW_AUTO_CREATE_TOPICS_DOC)
                                // security support
                                .define(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                                        Type.STRING,
                                        CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
                                        Importance.MEDIUM,
                                        CommonClientConfigs.SECURITY_PROTOCOL_DOC)
                                .withClientSslSupport()
                                .withClientSaslSupport();
    }

    @Override
    protected Map<String, Object> postProcessParsedConfig(final Map<String, Object> parsedValues) {
        return CommonClientConfigs.postProcessReconnectBackoffConfigs(this, parsedValues);
    }

    public static Map<String, Object> addDeserializerToConfig(Map<String, Object> configs,
                                                              Deserializer<?> keyDeserializer,
                                                              Deserializer<?> valueDeserializer) {
        Map<String, Object> newConfigs = new HashMap<>(configs);
        if (keyDeserializer != null)
            newConfigs.put(KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getClass());
        if (valueDeserializer != null)
            newConfigs.put(VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getClass());
        return newConfigs;
    }

    public static Properties addDeserializerToConfig(Properties properties,
                                                     Deserializer<?> keyDeserializer,
                                                     Deserializer<?> valueDeserializer) {
        Properties newProperties = new Properties();
        newProperties.putAll(properties);
        if (keyDeserializer != null)
            newProperties.put(KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getClass().getName());
        if (valueDeserializer != null)
            newProperties.put(VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getClass().getName());
        return newProperties;
    }

    public ConsumerConfig(Properties props) {
        super(CONFIG, props);
    }

    public ConsumerConfig(Map<String, Object> props) {
        super(CONFIG, props);
    }

    protected ConsumerConfig(Map<?, ?> props, boolean doLog) {
        super(CONFIG, props, doLog);
    }

    public static Set<String> configNames() {
        return CONFIG.names();
    }

    public static ConfigDef configDef() {
        return  new ConfigDef(CONFIG);
    }

    public static void main(String[] args) {
        System.out.println(CONFIG.toHtmlTable());
    }

}
