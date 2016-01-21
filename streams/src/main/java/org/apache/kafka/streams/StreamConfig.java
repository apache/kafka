/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.DefaultPartitionGrouper;
import org.apache.kafka.streams.processor.internals.KafkaStreamingPartitionAssignor;
import org.apache.kafka.streams.processor.internals.StreamThread;

import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

public class StreamConfig extends AbstractConfig {

    private static final ConfigDef CONFIG;

    /** <code>state.dir</code> */
    public static final String STATE_DIR_CONFIG = "state.dir";
    private static final String STATE_DIR_DOC = "Directory location for state store.";

    /** <code>zookeeper.connect<code/> */
    public static final String ZOOKEEPER_CONNECT_CONFIG = "zookeeper.connect";
    private static final String ZOOKEEPER_CONNECT_DOC = "Zookeeper connect string for Kafka topics management.";

    /** <code>commit.interval.ms</code> */
    public static final String COMMIT_INTERVAL_MS_CONFIG = "commit.interval.ms";
    private static final String COMMIT_INTERVAL_MS_DOC = "The frequency with which to save the position of the processor.";

    /** <code>poll.ms</code> */
    public static final String POLL_MS_CONFIG = "poll.ms";
    private static final String POLL_MS_DOC = "The amount of time in milliseconds to block waiting for input.";

    /** <code>num.stream.threads</code> */
    public static final String NUM_STREAM_THREADS_CONFIG = "num.stream.threads";
    private static final String NUM_STREAM_THREADS_DOC = "The number of threads to execute stream processing.";

    /** <code>num.stream.threads</code> */
    public static final String NUM_STANDBY_REPLICAS_CONFIG = "num.standby.replicas";
    private static final String NUM_STANDBY_REPLICAS_DOC = "The number of standby replicas for each task.";

    /** <code>buffered.records.per.partition</code> */
    public static final String BUFFERED_RECORDS_PER_PARTITION_CONFIG = "buffered.records.per.partition";
    private static final String BUFFERED_RECORDS_PER_PARTITION_DOC = "The maximum number of records to buffer per partition.";

    /** <code>state.cleanup.delay</code> */
    public static final String STATE_CLEANUP_DELAY_MS_CONFIG = "state.cleanup.delay.ms";
    private static final String STATE_CLEANUP_DELAY_MS_DOC = "The amount of time in milliseconds to wait before deleting state when a partition has migrated.";

    /** <code>total.records.to.process</code> */
    public static final String TOTAL_RECORDS_TO_PROCESS = "total.records.to.process";
    private static final String TOTAL_RECORDS_TO_DOC = "Exit after processing this many records.";

    /** <code>window.time.ms</code> */
    public static final String WINDOW_TIME_MS_CONFIG = "window.time.ms";
    private static final String WINDOW_TIME_MS_DOC = "Setting this to a non-negative value will cause the processor to get called "
                                                     + "with this frequency even if there is no message.";

    /** <code>timestamp.extractor</code> */
    public static final String TIMESTAMP_EXTRACTOR_CLASS_CONFIG = "timestamp.extractor";
    private static final String TIMESTAMP_EXTRACTOR_CLASS_DOC = "Timestamp extractor class that implements the <code>TimestampExtractor</code> interface.";

    /** <code>partition.grouper</code> */
    public static final String PARTITION_GROUPER_CLASS_CONFIG = "partition.grouper";
    private static final String PARTITION_GROUPER_CLASS_DOC = "Partition grouper class that implements the <code>PartitionGrouper</code> interface.";

    /** <code>job.id</code> */
    public static final String JOB_ID_CONFIG = "job.id";
    public static final String JOB_ID_DOC = "An id string to identify for the stream job. It is used as 1) the default client-id prefix, 2) the group-id for membership management, 3) the changelog topic prefix.";

    /** <code>key.serializer</code> */
    public static final String KEY_SERIALIZER_CLASS_CONFIG = ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;

    /** <code>value.serializer</code> */
    public static final String VALUE_SERIALIZER_CLASS_CONFIG = ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

    /** <code>key.deserializer</code> */
    public static final String KEY_DESERIALIZER_CLASS_CONFIG = ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;

    /** <code>value.deserializer</code> */
    public static final String VALUE_DESERIALIZER_CLASS_CONFIG = ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

    /** <code>metrics.sample.window.ms</code> */
    public static final String METRICS_SAMPLE_WINDOW_MS_CONFIG = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG;

    /** <code>metrics.num.samples</code> */
    public static final String METRICS_NUM_SAMPLES_CONFIG = CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG;

    /** <code>metric.reporters</code> */
    public static final String METRIC_REPORTER_CLASSES_CONFIG = CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG;

    /** <code>bootstrap.servers</code> */
    public static final String BOOTSTRAP_SERVERS_CONFIG = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

    /** <code>client.id</code> */
    public static final String CLIENT_ID_CONFIG = CommonClientConfigs.CLIENT_ID_CONFIG;

    private static final String SYSTEM_TEMP_DIRECTORY = System.getProperty("java.io.tmpdir");

    static {
        CONFIG = new ConfigDef().define(JOB_ID_CONFIG,
                                        Type.STRING,
                                        "",
                                        Importance.MEDIUM,
                                        StreamConfig.JOB_ID_DOC)
                                .define(CLIENT_ID_CONFIG,
                                        Type.STRING,
                                        "",
                                        Importance.MEDIUM,
                                        CommonClientConfigs.CLIENT_ID_DOC)
                                .define(ZOOKEEPER_CONNECT_CONFIG,
                                        Type.STRING,
                                        "",
                                        Importance.HIGH,
                                        StreamConfig.ZOOKEEPER_CONNECT_DOC)
                                .define(STATE_DIR_CONFIG,
                                        Type.STRING,
                                        SYSTEM_TEMP_DIRECTORY,
                                        Importance.MEDIUM,
                                        STATE_DIR_DOC)
                                .define(COMMIT_INTERVAL_MS_CONFIG,
                                        Type.LONG,
                                        30000,
                                        Importance.HIGH,
                                        COMMIT_INTERVAL_MS_DOC)
                                .define(POLL_MS_CONFIG,
                                        Type.LONG,
                                        100,
                                        Importance.LOW,
                                        POLL_MS_DOC)
                                .define(NUM_STREAM_THREADS_CONFIG,
                                        Type.INT,
                                        1,
                                        Importance.LOW,
                                        NUM_STREAM_THREADS_DOC)
                                .define(NUM_STANDBY_REPLICAS_CONFIG,
                                        Type.INT,
                                        0,
                                        Importance.LOW,
                                        NUM_STANDBY_REPLICAS_DOC)
                                .define(BUFFERED_RECORDS_PER_PARTITION_CONFIG,
                                        Type.INT,
                                        1000,
                                        Importance.LOW,
                                        BUFFERED_RECORDS_PER_PARTITION_DOC)
                                .define(STATE_CLEANUP_DELAY_MS_CONFIG,
                                        Type.LONG,
                                        60000,
                                        Importance.LOW,
                                        STATE_CLEANUP_DELAY_MS_DOC)
                                .define(TOTAL_RECORDS_TO_PROCESS,
                                        Type.LONG,
                                        -1L,
                                        Importance.LOW,
                                        TOTAL_RECORDS_TO_DOC)
                                .define(WINDOW_TIME_MS_CONFIG,
                                        Type.LONG,
                                        -1L,
                                        Importance.MEDIUM,
                                        WINDOW_TIME_MS_DOC)
                                .define(KEY_SERIALIZER_CLASS_CONFIG,
                                        Type.CLASS,
                                        Importance.HIGH,
                                        ProducerConfig.KEY_SERIALIZER_CLASS_DOC)
                                .define(VALUE_SERIALIZER_CLASS_CONFIG,
                                        Type.CLASS,
                                        Importance.HIGH,
                                        ProducerConfig.VALUE_SERIALIZER_CLASS_DOC)
                                .define(KEY_DESERIALIZER_CLASS_CONFIG,
                                        Type.CLASS,
                                        Importance.HIGH,
                                        ConsumerConfig.KEY_DESERIALIZER_CLASS_DOC)
                                .define(VALUE_DESERIALIZER_CLASS_CONFIG,
                                        Type.CLASS,
                                        Importance.HIGH,
                                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_DOC)
                                .define(TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
                                        Type.CLASS,
                                        Importance.HIGH,
                                        TIMESTAMP_EXTRACTOR_CLASS_DOC)
                                .define(PARTITION_GROUPER_CLASS_CONFIG,
                                        Type.CLASS,
                                        DefaultPartitionGrouper.class,
                                        Importance.HIGH,
                                        PARTITION_GROUPER_CLASS_DOC)
                                .define(BOOTSTRAP_SERVERS_CONFIG,
                                        Type.STRING,
                                        Importance.HIGH,
                                        CommonClientConfigs.BOOSTRAP_SERVERS_DOC)
                                .define(METRIC_REPORTER_CLASSES_CONFIG,
                                        Type.LIST,
                                        "",
                                        Importance.LOW,
                                        CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC)
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
                                        CommonClientConfigs.METRICS_NUM_SAMPLES_DOC);
    }

    public static class InternalConfig {
        public static final String STREAM_THREAD_INSTANCE = "__stream.thread.instance__";
    }

    public StreamConfig(Map<?, ?> props) {
        super(CONFIG, props);
    }

    public Map<String, Object> getConsumerConfigs(StreamThread streamThread, String groupId, String clientId) {
        Map<String, Object> props = getBaseConsumerConfigs();

        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId + "-consumer");
        props.put(StreamConfig.NUM_STANDBY_REPLICAS_CONFIG, getInt(StreamConfig.NUM_STANDBY_REPLICAS_CONFIG));
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, KafkaStreamingPartitionAssignor.class.getName());

        props.put(StreamConfig.InternalConfig.STREAM_THREAD_INSTANCE, streamThread);

        return props;
    }

    public Map<String, Object> getRestoreConsumerConfigs(String clientId) {
        Map<String, Object> props = getBaseConsumerConfigs();

        // no need to set group id for a restore consumer
        props.remove(ConsumerConfig.GROUP_ID_CONFIG);

        props.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId + "-restore-consumer");

        return props;
    }

    private Map<String, Object> getBaseConsumerConfigs() {
        Map<String, Object> props = this.originals();

        // set consumer default property values
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // remove properties that are not required for consumers
        props.remove(StreamConfig.KEY_SERIALIZER_CLASS_CONFIG);
        props.remove(StreamConfig.VALUE_SERIALIZER_CLASS_CONFIG);
        props.remove(StreamConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG);
        props.remove(StreamConfig.NUM_STANDBY_REPLICAS_CONFIG);

        return props;
    }

    public Map<String, Object> getProducerConfigs(String clientId) {
        Map<String, Object> props = this.originals();

        // set producer default property values
        props.put(ProducerConfig.LINGER_MS_CONFIG, "100");

        // remove properties that are not required for producers
        props.remove(StreamConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        props.remove(StreamConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
        props.remove(StreamConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG);

        props.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId + "-producer");

        return props;
    }

    public Serializer keySerializer() {
        return getConfiguredInstance(StreamConfig.KEY_SERIALIZER_CLASS_CONFIG, Serializer.class);
    }

    public Serializer valueSerializer() {
        return getConfiguredInstance(StreamConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serializer.class);
    }

    public Deserializer keyDeserializer() {
        return getConfiguredInstance(StreamConfig.KEY_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
    }

    public Deserializer valueDeserializer() {
        return getConfiguredInstance(StreamConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
    }

    public static void main(String[] args) {
        System.out.println(CONFIG.toHtmlTable());
    }
}
