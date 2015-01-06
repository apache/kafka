/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
*/
package org.apache.kafka.clients.consumer;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

/**
 * The consumer configuration keys
 */
public class ConsumerConfig extends AbstractConfig {
    private static final ConfigDef config;

    /**
     * The identifier of the group this consumer belongs to. This is required if the consumer uses either the
     * group management functionality by using {@link Consumer#subscribe(String...) subscribe(topics)}. This is also required
     * if the consumer uses the default Kafka based offset management strategy. 
     */
    public static final String GROUP_ID_CONFIG = "group.id";
    
    /**
     * The timeout after which, if the {@link Consumer#poll(long) poll(timeout)} is not invoked, the consumer is
     * marked dead and a rebalance operation is triggered for the group identified by {@link #GROUP_ID_CONFIG}. Relevant 
     * if the consumer uses the group management functionality by invoking {@link Consumer#subscribe(String...) subscribe(topics)} 
     */
    public static final String SESSION_TIMEOUT_MS = "session.timeout.ms";

    /**
     * The number of times a consumer sends a heartbeat to the co-ordinator broker within a {@link #SESSION_TIMEOUT_MS} time window.
     * This frequency affects the latency of a rebalance operation since the co-ordinator broker notifies a consumer of a rebalance 
     * in the heartbeat response. Relevant if the consumer uses the group management functionality by invoking 
     * {@link Consumer#subscribe(String...) subscribe(topics)} 
     */
    public static final String HEARTBEAT_FREQUENCY = "heartbeat.frequency";

    /**
     * A list of URLs to use for establishing the initial connection to the cluster. This list should be in the form
     * <code>host1:port1,host2:port2,...</code>. These urls are just used for the initial connection to discover the
     * full cluster membership (which may change dynamically) so this list need not contain the full set of servers (you
     * may want more than one, though, in case a server is down).
     */
    public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";

    /**
     * If true, periodically commit to Kafka the offsets of messages already returned by the consumer. This committed 
     * offset will be used when the process fails as the position from which the consumption will begin.
     */
    public static final String ENABLE_AUTO_COMMIT_CONFIG = "enable.auto.commit";
    
    /**
     * The friendly name of the partition assignment strategy that the server will use to distribute partition ownership
     * amongst consumer instances when group management is used
     */
    public static final String PARTITION_ASSIGNMENT_STRATEGY = "partition.assignment.strategy";
    
    /**
     * The frequency in milliseconds that the consumer offsets are committed to Kafka. Relevant if {@link #ENABLE_AUTO_COMMIT_CONFIG}
     * is turned on.
     */
    public static final String AUTO_COMMIT_INTERVAL_MS_CONFIG = "auto.commit.interval.ms";
    
    /**
     * What to do when there is no initial offset in Kafka or if an offset is out of range:
     * <ul>
     * <li> smallest:      automatically reset the offset to the smallest offset
     * <li> largest:       automatically reset the offset to the largest offset
     * <li> disable:       throw exception to the consumer if no previous offset is found for the consumer's group
     * <li> anything else: throw exception to the consumer. 
     * </ul> 
     */
    public static final String AUTO_OFFSET_RESET_CONFIG = "auto.offset.reset";
    
    /**
     * The minimum amount of data the server should return for a fetch request. If insufficient data is available the 
     * request will wait for that much data to accumulate before answering the request.
     */
    public static final String FETCH_MIN_BYTES_CONFIG = "fetch.min.bytes";
    
    /**
     * The maximum amount of time the server will block before answering the fetch request if there isn't sufficient 
     * data to immediately satisfy {@link #FETCH_MIN_BYTES_CONFIG}. This should be less than or equal to the timeout used in 
     * {@link KafkaConsumer#poll(long) poll(timeout)}
     */
    public static final String FETCH_MAX_WAIT_MS_CONFIG = "fetch.max.wait.ms";
    
    /**
     * The maximum amount of time to block waiting to fetch metadata about a topic the first time a record is received 
     * from that topic. The consumer will throw a TimeoutException if it could not successfully fetch metadata within
     * this timeout.
     */
    public static final String METADATA_FETCH_TIMEOUT_CONFIG = "metadata.fetch.timeout.ms";

    /**
     * The total memory used by the consumer to buffer records received from the server. This config is meant to control
     * the consumer's memory usage, so it is the size of the global fetch buffer that will be shared across all partitions. 
     */
    public static final String TOTAL_BUFFER_MEMORY_CONFIG = "total.memory.bytes";

    /**
     * The minimum amount of memory that should be used to fetch at least one message for a partition. This puts a lower
     * bound on the consumer's memory utilization when there is at least one message for a partition available on the server.
     * This size must be at least as large as the maximum message size the server allows or else it is possible for the producer 
     * to send messages larger than the consumer can fetch. If that happens, the consumer can get stuck trying to fetch a large 
     * message on a certain partition. 
     */
    public static final String FETCH_BUFFER_CONFIG = "fetch.buffer.bytes";
    
    /**
     * The id string to pass to the server when making requests. The purpose of this is to be able to track the source
     * of requests beyond just ip/port by allowing a logical application name to be included.
     */
    public static final String CLIENT_ID_CONFIG = "client.id";

    /**
     * The size of the TCP send buffer to use when fetching data
     */
    public static final String SOCKET_RECEIVE_BUFFER_CONFIG = "socket.receive.buffer.bytes";

    /**
     * The amount of time to wait before attempting to reconnect to a given host. This avoids repeatedly connecting to a
     * host in a tight loop. This backoff applies to all requests sent by the consumer to the broker.
     */
    public static final String RECONNECT_BACKOFF_MS_CONFIG = "reconnect.backoff.ms";

    /** <code>metrics.sample.window.ms</code> */
    public static final String METRICS_SAMPLE_WINDOW_MS_CONFIG = "metrics.sample.window.ms";
    private static final String METRICS_SAMPLE_WINDOW_MS_DOC = "The metrics system maintains a configurable number of samples over a fixed window size. This configuration " + "controls the size of the window. For example we might maintain two samples each measured over a 30 second period. "
                                                               + "When a window expires we erase and overwrite the oldest window.";

    /** <code>metrics.num.samples</code> */
    public static final String METRICS_NUM_SAMPLES_CONFIG = "metrics.num.samples";
    private static final String METRICS_NUM_SAMPLES_DOC = "The number of samples maintained to compute metrics.";

    /** <code>metric.reporters</code> */
    public static final String METRIC_REPORTER_CLASSES_CONFIG = "metric.reporters";
    private static final String METRIC_REPORTER_CLASSES_DOC = "A list of classes to use as metrics reporters. Implementing the <code>MetricReporter</code> interface allows " + "plugging in classes that will be notified of new metric creation. The JmxReporter is always included to register JMX statistics.";

    /** <code>key.deserializer</code> */
    public static final String KEY_DESERIALIZER_CLASS_CONFIG = "key.deserializer";
    private static final String KEY_DESERIALIZER_CLASS_DOC = "Deserializer class for key that implements the <code>Deserializer</code> interface.";

    /** <code>value.deserializer</code> */
    public static final String VALUE_DESERIALIZER_CLASS_CONFIG = "value.deserializer";
    private static final String VALUE_DESERIALIZER_CLASS_DOC = "Deserializer class for value that implements the <code>Deserializer</code> interface.";

    static {
        /* TODO: add config docs */
        config = new ConfigDef().define(BOOTSTRAP_SERVERS_CONFIG, Type.LIST, Importance.HIGH, "blah blah")
                                .define(GROUP_ID_CONFIG, Type.STRING, Importance.HIGH, "blah blah")
                                .define(SESSION_TIMEOUT_MS, Type.LONG, 1000, Importance.HIGH, "blah blah")
                                .define(HEARTBEAT_FREQUENCY, Type.INT, 3, Importance.MEDIUM, "blah blah")
                                .define(PARTITION_ASSIGNMENT_STRATEGY, Type.STRING, Importance.MEDIUM, "blah blah")
                                .define(METADATA_FETCH_TIMEOUT_CONFIG, Type.LONG, 60 * 1000, atLeast(0), Importance.MEDIUM, "blah blah")
                                .define(ENABLE_AUTO_COMMIT_CONFIG, Type.BOOLEAN, true, Importance.MEDIUM, "blah blah")
                                .define(AUTO_COMMIT_INTERVAL_MS_CONFIG, Type.LONG, 5000, atLeast(0), Importance.LOW, "blah blah")
                                .define(CLIENT_ID_CONFIG, Type.STRING, "", Importance.LOW, "blah blah")
                                .define(TOTAL_BUFFER_MEMORY_CONFIG, Type.LONG, 32 * 1024 * 1024L, atLeast(0L), Importance.LOW, "blah blah")
                                .define(FETCH_BUFFER_CONFIG, Type.INT, 1 * 1024 * 1024, atLeast(0), Importance.HIGH, "blah blah")
                                .define(SOCKET_RECEIVE_BUFFER_CONFIG, Type.INT, 128 * 1024, atLeast(0), Importance.LOW, "blah blah")
                                .define(FETCH_MIN_BYTES_CONFIG, Type.LONG, 1024, atLeast(0), Importance.HIGH, "blah blah")
                                .define(FETCH_MAX_WAIT_MS_CONFIG, Type.LONG, 500, atLeast(0), Importance.LOW, "blah blah")
                                .define(RECONNECT_BACKOFF_MS_CONFIG, Type.LONG, 10L, atLeast(0L), Importance.LOW, "blah blah")
                                .define(AUTO_OFFSET_RESET_CONFIG, Type.STRING, "largest", Importance.MEDIUM, "blah blah")
                                .define(METRICS_SAMPLE_WINDOW_MS_CONFIG,
                                        Type.LONG,
                                        30000,
                                        atLeast(0),
                                        Importance.LOW,
                                        METRICS_SAMPLE_WINDOW_MS_DOC)
                                .define(METRICS_NUM_SAMPLES_CONFIG, Type.INT, 2, atLeast(1), Importance.LOW, METRICS_NUM_SAMPLES_DOC)
                                .define(METRIC_REPORTER_CLASSES_CONFIG, Type.LIST, "", Importance.LOW, METRIC_REPORTER_CLASSES_DOC)
                                .define(KEY_DESERIALIZER_CLASS_CONFIG, Type.CLASS, Importance.HIGH, KEY_DESERIALIZER_CLASS_DOC)
                                .define(VALUE_DESERIALIZER_CLASS_CONFIG, Type.CLASS, Importance.HIGH, VALUE_DESERIALIZER_CLASS_DOC);

    }

    ConsumerConfig(Map<? extends Object, ? extends Object> props) {
        super(config, props);
    }

}
