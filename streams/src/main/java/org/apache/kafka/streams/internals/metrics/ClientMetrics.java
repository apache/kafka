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
package org.apache.kafka.streams.internals.metrics;

import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.CLIENT_LEVEL_GROUP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addSumMetricToSensor;

public class ClientMetrics {
    private ClientMetrics() {}

    private static final Logger log = LoggerFactory.getLogger(ClientMetrics.class);
    private static final String VERSION = "version";
    private static final String COMMIT_ID = "commit-id";
    private static final String APPLICATION_ID = "application-id";
    private static final String TOPOLOGY_DESCRIPTION = "topology-description";
    private static final String STATE = "state";
    private static final String ALIVE_STREAM_THREADS = "alive-stream-threads";
    private static final String VERSION_FROM_FILE;
    private static final String COMMIT_ID_FROM_FILE;
    private static final String DEFAULT_VALUE = "unknown";
    private static final String FAILED_STREAM_THREADS = "failed-stream-threads";

    static {
        final Properties props = new Properties();
        try (InputStream resourceStream = ClientMetrics.class.getResourceAsStream(
            "/kafka/kafka-streams-version.properties")) {

            props.load(resourceStream);
        } catch (final Exception exception) {
            log.warn("Error while loading kafka-streams-version.properties", exception);
        }
        VERSION_FROM_FILE = props.getProperty("version", DEFAULT_VALUE).trim();
        COMMIT_ID_FROM_FILE = props.getProperty("commitId", DEFAULT_VALUE).trim();
    }

    private static final String VERSION_DESCRIPTION = "The version of the Kafka Streams client";
    private static final String COMMIT_ID_DESCRIPTION = "The version control commit ID of the Kafka Streams client";
    private static final String APPLICATION_ID_DESCRIPTION = "The application ID of the Kafka Streams client";
    private static final String TOPOLOGY_DESCRIPTION_DESCRIPTION =
        "The description of the topology executed in the Kafka Streams client";
    private static final String STATE_DESCRIPTION = "The state of the Kafka Streams client";
    private static final String ALIVE_STREAM_THREADS_DESCRIPTION = "The current number of alive stream threads that are running or participating in rebalance";
    private static final String FAILED_STREAM_THREADS_DESCRIPTION = "The number of failed stream threads since the start of the Kafka Streams client";

    public static String version() {
        return VERSION_FROM_FILE;
    }

    public static String commitId() {
        return COMMIT_ID_FROM_FILE;
    }

    public static void addVersionMetric(final StreamsMetricsImpl streamsMetrics) {
        streamsMetrics.addClientLevelImmutableMetric(
            VERSION,
            VERSION_DESCRIPTION,
            RecordingLevel.INFO,
            VERSION_FROM_FILE
        );
    }

    public static void addCommitIdMetric(final StreamsMetricsImpl streamsMetrics) {
        streamsMetrics.addClientLevelImmutableMetric(
            COMMIT_ID,
            COMMIT_ID_DESCRIPTION,
            RecordingLevel.INFO,
            COMMIT_ID_FROM_FILE
        );
    }

    public static void addApplicationIdMetric(final StreamsMetricsImpl streamsMetrics, final String applicationId) {
        streamsMetrics.addClientLevelImmutableMetric(
            APPLICATION_ID,
            APPLICATION_ID_DESCRIPTION,
            RecordingLevel.INFO,
            applicationId
        );
    }

    public static void addTopologyDescriptionMetric(final StreamsMetricsImpl streamsMetrics,
                                                    final Gauge<String> topologyDescription) {
        streamsMetrics.addClientLevelMutableMetric(
            TOPOLOGY_DESCRIPTION,
            TOPOLOGY_DESCRIPTION_DESCRIPTION,
            RecordingLevel.INFO,
            topologyDescription
        );
    }

    public static void addStateMetric(final StreamsMetricsImpl streamsMetrics,
                                      final Gauge<State> stateProvider) {
        streamsMetrics.addClientLevelMutableMetric(
            STATE,
            STATE_DESCRIPTION,
            RecordingLevel.INFO,
            stateProvider
        );
    }

    public static void addNumAliveStreamThreadMetric(final StreamsMetricsImpl streamsMetrics,
                                                     final Gauge<Integer> stateProvider) {
        streamsMetrics.addClientLevelMutableMetric(
            ALIVE_STREAM_THREADS,
            ALIVE_STREAM_THREADS_DESCRIPTION,
            RecordingLevel.INFO,
            stateProvider
        );
    }

    public static Sensor failedStreamThreadSensor(final StreamsMetricsImpl streamsMetrics) {
        final Sensor sensor = streamsMetrics.clientLevelSensor(FAILED_STREAM_THREADS, RecordingLevel.INFO);
        addSumMetricToSensor(
            sensor,
            CLIENT_LEVEL_GROUP,
            streamsMetrics.clientLevelTagMap(),
            FAILED_STREAM_THREADS,
            false,
            FAILED_STREAM_THREADS_DESCRIPTION
        );
        return sensor;
    }
}
