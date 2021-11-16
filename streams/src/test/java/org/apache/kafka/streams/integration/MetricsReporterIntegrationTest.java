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
package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.test.IntegrationTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@Category({IntegrationTest.class})
public class MetricsReporterIntegrationTest {

    private static final int NUM_BROKERS = 1;

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    // topic names
    private static final String STREAM_INPUT = "STREAM_INPUT";
    private static final String STREAM_OUTPUT = "STREAM_OUTPUT";

    private StreamsBuilder builder;
    private Properties streamsConfiguration;

    @Rule
    public TestName testName = new TestName();

    @BeforeClass
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterClass
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @Before
    public void before() throws InterruptedException {
        builder = new StreamsBuilder();

        final String safeTestName = safeUniqueTestName(getClass(), testName);
        final String appId = "app-" + safeTestName;

        streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, MetricReporterImpl.class.getName());
    }

    final static Map<String, Object> METRIC_NAME_TO_INITIAL_VALUE = new HashMap<>();

    public static class MetricReporterImpl implements MetricsReporter {


        @Override
        public void configure(final Map<String, ?> configs) {
        }

        @Override
        public void init(final List<KafkaMetric> metrics) {
        }

        @Override
        public void metricChange(final KafkaMetric metric) {
            // get value of metric, e.g. if you wanted checking the type of the value
            METRIC_NAME_TO_INITIAL_VALUE.put(metric.metricName().name(), metric.metricValue());
        }

        @Override
        public void metricRemoval(final KafkaMetric metric) {
        }

        @Override
        public void close() {
        }

    }

    @Test
    public void shouldBeAbleToProvideInitialMetricValueToMetricsReporter() {
        // no need to create the topics, because we don't start the stream - just need to create the KafkaStreams object
        // to check all initial values from the metrics are not null
        builder.stream(STREAM_INPUT, Consumed.with(Serdes.Integer(), Serdes.String()))
                .to(STREAM_OUTPUT, Produced.with(Serdes.Integer(), Serdes.String()));
        final Topology topology = builder.build();
        final KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsConfiguration);

        kafkaStreams.metrics().keySet().forEach(metricName -> {
            final Object initialMetricValue = METRIC_NAME_TO_INITIAL_VALUE.get(metricName.name());
            assertThat(initialMetricValue, notNullValue());
        });
    }

}