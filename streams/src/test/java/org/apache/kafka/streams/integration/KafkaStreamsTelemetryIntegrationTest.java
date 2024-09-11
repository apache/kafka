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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.telemetry.ClientTelemetry;
import org.apache.kafka.server.telemetry.ClientTelemetryPayload;
import org.apache.kafka.server.telemetry.ClientTelemetryReceiver;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkObjectProperties;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(600)
@Tag("integration")
public class KafkaStreamsTelemetryIntegrationTest {

    private static EmbeddedKafkaCluster cluster;
    private String appId;
    private String inputTopicTwoPartitions;
    private String outputTopicTwoPartitions;
    private String inputTopicOnePartition;
    private String outputTopicOnePartition;
    private final List<Properties> streamsConfigurations = new ArrayList<>();
    private static final List<MetricsInterceptingConsumer<byte[], byte[]>> INTERCEPTING_CONSUMERS = new ArrayList<>();
    private static final List<TestingMetricsInterceptingAdminClient> INTERCEPTING_ADMIN_CLIENTS = new ArrayList<>();
    private static final int NUM_BROKERS = 1;
    private static final int FIRST_INSTANCE_CONSUMER = 0;
    private static final int SECOND_INSTANCE_CONSUMER = 1;

    @BeforeAll
    public static void startCluster() throws IOException {
        final Properties properties = new Properties();
        properties.put("metric.reporters", TestingClientTelemetry.class.getName());
        cluster = new EmbeddedKafkaCluster(NUM_BROKERS, properties);
        cluster.start();
    }

    @BeforeEach
    public void setUp(final TestInfo testInfo) throws InterruptedException {
        appId = safeUniqueTestName(testInfo);
        inputTopicTwoPartitions = appId + "-input-two";
        outputTopicTwoPartitions = appId + "-output-two";
        inputTopicOnePartition = appId + "-input-one";
        outputTopicOnePartition = appId + "-output-one";
        cluster.createTopic(inputTopicTwoPartitions, 2, 1);
        cluster.createTopic(outputTopicTwoPartitions, 2, 1);
        cluster.createTopic(inputTopicOnePartition, 1, 1);
        cluster.createTopic(outputTopicOnePartition, 1, 1);
    }

    @AfterAll
    public static void closeCluster() {
        cluster.stop();
    }

    @AfterEach
    public void tearDown() throws Exception {
        INTERCEPTING_CONSUMERS.clear();
        INTERCEPTING_ADMIN_CLIENTS.clear();
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfigurations);
        streamsConfigurations.clear();
    }

    @Test
    @DisplayName("Calling unregisterMetric on metrics not registered should not cause an error")
    public void shouldNotThrowExceptionWhenRemovingNonExistingMetrics() throws InterruptedException {
        final Properties properties = props(true);
        final Topology topology = complexTopology();
        try (final KafkaStreams streams = new KafkaStreams(topology, properties)) {
            streams.start();
            waitForCondition(() -> KafkaStreams.State.RUNNING == streams.state(),
                    IntegrationTestUtils.DEFAULT_TIMEOUT,
                    () -> "Kafka Streams never transitioned to a RUNNING state.");

            final Consumer<?, ?> embeddedConsumer = INTERCEPTING_CONSUMERS.get(FIRST_INSTANCE_CONSUMER);
            final MetricName metricName = new MetricName("fakeMetric", "fakeGroup", "It's a fake metric", new HashMap<>());
            final KafkaMetric nonExitingMetric = new KafkaMetric(new Object(), metricName, (Measurable) (m, now) -> 1.0, new MetricConfig(), Time.SYSTEM);
            assertDoesNotThrow(() -> embeddedConsumer.unregisterMetricFromSubscription(nonExitingMetric));
        }
    }

    @ParameterizedTest
    @MethodSource("singleAndMultiTaskParameters")
    @DisplayName("Streams metrics should get passed to Consumer")
    public void shouldPassMetrics(final String topologyType, final boolean stateUpdaterEnabled) throws InterruptedException {
        final Properties properties = props(stateUpdaterEnabled);
        final Topology topology = topologyType.equals("simple") ? simpleTopology() : complexTopology();
        /*
           This test verifies that all Kafka Streams metrics with a thread-id tag get passed to the consumer
         */
        try (final KafkaStreams streams = new KafkaStreams(topology, properties)) {
            streams.start();
            waitForCondition(() -> KafkaStreams.State.RUNNING == streams.state(),
                    IntegrationTestUtils.DEFAULT_TIMEOUT,
                    () -> "Kafka Streams never transitioned to a RUNNING state.");

            final List<MetricName> streamsThreadMetrics = streams.metrics().values().stream().map(Metric::metricName)
                    .filter(metricName -> metricName.tags().containsKey("thread-id")).collect(Collectors.toList());

            final List<MetricName> streamsClientMetrics = streams.metrics().values().stream().map(Metric::metricName)
                    .filter(metricName -> metricName.group().equals("stream-metrics")).collect(Collectors.toList());



            final List<MetricName> consumerPassedStreamThreadMetricNames = INTERCEPTING_CONSUMERS.get(FIRST_INSTANCE_CONSUMER).passedMetrics.stream().map(KafkaMetric::metricName).collect(Collectors.toList());
            final List<MetricName> adminPassedStreamClientMetricNames = INTERCEPTING_ADMIN_CLIENTS.get(FIRST_INSTANCE_CONSUMER).passedMetrics.stream().map(KafkaMetric::metricName).collect(Collectors.toList());


            assertEquals(streamsThreadMetrics.size(), consumerPassedStreamThreadMetricNames.size());
            consumerPassedStreamThreadMetricNames.forEach(metricName -> assertTrue(streamsThreadMetrics.contains(metricName), "Streams metrics doesn't contain " + metricName));

            assertEquals(streamsClientMetrics.size(), adminPassedStreamClientMetricNames.size());
            adminPassedStreamClientMetricNames.forEach(metricName -> assertTrue(streamsClientMetrics.contains(metricName), "Client metrics doesn't contain " + metricName));
        }
    }

    @ParameterizedTest
    @MethodSource("multiTaskParameters")
    @DisplayName("Correct streams metrics should get passed with dynamic membership")
    public void shouldPassCorrectMetricsDynamicInstances(final boolean stateUpdaterEnabled) throws InterruptedException {
        final Properties properties1 = props(stateUpdaterEnabled);
        properties1.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(appId).getPath() + "-ks1");
        properties1.put(StreamsConfig.CLIENT_ID_CONFIG, appId + "-ks1");


        final Properties properties2 = props(stateUpdaterEnabled);
        properties2.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(appId).getPath() + "-ks2");
        properties2.put(StreamsConfig.CLIENT_ID_CONFIG, appId + "-ks2");

        /*
          This test ensures metrics are registered and removed correctly with Kafka Steams dynamic membership changes
         */

        final Topology topology = complexTopology();
        try (final KafkaStreams streamsOne = new KafkaStreams(topology, properties1)) {
            streamsOne.start();
            waitForCondition(() -> KafkaStreams.State.RUNNING == streamsOne.state(),
                    IntegrationTestUtils.DEFAULT_TIMEOUT,
                    () -> "Kafka Streams never transitioned to a RUNNING state.");

            final List<MetricName> streamsTaskMetricNames = streamsOne.metrics().values().stream().map(Metric::metricName)
                    .filter(metricName -> metricName.tags().containsKey("task-id")).collect(Collectors.toList());

            final List<MetricName> consumerPassedStreamTaskMetricNames = INTERCEPTING_CONSUMERS.get(FIRST_INSTANCE_CONSUMER).passedMetrics.stream().map(KafkaMetric::metricName)
                    .filter(metricName -> metricName.tags().containsKey("task-id")).collect(Collectors.toList());

            /*
             With only one instance, Kafka Streams should register task metrics for all tasks 0_0, 0_1, 1_0, 1_1
             */
            final List<String> streamTaskIds = getTaskIdsAsStrings(streamsOne);
            final long consumerPassedTaskMetricCount = consumerPassedStreamTaskMetricNames.stream().filter(metricName -> streamTaskIds.contains(metricName.tags().get("task-id"))).count();
            assertEquals(streamsTaskMetricNames.size(), consumerPassedStreamTaskMetricNames.size());
            assertEquals(consumerPassedTaskMetricCount, streamsTaskMetricNames.size());


            try (final KafkaStreams streamsTwo = new KafkaStreams(topology, properties2)) {
                streamsTwo.start();
                waitForCondition(() -> KafkaStreams.State.RUNNING == streamsTwo.state() && KafkaStreams.State.RUNNING == streamsOne.state(),
                        IntegrationTestUtils.DEFAULT_TIMEOUT,
                        () -> "Kafka Streams one or two never transitioned to a RUNNING state.");

                /*
                  Now with 2 instances, the tasks will get split amongst both Kafka Streams applications
                 */
                final List<String> streamOneTaskIds = getTaskIdsAsStrings(streamsOne);
                final List<String> streamTwoTasksIds = getTaskIdsAsStrings(streamsTwo);

                final List<MetricName> streamsOneTaskMetrics = streamsOne.metrics().values().stream().map(Metric::metricName)
                        .filter(metricName -> metricName.tags().containsKey("task-id")).collect(Collectors.toList());
                final List<MetricName> streamsOneStateMetrics = streamsOne.metrics().values().stream().map(Metric::metricName)
                        .filter(metricName -> metricName.group().equals("stream-state-metrics")).collect(Collectors.toList());
                
                final List<MetricName> consumerOnePassedTaskMetrics = INTERCEPTING_CONSUMERS.get(FIRST_INSTANCE_CONSUMER)
                        .passedMetrics.stream().map(KafkaMetric::metricName).filter(metricName -> metricName.tags().containsKey("task-id")).collect(Collectors.toList());
                final List<MetricName> consumerOnePassedStateMetrics = INTERCEPTING_CONSUMERS.get(FIRST_INSTANCE_CONSUMER)
                        .passedMetrics.stream().map(KafkaMetric::metricName).filter(metricName -> metricName.group().equals("stream-state-metrics")).collect(Collectors.toList());

                final List<MetricName> streamsTwoTaskMetrics = streamsTwo.metrics().values().stream().map(Metric::metricName)
                        .filter(metricName -> metricName.tags().containsKey("task-id")).collect(Collectors.toList());
                final List<MetricName> streamsTwoStateMetrics = streamsTwo.metrics().values().stream().map(Metric::metricName)
                        .filter(metricName -> metricName.group().equals("stream-state-metrics")).collect(Collectors.toList());
                
                final List<MetricName> consumerTwoPassedTaskMetrics = INTERCEPTING_CONSUMERS.get(SECOND_INSTANCE_CONSUMER)
                        .passedMetrics.stream().map(KafkaMetric::metricName).filter(metricName -> metricName.tags().containsKey("task-id")).collect(Collectors.toList());
                final List<MetricName> consumerTwoPassedStateMetrics = INTERCEPTING_CONSUMERS.get(SECOND_INSTANCE_CONSUMER)
                        .passedMetrics.stream().map(KafkaMetric::metricName).filter(metricName -> metricName.group().equals("stream-state-metrics")).collect(Collectors.toList());
                /*
                 Confirm pre-existing KafkaStreams instance one only passes metrics for its tasks and has no metrics for previous tasks
                 */
                final long consumerOneStreamOneTaskCount = consumerOnePassedTaskMetrics.stream().filter(metricName -> streamOneTaskIds.contains(metricName.tags().get("task-id"))).count();
                final long consumerOneStateMetricCount = consumerOnePassedStateMetrics.stream().filter(metricName -> streamOneTaskIds.contains(metricName.tags().get("task-id"))).count();
                final long consumerOneTaskTwoMetricCount = consumerOnePassedTaskMetrics.stream().filter(metricName -> streamTwoTasksIds.contains(metricName.tags().get("task-id"))).count();
                final long consumerOneStateTwoMetricCount = consumerOnePassedStateMetrics.stream().filter(metricName -> streamTwoTasksIds.contains(metricName.tags().get("task-id"))).count();

                /*
                  Confirm new KafkaStreams instance only passes metrics for the newly assigned tasks
                 */
                final long consumerTwoStreamTwoTaskCount = consumerTwoPassedTaskMetrics.stream().filter(metricName -> streamTwoTasksIds.contains(metricName.tags().get("task-id"))).count();
                final long consumerTwoStateMetricCount = consumerTwoPassedStateMetrics.stream().filter(metricName -> streamTwoTasksIds.contains(metricName.tags().get("task-id"))).count();
                final long consumerTwoTaskOneMetricCount = consumerTwoPassedTaskMetrics.stream().filter(metricName -> streamOneTaskIds.contains(metricName.tags().get("task-id"))).count();
                final long consumerTwoStateMetricOneCount = consumerTwoPassedStateMetrics.stream().filter(metricName -> streamOneTaskIds.contains(metricName.tags().get("task-id"))).count();

                assertEquals(streamsOneTaskMetrics.size(), consumerOneStreamOneTaskCount);
                assertEquals(streamsOneStateMetrics.size(), consumerOneStateMetricCount);
                assertEquals(0, consumerOneTaskTwoMetricCount);
                assertEquals(0, consumerOneStateTwoMetricCount);

                assertEquals(streamsTwoTaskMetrics.size(), consumerTwoStreamTwoTaskCount);
                assertEquals(streamsTwoStateMetrics.size(), consumerTwoStateMetricCount);
                assertEquals(0, consumerTwoTaskOneMetricCount);
                assertEquals(0, consumerTwoStateMetricOneCount);
            }
        }
    }

    @Test
    @DisplayName("Streams metrics should not be visible in consumer metrics")
    public void passedMetricsShouldNotLeakIntoConsumerMetrics() throws InterruptedException {
        final Properties properties = props(true);
        final Topology topology =  complexTopology();

        try (final KafkaStreams streams = new KafkaStreams(topology, properties)) {
            streams.start();
            waitForCondition(() -> KafkaStreams.State.RUNNING == streams.state(),
                    IntegrationTestUtils.DEFAULT_TIMEOUT,
                    () -> "Kafka Streams never transitioned to a RUNNING state.");

            final List<MetricName> streamsThreadMetrics = streams.metrics().values().stream().map(Metric::metricName)
                    .filter(metricName -> metricName.tags().containsKey("thread-id")).collect(Collectors.toList());

            final Map<MetricName, ? extends Metric> embeddedConsumerMetrics = INTERCEPTING_CONSUMERS.get(FIRST_INSTANCE_CONSUMER).metrics();

            streamsThreadMetrics.forEach(metricName -> assertFalse(embeddedConsumerMetrics.containsKey(metricName), "Stream thread metric found in client metrics" + metricName));
        }
    }

    private List<String> getTaskIdsAsStrings(final KafkaStreams streams) {
        return streams.metadataForLocalThreads().stream()
                .flatMap(threadMeta -> threadMeta.activeTasks().stream()
                        .map(taskMeta -> taskMeta.taskId().toString()))
                .collect(Collectors.toList());
    }

    private static Stream<Arguments> singleAndMultiTaskParameters() {
        return Stream.of(Arguments.of("simple", true),
                Arguments.of("simple", false),
                Arguments.of("complex", true),
                Arguments.of("complex", false));
    }

    private static Stream<Arguments> multiTaskParameters() {
        return Stream.of(Arguments.of(true),
                Arguments.of(false));
    }

    private Properties props(final boolean stateUpdaterEnabled) {
        return props(mkObjectProperties(mkMap(mkEntry(StreamsConfig.InternalConfig.STATE_UPDATER_ENABLED, stateUpdaterEnabled))));
    }

    private Properties props(final Properties extraProperties) {
        final Properties streamsConfiguration = new Properties();

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(appId).getPath());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_CLIENT_SUPPLIER_CONFIG, TestClientSupplier.class);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.putAll(extraProperties);

        streamsConfigurations.add(streamsConfiguration);

        return streamsConfiguration;
    }

    private Topology complexTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopicTwoPartitions, Consumed.with(Serdes.String(), Serdes.String()))
                .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
                .groupBy((key, value) -> value)
                .count()
                .toStream().to(outputTopicTwoPartitions, Produced.with(Serdes.String(), Serdes.Long()));
        return builder.build();
    }

    private Topology simpleTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopicOnePartition, Consumed.with(Serdes.String(), Serdes.String()))
                .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
                .to(outputTopicOnePartition, Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }


    public static class TestClientSupplier implements KafkaClientSupplier {

        @Override
        public Producer<byte[], byte[]> getProducer(final Map<String, Object> config) {
            return new KafkaProducer<>(config, new ByteArraySerializer(), new ByteArraySerializer());
        }

        @Override
        public Consumer<byte[], byte[]> getConsumer(final Map<String, Object> config) {
            final MetricsInterceptingConsumer<byte[], byte[]> consumer = new MetricsInterceptingConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
            INTERCEPTING_CONSUMERS.add(consumer);
            return consumer;
        }

        @Override
        public Consumer<byte[], byte[]> getRestoreConsumer(final Map<String, Object> config) {
            return new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        }

        @Override
        public Consumer<byte[], byte[]> getGlobalConsumer(final Map<String, Object> config) {
            return new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        }

        @Override
        public Admin getAdmin(final Map<String, Object> config) {
            final TestingMetricsInterceptingAdminClient adminClient = new TestingMetricsInterceptingAdminClient(config);
            INTERCEPTING_ADMIN_CLIENTS.add(adminClient);
            return adminClient;
        }
    }

    public static class MetricsInterceptingConsumer<K, V> extends KafkaConsumer<K, V> {

        public List<KafkaMetric> passedMetrics = new ArrayList<>();

        public MetricsInterceptingConsumer(final Map<String, Object> configs) {
            super(configs);
        }

        public MetricsInterceptingConsumer(final Properties properties) {
            super(properties);
        }

        public MetricsInterceptingConsumer(final Properties properties, final Deserializer<K> keyDeserializer, final Deserializer<V> valueDeserializer) {
            super(properties, keyDeserializer, valueDeserializer);
        }

        public MetricsInterceptingConsumer(final Map<String, Object> configs, final Deserializer<K> keyDeserializer, final Deserializer<V> valueDeserializer) {
            super(configs, keyDeserializer, valueDeserializer);
        }

        @Override
        public void registerMetricForSubscription(final KafkaMetric metric) {
            passedMetrics.add(metric);
            super.registerMetricForSubscription(metric);
        }

        @Override
        public void unregisterMetricFromSubscription(final KafkaMetric metric) {
            passedMetrics.remove(metric);
            super.unregisterMetricFromSubscription(metric);
        }
    }

    public static class TestingClientTelemetry implements ClientTelemetry, MetricsReporter, ClientTelemetryReceiver {
        public static final List<ClientTelemetryPayload> SUBSCRIBED_METRICS = new ArrayList<>();

        public TestingClientTelemetry() {
        }

        @Override
        public void init(final List<KafkaMetric> metrics) {
        }

        @Override
        public void metricChange(final KafkaMetric metric) {
        }

        @Override
        public void metricRemoval(final KafkaMetric metric) {
        }

        @Override
        public void close() {

        }

        @Override
        public void configure(final Map<String, ?> configs) {

        }

        @Override
        public ClientTelemetryReceiver clientReceiver() {
            return this;
        }

        @Override
        public void exportMetrics(final AuthorizableRequestContext context, final ClientTelemetryPayload payload) {
            SUBSCRIBED_METRICS.add(payload);
        }
    }

}
