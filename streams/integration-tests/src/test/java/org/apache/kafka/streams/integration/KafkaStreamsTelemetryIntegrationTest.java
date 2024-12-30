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
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.telemetry.ClientTelemetry;
import org.apache.kafka.server.telemetry.ClientTelemetryPayload;
import org.apache.kafka.server.telemetry.ClientTelemetryReceiver;
import org.apache.kafka.shaded.io.opentelemetry.proto.metrics.v1.MetricsData;
import org.apache.kafka.streams.ClientInstanceIds;
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
import org.apache.kafka.tools.ClientMetricsCommand;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkObjectProperties;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(600)
@Tag("integration")
public class KafkaStreamsTelemetryIntegrationTest {
    private String appId;
    private String inputTopicTwoPartitions;
    private String outputTopicTwoPartitions;
    private String inputTopicOnePartition;
    private String outputTopicOnePartition;
    private Properties streamsApplicationProperties = new Properties();
    private Properties streamsSecondApplicationProperties = new Properties();

    private static EmbeddedKafkaCluster cluster;
    private static final List<TestingMetricsInterceptingConsumer<byte[], byte[]>> INTERCEPTING_CONSUMERS = new ArrayList<>();
    private static final List<TestingMetricsInterceptingAdminClient> INTERCEPTING_ADMIN_CLIENTS = new ArrayList<>();
    private static final int NUM_BROKERS = 3;
    private static final int FIRST_INSTANCE_CLIENT = 0;
    private static final int SECOND_INSTANCE_CLIENT = 1;
    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsTelemetryIntegrationTest.class);


    @BeforeAll
    public static void startCluster() throws IOException {
        final Properties properties = new Properties();
        properties.put("metric.reporters", TelemetryPlugin.class.getName());
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
        IntegrationTestUtils.purgeLocalStreamsState(streamsApplicationProperties);
        if (!streamsSecondApplicationProperties.isEmpty()) {
            IntegrationTestUtils.purgeLocalStreamsState(streamsSecondApplicationProperties);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INFO", "DEBUG", "TRACE"})
    public void shouldPushMetricsToBroker(final String recordingLevel) throws Exception {
        // End-to-end test validating metrics pushed to broker
        streamsApplicationProperties  = props(true);
        streamsApplicationProperties.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, recordingLevel);
        final Topology topology = simpleTopology();
        subscribeForStreamsMetrics();
        try (final KafkaStreams streams = new KafkaStreams(topology, streamsApplicationProperties)) {
            IntegrationTestUtils.startApplicationAndWaitUntilRunning(streams);
            final ClientInstanceIds clientInstanceIds = streams.clientInstanceIds(Duration.ofSeconds(60));
            final Uuid adminInstanceId = clientInstanceIds.adminInstanceId();
            
            final Uuid mainConsumerInstanceId = clientInstanceIds.consumerInstanceIds().entrySet().stream()
                    .filter(entry -> !entry.getKey().endsWith("-restore-consumer")
                            && !entry.getKey().endsWith("GlobalStreamThread-global-consumer"))
                    .map(Map.Entry::getValue)
                    .findFirst().orElseThrow();
            assertNotNull(adminInstanceId);
            assertNotNull(mainConsumerInstanceId);
            LOG.info("Main consumer instance id {}", mainConsumerInstanceId);

            final String expectedProcessId = streams.metrics().values().stream()
                    .filter(metric -> metric.metricName().tags().containsKey("process-id"))
                    .map(metric -> metric.metricName().tags().get("process-id"))
                    .findFirst().orElseThrow();

            TestUtils.waitForCondition(() -> !TelemetryPlugin.SUBSCRIBED_METRICS.get(mainConsumerInstanceId).isEmpty(),
                    30_000,
                    "Never received subscribed metrics");
            final List<String> expectedMetrics = streams.metrics().values().stream().map(Metric::metricName)
                    .filter(metricName -> metricName.tags().containsKey("thread-id")).map(mn -> {
                        final String name = mn.name().replace('-', '.');
                        final String group = mn.group().replace("-metrics", "").replace('-', '.');
                        return "org.apache.kafka." + group + "." + name;
                    }).filter(name -> !name.equals("org.apache.kafka.stream.thread.state"))// telemetry reporter filters out string metrics
                    .sorted().collect(Collectors.toList());
            final List<String> actualMetrics = new ArrayList<>(TelemetryPlugin.SUBSCRIBED_METRICS.get(mainConsumerInstanceId));
            assertEquals(expectedMetrics, actualMetrics);

            TestUtils.waitForCondition(() -> !TelemetryPlugin.SUBSCRIBED_METRICS.get(adminInstanceId).isEmpty(),
                    30_000,
                    "Never received subscribed metrics");
            final List<String> actualInstanceMetrics = TelemetryPlugin.SUBSCRIBED_METRICS.get(adminInstanceId);
            final List<String> expectedInstanceMetrics = Arrays.asList(
                "org.apache.kafka.stream.alive.stream.threads",
                "org.apache.kafka.stream.client.state",
                "org.apache.kafka.stream.failed.stream.threads",
                "org.apache.kafka.stream.recording.level");
            
            assertEquals(expectedInstanceMetrics, actualInstanceMetrics);

            TestUtils.waitForCondition(() -> TelemetryPlugin.processId != null,
                    30_000,
                    "Never received the process id");

            assertEquals(expectedProcessId, TelemetryPlugin.processId);
        }
    }

    @ParameterizedTest
    @MethodSource("singleAndMultiTaskParameters")
    public void shouldPassMetrics(final String topologyType, final boolean stateUpdaterEnabled) throws Exception {
        // Streams metrics should get passed to Admin and Consumer
        streamsApplicationProperties = props(stateUpdaterEnabled);
        final Topology topology = topologyType.equals("simple") ? simpleTopology() : complexTopology();
       
        try (final KafkaStreams streams = new KafkaStreams(topology, streamsApplicationProperties)) {
            IntegrationTestUtils.startApplicationAndWaitUntilRunning(streams);

            final List<MetricName> streamsThreadMetrics = streams.metrics().values().stream().map(Metric::metricName)
                    .filter(metricName -> metricName.tags().containsKey("thread-id")).collect(Collectors.toList());

            final List<MetricName> streamsClientMetrics = streams.metrics().values().stream().map(Metric::metricName)
                    .filter(metricName -> metricName.group().equals("stream-metrics")).collect(Collectors.toList());



            final List<MetricName> consumerPassedStreamThreadMetricNames = INTERCEPTING_CONSUMERS.get(FIRST_INSTANCE_CLIENT).passedMetrics.stream().map(KafkaMetric::metricName).collect(Collectors.toList());
            final List<MetricName> adminPassedStreamClientMetricNames = INTERCEPTING_ADMIN_CLIENTS.get(FIRST_INSTANCE_CLIENT).passedMetrics.stream().map(KafkaMetric::metricName).collect(Collectors.toList());


            assertEquals(streamsThreadMetrics.size(), consumerPassedStreamThreadMetricNames.size());
            consumerPassedStreamThreadMetricNames.forEach(metricName -> assertTrue(streamsThreadMetrics.contains(metricName), "Streams metrics doesn't contain " + metricName));

            assertEquals(streamsClientMetrics.size(), adminPassedStreamClientMetricNames.size());
            adminPassedStreamClientMetricNames.forEach(metricName -> assertTrue(streamsClientMetrics.contains(metricName), "Client metrics doesn't contain " + metricName));
        }
    }

    @ParameterizedTest
    @MethodSource("multiTaskParameters")
    public void shouldPassCorrectMetricsDynamicInstances(final boolean stateUpdaterEnabled) throws Exception {
        // Correct streams metrics should get passed with dynamic membership
        streamsApplicationProperties = props(stateUpdaterEnabled);
        streamsApplicationProperties.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(appId).getPath() + "-ks1");
        streamsApplicationProperties.put(StreamsConfig.CLIENT_ID_CONFIG, appId + "-ks1");


        streamsSecondApplicationProperties = props(stateUpdaterEnabled);
        streamsSecondApplicationProperties.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(appId).getPath() + "-ks2");
        streamsSecondApplicationProperties.put(StreamsConfig.CLIENT_ID_CONFIG, appId + "-ks2");
        

        final Topology topology = complexTopology();
        try (final KafkaStreams streamsOne = new KafkaStreams(topology, streamsApplicationProperties)) {
            IntegrationTestUtils.startApplicationAndWaitUntilRunning(streamsOne);

            final List<MetricName> streamsTaskMetricNames = streamsOne.metrics().values().stream().map(Metric::metricName)
                    .filter(metricName -> metricName.tags().containsKey("task-id")).collect(Collectors.toList());

            final List<MetricName> consumerPassedStreamTaskMetricNames = INTERCEPTING_CONSUMERS.get(FIRST_INSTANCE_CLIENT).passedMetrics.stream().map(KafkaMetric::metricName)
                    .filter(metricName -> metricName.tags().containsKey("task-id")).collect(Collectors.toList());

            /*
             With only one instance, Kafka Streams should register task metrics for all tasks 0_0, 0_1, 1_0, 1_1
             */
            final List<String> streamTaskIds = getTaskIdsAsStrings(streamsOne);
            final long consumerPassedTaskMetricCount = consumerPassedStreamTaskMetricNames.stream().filter(metricName -> streamTaskIds.contains(metricName.tags().get("task-id"))).count();
            assertEquals(streamsTaskMetricNames.size(), consumerPassedStreamTaskMetricNames.size());
            assertEquals(consumerPassedTaskMetricCount, streamsTaskMetricNames.size());


            try (final KafkaStreams streamsTwo = new KafkaStreams(topology, streamsSecondApplicationProperties)) {
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
                
                final List<MetricName> consumerOnePassedTaskMetrics = INTERCEPTING_CONSUMERS.get(FIRST_INSTANCE_CLIENT)
                        .passedMetrics.stream().map(KafkaMetric::metricName).filter(metricName -> metricName.tags().containsKey("task-id")).collect(Collectors.toList());
                final List<MetricName> consumerOnePassedStateMetrics = INTERCEPTING_CONSUMERS.get(FIRST_INSTANCE_CLIENT)
                        .passedMetrics.stream().map(KafkaMetric::metricName).filter(metricName -> metricName.group().equals("stream-state-metrics")).collect(Collectors.toList());

                final List<MetricName> streamsTwoTaskMetrics = streamsTwo.metrics().values().stream().map(Metric::metricName)
                        .filter(metricName -> metricName.tags().containsKey("task-id")).collect(Collectors.toList());
                final List<MetricName> streamsTwoStateMetrics = streamsTwo.metrics().values().stream().map(Metric::metricName)
                        .filter(metricName -> metricName.group().equals("stream-state-metrics")).collect(Collectors.toList());
                
                final List<MetricName> consumerTwoPassedTaskMetrics = INTERCEPTING_CONSUMERS.get(SECOND_INSTANCE_CLIENT)
                        .passedMetrics.stream().map(KafkaMetric::metricName).filter(metricName -> metricName.tags().containsKey("task-id")).collect(Collectors.toList());
                final List<MetricName> consumerTwoPassedStateMetrics = INTERCEPTING_CONSUMERS.get(SECOND_INSTANCE_CLIENT)
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
    public void passedMetricsShouldNotLeakIntoClientMetrics() throws Exception {
        // Streams metrics should not be visible in client metrics
        streamsApplicationProperties = props(true);
        final Topology topology =  complexTopology();

        try (final KafkaStreams streams = new KafkaStreams(topology, streamsApplicationProperties)) {
            IntegrationTestUtils.startApplicationAndWaitUntilRunning(streams);

            final List<MetricName> streamsThreadMetrics = streams.metrics().values().stream().map(Metric::metricName)
                    .filter(metricName -> metricName.tags().containsKey("thread-id")).collect(Collectors.toList());

            final List<MetricName> streamsClientMetrics = streams.metrics().values().stream().map(Metric::metricName)
                    .filter(metricName -> metricName.group().equals("stream-metrics")).collect(Collectors.toList());

            final Map<MetricName, ? extends Metric> embeddedConsumerMetrics = INTERCEPTING_CONSUMERS.get(FIRST_INSTANCE_CLIENT).metrics();
            final Map<MetricName, ? extends Metric> embeddedAdminMetrics = INTERCEPTING_ADMIN_CLIENTS.get(FIRST_INSTANCE_CLIENT).metrics();

            streamsThreadMetrics.forEach(metricName -> assertFalse(embeddedConsumerMetrics.containsKey(metricName), "Stream thread metric found in client metrics" + metricName));
            streamsClientMetrics.forEach(metricName -> assertFalse(embeddedAdminMetrics.containsKey(metricName), "Stream client metric found in client metrics" + metricName));
        }
    }

    private void subscribeForStreamsMetrics() throws Exception {
        final Properties clientProps = new Properties();
        clientProps.put("bootstrap.servers", cluster.bootstrapServers());
        try (final ClientMetricsCommand.ClientMetricsService clientMetricsService = new ClientMetricsCommand.ClientMetricsService(clientProps)) {
            final String[] metricsSubscriptionParameters = new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--metrics", "org.apache.kafka.stream", "--alter", "--name", "streams-task-metrics-subscription", "--interval", "1000"};
            final ClientMetricsCommand.ClientMetricsCommandOptions commandOptions = new ClientMetricsCommand.ClientMetricsCommandOptions(metricsSubscriptionParameters);
            clientMetricsService.alterClientMetrics(commandOptions);
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
            final TestingMetricsInterceptingConsumer<byte[], byte[]> consumer = new TestingMetricsInterceptingConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
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
            assertTrue((Boolean) config.get(AdminClientConfig.ENABLE_METRICS_PUSH_CONFIG));
            final TestingMetricsInterceptingAdminClient adminClient = new TestingMetricsInterceptingAdminClient(config);
            INTERCEPTING_ADMIN_CLIENTS.add(adminClient);
            return adminClient;
        }
    }

    public static class TestingMetricsInterceptingConsumer<K, V> extends KafkaConsumer<K, V> {

        public List<KafkaMetric> passedMetrics = new ArrayList<>();

        public TestingMetricsInterceptingConsumer(final Map<String, Object> configs, final Deserializer<K> keyDeserializer, final Deserializer<V> valueDeserializer) {
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

    public static class TelemetryPlugin implements ClientTelemetry, MetricsReporter, ClientTelemetryReceiver {

        public static final Map<Uuid, List<String>> SUBSCRIBED_METRICS = new ConcurrentHashMap<>();
        public static String processId;
        public TelemetryPlugin() {
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
            try {
                final MetricsData data = MetricsData.parseFrom(payload.data());
                
                final Optional<String> processIdOption = data.getResourceMetricsList()
                        .stream()
                        .flatMap(rm -> rm.getScopeMetricsList().stream())
                        .flatMap(sm -> sm.getMetricsList().stream())
                        .map(metric -> metric.getGauge())
                        .flatMap(gauge -> gauge.getDataPointsList().stream())
                        .flatMap(numberDataPoint -> numberDataPoint.getAttributesList().stream())
                        .filter(keyValue -> keyValue.getKey().equals("process_id"))
                        .map(keyValue -> keyValue.getValue().getStringValue())
                        .findFirst();

                processIdOption.ifPresent(pid -> processId = pid);

                final Uuid clientId = payload.clientInstanceId();
                final List<String> metricNames = data.getResourceMetricsList()
                        .stream()
                        .flatMap(rm -> rm.getScopeMetricsList().stream())
                        .flatMap(sm -> sm.getMetricsList().stream())
                        .map(metric -> metric.getName())
                        .sorted()
                        .collect(Collectors.toList());
                LOG.info("Found metrics {} for clientId={}", metricNames, clientId);
                SUBSCRIBED_METRICS.put(clientId, metricNames);
            } catch (final Exception e) {
                e.printStackTrace(System.err);
            }
        }
    }
}
