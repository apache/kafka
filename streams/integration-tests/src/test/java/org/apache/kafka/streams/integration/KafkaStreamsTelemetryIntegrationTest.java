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
import org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer;
import org.apache.kafka.clients.consumer.internals.StreamsRebalanceData;
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
import org.apache.kafka.server.telemetry.ClientTelemetryExporter;
import org.apache.kafka.server.telemetry.ClientTelemetryExporterProvider;
import org.apache.kafka.shaded.io.opentelemetry.proto.metrics.v1.MetricsData;
import org.apache.kafka.streams.ClientInstanceIds;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.internals.ConsumerWrapper;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
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
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
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
    private String globalStoreTopic;
    private Uuid globalStoreConsumerInstanceId;
    private Properties streamsApplicationProperties = new Properties();
    private Properties streamsSecondApplicationProperties = new Properties();
    private  KeyValueIterator<String, String> globalStoreIterator;

    private static EmbeddedKafkaCluster cluster;
    private static final List<TestingMetricsInterceptor> INTERCEPTING_CONSUMERS = new ArrayList<>();
    private static final List<TestingMetricsInterceptingAdminClient> INTERCEPTING_ADMIN_CLIENTS = new ArrayList<>();
    private static final int NUM_BROKERS = 3;
    private static final int FIRST_INSTANCE_CLIENT = 0;
    private static final int SECOND_INSTANCE_CLIENT = 1;
    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsTelemetryIntegrationTest.class);

    static Stream<Arguments> recordingLevelParameters() {
        return Stream.of(
            Arguments.of("INFO", "classic"),
            Arguments.of("DEBUG", "classic"),
            Arguments.of("TRACE", "classic"),
            Arguments.of("INFO", "streams"),
            Arguments.of("DEBUG", "streams"),
            Arguments.of("TRACE", "streams")
        );
    }

    @BeforeAll
    public static void startCluster() throws IOException {
        final Properties properties = new Properties();
        properties.put("metric.reporters", TelemetryPluginWithExporter.class.getName());
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
        globalStoreTopic = appId + "-global-store";
        cluster.createTopic(inputTopicTwoPartitions, 2, 1);
        cluster.createTopic(outputTopicTwoPartitions, 2, 1);
        cluster.createTopic(inputTopicOnePartition, 1, 1);
        cluster.createTopic(outputTopicOnePartition, 1, 1);
        cluster.createTopic(globalStoreTopic, 2, 1);
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
        if (globalStoreIterator != null) {
            globalStoreIterator.close();
        }
    }

    @ParameterizedTest
    @MethodSource("recordingLevelParameters")
    public void shouldPushGlobalThreadMetricsToBroker(final String recordingLevel, final String groupProtocol) throws Exception {
        streamsApplicationProperties = props(groupProtocol);
        streamsApplicationProperties.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, recordingLevel);
        final Topology topology = simpleTopology(true);
        subscribeForStreamsMetrics();
        try (final KafkaStreams streams = new KafkaStreams(topology, streamsApplicationProperties)) {
            IntegrationTestUtils.startApplicationAndWaitUntilRunning(streams);
            final ClientInstanceIds clientInstanceIds = streams.clientInstanceIds(Duration.ofSeconds(60));
            for (final Map.Entry<String, Uuid> instanceId : clientInstanceIds.consumerInstanceIds().entrySet()) {
                final String instanceIdKey = instanceId.getKey();
                if (instanceIdKey.endsWith("GlobalStreamThread-global-consumer")) {
                    globalStoreConsumerInstanceId = instanceId.getValue();
                }
            }

            assertNotNull(globalStoreConsumerInstanceId);
            LOG.info("Global consumer instance id {}", globalStoreConsumerInstanceId);
            TestUtils.waitForCondition(
                    () -> !TelemetryPluginWithExporter.SUBSCRIBED_METRICS.getOrDefault(globalStoreConsumerInstanceId, Collections.emptyList()).isEmpty(),
                    30_000,
                    "Never received subscribed metrics"
            );

            final List<String> expectedGlobalMetrics = streams.metrics().values().stream().map(Metric::metricName)
                    .filter(metricName -> metricName.tags().containsKey("thread-id") &&
                            metricName.tags().get("thread-id").endsWith("-GlobalStreamThread")).map(mn -> {
                                final String name = mn.name().replace('-', '.');
                                final String group = mn.group().replace("-metrics", "").replace('-', '.');
                                return "org.apache.kafka." + group + "." + name;
                            }).filter(name -> !name.equals("org.apache.kafka.stream.thread.state"))// telemetry reporter filters out string metrics
                    .sorted().toList();
            final List<String> actualGlobalMetrics = new ArrayList<>(TelemetryPluginWithExporter.SUBSCRIBED_METRICS.get(globalStoreConsumerInstanceId));
            assertEquals(expectedGlobalMetrics, actualGlobalMetrics);
        }
    }

    @ParameterizedTest
    @MethodSource("recordingLevelParameters")
    public void shouldPushMetricsToBroker(final String recordingLevel, final String groupProtocol) throws Exception {
        // End-to-end test validating metrics pushed to broker
        streamsApplicationProperties  = props(groupProtocol);
        streamsApplicationProperties.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, recordingLevel);
        final Topology topology = simpleTopology(false);
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

            TestUtils.waitForCondition(
                () -> !TelemetryPluginWithExporter.SUBSCRIBED_METRICS.getOrDefault(mainConsumerInstanceId, Collections.emptyList()).isEmpty(),
                30_000,
                "Never received subscribed metrics"
            );
            final List<String> expectedMetrics = streams.metrics().values().stream().map(Metric::metricName)
                    .filter(metricName -> metricName.tags().containsKey("thread-id")).map(mn -> {
                        final String name = mn.name().replace('-', '.');
                        final String group = mn.group().replace("-metrics", "").replace('-', '.');
                        return "org.apache.kafka." + group + "." + name;
                    }).filter(name -> !name.equals("org.apache.kafka.stream.thread.state"))// telemetry reporter filters out string metrics
                    .sorted().toList();
            TestUtils.waitForCondition(
                () -> TelemetryPluginWithExporter.SUBSCRIBED_METRICS.get(mainConsumerInstanceId).size() == expectedMetrics.size(),
                30_000,
                "Never received enough metrics"
            );
            final List<String> actualMetrics = new ArrayList<>(TelemetryPluginWithExporter.SUBSCRIBED_METRICS.get(mainConsumerInstanceId));
            assertEquals(expectedMetrics, actualMetrics);

            TestUtils.waitForCondition(
                () -> !TelemetryPluginWithExporter.SUBSCRIBED_METRICS.getOrDefault(adminInstanceId, Collections.emptyList()).isEmpty(),
                30_000,
                "Never received subscribed metrics"
            );
            final List<String> actualInstanceMetrics = TelemetryPluginWithExporter.SUBSCRIBED_METRICS.get(adminInstanceId);
            final List<String> expectedInstanceMetrics = Arrays.asList(
                "org.apache.kafka.stream.alive.stream.threads",
                "org.apache.kafka.stream.client.state",
                "org.apache.kafka.stream.failed.stream.threads",
                "org.apache.kafka.stream.recording.level");

            assertEquals(expectedInstanceMetrics, actualInstanceMetrics);

            TestUtils.waitForCondition(() -> TelemetryPluginWithExporter.processId != null,
                    30_000,
                    "Never received the process id");

            assertEquals(expectedProcessId, TelemetryPluginWithExporter.processId);
        }
    }

    @ParameterizedTest
    @MethodSource("topologyComplexityAndRebalanceProtocol")
    public void shouldPassMetrics(final String topologyType, final String groupProtocol) throws Exception {
        // Streams metrics should get passed to Admin and Consumer
        streamsApplicationProperties = props(groupProtocol);
        final Topology topology = topologyType.equals("simple") ? simpleTopology(false) : complexTopology();

        try (final KafkaStreams streams = new KafkaStreams(topology, streamsApplicationProperties)) {
            IntegrationTestUtils.startApplicationAndWaitUntilRunning(streams);

            final List<MetricName> streamsThreadMetrics = streams.metrics().values().stream().map(Metric::metricName)
                    .filter(metricName -> metricName.tags().containsKey("thread-id")).toList();

            final List<MetricName> streamsClientMetrics = streams.metrics().values().stream().map(Metric::metricName)
                    .filter(metricName -> metricName.group().equals("stream-metrics")).toList();



            final List<MetricName> consumerPassedStreamThreadMetricNames = INTERCEPTING_CONSUMERS.get(FIRST_INSTANCE_CLIENT).passedMetrics().stream().map(KafkaMetric::metricName).toList();
            final List<MetricName> adminPassedStreamClientMetricNames = INTERCEPTING_ADMIN_CLIENTS.get(FIRST_INSTANCE_CLIENT).passedMetrics.stream().map(KafkaMetric::metricName).toList();


            assertEquals(streamsThreadMetrics.size(), consumerPassedStreamThreadMetricNames.size());
            consumerPassedStreamThreadMetricNames.forEach(metricName -> assertTrue(streamsThreadMetrics.contains(metricName), "Streams metrics doesn't contain " + metricName));

            assertEquals(streamsClientMetrics.size(), adminPassedStreamClientMetricNames.size());
            adminPassedStreamClientMetricNames.forEach(metricName -> assertTrue(streamsClientMetrics.contains(metricName), "Client metrics doesn't contain " + metricName));
        }
    }

    @Test
    public void shouldPassCorrectMetricsDynamicInstances() throws Exception {
        // Correct streams metrics should get passed with dynamic membership
        streamsApplicationProperties = props("classic");
        streamsApplicationProperties.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(appId).getPath() + "-ks1");
        streamsApplicationProperties.put(StreamsConfig.CLIENT_ID_CONFIG, appId + "-ks1");


        streamsSecondApplicationProperties = props("classic");
        streamsSecondApplicationProperties.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(appId).getPath() + "-ks2");
        streamsSecondApplicationProperties.put(StreamsConfig.CLIENT_ID_CONFIG, appId + "-ks2");


        final Topology topology = complexTopology();
        try (final KafkaStreams streamsOne = new KafkaStreams(topology, streamsApplicationProperties)) {
            IntegrationTestUtils.startApplicationAndWaitUntilRunning(streamsOne);

            final List<MetricName> streamsTaskMetricNames = streamsOne.metrics().values().stream().map(Metric::metricName)
                    .filter(metricName -> metricName.tags().containsKey("task-id")).toList();

            final List<MetricName> consumerPassedStreamTaskMetricNames = INTERCEPTING_CONSUMERS.get(FIRST_INSTANCE_CLIENT).passedMetrics().stream().map(KafkaMetric::metricName)
                    .filter(metricName -> metricName.tags().containsKey("task-id")).toList();

            /*
             With only one instance, Kafka Streams should register task metrics for all tasks 0_0, 0_1, 1_0, 1_1
             */
            final List<String> streamTaskIds = getTaskIdsAsStrings(streamsOne);
            final long consumerPassedTaskMetricCount = consumerPassedStreamTaskMetricNames.stream().filter(metricName -> streamTaskIds.contains(metricName.tags().get("task-id"))).count();
            assertEquals(streamsTaskMetricNames.size(), consumerPassedStreamTaskMetricNames.size());
            assertEquals(consumerPassedTaskMetricCount, streamsTaskMetricNames.size());

            try (final KafkaStreams streamsTwo = new KafkaStreams(topology, streamsSecondApplicationProperties)) {
                streamsTwo.start();

                /*
                  Now with 2 instances, the tasks will get split amongst both Kafka Streams applications
                 */
                final List<String> streamOneTaskIds = new ArrayList<>();
                final List<String> streamTwoTasksIds = new ArrayList<>();
                waitForCondition(() -> {
                        streamOneTaskIds.clear();
                        streamTwoTasksIds.clear();

                        streamOneTaskIds.addAll(getTaskIdsAsStrings(streamsOne));
                        streamTwoTasksIds.addAll(getTaskIdsAsStrings(streamsTwo));

                        return streamOneTaskIds.size() == 2 && streamTwoTasksIds.size() == 2;
                    },
                    "Task assignment did not complete."
                );

                final List<MetricName> streamsOneTaskMetrics = streamsOne.metrics().values().stream().map(Metric::metricName)
                        .filter(metricName -> metricName.tags().containsKey("task-id")).toList();
                final List<MetricName> streamsOneStateMetrics = streamsOne.metrics().values().stream().map(Metric::metricName)
                        .filter(metricName -> metricName.group().equals("stream-state-metrics")).toList();

                final List<MetricName> consumerOnePassedTaskMetrics = INTERCEPTING_CONSUMERS.get(FIRST_INSTANCE_CLIENT)
                        .passedMetrics().stream().map(KafkaMetric::metricName).filter(metricName -> metricName.tags().containsKey("task-id")).toList();
                final List<MetricName> consumerOnePassedStateMetrics = INTERCEPTING_CONSUMERS.get(FIRST_INSTANCE_CLIENT)
                        .passedMetrics().stream().map(KafkaMetric::metricName).filter(metricName -> metricName.group().equals("stream-state-metrics")).toList();

                final List<MetricName> streamsTwoTaskMetrics = streamsTwo.metrics().values().stream().map(Metric::metricName)
                        .filter(metricName -> metricName.tags().containsKey("task-id")).toList();
                final List<MetricName> streamsTwoStateMetrics = streamsTwo.metrics().values().stream().map(Metric::metricName)
                        .filter(metricName -> metricName.group().equals("stream-state-metrics")).toList();

                final List<MetricName> consumerTwoPassedTaskMetrics = INTERCEPTING_CONSUMERS.get(SECOND_INSTANCE_CLIENT)
                        .passedMetrics().stream().map(KafkaMetric::metricName).filter(metricName -> metricName.tags().containsKey("task-id")).toList();
                final List<MetricName> consumerTwoPassedStateMetrics = INTERCEPTING_CONSUMERS.get(SECOND_INSTANCE_CLIENT)
                        .passedMetrics().stream().map(KafkaMetric::metricName).filter(metricName -> metricName.group().equals("stream-state-metrics")).toList();
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

    @ParameterizedTest
    @ValueSource(strings = {"classic", "streams"})
    public void passedMetricsShouldNotLeakIntoClientMetrics(final String groupProtocol) throws Exception {
        // Streams metrics should not be visible in client metrics
        streamsApplicationProperties = props(groupProtocol);
        final Topology topology =  complexTopology();

        try (final KafkaStreams streams = new KafkaStreams(topology, streamsApplicationProperties)) {
            IntegrationTestUtils.startApplicationAndWaitUntilRunning(streams);

            final List<MetricName> streamsThreadMetrics = streams.metrics().values().stream().map(Metric::metricName)
                    .filter(metricName -> metricName.tags().containsKey("thread-id")).toList();

            final List<MetricName> streamsClientMetrics = streams.metrics().values().stream().map(Metric::metricName)
                    .filter(metricName -> metricName.group().equals("stream-metrics")).toList();

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
                .toList();
    }

    private static Stream<Arguments> topologyComplexityAndRebalanceProtocol() {
        return Stream.of(
            Arguments.of("simple", "classic"),
            Arguments.of("complex", "classic"),
            Arguments.of("simple", "streams")
        );
    }

    private Properties props(final String groupProtocol) {
        return props(mkObjectProperties(mkMap(
            mkEntry(StreamsConfig.GROUP_PROTOCOL_CONFIG, groupProtocol)
        )));
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
        streamsConfiguration.put(StreamsConfig.InternalConfig.INTERNAL_CONSUMER_WRAPPER, TestConsumerWrapper.class);
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


    private void addGlobalStore(final StreamsBuilder builder) {
        builder.addGlobalStore(
            Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("iq-test-store"),
                Serdes.String(),
                Serdes.String()
        ),
                globalStoreTopic,
                Consumed.with(Serdes.String(), Serdes.String()),
                () -> new Processor<>() {

                    // The store iterator is intentionally not closed here as it needs
                    // to be open during the test, so the Streams app will emit the
                    // org.apache.kafka.stream.state.oldest.iterator.open.since.ms metric
                    // that is expected. So the globalStoreIterator is a global variable
                    // (pun not intended), so it can be closed in the tearDown method.
                    @SuppressWarnings("unchecked")
                    @Override
                    public void init(final ProcessorContext<Void, Void> context) {
                        globalStoreIterator = ((KeyValueStore<String, String>) context.getStateStore("iq-test-store")).all();
                    }

                    @Override
                    public void process(final Record<String, String> record) {
                        // no-op
                    }
                });
    }

    private Topology simpleTopology(final boolean includeGlobalStore) {
        final StreamsBuilder builder = new StreamsBuilder();
        if (includeGlobalStore) {
            addGlobalStore(builder);
        }
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
            return new TestingMetricsInterceptingConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        }

        @Override
        public Admin getAdmin(final Map<String, Object> config) {
            assertTrue((Boolean) config.get(AdminClientConfig.ENABLE_METRICS_PUSH_CONFIG));
            final TestingMetricsInterceptingAdminClient adminClient = new TestingMetricsInterceptingAdminClient(config);
            INTERCEPTING_ADMIN_CLIENTS.add(adminClient);
            return adminClient;
        }
    }

    public static class TestConsumerWrapper extends ConsumerWrapper {
        @Override
        public void wrapConsumer(final AsyncKafkaConsumer<byte[], byte[]> delegate, final Map<String, Object> config, final Optional<StreamsRebalanceData> streamsRebalanceData) {
            final TestingMetricsInterceptingAsyncConsumer<byte[], byte[]> consumer = new TestingMetricsInterceptingAsyncConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer(), streamsRebalanceData);
            INTERCEPTING_CONSUMERS.add(consumer);

            super.wrapConsumer(consumer, config, streamsRebalanceData);
        }
    }

    public interface TestingMetricsInterceptor {
        List<KafkaMetric> passedMetrics();
        Map<MetricName, ? extends Metric> metrics();
    }

    public static class TestingMetricsInterceptingConsumer<K, V> extends KafkaConsumer<K, V> implements TestingMetricsInterceptor {
        private final List<KafkaMetric> passedMetrics = new ArrayList<>();

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

        @Override
        public List<KafkaMetric> passedMetrics() {
            return passedMetrics;
        }
    }

    public static class TestingMetricsInterceptingAsyncConsumer<K, V> extends AsyncKafkaConsumer<K, V> implements TestingMetricsInterceptor {
        private final List<KafkaMetric> passedMetrics = new ArrayList<>();

        public TestingMetricsInterceptingAsyncConsumer(
            final Map<String, Object> configs,
            final Deserializer<K> keyDeserializer,
            final Deserializer<V> valueDeserializer,
            final Optional<StreamsRebalanceData> streamsRebalanceData
        ) {
            super(
                new ConsumerConfig(
                    ConsumerConfig.appendDeserializerToConfig(configs, keyDeserializer, valueDeserializer)
                ),
                keyDeserializer,
                valueDeserializer,
                streamsRebalanceData
            );
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

        @Override
        public List<KafkaMetric> passedMetrics() {
            return passedMetrics;
        }
    }

    public static class TelemetryPluginWithExporter implements ClientTelemetryExporterProvider, MetricsReporter {

        public static final Map<Uuid, List<String>> SUBSCRIBED_METRICS = new ConcurrentHashMap<>();
        public static String processId;

        public TelemetryPluginWithExporter() {
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
        public ClientTelemetryExporter clientTelemetryExporter() {
            return (context, payload) -> {
                try {
                    final MetricsData data = MetricsData.parseFrom(payload.data());

                    final Optional<String> processIdOption = data.getResourceMetricsList()
                            .stream()
                            .flatMap(rm -> rm.getScopeMetricsList().stream())
                            .flatMap(sm -> sm.getMetricsList().stream())
                            .map(org.apache.kafka.shaded.io.opentelemetry.proto.metrics.v1.Metric::getGauge)
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
                            .map(org.apache.kafka.shaded.io.opentelemetry.proto.metrics.v1.Metric::getName)
                            .sorted()
                            .toList();
                    LOG.info("Found metrics {} for clientId={} with pushIntervalMs={}", metricNames, clientId, context.pushIntervalMs());
                    SUBSCRIBED_METRICS.put(clientId, metricNames);
                } catch (final Exception e) {
                    e.printStackTrace(System.err);
                }
            };
        }
    }
}
