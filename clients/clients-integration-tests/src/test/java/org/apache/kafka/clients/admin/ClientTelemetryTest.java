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

package org.apache.kafka.clients.admin;

import kafka.server.BrokerServer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.server.metrics.ClientMetricsConfigs;
import org.apache.kafka.server.telemetry.ClientTelemetryExporter;
import org.apache.kafka.server.telemetry.ClientTelemetryExporterProvider;
import org.apache.kafka.shaded.io.opentelemetry.proto.metrics.v1.Metric;
import org.apache.kafka.shaded.io.opentelemetry.proto.metrics.v1.MetricsData;
import org.apache.kafka.test.TestUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.admin.AdminClientConfig.METRIC_REPORTER_CLASSES_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClientTelemetryTest {

    private static final String TELEMETRY_EXPORTER = "org.apache.kafka.clients.admin.ClientTelemetryTest$TelemetryExporter";
    private static final String TOPIC = "client-metrics-topic";
    private static final int PUSH_INTERVAL_MS = 1000;
    private static final String PRODUCER_METRICS_PREFIX = "org.apache.kafka.producer.";
    private static final String CONSUMER_METRICS_PREFIX = "org.apache.kafka.consumer.";
    private static final String ADMIN_METRICS_PREFIX = "org.apache.kafka.admin.client.";

    @ClusterTest(
            types = Type.KRAFT,
            brokers = 3,
            serverProperties = {
                @ClusterConfigProperty(key = METRIC_REPORTER_CLASSES_CONFIG, value = TELEMETRY_EXPORTER),
            })
    public void testClientInstanceId(ClusterInstance clusterInstance) throws InterruptedException, ExecutionException {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        configs.put(AdminClientConfig.ENABLE_METRICS_PUSH_CONFIG, true);
        try (Admin admin = Admin.create(configs)) {
            String testTopicName = "test_topic";
            admin.createTopics(Collections.singletonList(new NewTopic(testTopicName, 1, (short) 1)));
            clusterInstance.waitTopicCreation(testTopicName, 1);

            Map<String, Object> producerConfigs = new HashMap<>();
            producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
            producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            try (Producer<String, String> producer = new KafkaProducer<>(producerConfigs)) {
                producer.send(new ProducerRecord<>(testTopicName, 0, null, "bar")).get();
                producer.flush();
                Uuid producerClientId = producer.clientInstanceId(Duration.ofSeconds(3));
                assertNotNull(producerClientId);
                assertEquals(producerClientId, producer.clientInstanceId(Duration.ofSeconds(3)));
            }

            Map<String, Object> consumerConfigs = new HashMap<>();
            consumerConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
            consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
            consumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerConfigs)) {
                consumer.assign(Collections.singletonList(new TopicPartition(testTopicName, 0)));
                consumer.seekToBeginning(Collections.singletonList(new TopicPartition(testTopicName, 0)));
                Uuid consumerClientId = consumer.clientInstanceId(Duration.ofSeconds(5));
                //  before poll, the clientInstanceId will return null
                assertNull(consumerClientId);
                List<String> values = new ArrayList<>();
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    values.add(record.value());
                }
                assertEquals(1, values.size());
                assertEquals("bar", values.get(0));
                consumerClientId = consumer.clientInstanceId(Duration.ofSeconds(3));
                assertNotNull(consumerClientId);
                assertEquals(consumerClientId, consumer.clientInstanceId(Duration.ofSeconds(3)));
            }
            Uuid uuid = admin.clientInstanceId(Duration.ofSeconds(3));
            assertNotNull(uuid);
            assertEquals(uuid, admin.clientInstanceId(Duration.ofSeconds(3)));
        }
    }

    @ClusterTest(types = Type.KRAFT)
    public void testMetrics(ClusterInstance clusterInstance) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        List<String> expectedMetricsName = Arrays.asList("request-size-max", "io-wait-ratio", "response-total",
                "version", "io-time-ns-avg", "network-io-rate");
        try (Admin admin = Admin.create(configs)) {
            Set<String> actualMetricsName = admin.metrics().keySet().stream()
                    .map(MetricName::name)
                    .collect(Collectors.toSet());
            expectedMetricsName.forEach(expectedName -> assertTrue(actualMetricsName.contains(expectedName),
                    String.format("actual metrics name: %s dont contains expected: %s", actualMetricsName,
                            expectedName)));
            assertTrue(actualMetricsName.containsAll(expectedMetricsName));
        }
    }

    @ClusterTest(
            types = Type.KRAFT,
            serverProperties = {
                @ClusterConfigProperty(key = METRIC_REPORTER_CLASSES_CONFIG, value = TELEMETRY_EXPORTER),
            })
    public void testProducerMetricsPush(ClusterInstance clusterInstance) throws Exception {
        clusterInstance.createTopic(TOPIC, 1, (short) 1);
        createSubscription(clusterInstance, "producer-metrics", PRODUCER_METRICS_PREFIX, Optional.empty());

        Uuid clientInstanceId;
        try (Producer<byte[], byte[]> producer = clusterInstance.producer()) {
            producer.send(new ProducerRecord<>(TOPIC, "value".getBytes())).get();
            clientInstanceId = producer.clientInstanceId(Duration.ofSeconds(10));
            assertNotNull(clientInstanceId);

            // Wait for two pushes so that the regular push interval, not just the initial push, is exercised.
            List<PushedMetrics> pushes = waitForPushes(clientInstanceId, 2);
            PushedMetrics push = pushes.get(0);
            assertEquals(PUSH_INTERVAL_MS, push.pushIntervalMs());
            assertEquals("OTLP", push.contentType());
            assertFalse(push.terminating());
            assertOnlyMetricsWithPrefix(push, PRODUCER_METRICS_PREFIX);
            assertTrue(push.metricNames().contains("org.apache.kafka.producer.record.send.total"),
                "Expected producer metrics in " + push.metricNames());
            assertTrue(push.metricNames().contains("org.apache.kafka.producer.topic.record.send.total"),
                "Expected per-topic producer metrics in " + push.metricNames());
        }

        // Closing the producer makes a final, terminating push.
        waitForTerminatingPush(clientInstanceId);
    }

    @ClusterTest(
            types = Type.KRAFT,
            serverProperties = {
                @ClusterConfigProperty(key = METRIC_REPORTER_CLASSES_CONFIG, value = TELEMETRY_EXPORTER),
                @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
            })
    public void testClassicConsumerMetricsPush(ClusterInstance clusterInstance) throws Exception {
        consumerMetricsPush(clusterInstance, GroupProtocol.CLASSIC);
    }

    @ClusterTest(
            types = Type.KRAFT,
            serverProperties = {
                @ClusterConfigProperty(key = METRIC_REPORTER_CLASSES_CONFIG, value = TELEMETRY_EXPORTER),
                @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
            })
    public void testConsumerMetricsPush(ClusterInstance clusterInstance) throws Exception {
        consumerMetricsPush(clusterInstance, GroupProtocol.CONSUMER);
    }

    private void consumerMetricsPush(ClusterInstance clusterInstance, GroupProtocol groupProtocol) throws Exception {
        clusterInstance.createTopic(TOPIC, 1, (short) 1);
        try (Producer<byte[], byte[]> producer = clusterInstance.producer()) {
            producer.send(new ProducerRecord<>(TOPIC, "value".getBytes())).get();
        }
        createSubscription(clusterInstance, "consumer-metrics", CONSUMER_METRICS_PREFIX, Optional.empty());

        Uuid clientInstanceId;
        try (Consumer<byte[], byte[]> consumer = clusterInstance.consumer(Map.of(
            ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name))) {
            TopicPartition tp = new TopicPartition(TOPIC, 0);
            consumer.assign(List.of(tp));
            consumer.seekToBeginning(List.of(tp));
            TestUtils.waitForCondition(() -> consumer.poll(Duration.ofMillis(100)).count() == 1, 10_000, "Failed to poll data.");
            clientInstanceId = consumer.clientInstanceId(Duration.ofSeconds(10));
            assertNotNull(clientInstanceId);

            // The classic consumer only performs network I/O, including telemetry pushes, while the application
            // is polling, so keep polling while waiting for the pushes to arrive.
            List<PushedMetrics> pushes = waitForPushes(clientInstanceId, 2, () -> consumer.poll(Duration.ofMillis(100)));
            PushedMetrics push = pushes.get(0);
            assertEquals(PUSH_INTERVAL_MS, push.pushIntervalMs());
            assertFalse(push.terminating());
            assertOnlyMetricsWithPrefix(push, CONSUMER_METRICS_PREFIX);
            assertTrue(push.metricNames().contains("org.apache.kafka.consumer.fetch.manager.records.consumed.total"),
                "Expected consumer fetch metrics in " + push.metricNames());
        }

        // Closing the consumer makes a final, terminating push.
        waitForTerminatingPush(clientInstanceId);
    }

    @ClusterTest(
            types = Type.KRAFT,
            serverProperties = {
                @ClusterConfigProperty(key = METRIC_REPORTER_CLASSES_CONFIG, value = TELEMETRY_EXPORTER),
            })
    public void testAdminMetricsPush(ClusterInstance clusterInstance) throws Exception {
        createSubscription(clusterInstance, "admin-metrics", ADMIN_METRICS_PREFIX, Optional.empty());

        Uuid clientInstanceId;
        try (Admin admin = clusterInstance.admin(Map.of(AdminClientConfig.ENABLE_METRICS_PUSH_CONFIG, true))) {
            admin.describeCluster().clusterId().get();
            clientInstanceId = admin.clientInstanceId(Duration.ofSeconds(10));
            assertNotNull(clientInstanceId);

            List<PushedMetrics> pushes = waitForPushes(clientInstanceId, 2);
            PushedMetrics push = pushes.get(0);
            assertEquals(PUSH_INTERVAL_MS, push.pushIntervalMs());
            assertFalse(push.terminating());
            assertOnlyMetricsWithPrefix(push, ADMIN_METRICS_PREFIX);
            assertTrue(push.metricNames().contains("org.apache.kafka.admin.client.connection.count"),
                "Expected admin client metrics in " + push.metricNames());
        }

        // Closing the admin client makes a final, terminating push.
        waitForTerminatingPush(clientInstanceId);
    }

    @ClusterTest(
            types = Type.KRAFT,
            serverProperties = {
                @ClusterConfigProperty(key = METRIC_REPORTER_CLASSES_CONFIG, value = TELEMETRY_EXPORTER),
            })
    public void testSubscriptionMatchSelectsClients(ClusterInstance clusterInstance) throws Exception {
        clusterInstance.createTopic(TOPIC, 1, (short) 1);
        createSubscription(clusterInstance, "matched-producer-metrics", PRODUCER_METRICS_PREFIX,
            Optional.of(ClientMetricsConfigs.CLIENT_ID + "=matched.*"));

        try (Producer<byte[], byte[]> matchedProducer = clusterInstance.producer(Map.of(ProducerConfig.CLIENT_ID_CONFIG, "matched-producer"));
             Producer<byte[], byte[]> otherProducer = clusterInstance.producer(Map.of(ProducerConfig.CLIENT_ID_CONFIG, "other-producer"))) {
            matchedProducer.send(new ProducerRecord<>(TOPIC, "value".getBytes())).get();
            otherProducer.send(new ProducerRecord<>(TOPIC, "value".getBytes())).get();

            // Both clients register with the broker and are assigned a client instance ID, but only the one
            // matching the subscription is asked for any metrics and so only that one pushes.
            Uuid matchedInstanceId = matchedProducer.clientInstanceId(Duration.ofSeconds(10));
            Uuid otherInstanceId = otherProducer.clientInstanceId(Duration.ofSeconds(10));
            assertNotNull(matchedInstanceId);
            assertNotNull(otherInstanceId);

            List<PushedMetrics> pushes = waitForPushes(matchedInstanceId, 2);
            assertOnlyMetricsWithPrefix(pushes.get(0), PRODUCER_METRICS_PREFIX);
            assertTrue(TelemetryExporter.pushes(otherInstanceId).isEmpty(),
                "Unexpected metrics pushed by a client which does not match the subscription");
        }
    }

    /**
     * Creates a client metrics subscription and waits until every broker has applied it, so that the clients
     * created afterwards receive it when they first ask the broker for their subscription.
     */
    private static void createSubscription(ClusterInstance clusterInstance, String name, String metricsPrefix, Optional<String> match)
        throws ExecutionException, InterruptedException {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, name);
        List<AlterConfigOp> ops = new ArrayList<>();
        ops.add(new AlterConfigOp(new ConfigEntry(ClientMetricsConfigs.METRICS_CONFIG, metricsPrefix), AlterConfigOp.OpType.SET));
        ops.add(new AlterConfigOp(new ConfigEntry(ClientMetricsConfigs.INTERVAL_MS_CONFIG, String.valueOf(PUSH_INTERVAL_MS)), AlterConfigOp.OpType.SET));
        match.ifPresent(m -> ops.add(new AlterConfigOp(new ConfigEntry(ClientMetricsConfigs.MATCH_CONFIG, m), AlterConfigOp.OpType.SET)));

        try (Admin admin = clusterInstance.admin()) {
            admin.incrementalAlterConfigs(Map.of(resource, ops)).all().get();
        }

        TestUtils.waitForCondition(() -> clusterInstance.brokers().values().stream()
                .allMatch(broker -> ((BrokerServer) broker).clientMetricsManager().listClientMetricsResources().contains(name)),
            "Client metrics subscription " + name + " was not propagated to all brokers");
    }

    private static List<PushedMetrics> waitForPushes(Uuid clientInstanceId, int count) throws InterruptedException {
        return waitForPushes(clientInstanceId, count, () -> { });
    }

    /**
     * Waits until the broker-side exporter has received at least {@code count} pushes from the client, running
     * {@code keepAlive} between checks for clients which need the application to drive their network I/O.
     */
    private static List<PushedMetrics> waitForPushes(Uuid clientInstanceId, int count, Runnable keepAlive) throws InterruptedException {
        TestUtils.waitForCondition(() -> {
            keepAlive.run();
            return TelemetryExporter.pushes(clientInstanceId).size() >= count;
        }, 30_000, () -> "Expected at least " + count + " metrics pushes from client " + clientInstanceId
            + " but received " + TelemetryExporter.pushes(clientInstanceId).size());
        return TelemetryExporter.pushes(clientInstanceId);
    }

    private static void waitForTerminatingPush(Uuid clientInstanceId) throws InterruptedException {
        TestUtils.waitForCondition(() -> TelemetryExporter.pushes(clientInstanceId).stream().anyMatch(PushedMetrics::terminating),
            30_000, "Expected a terminating metrics push from client " + clientInstanceId);
    }

    private static void assertOnlyMetricsWithPrefix(PushedMetrics push, String prefix) {
        assertFalse(push.metricNames().isEmpty(), "Expected some metrics to be pushed");
        assertTrue(push.metricNames().stream().allMatch(name -> name.startsWith(prefix)),
            "Expected only metrics with prefix " + prefix + " but received " + push.metricNames());
    }

    /**
     * A decoded metrics push as received by the broker-side exporter.
     */
    public record PushedMetrics(Uuid clientInstanceId, boolean terminating, int pushIntervalMs, String contentType, List<String> metricNames) {
    }

    @SuppressWarnings("unused")
    public static class TelemetryExporter implements ClientTelemetryExporterProvider, MetricsReporter {

        private static final Map<Uuid, List<PushedMetrics>> PUSHES = new ConcurrentHashMap<>();

        static List<PushedMetrics> pushes(Uuid clientInstanceId) {
            return List.copyOf(PUSHES.getOrDefault(clientInstanceId, List.of()));
        }

        @Override
        public void init(List<KafkaMetric> metrics) {
        }

        @Override
        public void metricChange(KafkaMetric metric) {
        }

        @Override
        public void metricRemoval(KafkaMetric metric) {
        }

        @Override
        public void close() {
        }

        @Override
        public void configure(Map<String, ?> configs) {
        }

        @Override
        public ClientTelemetryExporter clientTelemetryExporter() {
            return (context, payload) -> {
                List<String> metricNames;
                try {
                    MetricsData data = MetricsData.parseFrom(payload.data());
                    metricNames = data.getResourceMetricsList().stream()
                        .flatMap(resourceMetrics -> resourceMetrics.getScopeMetricsList().stream())
                        .flatMap(scopeMetrics -> scopeMetrics.getMetricsList().stream())
                        .map(Metric::getName)
                        .sorted()
                        .toList();
                } catch (Exception e) {
                    throw new RuntimeException("Failed to decode client telemetry payload", e);
                }
                PUSHES.computeIfAbsent(payload.clientInstanceId(), id -> new CopyOnWriteArrayList<>())
                    .add(new PushedMetrics(payload.clientInstanceId(), payload.isTerminating(), context.pushIntervalMs(),
                        payload.contentType(), metricNames));
            };
        }
    }
}
