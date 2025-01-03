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
package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.LeastLoadedNode;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.internals.FutureRecordMetadata;
import org.apache.kafka.clients.producer.internals.ProduceRequestResult;
import org.apache.kafka.clients.producer.internals.ProducerInterceptors;
import org.apache.kafka.clients.producer.internals.ProducerMetadata;
import org.apache.kafka.clients.producer.internals.RecordAccumulator;
import org.apache.kafka.clients.producer.internals.Sender;
import org.apache.kafka.clients.producer.internals.TransactionManager;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.AddOffsetsToTxnResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.EndTxnResponseData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AddOffsetsToTxnResponse;
import org.apache.kafka.common.requests.EndTxnResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest;
import org.apache.kafka.common.requests.TxnOffsetCommitResponse;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryReporter;
import org.apache.kafka.common.telemetry.internals.ClientTelemetrySender;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.MockMetricsReporter;
import org.apache.kafka.test.MockPartitioner;
import org.apache.kafka.test.MockProducerInterceptor;
import org.apache.kafka.test.MockSerializer;
import org.apache.kafka.test.TestUtils;

import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.internal.stubbing.answers.CallsRealMethods;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KafkaProducerTest {
    private final String topic = "topic";
    private final Collection<Node> nodes = Collections.singletonList(NODE);
    private final Cluster emptyCluster = new Cluster(
            null,
            nodes,
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet());
    private final Cluster onePartitionCluster = new Cluster(
            "dummy",
            nodes,
            Collections.singletonList(new PartitionInfo(topic, 0, null, null, null)),
            Collections.emptySet(),
            Collections.emptySet());
    private final Cluster threePartitionCluster = new Cluster(
            "dummy",
            nodes,
            Arrays.asList(
                    new PartitionInfo(topic, 0, null, null, null),
                    new PartitionInfo(topic, 1, null, null, null),
                    new PartitionInfo(topic, 2, null, null, null)),
            Collections.emptySet(),
            Collections.emptySet());
    private TestInfo testInfo;

    private static final int DEFAULT_METADATA_IDLE_MS = 5 * 60 * 1000;
    private static final Node NODE = new Node(0, "host1", 1000);

    private static <K, V> KafkaProducer<K, V> kafkaProducer(Map<String, Object> configs,
                  Serializer<K> keySerializer,
                  Serializer<V> valueSerializer,
                  ProducerMetadata metadata,
                  KafkaClient kafkaClient,
                  ProducerInterceptors<K, V> interceptors,
                  Time time) {
        return new KafkaProducer<>(new ProducerConfig(ProducerConfig.appendSerializerToConfig(configs, keySerializer, valueSerializer)),
            keySerializer, valueSerializer, metadata, kafkaClient, interceptors, new ApiVersions(), time);
    }

    @BeforeEach
    public void setup(TestInfo testInfo) {
        this.testInfo = testInfo;
    }

    @Test
    public void testOverwriteAcksAndRetriesForIdempotentProducers() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactionalId");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        ProducerConfig config = new ProducerConfig(props);
        assertTrue(config.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        assertTrue(Stream.of("-1", "all").anyMatch(each -> each.equalsIgnoreCase(config.getString(ProducerConfig.ACKS_CONFIG))));
        assertEquals((int) config.getInt(ProducerConfig.RETRIES_CONFIG), Integer.MAX_VALUE);
        assertTrue(config.getString(ProducerConfig.CLIENT_ID_CONFIG).equalsIgnoreCase("producer-" +
                config.getString(ProducerConfig.TRANSACTIONAL_ID_CONFIG)));
    }

    @Test
    public void testAcksAndIdempotenceForIdempotentProducers() {
        Properties baseProps = new Properties() {{
                setProperty(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
                setProperty(
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                setProperty(
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            }};

        Properties validProps = new Properties() {{
                putAll(baseProps);
                setProperty(ProducerConfig.ACKS_CONFIG, "0");
                setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
            }};
        ProducerConfig config = new ProducerConfig(validProps);
        assertFalse(
            config.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG),
            "idempotence should be overwritten");
        assertEquals(
            "0",
            config.getString(ProducerConfig.ACKS_CONFIG),
            "acks should be overwritten");

        Properties validProps2 = new Properties() {{
                putAll(baseProps);
                setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactionalId");
            }};
        config = new ProducerConfig(validProps2);
        assertTrue(
            config.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG),
            "idempotence should be set with the default value");
        assertEquals(
            "-1",
            config.getString(ProducerConfig.ACKS_CONFIG),
            "acks should be set with the default value");

        Properties validProps3 = new Properties() {{
                putAll(baseProps);
                setProperty(ProducerConfig.ACKS_CONFIG, "all");
                setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
            }};
        config = new ProducerConfig(validProps3);
        assertFalse(config.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG),
            "idempotence should be overwritten");
        assertEquals(
            "-1",
            config.getString(ProducerConfig.ACKS_CONFIG),
            "acks should be overwritten");

        Properties validProps4 = new Properties() {{
                putAll(baseProps);
                setProperty(ProducerConfig.ACKS_CONFIG, "0");
            }};
        config = new ProducerConfig(validProps4);
        assertFalse(
            config.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG),
            "idempotence should be disabled when acks not set to all and `enable.idempotence` config is unset.");
        assertEquals(
            "0",
            config.getString(ProducerConfig.ACKS_CONFIG),
            "acks should be set with overridden value");

        Properties validProps5 = new Properties() {{
                putAll(baseProps);
                setProperty(ProducerConfig.ACKS_CONFIG, "1");
            }};
        config = new ProducerConfig(validProps5);
        assertFalse(
            config.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG),
            "idempotence should be disabled when acks not set to all and `enable.idempotence` config is unset.");
        assertEquals(
            "1",
            config.getString(ProducerConfig.ACKS_CONFIG),
            "acks should be set with overridden value");

        Properties invalidProps = new Properties() {{
                putAll(baseProps);
                setProperty(ProducerConfig.ACKS_CONFIG, "0");
                setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
                setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactionalId");
            }};
        assertThrows(
            ConfigException.class,
            () -> new ProducerConfig(invalidProps),
            "Cannot set a transactional.id without also enabling idempotence");

        Properties invalidProps2 = new Properties() {{
                putAll(baseProps);
                setProperty(ProducerConfig.ACKS_CONFIG, "1");
                // explicitly enabling idempotence should still throw exception
                setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
            }};
        assertThrows(
            ConfigException.class,
            () -> new ProducerConfig(invalidProps2),
            "Must set acks to all in order to use the idempotent producer");

        Properties invalidProps3 = new Properties() {{
                putAll(baseProps);
                setProperty(ProducerConfig.ACKS_CONFIG, "0");
                setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactionalId");
            }};
        assertThrows(
            ConfigException.class,
            () -> new ProducerConfig(invalidProps3),
            "Must set acks to all when using the transactional producer.");
    }

    @Test
    public void testRetriesAndIdempotenceForIdempotentProducers() {
        Properties baseProps = new Properties() {{
                setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
                setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            }};

        Properties validProps = new Properties() {{
                putAll(baseProps);
                setProperty(ProducerConfig.RETRIES_CONFIG, "0");
                setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
            }};
        ProducerConfig config = new ProducerConfig(validProps);
        assertFalse(
            config.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG),
            "idempotence should be overwritten");
        assertEquals(
            0,
            config.getInt(ProducerConfig.RETRIES_CONFIG),
            "retries should be overwritten");

        Properties validProps2 = new Properties() {{
                putAll(baseProps);
                setProperty(ProducerConfig.RETRIES_CONFIG, "0");
            }};
        config = new ProducerConfig(validProps2);
        assertFalse(
            config.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG),
            "idempotence should be disabled when retries set to 0 and `enable.idempotence` config is unset.");
        assertEquals(
            0,
            config.getInt(ProducerConfig.RETRIES_CONFIG),
            "retries should be set with overridden value");

        Properties invalidProps = new Properties() {{
                putAll(baseProps);
                setProperty(ProducerConfig.RETRIES_CONFIG, "0");
                setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
                setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactionalId");
            }};
        assertThrows(
            ConfigException.class,
            () -> new ProducerConfig(invalidProps),
            "Cannot set a transactional.id without also enabling idempotence");

        Properties invalidProps2 = new Properties() {{
                putAll(baseProps);
                setProperty(ProducerConfig.RETRIES_CONFIG, "0");
                // explicitly enabling idempotence should still throw exception
                setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
            }};
        assertThrows(
            ConfigException.class,
            () -> new ProducerConfig(invalidProps2),
            "Must set retries to non-zero when using the idempotent producer.");

        Properties invalidProps3 = new Properties() {{
                putAll(baseProps);
                setProperty(ProducerConfig.RETRIES_CONFIG, "0");
                setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactionalId");
            }};
        assertThrows(
            ConfigException.class,
            () -> new ProducerConfig(invalidProps3),
            "Must set retries to non-zero when using the transactional producer.");
    }

    @Test
    public void testInflightRequestsAndIdempotenceForIdempotentProducers() {
        Properties baseProps = new Properties() {{
                setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
                setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            }};

        Properties validProps = new Properties() {{
                putAll(baseProps);
                setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "6");
                setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
            }};
        ProducerConfig config = new ProducerConfig(validProps);
        assertFalse(
            config.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG),
            "idempotence should be overwritten");
        assertEquals(
            6,
            config.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION),
            "max.in.flight.requests.per.connection should be overwritten");

        Properties validProps2 = new Properties() {{
                putAll(baseProps);
                setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "6");
            }};

        ConfigException configException = assertThrows(ConfigException.class, () -> new ProducerConfig(validProps2));
        assertEquals("To use the idempotent producer, " + ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION +
                     " must be set to at most 5. Current value is 6.", configException.getMessage());

        Properties invalidProps = new Properties() {{
                putAll(baseProps);
                setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
                setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
                setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactionalId");
            }};
        assertThrows(
            ConfigException.class,
            () -> new ProducerConfig(invalidProps),
            "Cannot set a transactional.id without also enabling idempotence");

        Properties invalidProps2 = new Properties() {{
                putAll(baseProps);
                setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "6");
                // explicitly enabling idempotence should still throw exception
                setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
            }};
        assertThrows(
            ConfigException.class,
            () -> new ProducerConfig(invalidProps2),
            "Must set max.in.flight.requests.per.connection to at most 5 when using the idempotent producer.");

        Properties invalidProps3 = new Properties() {{
                putAll(baseProps);
                setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "6");
                setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactionalId");
            }};
        assertThrows(
            ConfigException.class,
            () -> new ProducerConfig(invalidProps3),
            "Must set retries to non-zero when using the idempotent producer.");
    }

    @Test
    public void testMetricsReporterAutoGeneratedClientId() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(
                props, new StringSerializer(), new StringSerializer());

        assertEquals(2, producer.metrics.reporters().size());

        MockMetricsReporter mockMetricsReporter = (MockMetricsReporter) producer.metrics.reporters().stream()
            .filter(reporter -> reporter instanceof MockMetricsReporter).findFirst().get();
        assertEquals(producer.getClientId(), mockMetricsReporter.clientId);

        producer.close();
    }

    @Test
    public void testDisableJmxAndClientTelemetryReporter() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, "");
        props.setProperty(ProducerConfig.ENABLE_METRICS_PUSH_CONFIG, "false");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
        assertTrue(producer.metrics.reporters().isEmpty());
        producer.close();
    }

    @Test
    public void testExplicitlyOnlyEnableJmxReporter() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, "org.apache.kafka.common.metrics.JmxReporter");
        props.setProperty(ProducerConfig.ENABLE_METRICS_PUSH_CONFIG, "false");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
        assertEquals(1, producer.metrics.reporters().size());
        assertInstanceOf(JmxReporter.class, producer.metrics.reporters().get(0));
        producer.close();
    }

    @Test
    public void testExplicitlyOnlyEnableClientTelemetryReporter() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, "");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
        assertEquals(1, producer.metrics.reporters().size());
        assertInstanceOf(ClientTelemetryReporter.class, producer.metrics.reporters().get(0));
        producer.close();
    }

    @Test
    public void testConstructorWithSerializers() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");
        new KafkaProducer<>(producerProps, new ByteArraySerializer(), new ByteArraySerializer()).close();
    }

    @Test
    public void testNoSerializerProvided() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");
        assertThrows(ConfigException.class, () -> new KafkaProducer(producerProps));

        final Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");

        // Invalid value null for configuration key.serializer: must be non-null.
        assertThrows(ConfigException.class, () -> new KafkaProducer<String, String>(configs));
    }

    @Test
    public void testConstructorFailureCloseResource() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "testConstructorClose");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "some.invalid.hostname.foo.bar.local:9999");
        props.setProperty(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());

        final int oldInitCount = MockMetricsReporter.INIT_COUNT.get();
        final int oldCloseCount = MockMetricsReporter.CLOSE_COUNT.get();
        try (KafkaProducer<byte[], byte[]> ignored = new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer())) {
            fail("should have caught an exception and returned");
        } catch (KafkaException e) {
            assertEquals(oldInitCount + 1, MockMetricsReporter.INIT_COUNT.get());
            assertEquals(oldCloseCount + 1, MockMetricsReporter.CLOSE_COUNT.get());
            assertEquals("Failed to construct kafka producer", e.getMessage());
        }
    }

    @Test
    public void testConstructorWithNotStringKey() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.put(1, "not string key");
        ConfigException ce = assertThrows(
            ConfigException.class,
            () -> new KafkaProducer<>(props, new StringSerializer(), new StringSerializer()));
        assertTrue(ce.getMessage().contains("not string key"), "Unexpected exception message: " + ce.getMessage());
    }

    @Test
    public void testConstructorWithInvalidMetricReporterClass() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, "an.invalid.class");
        KafkaException ce = assertThrows(
                KafkaException.class,
                () -> new KafkaProducer<>(props, new StringSerializer(), new StringSerializer()));
        assertTrue(ce.getMessage().contains("Failed to construct kafka producer"), "Unexpected exception message: " + ce.getMessage());
        assertTrue(ce.getCause().getMessage().contains("Class an.invalid.class cannot be found"), "Unexpected cause: " + ce.getCause());
    }

    @Test
    public void testSerializerClose() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.CLIENT_ID_CONFIG, "testConstructorClose");
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        configs.put(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        configs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL);
        final int oldInitCount = MockSerializer.INIT_COUNT.get();
        final int oldCloseCount = MockSerializer.CLOSE_COUNT.get();

        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(
                configs, new MockSerializer(), new MockSerializer());
        assertEquals(oldInitCount + 2, MockSerializer.INIT_COUNT.get());
        assertEquals(oldCloseCount, MockSerializer.CLOSE_COUNT.get());

        producer.close();
        assertEquals(oldInitCount + 2, MockSerializer.INIT_COUNT.get());
        assertEquals(oldCloseCount + 2, MockSerializer.CLOSE_COUNT.get());
    }

    @Test
    public void testInterceptorConstructClose() {
        try {
            Properties props = new Properties();
            // test with client ID assigned by KafkaProducer
            props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
            props.setProperty(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, MockProducerInterceptor.class.getName());
            props.setProperty(MockProducerInterceptor.APPEND_STRING_PROP, "something");

            KafkaProducer<String, String> producer = new KafkaProducer<>(
                    props, new StringSerializer(), new StringSerializer());
            assertEquals(1, MockProducerInterceptor.INIT_COUNT.get());
            assertEquals(0, MockProducerInterceptor.CLOSE_COUNT.get());

            // Cluster metadata will only be updated on calling onSend.
            assertNull(MockProducerInterceptor.CLUSTER_META.get());

            producer.close();
            assertEquals(1, MockProducerInterceptor.INIT_COUNT.get());
            assertEquals(1, MockProducerInterceptor.CLOSE_COUNT.get());
        } finally {
            // cleanup since we are using mutable static variables in MockProducerInterceptor
            MockProducerInterceptor.resetCounters();
        }
    }
    @Test
    public void testInterceptorConstructorConfigurationWithExceptionShouldCloseRemainingInstances() {
        final int targetInterceptor = 3;
        try {
            Properties props = new Properties();
            props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
            props.setProperty(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, org.apache.kafka.test.MockProducerInterceptor.class.getName() + ", "
                    +  org.apache.kafka.test.MockProducerInterceptor.class.getName() + ", "
                    +  org.apache.kafka.test.MockProducerInterceptor.class.getName());
            props.setProperty(MockProducerInterceptor.APPEND_STRING_PROP, "something");

            MockProducerInterceptor.setThrowOnConfigExceptionThreshold(targetInterceptor);

            assertThrows(KafkaException.class, () ->
                new KafkaProducer<>(props, new StringSerializer(), new StringSerializer())
            );

            assertEquals(3, MockProducerInterceptor.CONFIG_COUNT.get());
            assertEquals(3, MockProducerInterceptor.CLOSE_COUNT.get());

        } finally {
            MockProducerInterceptor.resetCounters();
        }
    }
    @Test
    public void testPartitionerClose() {
        try {
            Properties props = new Properties();
            props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
            MockPartitioner.resetCounters();
            props.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, MockPartitioner.class.getName());

            KafkaProducer<String, String> producer = new KafkaProducer<>(
                    props, new StringSerializer(), new StringSerializer());
            assertEquals(1, MockPartitioner.INIT_COUNT.get());
            assertEquals(0, MockPartitioner.CLOSE_COUNT.get());

            producer.close();
            assertEquals(1, MockPartitioner.INIT_COUNT.get());
            assertEquals(1, MockPartitioner.CLOSE_COUNT.get());
        } finally {
            // cleanup since we are using mutable static variables in MockPartitioner
            MockPartitioner.resetCounters();
        }
    }

    @Test
    public void shouldCloseProperlyAndThrowIfInterrupted() throws Exception {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MockPartitioner.class.getName());
        configs.put(ProducerConfig.BATCH_SIZE_CONFIG, "1");

        Time time = new MockTime();
        MetadataResponse initialUpdateResponse = RequestTestUtils.metadataUpdateWith(1, singletonMap("topic", 1));
        ProducerMetadata metadata = newMetadata(0, 0, Long.MAX_VALUE);
        MockClient client = new MockClient(time, metadata);
        client.updateMetadata(initialUpdateResponse);

        final Producer<String, String> producer = kafkaProducer(configs, new StringSerializer(),
                new StringSerializer(), metadata, client, null, time);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        final AtomicReference<Exception> closeException = new AtomicReference<>();
        try {
            Future<?> future = executor.submit(() -> {
                producer.send(new ProducerRecord<>("topic", "key", "value"));
                try {
                    producer.close();
                    fail("Close should block and throw.");
                } catch (Exception e) {
                    closeException.set(e);
                }
            });

            // Close producer should not complete until send succeeds
            try {
                future.get(100, TimeUnit.MILLISECONDS);
                fail("Close completed without waiting for send");
            } catch (java.util.concurrent.TimeoutException expected) { /* ignore */ }

            // Ensure send has started
            client.waitForRequests(1, 1000);

            assertTrue(future.cancel(true), "Close terminated prematurely");

            TestUtils.waitForCondition(() -> closeException.get() != null,
                    "InterruptException did not occur within timeout.");

            assertInstanceOf(InterruptException.class, closeException.get(), "Expected exception not thrown " + closeException);
        } finally {
            executor.shutdownNow();
        }

    }

    @Test
    public void testOsDefaultSocketBufferSizes() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        config.put(ProducerConfig.SEND_BUFFER_CONFIG, Selectable.USE_DEFAULT_BUFFER_SIZE);
        config.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, Selectable.USE_DEFAULT_BUFFER_SIZE);
        new KafkaProducer<>(config, new ByteArraySerializer(), new ByteArraySerializer()).close();
    }

    @Test
    public void testInvalidSocketSendBufferSize() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        config.put(ProducerConfig.SEND_BUFFER_CONFIG, -2);
        assertThrows(KafkaException.class, () -> new KafkaProducer<>(config, new ByteArraySerializer(), new ByteArraySerializer()));
    }

    @Test
    public void testInvalidSocketReceiveBufferSize() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        config.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, -2);
        assertThrows(KafkaException.class, () -> new KafkaProducer<>(config, new ByteArraySerializer(), new ByteArraySerializer()));
    }

    private static KafkaProducer<String, String> producerWithOverrideNewSender(Map<String, Object> configs,
                                                                               ProducerMetadata metadata) {
        return producerWithOverrideNewSender(configs, metadata, Time.SYSTEM);
    }

    private static KafkaProducer<String, String> producerWithOverrideNewSender(Map<String, Object> configs,
                                                                               ProducerMetadata metadata,
                                                                               Time time) {
        // let mockClient#leastLoadedNode return the node directly so that we can isolate Metadata calls from KafkaProducer for idempotent producer
        MockClient mockClient = new MockClient(Time.SYSTEM, metadata) {
            @Override
            public LeastLoadedNode leastLoadedNode(long now) {
                return new LeastLoadedNode(NODE, true);
            }
        };

        return new KafkaProducer<>(
                new ProducerConfig(ProducerConfig.appendSerializerToConfig(configs, new StringSerializer(), new StringSerializer())),
                new StringSerializer(), new StringSerializer(), metadata, mockClient, null, new ApiVersions(), time) {
            @Override
            Sender newSender(LogContext logContext, KafkaClient kafkaClient, ProducerMetadata metadata) {
                // give Sender its own Metadata instance so that we can isolate Metadata calls from KafkaProducer
                return super.newSender(logContext, kafkaClient, newMetadata(0, 0, 100_000));
            }
        };
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testMetadataFetch(boolean isIdempotenceEnabled) throws InterruptedException {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        configs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, isIdempotenceEnabled);

        ProducerMetadata metadata = mock(ProducerMetadata.class);

        // Return empty cluster 4 times and cluster from then on
        when(metadata.fetch()).thenReturn(emptyCluster, emptyCluster, emptyCluster, emptyCluster, onePartitionCluster);

        KafkaProducer<String, String> producer = producerWithOverrideNewSender(configs, metadata);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "value");
        producer.send(record);

        // One request update for each empty cluster returned
        verify(metadata, times(4)).requestUpdateForTopic(topic);
        verify(metadata, times(4)).awaitUpdate(anyInt(), anyLong());
        verify(metadata, times(5)).fetch();

        // Should not request update for subsequent `send`
        producer.send(record, null);
        verify(metadata, times(4)).requestUpdateForTopic(topic);
        verify(metadata, times(4)).awaitUpdate(anyInt(), anyLong());
        verify(metadata, times(6)).fetch();

        // Should not request update for subsequent `partitionsFor`
        producer.partitionsFor(topic);
        verify(metadata, times(4)).requestUpdateForTopic(topic);
        verify(metadata, times(4)).awaitUpdate(anyInt(), anyLong());
        verify(metadata, times(7)).fetch();

        producer.close(Duration.ofMillis(0));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testMetadataExpiry(boolean isIdempotenceEnabled) throws InterruptedException {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        configs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, isIdempotenceEnabled);
        ProducerMetadata metadata = mock(ProducerMetadata.class);

        when(metadata.fetch()).thenReturn(onePartitionCluster, emptyCluster, onePartitionCluster);

        KafkaProducer<String, String> producer = producerWithOverrideNewSender(configs, metadata);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "value");
        producer.send(record);

        // Verify the topic's metadata isn't requested since it's already present.
        verify(metadata, times(0)).requestUpdateForTopic(topic);
        verify(metadata, times(0)).awaitUpdate(anyInt(), anyLong());
        verify(metadata, times(1)).fetch();

        // The metadata has been expired. Verify the producer requests the topic's metadata.
        producer.send(record, null);
        verify(metadata, times(1)).requestUpdateForTopic(topic);
        verify(metadata, times(1)).awaitUpdate(anyInt(), anyLong());
        verify(metadata, times(3)).fetch();

        producer.close(Duration.ofMillis(0));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testMetadataTimeoutWithMissingTopic(boolean isIdempotenceEnabled) throws Exception {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        configs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60000);
        configs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, isIdempotenceEnabled);

        // Create a record for a not-yet-created topic
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, 2, null, "value");
        ProducerMetadata metadata = mock(ProducerMetadata.class);

        MockTime mockTime = new MockTime();
        AtomicInteger invocationCount = new AtomicInteger(0);
        when(metadata.fetch()).then(invocation -> {
            invocationCount.incrementAndGet();
            if (invocationCount.get() == 5) {
                mockTime.setCurrentTimeMs(mockTime.milliseconds() + 70000);
            }

            return emptyCluster;
        });

        KafkaProducer<String, String> producer = producerWithOverrideNewSender(configs, metadata, mockTime);

        // Four request updates where the topic isn't present, at which point the timeout expires and a
        // TimeoutException is thrown
        // For idempotence enabled case, the first metadata.fetch will be called in Sender#maybeSendAndPollTransactionalRequest
        Future<RecordMetadata> future = producer.send(record);
        verify(metadata, times(4)).requestUpdateForTopic(topic);
        verify(metadata, times(4)).awaitUpdate(anyInt(), anyLong());
        verify(metadata, times(5)).fetch();
        try {
            assertInstanceOf(TimeoutException.class, assertThrows(ExecutionException.class, future::get).getCause());
        } finally {
            producer.close(Duration.ofMillis(0));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testMetadataWithPartitionOutOfRange(boolean isIdempotenceEnabled) throws Exception {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        configs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60000);
        configs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, isIdempotenceEnabled);

        // Create a record with a partition higher than the initial (outdated) partition range
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, 2, null, "value");
        ProducerMetadata metadata = mock(ProducerMetadata.class);

        MockTime mockTime = new MockTime();

        when(metadata.fetch()).thenReturn(onePartitionCluster, onePartitionCluster, threePartitionCluster);

        KafkaProducer<String, String> producer = producerWithOverrideNewSender(configs, metadata, mockTime);
        // One request update if metadata is available but outdated for the given record
        producer.send(record);
        verify(metadata, times(2)).requestUpdateForTopic(topic);
        verify(metadata, times(2)).awaitUpdate(anyInt(), anyLong());
        verify(metadata, times(3)).fetch();

        producer.close(Duration.ofMillis(0));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testMetadataTimeoutWithPartitionOutOfRange(boolean isIdempotenceEnabled) throws Exception {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        configs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60000);
        configs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, isIdempotenceEnabled);

        // Create a record with a partition higher than the initial (outdated) partition range
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, 2, null, "value");
        ProducerMetadata metadata = mock(ProducerMetadata.class);

        MockTime mockTime = new MockTime();
        AtomicInteger invocationCount = new AtomicInteger(0);
        when(metadata.fetch()).then(invocation -> {
            invocationCount.incrementAndGet();
            if (invocationCount.get() == 5) {
                mockTime.setCurrentTimeMs(mockTime.milliseconds() + 70000);
            }

            return onePartitionCluster;
        });

        KafkaProducer<String, String> producer = producerWithOverrideNewSender(configs, metadata, mockTime);

        // Four request updates where the requested partition is out of range, at which point the timeout expires
        // and a TimeoutException is thrown
        // For idempotence enabled case, the first and last metadata.fetch will be called in Sender#maybeSendAndPollTransactionalRequest,
        // before the producer#send and after it finished
        Future<RecordMetadata> future = producer.send(record);

        verify(metadata, times(4)).requestUpdateForTopic(topic);
        verify(metadata, times(4)).awaitUpdate(anyInt(), anyLong());
        verify(metadata, times(5)).fetch();
        try {
            assertInstanceOf(TimeoutException.class, assertThrows(ExecutionException.class, future::get).getCause());
        } finally {
            producer.close(Duration.ofMillis(0));
        }
    }

    @Test
    public void testTopicRefreshInMetadata() throws InterruptedException {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        configs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "600000");
        // test under normal producer for simplicity
        configs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        long refreshBackoffMs = 500L;
        long refreshBackoffMaxMs = 5000L;
        long metadataExpireMs = 60000L;
        long metadataIdleMs = 60000L;
        final Time time = new MockTime();
        final ProducerMetadata metadata = new ProducerMetadata(refreshBackoffMs, refreshBackoffMaxMs, metadataExpireMs, metadataIdleMs,
                new LogContext(), new ClusterResourceListeners(), time);
        final String topic = "topic";
        try (KafkaProducer<String, String> producer = kafkaProducer(configs,
                new StringSerializer(), new StringSerializer(), metadata, new MockClient(time, metadata), null, time)) {

            AtomicBoolean running = new AtomicBoolean(true);
            Thread t = new Thread(() -> {
                long startTimeMs = System.currentTimeMillis();
                while (running.get()) {
                    while (!metadata.updateRequested() && System.currentTimeMillis() - startTimeMs < 100)
                        Thread.yield();
                    MetadataResponse updateResponse = RequestTestUtils.metadataUpdateWith("kafka-cluster", 1,
                            singletonMap(topic, Errors.UNKNOWN_TOPIC_OR_PARTITION), emptyMap());
                    metadata.updateWithCurrentRequestVersion(updateResponse, false, time.milliseconds());
                    time.sleep(60 * 1000L);
                }
            });
            t.start();
            Throwable throwable = assertThrows(TimeoutException.class, () -> producer.partitionsFor(topic));
            assertInstanceOf(UnknownTopicOrPartitionException.class, throwable.getCause());
            running.set(false);
            t.join();
        }
    }

    @Test
    public void testTopicNotExistingInMetadata() throws InterruptedException {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        configs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "30000");
        long refreshBackoffMs = 500L;
        long refreshBackoffMaxMs = 5000L;
        long metadataExpireMs = 60000L;
        long metadataIdleMs = 60000L;
        final Time time = new MockTime();
        final ProducerMetadata metadata = new ProducerMetadata(refreshBackoffMs, refreshBackoffMaxMs, metadataExpireMs, metadataIdleMs,
                new LogContext(), new ClusterResourceListeners(), time);
        final String topic = "topic";
        try (KafkaProducer<String, String> producer = kafkaProducer(configs, new StringSerializer(),
                new StringSerializer(), metadata, new MockClient(time, metadata), null, time)) {

            Exchanger<Void> exchanger = new Exchanger<>();

            Thread t = new Thread(() -> {
                try {
                    // Update the metadata with non-existing topic.
                    MetadataResponse updateResponse = RequestTestUtils.metadataUpdateWith("kafka-cluster", 1,
                            singletonMap(topic, Errors.UNKNOWN_TOPIC_OR_PARTITION), emptyMap());
                    metadata.updateWithCurrentRequestVersion(updateResponse, false, time.milliseconds());
                    exchanger.exchange(null);
                    while (!metadata.updateRequested())
                        Thread.sleep(100);
                    time.sleep(30 * 1000L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            t.start();
            exchanger.exchange(null);
            Throwable throwable = assertThrows(TimeoutException.class, () -> producer.partitionsFor(topic));
            assertInstanceOf(UnknownTopicOrPartitionException.class, throwable.getCause());
            t.join();
        }
    }

    @Test
    public void testTopicExpiryInMetadata() throws InterruptedException {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        configs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "30000");
        long refreshBackoffMs = 500L;
        long refreshBackoffMaxMs = 5000L;
        long metadataExpireMs = 60000L;
        long metadataIdleMs = 60000L;
        final Time time = new MockTime();
        final ProducerMetadata metadata = new ProducerMetadata(refreshBackoffMs, refreshBackoffMaxMs, metadataExpireMs, metadataIdleMs,
                new LogContext(), new ClusterResourceListeners(), time);
        final String topic = "topic";
        try (KafkaProducer<String, String> producer = kafkaProducer(configs, new StringSerializer(),
                new StringSerializer(), metadata, new MockClient(time, metadata), null, time)) {

            Exchanger<Void> exchanger = new Exchanger<>();

            Thread t = new Thread(() -> {
                try {
                    exchanger.exchange(null);  // 1
                    while (!metadata.updateRequested())
                        Thread.sleep(100);
                    MetadataResponse updateResponse = RequestTestUtils.metadataUpdateWith(1, singletonMap(topic, 1));
                    metadata.updateWithCurrentRequestVersion(updateResponse, false, time.milliseconds());
                    exchanger.exchange(null);  // 2
                    time.sleep(120 * 1000L);

                    // Update the metadata again, but it should be expired at this point.
                    updateResponse = RequestTestUtils.metadataUpdateWith(1, singletonMap(topic, 1));
                    metadata.updateWithCurrentRequestVersion(updateResponse, false, time.milliseconds());
                    exchanger.exchange(null);  // 3
                    while (!metadata.updateRequested())
                        Thread.sleep(100);
                    time.sleep(30 * 1000L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            t.start();
            exchanger.exchange(null);  // 1
            assertNotNull(producer.partitionsFor(topic));
            exchanger.exchange(null);  // 2
            exchanger.exchange(null);  // 3
            assertThrows(TimeoutException.class, () -> producer.partitionsFor(topic));
            t.join();
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testHeaders() {
        doTestHeaders(Serializer.class);
    }

    private <T extends Serializer<String>> void doTestHeaders(Class<T> serializerClassToMock) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        Serializer<String> keySerializer = mock(serializerClassToMock);
        Serializer<String> valueSerializer = mock(serializerClassToMock);

        long nowMs = Time.SYSTEM.milliseconds();
        String topic = "topic";
        ProducerMetadata metadata = newMetadata(0, 0, 90000);
        metadata.add(topic, nowMs);

        MetadataResponse initialUpdateResponse = RequestTestUtils.metadataUpdateWith(1, singletonMap(topic, 1));
        metadata.updateWithCurrentRequestVersion(initialUpdateResponse, false, nowMs);

        KafkaProducer<String, String> producer = kafkaProducer(configs, keySerializer, valueSerializer, metadata,
                null, null, Time.SYSTEM);

        when(keySerializer.serialize(any(), any(), any())).then(invocation ->
                invocation.<String>getArgument(2).getBytes());
        when(valueSerializer.serialize(any(), any(), any())).then(invocation ->
                invocation.<String>getArgument(2).getBytes());

        String value = "value";
        String key = "key";
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

        //ensure headers can be mutated pre send.
        record.headers().add(new RecordHeader("test", "header2".getBytes()));
        producer.send(record, null);

        //ensure headers are closed and cannot be mutated post send
        assertThrows(IllegalStateException.class, () -> record.headers().add(new RecordHeader("test", "test".getBytes())));

        //ensure existing headers are not changed, and last header for key is still original value
        assertArrayEquals(record.headers().lastHeader("test").value(), "header2".getBytes());

        verify(valueSerializer).serialize(topic, record.headers(), value);
        verify(keySerializer).serialize(topic, record.headers(), key);

        producer.close(Duration.ofMillis(0));
    }

    @Test
    public void closeShouldBeIdempotent() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");
        Producer<byte[], byte[]> producer = new KafkaProducer<>(producerProps, new ByteArraySerializer(), new ByteArraySerializer());
        producer.close();
        producer.close();
    }

    @Test
    public void closeWithNegativeTimestampShouldThrow() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");
        try (Producer<byte[], byte[]> producer = new KafkaProducer<>(producerProps, new ByteArraySerializer(), new ByteArraySerializer())) {
            assertThrows(IllegalArgumentException.class, () -> producer.close(Duration.ofMillis(-100)));
        }
    }

    @Test
    public void testFlushCompleteSendOfInflightBatches() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");
        // only test in idempotence disabled producer for simplicity
        // flush operation acts the same for idempotence enabled and disabled cases
        configs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);

        Time time = new MockTime(1);
        MetadataResponse initialUpdateResponse = RequestTestUtils.metadataUpdateWith(1, singletonMap("topic", 1));
        ProducerMetadata metadata = newMetadata(0, 0, Long.MAX_VALUE);

        MockClient client = new MockClient(time, metadata);
        client.updateMetadata(initialUpdateResponse);

        try (Producer<String, String> producer = kafkaProducer(configs, new StringSerializer(),
                new StringSerializer(), metadata, client, null, time)) {
            ArrayList<Future<RecordMetadata>> futureResponses = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                Future<RecordMetadata> response = producer.send(new ProducerRecord<>("topic", "value" + i));
                futureResponses.add(response);
            }

            futureResponses.forEach(res -> assertFalse(res.isDone()));
            producer.flush();
            futureResponses.forEach(res -> assertTrue(res.isDone()));
        }
    }

    private static Double getMetricValue(final KafkaProducer<?, ?> producer, final String name) {
        Metrics metrics = producer.metrics;
        Metric metric =  metrics.metric(metrics.metricName(name, "producer-metrics"));
        return (Double) metric.metricValue();
    }

    @Test
    public void testFlushMeasureLatency() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");

        Time time = new MockTime(1);
        MetadataResponse initialUpdateResponse = RequestTestUtils.metadataUpdateWith(1, singletonMap("topic", 1));
        ProducerMetadata metadata = newMetadata(0, 0, Long.MAX_VALUE);

        MockClient client = new MockClient(time, metadata);
        client.updateMetadata(initialUpdateResponse);

        try (KafkaProducer<String, String> producer = kafkaProducer(
            configs,
            new StringSerializer(),
            new StringSerializer(),
            metadata,
            client,
            null,
            time
        )) {
            producer.flush();
            double first = getMetricValue(producer, "flush-time-ns-total");
            assertTrue(first > 0);
            producer.flush();
            assertTrue(getMetricValue(producer, "flush-time-ns-total") > first);
        }
    }

    @Test
    public void testMetricConfigRecordingLevel() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer())) {
            assertEquals(Sensor.RecordingLevel.INFO, producer.metrics.config().recordLevel());
        }

        props.put(ProducerConfig.METRICS_RECORDING_LEVEL_CONFIG, "DEBUG");
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer())) {
            assertEquals(Sensor.RecordingLevel.DEBUG, producer.metrics.config().recordLevel());
        }
    }

    @Test
    public void testInterceptorPartitionSetOnTooLargeRecord() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        configs.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "1");
        String topic = "topic";
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "value");

        long nowMs = Time.SYSTEM.milliseconds();
        ProducerMetadata metadata = newMetadata(0, 0, 90000);
        metadata.add(topic, nowMs);
        MetadataResponse initialUpdateResponse = RequestTestUtils.metadataUpdateWith(1, singletonMap(topic, 1));
        metadata.updateWithCurrentRequestVersion(initialUpdateResponse, false, nowMs);

        @SuppressWarnings("unchecked") // it is safe to suppress, since this is a mock class
                ProducerInterceptors<String, String> interceptors = mock(ProducerInterceptors.class);
        KafkaProducer<String, String> producer = kafkaProducer(configs, new StringSerializer(),
                new StringSerializer(), metadata, null, interceptors, Time.SYSTEM);

        when(interceptors.onSend(any())).then(invocation -> invocation.getArgument(0));

        producer.send(record);

        verify(interceptors).onSend(record);
        verify(interceptors).onSendError(eq(record), notNull(), notNull());

        producer.close(Duration.ofMillis(0));
    }

    @Test
    public void testPartitionsForWithNullTopic() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer())) {
            assertThrows(NullPointerException.class, () -> producer.partitionsFor(null));
        }
    }

    @Test
    public void testInitTransactionsResponseAfterTimeout() throws Exception {
        int maxBlockMs = 500;

        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "bad-transaction");
        configs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMs);
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");

        Time time = new MockTime();
        MetadataResponse initialUpdateResponse = RequestTestUtils.metadataUpdateWith(1, singletonMap("topic", 1));
        ProducerMetadata metadata = newMetadata(0, 0, Long.MAX_VALUE);
        metadata.updateWithCurrentRequestVersion(initialUpdateResponse, false, time.milliseconds());

        MockClient client = new MockClient(time, metadata);

        ExecutorService executor = Executors.newFixedThreadPool(1);

        try (Producer<String, String> producer = kafkaProducer(configs, new StringSerializer(),
                new StringSerializer(), metadata, client, null, time)) {
            client.prepareResponse(
                request -> request instanceof FindCoordinatorRequest &&
                    ((FindCoordinatorRequest) request).data().keyType() == FindCoordinatorRequest.CoordinatorType.TRANSACTION.id(),
                FindCoordinatorResponse.prepareResponse(Errors.NONE, "bad-transaction", NODE));

            Future<?> future = executor.submit(producer::initTransactions);
            TestUtils.waitForCondition(client::hasInFlightRequests,
                "Timed out while waiting for expected `InitProducerId` request to be sent");

            time.sleep(maxBlockMs);
            TestUtils.assertFutureThrows(future, TimeoutException.class);

            client.respond(initProducerIdResponse(1L, (short) 5, Errors.NONE));

            Thread.sleep(1000);
            producer.initTransactions();
        }
    }

    @Test
    public void testInitTransactionTimeout() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "bad-transaction");
        configs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 500);
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");

        Time time = new MockTime(1);
        MetadataResponse initialUpdateResponse = RequestTestUtils.metadataUpdateWith(1, singletonMap("topic", 1));
        ProducerMetadata metadata = newMetadata(0, 0, Long.MAX_VALUE);
        metadata.updateWithCurrentRequestVersion(initialUpdateResponse, false, time.milliseconds());

        MockClient client = new MockClient(time, metadata);

        try (Producer<String, String> producer = kafkaProducer(configs, new StringSerializer(),
                new StringSerializer(), metadata, client, null, time)) {
            client.prepareResponse(
                request -> request instanceof FindCoordinatorRequest &&
                    ((FindCoordinatorRequest) request).data().keyType() == FindCoordinatorRequest.CoordinatorType.TRANSACTION.id(),
                FindCoordinatorResponse.prepareResponse(Errors.NONE, "bad-transaction", NODE));

            assertThrows(TimeoutException.class, producer::initTransactions);

            client.prepareResponse(
                request -> request instanceof FindCoordinatorRequest &&
                               ((FindCoordinatorRequest) request).data().keyType() == FindCoordinatorRequest.CoordinatorType.TRANSACTION.id(),
                FindCoordinatorResponse.prepareResponse(Errors.NONE, "bad-transaction", NODE));

            client.prepareResponse(initProducerIdResponse(1L, (short) 5, Errors.NONE));

            // retry initialization should work
            producer.initTransactions();
        }
    }

    @Test
    public void testInitTransactionWhileThrottled() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "some.id");
        configs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000);
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");

        Time time = new MockTime(1);
        MetadataResponse initialUpdateResponse = RequestTestUtils.metadataUpdateWith(1, singletonMap("topic", 1));
        ProducerMetadata metadata = newMetadata(0, 0, Long.MAX_VALUE);

        MockClient client = new MockClient(time, metadata);
        client.updateMetadata(initialUpdateResponse);

        Node node = metadata.fetch().nodes().get(0);
        client.throttle(node, 5000);

        client.prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.NONE, "some.id", NODE));
        client.prepareResponse(initProducerIdResponse(1L, (short) 5, Errors.NONE));

        try (Producer<String, String> producer = kafkaProducer(configs, new StringSerializer(),
                new StringSerializer(), metadata, client, null, time)) {
            producer.initTransactions();
        }
    }

    @Test
    public void testClusterAuthorizationFailure() throws Exception {
        int maxBlockMs = 500;

        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMs);
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");
        configs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "some-txn");

        Time time = new MockTime(1);
        MetadataResponse initialUpdateResponse = RequestTestUtils.metadataUpdateWith(1, singletonMap("topic", 1));
        ProducerMetadata metadata = newMetadata(500, 5000, Long.MAX_VALUE);

        MockClient client = new MockClient(time, metadata);
        client.updateMetadata(initialUpdateResponse);

        client.prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.NONE, "some-txn", NODE));
        client.prepareResponse(initProducerIdResponse(1L, (short) 5, Errors.CLUSTER_AUTHORIZATION_FAILED));
        Producer<String, String> producer = kafkaProducer(configs, new StringSerializer(),
                new StringSerializer(), metadata, client, null, time);
        assertThrows(ClusterAuthorizationException.class, producer::initTransactions);

        // retry initTransactions after the ClusterAuthorizationException not being thrown
        client.prepareResponse(initProducerIdResponse(1L, (short) 5, Errors.NONE));
        TestUtils.retryOnExceptionWithTimeout(1000, 100, producer::initTransactions);
        producer.close();
    }

    @Test
    public void testAbortTransaction() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "some.id");
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");

        Time time = new MockTime(1);
        MetadataResponse initialUpdateResponse = RequestTestUtils.metadataUpdateWith(1, singletonMap("topic", 1));
        ProducerMetadata metadata = newMetadata(0, 0, Long.MAX_VALUE);

        MockClient client = new MockClient(time, metadata);
        client.updateMetadata(initialUpdateResponse);

        client.prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.NONE, "some.id", NODE));
        client.prepareResponse(initProducerIdResponse(1L, (short) 5, Errors.NONE));
        client.prepareResponse(endTxnResponse(Errors.NONE));

        try (Producer<String, String> producer = kafkaProducer(configs, new StringSerializer(),
                new StringSerializer(), metadata, client, null, time)) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.abortTransaction();
        }
    }

    @Test
    public void testMeasureAbortTransactionDuration() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "some.id");
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");
        Time time = new MockTime(1);
        MetadataResponse initialUpdateResponse = RequestTestUtils.metadataUpdateWith(1, singletonMap("topic", 1));
        ProducerMetadata metadata = newMetadata(0, 0, Long.MAX_VALUE);
        MockClient client = new MockClient(time, metadata);
        client.updateMetadata(initialUpdateResponse);
        client.prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.NONE, "some.id", NODE));
        client.prepareResponse(initProducerIdResponse(1L, (short) 5, Errors.NONE));

        try (KafkaProducer<String, String> producer = kafkaProducer(configs, new StringSerializer(),
            new StringSerializer(), metadata, client, null, time)) {
            producer.initTransactions();

            client.prepareResponse(endTxnResponse(Errors.NONE));
            producer.beginTransaction();
            producer.abortTransaction();
            double first = getMetricValue(producer, "txn-abort-time-ns-total");
            assertTrue(first > 0);

            client.prepareResponse(endTxnResponse(Errors.NONE));
            producer.beginTransaction();
            producer.abortTransaction();
            assertTrue(getMetricValue(producer, "txn-abort-time-ns-total") > first);
        }
    }

    @Test
    public void testCommitTransactionWithRecordTooLargeException() throws Exception {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "some.id");
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");
        configs.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 1000);
        Time time = new MockTime(1);
        MetadataResponse initialUpdateResponse = RequestTestUtils.metadataUpdateWith(1, singletonMap("topic", 1));
        ProducerMetadata metadata = mock(ProducerMetadata.class);
        MockClient client = new MockClient(time, metadata);
        client.updateMetadata(initialUpdateResponse);
        client.prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.NONE, "some.id", NODE));
        client.prepareResponse(initProducerIdResponse(1L, (short) 5, Errors.NONE));

        when(metadata.fetch()).thenReturn(onePartitionCluster);

        String largeString = IntStream.range(0, 1000).mapToObj(i -> "*").collect(Collectors.joining());
        ProducerRecord<String, String> largeRecord = new ProducerRecord<>(topic, "large string", largeString);

        try (KafkaProducer<String, String> producer = kafkaProducer(configs, new StringSerializer(),
                new StringSerializer(), metadata, client, null, time)) {
            producer.initTransactions();

            client.prepareResponse(endTxnResponse(Errors.NONE));
            producer.beginTransaction();
            TestUtils.assertFutureError(producer.send(largeRecord), RecordTooLargeException.class);
            assertThrows(KafkaException.class, producer::commitTransaction);
        }
    }

    @Test
    public void testCommitTransactionWithMetadataTimeoutForMissingTopic() throws Exception {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "some.id");
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        configs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60000);

        // Create a record for a not-yet-created topic
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "value");
        ProducerMetadata metadata = mock(ProducerMetadata.class);

        MockTime mockTime = new MockTime();

        MockClient client = new MockClient(mockTime, metadata);
        client.prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.NONE, "some.id", NODE));
        client.prepareResponse(initProducerIdResponse(1L, (short) 5, Errors.NONE));

        AtomicInteger invocationCount = new AtomicInteger(0);
        when(metadata.fetch()).then(invocation -> {
            invocationCount.incrementAndGet();
            if (invocationCount.get() > 5) {
                mockTime.setCurrentTimeMs(mockTime.milliseconds() + 70000);
            }

            return emptyCluster;
        });

        try (KafkaProducer<String, String> producer = kafkaProducer(configs, new StringSerializer(),
                new StringSerializer(), metadata, client, null, mockTime)) {
            producer.initTransactions();
            producer.beginTransaction();

            TestUtils.assertFutureError(producer.send(record), TimeoutException.class);
            assertThrows(KafkaException.class, producer::commitTransaction);
        }
    }

    @Test
    public void testCommitTransactionWithMetadataTimeoutForPartitionOutOfRange() throws Exception {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "some.id");
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        configs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60000);

        // Create a record with a partition higher than the initial (outdated) partition range
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, 2, null, "value");
        ProducerMetadata metadata = mock(ProducerMetadata.class);

        MockTime mockTime = new MockTime();

        MockClient client = new MockClient(mockTime, metadata);
        client.prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.NONE, "some.id", NODE));
        client.prepareResponse(initProducerIdResponse(1L, (short) 5, Errors.NONE));

        AtomicInteger invocationCount = new AtomicInteger(0);
        when(metadata.fetch()).then(invocation -> {
            invocationCount.incrementAndGet();
            if (invocationCount.get() > 5) {
                mockTime.setCurrentTimeMs(mockTime.milliseconds() + 70000);
            }

            return onePartitionCluster;
        });

        try (KafkaProducer<String, String> producer = kafkaProducer(configs, new StringSerializer(),
                new StringSerializer(), metadata, client, null, mockTime)) {
            producer.initTransactions();
            producer.beginTransaction();

            TestUtils.assertFutureError(producer.send(record), TimeoutException.class);
            assertThrows(KafkaException.class, producer::commitTransaction);
        }
    }

    @Test
    public void testCommitTransactionWithSendToInvalidTopic() throws Exception {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "some.id");
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");
        configs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "15000");

        Time time = new MockTime();
        MetadataResponse initialUpdateResponse = RequestTestUtils.metadataUpdateWith(1, emptyMap());
        ProducerMetadata metadata = newMetadata(0, 0, Long.MAX_VALUE);
        metadata.updateWithCurrentRequestVersion(initialUpdateResponse, false, time.milliseconds());

        MockClient client = new MockClient(time, metadata);
        client.prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.NONE, "some.id", NODE));
        client.prepareResponse(initProducerIdResponse(1L, (short) 5, Errors.NONE));

        String invalidTopicName = "topic abc"; // Invalid topic name due to space
        ProducerRecord<String, String> record = new ProducerRecord<>(invalidTopicName, "HelloKafka");

        List<MetadataResponse.TopicMetadata> topicMetadata = new ArrayList<>();
        topicMetadata.add(new MetadataResponse.TopicMetadata(Errors.INVALID_TOPIC_EXCEPTION,
                invalidTopicName, false, Collections.emptyList()));
        MetadataResponse updateResponse =  RequestTestUtils.metadataResponse(
                new ArrayList<>(initialUpdateResponse.brokers()),
                initialUpdateResponse.clusterId(),
                initialUpdateResponse.controller().id(),
                topicMetadata);
        client.prepareMetadataUpdate(updateResponse);

        try (Producer<String, String> producer = kafkaProducer(configs, new StringSerializer(),
                new StringSerializer(), metadata, client, null, time)) {
            producer.initTransactions();
            producer.beginTransaction();

            TestUtils.assertFutureError(producer.send(record), InvalidTopicException.class);
            assertThrows(KafkaException.class, producer::commitTransaction);
        }
    }

    @Test
    public void testSendTxnOffsetsWithGroupId() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "some.id");
        configs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000);
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");

        Time time = new MockTime(1);
        MetadataResponse initialUpdateResponse = RequestTestUtils.metadataUpdateWith(1, singletonMap("topic", 1));
        ProducerMetadata metadata = newMetadata(0, 0, Long.MAX_VALUE);

        MockClient client = new MockClient(time, metadata);
        client.updateMetadata(initialUpdateResponse);

        Node node = metadata.fetch().nodes().get(0);
        client.throttle(node, 5000);

        client.prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.NONE, "some.id", NODE));
        client.prepareResponse(initProducerIdResponse(1L, (short) 5, Errors.NONE));
        client.prepareResponse(addOffsetsToTxnResponse(Errors.NONE));
        client.prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.NONE, "some.id", NODE));
        String groupId = "group";
        client.prepareResponse(request ->
            ((TxnOffsetCommitRequest) request).data().groupId().equals(groupId),
            txnOffsetsCommitResponse(Collections.singletonMap(
                new TopicPartition("topic", 0), Errors.NONE)));
        client.prepareResponse(endTxnResponse(Errors.NONE));

        try (Producer<String, String> producer = kafkaProducer(configs, new StringSerializer(),
            new StringSerializer(), metadata, client, null, time)) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.sendOffsetsToTransaction(Collections.emptyMap(), new ConsumerGroupMetadata(groupId));
            producer.commitTransaction();
        }
    }

    @Test
    public void testSendTxnOffsetsWithGroupIdTransactionV2() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "some.id");
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Time time = new MockTime(1);
        MetadataResponse initialUpdateResponse = RequestTestUtils.metadataUpdateWith(1, singletonMap("topic", 1));
        ProducerMetadata metadata = newMetadata(0, 0, Long.MAX_VALUE);

        MockClient client = new MockClient(time, metadata);
        client.updateMetadata(initialUpdateResponse);

        Node node = metadata.fetch().nodes().get(0);
        client.setNodeApiVersions(NodeApiVersions.create());
        NodeApiVersions nodeApiVersions = new NodeApiVersions(NodeApiVersions.create().allSupportedApiVersions().values(),
            Arrays.asList(new ApiVersionsResponseData.SupportedFeatureKey()
                .setName("transaction.version")
                .setMaxVersion((short) 2)
                .setMinVersion((short) 0)),
            false,
            Arrays.asList(new ApiVersionsResponseData.FinalizedFeatureKey()
                .setName("transaction.version")
                .setMaxVersionLevel((short) 2)
                .setMinVersionLevel((short) 2)),
            0);
        client.setNodeApiVersions(nodeApiVersions);
        ApiVersions apiVersions = new ApiVersions();
        apiVersions.update(NODE.idString(), nodeApiVersions);

        client.throttle(node, 5000);

        client.prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.NONE, "some.id", NODE));
        client.prepareResponse(initProducerIdResponse(1L, (short) 5, Errors.NONE));
        client.prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.NONE, "some.id", NODE));
        String groupId = "group";
        client.prepareResponse(request ->
            ((TxnOffsetCommitRequest) request).data().groupId().equals(groupId),
            txnOffsetsCommitResponse(Collections.singletonMap(
                new TopicPartition("topic", 0), Errors.NONE)));
        client.prepareResponse(endTxnResponse(Errors.NONE));

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(
            new ProducerConfig(properties), new StringSerializer(), new StringSerializer(), metadata, client,
            new ProducerInterceptors<>(Collections.emptyList()), apiVersions, time)) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.sendOffsetsToTransaction(Collections.singletonMap(
                new TopicPartition("topic", 0),
                new OffsetAndMetadata(5L)),
                new ConsumerGroupMetadata(groupId));
            producer.commitTransaction();
        }
    }

    @Test
    public void testTransactionV2Produce() throws Exception {
        StringSerializer serializer = new StringSerializer();
        KafkaProducerTestContext<String> ctx = new KafkaProducerTestContext<>(testInfo, serializer);

        String topic = "foo";
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        Cluster cluster = TestUtils.singletonCluster(topic, 1);

        when(ctx.sender.isRunning()).thenReturn(true);
        when(ctx.metadata.fetch()).thenReturn(cluster);

        long timestamp = ctx.time.milliseconds();
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, 0, timestamp, "key", "value");

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "some-txn");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        ProducerConfig config = new ProducerConfig(props);

        Time time = new MockTime(1);
        MetadataResponse initialUpdateResponse = RequestTestUtils.metadataUpdateWith(1, singletonMap(topic, 1));
        ProducerMetadata metadata = newMetadata(0, 0, Long.MAX_VALUE);
        MockClient client = new MockClient(time, metadata);
        client.updateMetadata(initialUpdateResponse);
        NodeApiVersions nodeApiVersions = new NodeApiVersions(NodeApiVersions.create().allSupportedApiVersions().values(),
            Arrays.asList(new ApiVersionsResponseData.SupportedFeatureKey()
                .setName("transaction.version")
                .setMaxVersion((short) 2)
                .setMinVersion((short) 0)),
            false,
            Arrays.asList(new ApiVersionsResponseData.FinalizedFeatureKey()
                .setName("transaction.version")
                .setMaxVersionLevel((short) 2)
                .setMinVersionLevel((short) 2)),
            0);
        client.setNodeApiVersions(nodeApiVersions);
        ApiVersions apiVersions = new ApiVersions();
        apiVersions.update(NODE.idString(), nodeApiVersions);

        ProducerInterceptors<String, String> interceptor = new ProducerInterceptors<>(Collections.emptyList());

        client.prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.NONE, "some-txn", NODE));
        client.prepareResponse(initProducerIdResponse(1L, (short) 5, Errors.NONE));
        client.prepareResponse(produceResponse(topicPartition, 1L, Errors.NONE, 0, 1));
        client.prepareResponse(endTxnResponse(Errors.NONE));

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(
                config, new StringSerializer(), new StringSerializer(), metadata, client, interceptor, apiVersions, time)
        ) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(record).get();
            producer.commitTransaction();
        }
    }

    private void assertDurationAtLeast(KafkaProducer<?, ?> producer, String name, double floor) {
        getAndAssertDurationAtLeast(producer, name, floor);
    }

    private double getAndAssertDurationAtLeast(KafkaProducer<?, ?> producer, String name, double floor) {
        double value = getMetricValue(producer, name);
        assertTrue(value >= floor);
        return value;
    }

    @Test
    public void testMeasureTransactionDurations() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "some.id");
        configs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000);
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");
        Duration tick = Duration.ofSeconds(1);
        Time time = new MockTime(tick.toMillis());
        MetadataResponse initialUpdateResponse = RequestTestUtils.metadataUpdateWith(1, singletonMap("topic", 1));
        ProducerMetadata metadata = newMetadata(0, 0, Long.MAX_VALUE);

        MockClient client = new MockClient(time, metadata);
        client.updateMetadata(initialUpdateResponse);
        client.prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.NONE, "some.id", NODE));
        client.prepareResponse(initProducerIdResponse(1L, (short) 5, Errors.NONE));

        try (KafkaProducer<String, String> producer = kafkaProducer(configs, new StringSerializer(),
            new StringSerializer(), metadata, client, null, time)) {
            producer.initTransactions();
            assertDurationAtLeast(producer, "txn-init-time-ns-total", tick.toNanos());

            client.prepareResponse(addOffsetsToTxnResponse(Errors.NONE));
            client.prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.NONE, "some.id", NODE));
            client.prepareResponse(txnOffsetsCommitResponse(Collections.singletonMap(
                new TopicPartition("topic", 0), Errors.NONE)));
            client.prepareResponse(endTxnResponse(Errors.NONE));
            producer.beginTransaction();
            double beginFirst = getAndAssertDurationAtLeast(producer, "txn-begin-time-ns-total", tick.toNanos());
            producer.sendOffsetsToTransaction(Collections.singletonMap(
                    new TopicPartition("topic", 0),
                    new OffsetAndMetadata(5L)),
                    new ConsumerGroupMetadata("group"));
            double sendOffFirst = getAndAssertDurationAtLeast(producer, "txn-send-offsets-time-ns-total", tick.toNanos());
            producer.commitTransaction();
            double commitFirst = getAndAssertDurationAtLeast(producer, "txn-commit-time-ns-total", tick.toNanos());

            client.prepareResponse(addOffsetsToTxnResponse(Errors.NONE));
            client.prepareResponse(txnOffsetsCommitResponse(Collections.singletonMap(
                new TopicPartition("topic", 0), Errors.NONE)));
            client.prepareResponse(endTxnResponse(Errors.NONE));
            producer.beginTransaction();
            assertDurationAtLeast(producer, "txn-begin-time-ns-total", beginFirst + tick.toNanos());
            producer.sendOffsetsToTransaction(Collections.singletonMap(
                    new TopicPartition("topic", 0),
                    new OffsetAndMetadata(10L)),
                    new ConsumerGroupMetadata("group"));
            assertDurationAtLeast(producer, "txn-send-offsets-time-ns-total", sendOffFirst + tick.toNanos());
            producer.commitTransaction();
            assertDurationAtLeast(producer, "txn-commit-time-ns-total", commitFirst + tick.toNanos());
        }
    }

    @Test
    public void testSendTxnOffsetsWithGroupMetadata() {
        final short maxVersion = (short) 3;
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "some.id");
        configs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000);
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");

        Time time = new MockTime(1);
        MetadataResponse initialUpdateResponse = RequestTestUtils.metadataUpdateWith(1, singletonMap("topic", 1));
        ProducerMetadata metadata = newMetadata(0, 0, Long.MAX_VALUE);

        MockClient client = new MockClient(time, metadata);
        client.updateMetadata(initialUpdateResponse);
        client.setNodeApiVersions(NodeApiVersions.create(ApiKeys.TXN_OFFSET_COMMIT.id, (short) 0, maxVersion));

        Node node = metadata.fetch().nodes().get(0);
        client.throttle(node, 5000);

        client.prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.NONE, "some.id", NODE));
        client.prepareResponse(initProducerIdResponse(1L, (short) 5, Errors.NONE));
        client.prepareResponse(addOffsetsToTxnResponse(Errors.NONE));
        client.prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.NONE, "some.id", NODE));
        String groupId = "group";
        String memberId = "member";
        int generationId = 5;
        String groupInstanceId = "instance";
        client.prepareResponse(request -> {
            TxnOffsetCommitRequestData data = ((TxnOffsetCommitRequest) request).data();
            return data.groupId().equals(groupId) &&
                data.memberId().equals(memberId) &&
                data.generationId() == generationId &&
                data.groupInstanceId().equals(groupInstanceId);
        }, txnOffsetsCommitResponse(Collections.singletonMap(
            new TopicPartition("topic", 0), Errors.NONE)));
        client.prepareResponse(endTxnResponse(Errors.NONE));

        try (Producer<String, String> producer = kafkaProducer(configs, new StringSerializer(),
                                                               new StringSerializer(), metadata, client, null, time)) {
            producer.initTransactions();
            producer.beginTransaction();
            ConsumerGroupMetadata groupMetadata = new ConsumerGroupMetadata(groupId,
                generationId, memberId, Optional.of(groupInstanceId));

            producer.sendOffsetsToTransaction(Collections.emptyMap(), groupMetadata);
            producer.commitTransaction();
        }
    }

    @Test
    public void testNullGroupMetadataInSendOffsets() {
        verifyInvalidGroupMetadata(null);
    }

    @Test
    public void testInvalidGenerationIdAndMemberIdCombinedInSendOffsets() {
        verifyInvalidGroupMetadata(new ConsumerGroupMetadata("group", 2, JoinGroupRequest.UNKNOWN_MEMBER_ID, Optional.empty()));
    }

    @Test
    public void testClientInstanceId() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");

        ClientTelemetryReporter clientTelemetryReporter = mock(ClientTelemetryReporter.class);
        clientTelemetryReporter.configure(any());

        try (MockedStatic<CommonClientConfigs> mockedCommonClientConfigs = mockStatic(CommonClientConfigs.class, new CallsRealMethods())) {
            mockedCommonClientConfigs.when(() -> CommonClientConfigs.telemetryReporter(anyString(), any())).thenReturn(Optional.of(clientTelemetryReporter));

            ClientTelemetrySender clientTelemetrySender = mock(ClientTelemetrySender.class);
            Uuid expectedUuid = Uuid.randomUuid();
            when(clientTelemetryReporter.telemetrySender()).thenReturn(clientTelemetrySender);
            when(clientTelemetrySender.clientInstanceId(any())).thenReturn(Optional.of(expectedUuid));

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer())) {
                Uuid uuid = producer.clientInstanceId(Duration.ofMillis(0));
                assertEquals(expectedUuid, uuid);
            }
        }
    }

    @Test
    public void testClientInstanceIdInvalidTimeout() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
        Exception exception = assertThrows(IllegalArgumentException.class, () -> producer.clientInstanceId(Duration.ofMillis(-1)));
        assertEquals("The timeout cannot be negative.", exception.getMessage());

        producer.close();
    }

    @Test
    public void testClientInstanceIdNoTelemetryReporterRegistered() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(ProducerConfig.ENABLE_METRICS_PUSH_CONFIG, "false");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
        Exception exception = assertThrows(IllegalStateException.class, () -> producer.clientInstanceId(Duration.ofMillis(0)));
        assertEquals("Telemetry is not enabled. Set config `enable.metrics.push` to `true`.", exception.getMessage());

        producer.close();
    }

    private void verifyInvalidGroupMetadata(ConsumerGroupMetadata groupMetadata) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "some.id");
        configs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000);
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");

        Time time = new MockTime(1);
        MetadataResponse initialUpdateResponse = RequestTestUtils.metadataUpdateWith(1, singletonMap("topic", 1));
        ProducerMetadata metadata = newMetadata(0, 0, Long.MAX_VALUE);

        MockClient client = new MockClient(time, metadata);
        client.updateMetadata(initialUpdateResponse);

        Node node = metadata.fetch().nodes().get(0);
        client.throttle(node, 5000);

        client.prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.NONE, "some.id", NODE));
        client.prepareResponse(initProducerIdResponse(1L, (short) 5, Errors.NONE));

        try (Producer<String, String> producer = kafkaProducer(configs, new StringSerializer(),
            new StringSerializer(), metadata, client, null, time)) {
            producer.initTransactions();
            producer.beginTransaction();
            assertThrows(IllegalArgumentException.class,
                () -> producer.sendOffsetsToTransaction(Collections.emptyMap(), groupMetadata));
        }
    }

    private InitProducerIdResponse initProducerIdResponse(long producerId, short producerEpoch, Errors error) {
        InitProducerIdResponseData responseData = new InitProducerIdResponseData()
                .setErrorCode(error.code())
                .setProducerEpoch(producerEpoch)
                .setProducerId(producerId)
                .setThrottleTimeMs(0);
        return new InitProducerIdResponse(responseData);
    }

    private AddOffsetsToTxnResponse addOffsetsToTxnResponse(Errors error) {
        return new AddOffsetsToTxnResponse(new AddOffsetsToTxnResponseData()
                                               .setErrorCode(error.code())
                                               .setThrottleTimeMs(10));
    }

    private TxnOffsetCommitResponse txnOffsetsCommitResponse(Map<TopicPartition, Errors> errorMap) {
        return new TxnOffsetCommitResponse(10, errorMap);
    }

    private EndTxnResponse endTxnResponse(Errors error) {
        return new EndTxnResponse(new EndTxnResponseData()
                                      .setErrorCode(error.code())
                                      .setThrottleTimeMs(0));
    }

    @Test
    public void testOnlyCanExecuteCloseAfterInitTransactionsTimeout() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "bad-transaction");
        configs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5);
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");

        Time time = new MockTime();
        MetadataResponse initialUpdateResponse = RequestTestUtils.metadataUpdateWith(1, singletonMap("topic", 1));
        ProducerMetadata metadata = newMetadata(0, 0, Long.MAX_VALUE);
        metadata.updateWithCurrentRequestVersion(initialUpdateResponse, false, time.milliseconds());

        MockClient client = new MockClient(time, metadata);

        Producer<String, String> producer = kafkaProducer(configs, new StringSerializer(), new StringSerializer(),
                metadata, client, null, time);
        assertThrows(TimeoutException.class, producer::initTransactions);
        // other transactional operations should not be allowed if we catch the error after initTransactions failed
        try {
            assertThrows(IllegalStateException.class, producer::beginTransaction);
        } finally {
            producer.close(Duration.ofMillis(0));
        }
    }

    @Test
    public void testSendToInvalidTopic() throws Exception {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");
        configs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "15000");

        Time time = new MockTime();
        MetadataResponse initialUpdateResponse = RequestTestUtils.metadataUpdateWith(1, emptyMap());
        ProducerMetadata metadata = newMetadata(0, 0, Long.MAX_VALUE);
        metadata.updateWithCurrentRequestVersion(initialUpdateResponse, false, time.milliseconds());

        MockClient client = new MockClient(time, metadata);

        Producer<String, String> producer = kafkaProducer(configs, new StringSerializer(), new StringSerializer(),
                metadata, client, null, time);

        String invalidTopicName = "topic abc"; // Invalid topic name due to space
        ProducerRecord<String, String> record = new ProducerRecord<>(invalidTopicName, "HelloKafka");

        List<MetadataResponse.TopicMetadata> topicMetadata = new ArrayList<>();
        topicMetadata.add(new MetadataResponse.TopicMetadata(Errors.INVALID_TOPIC_EXCEPTION,
                invalidTopicName, false, Collections.emptyList()));
        MetadataResponse updateResponse =  RequestTestUtils.metadataResponse(
                new ArrayList<>(initialUpdateResponse.brokers()),
                initialUpdateResponse.clusterId(),
                initialUpdateResponse.controller().id(),
                topicMetadata);
        client.prepareMetadataUpdate(updateResponse);

        Future<RecordMetadata> future = producer.send(record);

        assertEquals(Collections.singleton(invalidTopicName),
                metadata.fetch().invalidTopics(), "Cluster has incorrect invalid topic list.");
        TestUtils.assertFutureError(future, InvalidTopicException.class);

        producer.close(Duration.ofMillis(0));
    }

    @Test
    public void testCloseWhenWaitingForMetadataUpdate() throws InterruptedException {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.MAX_VALUE);
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");

        // Simulate a case where metadata for a particular topic is not available. This will cause KafkaProducer#send to
        // block in Metadata#awaitUpdate for the configured max.block.ms. When close() is invoked, KafkaProducer#send should
        // return with a KafkaException.
        String topicName = "test";
        Time time = Time.SYSTEM;
        MetadataResponse initialUpdateResponse = RequestTestUtils.metadataUpdateWith(1, emptyMap());
        ProducerMetadata metadata = new ProducerMetadata(0, 0, Long.MAX_VALUE, Long.MAX_VALUE,
                new LogContext(), new ClusterResourceListeners(), time);
        metadata.updateWithCurrentRequestVersion(initialUpdateResponse, false, time.milliseconds());
        MockClient client = new MockClient(time, metadata);

        Producer<String, String> producer = kafkaProducer(configs, new StringSerializer(), new StringSerializer(),
                metadata, client, null, time);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        final AtomicReference<Exception> sendException = new AtomicReference<>();

        try {
            executor.submit(() -> {
                try {
                    // Metadata for topic "test" will not be available which will cause us to block indefinitely until
                    // KafkaProducer#close is invoked.
                    producer.send(new ProducerRecord<>(topicName, "key", "value"));
                    fail();
                } catch (Exception e) {
                    sendException.set(e);
                }
            });

            // Wait until metadata update for the topic has been requested
            TestUtils.waitForCondition(() -> metadata.containsTopic(topicName),
                    "Timeout when waiting for topic to be added to metadata");
            producer.close(Duration.ofMillis(0));
            TestUtils.waitForCondition(() -> sendException.get() != null, "No producer exception within timeout");
            assertEquals(KafkaException.class, sendException.get().getClass());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testTransactionalMethodThrowsWhenSenderClosed() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");
        configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "this-is-a-transactional-id");

        Time time = new MockTime();
        MetadataResponse initialUpdateResponse = RequestTestUtils.metadataUpdateWith(1, emptyMap());
        ProducerMetadata metadata = newMetadata(0, 0, Long.MAX_VALUE);
        metadata.updateWithCurrentRequestVersion(initialUpdateResponse, false, time.milliseconds());

        MockClient client = new MockClient(time, metadata);

        Producer<String, String> producer = kafkaProducer(configs, new StringSerializer(), new StringSerializer(),
                metadata, client, null, time);
        producer.close();
        assertThrows(IllegalStateException.class, producer::initTransactions);
    }

    @Test
    public void testCloseIsForcedOnPendingFindCoordinator() throws InterruptedException {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");
        configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "this-is-a-transactional-id");

        Time time = new MockTime();
        MetadataResponse initialUpdateResponse = RequestTestUtils.metadataUpdateWith(1, singletonMap("testTopic", 1));
        ProducerMetadata metadata = newMetadata(0, 0, Long.MAX_VALUE);
        metadata.updateWithCurrentRequestVersion(initialUpdateResponse, false, time.milliseconds());

        MockClient client = new MockClient(time, metadata);

        Producer<String, String> producer = kafkaProducer(configs, new StringSerializer(), new StringSerializer(),
                metadata, client, null, time);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        CountDownLatch assertionDoneLatch = new CountDownLatch(1);
        executorService.submit(() -> {
            assertThrows(KafkaException.class, producer::initTransactions);
            assertionDoneLatch.countDown();
        });

        client.waitForRequests(1, 2000);
        producer.close(Duration.ofMillis(1000));
        assertionDoneLatch.await(5000, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testCloseIsForcedOnPendingInitProducerId() throws InterruptedException {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");
        configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "this-is-a-transactional-id");

        Time time = new MockTime();
        MetadataResponse initialUpdateResponse = RequestTestUtils.metadataUpdateWith(1, singletonMap("testTopic", 1));
        ProducerMetadata metadata = newMetadata(0, 0, Long.MAX_VALUE);
        metadata.updateWithCurrentRequestVersion(initialUpdateResponse, false, time.milliseconds());

        MockClient client = new MockClient(time, metadata);

        Producer<String, String> producer = kafkaProducer(configs, new StringSerializer(), new StringSerializer(),
                metadata, client, null, time);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        CountDownLatch assertionDoneLatch = new CountDownLatch(1);
        client.prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.NONE, "this-is-a-transactional-id", NODE));
        executorService.submit(() -> {
            assertThrows(KafkaException.class, producer::initTransactions);
            assertionDoneLatch.countDown();
        });

        client.waitForRequests(1, 2000);
        producer.close(Duration.ofMillis(1000));
        assertionDoneLatch.await(5000, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testCloseIsForcedOnPendingAddOffsetRequest() throws InterruptedException {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");
        configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "this-is-a-transactional-id");

        Time time = new MockTime();
        MetadataResponse initialUpdateResponse = RequestTestUtils.metadataUpdateWith(1, singletonMap("testTopic", 1));
        ProducerMetadata metadata = newMetadata(0, 0, Long.MAX_VALUE);
        metadata.updateWithCurrentRequestVersion(initialUpdateResponse, false, time.milliseconds());

        MockClient client = new MockClient(time, metadata);

        Producer<String, String> producer = kafkaProducer(configs, new StringSerializer(), new StringSerializer(),
                metadata, client, null, time);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        CountDownLatch assertionDoneLatch = new CountDownLatch(1);
        client.prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.NONE, "this-is-a-transactional-id", NODE));
        executorService.submit(() -> {
            assertThrows(KafkaException.class, producer::initTransactions);
            assertionDoneLatch.countDown();
        });

        client.waitForRequests(1, 2000);
        producer.close(Duration.ofMillis(1000));
        assertionDoneLatch.await(5000, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testProducerJmxPrefix() throws  Exception {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.put("client.id", "client-1");

        KafkaProducer<String, String> producer = new KafkaProducer<>(
                props, new StringSerializer(), new StringSerializer());

        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        MetricName testMetricName = producer.metrics.metricName("test-metric",
                "grp1", "test metric");
        producer.metrics.addMetric(testMetricName, new Avg());
        assertNotNull(server.getObjectInstance(new ObjectName("kafka.producer:type=grp1,client-id=client-1")));
        producer.close();
    }

    private static ProducerMetadata newMetadata(long refreshBackoffMs, long refreshBackoffMaxMs, long expirationMs) {
        return new ProducerMetadata(refreshBackoffMs, refreshBackoffMaxMs, expirationMs, DEFAULT_METADATA_IDLE_MS,
                new LogContext(), new ClusterResourceListeners(), Time.SYSTEM);
    }

    @Test
    public void configurableObjectsShouldSeeGeneratedClientId() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, SerializerForClientId.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SerializerForClientId.class.getName());
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, PartitionerForClientId.class.getName());
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ProducerInterceptorForClientId.class.getName());

        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props);
        assertNotNull(producer.getClientId());
        assertNotEquals(0, producer.getClientId().length());
        assertEquals(4, CLIENT_IDS.size());
        CLIENT_IDS.forEach(id -> assertEquals(id, producer.getClientId()));
        producer.close();
    }

    @Test
    public void testUnusedConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.put(SslConfigs.SSL_PROTOCOL_CONFIG, "TLS");
        ProducerConfig config = new ProducerConfig(ProducerConfig.appendSerializerToConfig(props,
                new StringSerializer(), new StringSerializer()));

        assertTrue(config.unused().contains(SslConfigs.SSL_PROTOCOL_CONFIG));

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(config, null, null,
                null, null, null, null, Time.SYSTEM)) {
            assertTrue(config.unused().contains(SslConfigs.SSL_PROTOCOL_CONFIG));
        }
    }

    @Test
    public void testNullTopicName() {
        // send a record with null topic should fail
        assertThrows(IllegalArgumentException.class, () -> new ProducerRecord<>(null, 1,
            "key".getBytes(StandardCharsets.UTF_8), "value".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void testCallbackAndInterceptorHandleError() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");
        configs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");
        configs.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, MockProducerInterceptor.class.getName());
        configs.put(MockProducerInterceptor.APPEND_STRING_PROP, "something");


        Time time = new MockTime();
        ProducerMetadata producerMetadata = newMetadata(0, 0, Long.MAX_VALUE);
        MockClient client = new MockClient(time, producerMetadata);

        String invalidTopicName = "topic abc"; // Invalid topic name due to space

        ProducerInterceptors<String, String> producerInterceptors =
                new ProducerInterceptors<>(Collections.singletonList(new MockProducerInterceptor()));

        try (Producer<String, String> producer = kafkaProducer(configs, new StringSerializer(), new StringSerializer(),
                producerMetadata, client, producerInterceptors, time)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(invalidTopicName, "HelloKafka");

            // Here's the important piece of the test. Let's make sure that the RecordMetadata we get
            // is non-null and adheres to the onCompletion contract.
            Callback callBack = (recordMetadata, exception) -> {
                assertNotNull(exception);
                assertNotNull(recordMetadata);

                assertNotNull(recordMetadata.topic(), "Topic name should be valid even on send failure");
                assertEquals(invalidTopicName, recordMetadata.topic());

                assertFalse(recordMetadata.hasOffset());
                assertEquals(ProduceResponse.INVALID_OFFSET, recordMetadata.offset());

                assertFalse(recordMetadata.hasTimestamp());
                assertEquals(RecordBatch.NO_TIMESTAMP, recordMetadata.timestamp());

                assertEquals(-1, recordMetadata.serializedKeySize());
                assertEquals(-1, recordMetadata.serializedValueSize());
                assertEquals(-1, recordMetadata.partition());
            };

            producer.send(record, callBack);
            assertEquals(1, MockProducerInterceptor.ON_ACKNOWLEDGEMENT_COUNT.intValue());
        }
    }

    @Test
    public void negativePartitionShouldThrow() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");
        configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, BuggyPartitioner.class.getName());

        Time time = new MockTime(1);
        MetadataResponse initialUpdateResponse = RequestTestUtils.metadataUpdateWith(1, singletonMap("topic", 1));
        ProducerMetadata metadata = newMetadata(0, 0, Long.MAX_VALUE);

        MockClient client = new MockClient(time, metadata);
        client.updateMetadata(initialUpdateResponse);

        try (Producer<String, String> producer = kafkaProducer(configs, new StringSerializer(),
                new StringSerializer(), metadata, client, null, time)) {
            assertThrows(IllegalArgumentException.class, () -> producer.send(new ProducerRecord<>("topic", "key", "value")));
        }
    }

    @Test
    public void testPartitionAddedToTransaction() throws Exception {
        StringSerializer serializer = new StringSerializer();
        KafkaProducerTestContext<String> ctx = new KafkaProducerTestContext<>(testInfo, serializer);

        String topic = "foo";
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        Cluster cluster = TestUtils.singletonCluster(topic, 1);

        when(ctx.sender.isRunning()).thenReturn(true);
        when(ctx.metadata.fetch()).thenReturn(cluster);

        long timestamp = ctx.time.milliseconds();
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, timestamp, "key", "value");
        FutureRecordMetadata future = expectAppend(ctx, record, topicPartition, cluster);

        try (KafkaProducer<String, String> producer = ctx.newKafkaProducer()) {
            assertEquals(future, producer.send(record));
            assertFalse(future.isDone());
            verify(ctx.transactionManager).maybeAddPartition(topicPartition);
        }
    }

    private <T> FutureRecordMetadata expectAppend(
        KafkaProducerTestContext<T> ctx,
        ProducerRecord<T, T> record,
        TopicPartition initialSelectedPartition,
        Cluster cluster
    ) throws InterruptedException {
        byte[] serializedKey = ctx.serializer.serialize(topic, record.key());
        byte[] serializedValue = ctx.serializer.serialize(topic, record.value());
        long timestamp = record.timestamp() == null ? ctx.time.milliseconds() : record.timestamp();

        ProduceRequestResult requestResult = new ProduceRequestResult(initialSelectedPartition);
        FutureRecordMetadata futureRecordMetadata = new FutureRecordMetadata(
            requestResult,
            5,
            timestamp,
            serializedKey.length,
            serializedValue.length,
            ctx.time
        );

        when(ctx.partitioner.partition(
            initialSelectedPartition.topic(),
            record.key(),
            serializedKey,
            record.value(),
            serializedValue,
            cluster
        )).thenReturn(initialSelectedPartition.partition());

        when(ctx.accumulator.append(
            eq(initialSelectedPartition.topic()),            // 0
            eq(initialSelectedPartition.partition()),        // 1
            eq(timestamp),                                   // 2
            eq(serializedKey),                               // 3
            eq(serializedValue),                             // 4
            eq(Record.EMPTY_HEADERS),                        // 5
            any(RecordAccumulator.AppendCallbacks.class),    // 6 <--
            anyLong(),
            anyLong(),
            any()
        )).thenAnswer(invocation -> {
            RecordAccumulator.AppendCallbacks callbacks =
                (RecordAccumulator.AppendCallbacks) invocation.getArguments()[6];
            callbacks.setPartition(initialSelectedPartition.partition());
            return new RecordAccumulator.RecordAppendResult(
                futureRecordMetadata,
                false,
                false,
                0);
        });

        return futureRecordMetadata;
    }

    private static final List<String> CLIENT_IDS = new ArrayList<>();

    public static class SerializerForClientId implements Serializer<byte[]> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            CLIENT_IDS.add(configs.get(ProducerConfig.CLIENT_ID_CONFIG).toString());
        }

        @Override
        public byte[] serialize(String topic, byte[] data) {
            return data;
        }
    }

    public static class PartitionerForClientId implements Partitioner {

        @Override
        public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
            return 0;
        }

        @Override
        public void close() {

        }

        @Override
        public void configure(Map<String, ?> configs) {
            CLIENT_IDS.add(configs.get(ProducerConfig.CLIENT_ID_CONFIG).toString());
        }
    }

    public static class ProducerInterceptorForClientId implements ProducerInterceptor<byte[], byte[]> {

        @Override
        public ProducerRecord<byte[], byte[]> onSend(ProducerRecord<byte[], byte[]> record) {
            return record;
        }

        @Override
        public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        }

        @Override
        public void close() {
        }

        @Override
        public void configure(Map<String, ?> configs) {
            CLIENT_IDS.add(configs.get(ProducerConfig.CLIENT_ID_CONFIG).toString());
        }
    }

    public static class BuggyPartitioner implements Partitioner {

        @Override
        public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
            return -1;
        }

        @Override
        public void close() {
        }

        @Override
        public void configure(Map<String, ?> configs) {
        }
    }

    private static class KafkaProducerTestContext<T> {
        private final TestInfo testInfo;
        private final Map<String, Object> configs;
        private final Serializer<T> serializer;
        private final Partitioner partitioner = mock(Partitioner.class);
        private final KafkaThread ioThread = mock(KafkaThread.class);
        private final List<ProducerInterceptor<T, T>> interceptors = new ArrayList<>();
        private ProducerMetadata metadata = mock(ProducerMetadata.class);
        private RecordAccumulator accumulator = mock(RecordAccumulator.class);
        private Sender sender = mock(Sender.class);
        private TransactionManager transactionManager = mock(TransactionManager.class);
        private Time time = new MockTime();
        private final Metrics metrics = new Metrics(time);

        public KafkaProducerTestContext(
            TestInfo testInfo,
            Serializer<T> serializer
        ) {
            this(testInfo, new HashMap<>(), serializer);
        }

        public KafkaProducerTestContext(
            TestInfo testInfo,
            Map<String, Object> configs,
            Serializer<T> serializer
        ) {
            this.testInfo = testInfo;
            this.configs = configs;
            this.serializer = serializer;

            if (!configs.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
                configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
            }
        }

        public KafkaProducerTestContext<T> setProducerMetadata(ProducerMetadata metadata) {
            this.metadata = metadata;
            return this;
        }

        public KafkaProducerTestContext<T> setAccumulator(RecordAccumulator accumulator) {
            this.accumulator = accumulator;
            return this;
        }

        public KafkaProducerTestContext<T> setSender(Sender sender) {
            this.sender = sender;
            return this;
        }

        public KafkaProducerTestContext<T> setTransactionManager(TransactionManager transactionManager) {
            this.transactionManager = transactionManager;
            return this;
        }

        public KafkaProducerTestContext<T> addInterceptor(ProducerInterceptor<T, T> interceptor) {
            this.interceptors.add(interceptor);
            return this;
        }

        public KafkaProducerTestContext<T> setTime(Time time) {
            this.time = time;
            return this;
        }

        public KafkaProducer<T, T> newKafkaProducer() {
            LogContext logContext = new LogContext("[Producer test=" + testInfo.getDisplayName() + "] ");

            ProducerConfig producerConfig = new ProducerConfig(
                ProducerConfig.appendSerializerToConfig(configs, serializer, serializer));

            ProducerInterceptors<T, T> interceptors = new ProducerInterceptors<>(this.interceptors);

            return new KafkaProducer<>(
                producerConfig,
                logContext,
                metrics,
                serializer,
                serializer,
                metadata,
                accumulator,
                transactionManager,
                sender,
                interceptors,
                partitioner,
                time,
                ioThread,
                Optional.empty()
            );
        }
    }

    @Test
    void testDeliveryTimeoutAndLingerMsConfig() {
        final Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.CLIENT_ID_CONFIG, "testDeliveryTimeoutAndLingerMsConfig");
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        configs.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 1000);
        configs.put(ProducerConfig.LINGER_MS_CONFIG, 1000);
        configs.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1);

        // delivery.timeout.ms should be equal to or larger than linger.ms + request.timeout.ms
        assertThrows(KafkaException.class, () -> new KafkaProducer<>(configs, new StringSerializer(), new StringSerializer()));

        configs.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 1000);
        configs.put(ProducerConfig.LINGER_MS_CONFIG, 999);
        configs.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1);

        assertDoesNotThrow(() -> new KafkaProducer<>(configs, new StringSerializer(), new StringSerializer()).close());
    }

    @SuppressWarnings("deprecation")
    private ProduceResponse produceResponse(TopicPartition tp, long offset, Errors error, int throttleTimeMs, int logStartOffset) {
        ProduceResponse.PartitionResponse resp = new ProduceResponse.PartitionResponse(error, offset, RecordBatch.NO_TIMESTAMP, logStartOffset);
        Map<TopicPartition, ProduceResponse.PartitionResponse> partResp = singletonMap(tp, resp);
        return new ProduceResponse(partResp, throttleTimeMs);
    }

    @Test
    public void testSubscribingCustomMetricsDoesntAffectProducerMetrics() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");

        KafkaProducer<String, String> producer = new KafkaProducer<>(
            props, new StringSerializer(), new StringSerializer());

        Map<MetricName, KafkaMetric> customMetrics = customMetrics();
        customMetrics.forEach((name, metric) -> producer.registerMetricForSubscription(metric));

        Map<MetricName, ? extends Metric> producerMetrics = producer.metrics();
        customMetrics.forEach((name, metric) -> assertFalse(producerMetrics.containsKey(name)));
    }

    @Test
    public void testUnSubscribingNonExisingMetricsDoesntCauseError() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");

        KafkaProducer<String, String> producer = new KafkaProducer<>(
            props, new StringSerializer(), new StringSerializer());

        Map<MetricName, KafkaMetric> customMetrics = customMetrics();
        //Metrics never registered but removed should not cause an error
        customMetrics.forEach((name, metric) -> assertDoesNotThrow(() -> producer.unregisterMetricFromSubscription(metric)));
    }

    @Test
    public void testSubscribingCustomMetricsWithSameNameDoesntAffectProducerMetrics() {
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister()) {
            appender.setClassLogger(KafkaProducer.class, Level.DEBUG);
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
            KafkaProducer<String, String> producer = new KafkaProducer<>(
                    props, new StringSerializer(), new StringSerializer());
            KafkaMetric existingMetricToAdd = (KafkaMetric) producer.metrics().entrySet().iterator().next().getValue();
            producer.registerMetricForSubscription(existingMetricToAdd);
            final String expectedMessage = String.format("Skipping registration for metric %s. Existing producer metrics cannot be overwritten.", existingMetricToAdd.metricName());
            assertTrue(appender.getMessages().stream().anyMatch(m -> m.contains(expectedMessage)));
        }
    }

    @Test
    public void testUnsubscribingCustomMetricWithSameNameAsExistingMetricDoesntAffectProducerMetric() {
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister()) {
            appender.setClassLogger(KafkaProducer.class, Level.DEBUG);
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
            KafkaProducer<String, String> producer = new KafkaProducer<>(
                    props, new StringSerializer(), new StringSerializer());
            KafkaMetric existingMetricToRemove = (KafkaMetric) producer.metrics().entrySet().iterator().next().getValue();
            producer.unregisterMetricFromSubscription(existingMetricToRemove);
            final String expectedMessage = String.format("Skipping unregistration for metric %s. Existing producer metrics cannot be removed.", existingMetricToRemove.metricName());
            assertTrue(appender.getMessages().stream().anyMatch(m -> m.contains(expectedMessage)));
        }
    }

    @Test
    public void testShouldOnlyCallMetricReporterMetricChangeOnceWithExistingProducerMetric() {
        try (MockedStatic<CommonClientConfigs> mockedCommonClientConfigs = mockStatic(CommonClientConfigs.class, new CallsRealMethods())) {
            ClientTelemetryReporter clientTelemetryReporter = mock(ClientTelemetryReporter.class);
            clientTelemetryReporter.configure(any());
            mockedCommonClientConfigs.when(() -> CommonClientConfigs.telemetryReporter(anyString(), any())).thenReturn(Optional.of(clientTelemetryReporter));

            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
            KafkaProducer<String, String> producer = new KafkaProducer<>(
                    props, new StringSerializer(), new StringSerializer());
            KafkaMetric existingMetric = (KafkaMetric) producer.metrics().entrySet().iterator().next().getValue();
            producer.registerMetricForSubscription(existingMetric);
            // This test would fail without the check as the exising metric is registered in the producer on startup
            Mockito.verify(clientTelemetryReporter, atMostOnce()).metricChange(existingMetric);
        }
    }

    @Test
    public void testShouldNotCallMetricReporterMetricRemovalWithExistingProducerMetric() {
        try (MockedStatic<CommonClientConfigs> mockedCommonClientConfigs = mockStatic(CommonClientConfigs.class, new CallsRealMethods())) {
            ClientTelemetryReporter clientTelemetryReporter = mock(ClientTelemetryReporter.class);
            clientTelemetryReporter.configure(any());
            mockedCommonClientConfigs.when(() -> CommonClientConfigs.telemetryReporter(anyString(), any())).thenReturn(Optional.of(clientTelemetryReporter));

            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
            KafkaProducer<String, String> producer = new KafkaProducer<>(
                    props, new StringSerializer(), new StringSerializer());
            KafkaMetric existingMetric = (KafkaMetric) producer.metrics().entrySet().iterator().next().getValue();
            producer.unregisterMetricFromSubscription(existingMetric);
            // This test would fail without the check as the exising metric is registered in the consumer on startup
            Mockito.verify(clientTelemetryReporter, never()).metricRemoval(existingMetric);
        }
    }


    private Map<MetricName, KafkaMetric> customMetrics() {
        MetricConfig metricConfig = new MetricConfig();
        Object lock = new Object();
        MetricName metricNameOne = new MetricName("metricOne", "stream-metrics", "description for metric one", new HashMap<>());
        MetricName metricNameTwo = new MetricName("metricTwo", "stream-metrics", "description for metric two", new HashMap<>());

        KafkaMetric streamClientMetricOne = new KafkaMetric(lock, metricNameOne, (Measurable) (m, now) -> 1.0, metricConfig, Time.SYSTEM);
        KafkaMetric streamClientMetricTwo = new KafkaMetric(lock, metricNameTwo, (Measurable) (m, now) -> 2.0, metricConfig, Time.SYSTEM);
        return Map.of(metricNameOne, streamClientMetricOne, metricNameTwo, streamClientMetricTwo);
    }
}
