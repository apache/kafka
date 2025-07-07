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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.TestUtils;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.Long.MAX_VALUE;
import static java.time.Duration.ofMillis;
import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.apache.kafka.streams.StreamsConfig.AT_LEAST_ONCE;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.DEFAULT_TIMEOUT;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.cleanStateBeforeTest;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.quietlyCleanStateAfterTest;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.maxBytes;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.maxRecords;
import static org.apache.kafka.streams.kstream.Suppressed.untilTimeLimit;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;

@Tag("integration")
@Timeout(600)
public class SuppressionIntegrationTest {
    private static final long NOW = Instant.now().toEpochMilli();
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }
    private static final StringSerializer STRING_SERIALIZER = new StringSerializer();
    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final int COMMIT_INTERVAL = 100;

    private static KTable<String, Long> buildCountsTable(final String input, final StreamsBuilder builder) {
        return builder
            .table(
                input,
                Consumed.with(STRING_SERDE, STRING_SERDE),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>with(STRING_SERDE, STRING_SERDE)
                    .withCachingDisabled()
                    .withLoggingDisabled()
            )
            .groupBy((k, v) -> new KeyValue<>(v, k), Grouped.with(STRING_SERDE, STRING_SERDE))
            .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts").withCachingDisabled());
    }

    @Test
    public void shouldUseDefaultSerdes() {
        final String testId = "-shouldInheritSerdes";
        final String appId = getClass().getSimpleName().toLowerCase(Locale.getDefault()) + testId;
        final String input = "input" + testId;
        final String outputSuppressed = "output-suppressed" + testId;
        final String outputRaw = "output-raw" + testId;

        cleanStateBeforeTest(CLUSTER, input, outputRaw, outputSuppressed);

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> inputStream = builder.stream(input);

        final KTable<String, String> valueCounts = inputStream
            .groupByKey()
            .aggregate(() -> "()", (key, value, aggregate) -> aggregate + ",(" + key + ": " + value + ")");

        valueCounts
            .suppress(untilTimeLimit(ofMillis(MAX_VALUE), maxRecords(1L).emitEarlyWhenFull()))
            .toStream()
            .to(outputSuppressed);

        valueCounts
            .toStream()
            .to(outputRaw);

        final Properties streamsConfig = getStreamsConfig(appId);
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        final KafkaStreams driver = IntegrationTestUtils.getStartedStreams(streamsConfig, builder, true);
        try {
            produceSynchronously(
                input,
                asList(
                    new KeyValueTimestamp<>("k1", "v1", scaledTime(0L)),
                    new KeyValueTimestamp<>("k1", "v2", scaledTime(1L)),
                    new KeyValueTimestamp<>("k2", "v1", scaledTime(2L)),
                    new KeyValueTimestamp<>("x", "x", scaledTime(3L))
                )
            );
            final boolean rawRecords = waitForAnyRecord(outputRaw);
            final boolean suppressedRecords = waitForAnyRecord(outputSuppressed);
            assertThat(rawRecords, Matchers.is(true));
            assertThat(suppressedRecords, is(true));
        } finally {
            driver.close();
            quietlyCleanStateAfterTest(CLUSTER, driver);
        }
    }

    @Test
    public void shouldInheritSerdes() {
        final String testId = "-shouldInheritSerdes";
        final String appId = getClass().getSimpleName().toLowerCase(Locale.getDefault()) + testId;
        final String input = "input" + testId;
        final String outputSuppressed = "output-suppressed" + testId;
        final String outputRaw = "output-raw" + testId;

        cleanStateBeforeTest(CLUSTER, input, outputRaw, outputSuppressed);

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> inputStream = builder.stream(input);

        // count sets the serde to Long
        final KTable<String, Long> valueCounts = inputStream
            .groupByKey()
            .count();

        valueCounts
            .suppress(untilTimeLimit(ofMillis(MAX_VALUE), maxRecords(1L).emitEarlyWhenFull()))
            .toStream()
            .to(outputSuppressed);

        valueCounts
            .toStream()
            .to(outputRaw);

        final Properties streamsConfig = getStreamsConfig(appId);
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        final KafkaStreams driver = IntegrationTestUtils.getStartedStreams(streamsConfig, builder, true);
        try {
            produceSynchronously(
                input,
                asList(
                    new KeyValueTimestamp<>("k1", "v1", scaledTime(0L)),
                    new KeyValueTimestamp<>("k1", "v2", scaledTime(1L)),
                    new KeyValueTimestamp<>("k2", "v1", scaledTime(2L)),
                    new KeyValueTimestamp<>("x", "x", scaledTime(3L))
                )
            );
            final boolean rawRecords = waitForAnyRecord(outputRaw);
            final boolean suppressedRecords = waitForAnyRecord(outputSuppressed);
            assertThat(rawRecords, Matchers.is(true));
            assertThat(suppressedRecords, is(true));
        } finally {
            driver.close();
            quietlyCleanStateAfterTest(CLUSTER, driver);
        }
    }

    private static boolean waitForAnyRecord(final String topic) {
        final Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        try (final Consumer<Object, Object> consumer = new KafkaConsumer<>(properties)) {
            final List<TopicPartition> partitions =
                consumer.partitionsFor(topic)
                        .stream()
                        .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
                        .collect(Collectors.toList());
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            final long start = System.currentTimeMillis();
            while ((System.currentTimeMillis() - start) < DEFAULT_TIMEOUT) {
                final ConsumerRecords<Object, Object> records = consumer.poll(ofMillis(500));

                if (!records.isEmpty()) {
                    return true;
                }
            }

            return false;
        }
    }

    @Test
    public void shouldShutdownWhenRecordConstraintIsViolated() throws InterruptedException {
        final String testId = "-shouldShutdownWhenRecordConstraintIsViolated";
        final String appId = getClass().getSimpleName().toLowerCase(Locale.getDefault()) + testId;
        final String input = "input" + testId;
        final String outputSuppressed = "output-suppressed" + testId;
        final String outputRaw = "output-raw" + testId;

        cleanStateBeforeTest(CLUSTER, input, outputRaw, outputSuppressed);

        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<String, Long> valueCounts = buildCountsTable(input, builder);

        valueCounts
            .suppress(untilTimeLimit(ofMillis(MAX_VALUE), maxRecords(1L).shutDownWhenFull()))
            .toStream()
            .to(outputSuppressed, Produced.with(STRING_SERDE, Serdes.Long()));

        valueCounts
            .toStream()
            .to(outputRaw, Produced.with(STRING_SERDE, Serdes.Long()));

        final Properties streamsConfig = getStreamsConfig(appId);
        final KafkaStreams driver = IntegrationTestUtils.getStartedStreams(streamsConfig, builder, true);
        try {
            produceSynchronously(
                input,
                asList(
                    new KeyValueTimestamp<>("k1", "v1", scaledTime(0L)),
                    new KeyValueTimestamp<>("k1", "v2", scaledTime(1L)),
                    new KeyValueTimestamp<>("k2", "v1", scaledTime(2L)),
                    new KeyValueTimestamp<>("x", "x", scaledTime(3L))
                )
            );
            verifyErrorShutdown(driver);
        } finally {
            driver.close();
            quietlyCleanStateAfterTest(CLUSTER, driver);
        }
    }

    @Test
    public void shouldShutdownWhenBytesConstraintIsViolated() throws InterruptedException {
        final String testId = "-shouldShutdownWhenBytesConstraintIsViolated";
        final String appId = getClass().getSimpleName().toLowerCase(Locale.getDefault()) + testId;
        final String input = "input" + testId;
        final String outputSuppressed = "output-suppressed" + testId;
        final String outputRaw = "output-raw" + testId;

        cleanStateBeforeTest(CLUSTER, input, outputRaw, outputSuppressed);

        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<String, Long> valueCounts = buildCountsTable(input, builder);

        valueCounts
            // this is a bit brittle, but I happen to know that the entries are a little over 100 bytes in size.
            .suppress(untilTimeLimit(ofMillis(MAX_VALUE), maxBytes(200L).shutDownWhenFull()))
            .toStream()
            .to(outputSuppressed, Produced.with(STRING_SERDE, Serdes.Long()));

        valueCounts
            .toStream()
            .to(outputRaw, Produced.with(STRING_SERDE, Serdes.Long()));

        final Properties streamsConfig = getStreamsConfig(appId);
        final KafkaStreams driver = IntegrationTestUtils.getStartedStreams(streamsConfig, builder, true);
        try {
            produceSynchronously(
                input,
                asList(
                    new KeyValueTimestamp<>("k1", "v1", scaledTime(0L)),
                    new KeyValueTimestamp<>("k1", "v2", scaledTime(1L)),
                    new KeyValueTimestamp<>("k2", "v1", scaledTime(2L)),
                    new KeyValueTimestamp<>("x", "x", scaledTime(3L))
                )
            );
            verifyErrorShutdown(driver);
        } finally {
            driver.close();
            quietlyCleanStateAfterTest(CLUSTER, driver);
        }
    }

    @Test
    public void shouldAllowOverridingChangelogConfig() {
        final String testId = "-shouldAllowOverridingChangelogConfig";
        final String appId = getClass().getSimpleName().toLowerCase(Locale.getDefault()) + testId;
        final String input = "input" + testId;
        final String outputSuppressed = "output-suppressed" + testId;
        final String outputRaw = "output-raw" + testId;
        final Map<String, String> logConfig = Collections.singletonMap("retention.ms", "1000");
        final String changeLog = "suppressionintegrationtest-shouldAllowOverridingChangelogConfig-KTABLE-SUPPRESS-STATE-STORE-0000000004-changelog";

        cleanStateBeforeTest(CLUSTER, input, outputRaw, outputSuppressed);

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> inputStream = builder.stream(input);

        final KTable<String, String> valueCounts = inputStream
            .groupByKey()
            .aggregate(() -> "()", (key, value, aggregate) -> aggregate + ",(" + key + ": " + value + ")");

        valueCounts
            .suppress(untilTimeLimit(ofMillis(MAX_VALUE), maxRecords(1L)
                .emitEarlyWhenFull()
                .withLoggingEnabled(logConfig)))
            .toStream()
            .to(outputSuppressed);

        valueCounts
            .toStream()
            .to(outputRaw);

        final Properties streamsConfig = getStreamsConfig(appId);
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        final KafkaStreams driver = IntegrationTestUtils.getStartedStreams(streamsConfig, builder, true);
        try {
            produceSynchronously(
                input,
                asList(
                    new KeyValueTimestamp<>("k1", "v1", scaledTime(0L)),
                    new KeyValueTimestamp<>("k1", "v2", scaledTime(1L)),
                    new KeyValueTimestamp<>("k2", "v1", scaledTime(2L)),
                    new KeyValueTimestamp<>("x", "x", scaledTime(3L))
                )
            );
            final boolean rawRecords = waitForAnyRecord(outputRaw);
            final boolean suppressedRecords = waitForAnyRecord(outputSuppressed);
            final Properties config = CLUSTER.getLogConfig(changeLog);

            assertThat(config.getProperty("retention.ms"), is(logConfig.get("retention.ms")));
            assertThat(CLUSTER.getAllTopicsInCluster(), hasItem(changeLog));
            assertThat(rawRecords, Matchers.is(true));
            assertThat(suppressedRecords, is(true));
        } finally {
            driver.close();
            quietlyCleanStateAfterTest(CLUSTER, driver);
        }
    }

    @Test
    public void shouldCreateChangelogByDefault() {
        final String testId = "-shouldCreateChangelogByDefault";
        final String appId = getClass().getSimpleName().toLowerCase(Locale.getDefault()) + testId;
        final String input = "input" + testId;
        final String outputSuppressed = "output-suppressed" + testId;
        final String outputRaw = "output-raw" + testId;
        final String changeLog = "suppressionintegrationtest-shouldCreateChangelogByDefault-KTABLE-SUPPRESS-STATE-STORE-0000000004-changelog";

        cleanStateBeforeTest(CLUSTER, input, outputRaw, outputSuppressed);

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> inputStream = builder.stream(input);

        final KTable<String, String> valueCounts = inputStream
            .groupByKey()
            .aggregate(() -> "()", (key, value, aggregate) -> aggregate + ",(" + key + ": " + value + ")");

        valueCounts
            .suppress(untilTimeLimit(ofMillis(MAX_VALUE), maxRecords(1L)
                .emitEarlyWhenFull()))
            .toStream()
            .to(outputSuppressed);

        valueCounts
            .toStream()
            .to(outputRaw);

        final Properties streamsConfig = getStreamsConfig(appId);
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        final KafkaStreams driver = IntegrationTestUtils.getStartedStreams(streamsConfig, builder, true);
        try {
            produceSynchronously(
                input,
                asList(
                    new KeyValueTimestamp<>("k1", "v1", scaledTime(0L)),
                    new KeyValueTimestamp<>("k1", "v2", scaledTime(1L)),
                    new KeyValueTimestamp<>("k2", "v1", scaledTime(2L)),
                    new KeyValueTimestamp<>("x", "x", scaledTime(3L))
                )
            );
            final boolean rawRecords = waitForAnyRecord(outputRaw);
            final boolean suppressedRecords = waitForAnyRecord(outputSuppressed);

            assertThat(CLUSTER.getAllTopicsInCluster(), hasItem(changeLog));
            assertThat(rawRecords, Matchers.is(true));
            assertThat(suppressedRecords, is(true));
        } finally {
            driver.close();
            quietlyCleanStateAfterTest(CLUSTER, driver);
        }
    }

    @Test
    public void shouldAllowDisablingChangelog() {
        final String testId = "-shouldAllowDisablingChangelog";
        final String appId = getClass().getSimpleName().toLowerCase(Locale.getDefault()) + testId;
        final String input = "input" + testId;
        final String outputSuppressed = "output-suppressed" + testId;
        final String outputRaw = "output-raw" + testId;

        cleanStateBeforeTest(CLUSTER, input, outputRaw, outputSuppressed);

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> inputStream = builder.stream(input);

        final KTable<String, String> valueCounts = inputStream
            .groupByKey()
            .aggregate(() -> "()", (key, value, aggregate) -> aggregate + ",(" + key + ": " + value + ")");

        valueCounts
            .suppress(untilTimeLimit(ofMillis(MAX_VALUE), maxRecords(1L)
                .emitEarlyWhenFull()
                .withLoggingDisabled()))
            .toStream()
            .to(outputSuppressed);

        valueCounts
            .toStream()
            .to(outputRaw);

        final Properties streamsConfig = getStreamsConfig(appId);
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        final KafkaStreams driver = IntegrationTestUtils.getStartedStreams(streamsConfig, builder, true);
        try {
            produceSynchronously(
                input,
                asList(
                    new KeyValueTimestamp<>("k1", "v1", scaledTime(0L)),
                    new KeyValueTimestamp<>("k1", "v2", scaledTime(1L)),
                    new KeyValueTimestamp<>("k2", "v1", scaledTime(2L)),
                    new KeyValueTimestamp<>("x", "x", scaledTime(3L))
                )
            );
            final boolean rawRecords = waitForAnyRecord(outputRaw);
            final boolean suppressedRecords = waitForAnyRecord(outputSuppressed);
            final Set<String> suppressChangeLog = CLUSTER.getAllTopicsInCluster()
                .stream()
                .filter(s -> s.contains("-changelog"))
                .filter(s -> s.contains("KTABLE-SUPPRESS"))
                .collect(Collectors.toSet());

            assertThat(suppressChangeLog, is(empty()));
            assertThat(rawRecords, Matchers.is(true));
            assertThat(suppressedRecords, is(true));
        } finally {
            driver.close();
            quietlyCleanStateAfterTest(CLUSTER, driver);
        }
    }

    private static Properties getStreamsConfig(final String appId) {
        return mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, appId),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
            mkEntry(StreamsConfig.POLL_MS_CONFIG, Integer.toString(COMMIT_INTERVAL)),
            mkEntry(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Long.toString(COMMIT_INTERVAL)),
            mkEntry(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, AT_LEAST_ONCE),
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath())
        ));
    }

    /**
     * scaling to ensure that there are commits in between the various test events,
     * just to exercise that everything works properly in the presence of commits.
     */
    private static long scaledTime(final long unscaledTime) {
        return NOW + COMMIT_INTERVAL * 2 * unscaledTime;
    }

    private static void produceSynchronously(final String topic, final List<KeyValueTimestamp<String, String>> toProduce) {
        final Properties producerConfig = mkProperties(mkMap(
            mkEntry(ProducerConfig.CLIENT_ID_CONFIG, "anything"),
            mkEntry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ((Serializer<String>) STRING_SERIALIZER).getClass().getName()),
            mkEntry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ((Serializer<String>) STRING_SERIALIZER).getClass().getName()),
            mkEntry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
        ));
        IntegrationTestUtils.produceSynchronously(producerConfig, false, topic, Optional.empty(), toProduce);
    }

    private static void verifyErrorShutdown(final KafkaStreams driver) throws InterruptedException {
        waitForCondition(() -> !driver.state().isRunningOrRebalancing(), DEFAULT_TIMEOUT, "Streams didn't shut down.");
        waitForCondition(() -> driver.state() == KafkaStreams.State.ERROR, "Streams didn't transit to ERROR state");
    }
}
