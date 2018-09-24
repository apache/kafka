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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
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
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.IntegrationTest;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Duration;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.Suppressed.untilTimeLimit;

@Category({IntegrationTest.class})
public class SuppressionIntegrationTest {
    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);
    private static final StringDeserializer STRING_DESERIALIZER = new StringDeserializer();
    private static final StringSerializer STRING_SERIALIZER = new StringSerializer();
    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final LongDeserializer LONG_DESERIALIZER = new LongDeserializer();
    private static final int COMMIT_INTERVAL = 100;
    private static final int SCALE_FACTOR = COMMIT_INTERVAL * 2;

    @Test
    public void shouldNotSuppressIntermediateEventsWithZeroEmitAfter() throws InterruptedException {
        final String testId = "-shouldNotSuppressIntermediateEventsWithZeroEmitAfter";
        final String appId = getClass().getSimpleName().toLowerCase(Locale.getDefault()) + testId;
        final String input = "input" + testId;
        final String outputSuppressed = "output-suppressed" + testId;
        final String outputRaw = "output-raw" + testId;

        cleanStateBeforeTest(input, outputSuppressed, outputRaw);

        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<String, Long> valueCounts = builder
            .table(
                input,
                Consumed.with(STRING_SERDE, STRING_SERDE),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>with(STRING_SERDE, STRING_SERDE)
                    .withCachingDisabled()
                    .withLoggingDisabled()
            )
            .groupBy((k, v) -> new KeyValue<>(v, k), Serialized.with(STRING_SERDE, STRING_SERDE))
            .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts").withCachingDisabled());

        valueCounts
            .suppress(untilTimeLimit(Duration.ZERO, unbounded()))
            .toStream()
            .to(outputSuppressed, Produced.with(STRING_SERDE, Serdes.Long()));

        valueCounts
            .toStream()
            .to(outputRaw, Produced.with(STRING_SERDE, Serdes.Long()));

        final KafkaStreams driver = getCleanStartedStreams(appId, builder);

        try {
            produceSynchronously(
                input,
                asList(
                    new KeyValueTimestamp<>("k1", "v1", scaledTime(0L)),
                    new KeyValueTimestamp<>("k1", "v2", scaledTime(1L)),
                    new KeyValueTimestamp<>("k2", "v1", scaledTime(2L)),
                    new KeyValueTimestamp<>("x", "x", scaledTime(4L))
                )
            );
            verifyOutput(
                outputRaw,
                asList(
                    new KeyValueTimestamp<>("v1", 1L, scaledTime(0L)),
                    new KeyValueTimestamp<>("v1", 0L, scaledTime(1L)),
                    new KeyValueTimestamp<>("v2", 1L, scaledTime(1L)),
                    new KeyValueTimestamp<>("v1", 1L, scaledTime(2L)),
                    new KeyValueTimestamp<>("x", 1L, scaledTime(4L))
                )
            );
            verifyOutput(
                outputSuppressed,
                asList(
                    new KeyValueTimestamp<>("v1", 1L, scaledTime(0L)),
                    new KeyValueTimestamp<>("v1", 0L, scaledTime(1L)),
                    new KeyValueTimestamp<>("v2", 1L, scaledTime(1L)),
                    new KeyValueTimestamp<>("v1", 1L, scaledTime(2L)),
                    new KeyValueTimestamp<>("x", 1L, scaledTime(4L))
                )
            );
        } finally {
            driver.close();
            cleanStateAfterTest(driver);
        }
    }

    private void cleanStateBeforeTest(final String... topic) throws InterruptedException {
        CLUSTER.deleteAllTopicsAndWait(30_000L);
        for (final String s : topic) {
            CLUSTER.createTopic(s, 1, 1);
        }
    }

    private KafkaStreams getCleanStartedStreams(final String appId, final StreamsBuilder builder) {
        final Properties streamsConfig = mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, appId),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
            mkEntry(StreamsConfig.POLL_MS_CONFIG, Objects.toString(COMMIT_INTERVAL)),
            mkEntry(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Objects.toString(COMMIT_INTERVAL))
        ));
        final KafkaStreams driver = new KafkaStreams(builder.build(), streamsConfig);
        driver.cleanUp();
        driver.start();
        return driver;
    }

    private void cleanStateAfterTest(final KafkaStreams driver) throws InterruptedException {
        driver.cleanUp();
        CLUSTER.deleteAllTopicsAndWait(30_000L);
    }

    private long scaledTime(final long unscaledTime) {
        return SCALE_FACTOR * unscaledTime;
    }

    private void produceSynchronously(final String topic, final List<KeyValueTimestamp<String, String>> toProduce) {
        final Properties producerConfig = mkProperties(mkMap(
            mkEntry(ProducerConfig.CLIENT_ID_CONFIG, "anything"),
            mkEntry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER.getClass().getName()),
            mkEntry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER.getClass().getName()),
            mkEntry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
        ));
        try (final Producer<String, String> producer = new KafkaProducer<>(producerConfig)) {
            // TODO: test EOS
            //noinspection ConstantConditions
            if (false) {
                producer.initTransactions();
                producer.beginTransaction();
            }
            final LinkedList<Future<RecordMetadata>> futures = new LinkedList<>();
            for (final KeyValueTimestamp<String, String> record : toProduce) {
                final Future<RecordMetadata> f = producer.send(
                    new ProducerRecord<>(topic, null, record.timestamp(), record.key(), record.value(), null)
                );
                futures.add(f);
            }
            for (final Future<RecordMetadata> future : futures) {
                try {
                    future.get();
                } catch (final InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
            // TODO: test EOS
            //noinspection ConstantConditions
            if (false) {
                producer.commitTransaction();
            } else {
                producer.flush();
            }
        }
    }

    private void verifyOutput(final String topic, final List<KeyValueTimestamp<String, Long>> expected) {
        final List<ConsumerRecord<String, Long>> results;
        try {
            final Properties properties = mkProperties(
                mkMap(
                    mkEntry(ConsumerConfig.GROUP_ID_CONFIG, "test-group"),
                    mkEntry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
                    mkEntry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ((Deserializer<String>) STRING_DESERIALIZER).getClass().getName()),
                    mkEntry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ((Deserializer<Long>) LONG_DESERIALIZER).getClass().getName())
                )
            );
            results = IntegrationTestUtils.waitUntilMinRecordsReceived(properties, topic, expected.size());
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }

        if (results.size() != expected.size()) {
            throw new AssertionError(printRecords(results) + " != " + expected);
        }
        final Iterator<KeyValueTimestamp<String, Long>> expectedIterator = expected.iterator();
        for (final ConsumerRecord<String, Long> result : results) {
            final KeyValueTimestamp<String, Long> expected1 = expectedIterator.next();
            try {
                compareKeyValueTimestamp(result, expected1.key(), expected1.value(), expected1.timestamp());
            } catch (final AssertionError e) {
                throw new AssertionError(printRecords(results) + " != " + expected, e);
            }
        }
    }

    private <K, V> void compareKeyValueTimestamp(final ConsumerRecord<K, V> record, final K expectedKey, final V expectedValue, final long expectedTimestamp) {
        Objects.requireNonNull(record);
        final K recordKey = record.key();
        final V recordValue = record.value();
        final long recordTimestamp = record.timestamp();
        final AssertionError error = new AssertionError("Expected <" + expectedKey + ", " + expectedValue + "> with timestamp=" + expectedTimestamp +
                                                            " but was <" + recordKey + ", " + recordValue + "> with timestamp=" + recordTimestamp);
        if (recordKey != null) {
            if (!recordKey.equals(expectedKey)) {
                throw error;
            }
        } else if (expectedKey != null) {
            throw error;
        }
        if (recordValue != null) {
            if (!recordValue.equals(expectedValue)) {
                throw error;
            }
        } else if (expectedValue != null) {
            throw error;
        }
        if (recordTimestamp != expectedTimestamp) {
            throw error;
        }
    }

    private <K, V> String printRecords(final List<ConsumerRecord<K, V>> result) {
        final StringBuilder resultStr = new StringBuilder();
        resultStr.append("[\n");
        for (final ConsumerRecord<?, ?> record : result) {
            resultStr.append("  ").append(record.toString()).append("\n");
        }
        resultStr.append("]");
        return resultStr.toString();
    }
}