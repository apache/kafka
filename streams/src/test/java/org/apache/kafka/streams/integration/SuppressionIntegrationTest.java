package org.apache.kafka.streams.integration;

import kafka.common.TopicAlreadyMarkedForDeletionException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.Suppress;
import org.apache.kafka.streams.kstream.Suppress.BufferConfig;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.IntegrationTest;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.apache.kafka.streams.kstream.Suppress.BufferConfig.withBufferBytes;
import static org.apache.kafka.streams.kstream.Suppress.BufferConfig.withBufferFullStrategy;
import static org.apache.kafka.streams.kstream.Suppress.BufferFullStrategy.EMIT;
import static org.apache.kafka.streams.kstream.Suppress.BufferFullStrategy.SHUT_DOWN;
import static org.apache.kafka.streams.kstream.Suppress.IntermediateSuppression.withBufferConfig;
import static org.apache.kafka.streams.kstream.Suppress.emitFinalResultsOnly;
import static org.apache.kafka.streams.kstream.Suppress.intermediateEvents;

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

    private static class KVT<K, V> {
        private final K key;
        private final V value;
        private final long timestamp;

        private KVT(final K key, final V value, final long timestamp) {
            this.key = key;
            this.value = value;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "KVT{key=" + key + ", value=" + value + ", timestamp=" + timestamp + '}';
        }
    }

    @Test
    public void shouldSuppressIntermediateEventsWithEmitAfter() throws InterruptedException {
        final String testId = "-shouldSuppressIntermediateEventsWithEmitAfter";
        final String appId = getClass().getSimpleName().toLowerCase() + testId;
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
            .suppress(intermediateEvents(Suppress.IntermediateSuppression.withEmitAfter(Duration.ofMillis(scaledTime(2L)))))
            .toStream()
            .to(outputSuppressed, Produced.with(STRING_SERDE, Serdes.Long()));

        valueCounts
            .toStream()
            .filterNot((k, v) -> k.equals("tick"))
            .to(outputRaw, Produced.with(STRING_SERDE, Serdes.Long()));


        final KafkaStreams driver = getCleanStartedStreams(appId, builder);
        try {
            produceSynchronously(
                input,
                asList(
                    new KVT<>("k1", "v1", scaledTime(0L)),
                    new KVT<>("k1", "v2", scaledTime(1L)),
                    new KVT<>("k2", "v1", scaledTime(2L)),
                    new KVT<>("tick", "tick", scaledTime(5L))
                )
            );

            verifyOutput(
                outputRaw,
                asList(
                    new KVT<>("v1", 1L, scaledTime(0L)),
                    new KVT<>("v1", 0L, scaledTime(1L)),
                    new KVT<>("v2", 1L, scaledTime(1L)),
                    new KVT<>("v1", 1L, scaledTime(2L))
                )
            );
            verifyOutput(
                outputSuppressed,
                asList(
                    new KVT<>("v2", 1L, scaledTime(1L)),
                    new KVT<>("v1", 1L, scaledTime(2L))
                )
            );

        } finally {
            driver.close();
            cleanStateAfterTest(driver, input, outputRaw, outputSuppressed);
        }
    }

    @Test
    public void shouldNotSuppressIntermediateEventsWithZeroEmitAfter() throws InterruptedException {
        final String testId = "-shouldNotSuppressIntermediateEventsWithZeroEmitAfter";
        final String appId = getClass().getSimpleName().toLowerCase() + testId;
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
            .suppress(intermediateEvents(Suppress.IntermediateSuppression.withEmitAfter(Duration.ZERO)))
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
                    new KVT<>("k1", "v1", scaledTime(0L)),
                    new KVT<>("k1", "v2", scaledTime(1L)),
                    new KVT<>("k2", "v1", scaledTime(2L)),
                    new KVT<>("x", "x", scaledTime(4L))
                )
            );

            verifyOutput(
                outputRaw,
                asList(
                    new KVT<>("v1", 1L, scaledTime(0L)),
                    new KVT<>("v1", 0L, scaledTime(1L)),
                    new KVT<>("v2", 1L, scaledTime(1L)),
                    new KVT<>("v1", 1L, scaledTime(2L)),
                    new KVT<>("x", 1L, scaledTime(4L))
                )
            );
            verifyOutput(
                outputSuppressed,
                asList(
                    new KVT<>("v1", 1L, scaledTime(0L)),
                    new KVT<>("v1", 0L, scaledTime(1L)),
                    new KVT<>("v2", 1L, scaledTime(1L)),
                    new KVT<>("v1", 1L, scaledTime(2L)),
                    new KVT<>("x", 1L, scaledTime(4L))
                )
            );

        } finally {
            driver.close();
            cleanStateAfterTest(driver, input, outputRaw, outputSuppressed);
        }
    }

    @Test
    public void shouldSuppressIntermediateEventsWithKeyLimit() throws InterruptedException {
        final String testId = "-shouldSuppressIntermediateEventsWithKeyLimit";
        final String appId = getClass().getSimpleName().toLowerCase() + testId;
        final String input = "input" + testId;
        final String outputSuppressed = "output-suppressed" + testId;
        final String outputRaw = "output-raw" + testId;

        cleanStateBeforeTest(input, outputRaw, outputSuppressed);

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
            .suppress(intermediateEvents(
                withBufferConfig(BufferConfig.<String, Long>withBufferKeys((long) 1).bufferFullStrategy(EMIT))
            ))
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
                    new KVT<>("k1", "v1", scaledTime(0L)),
                    new KVT<>("k1", "v2", scaledTime(1L)),
                    new KVT<>("k2", "v1", scaledTime(2L)),
                    new KVT<>("x", "x", scaledTime(3L))
                )
            );

            verifyOutput(
                outputRaw,
                asList(
                    new KVT<>("v1", 1L, scaledTime(0L)),
                    new KVT<>("v1", 0L, scaledTime(1L)),
                    new KVT<>("v2", 1L, scaledTime(1L)),
                    new KVT<>("v1", 1L, scaledTime(2L)),
                    new KVT<>("x", 1L, scaledTime(3L))
                )
            );
            verifyOutput(
                outputSuppressed,
                asList(
                    // consecutive updates to v1 get suppressed into only the latter.
                    new KVT<>("v1", 0L, scaledTime(1L)),
                    new KVT<>("v2", 1L, scaledTime(1L)),
                    new KVT<>("v1", 1L, scaledTime(2L))
                )
            );

        } finally {
            driver.close();
            cleanStateAfterTest(driver, input, outputRaw, outputSuppressed);
        }
    }

    @Test
    public void shouldSuppressIntermediateEventsWithBytesLimit() throws InterruptedException {
        final String testId = "-shouldSuppressIntermediateEventsWithBytesLimit";
        final String appId = getClass().getSimpleName().toLowerCase() + testId;
        final String input = "input" + testId;
        final String outputSuppressed = "output-suppressed" + testId;
        final String outputRaw = "output-raw" + testId;

        cleanStateBeforeTest(input, outputRaw, outputSuppressed);

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
            .suppress(intermediateEvents(
                // this is a bit brittle, but I happen to know that the entries are a little over 100 bytes in size.
                withBufferConfig(
                    withBufferBytes((long) 200, STRING_SERIALIZER, new LongSerializer())
                        .bufferFullStrategy(EMIT)
                )
            ))
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
                    new KVT<>("k1", "v1", scaledTime(0L)),
                    new KVT<>("k1", "v2", scaledTime(1L)),
                    new KVT<>("k2", "v1", scaledTime(2L)),
                    new KVT<>("x", "x", scaledTime(3L))
                )
            );

            verifyOutput(
                outputRaw,
                asList(
                    new KVT<>("v1", 1L, scaledTime(0L)),
                    new KVT<>("v1", 0L, scaledTime(1L)),
                    new KVT<>("v2", 1L, scaledTime(1L)),
                    new KVT<>("v1", 1L, scaledTime(2L)),
                    new KVT<>("x", 1L, scaledTime(3L))
                )
            );
            verifyOutput(
                outputSuppressed,
                asList(
                    // consecutive updates to v1 get suppressed into only the latter.
                    new KVT<>("v1", 0L, scaledTime(1L)),
                    new KVT<>("v2", 1L, scaledTime(1L)),
                    new KVT<>("v1", 1L, scaledTime(2L))
                )
            );

        } finally {
            driver.close();
            cleanStateAfterTest(driver, input, outputRaw, outputSuppressed);
        }
    }

    @Test
    public void shouldSupportFinalResultsForTimeWindows() throws InterruptedException {
        final String testId = "-shouldSupportFinalResultsForTimeWindows";
        final String appId = getClass().getSimpleName().toLowerCase() + testId;
        final String input = "input" + testId;
        final String outputSuppressed = "output-suppressed" + testId;
        final String outputRaw = "output-raw" + testId;

        cleanStateBeforeTest(input, outputRaw, outputSuppressed);

        final StreamsBuilder builder = new StreamsBuilder();


        final KTable<Windowed<String>, Long> valueCounts = builder
            .stream(input,
                Consumed.with(STRING_SERDE, STRING_SERDE)
            )
            .groupBy((String k, String v) -> k, Serialized.with(STRING_SERDE, STRING_SERDE))
            .windowedBy(
                TimeWindows
                    .of(scaledTime(2L))
                    .until(scaledTime(3L))
                    .allowedLateness(scaledTime(1L))
            )
            .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("counts").withCachingDisabled().withLoggingDisabled());

        valueCounts
            .suppress(emitFinalResultsOnly(withBufferFullStrategy(SHUT_DOWN)))
            .toStream()
            .map((final Windowed<String> k, final Long v) -> new KeyValue<>(k.toString(), v))
            .to(outputSuppressed, Produced.with(STRING_SERDE, Serdes.Long()));

        valueCounts
            .toStream()
            .map((final Windowed<String> k, final Long v) -> new KeyValue<>(k.toString(), v))
            .to(outputRaw, Produced.with(STRING_SERDE, Serdes.Long()));

        final KafkaStreams driver = getCleanStartedStreams(appId, builder);
        try {
            // We have to split up some of the produces this time because the stream-time computation is currently non-deterministic
            // based on what happens to be in the buffer when Streams polls it.
            // By producing one value at a time, we get more control over the stream time at various points.

            // If we ever make the stream-time computation more rigorous, then this test can probably get simpler.


            produceSynchronously(input, singletonList(new KVT<>("k1", "v1", scaledTime(0L))));
            verifyOutput(
                outputRaw,
                singletonList(new KVT<>(scaledWindowKey("k1", 0L, 2L), 1L, scaledTime(0L)))
            );
            verifyOutput(
                outputSuppressed,
                emptyList()
            );

            produceSynchronously(input, singletonList(new KVT<>("k1", "v1", scaledTime(1L))));
            verifyOutput(
                outputRaw,
                asList(
                    new KVT<>(scaledWindowKey("k1", 0L, 2L), 1L, scaledTime(0L)),
                    new KVT<>(scaledWindowKey("k1", 0L, 2L), 2L, scaledTime(1L))
                )
            );
            verifyOutput(
                outputSuppressed,
                emptyList()
            );
            produceSynchronously(input, singletonList(new KVT<>("k1", "v1", scaledTime(2L))));
            verifyOutput(
                outputRaw,
                asList(
                    new KVT<>(scaledWindowKey("k1", 0L, 2L), 1L, scaledTime(0L)),
                    new KVT<>(scaledWindowKey("k1", 0L, 2L), 2L, scaledTime(1L)),
                    new KVT<>(scaledWindowKey("k1", 2L, 4L), 1L, scaledTime(2L))
                )
            );
            verifyOutput(
                outputSuppressed,
                emptyList()
            );
            produceSynchronously(input, singletonList(new KVT<>("k1", "v1", scaledTime(1L))));
            verifyOutput(
                outputRaw,
                asList(
                    new KVT<>(scaledWindowKey("k1", 0L, 2L), 1L, scaledTime(0L)),
                    new KVT<>(scaledWindowKey("k1", 0L, 2L), 2L, scaledTime(1L)),
                    new KVT<>(scaledWindowKey("k1", 2L, 4L), 1L, scaledTime(2L)),
                    new KVT<>(scaledWindowKey("k1", 0L, 2L), 3L, scaledTime(1L))
                )
            );
            verifyOutput(
                outputSuppressed,
                emptyList()
            );
            produceSynchronously(input, singletonList(new KVT<>("k1", "v1", scaledTime(0L))));
            verifyOutput(
                outputRaw,
                asList(
                    new KVT<>(scaledWindowKey("k1", 0L, 2L), 1L, scaledTime(0L)),
                    new KVT<>(scaledWindowKey("k1", 0L, 2L), 2L, scaledTime(1L)),
                    new KVT<>(scaledWindowKey("k1", 2L, 4L), 1L, scaledTime(2L)),
                    new KVT<>(scaledWindowKey("k1", 0L, 2L), 3L, scaledTime(1L)),
                    new KVT<>(scaledWindowKey("k1", 0L, 2L), 4L, scaledTime(0L))
                )
            );
            verifyOutput(
                outputSuppressed,
                emptyList()
            );

            produceSynchronously(input, singletonList(new KVT<>("k1", "v1", scaledTime(3L))));
            verifyOutput(
                outputRaw,
                asList(
                    new KVT<>(scaledWindowKey("k1", 0L, 2L), 1L, scaledTime(0L)),
                    new KVT<>(scaledWindowKey("k1", 0L, 2L), 2L, scaledTime(1L)),
                    new KVT<>(scaledWindowKey("k1", 2L, 4L), 1L, scaledTime(2L)),
                    new KVT<>(scaledWindowKey("k1", 0L, 2L), 3L, scaledTime(1L)),
                    new KVT<>(scaledWindowKey("k1", 0L, 2L), 4L, scaledTime(0L)),
                    new KVT<>(scaledWindowKey("k1", 2L, 4L), 2L, scaledTime(3L))
                )
            );
            verifyOutput(
                outputSuppressed,
                singletonList(
                    new KVT<>(scaledWindowKey("k1", 0L, 2L), 4L, scaledTime(0L))
                )
            );
        } finally {
            driver.close();
            cleanStateAfterTest(driver, input, outputRaw, outputSuppressed);
        }
    }

    private String scaledWindowKey(final String key, final long unscaledStart, final long unscaledEnd) {
        return new Windowed<>(key, new TimeWindow(scaledTime(unscaledStart), scaledTime(unscaledEnd))).toString();
    }

    private void cleanStateBeforeTest(final String... topic) throws InterruptedException {
        for (final String s : topic) {
            safeDelete(s);
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

    private void safeDelete(final String topic) throws InterruptedException {
        try {
            CLUSTER.deleteTopic(topic);
        } catch (final TopicAlreadyMarkedForDeletionException ignored) {
            // ignore
        }
    }

    private void cleanStateAfterTest(final KafkaStreams driver, final String... topics) throws InterruptedException {
        driver.cleanUp();
        for (final String topic : topics) {
            safeDelete(topic);
        }
    }

    private long scaledTime(final long unscaledTime) {
        return SCALE_FACTOR * unscaledTime;
    }

    private void produceSynchronously(final String topic, final List<KVT<String, String>> toProduce) {
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
            for (final KVT<String, String> record : toProduce) {
                final Future<RecordMetadata> f = producer.send(
                    new ProducerRecord<>(topic, null, record.timestamp, record.key, record.value, null)
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
            }
            producer.flush();
        }

    }

    private void verifyOutput(final String topic, final List<KVT<String, Long>> expected) {
        final List<ConsumerRecord<String, Long>> results = drainConsumerRecords(topic, STRING_DESERIALIZER, LONG_DESERIALIZER, expected.size());
        if (results.size() != expected.size()) {
            throw new AssertionError(printRecords(results) + " != " + expected);
        }
        final Iterator<KVT<String, Long>> expectedIterator = expected.iterator();
        for (final ConsumerRecord<String, Long> result : results) {
            final KVT<String, Long> expected1 = expectedIterator.next();
            try {
                compareKeyValueTimestamp(result, expected1.key, expected1.value, expected1.timestamp);
            } catch (final AssertionError e) {
                throw new AssertionError(printRecords(results) + " != " + expected, e);
            }
        }
    }

    private <K, V> List<ConsumerRecord<K, V>> drainConsumerRecords(final String topic, final Deserializer<K> keyDeserializer, final Deserializer<V> valueDeserializer, final int numRecords) {
        final KafkaConsumer<K, V> consumer = new KafkaConsumer<>(
            mkProperties(
                mkMap(
                    mkEntry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
                    mkEntry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getClass().getName()),
                    mkEntry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getClass().getName())
                )
            )
        );

        final TopicPartition topicPartition = new TopicPartition(topic, 0);
        consumer.assign(singletonList(topicPartition));
        consumer.seekToBeginning(singletonList(topicPartition));

        final List<ConsumerRecord<K, V>> results = new LinkedList<>();
        while (results.size() < numRecords) {
            final ConsumerRecords<K, V> consumerRecords = consumer.poll(Duration.ofMillis(100));
            for (final ConsumerRecord<K, V> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
                results.add(consumerRecord);
            }
        }

        return new ArrayList<>(results);
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
