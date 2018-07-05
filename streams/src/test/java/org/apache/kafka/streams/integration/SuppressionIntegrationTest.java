package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
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
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.Suppress;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.IntegrationTest;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static java.util.Collections.singletonList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.apache.kafka.streams.KeyValue.pair;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp;
import static org.apache.kafka.streams.kstream.Suppress.BufferFullStrategy.EMIT;
import static org.apache.kafka.streams.kstream.Suppress.IntermediateSuppression.withBufferBytes;
import static org.apache.kafka.streams.kstream.Suppress.intermediateEvents;
import static org.apache.kafka.streams.kstream.Suppress.lateEvents;

@Category({IntegrationTest.class})
public class SuppressionIntegrationTest {
    private static final int NUM_BROKERS = 1;

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);


    private static final StringDeserializer STRING_DESERIALIZER = new StringDeserializer();
    private static final StringSerializer STRING_SERIALIZER = new StringSerializer();
    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final LongDeserializer LONG_DESERIALIZER = new LongDeserializer();

    private static final int COMMIT_INTERVAL = 100;
    private static final int SCALE_FACTOR = COMMIT_INTERVAL * 2;

    @Test
    public void shouldSuppressIntermediateEventsWithEmitAfter() throws InterruptedException, ExecutionException {
        CLUSTER.createTopic("input", 1, 1);
        CLUSTER.createTopic("output-suppressed", 1, 1);
        CLUSTER.createTopic("output-raw", 1, 1);

        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<String, Long> valueCounts = builder
            .table(
                "input",
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
            .to("output-suppressed", Produced.with(STRING_SERDE, Serdes.Long()));

        valueCounts
            .toStream()
            .to("output-raw", Produced.with(STRING_SERDE, Serdes.Long()));

        final Properties streamsConfig = mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, getClass().getSimpleName().toLowerCase()),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
            mkEntry(StreamsConfig.POLL_MS_CONFIG, Objects.toString(COMMIT_INTERVAL)),
            mkEntry(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Objects.toString(COMMIT_INTERVAL))
        ));

        final Properties producerConfig = mkProperties(mkMap(
            mkEntry(ProducerConfig.CLIENT_ID_CONFIG, getClass().getSimpleName().toLowerCase()),
            mkEntry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER.getClass().getName()),
            mkEntry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER.getClass().getName()),
            mkEntry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
        ));


        final KafkaStreams driver = new KafkaStreams(builder.build(), streamsConfig);
        driver.start();
        try {
            produceKeyValuesSynchronouslyWithTimestamp("input", singletonList(pair("k1", "v1")), producerConfig, scaledTime(0L));
            produceKeyValuesSynchronouslyWithTimestamp("input", singletonList(pair("k1", "v2")), producerConfig, scaledTime(1L));
            produceKeyValuesSynchronouslyWithTimestamp("input", singletonList(pair("k2", "v1")), producerConfig, scaledTime(2L));
            produceKeyValuesSynchronouslyWithTimestamp("input", singletonList(pair("x", "x")), producerConfig, scaledTime(4L));

            verify(
                drainConsumerRecords("output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER, 5),
                Arrays.asList(
                    new KVT<>("v1", 1L, scaledTime(0L)),
                    new KVT<>("v1", 0L, scaledTime(1L)),
                    new KVT<>("v2", 1L, scaledTime(1L)),
                    new KVT<>("v1", 1L, scaledTime(2L)),
                    new KVT<>("x", 1L, scaledTime(4L))
                )
            );
            verify(
                drainConsumerRecords("output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER, 2),
                Arrays.asList(
                    new KVT<>("v2", 1L, scaledTime(1L)),
                    new KVT<>("v1", 1L, scaledTime(2L))
                )
            );

        } finally {
            driver.close();
            driver.cleanUp();
            CLUSTER.deleteTopic("input");
            CLUSTER.deleteTopic("output-suppressed");
            CLUSTER.deleteTopic("output-raw");
        }
    }

    private long scaledTime(final long unscaledTime) {
        return SCALE_FACTOR * unscaledTime;
    }

    @Test
    public void shouldNotSuppressIntermediateEventsWithZeroEmitAfter() throws ExecutionException, InterruptedException {
        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<String, Long> valueCounts = builder
            .table(
                "input",
                Consumed.with(STRING_SERDE, STRING_SERDE),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>with(STRING_SERDE, STRING_SERDE)
                    .withCachingDisabled()
                    .withLoggingDisabled()
            )
            .groupBy((k, v) -> new KeyValue<>(v, k), Serialized.with(STRING_SERDE, STRING_SERDE))
            .count();

        valueCounts
            .suppress(intermediateEvents(Suppress.IntermediateSuppression.withEmitAfter(Duration.ZERO)))
            .toStream()
            .to("output-suppressed", Produced.with(STRING_SERDE, Serdes.Long()));

        valueCounts
            .toStream()
            .to("output-raw", Produced.with(STRING_SERDE, Serdes.Long()));

        final Topology topology = builder.build();
        final Properties config = mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, getClass().getSimpleName().toLowerCase()),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "bogus"),
            mkEntry(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Objects.toString(COMMIT_INTERVAL))
        ));

        final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

        final Properties streamsConfig = mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, getClass().getSimpleName().toLowerCase()),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
            mkEntry(StreamsConfig.POLL_MS_CONFIG, Objects.toString(COMMIT_INTERVAL)),
            mkEntry(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Objects.toString(COMMIT_INTERVAL))
        ));

        final Properties producerConfig = mkProperties(mkMap(
            mkEntry(ProducerConfig.CLIENT_ID_CONFIG, getClass().getSimpleName().toLowerCase()),
            mkEntry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER.getClass().getName()),
            mkEntry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER.getClass().getName()),
            mkEntry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
        ));


        final KafkaStreams driver = new KafkaStreams(builder.build(), streamsConfig);
        driver.start();
        try {
            produceKeyValuesSynchronouslyWithTimestamp("input", singletonList(pair("k1", "v1")), producerConfig, scaledTime(0L));
            produceKeyValuesSynchronouslyWithTimestamp("input", singletonList(pair("k1", "v2")), producerConfig, scaledTime(1L));
            produceKeyValuesSynchronouslyWithTimestamp("input", singletonList(pair("k2", "v1")), producerConfig, scaledTime(2L));
            produceKeyValuesSynchronouslyWithTimestamp("input", singletonList(pair("x", "x")), producerConfig, scaledTime(4L));

            verify(
                drainConsumerRecords("output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER, 5),
                Arrays.asList(
                    new KVT<>("v1", 1L, scaledTime(0L)),
                    new KVT<>("v1", 0L, scaledTime(1L)),
                    new KVT<>("v2", 1L, scaledTime(1L)),
                    new KVT<>("v1", 1L, scaledTime(2L)),
                    new KVT<>("x", 1L, scaledTime(4L))
                )
            );
            verify(
                drainConsumerRecords("output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER, 2),
                Arrays.asList(
                    new KVT<>("v1", 1L, scaledTime(0L)),
                    new KVT<>("v1", 0L, scaledTime(1L)),
                    new KVT<>("v2", 1L, scaledTime(1L)),
                    new KVT<>("v1", 1L, scaledTime(2L)),
                    new KVT<>("x", 1L, scaledTime(4L))
                )
            );

        } finally {
            driver.close();
            driver.cleanUp();
            CLUSTER.deleteTopic("input");
            CLUSTER.deleteTopic("output-suppressed");
            CLUSTER.deleteTopic("output-raw");
        }
    }

    @Test
    public void shouldSuppressIntermediateEventsWithKeyLimit() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<String, Long> valueCounts = builder
            .table(
                "input",
                Consumed.with(STRING_SERDE, STRING_SERDE),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>with(STRING_SERDE, STRING_SERDE)
                    .withCachingDisabled()
                    .withLoggingDisabled()
            )
            .groupBy((k, v) -> new KeyValue<>(v, k), Serialized.with(STRING_SERDE, STRING_SERDE))
            .count();

        valueCounts
            .suppress(intermediateEvents(Suppress.IntermediateSuppression.<String, Long>withBufferKeys(1).bufferFullStrategy(EMIT)))
            .toStream()
            .to("output-suppressed", Produced.with(STRING_SERDE, Serdes.Long()));

        valueCounts
            .toStream()
            .to("output-raw", Produced.with(STRING_SERDE, Serdes.Long()));

        final Topology topology = builder.build();
        System.out.println(topology.describe());
        final Properties config = mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, getClass().getSimpleName().toLowerCase()),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "bogus"),
            mkEntry(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Objects.toString(COMMIT_INTERVAL))
        ));

        final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {

            driver.pipeInput(recordFactory.create("input", "k1", "v1", scaledTime(0L)));
            driver.pipeInput(recordFactory.create("input", "k1", "v2", scaledTime(1L)));
            driver.pipeInput(recordFactory.create("input", "k2", "v1", scaledTime(2L)));

            /*verify(
                drainConsumerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                Arrays.asList(
                    new KVT<>("v1", 1L, scaledTime(0L)),
                    new KVT<>("v1", 0L, scaledTime(1L)),
                    new KVT<>("v2", 1L, scaledTime(1L)),
                    new KVT<>("v1", 1L, scaledTime(2L))
                )
            );
            verify(
                drainConsumerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                Arrays.asList(
                    // consecutive updates to v1 get suppressed into only the latter.
                    new KVT<>("v1", 0L, scaledTime(1L)),
                    new KVT<>("v2", 1L, scaledTime(1L))
                    // the last update won't be evicted until another key comes along.
                )
            );*/


            driver.pipeInput(recordFactory.create("input", "x", "x", scaledTime(3L)));

            /*verify(
                drainConsumerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                singletonList(
                    new KVT<>("x", 1L, scaledTime(3L))
                )
            );
            verify(
                drainConsumerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                singletonList(
                    // now we see that last update to v1, but we won't see the update to x until it gets evicted
                    new KVT<>("v1", 1L, scaledTime(2L))
                )
            );*/
        }
    }


    @Test
    public void shouldSuppressIntermediateEventsWithBytesLimit() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<String, Long> valueCounts = builder
            .table(
                "input",
                Consumed.with(STRING_SERDE, STRING_SERDE),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>with(STRING_SERDE, STRING_SERDE)
                    .withCachingDisabled()
                    .withLoggingDisabled()
            )
            .groupBy((k, v) -> new KeyValue<>(v, k), Serialized.with(STRING_SERDE, STRING_SERDE))
            .count();

        valueCounts
            .suppress(intermediateEvents(
                // this is a bit brittle, but I happen to know that the entries are a little over 100 bytes in size.
                withBufferBytes(200, STRING_SERIALIZER, new LongSerializer())
                    .bufferFullStrategy(EMIT)
            ))
            .toStream()
            .to("output-suppressed", Produced.with(STRING_SERDE, Serdes.Long()));

        valueCounts
            .toStream()
            .to("output-raw", Produced.with(STRING_SERDE, Serdes.Long()));

        final Topology topology = builder.build();
        System.out.println(topology.describe());
        final Properties config = mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, getClass().getSimpleName().toLowerCase()),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "bogus"),
            mkEntry(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Objects.toString(COMMIT_INTERVAL))
        ));

        final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {

            driver.pipeInput(recordFactory.create("input", "k1", "v1", scaledTime(0L)));
            driver.pipeInput(recordFactory.create("input", "k1", "v2", scaledTime(1L)));
            final ConsumerRecord<byte[], byte[]> consumerRecord = recordFactory.create("input", "k2", "v1", scaledTime(2L));
            driver.pipeInput(consumerRecord);

            /*verify(
                drainConsumerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                Arrays.asList(
                    new KVT<>("v1", 1L, scaledTime(0L)),
                    new KVT<>("v1", 0L, scaledTime(1L)),
                    new KVT<>("v2", 1L, scaledTime(1L)),
                    new KVT<>("v1", 1L, scaledTime(2L))
                )
            );
            verify(
                drainConsumerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                Arrays.asList(
                    // consecutive updates to v1 get suppressed into only the latter.
                    new KVT<>("v1", 0L, scaledTime(1L)),
                    new KVT<>("v2", 1L, scaledTime(1L))
                    // the last update won't be evicted until another key comes along.
                )
            );*/


            driver.pipeInput(recordFactory.create("input", "x", "x", scaledTime(3L)));

            /*verify(
                drainConsumerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                singletonList(
                    new KVT<>("x", 1L, scaledTime(3L))
                )
            );
            verify(
                drainConsumerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                singletonList(
                    // now we see that last update to v1, but we won't see the update to x until it gets evicted
                    new KVT<>("v1", 1L, scaledTime(2L))
                )
            );*/
        }
    }


    @Test
    public void shouldSuppressLateEvents() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<String, Long> valueCounts = builder
            .table(
                "input",
                Consumed.with(STRING_SERDE, STRING_SERDE),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>with(STRING_SERDE, STRING_SERDE)
                    .withCachingDisabled()
                    .withLoggingDisabled()
            )
            .groupBy((k, v) -> new KeyValue<>(v, k), Serialized.with(STRING_SERDE, STRING_SERDE))
            .count();

        valueCounts
            .suppress(lateEvents(Duration.ofMillis(scaledTime(1L))))
            .toStream()
            .to("output-suppressed", Produced.with(STRING_SERDE, Serdes.Long()));

        valueCounts
            .toStream()
            .to("output-raw", Produced.with(STRING_SERDE, Serdes.Long()));

        final Topology topology = builder.build();
        System.out.println(topology.describe());
        final Properties config = mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, getClass().getSimpleName().toLowerCase()),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "bogus"),
            mkEntry(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Objects.toString(COMMIT_INTERVAL))
        ));

        final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {

            driver.pipeInput(recordFactory.create("input", "k1", "v1", scaledTime(0L)));
            driver.pipeInput(recordFactory.create("input", "k2", "v1", scaledTime(2L)));

            /*verify(
                drainConsumerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                Arrays.asList(
                    new KVT<>("v1", 1L, scaledTime(0L)),
                    new KVT<>("v1", 2L, scaledTime(2L))
                )
            );
            verify(
                drainConsumerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                Arrays.asList(
                    new KVT<>("v1", 1L, scaledTime(0L)),
                    new KVT<>("v1", 2L, scaledTime(2L))
                )
            );*/

            // An event 1ms late should be admitted
            driver.pipeInput(recordFactory.create("input", "k1", "v2", scaledTime(1L)));

            /*verify(
                drainConsumerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                Arrays.asList(
                    new KVT<>("v1", 1L, scaledTime(1L)),
                    new KVT<>("v2", 1L, scaledTime(1L))
                )
            );
            verify(
                drainConsumerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                Arrays.asList(
                    new KVT<>("v1", 1L, scaledTime(1L)),
                    new KVT<>("v2", 1L, scaledTime(1L))
                )
            );*/

            // An event 2ms late should be rejected
            driver.pipeInput(recordFactory.create("input", "k2", "v2", scaledTime(0L)));

            /*verify(
                drainConsumerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                Arrays.asList(
                    new KVT<>("v1", 0L, scaledTime(0L)),
                    new KVT<>("v2", 2L, scaledTime(0L))
                )
            );
            verify(
                drainConsumerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                Collections.emptyList()
            );*/

        }
    }

    private <K, V> void verify(final List<ConsumerRecord<K, V>> results, final List<KVT<K, V>> expectedResults) {
        if (results.size() != expectedResults.size()) {
            throw new AssertionError(printRecords(results) + " != " + expectedResults);
        }
        final Iterator<KVT<K, V>> expectedIterator = expectedResults.iterator();
        for (final ConsumerRecord<K, V> result : results) {
            final KVT<K, V> expected = expectedIterator.next();
            try {
                compareKeyValueTimestamp(result, expected.key, expected.value, expected.timestamp);
            } catch (final AssertionError e) {
                throw new AssertionError(printRecords(results) + " != " + expectedResults, e);
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
            return "KVT{" +
                "key=" + key +
                ", value=" + value +
                ", timestamp=" + timestamp +
                '}';
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

        final List<ConsumerRecord<K, V>> results = new LinkedList<>();
        final long endOffset = consumer.endOffsets(singletonList(topicPartition)).get(topicPartition) - 1;
        System.out.println(endOffset);
        consumer.seekToBeginning(singletonList(topicPartition));
//        outer:
        while (results.size() < numRecords) {
            final ConsumerRecords<K, V> consumerRecords = consumer.poll(Duration.ofMillis(100));
            for (final ConsumerRecord<K, V> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
                results.add(consumerRecord);
//                if (endOffset <= consumerRecord.offset() + 1) {
//                    break outer;
//                }
            }
        }

        return new ArrayList<>(results);
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
