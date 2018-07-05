package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.Suppress.IntermediateSuppression;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import static org.apache.kafka.streams.kstream.Suppress.BufferFullStrategy.EMIT;
import static org.apache.kafka.streams.kstream.Suppress.IntermediateSuppression.withBufferBytes;
import static org.apache.kafka.streams.kstream.Suppress.intermediateEvents;
import static org.apache.kafka.streams.kstream.Suppress.lateEvents;

@SuppressWarnings("PointlessArithmeticExpression")
public class KTableSuppressProcessorTest {

    private static final StringDeserializer STRING_DESERIALIZER = new StringDeserializer();
    private static final StringSerializer STRING_SERIALIZER = new StringSerializer();
    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final LongDeserializer LONG_DESERIALIZER = new LongDeserializer();

    private static final int COMMIT_INTERVAL = 30_000;
    private static final int SCALE_FACTOR = COMMIT_INTERVAL * 2;

    @Test
    public void shouldSuppressIntermediateEventsWithEmitAfter() {
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
            .suppress(intermediateEvents(IntermediateSuppression.withEmitAfter(Duration.ofMillis(scaledTime(2L)))))
            .toStream()
            .to("output-suppressed", Produced.with(STRING_SERDE, Serdes.Long()));

        valueCounts
            .toStream()
            .to("output-raw", Produced.with(STRING_SERDE, Serdes.Long()));

        final Topology topology = builder.build();
        final Properties config = Utils.mkProperties(Utils.mkMap(
            Utils.mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, getClass().getSimpleName().toLowerCase()),
            Utils.mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "bogus"),
            Utils.mkEntry(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Objects.toString(COMMIT_INTERVAL))
        ));

        final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {

            driver.pipeInput(recordFactory.create("input", "k1", "v1", scaledTime(0L)));
            driver.pipeInput(recordFactory.create("input", "k1", "v2", scaledTime(1L)));
            driver.pipeInput(recordFactory.create("input", "k2", "v1", scaledTime(2L)));

            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                Arrays.asList(
                    new KVT<>("v1", 1L, scaledTime(0L)),
                    new KVT<>("v1", 0L, scaledTime(1L)),
                    new KVT<>("v2", 1L, scaledTime(1L)),
                    new KVT<>("v1", 1L, scaledTime(2L))
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                Collections.emptyList()
            );


            driver.pipeInput(recordFactory.create("input", "x", "x", scaledTime(3L)));

            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                Collections.singletonList(
                    new KVT<>("x", 1L, scaledTime(3L))
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                Collections.emptyList()
            );

            driver.pipeInput(recordFactory.create("input", "x", "x", scaledTime(4L)));

            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                Arrays.asList(
                    new KVT<>("x", 0L, scaledTime(4L)),
                    new KVT<>("x", 1L, scaledTime(4L))
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                Arrays.asList(
                    new KVT<>("v2", 1L, scaledTime(1L)),
                    new KVT<>("v1", 1L, scaledTime(2L))
                )
            );

        }
    }

    private long scaledTime(final long unscaledTime) {
        return SCALE_FACTOR * unscaledTime;
    }

    @Test
    public void shouldNotSuppressIntermediateEventsWithZeroEmitAfter() {
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
            .suppress(intermediateEvents(IntermediateSuppression.withEmitAfter(Duration.ZERO)))
            .toStream()
            .to("output-suppressed", Produced.with(STRING_SERDE, Serdes.Long()));

        valueCounts
            .toStream()
            .to("output-raw", Produced.with(STRING_SERDE, Serdes.Long()));

        final Topology topology = builder.build();
        final Properties config = Utils.mkProperties(Utils.mkMap(
            Utils.mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, getClass().getSimpleName().toLowerCase()),
            Utils.mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "bogus"),
            Utils.mkEntry(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Objects.toString(COMMIT_INTERVAL))
        ));

        final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {

            driver.pipeInput(recordFactory.create("input", "k1", "v1", scaledTime(0L)));
            driver.pipeInput(recordFactory.create("input", "k1", "v2", scaledTime(1L)));
            driver.pipeInput(recordFactory.create("input", "k2", "v1", scaledTime(2L)));

            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                Arrays.asList(
                    new KVT<>("v1", 1L, scaledTime(0L)),
                    new KVT<>("v1", 0L, scaledTime(1L)),
                    new KVT<>("v2", 1L, scaledTime(1L)),
                    new KVT<>("v1", 1L, scaledTime(2L))
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                Arrays.asList(
                    new KVT<>("v1", 1L, scaledTime(0L)),
                    new KVT<>("v1", 0L, scaledTime(1L)),
                    new KVT<>("v2", 1L, scaledTime(1L)),
                    new KVT<>("v1", 1L, scaledTime(2L))
                )
            );


            driver.pipeInput(recordFactory.create("input", "x", "x", scaledTime(3L)));

            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                Collections.singletonList(
                    new KVT<>("x", 1L, scaledTime(3L))
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                Collections.singletonList(
                    new KVT<>("x", 1L, scaledTime(3L))
                )
            );

            driver.pipeInput(recordFactory.create("input", "x", "x", scaledTime(4L)));

            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                Arrays.asList(
                    new KVT<>("x", 0L, scaledTime(4L)),
                    new KVT<>("x", 1L, scaledTime(4L))
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                Arrays.asList(
                    new KVT<>("x", 0L, scaledTime(4L)),
                    new KVT<>("x", 1L, scaledTime(4L))
                )
            );

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
            .suppress(intermediateEvents(IntermediateSuppression.<String, Long>withBufferKeys(1).bufferFullStrategy(EMIT)))
            .toStream()
            .to("output-suppressed", Produced.with(STRING_SERDE, Serdes.Long()));

        valueCounts
            .toStream()
            .to("output-raw", Produced.with(STRING_SERDE, Serdes.Long()));

        final Topology topology = builder.build();
        System.out.println(topology.describe());
        final Properties config = Utils.mkProperties(Utils.mkMap(
            Utils.mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, getClass().getSimpleName().toLowerCase()),
            Utils.mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "bogus"),
            Utils.mkEntry(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Objects.toString(COMMIT_INTERVAL))
        ));

        final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {

            driver.pipeInput(recordFactory.create("input", "k1", "v1", scaledTime(0L)));
            driver.pipeInput(recordFactory.create("input", "k1", "v2", scaledTime(1L)));
            driver.pipeInput(recordFactory.create("input", "k2", "v1", scaledTime(2L)));

            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                Arrays.asList(
                    new KVT<>("v1", 1L, scaledTime(0L)),
                    new KVT<>("v1", 0L, scaledTime(1L)),
                    new KVT<>("v2", 1L, scaledTime(1L)),
                    new KVT<>("v1", 1L, scaledTime(2L))
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                Arrays.asList(
                    // consecutive updates to v1 get suppressed into only the latter.
                    new KVT<>("v1", 0L, scaledTime(1L)),
                    new KVT<>("v2", 1L, scaledTime(1L))
                    // the last update won't be evicted until another key comes along.
                )
            );


            driver.pipeInput(recordFactory.create("input", "x", "x", scaledTime(3L)));

            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                Collections.singletonList(
                    new KVT<>("x", 1L, scaledTime(3L))
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                Collections.singletonList(
                    // now we see that last update to v1, but we won't see the update to x until it gets evicted
                    new KVT<>("v1", 1L, scaledTime(2L))
                )
            );
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
        final Properties config = Utils.mkProperties(Utils.mkMap(
            Utils.mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, getClass().getSimpleName().toLowerCase()),
            Utils.mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "bogus"),
            Utils.mkEntry(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Objects.toString(COMMIT_INTERVAL))
        ));

        final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {

            driver.pipeInput(recordFactory.create("input", "k1", "v1", scaledTime(0L)));
            driver.pipeInput(recordFactory.create("input", "k1", "v2", scaledTime(1L)));
            final ConsumerRecord<byte[], byte[]> consumerRecord = recordFactory.create("input", "k2", "v1", scaledTime(2L));
            driver.pipeInput(consumerRecord);

            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                Arrays.asList(
                    new KVT<>("v1", 1L, scaledTime(0L)),
                    new KVT<>("v1", 0L, scaledTime(1L)),
                    new KVT<>("v2", 1L, scaledTime(1L)),
                    new KVT<>("v1", 1L, scaledTime(2L))
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                Arrays.asList(
                    // consecutive updates to v1 get suppressed into only the latter.
                    new KVT<>("v1", 0L, scaledTime(1L)),
                    new KVT<>("v2", 1L, scaledTime(1L))
                    // the last update won't be evicted until another key comes along.
                )
            );


            driver.pipeInput(recordFactory.create("input", "x", "x", scaledTime(3L)));

            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                Collections.singletonList(
                    new KVT<>("x", 1L, scaledTime(3L))
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                Collections.singletonList(
                    // now we see that last update to v1, but we won't see the update to x until it gets evicted
                    new KVT<>("v1", 1L, scaledTime(2L))
                )
            );
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
        final Properties config = Utils.mkProperties(Utils.mkMap(
            Utils.mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, getClass().getSimpleName().toLowerCase()),
            Utils.mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "bogus"),
            Utils.mkEntry(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Objects.toString(COMMIT_INTERVAL))
        ));

        final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {

            driver.pipeInput(recordFactory.create("input", "k1", "v1", scaledTime(0L)));
            driver.pipeInput(recordFactory.create("input", "k2", "v1", scaledTime(2L)));

            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                Arrays.asList(
                    new KVT<>("v1", 1L, scaledTime(0L)),
                    new KVT<>("v1", 2L, scaledTime(2L))
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                Arrays.asList(
                    new KVT<>("v1", 1L, scaledTime(0L)),
                    new KVT<>("v1", 2L, scaledTime(2L))
                )
            );

            // An event 1ms late should be admitted
            driver.pipeInput(recordFactory.create("input", "k1", "v2", scaledTime(1L)));

            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                Arrays.asList(
                    new KVT<>("v1", 1L, scaledTime(1L)),
                    new KVT<>("v2", 1L, scaledTime(1L))
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                Arrays.asList(
                    new KVT<>("v1", 1L, scaledTime(1L)),
                    new KVT<>("v2", 1L, scaledTime(1L))
                )
            );

            // An event 2ms late should be rejected
            driver.pipeInput(recordFactory.create("input", "k2", "v2", scaledTime(0L)));

            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                Arrays.asList(
                    new KVT<>("v1", 0L, scaledTime(0L)),
                    new KVT<>("v2", 2L, scaledTime(0L))
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                Collections.emptyList()
            );

        }
    }

    private <K, V> void verify(final List<ProducerRecord<K, V>> results, final List<KVT<K, V>> expectedResults) {
        if (results.size() != expectedResults.size()) {
            throw new AssertionError(printRecords(results) + " != " + expectedResults);
        }
        final Iterator<KVT<K, V>> expectedIterator = expectedResults.iterator();
        for (final ProducerRecord<K, V> result : results) {
            final KVT<K, V> expected = expectedIterator.next();
            try {
                OutputVerifier.compareKeyValueTimestamp(result, expected.key, expected.value, expected.timestamp);
            } catch (AssertionError e) {
                throw new AssertionError(printRecords(results) + " != " + expectedResults, e);
            }
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

    private <K, V> List<ProducerRecord<K, V>> drainProducerRecords(final TopologyTestDriver driver, final String topic, final Deserializer<K> keyDeserializer, final Deserializer<V> valueDeserializer) {
        final List<ProducerRecord<K, V>> result = new LinkedList<>();
        for (ProducerRecord<K, V> next = driver.readOutput(topic, keyDeserializer, valueDeserializer);
             next != null;
             next = driver.readOutput(topic, keyDeserializer, valueDeserializer)
            ) {
            result.add(next);
        }
        return new ArrayList<>(result);
    }

    private <K, V> String printRecords(final List<ProducerRecord<K, V>> result) {
        final StringBuilder resultStr = new StringBuilder();
        resultStr.append("[\n");
        for (final ProducerRecord<?, ?> record : result) {
            resultStr.append("  ").append(record.toString()).append("\n");
        }
        resultStr.append("]");
        return resultStr.toString();
    }
}
