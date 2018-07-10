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
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Suppress.BufferConfig;
import org.apache.kafka.streams.kstream.Suppress.IntermediateSuppression;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.kafka.streams.kstream.Suppress.BufferConfig.withBufferBytes;
import static org.apache.kafka.streams.kstream.Suppress.BufferConfig.withBufferFullStrategy;
import static org.apache.kafka.streams.kstream.Suppress.BufferFullStrategy.EMIT;
import static org.apache.kafka.streams.kstream.Suppress.BufferFullStrategy.SHUT_DOWN;
import static org.apache.kafka.streams.kstream.Suppress.IntermediateSuppression.withBufferConfig;
import static org.apache.kafka.streams.kstream.Suppress.emitFinalResultsOnly;
import static org.apache.kafka.streams.kstream.Suppress.intermediateEvents;

@SuppressWarnings("PointlessArithmeticExpression")
public class KTableSuppressProcessorTest {

    private static final StringDeserializer STRING_DESERIALIZER = new StringDeserializer();
    private static final StringSerializer STRING_SERIALIZER = new StringSerializer();
    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final LongDeserializer LONG_DESERIALIZER = new LongDeserializer();
    private static final LongSerializer LONG_SERIALIZER = new LongSerializer();

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
            .suppress(intermediateEvents(IntermediateSuppression.withEmitAfter(Duration.ofMillis(2L))))
            .toStream()
            .to("output-suppressed", Produced.with(STRING_SERDE, Serdes.Long()));

        valueCounts
            .toStream()
            .filterNot((k, v) -> k.equals("tick"))
            .to("output-raw", Produced.with(STRING_SERDE, Serdes.Long()));

        final Topology topology = builder.build();
        final Properties config = Utils.mkProperties(Utils.mkMap(
            Utils.mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, getClass().getSimpleName().toLowerCase()),
            Utils.mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "bogus")
        ));

        final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {

            driver.pipeInput(recordFactory.create("input", "k1", "v1", 0L));
            driver.pipeInput(recordFactory.create("input", "k1", "v2", 1L));
            driver.pipeInput(recordFactory.create("input", "k2", "v1", 2L));

            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    new KVT<>("v1", 1L, 0L),
                    new KVT<>("v1", 0L, 1L),
                    new KVT<>("v2", 1L, 1L),
                    new KVT<>("v1", 1L, 2L)
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                emptyList()
            );

            driver.pipeInput(recordFactory.create("input", "tick", "tick", 3L));

            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                emptyList()
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                emptyList()
            );

            driver.pipeInput(recordFactory.create("input", "tick", "tick", 4L));

            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                emptyList()
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                singletonList(new KVT<>("v2", 1L, 1L))
            );

            driver.pipeInput(recordFactory.create("input", "tick", "tick", 5L));

            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                emptyList()
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                singletonList(
                    new KVT<>("v1", 1L, 2L)
                )
            );

        }
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
            Utils.mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "bogus")
        ));

        final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {

            driver.pipeInput(recordFactory.create("input", "k1", "v1", 0L));
            driver.pipeInput(recordFactory.create("input", "k1", "v2", 1L));
            driver.pipeInput(recordFactory.create("input", "k2", "v1", 2L));

            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    new KVT<>("v1", 1L, 0L),
                    new KVT<>("v1", 0L, 1L),
                    new KVT<>("v2", 1L, 1L),
                    new KVT<>("v1", 1L, 2L)
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    new KVT<>("v1", 1L, 0L),
                    new KVT<>("v1", 0L, 1L),
                    new KVT<>("v2", 1L, 1L),
                    new KVT<>("v1", 1L, 2L)
                )
            );


            driver.pipeInput(recordFactory.create("input", "x", "x", 3L));

            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                singletonList(
                    new KVT<>("x", 1L, 3L)
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                singletonList(
                    new KVT<>("x", 1L, 3L)
                )
            );

            driver.pipeInput(recordFactory.create("input", "x", "x", 4L));

            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    new KVT<>("x", 0L, 4L),
                    new KVT<>("x", 1L, 4L)
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    new KVT<>("x", 0L, 4L),
                    new KVT<>("x", 1L, 4L)
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
            .suppress(intermediateEvents(
                withBufferConfig(BufferConfig.<String, Long>withBufferKeys((long) 1).bufferFullStrategy(EMIT))
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
            Utils.mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "bogus")
        ));

        final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {

            driver.pipeInput(recordFactory.create("input", "k1", "v1", 0L));
            driver.pipeInput(recordFactory.create("input", "k1", "v2", 1L));
            driver.pipeInput(recordFactory.create("input", "k2", "v1", 2L));

            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    new KVT<>("v1", 1L, 0L),
                    new KVT<>("v1", 0L, 1L),
                    new KVT<>("v2", 1L, 1L),
                    new KVT<>("v1", 1L, 2L)
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    // consecutive updates to v1 get suppressed into only the latter.
                    new KVT<>("v1", 0L, 1L),
                    new KVT<>("v2", 1L, 1L)
                    // the last update won't be evicted until another key comes along.
                )
            );


            driver.pipeInput(recordFactory.create("input", "x", "x", 3L));

            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                singletonList(
                    new KVT<>("x", 1L, 3L)
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                singletonList(
                    // now we see that last update to v1, but we won't see the update to x until it gets evicted
                    new KVT<>("v1", 1L, 2L)
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
                withBufferConfig(
                    withBufferBytes(200L, STRING_SERIALIZER, LONG_SERIALIZER)
                        .bufferFullStrategy(EMIT)
                )
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
            Utils.mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "bogus")
        ));

        final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {

            driver.pipeInput(recordFactory.create("input", "k1", "v1", 0L));
            driver.pipeInput(recordFactory.create("input", "k1", "v2", 1L));
            final ConsumerRecord<byte[], byte[]> consumerRecord = recordFactory.create("input", "k2", "v1", 2L);
            driver.pipeInput(consumerRecord);

            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    new KVT<>("v1", 1L, 0L),
                    new KVT<>("v1", 0L, 1L),
                    new KVT<>("v2", 1L, 1L),
                    new KVT<>("v1", 1L, 2L)
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    // consecutive updates to v1 get suppressed into only the latter.
                    new KVT<>("v1", 0L, 1L),
                    new KVT<>("v2", 1L, 1L)
                    // the last update won't be evicted until another key comes along.
                )
            );


            driver.pipeInput(recordFactory.create("input", "x", "x", 3L));

            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                singletonList(
                    new KVT<>("x", 1L, 3L)
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                singletonList(
                    // now we see that last update to v1, but we won't see the update to x until it gets evicted
                    new KVT<>("v1", 1L, 2L)
                )
            );
        }
    }

    @Test
    public void shouldSupportFinalResultsForTimeWindows() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<Windowed<String>, Long> valueCounts = builder
            .stream("input", Consumed.with(STRING_SERDE, STRING_SERDE))
            .groupBy((String k, String v) -> k, Serialized.with(STRING_SERDE, STRING_SERDE))
            .windowedBy(TimeWindows
                .of(2L)
                .until(3L)
                .close(1L)
            )
            .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("counts").withCachingDisabled());

        valueCounts
            .suppress(emitFinalResultsOnly(withBufferFullStrategy(SHUT_DOWN)))
            .toStream()
            .map((final Windowed<String> k, final Long v) -> new KeyValue<>(k.toString(), v))
            .to("output-suppressed", Produced.with(STRING_SERDE, Serdes.Long()));

        valueCounts
            .toStream()
            .map((final Windowed<String> k, final Long v) -> new KeyValue<>(k.toString(), v))
            .to("output-raw", Produced.with(STRING_SERDE, Serdes.Long()));

        final Topology topology = builder.build();
        System.out.println(topology.describe());
        final Properties config = Utils.mkProperties(Utils.mkMap(
            Utils.mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, getClass().getSimpleName().toLowerCase()),
            Utils.mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "bogus")
        ));

        final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {

            driver.pipeInput(recordFactory.create("input", "k1", "v1", 0L));
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 1L));
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 2L));
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 1L));
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 0L));
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 5L));


            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    new KVT<>("[k1@0/2]", 1L, 0L),
                    new KVT<>("[k1@0/2]", 2L, 1L),
                    new KVT<>("[k1@2/4]", 1L, 2L),
                    new KVT<>("[k1@0/2]", 3L, 1L),
                    new KVT<>("[k1@0/2]", 4L, 0L),
                    new KVT<>("[k1@4/6]", 1L, 5L)
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                singletonList(
                    new KVT<>("[k1@0/2]", 4L, 0L)
                )
            );

        }
    }

    @Test
    public void shouldSupportFinalResultsForTimeWindowsWithLargeJump() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<Windowed<String>, Long> valueCounts = builder
            .stream("input", Consumed.with(STRING_SERDE, STRING_SERDE))
            .groupBy((String k, String v) -> k, Serialized.with(STRING_SERDE, STRING_SERDE))
            .windowedBy(TimeWindows
                .of(2L)
                .until(10L)
                .close(2L)
            )
            .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("counts").withCachingDisabled());

        valueCounts
            .suppress(emitFinalResultsOnly(withBufferFullStrategy(SHUT_DOWN)))
            .toStream()
            .map((final Windowed<String> k, final Long v) -> new KeyValue<>(k.toString(), v))
            .to("output-suppressed", Produced.with(STRING_SERDE, Serdes.Long()));

        valueCounts
            .toStream()
            .map((final Windowed<String> k, final Long v) -> new KeyValue<>(k.toString(), v))
            .to("output-raw", Produced.with(STRING_SERDE, Serdes.Long()));

        final Topology topology = builder.build();
        System.out.println(topology.describe());
        final Properties config = Utils.mkProperties(Utils.mkMap(
            Utils.mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, getClass().getSimpleName().toLowerCase()),
            Utils.mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "bogus")
        ));

        final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {

            driver.pipeInput(recordFactory.create("input", "k1", "v1", 0L));
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 1L));
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 2L));
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 0L));
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 3L));
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 0L));
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 4L));
            // this update should get dropped, since the previous event advanced the stream time and closed the window.
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 0L));
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 30L));


            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    new KVT<>("[k1@0/2]", 1L, 0L),
                    new KVT<>("[k1@0/2]", 2L, 1L),
                    new KVT<>("[k1@2/4]", 1L, 2L),
                    new KVT<>("[k1@0/2]", 3L, 0L),
                    new KVT<>("[k1@2/4]", 2L, 3L),
                    new KVT<>("[k1@0/2]", 4L, 0L),
                    new KVT<>("[k1@4/6]", 1L, 4L),
                    new KVT<>("[k1@30/32]", 1L, 30L)
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    new KVT<>("[k1@0/2]", 4L, 0L),
                    new KVT<>("[k1@2/4]", 2L, 3L),
                    new KVT<>("[k1@4/6]", 1L, 4L)
                )
            );

        }
    }


    @Test
    public void shouldSupportFinalResultsForSessionWindows() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<Windowed<String>, Long> valueCounts = builder
            .stream("input", Consumed.with(STRING_SERDE, STRING_SERDE))
            .groupBy((String k, String v) -> k, Serialized.with(STRING_SERDE, STRING_SERDE))
            .windowedBy(
                SessionWindows
                    .with(5L)
                    .close(2L)
            )
            .count(Materialized.<String, Long, SessionStore<Bytes, byte[]>>as("counts").withCachingDisabled());

        valueCounts
            .suppress(emitFinalResultsOnly(withBufferFullStrategy(SHUT_DOWN)))
            .toStream()
            .map((final Windowed<String> k, final Long v) -> new KeyValue<>(k.toString(), v))
            .to("output-suppressed", Produced.with(STRING_SERDE, Serdes.Long()));

        valueCounts
            .toStream()
            .map((final Windowed<String> k, final Long v) -> new KeyValue<>(k.toString(), v))
            .to("output-raw", Produced.with(STRING_SERDE, Serdes.Long()));

        final Topology topology = builder.build();
        System.out.println(topology.describe());
        final Properties config = Utils.mkProperties(Utils.mkMap(
            Utils.mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, getClass().getSimpleName().toLowerCase()),
            Utils.mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "bogus")
        ));

        final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {

            // first window
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 0L));
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 1L));
            // second window
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 7L));
            // late event for first window
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 1L));


            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    new KVT<>("[k1@0/0]", 1L, 0L),
                    new KVT<>("[k1@0/0]", null, 1L),
                    new KVT<>("[k1@0/1]", 2L, 1L),
                    new KVT<>("[k1@7/7]", 1L, 7L),
                    new KVT<>("[k1@0/1]", null, 1L),
                    new KVT<>("[k1@0/1]", 3L, 1L)
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    new KVT<>("[k1@0/0]", null, 1L),
                    new KVT<>("[k1@0/1]", 2L, 1L)
                )
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
            } catch (final AssertionError e) {
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
