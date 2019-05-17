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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TopologyWrapper;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;
import org.apache.kafka.streams.processor.internals.SourceNode;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.MockMapper;
import org.apache.kafka.test.MockProcessor;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.time.Duration.ofMillis;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings("unchecked")
public class KStreamImplTest {

    private final Consumed<String, String> stringConsumed = Consumed.with(Serdes.String(), Serdes.String());
    private final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();

    private KStream<String, String> testStream;
    private StreamsBuilder builder;

    private final ConsumerRecordFactory<String, String> recordFactory =
        new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer(), 0L);
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());

    private final Serde<String> mySerde = new Serdes.StringSerde();

    @Before
    public void before() {
        builder = new StreamsBuilder();
        testStream = builder.stream("source");
    }

    @Test
    public void testNumProcesses() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> source1 = builder.stream(Arrays.asList("topic-1", "topic-2"), stringConsumed);

        final KStream<String, String> source2 = builder.stream(Arrays.asList("topic-3", "topic-4"), stringConsumed);

        final KStream<String, String> stream1 = source1.filter((key, value) -> true)
                                                       .filterNot((key, value) -> false);

        final KStream<String, Integer> stream2 = stream1.mapValues(Integer::new);

        final KStream<String, Integer> stream3 = source2.flatMapValues((ValueMapper<String, Iterable<Integer>>)
            value -> Collections.singletonList(new Integer(value)));

        final KStream<String, Integer>[] streams2 = stream2.branch(
            (key, value) -> (value % 2) == 0,
            (key, value) -> true
        );

        final KStream<String, Integer>[] streams3 = stream3.branch(
            (key, value) -> (value % 2) == 0,
            (key, value) -> true
        );

        final int anyWindowSize = 1;
        final Joined<String, Integer, Integer> joined = Joined.with(Serdes.String(), Serdes.Integer(), Serdes.Integer());
        final KStream<String, Integer> stream4 = streams2[0].join(streams3[0],
            (value1, value2) -> value1 + value2, JoinWindows.of(ofMillis(anyWindowSize)), joined);

        streams2[1].join(streams3[1], (value1, value2) -> value1 + value2,
            JoinWindows.of(ofMillis(anyWindowSize)), joined);

        stream4.to("topic-5");

        streams2[1].through("topic-6").process(new MockProcessorSupplier<>());

        assertEquals(2 + // sources
            2 + // stream1
            1 + // stream2
            1 + // stream3
            1 + 2 + // streams2
            1 + 2 + // streams3
            5 * 2 + // stream2-stream3 joins
            1 + // to
            2 + // through
            1, // process
            TopologyWrapper.getInternalTopologyBuilder(builder.build()).setApplicationId("X").build(null).processors().size());
    }

    @Test
    public void shouldPreserveSerdesForOperators() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> stream1 = builder.stream(Collections.singleton("topic-1"), stringConsumed);
        final KTable<String, String> table1 = builder.table("topic-2", stringConsumed);
        final GlobalKTable<String, String> table2 = builder.globalTable("topic-2", stringConsumed);
        final ConsumedInternal<String, String> consumedInternal = new ConsumedInternal<>(stringConsumed);

        final KeyValueMapper<String, String, String> selector = (key, value) -> key;
        final KeyValueMapper<String, String, Iterable<KeyValue<String, String>>> flatSelector = (key, value) -> Collections.singleton(new KeyValue<>(key, value));
        final ValueMapper<String, String> mapper = value -> value;
        final ValueMapper<String, Iterable<String>> flatMapper = Collections::singleton;
        final ValueJoiner<String, String, String> joiner = (value1, value2) -> value1;
        final TransformerSupplier<String, String, KeyValue<String, String>> transformerSupplier = () -> new Transformer<String, String, KeyValue<String, String>>() {
            @Override
            public void init(final ProcessorContext context) {}

            @Override
            public KeyValue<String, String> transform(final String key, final String value) {
                return new KeyValue<>(key, value);
            }

            @Override
            public void close() {}
        };
        final ValueTransformerSupplier<String, String> valueTransformerSupplier = () -> new ValueTransformer<String, String>() {
            @Override
            public void init(final ProcessorContext context) {}

            @Override
            public String transform(final String value) {
                return value;
            }

            @Override
            public void close() {}
        };

        assertEquals(((AbstractStream) stream1.filter((key, value) -> false)).keySerde(), consumedInternal.keySerde());
        assertEquals(((AbstractStream) stream1.filter((key, value) -> false)).valueSerde(), consumedInternal.valueSerde());

        assertEquals(((AbstractStream) stream1.filterNot((key, value) -> false)).keySerde(), consumedInternal.keySerde());
        assertEquals(((AbstractStream) stream1.filterNot((key, value) -> false)).valueSerde(), consumedInternal.valueSerde());

        assertNull(((AbstractStream) stream1.selectKey(selector)).keySerde());
        assertEquals(((AbstractStream) stream1.selectKey(selector)).valueSerde(), consumedInternal.valueSerde());

        assertNull(((AbstractStream) stream1.map(KeyValue::new)).keySerde());
        assertNull(((AbstractStream) stream1.map(KeyValue::new)).valueSerde());

        assertEquals(((AbstractStream) stream1.mapValues(mapper)).keySerde(), consumedInternal.keySerde());
        assertNull(((AbstractStream) stream1.mapValues(mapper)).valueSerde());

        assertNull(((AbstractStream) stream1.flatMap(flatSelector)).keySerde());
        assertNull(((AbstractStream) stream1.flatMap(flatSelector)).valueSerde());

        assertEquals(((AbstractStream) stream1.flatMapValues(flatMapper)).keySerde(), consumedInternal.keySerde());
        assertNull(((AbstractStream) stream1.flatMapValues(flatMapper)).valueSerde());

        assertNull(((AbstractStream) stream1.transform(transformerSupplier)).keySerde());
        assertNull(((AbstractStream) stream1.transform(transformerSupplier)).valueSerde());

        assertEquals(((AbstractStream) stream1.transformValues(valueTransformerSupplier)).keySerde(), consumedInternal.keySerde());
        assertNull(((AbstractStream) stream1.transformValues(valueTransformerSupplier)).valueSerde());

        assertNull(((AbstractStream) stream1.merge(stream1)).keySerde());
        assertNull(((AbstractStream) stream1.merge(stream1)).valueSerde());

        assertEquals(((AbstractStream) stream1.through("topic-3")).keySerde(), consumedInternal.keySerde());
        assertEquals(((AbstractStream) stream1.through("topic-3")).valueSerde(), consumedInternal.valueSerde());
        assertEquals(((AbstractStream) stream1.through("topic-3", Produced.with(mySerde, mySerde))).keySerde(), mySerde);
        assertEquals(((AbstractStream) stream1.through("topic-3", Produced.with(mySerde, mySerde))).valueSerde(), mySerde);

        assertEquals(((AbstractStream) stream1.groupByKey()).keySerde(), consumedInternal.keySerde());
        assertEquals(((AbstractStream) stream1.groupByKey()).valueSerde(), consumedInternal.valueSerde());
        assertEquals(((AbstractStream) stream1.groupByKey(Grouped.with(mySerde, mySerde))).keySerde(), mySerde);
        assertEquals(((AbstractStream) stream1.groupByKey(Grouped.with(mySerde, mySerde))).valueSerde(), mySerde);

        assertNull(((AbstractStream) stream1.groupBy(selector)).keySerde());
        assertEquals(((AbstractStream) stream1.groupBy(selector)).valueSerde(), consumedInternal.valueSerde());
        assertEquals(((AbstractStream) stream1.groupBy(selector, Grouped.with(mySerde, mySerde))).keySerde(), mySerde);
        assertEquals(((AbstractStream) stream1.groupBy(selector, Grouped.with(mySerde, mySerde))).valueSerde(), mySerde);

        assertNull(((AbstractStream) stream1.join(stream1, joiner, JoinWindows.of(Duration.ofMillis(100L)))).keySerde());
        assertNull(((AbstractStream) stream1.join(stream1, joiner, JoinWindows.of(Duration.ofMillis(100L)))).valueSerde());
        assertEquals(((AbstractStream) stream1.join(stream1, joiner, JoinWindows.of(Duration.ofMillis(100L)), Joined.with(mySerde, mySerde, mySerde))).keySerde(), mySerde);
        assertNull(((AbstractStream) stream1.join(stream1, joiner, JoinWindows.of(Duration.ofMillis(100L)), Joined.with(mySerde, mySerde, mySerde))).valueSerde());

        assertNull(((AbstractStream) stream1.leftJoin(stream1, joiner, JoinWindows.of(Duration.ofMillis(100L)))).keySerde());
        assertNull(((AbstractStream) stream1.leftJoin(stream1, joiner, JoinWindows.of(Duration.ofMillis(100L)))).valueSerde());
        assertEquals(((AbstractStream) stream1.leftJoin(stream1, joiner, JoinWindows.of(Duration.ofMillis(100L)), Joined.with(mySerde, mySerde, mySerde))).keySerde(), mySerde);
        assertNull(((AbstractStream) stream1.leftJoin(stream1, joiner, JoinWindows.of(Duration.ofMillis(100L)), Joined.with(mySerde, mySerde, mySerde))).valueSerde());

        assertNull(((AbstractStream) stream1.outerJoin(stream1, joiner, JoinWindows.of(Duration.ofMillis(100L)))).keySerde());
        assertNull(((AbstractStream) stream1.outerJoin(stream1, joiner, JoinWindows.of(Duration.ofMillis(100L)))).valueSerde());
        assertEquals(((AbstractStream) stream1.outerJoin(stream1, joiner, JoinWindows.of(Duration.ofMillis(100L)), Joined.with(mySerde, mySerde, mySerde))).keySerde(), mySerde);
        assertNull(((AbstractStream) stream1.outerJoin(stream1, joiner, JoinWindows.of(Duration.ofMillis(100L)), Joined.with(mySerde, mySerde, mySerde))).valueSerde());

        assertEquals(((AbstractStream) stream1.join(table1, joiner)).keySerde(), consumedInternal.keySerde());
        assertNull(((AbstractStream) stream1.join(table1, joiner)).valueSerde());
        assertEquals(((AbstractStream) stream1.join(table1, joiner, Joined.with(mySerde, mySerde, mySerde))).keySerde(), mySerde);
        assertNull(((AbstractStream) stream1.join(table1, joiner, Joined.with(mySerde, mySerde, mySerde))).valueSerde());

        assertEquals(((AbstractStream) stream1.leftJoin(table1, joiner)).keySerde(), consumedInternal.keySerde());
        assertNull(((AbstractStream) stream1.leftJoin(table1, joiner)).valueSerde());
        assertEquals(((AbstractStream) stream1.leftJoin(table1, joiner, Joined.with(mySerde, mySerde, mySerde))).keySerde(), mySerde);
        assertNull(((AbstractStream) stream1.leftJoin(table1, joiner, Joined.with(mySerde, mySerde, mySerde))).valueSerde());

        assertEquals(((AbstractStream) stream1.join(table2, selector, joiner)).keySerde(), consumedInternal.keySerde());
        assertNull(((AbstractStream) stream1.join(table2, selector, joiner)).valueSerde());

        assertEquals(((AbstractStream) stream1.leftJoin(table2, selector, joiner)).keySerde(), consumedInternal.keySerde());
        assertNull(((AbstractStream) stream1.leftJoin(table2, selector, joiner)).valueSerde());
    }

    @Test
    public void shouldUseRecordMetadataTimestampExtractorWithThrough() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> stream1 = builder.stream(Arrays.asList("topic-1", "topic-2"), stringConsumed);
        final KStream<String, String> stream2 = builder.stream(Arrays.asList("topic-3", "topic-4"), stringConsumed);

        stream1.to("topic-5");
        stream2.through("topic-6");

        final ProcessorTopology processorTopology = TopologyWrapper.getInternalTopologyBuilder(builder.build()).setApplicationId("X").build(null);
        assertThat(processorTopology.source("topic-6").getTimestampExtractor(), instanceOf(FailOnInvalidTimestamp.class));
        assertNull(processorTopology.source("topic-4").getTimestampExtractor());
        assertNull(processorTopology.source("topic-3").getTimestampExtractor());
        assertNull(processorTopology.source("topic-2").getTimestampExtractor());
        assertNull(processorTopology.source("topic-1").getTimestampExtractor());
    }

    @Test
    public void shouldSendDataThroughTopicUsingProduced() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String input = "topic";
        final KStream<String, String> stream = builder.stream(input, stringConsumed);
        stream.through("through-topic", Produced.with(Serdes.String(), Serdes.String())).process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            driver.pipeInput(recordFactory.create(input, "a", "b"));
        }
        assertThat(processorSupplier.theCapturedProcessor().processed, equalTo(Collections.singletonList("a:b (ts: 0)")));
    }

    @Test
    public void shouldSendDataToTopicUsingProduced() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String input = "topic";
        final KStream<String, String> stream = builder.stream(input, stringConsumed);
        stream.to("to-topic", Produced.with(Serdes.String(), Serdes.String()));
        builder.stream("to-topic", stringConsumed).process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            driver.pipeInput(recordFactory.create(input, "e", "f"));
        }
        assertThat(processorSupplier.theCapturedProcessor().processed, equalTo(Collections.singletonList("e:f (ts: 0)")));
    }

    @Test
    public void shouldSendDataToDynamicTopics() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String input = "topic";
        final KStream<String, String> stream = builder.stream(input, stringConsumed);
        stream.to((key, value, context) -> context.topic() + "-" + key + "-" + value.substring(0, 1),
                  Produced.with(Serdes.String(), Serdes.String()));
        builder.stream(input + "-a-v", stringConsumed).process(processorSupplier);
        builder.stream(input + "-b-v", stringConsumed).process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            driver.pipeInput(recordFactory.create(input, "a", "v1"));
            driver.pipeInput(recordFactory.create(input, "a", "v2"));
            driver.pipeInput(recordFactory.create(input, "b", "v1"));
        }
        final List<MockProcessor<String, String>> mockProcessors = processorSupplier.capturedProcessors(2);
        assertThat(mockProcessors.get(0).processed, equalTo(asList("a:v1 (ts: 0)", "a:v2 (ts: 0)")));
        assertThat(mockProcessors.get(1).processed, equalTo(Collections.singletonList("b:v1 (ts: 0)")));
    }

    @SuppressWarnings("deprecation") // specifically testing the deprecated variant
    @Test
    public void shouldUseRecordMetadataTimestampExtractorWhenInternalRepartitioningTopicCreatedWithRetention() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> kStream = builder.stream("topic-1", stringConsumed);
        final ValueJoiner<String, String, String> valueJoiner = MockValueJoiner.instance(":");
        final long windowSize = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);
        final KStream<String, String> stream = kStream
            .map((key, value) -> KeyValue.pair(value, value));
        stream.join(kStream,
                    valueJoiner,
                    JoinWindows.of(ofMillis(windowSize)).grace(ofMillis(3 * windowSize)),
                    Joined.with(Serdes.String(),
                                Serdes.String(),
                                Serdes.String()))
              .to("output-topic", Produced.with(Serdes.String(), Serdes.String()));

        final ProcessorTopology topology = TopologyWrapper.getInternalTopologyBuilder(builder.build()).setApplicationId("X").build();

        final SourceNode originalSourceNode = topology.source("topic-1");

        for (final SourceNode sourceNode : topology.sources()) {
            if (sourceNode.name().equals(originalSourceNode.name())) {
                assertNull(sourceNode.getTimestampExtractor());
            } else {
                assertThat(sourceNode.getTimestampExtractor(), instanceOf(FailOnInvalidTimestamp.class));
            }
        }
    }

    @Test
    public void shouldUseRecordMetadataTimestampExtractorWhenInternalRepartitioningTopicCreated() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> kStream = builder.stream("topic-1", stringConsumed);
        final ValueJoiner<String, String, String> valueJoiner = MockValueJoiner.instance(":");
        final long windowSize = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);
        final KStream<String, String> stream = kStream
            .map((key, value) -> KeyValue.pair(value, value));
        stream.join(
            kStream,
            valueJoiner,
            JoinWindows.of(ofMillis(windowSize)).grace(ofMillis(3L * windowSize)),
            Joined.with(Serdes.String(), Serdes.String(), Serdes.String())
        )
              .to("output-topic", Produced.with(Serdes.String(), Serdes.String()));

        final ProcessorTopology topology = TopologyWrapper.getInternalTopologyBuilder(builder.build()).setApplicationId("X").build();

        final SourceNode originalSourceNode = topology.source("topic-1");

        for (final SourceNode sourceNode : topology.sources()) {
            if (sourceNode.name().equals(originalSourceNode.name())) {
                assertNull(sourceNode.getTimestampExtractor());
            } else {
                assertThat(sourceNode.getTimestampExtractor(), instanceOf(FailOnInvalidTimestamp.class));
            }
        }
    }

    @Test
    public void shouldPropagateRepartitionFlagAfterGlobalKTableJoin() {
        final StreamsBuilder builder = new StreamsBuilder();
        final GlobalKTable<String, String> globalKTable = builder.globalTable("globalTopic");
        final KeyValueMapper<String, String, String> kvMappper = (k, v) -> k + v;
        final ValueJoiner<String, String, String> valueJoiner = (v1, v2) -> v1 + v2;
        builder.<String, String>stream("topic").selectKey((k, v) -> v)
            .join(globalKTable, kvMappper, valueJoiner)
            .groupByKey()
            .count();

        final Pattern repartitionTopicPattern = Pattern.compile("Sink: .*-repartition");
        final String topology = builder.build().describe().toString();
        final Matcher matcher = repartitionTopicPattern.matcher(topology);
        assertTrue(matcher.find());
        final String match = matcher.group();
        assertThat(match, notNullValue());
        assertTrue(match.endsWith("repartition"));
    }

    @Test
    public void testToWithNullValueSerdeDoesntNPE() {
        final StreamsBuilder builder = new StreamsBuilder();
        final Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());
        final KStream<String, String> inputStream = builder.stream(Collections.singleton("input"), consumed);

        inputStream.to("output", Produced.with(Serdes.String(), Serdes.String()));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullPredicateOnFilter() {
        testStream.filter(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullPredicateOnFilterNot() {
        testStream.filterNot(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullMapperOnSelectKey() {
        testStream.selectKey(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullMapperOnMap() {
        testStream.map(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullMapperOnMapValues() {
        testStream.mapValues((ValueMapper) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullMapperOnMapValuesWithKey() {
        testStream.mapValues((ValueMapperWithKey) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullMapperOnFlatMap() {
        testStream.flatMap(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullMapperOnFlatMapValues() {
        testStream.flatMapValues((ValueMapper<? super String, ? extends Iterable<? extends String>>) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullMapperOnFlatMapValuesWithKey() {
        testStream.flatMapValues((ValueMapperWithKey<? super String, ? super String, ? extends Iterable<? extends String>>) null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldHaveAtLeastOnPredicateWhenBranching() {
        testStream.branch();
    }

    @Test(expected = NullPointerException.class)
    public void shouldCantHaveNullPredicate() {
        testStream.branch((Predicate) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullTopicOnThrough() {
        testStream.through(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullTopicOnTo() {
        testStream.to((String) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullTopicChooserOnTo() {
        testStream.to((TopicNameExtractor<String, String>) null);
    }

    @Test
    public void shouldNotAllowNullTransformerSupplierOnTransform() {
        final Exception e = assertThrows(NullPointerException.class, () -> testStream.transform(null));
        assertEquals("transformerSupplier can't be null", e.getMessage());
    }

    @Test
    public void shouldNotAllowNullTransformerSupplierOnFlatTransform() {
        final Exception e = assertThrows(NullPointerException.class, () -> testStream.flatTransform(null));
        assertEquals("transformerSupplier can't be null", e.getMessage());
    }

    @Test
    public void shouldNotAllowNullValueTransformerWithKeySupplierOnTransformValues() {
        final Exception e =
            assertThrows(NullPointerException.class, () -> testStream.transformValues((ValueTransformerWithKeySupplier) null));
        assertEquals("valueTransformerSupplier can't be null", e.getMessage());
    }

    @Test
    public void shouldNotAllowNullValueTransformerSupplierOnTransformValues() {
        final Exception e =
            assertThrows(NullPointerException.class, () -> testStream.transformValues((ValueTransformerSupplier) null));
        assertEquals("valueTransformerSupplier can't be null", e.getMessage());
    }

    @Test
    public void shouldNotAllowNullValueTransformerWithKeySupplierOnFlatTransformValues() {
        final Exception e =
            assertThrows(NullPointerException.class, () -> testStream.flatTransformValues((ValueTransformerWithKeySupplier) null));
        assertEquals("valueTransformerSupplier can't be null", e.getMessage());
    }

    @Test
    public void shouldNotAllowNullValueTransformerSupplierOnFlatTransformValues() {
        final Exception e =
            assertThrows(NullPointerException.class, () -> testStream.flatTransformValues((ValueTransformerSupplier) null));
        assertEquals("valueTransformerSupplier can't be null", e.getMessage());
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullProcessSupplier() {
        testStream.process(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullOtherStreamOnJoin() {
        testStream.join(null, MockValueJoiner.TOSTRING_JOINER, JoinWindows.of(ofMillis(10)));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullValueJoinerOnJoin() {
        testStream.join(testStream, null, JoinWindows.of(ofMillis(10)));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullJoinWindowsOnJoin() {
        testStream.join(testStream, MockValueJoiner.TOSTRING_JOINER, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullTableOnTableJoin() {
        testStream.leftJoin((KTable) null, MockValueJoiner.TOSTRING_JOINER);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullValueMapperOnTableJoin() {
        testStream.leftJoin(builder.table("topic", stringConsumed), null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullSelectorOnGroupBy() {
        testStream.groupBy(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullActionOnForEach() {
        testStream.foreach(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullTableOnJoinWithGlobalTable() {
        testStream.join((GlobalKTable) null,
                        MockMapper.selectValueMapper(),
                        MockValueJoiner.TOSTRING_JOINER);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullMapperOnJoinWithGlobalTable() {
        testStream.join(builder.globalTable("global", stringConsumed),
                        null,
                        MockValueJoiner.TOSTRING_JOINER);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullJoinerOnJoinWithGlobalTable() {
        testStream.join(builder.globalTable("global", stringConsumed),
                        MockMapper.selectValueMapper(),
                        null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullTableOnJLeftJoinWithGlobalTable() {
        testStream.leftJoin((GlobalKTable) null,
                        MockMapper.selectValueMapper(),
                        MockValueJoiner.TOSTRING_JOINER);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullMapperOnLeftJoinWithGlobalTable() {
        testStream.leftJoin(builder.globalTable("global", stringConsumed),
                        null,
                        MockValueJoiner.TOSTRING_JOINER);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullJoinerOnLeftJoinWithGlobalTable() {
        testStream.leftJoin(builder.globalTable("global", stringConsumed),
                        MockMapper.selectValueMapper(),
                        null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnPrintIfPrintedIsNull() {
        testStream.print(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnThroughWhenProducedIsNull() {
        testStream.through("topic", null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnToWhenProducedIsNull() {
        testStream.to("topic", null);
    }

    @Test
    public void shouldThrowNullPointerOnLeftJoinWithTableWhenJoinedIsNull() {
        final KTable<String, String> table = builder.table("blah", stringConsumed);
        try {
            testStream.leftJoin(table,
                                MockValueJoiner.TOSTRING_JOINER,
                                null);
            fail("Should have thrown NullPointerException");
        } catch (final NullPointerException e) {
            // ok
        }
    }

    @Test
    public void shouldThrowNullPointerOnJoinWithTableWhenJoinedIsNull() {
        final KTable<String, String> table = builder.table("blah", stringConsumed);
        try {
            testStream.join(table,
                            MockValueJoiner.TOSTRING_JOINER,
                            null);
            fail("Should have thrown NullPointerException");
        } catch (final NullPointerException e) {
            // ok
        }
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnJoinWithStreamWhenJoinedIsNull() {
        testStream.join(testStream, MockValueJoiner.TOSTRING_JOINER, JoinWindows.of(ofMillis(10)), null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnOuterJoinJoinedIsNull() {
        testStream.outerJoin(testStream, MockValueJoiner.TOSTRING_JOINER, JoinWindows.of(ofMillis(10)), null);
    }

    @Test
    public void shouldMergeTwoStreams() {
        final String topic1 = "topic-1";
        final String topic2 = "topic-2";

        final KStream<String, String> source1 = builder.stream(topic1);
        final KStream<String, String> source2 = builder.stream(topic2);
        final KStream<String, String> merged = source1.merge(source2);

        merged.process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            driver.pipeInput(recordFactory.create(topic1, "A", "aa"));
            driver.pipeInput(recordFactory.create(topic2, "B", "bb"));
            driver.pipeInput(recordFactory.create(topic2, "C", "cc"));
            driver.pipeInput(recordFactory.create(topic1, "D", "dd"));
        }

        assertEquals(asList("A:aa (ts: 0)", "B:bb (ts: 0)", "C:cc (ts: 0)", "D:dd (ts: 0)"), processorSupplier.theCapturedProcessor().processed);
    }

    @Test
    public void shouldMergeMultipleStreams() {
        final String topic1 = "topic-1";
        final String topic2 = "topic-2";
        final String topic3 = "topic-3";
        final String topic4 = "topic-4";

        final KStream<String, String> source1 = builder.stream(topic1);
        final KStream<String, String> source2 = builder.stream(topic2);
        final KStream<String, String> source3 = builder.stream(topic3);
        final KStream<String, String> source4 = builder.stream(topic4);
        final KStream<String, String> merged = source1.merge(source2).merge(source3).merge(source4);

        merged.process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            driver.pipeInput(recordFactory.create(topic1, "A", "aa", 1L));
            driver.pipeInput(recordFactory.create(topic2, "B", "bb", 9L));
            driver.pipeInput(recordFactory.create(topic3, "C", "cc", 2L));
            driver.pipeInput(recordFactory.create(topic4, "D", "dd", 8L));
            driver.pipeInput(recordFactory.create(topic4, "E", "ee", 3L));
            driver.pipeInput(recordFactory.create(topic3, "F", "ff", 7L));
            driver.pipeInput(recordFactory.create(topic2, "G", "gg", 4L));
            driver.pipeInput(recordFactory.create(topic1, "H", "hh", 6L));
        }

        assertEquals(asList("A:aa (ts: 1)", "B:bb (ts: 9)", "C:cc (ts: 2)", "D:dd (ts: 8)", "E:ee (ts: 3)", "F:ff (ts: 7)", "G:gg (ts: 4)", "H:hh (ts: 6)"),
                     processorSupplier.theCapturedProcessor().processed);
    }

    @Test
    public void shouldProcessFromSourceThatMatchPattern() {
        final KStream<String, String> pattern2Source = builder.stream(Pattern.compile("topic-\\d"));

        pattern2Source.process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            driver.pipeInput(recordFactory.create("topic-3", "A", "aa", 1L));
            driver.pipeInput(recordFactory.create("topic-4", "B", "bb", 5L));
            driver.pipeInput(recordFactory.create("topic-5", "C", "cc", 10L));
            driver.pipeInput(recordFactory.create("topic-6", "D", "dd", 8L));
            driver.pipeInput(recordFactory.create("topic-7", "E", "ee", 3L));
        }

        assertEquals(asList("A:aa (ts: 1)", "B:bb (ts: 5)", "C:cc (ts: 10)", "D:dd (ts: 8)", "E:ee (ts: 3)"),
                processorSupplier.theCapturedProcessor().processed);
    }

    @Test
    public void shouldProcessFromSourcesThatMatchMultiplePattern() {
        final String topic3 = "topic-without-pattern";

        final KStream<String, String> pattern2Source1 = builder.stream(Pattern.compile("topic-\\d"));
        final KStream<String, String> pattern2Source2 = builder.stream(Pattern.compile("topic-[A-Z]"));
        final KStream<String, String> source3 = builder.stream(topic3);
        final KStream<String, String> merged = pattern2Source1.merge(pattern2Source2).merge(source3);

        merged.process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            driver.pipeInput(recordFactory.create("topic-3", "A", "aa", 1L));
            driver.pipeInput(recordFactory.create("topic-4", "B", "bb", 5L));
            driver.pipeInput(recordFactory.create("topic-A", "C", "cc", 10L));
            driver.pipeInput(recordFactory.create("topic-Z", "D", "dd", 8L));
            driver.pipeInput(recordFactory.create(topic3, "E", "ee", 3L));
        }

        assertEquals(asList("A:aa (ts: 1)", "B:bb (ts: 5)", "C:cc (ts: 10)", "D:dd (ts: 8)", "E:ee (ts: 3)"),
                processorSupplier.theCapturedProcessor().processed);
    }
}
