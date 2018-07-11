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

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockReducer;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.test.StreamsTestUtils.getMetricByName;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

public class KGroupedStreamImplTest {

    private static final String TOPIC = "topic";
    private static final String INVALID_STORE_NAME = "~foo bar~";
    private final StreamsBuilder builder = new StreamsBuilder();
    private KGroupedStream<String, String> groupedStream;

    private final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());
    private final Properties props = StreamsTestUtils.topologyTestConfig(Serdes.String(), Serdes.String());

    @Before
    public void before() {
        final KStream<String, String> stream = builder.stream(TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        groupedStream = stream.groupByKey(Serialized.with(Serdes.String(), Serdes.String()));

    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullReducerOnReduce() {
        groupedStream.reduce(null);
    }

    @Test(expected = InvalidTopicException.class)
    public void shouldNotHaveInvalidStoreNameOnReduce() {
        groupedStream.reduce(MockReducer.STRING_ADDER, Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(INVALID_STORE_NAME));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullReducerWithWindowedReduce() {
        groupedStream.windowedBy(TimeWindows.of(10)).reduce(null, Materialized.<String, String, WindowStore<Bytes, byte[]>>as("store"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullWindowsWithWindowedReduce() {
        groupedStream.windowedBy((Windows) null);
    }

    @Test(expected = InvalidTopicException.class)
    public void shouldNotHaveInvalidStoreNameWithWindowedReduce() {
        groupedStream.windowedBy(TimeWindows.of(10)).reduce(MockReducer.STRING_ADDER, Materialized.<String, String, WindowStore<Bytes, byte[]>>as(INVALID_STORE_NAME));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullInitializerOnAggregate() {
        groupedStream.aggregate(null, MockAggregator.TOSTRING_ADDER, Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("store"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullAdderOnAggregate() {
        groupedStream.aggregate(MockInitializer.STRING_INIT, null, Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("store"));
    }

    @Test(expected = InvalidTopicException.class)
    public void shouldNotHaveInvalidStoreNameOnAggregate() {
        groupedStream.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(INVALID_STORE_NAME));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullInitializerOnWindowedAggregate() {
        groupedStream.windowedBy(TimeWindows.of(10)).aggregate(null, MockAggregator.TOSTRING_ADDER, Materialized.<String, String, WindowStore<Bytes, byte[]>>as("store"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullAdderOnWindowedAggregate() {
        groupedStream.windowedBy(TimeWindows.of(10)).aggregate(MockInitializer.STRING_INIT, null, Materialized.<String, String, WindowStore<Bytes, byte[]>>as("store"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullWindowsOnWindowedAggregate() {
        groupedStream.windowedBy((Windows) null);
    }

    @Test(expected = InvalidTopicException.class)
    public void shouldNotHaveInvalidStoreNameOnWindowedAggregate() {
        groupedStream.windowedBy(TimeWindows.of(10)).aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, Materialized.<String, String, WindowStore<Bytes, byte[]>>as(INVALID_STORE_NAME));
    }

    private void doAggregateSessionWindows(final Map<Windowed<String>, Integer> results) {
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            driver.pipeInput(recordFactory.create(TOPIC, "1", "1", 10));
            driver.pipeInput(recordFactory.create(TOPIC, "2", "2", 15));
            driver.pipeInput(recordFactory.create(TOPIC, "1", "1", 30));
            driver.pipeInput(recordFactory.create(TOPIC, "1", "1", 70));
            driver.pipeInput(recordFactory.create(TOPIC, "1", "1", 90));
            driver.pipeInput(recordFactory.create(TOPIC, "1", "1", 100));
        }
        assertEquals(Integer.valueOf(2), results.get(new Windowed<>("1", new SessionWindow(10, 30))));
        assertEquals(Integer.valueOf(1), results.get(new Windowed<>("2", new SessionWindow(15, 15))));
        assertEquals(Integer.valueOf(3), results.get(new Windowed<>("1", new SessionWindow(70, 100))));
    }

    @Test
    public void shouldAggregateSessionWindows() {
        final Map<Windowed<String>, Integer> results = new HashMap<>();
        final KTable<Windowed<String>, Integer> table = groupedStream.windowedBy(SessionWindows.with(30)).aggregate(new Initializer<Integer>() {
            @Override
            public Integer apply() {
                return 0;
            }
        }, new Aggregator<String, String, Integer>() {
            @Override
            public Integer apply(final String aggKey, final String value, final Integer aggregate) {
                return aggregate + 1;
            }
        }, new Merger<String, Integer>() {
            @Override
            public Integer apply(final String aggKey, final Integer aggOne, final Integer aggTwo) {
                return aggOne + aggTwo;
            }
        }, Materialized.<String, Integer, SessionStore<Bytes, byte[]>>as("session-store").withValueSerde(Serdes.Integer()));
        table.toStream().foreach(new ForeachAction<Windowed<String>, Integer>() {
            @Override
            public void apply(final Windowed<String> key, final Integer value) {
                results.put(key, value);
            }
        });

        doAggregateSessionWindows(results);
        assertEquals(table.queryableStoreName(), "session-store");
    }

    @Test
    public void shouldAggregateSessionWindowsWithInternalStoreName() {
        final Map<Windowed<String>, Integer> results = new HashMap<>();
        final KTable<Windowed<String>, Integer> table = groupedStream.windowedBy(SessionWindows.with(30)).aggregate(new Initializer<Integer>() {
            @Override
            public Integer apply() {
                return 0;
            }
        }, new Aggregator<String, String, Integer>() {
            @Override
            public Integer apply(final String aggKey, final String value, final Integer aggregate) {
                return aggregate + 1;
            }
        }, new Merger<String, Integer>() {
            @Override
            public Integer apply(final String aggKey, final Integer aggOne, final Integer aggTwo) {
                return aggOne + aggTwo;
            }
        }, Materialized.<String, Integer, SessionStore<Bytes, byte[]>>with(null, Serdes.Integer()));
        table.toStream().foreach(new ForeachAction<Windowed<String>, Integer>() {
            @Override
            public void apply(final Windowed<String> key, final Integer value) {
                results.put(key, value);
            }
        });

        doAggregateSessionWindows(results);
    }

    private void doCountSessionWindows(final Map<Windowed<String>, Long> results) {
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            driver.pipeInput(recordFactory.create(TOPIC, "1", "1", 10));
            driver.pipeInput(recordFactory.create(TOPIC, "2", "2", 15));
            driver.pipeInput(recordFactory.create(TOPIC, "1", "1", 30));
            driver.pipeInput(recordFactory.create(TOPIC, "1", "1", 70));
            driver.pipeInput(recordFactory.create(TOPIC, "1", "1", 90));
            driver.pipeInput(recordFactory.create(TOPIC, "1", "1", 100));
        }
        assertEquals(Long.valueOf(2), results.get(new Windowed<>("1", new SessionWindow(10, 30))));
        assertEquals(Long.valueOf(1), results.get(new Windowed<>("2", new SessionWindow(15, 15))));
        assertEquals(Long.valueOf(3), results.get(new Windowed<>("1", new SessionWindow(70, 100))));
    }

    @Test
    public void shouldCountSessionWindows() {
        final Map<Windowed<String>, Long> results = new HashMap<>();
        final KTable<Windowed<String>, Long> table = groupedStream.windowedBy(SessionWindows.with(30))
                .count(Materialized.<String, Long, SessionStore<Bytes, byte[]>>as("session-store"));
        table.toStream().foreach(new ForeachAction<Windowed<String>, Long>() {
            @Override
            public void apply(final Windowed<String> key, final Long value) {
                results.put(key, value);
            }
        });
        doCountSessionWindows(results);
        assertEquals(table.queryableStoreName(), "session-store");
    }

    @Test
    public void shouldCountSessionWindowsWithInternalStoreName() {
        final Map<Windowed<String>, Long> results = new HashMap<>();
        final KTable<Windowed<String>, Long> table = groupedStream.windowedBy(SessionWindows.with(30)).count();
        table.toStream().foreach(new ForeachAction<Windowed<String>, Long>() {
            @Override
            public void apply(final Windowed<String> key, final Long value) {
                results.put(key, value);
            }
        });
        doCountSessionWindows(results);
        assertNull(table.queryableStoreName());
    }

    private void doReduceSessionWindows(final Map<Windowed<String>, String> results) {
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            driver.pipeInput(recordFactory.create(TOPIC, "1", "A", 10));
            driver.pipeInput(recordFactory.create(TOPIC, "2", "Z", 15));
            driver.pipeInput(recordFactory.create(TOPIC, "1", "B", 30));
            driver.pipeInput(recordFactory.create(TOPIC, "1", "A", 70));
            driver.pipeInput(recordFactory.create(TOPIC, "1", "B", 90));
            driver.pipeInput(recordFactory.create(TOPIC, "1", "C", 100));
        }
        assertEquals("A:B", results.get(new Windowed<>("1", new SessionWindow(10, 30))));
        assertEquals("Z", results.get(new Windowed<>("2", new SessionWindow(15, 15))));
        assertEquals("A:B:C", results.get(new Windowed<>("1", new SessionWindow(70, 100))));
    }

    @Test
    public void shouldReduceSessionWindows() {
        final Map<Windowed<String>, String> results = new HashMap<>();
        final KTable<Windowed<String>, String> table = groupedStream.windowedBy(SessionWindows.with(30))
                .reduce(new Reducer<String>() {
                    @Override
                    public String apply(final String value1, final String value2) {
                        return value1 + ":" + value2;
                    }
                }, Materialized.<String, String, SessionStore<Bytes, byte[]>>as("session-store"));
        table.toStream().foreach(new ForeachAction<Windowed<String>, String>() {
            @Override
            public void apply(final Windowed<String> key, final String value) {
                results.put(key, value);
            }
        });
        doReduceSessionWindows(results);
        assertEquals(table.queryableStoreName(), "session-store");
    }

    @Test
    public void shouldReduceSessionWindowsWithInternalStoreName() {
        final Map<Windowed<String>, String> results = new HashMap<>();
        final KTable<Windowed<String>, String> table = groupedStream.windowedBy(SessionWindows.with(30))
                .reduce(new Reducer<String>() {
                    @Override
                    public String apply(final String value1, final String value2) {
                        return value1 + ":" + value2;
                    }
                });
        table.toStream().foreach(new ForeachAction<Windowed<String>, String>() {
            @Override
            public void apply(final Windowed<String> key, final String value) {
                results.put(key, value);
            }
        });
        doReduceSessionWindows(results);
        assertNull(table.queryableStoreName());
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAcceptNullReducerWhenReducingSessionWindows() {
        groupedStream.windowedBy(SessionWindows.with(30)).reduce(null, Materialized.<String, String, SessionStore<Bytes, byte[]>>as("store"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAcceptNullSessionWindowsReducingSessionWindows() {
        groupedStream.windowedBy((SessionWindows) null);
    }

    @Test(expected = InvalidTopicException.class)
    public void shouldNotAcceptInvalidStoreNameWhenReducingSessionWindows() {
        groupedStream.windowedBy(SessionWindows.with(30)).reduce(MockReducer.STRING_ADDER, Materialized.<String, String, SessionStore<Bytes, byte[]>>as(INVALID_STORE_NAME));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAcceptNullStateStoreSupplierWhenReducingSessionWindows() {
        groupedStream.windowedBy(SessionWindows.with(30)).reduce(null, Materialized.<String, String, SessionStore<Bytes, byte[]>>as(null));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAcceptNullInitializerWhenAggregatingSessionWindows() {
        groupedStream.windowedBy(SessionWindows.with(30)).aggregate(null, MockAggregator.TOSTRING_ADDER, new Merger<String, String>() {
            @Override
            public String apply(final String aggKey, final String aggOne, final String aggTwo) {
                return null;
            }
        }, Materialized.<String, String, SessionStore<Bytes, byte[]>>as("storeName"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAcceptNullAggregatorWhenAggregatingSessionWindows() {
        groupedStream.windowedBy(SessionWindows.with(30)).aggregate(MockInitializer.STRING_INIT, null, new Merger<String, String>() {
            @Override
            public String apply(final String aggKey, final String aggOne, final String aggTwo) {
                return null;
            }
        }, Materialized.<String, String, SessionStore<Bytes, byte[]>>as("storeName"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAcceptNullSessionMergerWhenAggregatingSessionWindows() {
        groupedStream.windowedBy(SessionWindows.with(30)).aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER,
                null,
                Materialized.<String, String, SessionStore<Bytes, byte[]>>as("storeName"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAcceptNullSessionWindowsWhenAggregatingSessionWindows() {
        groupedStream.windowedBy((SessionWindows) null);
    }

    @Test
    public void shouldAcceptNullStoreNameWhenAggregatingSessionWindows() {
        groupedStream.windowedBy(SessionWindows.with(10))
                .aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, new Merger<String, String>() {
                    @Override
                    public String apply(final String aggKey, final String aggOne, final String aggTwo) {
                        return null;
                    }
                }, Materialized.<String, String, SessionStore<Bytes, byte[]>>with(Serdes.String(), Serdes.String()));
    }

    @Test(expected = InvalidTopicException.class)
    public void shouldNotAcceptInvalidStoreNameWhenAggregatingSessionWindows() {
        groupedStream.windowedBy(SessionWindows.with(10))
                .aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, new Merger<String, String>() {
                    @Override
                    public String apply(final String aggKey, final String aggOne, final String aggTwo) {
                        return null;
                    }
                }, Materialized.<String, String, SessionStore<Bytes, byte[]>>as(INVALID_STORE_NAME));
    }

    @SuppressWarnings("unchecked")
    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnReduceWhenMaterializedIsNull() {
        groupedStream.reduce(MockReducer.STRING_ADDER, (Materialized) null);
    }

    @SuppressWarnings("unchecked")
    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnAggregateWhenMaterializedIsNull() {
        groupedStream.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, (Materialized) null);
    }

    @SuppressWarnings("unchecked")
    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnCountWhenMaterializedIsNull() {
        groupedStream.count((Materialized) null);
    }

    @Test
    public void shouldCountAndMaterializeResults() {
        groupedStream.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("count").withKeySerde(Serdes.String()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);

            final KeyValueStore<String, Long> count = driver.getKeyValueStore("count");

            assertThat(count.get("1"), equalTo(3L));
            assertThat(count.get("2"), equalTo(1L));
            assertThat(count.get("3"), equalTo(2L));
        }
    }

    @Test
    public void shouldLogAndMeasureSkipsInAggregate() {
        groupedStream.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("count").withKeySerde(Serdes.String()));
        final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);
            LogCaptureAppender.unregister(appender);

            final Map<MetricName, ? extends Metric> metrics = driver.metrics();
            assertEquals(1.0, getMetricByName(metrics, "skipped-records-total", "stream-metrics").metricValue());
            assertNotEquals(0.0, getMetricByName(metrics, "skipped-records-rate", "stream-metrics").metricValue());
            assertThat(appender.getMessages(), hasItem("Skipping record due to null key or value. key=[3] value=[null] topic=[topic] partition=[0] offset=[6]"));
        }
    }


    @SuppressWarnings("unchecked")
    @Test
    public void shouldReduceAndMaterializeResults() {
        groupedStream.reduce(
            MockReducer.STRING_ADDER,
            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("reduce")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);

            final KeyValueStore<String, String> reduced = driver.getKeyValueStore("reduce");

            assertThat(reduced.get("1"), equalTo("A+C+D"));
            assertThat(reduced.get("2"), equalTo("B"));
            assertThat(reduced.get("3"), equalTo("E+F"));
        }
    }

    @Test
    public void shouldLogAndMeasureSkipsInReduce() {
        groupedStream.reduce(
            MockReducer.STRING_ADDER,
            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("reduce")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())
        );

        final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);
            LogCaptureAppender.unregister(appender);

            final Map<MetricName, ? extends Metric> metrics = driver.metrics();
            assertEquals(1.0, getMetricByName(metrics, "skipped-records-total", "stream-metrics").metricValue());
            assertNotEquals(0.0, getMetricByName(metrics, "skipped-records-rate", "stream-metrics").metricValue());
            assertThat(appender.getMessages(), hasItem("Skipping record due to null key or value. key=[3] value=[null] topic=[topic] partition=[0] offset=[6]"));
        }
    }


    @SuppressWarnings("unchecked")
    @Test
    public void shouldAggregateAndMaterializeResults() {
        groupedStream.aggregate(
            MockInitializer.STRING_INIT,
            MockAggregator.TOSTRING_ADDER,
            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("aggregate")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);

            final KeyValueStore<String, String> aggregate = driver.getKeyValueStore("aggregate");

            assertThat(aggregate.get("1"), equalTo("0+A+C+D"));
            assertThat(aggregate.get("2"), equalTo("0+B"));
            assertThat(aggregate.get("3"), equalTo("0+E+F"));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldAggregateWithDefaultSerdes() {
        final Map<String, String> results = new HashMap<>();
        groupedStream.aggregate(
            MockInitializer.STRING_INIT,
            MockAggregator.TOSTRING_ADDER)
            .toStream()
            .foreach(new ForeachAction<String, String>() {
                @Override
                public void apply(final String key, final String value) {
                    results.put(key, value);
                }
            });

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);

            assertThat(results.get("1"), equalTo("0+A+C+D"));
            assertThat(results.get("2"), equalTo("0+B"));
            assertThat(results.get("3"), equalTo("0+E+F"));
        }
    }

    private void processData(final TopologyTestDriver driver) {
        driver.pipeInput(recordFactory.create(TOPIC, "1", "A"));
        driver.pipeInput(recordFactory.create(TOPIC, "2", "B"));
        driver.pipeInput(recordFactory.create(TOPIC, "1", "C"));
        driver.pipeInput(recordFactory.create(TOPIC, "1", "D"));
        driver.pipeInput(recordFactory.create(TOPIC, "3", "E"));
        driver.pipeInput(recordFactory.create(TOPIC, "3", "F"));
        driver.pipeInput(recordFactory.create(TOPIC, "3", (String) null));
    }

    private void doCountWindowed(final List<KeyValue<Windowed<String>, Long>> results) {
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            driver.pipeInput(recordFactory.create(TOPIC, "1", "A", 0));
            driver.pipeInput(recordFactory.create(TOPIC, "2", "B", 0));
            driver.pipeInput(recordFactory.create(TOPIC, "3", "C", 0));
            driver.pipeInput(recordFactory.create(TOPIC, "1", "A", 500));
            driver.pipeInput(recordFactory.create(TOPIC, "1", "A", 500));
            driver.pipeInput(recordFactory.create(TOPIC, "2", "B", 500));
            driver.pipeInput(recordFactory.create(TOPIC, "2", "B", 500));
        }
        assertThat(results, equalTo(Arrays.asList(
                KeyValue.pair(new Windowed<>("1", new TimeWindow(0, 500)), 1L),
                KeyValue.pair(new Windowed<>("2", new TimeWindow(0, 500)), 1L),
                KeyValue.pair(new Windowed<>("3", new TimeWindow(0, 500)), 1L),
                KeyValue.pair(new Windowed<>("1", new TimeWindow(500, 1000)), 1L),
                KeyValue.pair(new Windowed<>("1", new TimeWindow(500, 1000)), 2L),
                KeyValue.pair(new Windowed<>("2", new TimeWindow(500, 1000)), 1L),
                KeyValue.pair(new Windowed<>("2", new TimeWindow(500, 1000)), 2L)
        )));
    }

    @Test
    public void shouldCountWindowed() {
        final List<KeyValue<Windowed<String>, Long>> results = new ArrayList<>();
        groupedStream.windowedBy(TimeWindows.of(500L)).count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("aggregate-by-key-windowed"))
            .toStream()
            .foreach(new ForeachAction<Windowed<String>, Long>() {
                @Override
                public void apply(final Windowed<String> key, final Long value) {
                    results.add(KeyValue.pair(key, value));
                }
            });

        doCountWindowed(results);
    }

    @Test
    public void shouldCountWindowedWithInternalStoreName() {
        final List<KeyValue<Windowed<String>, Long>> results = new ArrayList<>();
        groupedStream.windowedBy(TimeWindows.of(500L)).count()
            .toStream()
            .foreach(new ForeachAction<Windowed<String>, Long>() {
                @Override
                public void apply(final Windowed<String> key, final Long value) {
                    results.add(KeyValue.pair(key, value));
                }
            });

        doCountWindowed(results);
    }
}