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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
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
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockReducer;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    @Rule
    public final KStreamTestDriver driver = new KStreamTestDriver();

    @Before
    public void before() {
        final KStream<String, String> stream = builder.stream(TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        groupedStream = stream.groupByKey(Serialized.with(Serdes.String(), Serdes.String()));
    }

    @SuppressWarnings("deprecation")
    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullReducerOnReduce() {
        groupedStream.reduce(null, "store");
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldAllowNullStoreNameOnReduce() {
        groupedStream.reduce(MockReducer.STRING_ADDER, (String) null);
    }

    @SuppressWarnings("deprecation")
    @Test(expected = InvalidTopicException.class)
    public void shouldNotHaveInvalidStoreNameOnReduce() {
        groupedStream.reduce(MockReducer.STRING_ADDER, INVALID_STORE_NAME);
    }

    @SuppressWarnings("deprecation")
    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullStoreSupplierOnReduce() {
        groupedStream.reduce(MockReducer.STRING_ADDER, (StateStoreSupplier<KeyValueStore>) null);
    }

    @SuppressWarnings("deprecation")
    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullStoreSupplierOnCount() {
        groupedStream.count((StateStoreSupplier<KeyValueStore>) null);
    }


    @SuppressWarnings("deprecation")
    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullStoreSupplierOnWindowedCount() {
        groupedStream.count(TimeWindows.of(10), (StateStoreSupplier<WindowStore>) null);
    }

    @SuppressWarnings("deprecation")
    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullReducerWithWindowedReduce() {
        groupedStream.reduce(null, TimeWindows.of(10), "store");
    }

    @SuppressWarnings("deprecation")
    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullWindowsWithWindowedReduce() {
        groupedStream.reduce(MockReducer.STRING_ADDER, (Windows) null, "store");
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldAllowNullStoreNameWithWindowedReduce() {
        groupedStream.reduce(MockReducer.STRING_ADDER, TimeWindows.of(10), (String) null);
    }

    @SuppressWarnings("deprecation")
    @Test(expected = InvalidTopicException.class)
    public void shouldNotHaveInvalidStoreNameWithWindowedReduce() {
        groupedStream.reduce(MockReducer.STRING_ADDER, TimeWindows.of(10), INVALID_STORE_NAME);
    }

    @SuppressWarnings("deprecation")
    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullInitializerOnAggregate() {
        groupedStream.aggregate(null, MockAggregator.TOSTRING_ADDER, Serdes.String(), "store");
    }

    @SuppressWarnings("deprecation")
    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullAdderOnAggregate() {
        groupedStream.aggregate(MockInitializer.STRING_INIT, null, Serdes.String(), "store");
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldAllowNullStoreNameOnAggregate() {
        groupedStream.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, Serdes.String(), null);
    }

    @SuppressWarnings("deprecation")
    @Test(expected = InvalidTopicException.class)
    public void shouldNotHaveInvalidStoreNameOnAggregate() {
        groupedStream.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, Serdes.String(), INVALID_STORE_NAME);
    }

    @SuppressWarnings("deprecation")
    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullInitializerOnWindowedAggregate() {
        groupedStream.aggregate(null, MockAggregator.TOSTRING_ADDER, TimeWindows.of(10), Serdes.String(), "store");
    }

    @SuppressWarnings("deprecation")
    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullAdderOnWindowedAggregate() {
        groupedStream.aggregate(MockInitializer.STRING_INIT, null, TimeWindows.of(10), Serdes.String(), "store");
    }

    @SuppressWarnings("deprecation")
    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullWindowsOnWindowedAggregate() {
        groupedStream.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, null, Serdes.String(), "store");
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldAllowNullStoreNameOnWindowedAggregate() {
        groupedStream.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, TimeWindows.of(10), Serdes.String(), null);
    }

    @SuppressWarnings("deprecation")
    @Test(expected = InvalidTopicException.class)
    public void shouldNotHaveInvalidStoreNameOnWindowedAggregate() {
        groupedStream.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, TimeWindows.of(10), Serdes.String(), INVALID_STORE_NAME);
    }

    @SuppressWarnings("deprecation")
    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullStoreSupplierOnWindowedAggregate() {
        groupedStream.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, TimeWindows.of(10), (StateStoreSupplier<WindowStore>) null);
    }

    private void doAggregateSessionWindows(final Map<Windowed<String>, Integer> results) {
        driver.setUp(builder, TestUtils.tempDirectory());
        driver.setTime(10);
        driver.process(TOPIC, "1", "1");
        driver.setTime(15);
        driver.process(TOPIC, "2", "2");
        driver.setTime(30);
        driver.process(TOPIC, "1", "1");
        driver.setTime(70);
        driver.process(TOPIC, "1", "1");
        driver.setTime(90);
        driver.process(TOPIC, "1", "1");
        driver.setTime(100);
        driver.process(TOPIC, "1", "1");
        driver.flushState();
        assertEquals(Integer.valueOf(2), results.get(new Windowed<>("1", new SessionWindow(10, 30))));
        assertEquals(Integer.valueOf(1), results.get(new Windowed<>("2", new SessionWindow(15, 15))));
        assertEquals(Integer.valueOf(3), results.get(new Windowed<>("1", new SessionWindow(70, 100))));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldAggregateSessionWindows() {
        final Map<Windowed<String>, Integer> results = new HashMap<>();
        final KTable<Windowed<String>, Integer> table = groupedStream.aggregate(new Initializer<Integer>() {
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
        }, SessionWindows.with(30), Serdes.Integer(), "session-store");
        table.toStream().foreach(new ForeachAction<Windowed<String>, Integer>() {
            @Override
            public void apply(final Windowed<String> key, final Integer value) {
                results.put(key, value);
            }
        });

        doAggregateSessionWindows(results);
        assertEquals(table.queryableStoreName(), "session-store");
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldAggregateSessionWindowsWithInternalStoreName() {
        final Map<Windowed<String>, Integer> results = new HashMap<>();
        final KTable<Windowed<String>, Integer> table = groupedStream.aggregate(new Initializer<Integer>() {
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
        }, SessionWindows.with(30), Serdes.Integer());
        table.toStream().foreach(new ForeachAction<Windowed<String>, Integer>() {
            @Override
            public void apply(final Windowed<String> key, final Integer value) {
                results.put(key, value);
            }
        });

        doAggregateSessionWindows(results);
    }

    private void doCountSessionWindows(final Map<Windowed<String>, Long> results) {
        driver.setUp(builder, TestUtils.tempDirectory());
        driver.setTime(10);
        driver.process(TOPIC, "1", "1");
        driver.setTime(15);
        driver.process(TOPIC, "2", "2");
        driver.setTime(30);
        driver.process(TOPIC, "1", "1");
        driver.setTime(70);
        driver.process(TOPIC, "1", "1");
        driver.setTime(90);
        driver.process(TOPIC, "1", "1");
        driver.setTime(100);
        driver.process(TOPIC, "1", "1");
        driver.flushState();
        assertEquals(Long.valueOf(2), results.get(new Windowed<>("1", new SessionWindow(10, 30))));
        assertEquals(Long.valueOf(1), results.get(new Windowed<>("2", new SessionWindow(15, 15))));
        assertEquals(Long.valueOf(3), results.get(new Windowed<>("1", new SessionWindow(70, 100))));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldCountSessionWindows() {
        final Map<Windowed<String>, Long> results = new HashMap<>();
        final KTable<Windowed<String>, Long> table = groupedStream.count(SessionWindows.with(30), "session-store");
        table.toStream().foreach(new ForeachAction<Windowed<String>, Long>() {
            @Override
            public void apply(final Windowed<String> key, final Long value) {
                results.put(key, value);
            }
        });
        doCountSessionWindows(results);
        assertEquals(table.queryableStoreName(), "session-store");
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldCountSessionWindowsWithInternalStoreName() {
        final Map<Windowed<String>, Long> results = new HashMap<>();
        final KTable<Windowed<String>, Long> table = groupedStream.count(SessionWindows.with(30));
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
        driver.setUp(builder, TestUtils.tempDirectory());
        driver.setTime(10);
        driver.process(TOPIC, "1", "A");
        driver.setTime(15);
        driver.process(TOPIC, "2", "Z");
        driver.setTime(30);
        driver.process(TOPIC, "1", "B");
        driver.setTime(70);
        driver.process(TOPIC, "1", "A");
        driver.setTime(90);
        driver.process(TOPIC, "1", "B");
        driver.setTime(100);
        driver.process(TOPIC, "1", "C");
        driver.flushState();
        assertEquals("A:B", results.get(new Windowed<>("1", new SessionWindow(10, 30))));
        assertEquals("Z", results.get(new Windowed<>("2", new SessionWindow(15, 15))));
        assertEquals("A:B:C", results.get(new Windowed<>("1", new SessionWindow(70, 100))));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldReduceSessionWindows() {
        final Map<Windowed<String>, String> results = new HashMap<>();
        final KTable<Windowed<String>, String> table = groupedStream.reduce(
            new Reducer<String>() {
                @Override
                public String apply(final String value1, final String value2) {
                    return value1 + ":" + value2;
                }
            },
            SessionWindows.with(30),
            "session-store"
        );
        table.toStream().foreach(new ForeachAction<Windowed<String>, String>() {
            @Override
            public void apply(final Windowed<String> key, final String value) {
                results.put(key, value);
            }
        });
        doReduceSessionWindows(results);
        assertEquals(table.queryableStoreName(), "session-store");
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldReduceSessionWindowsWithInternalStoreName() {
        final Map<Windowed<String>, String> results = new HashMap<>();
        final KTable<Windowed<String>, String> table = groupedStream.reduce(
            new Reducer<String>() {
                @Override
                public String apply(final String value1, final String value2) {
                    return value1 + ":" + value2;
                }
            },
            SessionWindows.with(30)
        );
        table.toStream().foreach(new ForeachAction<Windowed<String>, String>() {
            @Override
            public void apply(final Windowed<String> key, final String value) {
                results.put(key, value);
            }
        });
        doReduceSessionWindows(results);
        assertNull(table.queryableStoreName());
    }

    @SuppressWarnings("deprecation")
    @Test(expected = NullPointerException.class)
    public void shouldNotAcceptNullReducerWhenReducingSessionWindows() {
        groupedStream.reduce(null, SessionWindows.with(10), "store");
    }

    @SuppressWarnings("deprecation")
    @Test(expected = NullPointerException.class)
    public void shouldNotAcceptNullSessionWindowsReducingSessionWindows() {
        groupedStream.reduce(MockReducer.STRING_ADDER, (SessionWindows) null, "store");
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldAcceptNullStoreNameWhenReducingSessionWindows() {
        groupedStream.reduce(MockReducer.STRING_ADDER, SessionWindows.with(10), (String) null);
    }

    @SuppressWarnings("deprecation")
    @Test(expected = InvalidTopicException.class)
    public void shouldNotAcceptInvalidStoreNameWhenReducingSessionWindows() {
        groupedStream.reduce(MockReducer.STRING_ADDER, SessionWindows.with(10), INVALID_STORE_NAME);
    }

    @SuppressWarnings("deprecation")
    @Test(expected = NullPointerException.class)
    public void shouldNotAcceptNullStateStoreSupplierWhenReducingSessionWindows() {
        groupedStream.reduce(MockReducer.STRING_ADDER, SessionWindows.with(10), (StateStoreSupplier<SessionStore>) null);
    }

    @SuppressWarnings("deprecation")
    @Test(expected = NullPointerException.class)
    public void shouldNotAcceptNullInitializerWhenAggregatingSessionWindows() {
        groupedStream.aggregate(null, MockAggregator.TOSTRING_ADDER, new Merger<String, String>() {
            @Override
            public String apply(final String aggKey, final String aggOne, final String aggTwo) {
                return null;
            }
        }, SessionWindows.with(10), Serdes.String(), "storeName");
    }

    @SuppressWarnings("deprecation")
    @Test(expected = NullPointerException.class)
    public void shouldNotAcceptNullAggregatorWhenAggregatingSessionWindows() {
        groupedStream.aggregate(MockInitializer.STRING_INIT, null, new Merger<String, String>() {
            @Override
            public String apply(final String aggKey, final String aggOne, final String aggTwo) {
                return null;
            }
        }, SessionWindows.with(10), Serdes.String(), "storeName");
    }

    @SuppressWarnings("deprecation")
    @Test(expected = NullPointerException.class)
    public void shouldNotAcceptNullSessionMergerWhenAggregatingSessionWindows() {
        groupedStream.aggregate(
            MockInitializer.STRING_INIT,
            MockAggregator.TOSTRING_ADDER,
            null,
            SessionWindows.with(10),
            Serdes.String(),
            "storeName");
    }

    @SuppressWarnings("deprecation")
    @Test(expected = NullPointerException.class)
    public void shouldNotAcceptNullSessionWindowsWhenAggregatingSessionWindows() {
        groupedStream.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, new Merger<String, String>() {
            @Override
            public String apply(final String aggKey, final String aggOne, final String aggTwo) {
                return null;
            }
        }, null, Serdes.String(), "storeName");
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldAcceptNullStoreNameWhenAggregatingSessionWindows() {
        groupedStream.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, new Merger<String, String>() {
            @Override
            public String apply(final String aggKey, final String aggOne, final String aggTwo) {
                return null;
            }
        }, SessionWindows.with(10), Serdes.String(), (String) null);
    }

    @SuppressWarnings("deprecation")
    @Test(expected = InvalidTopicException.class)
    public void shouldNotAcceptInvalidStoreNameWhenAggregatingSessionWindows() {
        groupedStream.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, new Merger<String, String>() {
            @Override
            public String apply(final String aggKey, final String aggOne, final String aggTwo) {
                return null;
            }
        }, SessionWindows.with(10), Serdes.String(), INVALID_STORE_NAME);
    }

    @SuppressWarnings("deprecation")
    @Test(expected = NullPointerException.class)
    public void shouldNotAcceptNullStateStoreSupplierNameWhenAggregatingSessionWindows() {
        groupedStream.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, new Merger<String, String>() {
            @Override
            public String apply(final String aggKey, final String aggOne, final String aggTwo) {
                return null;
            }
        }, SessionWindows.with(10), Serdes.String(), (StateStoreSupplier<SessionStore>) null);
    }

    @SuppressWarnings("deprecation")
    @Test(expected = NullPointerException.class)
    public void shouldNotAcceptNullSessionWindowsWhenCountingSessionWindows() {
        groupedStream.count((SessionWindows) null, "store");
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldAcceptNullStoreNameWhenCountingSessionWindows() {
        groupedStream.count(SessionWindows.with(90), (String) null);
    }

    @SuppressWarnings("deprecation")
    @Test(expected = InvalidTopicException.class)
    public void shouldNotAcceptInvalidStoreNameWhenCountingSessionWindows() {
        groupedStream.count(SessionWindows.with(90), INVALID_STORE_NAME);
    }

    @SuppressWarnings("deprecation")
    @Test(expected = NullPointerException.class)
    public void shouldNotAcceptNullStateStoreSupplierWhenCountingSessionWindows() {
        groupedStream.count(SessionWindows.with(90), (StateStoreSupplier<SessionStore>) null);
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

        processData();

        @SuppressWarnings("unchecked") final KeyValueStore<String, Long> count =
            (KeyValueStore<String, Long>) driver.allStateStores().get("count");

        assertThat(count.get("1"), equalTo(3L));
        assertThat(count.get("2"), equalTo(1L));
        assertThat(count.get("3"), equalTo(2L));
    }

    @Test
    public void shouldLogAndMeasureSkipsInAggregate() {
        groupedStream.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("count").withKeySerde(Serdes.String()));
        final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
        processData();
        LogCaptureAppender.unregister(appender);

        final Map<MetricName, ? extends Metric> metrics = driver.context().metrics().metrics();
        assertEquals(1.0, getMetricByName(metrics, "skipped-records-total", "stream-metrics").metricValue());
        assertNotEquals(0.0, getMetricByName(metrics, "skipped-records-rate", "stream-metrics").metricValue());
        assertThat(appender.getMessages(), hasItem("Skipping record due to null key or value. key=[3] value=[null] topic=[topic] partition=[-1] offset=[-1]"));
    }


    @SuppressWarnings("unchecked")
    @Test
    public void shouldReduceAndMaterializeResults() {
        groupedStream.reduce(
            MockReducer.STRING_ADDER,
            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("reduce")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String()));

        processData();

        final KeyValueStore<String, String> reduced = (KeyValueStore<String, String>) driver.allStateStores().get("reduce");

        assertThat(reduced.get("1"), equalTo("A+C+D"));
        assertThat(reduced.get("2"), equalTo("B"));
        assertThat(reduced.get("3"), equalTo("E+F"));
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
        processData();
        LogCaptureAppender.unregister(appender);

        final Map<MetricName, ? extends Metric> metrics = driver.context().metrics().metrics();
        assertEquals(1.0, getMetricByName(metrics, "skipped-records-total", "stream-metrics").metricValue());
        assertNotEquals(0.0, getMetricByName(metrics, "skipped-records-rate", "stream-metrics").metricValue());
        assertThat(appender.getMessages(), hasItem("Skipping record due to null key or value. key=[3] value=[null] topic=[topic] partition=[-1] offset=[-1]"));
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

        processData();

        final KeyValueStore<String, String> aggregate = (KeyValueStore<String, String>) driver.allStateStores().get("aggregate");

        assertThat(aggregate.get("1"), equalTo("0+A+C+D"));
        assertThat(aggregate.get("2"), equalTo("0+B"));
        assertThat(aggregate.get("3"), equalTo("0+E+F"));
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

        processData();

        assertThat(results.get("1"), equalTo("0+A+C+D"));
        assertThat(results.get("2"), equalTo("0+B"));
        assertThat(results.get("3"), equalTo("0+E+F"));
    }

    private void processData() {
        driver.setUp(builder, TestUtils.tempDirectory(), Serdes.String(), Serdes.String(), 0);
        driver.setTime(0);
        driver.process(TOPIC, "1", "A");
        driver.process(TOPIC, "2", "B");
        driver.process(TOPIC, "1", "C");
        driver.process(TOPIC, "1", "D");
        driver.process(TOPIC, "3", "E");
        driver.process(TOPIC, "3", "F");
        driver.process(TOPIC, "3", null);
        driver.flushState();
    }

    private void doCountWindowed(final List<KeyValue<Windowed<String>, Long>> results) {
        driver.setUp(builder, TestUtils.tempDirectory(), 0);
        driver.setTime(0);
        driver.process(TOPIC, "1", "A");
        driver.process(TOPIC, "2", "B");
        driver.process(TOPIC, "3", "C");
        driver.setTime(500);
        driver.process(TOPIC, "1", "A");
        driver.process(TOPIC, "1", "A");
        driver.process(TOPIC, "2", "B");
        driver.process(TOPIC, "2", "B");
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

    @SuppressWarnings("deprecation")
    @Test
    public void shouldCountWindowed() {
        final List<KeyValue<Windowed<String>, Long>> results = new ArrayList<>();
        groupedStream.count(
            TimeWindows.of(500L),
            "aggregate-by-key-windowed")
            .toStream()
            .foreach(new ForeachAction<Windowed<String>, Long>() {
                @Override
                public void apply(final Windowed<String> key, final Long value) {
                    results.add(KeyValue.pair(key, value));
                }
            });

        doCountWindowed(results);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldCountWindowedWithInternalStoreName() {
        final List<KeyValue<Windowed<String>, Long>> results = new ArrayList<>();
        groupedStream.count(
            TimeWindows.of(500L))
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