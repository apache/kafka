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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KCogroupedStream;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreSupplier;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("unchecked")
public class KCogroupedStreamImplTest {

    private final Consumed<String, String> stringConsumed = Consumed
        .with(Serdes.String(), Serdes.String());
    private final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();
    private static final String TOPIC = "topic";
    private final StreamsBuilder builder = new StreamsBuilder();
    private KGroupedStream<String, String> groupedStream;
    private KCogroupedStream<String, String, String> cogroupedStream;

    private final ConsumerRecordFactory<String, String> recordFactory =
        new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());
    private final Properties props = StreamsTestUtils
        .getStreamsConfig(Serdes.String(), Serdes.String());

    @Before
    public void setup() {
        final KStream<String, String> stream = builder.stream(TOPIC, Consumed
            .with(Serdes.String(), Serdes.String()));

        groupedStream = stream.groupByKey(Grouped.with(Serdes.String(), Serdes.String()));
        cogroupedStream = groupedStream.cogroup(MockAggregator.TOSTRING_ADDER);
    }

    @Test
    public void cogroupTest() {
        assertNotNull(cogroupedStream);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullKGroupedStreamOnCogroup() throws Exception {
        cogroupedStream.cogroup(null, MockAggregator.TOSTRING_ADDER);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullAggregatorOnCogroup() throws Exception {
        cogroupedStream.cogroup(groupedStream, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullInitializerOnAggregate() throws Exception {
        cogroupedStream.aggregate(null, Materialized.as("store"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullMaterMaterializedOnAggregate() throws Exception {
        cogroupedStream.aggregate(SUM_INITIALIZER, (Materialized<String, String, KeyValueStore<Bytes,byte[]>>) null);
    }
    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullMaterStoreSupplierOnAggregate() throws Exception {
        cogroupedStream.aggregate(SUM_INITIALIZER, (StoreSupplier<KeyValueStore>) null);
    }

    private static final Aggregator SUM_AGGREGATOR = new Aggregator<String, String, String>() {
        @Override
        public String apply(final String key, final String value, final String aggregate) {
            return aggregate + value;
        }
    };

    private static final Initializer SUM_INITIALIZER = new Initializer() {
        @Override
        public Object apply() {
            return "";
        }
    };


    @Test
    public void testCogroupBassicOneTopic() {

        final KStream test = builder.stream("one", stringConsumed);

        final KGroupedStream groupedOne = test.groupByKey();

        final KTable customers = groupedOne.cogroup(SUM_AGGREGATOR, Materialized.as("store"))
            .aggregate(SUM_INITIALIZER, Materialized.as("store1"));

        customers.toStream().to("to-one", Produced.with(Serdes.String(), Serdes.String()));

        builder.stream("to-one", stringConsumed).process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            driver.pipeInput(recordFactory.create("one", "1", "A", 0));
            driver.pipeInput(recordFactory.create("one", "11", "A", 0));
            driver.pipeInput(recordFactory.create("one", "11", "A", 0));
            driver.pipeInput(recordFactory.create("one", "1", "A", 0));
        }

        assertThat(processorSupplier.theCapturedProcessor().processed, equalTo(Arrays.asList(
            new KeyValueTimestamp("1", "A", 0),
            new KeyValueTimestamp("11", "A", 0),
            new KeyValueTimestamp("11", "AA", 0),
            new KeyValueTimestamp("1", "AA", 0)
        )));
    }


    @Test
    public void testCogroupEachTopicUnique() {

        final KStream test = builder.stream("one", stringConsumed);
        final KStream test2 = builder.stream("two", stringConsumed);

        final KGroupedStream groupedOne = test.groupByKey();
        final KGroupedStream groupedTwo = test2.groupByKey();

        final KTable customers = groupedOne.cogroup(SUM_AGGREGATOR, Materialized.as("store"))
            .cogroup(groupedTwo, SUM_AGGREGATOR)
            .aggregate(SUM_INITIALIZER, Materialized.as("store1"));

        customers.toStream().to("to-one", Produced.with(Serdes.String(), Serdes.String()));

        builder.stream("to-one", stringConsumed).process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            driver.pipeInput(recordFactory.create("one", "1", "A", 0));
            driver.pipeInput(recordFactory.create("one", "1", "A", 1));
            driver.pipeInput(recordFactory.create("one", "1", "A", 10));
            driver.pipeInput(recordFactory.create("one", "1", "A", 100));
            driver.pipeInput(recordFactory.create("two", "2", "B", 100L));
            driver.pipeInput(recordFactory.create("two", "2", "B", 200L));
            driver.pipeInput(recordFactory.create("two", "2", "B", 1L));
            driver.pipeInput(recordFactory.create("two", "2", "B", 500L));
            driver.pipeInput(recordFactory.create("two", "2", "B", 500L));
            driver.pipeInput(recordFactory.create("two", "2", "B", 100L));
        }

        assertThat(processorSupplier.theCapturedProcessor().processed, equalTo(Arrays.asList(
            new KeyValueTimestamp("1", "A", 0),
            new KeyValueTimestamp("1", "AA", 1),
            new KeyValueTimestamp("1", "AAA", 10),
            new KeyValueTimestamp("1", "AAAA", 100),
            new KeyValueTimestamp("2", "B", 100),
            new KeyValueTimestamp("2", "BB", 200),
            new KeyValueTimestamp("2", "BBB", 200),
            new KeyValueTimestamp("2", "BBBB", 500),
            new KeyValueTimestamp("2", "BBBBB", 500),
            new KeyValueTimestamp("2", "BBBBBB", 500)
        )));
    }


    @Test
    public void testCogroupKeyMixedInTopics() {

        final KStream test = builder.stream("one", stringConsumed);
        final KStream test2 = builder.stream("two", stringConsumed);

        final KGroupedStream groupedOne = test.groupByKey();
        final KGroupedStream groupedTwo = test2.groupByKey();

        final KTable customers = groupedOne.cogroup(SUM_AGGREGATOR, Materialized.as("store"))
            .cogroup(groupedTwo, SUM_AGGREGATOR)
            .aggregate(SUM_INITIALIZER, Materialized.as("store1"));

        customers.toStream().to("to-one", Produced.with(Serdes.String(), Serdes.String()));

        builder.stream("to-one", stringConsumed).process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            driver.pipeInput(recordFactory.create("one", "1", "A", 0L));
            driver.pipeInput(recordFactory.create("one", "2", "A", 1L));
            driver.pipeInput(recordFactory.create("one", "1", "A", 10L));
            driver.pipeInput(recordFactory.create("one", "2", "A", 100L));
            driver.pipeInput(recordFactory.create("two", "2", "B", 100L));
            driver.pipeInput(recordFactory.create("two", "2", "B", 200L));
            driver.pipeInput(recordFactory.create("two", "1", "B", 1L));
            driver.pipeInput(recordFactory.create("two", "2", "B", 500L));
            driver.pipeInput(recordFactory.create("two", "1", "B", 500L));
            driver.pipeInput(recordFactory.create("two", "2", "B", 500L));
            driver.pipeInput(recordFactory.create("two", "3", "B", 500L));
            driver.pipeInput(recordFactory.create("two", "2", "B", 100L));
        }

        assertThat(processorSupplier.theCapturedProcessor().processed, equalTo(Arrays.asList(
            new KeyValueTimestamp("1", "A", 0),
            new KeyValueTimestamp("2", "A", 1),
            new KeyValueTimestamp("1", "AA", 10),
            new KeyValueTimestamp("2", "AA", 100),
            new KeyValueTimestamp("2", "AAB", 100),
            new KeyValueTimestamp("2", "AABB", 200),
            new KeyValueTimestamp("1", "AAB", 10),
            new KeyValueTimestamp("2", "AABBB", 500),
            new KeyValueTimestamp("1", "AABB", 500),
            new KeyValueTimestamp("2", "AABBBB", 500),
            new KeyValueTimestamp("3", "B", 500),
            new KeyValueTimestamp("2", "AABBBBB", 500)
        )));
    }
}
