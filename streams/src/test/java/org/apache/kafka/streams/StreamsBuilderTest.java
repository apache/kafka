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
package org.apache.kafka.streams;

import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.internals.KStreamImpl;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.MockMapper;
import org.apache.kafka.test.MockPredicate;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class StreamsBuilderTest {

    private final StreamsBuilder builder = new StreamsBuilder();
    private TopologyTestDriver driver;
    private final Properties props = new Properties();

    @Before
    public void setup() {
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-builder-test");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    }

    @After
    public void cleanup() {
        props.clear();
        if (driver != null) {
            driver.close();
        }
        driver = null;
    }

    @Test(expected = TopologyException.class)
    public void testFrom() {
        builder.stream(Arrays.asList("topic-1", "topic-2"));

        builder.build().addSource(KStreamImpl.SOURCE_NAME + "0000000000", "topic-3");
    }

    @Test
    public void shouldAllowJoinUnmaterializedFilteredKTable() {
        final KTable<Bytes, String> filteredKTable = builder.<Bytes, String>table("table-topic").filter(MockPredicate.<Bytes, String>allGoodPredicate());
        builder.<Bytes, String>stream("stream-topic").join(filteredKTable, MockValueJoiner.TOSTRING_JOINER);

        final ProcessorTopology topology = builder.internalTopologyBuilder.build();

        assertThat(topology.stateStores().size(), equalTo(1));
        assertThat(topology.processorConnectedStateStores("KSTREAM-JOIN-0000000005"), equalTo(Collections.singleton(topology.stateStores().get(0).name())));
        assertThat(topology.processorConnectedStateStores("KTABLE-FILTER-0000000003").isEmpty(), is(true));
    }

    @Test
    public void shouldAllowJoinMaterializedFilteredKTable() {
        final KTable<Bytes, String> filteredKTable = builder.<Bytes, String>table("table-topic")
                .filter(MockPredicate.<Bytes, String>allGoodPredicate(), Materialized.<Bytes, String, KeyValueStore<Bytes, byte[]>>as("store"));
        builder.<Bytes, String>stream("stream-topic").join(filteredKTable, MockValueJoiner.TOSTRING_JOINER);

        final ProcessorTopology topology = builder.internalTopologyBuilder.build();

        assertThat(topology.stateStores().size(), equalTo(2));
        assertThat(topology.processorConnectedStateStores("KSTREAM-JOIN-0000000005"), equalTo(Collections.singleton("store")));
        assertThat(topology.processorConnectedStateStores("KTABLE-FILTER-0000000003"), equalTo(Collections.singleton("store")));
    }

    @Test
    public void shouldAllowJoinUnmaterializedMapValuedKTable() {
        final KTable<Bytes, String> mappedKTable = builder.<Bytes, String>table("table-topic").mapValues(MockMapper.<String>noOpValueMapper());
        builder.<Bytes, String>stream("stream-topic").join(mappedKTable, MockValueJoiner.TOSTRING_JOINER);

        final ProcessorTopology topology = builder.internalTopologyBuilder.build();

        assertThat(topology.stateStores().size(), equalTo(1));
        assertThat(topology.processorConnectedStateStores("KSTREAM-JOIN-0000000005"), equalTo(Collections.singleton(topology.stateStores().get(0).name())));
        assertThat(topology.processorConnectedStateStores("KTABLE-MAPVALUES-0000000003").isEmpty(), is(true));
    }

    @Test
    public void shouldAllowJoinMaterializedMapValuedKTable() {
        final KTable<Bytes, String> mappedKTable = builder.<Bytes, String>table("table-topic")
                .mapValues(MockMapper.<String>noOpValueMapper(), Materialized.<Bytes, String, KeyValueStore<Bytes, byte[]>>as("store"));
        builder.<Bytes, String>stream("stream-topic").join(mappedKTable, MockValueJoiner.TOSTRING_JOINER);

        final ProcessorTopology topology = builder.internalTopologyBuilder.build();

        assertThat(topology.stateStores().size(), equalTo(2));
        assertThat(topology.processorConnectedStateStores("KSTREAM-JOIN-0000000005"), equalTo(Collections.singleton("store")));
        assertThat(topology.processorConnectedStateStores("KTABLE-MAPVALUES-0000000003"), equalTo(Collections.singleton("store")));
    }

    @Test
    public void shouldAllowJoinUnmaterializedJoinedKTable() {
        final KTable<Bytes, String> table1 = builder.table("table-topic1");
        final KTable<Bytes, String> table2 = builder.table("table-topic2");
        builder.<Bytes, String>stream("stream-topic").join(table1.join(table2, MockValueJoiner.TOSTRING_JOINER), MockValueJoiner.TOSTRING_JOINER);

        final ProcessorTopology topology = builder.internalTopologyBuilder.build();

        assertThat(topology.stateStores().size(), equalTo(2));
        assertThat(topology.processorConnectedStateStores("KSTREAM-JOIN-0000000010"), equalTo(Utils.mkSet(topology.stateStores().get(0).name(), topology.stateStores().get(1).name())));
        assertThat(topology.processorConnectedStateStores("KTABLE-MERGE-0000000007").isEmpty(), is(true));
    }

    @Test
    public void shouldAllowJoinMaterializedJoinedKTable() {
        final KTable<Bytes, String> table1 = builder.table("table-topic1");
        final KTable<Bytes, String> table2 = builder.table("table-topic2");
        builder.<Bytes, String>stream("stream-topic").join(table1.join(table2, MockValueJoiner.TOSTRING_JOINER, Materialized.<Bytes, String, KeyValueStore<Bytes, byte[]>>as("store")), MockValueJoiner.TOSTRING_JOINER);

        final ProcessorTopology topology = builder.internalTopologyBuilder.build();

        assertThat(topology.stateStores().size(), equalTo(3));
        assertThat(topology.processorConnectedStateStores("KSTREAM-JOIN-0000000010"), equalTo(Collections.singleton("store")));
        assertThat(topology.processorConnectedStateStores("KTABLE-MERGE-0000000007"), equalTo(Collections.singleton("store")));
    }

    @Test
    public void shouldAllowJoinMaterializedSourceKTable() {
        final KTable<Bytes, String> table = builder.table("table-topic");
        builder.<Bytes, String>stream("stream-topic").join(table, MockValueJoiner.TOSTRING_JOINER);

        final ProcessorTopology topology = builder.internalTopologyBuilder.build();

        assertThat(topology.stateStores().size(), equalTo(1));
        assertThat(topology.processorConnectedStateStores("KTABLE-SOURCE-0000000002"), equalTo(Collections.singleton(topology.stateStores().get(0).name())));
        assertThat(topology.processorConnectedStateStores("KSTREAM-JOIN-0000000004"), equalTo(Collections.singleton(topology.stateStores().get(0).name())));
    }

    @Test
    public void shouldProcessingFromSinkTopic() {
        final KStream<String, String> source = builder.stream("topic-source");
        source.to("topic-sink");

        final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();

        source.process(processorSupplier);

        driver = new TopologyTestDriver(builder.build(), props);

        final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());
        driver.pipeInput(recordFactory.create("topic-source", "A", "aa"));

        // no exception was thrown
        assertEquals(Utils.mkList("A:aa"), processorSupplier.theCapturedProcessor().processed);
    }

    @Test
    public void shouldProcessViaThroughTopic() {
        final KStream<String, String> source = builder.stream("topic-source");
        final KStream<String, String> through = source.through("topic-sink");

        final MockProcessorSupplier<String, String> sourceProcessorSupplier = new MockProcessorSupplier<>();
        final MockProcessorSupplier<String, String> throughProcessorSupplier = new MockProcessorSupplier<>();

        source.process(sourceProcessorSupplier);
        through.process(throughProcessorSupplier);

        driver = new TopologyTestDriver(builder.build(), props);

        final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());
        driver.pipeInput(recordFactory.create("topic-source", "A", "aa"));

        assertEquals(Utils.mkList("A:aa"), sourceProcessorSupplier.theCapturedProcessor().processed);
        assertEquals(Utils.mkList("A:aa"), throughProcessorSupplier.theCapturedProcessor().processed);
    }
    
    @Test
    public void testMerge() {
        final String topic1 = "topic-1";
        final String topic2 = "topic-2";

        final KStream<String, String> source1 = builder.stream(topic1);
        final KStream<String, String> source2 = builder.stream(topic2);
        final KStream<String, String> merged = source1.merge(source2);

        final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();
        merged.process(processorSupplier);

        driver = new TopologyTestDriver(builder.build(), props);

        final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());
        driver.pipeInput(recordFactory.create(topic1, "A", "aa"));
        driver.pipeInput(recordFactory.create(topic2, "B", "bb"));
        driver.pipeInput(recordFactory.create(topic2, "C", "cc"));
        driver.pipeInput(recordFactory.create(topic1, "D", "dd"));

        assertEquals(Utils.mkList("A:aa", "B:bb", "C:cc", "D:dd"), processorSupplier.theCapturedProcessor().processed);
    }

    @Test
    public void shouldUseSerdesDefinedInMaterializedToConsumeTable() {
        final Map<Long, String> results = new HashMap<>();
        final String topic = "topic";
        final ForeachAction<Long, String> action = new ForeachAction<Long, String>() {
            @Override
            public void apply(final Long key, final String value) {
                results.put(key, value);
            }
        };
        builder.table(topic, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("store")
                .withKeySerde(Serdes.Long())
                .withValueSerde(Serdes.String()))
                .toStream().foreach(action);

        driver = new TopologyTestDriver(builder.build(), props);

        final ConsumerRecordFactory<Long, String> recordFactory = new ConsumerRecordFactory<>(new LongSerializer(), new StringSerializer());
        driver.pipeInput(recordFactory.create(topic, 1L, "value1"));
        driver.pipeInput(recordFactory.create(topic, 2L, "value2"));

        final KeyValueStore<Long, String> store = driver.getKeyValueStore("store");
        assertThat(store.get(1L), equalTo("value1"));
        assertThat(store.get(2L), equalTo("value2"));
        assertThat(results.get(1L), equalTo("value1"));
        assertThat(results.get(2L), equalTo("value2"));
    }

    @Test
    public void shouldUseSerdesDefinedInMaterializedToConsumeGlobalTable() {
        final String topic = "topic";
        builder.globalTable(topic, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("store")
                .withKeySerde(Serdes.Long())
                .withValueSerde(Serdes.String()));

        driver = new TopologyTestDriver(builder.build(), props);

        final ConsumerRecordFactory<Long, String> recordFactory = new ConsumerRecordFactory<>(new LongSerializer(), new StringSerializer());
        driver.pipeInput(recordFactory.create(topic, 1L, "value1"));
        driver.pipeInput(recordFactory.create(topic, 2L, "value2"));
        final KeyValueStore<Long, String> store = driver.getKeyValueStore("store");

        assertThat(store.get(1L), equalTo("value1"));
        assertThat(store.get(2L), equalTo("value2"));
    }

    @Test
    public void shouldUseDefaultNodeAndStoreNames() {
        final String topic = "topic";
        builder.table(topic,
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>with(Serdes.Long(), Serdes.String()));

        final Iterator<TopologyDescription.Subtopology> subtopologies = builder.build().describe().subtopologies().iterator();
        final TopologyDescription.Subtopology subtopology = subtopologies.next();

        final Iterator<TopologyDescription.Node> nodes = subtopology.nodes().iterator();
        TopologyDescription.Node node = nodes.next();
        assertThat(node.name(), equalTo("KSTREAM-SOURCE-0000000001"));
        node = nodes.next();
        assertThat(node.name(), equalTo("KTABLE-SOURCE-0000000002"));
        final Iterator<String> stores = ((TopologyDescription.Processor) node).stores().iterator();
        assertThat(stores.next(), equalTo(topic + "-STATE-STORE-0000000000"));

        assertFalse(nodes.hasNext());
        assertFalse(stores.hasNext());
        assertFalse(subtopologies.hasNext());
    }
    
    @Test(expected = TopologyException.class)
    public void shouldThrowExceptionWhenNoTopicPresent() {
        builder.stream(Collections.<String>emptyList());
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenTopicNamesAreNull() {
        builder.stream(Arrays.<String>asList(null, null));
    }

    // TODO: these two static functions are added because some non-TopologyBuilder unit tests need to access the internal topology builder,
    //       which is usually a bad sign of design patterns between TopologyBuilder and StreamThread. We need to consider getting rid of them later
    public static InternalTopologyBuilder internalTopologyBuilder(final StreamsBuilder builder) {
        return builder.internalTopologyBuilder;
    }

    public static Collection<Set<String>> getCopartitionedGroups(final StreamsBuilder builder) {
        return builder.internalTopologyBuilder.copartitionGroups();
    }
}