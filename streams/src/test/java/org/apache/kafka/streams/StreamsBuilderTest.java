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
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;
import org.apache.kafka.streams.state.BuiltInDslStoreSuppliers;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;
import org.apache.kafka.streams.state.internals.InMemorySessionStore;
import org.apache.kafka.streams.state.internals.InMemoryWindowStore;
import org.apache.kafka.streams.state.internals.RocksDBStore;
import org.apache.kafka.streams.state.internals.RocksDBWindowStore;
import org.apache.kafka.streams.state.internals.WrappedStateStore;
import org.apache.kafka.streams.utils.TestUtils.RecordingProcessorWrapper;
import org.apache.kafka.streams.utils.TestUtils.RecordingProcessorWrapper.WrapperRecorder;
import org.apache.kafka.test.MockApiFixedKeyProcessorSupplier;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.MockMapper;
import org.apache.kafka.test.MockPredicate;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.StreamsTestUtils;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;

import static java.util.Arrays.asList;
import static org.apache.kafka.streams.StreamsConfig.PROCESSOR_WRAPPER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.SUBTOPOLOGY_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.SUBTOPOLOGY_1;
import static org.apache.kafka.streams.state.Stores.inMemoryKeyValueStore;
import static org.apache.kafka.streams.state.Stores.timestampedKeyValueStoreBuilder;
import static org.apache.kafka.streams.utils.TestUtils.PROCESSOR_WRAPPER_COUNTER_CONFIG;
import static org.apache.kafka.streams.utils.TestUtils.dummyStreamsConfigMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Timeout(600)
public class StreamsBuilderTest {

    private static final String STREAM_TOPIC = "stream-topic";

    private static final String STREAM_OPERATION_NAME = "stream-operation";

    private static final String STREAM_TOPIC_TWO = "stream-topic-two";

    private static final String TABLE_TOPIC = "table-topic";

    private final StreamsBuilder builder = new StreamsBuilder();

    private Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());

    @BeforeEach
    public void before() {
        props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());
    }

    @Test
    public void shouldAddGlobalStore() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.addGlobalStore(
            Stores.keyValueStoreBuilder(
                inMemoryKeyValueStore("store"),
                Serdes.String(),
                Serdes.String()
            ),
            "topic",
            Consumed.with(Serdes.String(), Serdes.String()),
            () -> new Processor<String, String, Void, Void>() {
                private KeyValueStore<String, String> store;

                @Override
                public void init(final ProcessorContext<Void, Void> context) {
                    store = context.getStateStore("store");
                }

                @Override
                public void process(final Record<String, String> record) {
                    store.put(record.key(), record.value());
                }
            }
        );
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build())) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic("topic", new StringSerializer(), new StringSerializer());
            inputTopic.pipeInput("hey", "there");
            final KeyValueStore<String, String> store = driver.getKeyValueStore("store");
            final String hey = store.get("hey");
            assertThat(hey, is("there"));
        }
    }

    @Test
    public void shouldNotThrowNullPointerIfOptimizationsNotSpecified() {
        final Properties properties = new Properties();

        final StreamsBuilder builder = new StreamsBuilder();
        builder.build(properties);
    }

    @Test
    public void shouldAllowJoinUnmaterializedFilteredKTable() {
        final KTable<Bytes, String> filteredKTable = builder
            .<Bytes, String>table(TABLE_TOPIC)
            .filter(MockPredicate.allGoodPredicate());
        builder
            .<Bytes, String>stream(STREAM_TOPIC)
            .join(filteredKTable, MockValueJoiner.TOSTRING_JOINER);
        builder.build();

        final ProcessorTopology topology =
            builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();

        assertThat(
            topology.stateStores().size(),
            equalTo(1));
        assertThat(
            topology.processorConnectedStateStores("KSTREAM-JOIN-0000000005"),
            equalTo(Collections.singleton(topology.stateStores().get(0).name())));
        assertTrue(
            topology.processorConnectedStateStores("KTABLE-FILTER-0000000003").isEmpty());
    }

    @Test
    public void shouldAllowJoinMaterializedFilteredKTable() {
        final KTable<Bytes, String> filteredKTable = builder
            .<Bytes, String>table(TABLE_TOPIC)
            .filter(MockPredicate.allGoodPredicate(), Materialized.as("store"));
        builder
            .<Bytes, String>stream(STREAM_TOPIC)
            .join(filteredKTable, MockValueJoiner.TOSTRING_JOINER);
        builder.build();

        final ProcessorTopology topology =
            builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();

        assertThat(
            topology.stateStores().size(),
            equalTo(1));
        assertThat(
            topology.processorConnectedStateStores("KSTREAM-JOIN-0000000005"),
            equalTo(Collections.singleton("store")));
        assertThat(
            topology.processorConnectedStateStores("KTABLE-FILTER-0000000003"),
            equalTo(Collections.singleton("store")));
    }

    @Test
    public void shouldAllowJoinUnmaterializedMapValuedKTable() {
        final KTable<Bytes, String> mappedKTable = builder
            .<Bytes, String>table(TABLE_TOPIC)
            .mapValues(MockMapper.noOpValueMapper());
        builder
            .<Bytes, String>stream(STREAM_TOPIC)
            .join(mappedKTable, MockValueJoiner.TOSTRING_JOINER);
        builder.build();

        final ProcessorTopology topology =
            builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();

        assertThat(
            topology.stateStores().size(),
            equalTo(1));
        assertThat(
            topology.processorConnectedStateStores("KSTREAM-JOIN-0000000005"),
            equalTo(Collections.singleton(topology.stateStores().get(0).name())));
        assertTrue(
            topology.processorConnectedStateStores("KTABLE-MAPVALUES-0000000003").isEmpty());
    }

    @Test
    public void shouldAllowJoinMaterializedMapValuedKTable() {
        final KTable<Bytes, String> mappedKTable = builder
            .<Bytes, String>table(TABLE_TOPIC)
            .mapValues(MockMapper.noOpValueMapper(), Materialized.as("store"));
        builder
            .<Bytes, String>stream(STREAM_TOPIC)
            .join(mappedKTable, MockValueJoiner.TOSTRING_JOINER);
        builder.build();

        final ProcessorTopology topology =
            builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();

        assertThat(
            topology.stateStores().size(),
            equalTo(1));
        assertThat(
            topology.processorConnectedStateStores("KSTREAM-JOIN-0000000005"),
            equalTo(Collections.singleton("store")));
        assertThat(
            topology.processorConnectedStateStores("KTABLE-MAPVALUES-0000000003"),
            equalTo(Collections.singleton("store")));
    }

    @Test
    public void shouldAllowJoinUnmaterializedJoinedKTable() {
        final KTable<Bytes, String> table1 = builder.table("table-topic1");
        final KTable<Bytes, String> table2 = builder.table("table-topic2");
        builder
            .<Bytes, String>stream(STREAM_TOPIC)
            .join(table1.join(table2, MockValueJoiner.TOSTRING_JOINER), MockValueJoiner.TOSTRING_JOINER);
        builder.build();

        final ProcessorTopology topology =
            builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();

        assertThat(
            topology.stateStores().size(),
            equalTo(2));
        assertThat(
            topology.processorConnectedStateStores("KSTREAM-JOIN-0000000010"),
            equalTo(Set.of(topology.stateStores().get(0).name(), topology.stateStores().get(1).name())));
        assertTrue(
            topology.processorConnectedStateStores("KTABLE-MERGE-0000000007").isEmpty());
    }

    @Test
    public void shouldAllowJoinMaterializedJoinedKTable() {
        final KTable<Bytes, String> table1 = builder.table("table-topic1");
        final KTable<Bytes, String> table2 = builder.table("table-topic2");
        builder
            .<Bytes, String>stream(STREAM_TOPIC)
            .join(
                table1.join(table2, MockValueJoiner.TOSTRING_JOINER, Materialized.as("store")),
                MockValueJoiner.TOSTRING_JOINER);
        builder.build();

        final ProcessorTopology topology =
            builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();

        assertThat(
            topology.stateStores().size(),
            equalTo(3));
        assertThat(
            topology.processorConnectedStateStores("KSTREAM-JOIN-0000000010"),
            equalTo(Collections.singleton("store")));
        assertThat(
            topology.processorConnectedStateStores("KTABLE-MERGE-0000000007"),
            equalTo(Collections.singleton("store")));
    }

    @Test
    public void shouldAllowJoinMaterializedSourceKTable() {
        final KTable<Bytes, String> table = builder.table(TABLE_TOPIC);
        builder.<Bytes, String>stream(STREAM_TOPIC).join(table, MockValueJoiner.TOSTRING_JOINER);
        builder.build();

        final ProcessorTopology topology =
            builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();

        assertThat(
            topology.stateStores().size(),
            equalTo(1));
        assertThat(
            topology.processorConnectedStateStores("KTABLE-SOURCE-0000000002"),
            equalTo(Collections.singleton(topology.stateStores().get(0).name())));
        assertThat(
            topology.processorConnectedStateStores("KSTREAM-JOIN-0000000004"),
            equalTo(Collections.singleton(topology.stateStores().get(0).name())));
    }

    @Test
    public void shouldProcessingFromSinkTopic() {
        final KStream<String, String> source = builder.stream("topic-source");
        source.to("topic-sink");

        final MockApiProcessorSupplier<String, String, Void, Void> processorSupplier = new MockApiProcessorSupplier<>();
        source.process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic("topic-source", new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            inputTopic.pipeInput("A", "aa");
        }

        // no exception was thrown
        assertEquals(Collections.singletonList(new KeyValueTimestamp<>("A", "aa", 0)),
                     processorSupplier.theCapturedProcessor().processed());
    }

    @Test
    public void shouldProcessViaRepartitionTopic() {
        final KStream<String, String> source = builder.stream("topic-source");
        final KStream<String, String> through = source.repartition();

        final MockApiProcessorSupplier<String, String, Void, Void> sourceProcessorSupplier = new MockApiProcessorSupplier<>();
        source.process(sourceProcessorSupplier);

        final MockApiProcessorSupplier<String, String, Void, Void> throughProcessorSupplier = new MockApiProcessorSupplier<>();
        through.process(throughProcessorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic("topic-source", new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            inputTopic.pipeInput("A", "aa");
        }

        assertEquals(Collections.singletonList(new KeyValueTimestamp<>("A", "aa", 0)), sourceProcessorSupplier.theCapturedProcessor().processed());
        assertEquals(Collections.singletonList(new KeyValueTimestamp<>("A", "aa", 0)), throughProcessorSupplier.theCapturedProcessor().processed());
    }

    @Test
    public void shouldMergeStreams() {
        final String topic1 = "topic-1";
        final String topic2 = "topic-2";

        final KStream<String, String> source1 = builder.stream(topic1);
        final KStream<String, String> source2 = builder.stream(topic2);
        final KStream<String, String> merged = source1.merge(source2);

        final MockApiProcessorSupplier<String, String, Void, Void> processorSupplier = new MockApiProcessorSupplier<>();
        merged.process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic1 =
                driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<String, String> inputTopic2 =
                driver.createInputTopic(topic2, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);

            inputTopic1.pipeInput("A", "aa");
            inputTopic2.pipeInput("B", "bb");
            inputTopic2.pipeInput("C", "cc");
            inputTopic1.pipeInput("D", "dd");
        }

        assertEquals(asList(new KeyValueTimestamp<>("A", "aa", 0),
                            new KeyValueTimestamp<>("B", "bb", 0),
                            new KeyValueTimestamp<>("C", "cc", 0),
                            new KeyValueTimestamp<>("D", "dd", 0)), processorSupplier.theCapturedProcessor().processed());
    }

    @Test
    public void shouldUseDslStoreSupplierDefinedInMaterialized() {
        final String topic = "topic";
        builder.stream(topic)
                .groupByKey()
                .count(Materialized.<Object, Long, KeyValueStore<Bytes, byte[]>>as("store")
                        .withStoreType(BuiltInDslStoreSuppliers.IN_MEMORY))
                .toStream();

        builder.build();
        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertTypesForStateStore(topology.stateStores(), InMemoryKeyValueStore.class);
    }

    @Test
    public void shouldUseDslStoreSupplierDefinedInMaterializedOverTopologyOverrides() {
        final String topic = "topic";

        final Properties topoOverrides = new Properties();
        topoOverrides.putAll(props);
        topoOverrides.put(StreamsConfig.DSL_STORE_SUPPLIERS_CLASS_CONFIG, BuiltInDslStoreSuppliers.RocksDBDslStoreSuppliers.class);
        final StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(topoOverrides)));

        builder.stream(topic)
                .groupByKey()
                .count(Materialized.<Object, Long, KeyValueStore<Bytes, byte[]>>as("store")
                        .withStoreType(BuiltInDslStoreSuppliers.IN_MEMORY))
                .toStream();

        builder.build();
        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertTypesForStateStore(topology.stateStores(), InMemoryKeyValueStore.class);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldUseDslStoreSupplierOverStoreType() {
        final String topic = "topic";
        final Properties topoOverrides = new Properties();
        topoOverrides.putAll(props);
        topoOverrides.put(StreamsConfig.DEFAULT_DSL_STORE_CONFIG, StreamsConfig.ROCKS_DB);
        topoOverrides.put(StreamsConfig.DSL_STORE_SUPPLIERS_CLASS_CONFIG, BuiltInDslStoreSuppliers.InMemoryDslStoreSuppliers.class);
        final StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(topoOverrides)));

        builder.stream(topic)
                .groupByKey()
                .count(Materialized.<Object, Long, KeyValueStore<Bytes, byte[]>>as("store"))
                .toStream();

        builder.build();

        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertTypesForStateStore(topology.stateStores(), InMemoryKeyValueStore.class);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldUseTopologyOverrideStoreTypeOverConfiguredDslStoreSupplier() {
        final String topic = "topic";
        final Properties topoOverrides = new Properties();
        topoOverrides.putAll(props);
        topoOverrides.put(StreamsConfig.DEFAULT_DSL_STORE_CONFIG, StreamsConfig.IN_MEMORY);
        final StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(topoOverrides)));

        builder.stream(topic)
                .groupByKey()
                .count(Materialized.<Object, Long, KeyValueStore<Bytes, byte[]>>as("store"))
                .toStream();

        builder.build();

        props.put(StreamsConfig.DSL_STORE_SUPPLIERS_CLASS_CONFIG, BuiltInDslStoreSuppliers.RocksDBDslStoreSuppliers.class);
        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertTypesForStateStore(topology.stateStores(), InMemoryKeyValueStore.class);
    }

    @Test
    public void shouldUseDslStoreSupplierDefinedConfiguredInStreamsConfig() {
        final String topic = "topic";
        builder.stream(topic)
                .groupByKey()
                .count()
                .toStream();

        builder.build();
        props.put(StreamsConfig.DSL_STORE_SUPPLIERS_CLASS_CONFIG, BuiltInDslStoreSuppliers.InMemoryDslStoreSuppliers.class);
        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertTypesForStateStore(topology.stateStores(), InMemoryKeyValueStore.class);
    }

    @Test
    public void shouldUseDslStoreSupplierDefinedConfiguredInTopologyConfigOverStreamsConfig() {
        final String topic = "topic";

        final Properties topoOverrides = new Properties();
        topoOverrides.putAll(props);
        topoOverrides.put(StreamsConfig.DSL_STORE_SUPPLIERS_CLASS_CONFIG, BuiltInDslStoreSuppliers.InMemoryDslStoreSuppliers.class);
        final StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(topoOverrides)));

        builder.stream(topic)
                .groupByKey()
                .count()
                .toStream();

        builder.build();
        props.put(StreamsConfig.DSL_STORE_SUPPLIERS_CLASS_CONFIG, BuiltInDslStoreSuppliers.RocksDBDslStoreSuppliers.class);
        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertTypesForStateStore(topology.stateStores(), InMemoryKeyValueStore.class);
    }

    @Test
    public void shouldUseDslStoreSupplierDefinedInMaterializedForWindowedOperation() {
        final String topic = "topic";
        builder.stream(topic)
                .groupByKey()
                .windowedBy(JoinWindows.ofTimeDifferenceAndGrace(Duration.ofHours(1), Duration.ZERO))
                .count(Materialized.<Object, Long, WindowStore<Bytes, byte[]>>as("store")
                        .withStoreType(BuiltInDslStoreSuppliers.IN_MEMORY))
                .toStream();

        builder.build();
        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertTypesForStateStore(topology.stateStores(), InMemoryWindowStore.class);
    }

    @Test
    public void shouldUseDslStoreSupplierDefinedConfiguredInStreamsConfigForWindowedOperation() {
        final String topic = "topic";
        builder.stream(topic)
                .groupByKey()
                .windowedBy(JoinWindows.ofTimeDifferenceAndGrace(Duration.ofHours(1), Duration.ZERO))
                .count()
                .toStream();

        builder.build();
        props.put(StreamsConfig.DSL_STORE_SUPPLIERS_CLASS_CONFIG, BuiltInDslStoreSuppliers.InMemoryDslStoreSuppliers.class);
        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertTypesForStateStore(topology.stateStores(), InMemoryWindowStore.class);
    }

    @Test
    public void shouldUseDslStoreSupplierDefinedConfiguredInTopologyConfigOverStreamsConfigForWindowedOperation() {
        final String topic = "topic";

        final Properties topoOverrides = new Properties();
        topoOverrides.putAll(props);
        topoOverrides.put(StreamsConfig.DSL_STORE_SUPPLIERS_CLASS_CONFIG, BuiltInDslStoreSuppliers.InMemoryDslStoreSuppliers.class);
        final StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(topoOverrides)));

        builder.stream(topic)
                .groupByKey()
                .windowedBy(JoinWindows.ofTimeDifferenceAndGrace(Duration.ofHours(1), Duration.ZERO))
                .count()
                .toStream();

        builder.build();
        props.put(StreamsConfig.DSL_STORE_SUPPLIERS_CLASS_CONFIG, BuiltInDslStoreSuppliers.RocksDBDslStoreSuppliers.class);
        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertTypesForStateStore(topology.stateStores(), InMemoryWindowStore.class);
    }

    @Test
    public void shouldUseDslStoreSupplierDefinedInMaterializedForSessionWindowedOperation() {
        final String topic = "topic";
        builder.stream(topic)
                .groupByKey()
                .windowedBy(SessionWindows.ofInactivityGapAndGrace(Duration.ofHours(1), Duration.ZERO))
                .count(Materialized.<Object, Long, SessionStore<Bytes, byte[]>>as("store")
                        .withStoreType(BuiltInDslStoreSuppliers.IN_MEMORY))
                .toStream();

        builder.build();
        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertTypesForStateStore(topology.stateStores(), InMemorySessionStore.class);
    }

    @Test
    public void shouldUseDslStoreSupplierDefinedConfiguredInStreamsConfigForSessionWindowedOperation() {
        final String topic = "topic";
        builder.stream(topic)
                .groupByKey()
                .windowedBy(SessionWindows.ofInactivityGapAndGrace(Duration.ofHours(1), Duration.ZERO))
                .count()
                .toStream();

        builder.build();
        props.put(StreamsConfig.DSL_STORE_SUPPLIERS_CLASS_CONFIG, BuiltInDslStoreSuppliers.InMemoryDslStoreSuppliers.class);
        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertTypesForStateStore(topology.stateStores(), InMemorySessionStore.class);
    }

    @Test
    public void shouldUseDslStoreSupplierDefinedConfiguredInTopologyConfigOverStreamsConfigForSessionWindowedOperation() {
        final String topic = "topic";

        final Properties topoOverrides = new Properties();
        topoOverrides.putAll(props);
        topoOverrides.put(StreamsConfig.DSL_STORE_SUPPLIERS_CLASS_CONFIG, BuiltInDslStoreSuppliers.InMemoryDslStoreSuppliers.class);
        final StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(topoOverrides)));

        builder.stream(topic)
                .groupByKey()
                .windowedBy(SessionWindows.ofInactivityGapAndGrace(Duration.ofHours(1), Duration.ZERO))
                .count()
                .toStream();

        builder.build();
        props.put(StreamsConfig.DSL_STORE_SUPPLIERS_CLASS_CONFIG, BuiltInDslStoreSuppliers.RocksDBDslStoreSuppliers.class);
        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertTypesForStateStore(topology.stateStores(), InMemorySessionStore.class);
    }

    @Test
    public void shouldUseSerdesDefinedInMaterializedToConsumeTable() {
        final Map<Long, String> results = new HashMap<>();
        final String topic = "topic";
        final ForeachAction<Long, String> action = results::put;
        builder.table(topic, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("store")
            .withKeySerde(Serdes.Long())
            .withValueSerde(Serdes.String()))
               .toStream().foreach(action);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<Long, String> inputTopic =
                driver.createInputTopic(topic, new LongSerializer(), new StringSerializer());
            inputTopic.pipeInput(1L, "value1");
            inputTopic.pipeInput(2L, "value2");

            final KeyValueStore<Long, String> store = driver.getKeyValueStore("store");
            assertThat(store.get(1L), equalTo("value1"));
            assertThat(store.get(2L), equalTo("value2"));
            assertThat(results.get(1L), equalTo("value1"));
            assertThat(results.get(2L), equalTo("value2"));
        }
    }

    @Test
    public void shouldUseSerdesDefinedInMaterializedToConsumeGlobalTable() {
        final String topic = "topic";
        builder.globalTable(topic, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("store")
            .withKeySerde(Serdes.Long())
            .withValueSerde(Serdes.String()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<Long, String> inputTopic =
                driver.createInputTopic(topic, new LongSerializer(), new StringSerializer());
            inputTopic.pipeInput(1L, "value1");
            inputTopic.pipeInput(2L, "value2");
            final KeyValueStore<Long, String> store = driver.getKeyValueStore("store");

            assertThat(store.get(1L), equalTo("value1"));
            assertThat(store.get(2L), equalTo("value2"));
        }
    }

    @Test
    public void shouldThrowOnVersionedStoreSupplierForGlobalTable() {
        final String topic = "topic";
        assertThrows(
            TopologyException.class,
            () -> builder.globalTable(
                topic,
                Materialized.<Long, String>as(Stores.persistentVersionedKeyValueStore("store", Duration.ZERO))
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.String()
                )
            )
        );
    }

    @Test
    public void shouldNotMaterializeStoresIfNotRequired() {
        final String topic = "topic";
        builder.table(topic, Materialized.with(Serdes.Long(), Serdes.String()));

        final ProcessorTopology topology =
            builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();

        assertThat(topology.stateStores().size(), equalTo(0));
    }

    @Test
    public void shouldReuseSourceTopicAsChangelogsWithOptimization20() {
        final String topic = "topic";
        builder.table(topic, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("store"));
        final Properties props = StreamsTestUtils.getStreamsConfig();
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        final Topology topology = builder.build(props);

        final InternalTopologyBuilder internalTopologyBuilder = TopologyWrapper.getInternalTopologyBuilder(topology);
        internalTopologyBuilder.rewriteTopology(new StreamsConfig(props));

        assertThat(
            internalTopologyBuilder.buildTopology().storeToChangelogTopic(),
            equalTo(Collections.singletonMap("store", "topic")));
        assertThat(
            internalTopologyBuilder.stateStores().keySet(),
            equalTo(Collections.singleton("store")));
        assertThat(
            internalTopologyBuilder.stateStores().get("store").loggingEnabled(),
            equalTo(false));
        assertThat(
            internalTopologyBuilder.subtopologyToTopicsInfo().get(SUBTOPOLOGY_0).nonSourceChangelogTopics().isEmpty(),
            equalTo(true));
    }

    @Test
    public void shouldNotReuseRepartitionTopicAsChangelogs() {
        final String topic = "topic";
        builder.<Long, String>stream(topic).repartition().toTable(Materialized.as("store"));
        final Properties props = StreamsTestUtils.getStreamsConfig("appId");
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        final Topology topology = builder.build(props);

        final InternalTopologyBuilder internalTopologyBuilder = TopologyWrapper.getInternalTopologyBuilder(topology);
        internalTopologyBuilder.rewriteTopology(new StreamsConfig(props));

        assertThat(
            internalTopologyBuilder.buildTopology().storeToChangelogTopic(),
            equalTo(Collections.singletonMap("store", "appId-store-changelog"))
        );
        assertThat(
            internalTopologyBuilder.stateStores().keySet(),
            equalTo(Collections.singleton("store"))
        );
        assertThat(
            internalTopologyBuilder.stateStores().get("store").loggingEnabled(),
            equalTo(true)
        );
        assertThat(
            internalTopologyBuilder.subtopologyToTopicsInfo().get(SUBTOPOLOGY_1).stateChangelogTopics.keySet(),
            equalTo(Collections.singleton("appId-store-changelog"))
        );
    }

    @Test
    public void shouldNotReuseSourceTopicAsChangelogsByDefault() {
        final String topic = "topic";
        builder.table(topic, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("store"));

        final InternalTopologyBuilder internalTopologyBuilder = TopologyWrapper.getInternalTopologyBuilder(builder.build());
        internalTopologyBuilder.setApplicationId("appId");

        assertThat(
            internalTopologyBuilder.buildTopology().storeToChangelogTopic(),
            equalTo(Collections.singletonMap("store", "appId-store-changelog")));
        assertThat(
            internalTopologyBuilder.stateStores().keySet(),
            equalTo(Collections.singleton("store")));
        assertThat(
            internalTopologyBuilder.stateStores().get("store").loggingEnabled(),
            equalTo(true));
        assertThat(
            internalTopologyBuilder.subtopologyToTopicsInfo().get(SUBTOPOLOGY_0).stateChangelogTopics.keySet(),
            equalTo(Collections.singleton("appId-store-changelog")));
    }

    @Test
    public void shouldThrowExceptionWhenNoTopicPresent() {
        builder.stream(Collections.emptyList());
        assertThrows(TopologyException.class, builder::build);
    }

    @Test
    public void shouldThrowExceptionWhenTopicNamesAreNull() {
        builder.stream(Arrays.asList(null, null));
        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    public void shouldUseSpecifiedNameForStreamSourceProcessor() {
        final String expected = "source-node";
        builder.stream(STREAM_TOPIC, Consumed.as(expected));
        builder.stream(STREAM_TOPIC_TWO);
        builder.build();
        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertNamesForOperation(topology, expected, "KSTREAM-SOURCE-0000000001");
    }

    @Test
    public void shouldUseSpecifiedNameForTableSourceProcessor() {
        final String expected = "source-node";
        builder.table(STREAM_TOPIC, Consumed.as(expected));
        builder.table(STREAM_TOPIC_TWO);
        builder.build();

        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();

        assertNamesForOperation(
            topology,
            expected + "-source",
            expected,
            "KSTREAM-SOURCE-0000000004",
            "KTABLE-SOURCE-0000000005");
    }

    @Test
    public void shouldUseSpecifiedNameForGlobalTableSourceProcessor() {
        final String expected = "source-processor";
        builder.globalTable(STREAM_TOPIC, Consumed.as(expected));
        builder.globalTable(STREAM_TOPIC_TWO);
        builder.build();
        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();

        assertNamesForStateStore(
            topology.globalStateStores(),
            "stream-topic-STATE-STORE-0000000000",
            "stream-topic-two-STATE-STORE-0000000003"
        );
    }

    @Test
    public void shouldUseSpecifiedNameForSinkProcessor() {
        final String expected = "sink-processor";
        final KStream<Object, Object> stream = builder.stream(STREAM_TOPIC);
        stream.to(STREAM_TOPIC_TWO, Produced.as(expected));
        stream.to(STREAM_TOPIC_TWO);
        builder.build();
        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertNamesForOperation(topology, "KSTREAM-SOURCE-0000000000", expected, "KSTREAM-SINK-0000000002");
    }

    @Test
    public void shouldUseSpecifiedNameForMapOperation() {
        builder.stream(STREAM_TOPIC).map(KeyValue::pair, Named.as(STREAM_OPERATION_NAME));
        builder.build();
        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertNamesForOperation(topology, "KSTREAM-SOURCE-0000000000", STREAM_OPERATION_NAME);
    }

    @Test
    public void shouldUseSpecifiedNameForMapValuesOperation() {
        builder.stream(STREAM_TOPIC).mapValues(v -> v, Named.as(STREAM_OPERATION_NAME));
        builder.build();
        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertNamesForOperation(topology, "KSTREAM-SOURCE-0000000000", STREAM_OPERATION_NAME);
    }

    @Test
    public void shouldUseSpecifiedNameForMapValuesWithKeyOperation() {
        builder.stream(STREAM_TOPIC).mapValues((k, v) -> v, Named.as(STREAM_OPERATION_NAME));
        builder.build();
        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertNamesForOperation(topology, "KSTREAM-SOURCE-0000000000", STREAM_OPERATION_NAME);
    }

    @Test
    public void shouldUseSpecifiedNameForFilterOperation() {
        builder.stream(STREAM_TOPIC).filter((k, v) -> true, Named.as(STREAM_OPERATION_NAME));
        builder.build();
        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertNamesForOperation(topology, "KSTREAM-SOURCE-0000000000", STREAM_OPERATION_NAME);
    }

    @Test
    public void shouldUseSpecifiedNameForForEachOperation() {
        builder.stream(STREAM_TOPIC).foreach((k, v) -> { }, Named.as(STREAM_OPERATION_NAME));
        builder.build();
        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertNamesForOperation(topology, "KSTREAM-SOURCE-0000000000", STREAM_OPERATION_NAME);
    }

    @Test
    public void shouldUseSpecifiedNameForSplitOperation() {
        builder.stream(STREAM_TOPIC)
                .split(Named.as("branch-processor"))
                .branch((k, v) -> true, Branched.as("-1"))
                .branch((k, v) -> false, Branched.as("-2"));
        builder.build();
        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertNamesForOperation(topology,
                "KSTREAM-SOURCE-0000000000",
                "branch-processor",
                "branch-processor-1",
                "branch-processor-2");
    }

    @Test
    public void shouldUseSpecifiedNameForJoinOperationBetweenKStreamAndKTable() {
        final KStream<String, String> streamOne = builder.stream(STREAM_TOPIC);
        final KTable<String, String> streamTwo = builder.table("table-topic");
        streamOne.join(streamTwo, (value1, value2) -> value1, Joined.as(STREAM_OPERATION_NAME));
        builder.build();

        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertNamesForOperation(topology,
                                "KSTREAM-SOURCE-0000000000",
                                "KSTREAM-SOURCE-0000000002",
                                "KTABLE-SOURCE-0000000003",
                                STREAM_OPERATION_NAME);
    }

    @Test
    public void shouldUseSpecifiedNameForLeftJoinOperationBetweenKStreamAndKTable() {
        final KStream<String, String> streamOne = builder.stream(STREAM_TOPIC);
        final KTable<String, String> streamTwo = builder.table(STREAM_TOPIC_TWO);
        streamOne.leftJoin(streamTwo, (value1, value2) -> value1, Joined.as(STREAM_OPERATION_NAME));
        builder.build();

        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertNamesForOperation(topology,
                                "KSTREAM-SOURCE-0000000000",
                                "KSTREAM-SOURCE-0000000002",
                                "KTABLE-SOURCE-0000000003",
                                STREAM_OPERATION_NAME);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldNotAddThirdStateStoreIfStreamStreamJoinFixIsDisabledViaOldApi() {
        final KStream<String, String> streamOne = builder.stream(STREAM_TOPIC);
        final KStream<String, String> streamTwo = builder.stream(STREAM_TOPIC_TWO);

        streamOne.leftJoin(
                streamTwo,
                (value1, value2) -> value1,
                JoinWindows.of(Duration.ofHours(1)),
                StreamJoined.<String, String, String>as(STREAM_OPERATION_NAME)
                        .withName(STREAM_OPERATION_NAME)
        );

        final Properties properties = new Properties();
        builder.build(properties);

        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertNamesForStateStore(topology.stateStores(),
                STREAM_OPERATION_NAME + "-this-join-store",
                STREAM_OPERATION_NAME + "-outer-other-join-store"
        );
        assertNamesForOperation(topology,
                "KSTREAM-SOURCE-0000000000",
                "KSTREAM-SOURCE-0000000001",
                STREAM_OPERATION_NAME + "-this-windowed",
                STREAM_OPERATION_NAME + "-other-windowed",
                STREAM_OPERATION_NAME + "-this-join",
                STREAM_OPERATION_NAME + "-outer-other-join",
                STREAM_OPERATION_NAME + "-merge");
    }

    @Test
    public void shouldUseSpecifiedNameForLeftJoinOperationBetweenKStreamAndKStream() {
        final KStream<String, String> streamOne = builder.stream(STREAM_TOPIC);
        final KStream<String, String> streamTwo = builder.stream(STREAM_TOPIC_TWO);

        streamOne.leftJoin(
            streamTwo,
            (value1, value2) -> value1,
            JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofHours(1)),
            StreamJoined.<String, String, String>as(STREAM_OPERATION_NAME)
                .withName(STREAM_OPERATION_NAME)
        );
        builder.build();

        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertNamesForStateStore(topology.stateStores(),
            STREAM_OPERATION_NAME + "-this-join-store",
            STREAM_OPERATION_NAME + "-outer-other-join-store",
            STREAM_OPERATION_NAME + "-left-shared-join-store"
        );
        assertNamesForOperation(topology,
                                "KSTREAM-SOURCE-0000000000",
                                "KSTREAM-SOURCE-0000000001",
                                STREAM_OPERATION_NAME + "-this-windowed",
                                STREAM_OPERATION_NAME + "-other-windowed",
                                STREAM_OPERATION_NAME + "-this-join",
                                STREAM_OPERATION_NAME + "-outer-other-join",
                                STREAM_OPERATION_NAME + "-merge");
    }

    @Test
    public void shouldUseGeneratedStoreNamesForLeftJoinOperationBetweenKStreamAndKStream() {
        final KStream<String, String> streamOne = builder.stream(STREAM_TOPIC);
        final KStream<String, String> streamTwo = builder.stream(STREAM_TOPIC_TWO);

        streamOne.leftJoin(
            streamTwo,
            (value1, value2) -> value1,
            JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofHours(1)),
            StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
                .withName(STREAM_OPERATION_NAME)
        );
        builder.build();

        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertNamesForStateStore(topology.stateStores(),
                                 "KSTREAM-JOINTHIS-0000000004-store",
                                 "KSTREAM-OUTEROTHER-0000000005-store",
                                 "KSTREAM-OUTERSHARED-0000000004-store"
        );
        assertNamesForOperation(topology,
                                "KSTREAM-SOURCE-0000000000",
                                "KSTREAM-SOURCE-0000000001",
                                STREAM_OPERATION_NAME + "-this-windowed",
                                STREAM_OPERATION_NAME + "-other-windowed",
                                STREAM_OPERATION_NAME + "-this-join",
                                STREAM_OPERATION_NAME + "-outer-other-join",
                                STREAM_OPERATION_NAME + "-merge");
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldUseSpecifiedNameForJoinOperationBetweenKStreamAndKStream() {
        final KStream<String, String> streamOne = builder.stream(STREAM_TOPIC);
        final KStream<String, String> streamTwo = builder.stream(STREAM_TOPIC_TWO);

        streamOne.join(streamTwo,
            (value1, value2) -> value1,
            JoinWindows.of(Duration.ofHours(1)),
            StreamJoined.<String, String, String>as(STREAM_OPERATION_NAME).withName(STREAM_OPERATION_NAME));
        builder.build();

        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertNamesForStateStore(topology.stateStores(),
                                 STREAM_OPERATION_NAME + "-this-join-store",
                                 STREAM_OPERATION_NAME + "-other-join-store"
        );
        assertNamesForOperation(topology,
                                "KSTREAM-SOURCE-0000000000",
                                "KSTREAM-SOURCE-0000000001",
                                STREAM_OPERATION_NAME + "-this-windowed",
                                STREAM_OPERATION_NAME + "-other-windowed",
                                STREAM_OPERATION_NAME + "-this-join",
                                STREAM_OPERATION_NAME + "-other-join",
                                STREAM_OPERATION_NAME + "-merge");
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldUseGeneratedNameForJoinOperationBetweenKStreamAndKStream() {
        final KStream<String, String> streamOne = builder.stream(STREAM_TOPIC);
        final KStream<String, String> streamTwo = builder.stream(STREAM_TOPIC_TWO);

        streamOne.join(streamTwo,
            (value1, value2) -> value1,
            JoinWindows.of(Duration.ofHours(1)),
            StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
                .withName(STREAM_OPERATION_NAME));
        builder.build();

        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertNamesForStateStore(topology.stateStores(),
                                 "KSTREAM-JOINTHIS-0000000004-store",
                                 "KSTREAM-JOINOTHER-0000000005-store"
        );
        assertNamesForOperation(topology,
                                "KSTREAM-SOURCE-0000000000",
                                "KSTREAM-SOURCE-0000000001",
                                STREAM_OPERATION_NAME + "-this-windowed",
                                STREAM_OPERATION_NAME + "-other-windowed",
                                STREAM_OPERATION_NAME + "-this-join",
                                STREAM_OPERATION_NAME + "-other-join",
                                STREAM_OPERATION_NAME + "-merge");
    }

    @Test
    public void shouldUseSpecifiedNameForOuterJoinOperationBetweenKStreamAndKStream() {
        final KStream<String, String> streamOne = builder.stream(STREAM_TOPIC);
        final KStream<String, String> streamTwo = builder.stream(STREAM_TOPIC_TWO);

        streamOne.outerJoin(
            streamTwo,
            (value1, value2) -> value1,
            JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofHours(1)),
            StreamJoined.<String, String, String>as(STREAM_OPERATION_NAME)
                .withName(STREAM_OPERATION_NAME)
        );
        builder.build();
        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertNamesForStateStore(topology.stateStores(),
                                 STREAM_OPERATION_NAME + "-outer-this-join-store",
                                 STREAM_OPERATION_NAME + "-outer-other-join-store",
                                 STREAM_OPERATION_NAME + "-outer-shared-join-store");
        assertNamesForOperation(topology,
                                "KSTREAM-SOURCE-0000000000",
                                "KSTREAM-SOURCE-0000000001",
                                STREAM_OPERATION_NAME + "-this-windowed",
                                STREAM_OPERATION_NAME + "-other-windowed",
                                STREAM_OPERATION_NAME + "-outer-this-join",
                                STREAM_OPERATION_NAME + "-outer-other-join",
                                STREAM_OPERATION_NAME + "-merge");

    }

    @Test
    public void shouldUseGeneratedStoreNamesForOuterJoinOperationBetweenKStreamAndKStream() {
        final KStream<String, String> streamOne = builder.stream(STREAM_TOPIC);
        final KStream<String, String> streamTwo = builder.stream(STREAM_TOPIC_TWO);

        streamOne.outerJoin(
            streamTwo,
            (value1, value2) -> value1,
            JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofHours(1)),
            StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
                .withName(STREAM_OPERATION_NAME)
        );
        builder.build();

        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertNamesForStateStore(topology.stateStores(),
                                 "KSTREAM-OUTERTHIS-0000000004-store",
                                 "KSTREAM-OUTEROTHER-0000000005-store",
                                 "KSTREAM-OUTERSHARED-0000000004-store"
        );
        assertNamesForOperation(topology,
                                "KSTREAM-SOURCE-0000000000",
                                "KSTREAM-SOURCE-0000000001",
                                STREAM_OPERATION_NAME + "-this-windowed",
                                STREAM_OPERATION_NAME + "-other-windowed",
                                STREAM_OPERATION_NAME + "-outer-this-join",
                                STREAM_OPERATION_NAME + "-outer-other-join",
                                STREAM_OPERATION_NAME + "-merge");
    }

    @Test
    public void shouldUseSpecifiedDslStoreSuppliersForAllOuterJoinOperationBetweenKStreamAndKStream() {
        final KStream<String, String> streamOne = builder.stream(STREAM_TOPIC);
        final KStream<String, String> streamTwo = builder.stream(STREAM_TOPIC_TWO);

        streamOne.outerJoin(
                streamTwo,
                (value1, value2) -> value1,
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofHours(1)),
                StreamJoined.<String, String, String>as(STREAM_OPERATION_NAME)
                        .withName(STREAM_OPERATION_NAME)
                        .withDslStoreSuppliers(BuiltInDslStoreSuppliers.IN_MEMORY)
        );

        builder.build();
        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertTypesForStateStore(topology.stateStores(),
                InMemoryWindowStore.class,
                InMemoryWindowStore.class,
                InMemoryKeyValueStore.class);
    }

    @Test
    public void shouldUseConfiguredInStreamsConfigIfNoTopologyOverrideDslStoreSuppliersForAllOuterJoinOperationBetweenKStreamAndKStream() {
        final KStream<String, String> streamOne = builder.stream(STREAM_TOPIC);
        final KStream<String, String> streamTwo = builder.stream(STREAM_TOPIC_TWO);

        streamOne.outerJoin(
                streamTwo,
                (value1, value2) -> value1,
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofHours(1)),
                StreamJoined.<String, String, String>as(STREAM_OPERATION_NAME)
                        .withName(STREAM_OPERATION_NAME)
        );

        builder.build();
        props.put(StreamsConfig.DSL_STORE_SUPPLIERS_CLASS_CONFIG, BuiltInDslStoreSuppliers.InMemoryDslStoreSuppliers.class);
        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertTypesForStateStore(topology.stateStores(),
                InMemoryWindowStore.class,
                InMemoryWindowStore.class,
                InMemoryKeyValueStore.class);
    }

    @Test
    public void shouldUseConfiguredTopologyOverrideDslStoreSuppliersForAllOuterJoinOperationBetweenKStreamAndKStream() {
        final Properties topoOverrides = new Properties();
        topoOverrides.putAll(props);
        topoOverrides.put(StreamsConfig.DSL_STORE_SUPPLIERS_CLASS_CONFIG, BuiltInDslStoreSuppliers.InMemoryDslStoreSuppliers.class);
        final StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(topoOverrides)));

        final KStream<String, String> streamOne = builder.stream(STREAM_TOPIC);
        final KStream<String, String> streamTwo = builder.stream(STREAM_TOPIC_TWO);

        streamOne.outerJoin(
                streamTwo,
                (value1, value2) -> value1,
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofHours(1)),
                StreamJoined.<String, String, String>as(STREAM_OPERATION_NAME)
                        .withName(STREAM_OPERATION_NAME)
        );

        builder.build();
        props.put(StreamsConfig.DSL_STORE_SUPPLIERS_CLASS_CONFIG, BuiltInDslStoreSuppliers.RocksDBDslStoreSuppliers.class);
        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertTypesForStateStore(topology.stateStores(),
                InMemoryWindowStore.class,
                InMemoryWindowStore.class,
                InMemoryKeyValueStore.class);
    }

    @Test
    public void shouldUseSpecifiedStoreSupplierForEachOuterJoinOperationBetweenKStreamAndKStreamAndUseSameTypeAsThisSupplierForOuter() {
        final KStream<String, String> streamOne = builder.stream(STREAM_TOPIC);
        final KStream<String, String> streamTwo = builder.stream(STREAM_TOPIC_TWO);

        final JoinWindows windows = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofHours(1));
        streamOne.outerJoin(
                streamTwo,
                (value1, value2) -> value1,
                windows,
                StreamJoined.<String, String, String>as(STREAM_OPERATION_NAME)
                        .withName(STREAM_OPERATION_NAME)
                        .withThisStoreSupplier(Stores.inMemoryWindowStore(
                                "thisSupplier",
                                Duration.ofMillis(windows.size() + windows.gracePeriodMs()),
                                Duration.ofMillis(windows.size()),
                                true
                        ))
                        .withOtherStoreSupplier(Stores.persistentWindowStore(
                                "otherSupplier",
                                Duration.ofMillis(windows.size() + windows.gracePeriodMs()),
                                Duration.ofMillis(windows.size()),
                                true
                        ))
        );

        builder.build();
        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertTypesForStateStore(topology.stateStores(),
                InMemoryWindowStore.class,
                RocksDBWindowStore.class,
                InMemoryKeyValueStore.class);
    }

    @Test
    public void shouldUseSpecifiedStoreSuppliersOuterJoinStoreEvenIfThisSupplierIsSupplied() {
        final KStream<String, String> streamOne = builder.stream(STREAM_TOPIC);
        final KStream<String, String> streamTwo = builder.stream(STREAM_TOPIC_TWO);

        final JoinWindows windows = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofHours(1));
        streamOne.outerJoin(
                streamTwo,
                (value1, value2) -> value1,
                windows,
                StreamJoined.<String, String, String>as(STREAM_OPERATION_NAME)
                        .withName(STREAM_OPERATION_NAME)
                        .withDslStoreSuppliers(BuiltInDslStoreSuppliers.ROCKS_DB)
                        .withThisStoreSupplier(Stores.inMemoryWindowStore(
                                "thisSupplier",
                                Duration.ofMillis(windows.size() + windows.gracePeriodMs()),
                                Duration.ofMillis(windows.size()),
                                true
                        ))
                        .withOtherStoreSupplier(Stores.persistentWindowStore(
                                "otherSupplier",
                                Duration.ofMillis(windows.size() + windows.gracePeriodMs()),
                                Duration.ofMillis(windows.size()),
                                true
                        ))
        );

        builder.build();
        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertTypesForStateStore(topology.stateStores(),
                InMemoryWindowStore.class,
                RocksDBWindowStore.class,
                RocksDBStore.class);
    }

    @Test
    public void shouldUseThisStoreSupplierEvenIfDslStoreSuppliersConfiguredInTopologyConfig() {
        final Properties topoOverrides = new Properties();
        topoOverrides.putAll(props);
        topoOverrides.put(StreamsConfig.DSL_STORE_SUPPLIERS_CLASS_CONFIG, BuiltInDslStoreSuppliers.RocksDBDslStoreSuppliers.class);
        final StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(topoOverrides)));

        final KStream<String, String> streamOne = builder.stream(STREAM_TOPIC);
        final KStream<String, String> streamTwo = builder.stream(STREAM_TOPIC_TWO);

        final JoinWindows windows = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofHours(1));
        streamOne.outerJoin(
                streamTwo,
                (value1, value2) -> value1,
                windows,
                StreamJoined.<String, String, String>as(STREAM_OPERATION_NAME)
                        .withName(STREAM_OPERATION_NAME)
                        .withThisStoreSupplier(Stores.inMemoryWindowStore(
                                "thisSupplier",
                                Duration.ofMillis(windows.size() + windows.gracePeriodMs()),
                                Duration.ofMillis(windows.size()),
                                true
                        ))
                        .withOtherStoreSupplier(Stores.persistentWindowStore(
                                "otherSupplier",
                                Duration.ofMillis(windows.size() + windows.gracePeriodMs()),
                                Duration.ofMillis(windows.size()),
                                true
                        ))
        );

        builder.build();
        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertTypesForStateStore(topology.stateStores(),
                InMemoryWindowStore.class,
                RocksDBWindowStore.class,
                InMemoryKeyValueStore.class);
    }

    @Test
    public void shouldUseSpecifiedNameForMergeOperation() {
        final String topic1 = "topic-1";
        final String topic2 = "topic-2";

        final KStream<String, String> source1 = builder.stream(topic1);
        final KStream<String, String> source2 = builder.stream(topic2);
        source1.merge(source2, Named.as("merge-processor"));
        builder.build();
        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertNamesForOperation(topology, "KSTREAM-SOURCE-0000000000", "KSTREAM-SOURCE-0000000001", "merge-processor");
    }

    @Test
    public void shouldUseSpecifiedNameForProcessOperation() {
        builder.stream(STREAM_TOPIC)
                .process(new MockApiProcessorSupplier<>(), Named.as("test-processor"));

        builder.build();
        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertNamesForOperation(topology, "KSTREAM-SOURCE-0000000000", "test-processor");
    }

    @Test
    public void shouldUseSpecifiedNameForProcessValuesOperation() {
        builder.stream(STREAM_TOPIC)
            .processValues(new MockApiFixedKeyProcessorSupplier<>(), Named.as("test-fixed-key-processor"));

        builder.build();
        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertNamesForOperation(topology, "KSTREAM-SOURCE-0000000000", "test-fixed-key-processor");
    }

    @Test
    public void shouldUseSpecifiedNameForPrintOperation() {
        builder.stream(STREAM_TOPIC).print(Printed.toSysOut().withName("print-processor"));
        builder.build();
        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertNamesForOperation(topology, "KSTREAM-SOURCE-0000000000", "print-processor");
    }

    @Test
    public void shouldUseSpecifiedNameForToStream() {
        builder.table(STREAM_TOPIC)
               .toStream(Named.as("to-stream"));

        builder.build();
        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertNamesForOperation(topology,
                                "KSTREAM-SOURCE-0000000001",
                                "KTABLE-SOURCE-0000000002",
                                "to-stream");
    }

    @Test
    public void shouldUseSpecifiedNameForToStreamWithMapper() {
        builder.table(STREAM_TOPIC)
               .toStream(KeyValue::pair, Named.as("to-stream"));

        builder.build();
        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertNamesForOperation(topology,
                                "KSTREAM-SOURCE-0000000001",
                                "KTABLE-SOURCE-0000000002",
                                "to-stream",
                                "KSTREAM-KEY-SELECT-0000000004");
    }

    @Test
    public void shouldUseSpecifiedNameForAggregateOperationGivenTable() {
        builder.table(STREAM_TOPIC).groupBy(KeyValue::pair, Grouped.as("group-operation")).count(Named.as(STREAM_OPERATION_NAME));
        builder.build();
        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildTopology();
        assertNamesForStateStore(
            topology.stateStores(),
            STREAM_TOPIC + "-STATE-STORE-0000000000",
            "KTABLE-AGGREGATE-STATE-STORE-0000000004");

        assertNamesForOperation(
            topology,
            "KSTREAM-SOURCE-0000000001",
            "KTABLE-SOURCE-0000000002",
            "group-operation",
            STREAM_OPERATION_NAME + "-sink",
            STREAM_OPERATION_NAME + "-source",
            STREAM_OPERATION_NAME);
    }

    @Test
    public void shouldUseSpecifiedNameForGlobalStoreProcessor() {
        builder.addGlobalStore(Stores.keyValueStoreBuilder(
                        inMemoryKeyValueStore("store"),
                        Serdes.String(),
                        Serdes.String()
                ),
                "topic",
                Consumed.with(Serdes.String(), Serdes.String()).withName("test"),
                new MockApiProcessorSupplier<>()
        );
        builder.build();

        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildGlobalStateTopology();
        assertNamesForOperation(topology, "test-source", "test");
    }

    @Test
    public void shouldUseDefaultNameForGlobalStoreProcessor() {
        builder.addGlobalStore(Stores.keyValueStoreBuilder(
                        inMemoryKeyValueStore("store"),
                        Serdes.String(),
                        Serdes.String()
                ),
                "topic",
                Consumed.with(Serdes.String(), Serdes.String()),
                new MockApiProcessorSupplier<>()
        );
        builder.build();

        final ProcessorTopology topology = builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(props)).buildGlobalStateTopology();
        assertNamesForOperation(topology, "KSTREAM-SOURCE-0000000000", "KTABLE-SOURCE-0000000001");
    }

    @Test
    public void shouldWrapProcessorsForProcess() {
        final Map<Object, Object> props = dummyStreamsConfigMap();
        props.put(PROCESSOR_WRAPPER_CLASS_CONFIG, RecordingProcessorWrapper.class);

        final WrapperRecorder counter = new WrapperRecorder();
        props.put(PROCESSOR_WRAPPER_COUNTER_CONFIG, counter);

        final StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(props)));

        // Add a bit of randomness to the lambda-created processors to avoid them being
        // optimized into a shared instance that will cause the ApiUtils#checkSupplier
        // call to fail
        final Random random = new Random();

        final StoreBuilder<?> store = timestampedKeyValueStoreBuilder(inMemoryKeyValueStore("store"), Serdes.String(), Serdes.String());
        builder.stream("input", Consumed.as("source"))
            .process(
                new ProcessorSupplier<>() {
                    @Override
                    public Processor<Object, Object, Object, Object> get() {
                        return record -> System.out.println("Processing: " + random.nextInt());
                    }

                    @Override
                    public Set<StoreBuilder<?>> stores() {
                        return Collections.singleton(store);
                    }
                },
                Named.as("stateful-process-1"))
            .process(
                new ProcessorSupplier<>() {
                    @Override
                    public Processor<Object, Object, Object, Object> get() {
                        return record -> System.out.println("Processing: " + random.nextInt());
                    }

                    @Override
                    public Set<StoreBuilder<?>> stores() {
                        return Collections.singleton(store);
                    }
                },
                Named.as("stateful-process-2"))
            .processValues(
                () -> record -> System.out.println("Processing values: " + random.nextInt()),
                Named.as("stateless-processValues"))
            .to("output", Produced.as("sink"));

        builder.build();
        assertThat(counter.numWrappedProcessors(), CoreMatchers.is(3));
        assertThat(counter.wrappedProcessorNames(), Matchers.containsInAnyOrder(
            "stateful-process-1", "stateful-process-2", "stateless-processValues"));
        assertThat(counter.numUniqueStateStores(), CoreMatchers.is(1));
        assertThat(counter.numConnectedStateStores(), CoreMatchers.is(2));
    }

    @Test
    public void shouldWrapProcessorsForStreamReduce() {
        final Map<Object, Object> props = dummyStreamsConfigMap();
        props.put(PROCESSOR_WRAPPER_CLASS_CONFIG, RecordingProcessorWrapper.class);

        final WrapperRecorder counter = new WrapperRecorder();
        props.put(PROCESSOR_WRAPPER_COUNTER_CONFIG, counter);

        final StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(props)));

        builder.stream("input", Consumed.as("source"))
            .groupBy(KeyValue::new, Grouped.as("groupBy")) // wrapped 1 & 2 (implicit selectKey & repartition)
            .reduce((l, r) -> l, Named.as("reduce"), Materialized.as("store")) // wrapped 3
            .toStream(Named.as("toStream"))// wrapped 4
            .to("output", Produced.as("sink"));

        builder.build();
        assertThat(counter.wrappedProcessorNames(), Matchers.containsInAnyOrder(
            "groupBy", "groupBy-repartition-filter", "reduce", "toStream"));
        assertThat(counter.numWrappedProcessors(), CoreMatchers.is(4));
        assertThat(counter.numUniqueStateStores(), CoreMatchers.is(1));
        assertThat(counter.numConnectedStateStores(), CoreMatchers.is(1));
    }

    @Test
    public void shouldWrapProcessorsForStreamAggregate() {
        final Map<Object, Object> props = dummyStreamsConfigMap();
        props.put(PROCESSOR_WRAPPER_CLASS_CONFIG, RecordingProcessorWrapper.class);

        final WrapperRecorder counter = new WrapperRecorder();
        props.put(PROCESSOR_WRAPPER_COUNTER_CONFIG, counter);

        final StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(props)));

        builder.stream("input", Consumed.as("source"))
            .groupByKey()
            .count(Named.as("count")) // wrapped 1
            .toStream(Named.as("toStream"))// wrapped 2
            .to("output", Produced.as("sink"));

        builder.build();
        assertThat(counter.numWrappedProcessors(), CoreMatchers.is(2));
        assertThat(counter.wrappedProcessorNames(), Matchers.containsInAnyOrder("count", "toStream"));
        assertThat(counter.numUniqueStateStores(), CoreMatchers.is(1));
        assertThat(counter.numConnectedStateStores(), CoreMatchers.is(1));
    }

    @Test
    public void shouldWrapProcessorsForTimeWindowStreamAggregate() {
        final Map<Object, Object> props = dummyStreamsConfigMap();
        props.put(PROCESSOR_WRAPPER_CLASS_CONFIG, RecordingProcessorWrapper.class);

        final WrapperRecorder counter = new WrapperRecorder();
        props.put(PROCESSOR_WRAPPER_COUNTER_CONFIG, counter);

        final StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(props)));

        builder.stream("input", Consumed.as("source"))
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofDays(1)))
            .count(Named.as("count")) // wrapped 1
            .toStream(Named.as("toStream"))// wrapped 2
            .to("output", Produced.as("sink"));

        builder.build();
        assertThat(counter.numWrappedProcessors(), CoreMatchers.is(2));
        assertThat(counter.wrappedProcessorNames(), Matchers.containsInAnyOrder("count", "toStream"));
        assertThat(counter.numUniqueStateStores(), CoreMatchers.is(1));
        assertThat(counter.numConnectedStateStores(), CoreMatchers.is(1));
    }

    @Test
    public void shouldWrapProcessorsForSlidingWindowStreamAggregate() {
        final Map<Object, Object> props = dummyStreamsConfigMap();
        props.put(PROCESSOR_WRAPPER_CLASS_CONFIG, RecordingProcessorWrapper.class);

        final WrapperRecorder counter = new WrapperRecorder();
        props.put(PROCESSOR_WRAPPER_COUNTER_CONFIG, counter);

        final StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(props)));

        builder.stream("input", Consumed.as("source"))
            .groupByKey()
            .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofDays(1)))
            .count(Named.as("count")) // wrapped 1
            .toStream(Named.as("toStream"))// wrapped 2
            .to("output", Produced.as("sink"));

        builder.build();
        assertThat(counter.numWrappedProcessors(), CoreMatchers.is(2));
        assertThat(counter.wrappedProcessorNames(), Matchers.containsInAnyOrder("count", "toStream"));
        assertThat(counter.numUniqueStateStores(), CoreMatchers.is(1));
        assertThat(counter.numConnectedStateStores(), CoreMatchers.is(1));
    }

    @Test
    public void shouldWrapProcessorsForSessionWindowStreamAggregate() {
        final Map<Object, Object> props = dummyStreamsConfigMap();
        props.put(PROCESSOR_WRAPPER_CLASS_CONFIG, RecordingProcessorWrapper.class);

        final WrapperRecorder counter = new WrapperRecorder();
        props.put(PROCESSOR_WRAPPER_COUNTER_CONFIG, counter);

        final StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(props)));

        builder.stream("input", Consumed.as("source"))
            .groupByKey()
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofDays(1)))
            .count(Named.as("count")) // wrapped 1
            .toStream(Named.as("toStream"))// wrapped 2
            .to("output", Produced.as("sink"));

        builder.build();
        assertThat(counter.numWrappedProcessors(), CoreMatchers.is(2));
        assertThat(counter.wrappedProcessorNames(), Matchers.containsInAnyOrder("count", "toStream"));
        assertThat(counter.numUniqueStateStores(), CoreMatchers.is(1));
        assertThat(counter.numConnectedStateStores(), CoreMatchers.is(1));
    }

    @Test
    public void shouldWrapProcessorsForCoGroupedStreamAggregate() {
        final Map<Object, Object> props = dummyStreamsConfigMap();
        props.put(PROCESSOR_WRAPPER_CLASS_CONFIG, RecordingProcessorWrapper.class);

        final WrapperRecorder counter = new WrapperRecorder();
        props.put(PROCESSOR_WRAPPER_COUNTER_CONFIG, counter);

        final StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(props)));

        final KStream<String, String> stream1 = builder.stream("one", Consumed.as("source-1"));
        final KStream<String, String> stream2 = builder.stream("two", Consumed.as("source-2"));

        final KGroupedStream<String, String> grouped1 = stream1.groupByKey(Grouped.as("groupByKey-1"));
        final KGroupedStream<String, String> grouped2 = stream2.groupByKey(Grouped.as("groupByKey-2"));

        grouped1
            .cogroup((k, v, a) -> a + v) // wrapped 1
            .cogroup(grouped2, (k, v, a) -> a + v) // wrapped 2
            .aggregate(() -> "", Named.as("aggregate"), Materialized.as("store")) // wrapped 3, store 1
            .toStream(Named.as("toStream"))// wrapped 4
            .to("output", Produced.as("sink"));

        builder.build();

        assertThat(counter.wrappedProcessorNames(), Matchers.containsInAnyOrder(
            "aggregate-cogroup-agg-0", "aggregate-cogroup-agg-1", "aggregate-cogroup-merge", "toStream"
        ));
        assertThat(counter.numWrappedProcessors(), CoreMatchers.is(4));
        assertThat(counter.numUniqueStateStores(), CoreMatchers.is(1));
        assertThat(counter.numConnectedStateStores(), CoreMatchers.is(2));
    }

    @Test
    public void shouldWrapProcessorsForMapValuesWithMaterializedStore() {
        final Map<Object, Object> props = dummyStreamsConfigMap();
        props.put(PROCESSOR_WRAPPER_CLASS_CONFIG, RecordingProcessorWrapper.class);
        final WrapperRecorder counter = new WrapperRecorder();
        props.put(PROCESSOR_WRAPPER_COUNTER_CONFIG, counter);
        final StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(props)));

        builder.table("input", Consumed.as("source-table"))
            .mapValues(v -> null, Named.as("map-values"), Materialized.as("map-values-store"))
            .toStream(Named.as("to-stream"))
            .to("output-topic", Produced.as("sink"));
        builder.build();

        assertThat(counter.wrappedProcessorNames(),
            Matchers.containsInAnyOrder("source-table", "map-values", "to-stream"));
        assertThat(counter.numUniqueStateStores(), is(1));
        assertThat(counter.numConnectedStateStores(), is(1));
    }

    @Test
    public void shouldWrapProcessorsForTableAggregate() {
        final Map<Object, Object> props = dummyStreamsConfigMap();
        props.put(PROCESSOR_WRAPPER_CLASS_CONFIG, RecordingProcessorWrapper.class);

        final WrapperRecorder counter = new WrapperRecorder();
        props.put(PROCESSOR_WRAPPER_COUNTER_CONFIG, counter);

        final StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(props)));

        builder.table("input", Consumed.as("source-table")) // wrapped 1, store 1
            .groupBy(KeyValue::new, Grouped.as("groupBy")) // wrapped 2 (implicit selectKey)
            .count(Named.as("count")) // wrapped 3, store 2
            .toStream(Named.as("toStream"))// wrapped 4
            .to("output", Produced.as("sink"));

        builder.build();
        assertThat(counter.wrappedProcessorNames(), Matchers.containsInAnyOrder(
            "source-table", "groupBy", "count", "toStream"
        ));
        assertThat(counter.numWrappedProcessors(), CoreMatchers.is(4));
        assertThat(counter.numUniqueStateStores(), CoreMatchers.is(2));
        assertThat(counter.numConnectedStateStores(), CoreMatchers.is(2));
    }

    @Test
    public void shouldWrapProcessorsForTableReduce() {
        final Map<Object, Object> props = dummyStreamsConfigMap();
        props.put(PROCESSOR_WRAPPER_CLASS_CONFIG, RecordingProcessorWrapper.class);

        final WrapperRecorder counter = new WrapperRecorder();
        props.put(PROCESSOR_WRAPPER_COUNTER_CONFIG, counter);

        final StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(props)));

        builder.table("input", Consumed.as("source-table")) // wrapped 1, store 1
            .groupBy(KeyValue::new, Grouped.as("groupBy")) // wrapped 2 (implicit selectKey)
            .reduce((l, r) -> "", (l, r) -> "", Named.as("reduce"), Materialized.as("store")) // wrapped 3, store 2
            .toStream(Named.as("toStream"))// wrapped 4
            .to("output", Produced.as("sink"));

        builder.build();
        assertThat(counter.wrappedProcessorNames(), Matchers.containsInAnyOrder(
            "source-table", "groupBy", "reduce", "toStream"
        ));
        assertThat(counter.numWrappedProcessors(), CoreMatchers.is(4));
        assertThat(counter.numUniqueStateStores(), CoreMatchers.is(2));
        assertThat(counter.numConnectedStateStores(), CoreMatchers.is(2));
    }

    @Test
    public void shouldWrapProcessorsForStatelessOperators() {
        final Map<Object, Object> props = dummyStreamsConfigMap();
        props.put(PROCESSOR_WRAPPER_CLASS_CONFIG, RecordingProcessorWrapper.class);

        final WrapperRecorder counter = new WrapperRecorder();
        props.put(PROCESSOR_WRAPPER_COUNTER_CONFIG, counter);

        final StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(props)));

        builder.stream("input", Consumed.as("source"))
            .filter((k, v) -> true, Named.as("filter-stream")) // wrapped 1
            .map(KeyValue::new, Named.as("map")) // wrapped 2
            .selectKey((k, v) -> k, Named.as("selectKey")) // wrapped 3
            .peek((k, v) -> { }, Named.as("peek")) // wrapped 4
            .flatMapValues(e -> new ArrayList<>(), Named.as("flatMap")) // wrapped 5
            .to("output", Produced.as("sink"));

        builder.build();
        assertThat(counter.numWrappedProcessors(), CoreMatchers.is(5));
        assertThat(counter.wrappedProcessorNames(), Matchers.containsInAnyOrder(
            "filter-stream", "map", "selectKey", "peek", "flatMap"
        ));
        assertThat(counter.numUniqueStateStores(), CoreMatchers.is(0));
        assertThat(counter.numConnectedStateStores(), CoreMatchers.is(0));
    }

    @Test
    public void shouldWrapProcessorsWhenMultipleTableOperators() {
        final Map<Object, Object> props = dummyStreamsConfigMap();
        props.put(PROCESSOR_WRAPPER_CLASS_CONFIG, RecordingProcessorWrapper.class);

        final WrapperRecorder counter = new WrapperRecorder();
        props.put(PROCESSOR_WRAPPER_COUNTER_CONFIG, counter);

        final StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(props)));

        builder.stream("input", Consumed.as("source"))
            .toTable(Named.as("to-table"))
            .mapValues(v -> v, Named.as("map-values"))
            .mapValues(v -> v, Named.as("map-values-stateful"), Materialized.as("map-values-stateful"))
            .filter((k, v) -> true, Named.as("filter-table"))
            .filter((k, v) -> true, Named.as("filter-table-stateful"), Materialized.as("filter-table-stateful"))
            .toStream(Named.as("to-stream"))
            .to("output", Produced.as("sink"));

        builder.build();
        assertThat(counter.numWrappedProcessors(), CoreMatchers.is(6));
        assertThat(counter.wrappedProcessorNames(), Matchers.containsInAnyOrder(
            "to-table", "map-values", "map-values-stateful",
            "filter-table", "filter-table-stateful", "to-stream"
        ));
        assertThat(counter.numUniqueStateStores(), CoreMatchers.is(1));
        assertThat(counter.numConnectedStateStores(), CoreMatchers.is(1));
    }

    @Test
    public void shouldWrapProcessorsForUnmaterializedSourceTable() {
        final Map<Object, Object> props = dummyStreamsConfigMap();
        props.put(PROCESSOR_WRAPPER_CLASS_CONFIG, RecordingProcessorWrapper.class);

        final WrapperRecorder counter = new WrapperRecorder();
        props.put(PROCESSOR_WRAPPER_COUNTER_CONFIG, counter);

        final StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(props)));

        builder.table("input", Consumed.as("source")) // wrapped 1
            .toStream(Named.as("toStream")) // wrapped 2
            .to("output", Produced.as("sink"));

        builder.build();
        assertThat(counter.numWrappedProcessors(), CoreMatchers.is(2));
        assertThat(counter.wrappedProcessorNames(), Matchers.containsInAnyOrder(
            "source", "toStream"
        ));
        assertThat(counter.numUniqueStateStores(), CoreMatchers.is(0));
        assertThat(counter.numConnectedStateStores(), CoreMatchers.is(0));
    }

    @Test
    public void shouldWrapProcessorsForMaterializedSourceTable() {
        final Map<Object, Object> props = dummyStreamsConfigMap();
        props.put(PROCESSOR_WRAPPER_CLASS_CONFIG, RecordingProcessorWrapper.class);

        final WrapperRecorder counter = new WrapperRecorder();
        props.put(PROCESSOR_WRAPPER_COUNTER_CONFIG, counter);

        final StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(props)));

        builder.table("input", Consumed.as("source"), Materialized.as("store")) // wrapped 1
            .toStream(Named.as("toStream")) // wrapped 2
            .to("output", Produced.as("sink"));

        builder.build();
        assertThat(counter.numWrappedProcessors(), CoreMatchers.is(2));
        assertThat(counter.wrappedProcessorNames(), Matchers.containsInAnyOrder(
            "source", "toStream"
        ));
        assertThat(counter.numUniqueStateStores(), CoreMatchers.is(1));
        assertThat(counter.numConnectedStateStores(), CoreMatchers.is(1));
    }

    @Test
    public void shouldWrapProcessorsForStreamStreamInnerJoin() {
        final Map<Object, Object> props = dummyStreamsConfigMap();
        props.put(PROCESSOR_WRAPPER_CLASS_CONFIG, RecordingProcessorWrapper.class);

        final WrapperRecorder counter = new WrapperRecorder();
        props.put(PROCESSOR_WRAPPER_COUNTER_CONFIG, counter);

        final StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(props)));

        final KStream<String, String> stream1 = builder.stream("input-1", Consumed.as("source-1"));
        final KStream<String, String> stream2 = builder.stream("input-2", Consumed.as("source-2"));

        stream1.join(
                stream2,
                MockValueJoiner.TOSTRING_JOINER,
                JoinWindows.ofTimeDifferenceAndGrace(Duration.ofDays(1), Duration.ofDays(1)),
                StreamJoined.as("ss-join"))
            .to("output", Produced.as("sink"));

        builder.build();

        // TODO: fix these names once we address https://issues.apache.org/jira/browse/KAFKA-18191
        assertThat(counter.wrappedProcessorNames(), Matchers.containsInAnyOrder(
            "KSTREAM-JOINTHIS-0000000004", "KSTREAM-JOINOTHER-0000000005",
            "KSTREAM-WINDOWED-0000000003", "KSTREAM-WINDOWED-0000000002",
            "KSTREAM-MERGE-0000000006"
        ));
        assertThat(counter.numWrappedProcessors(), CoreMatchers.is(5));
        assertThat(counter.numUniqueStateStores(), CoreMatchers.is(2));
        assertThat(counter.numConnectedStateStores(), CoreMatchers.is(4));
    }

    @Test
    public void shouldWrapProcessorsForStreamStreamLeftJoin() {
        final Map<Object, Object> props = dummyStreamsConfigMap();
        props.put(PROCESSOR_WRAPPER_CLASS_CONFIG, RecordingProcessorWrapper.class);

        final WrapperRecorder counter = new WrapperRecorder();
        props.put(PROCESSOR_WRAPPER_COUNTER_CONFIG, counter);

        final StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(props)));

        final KStream<String, String> stream1 = builder.stream("input-1", Consumed.as("source-1"));
        final KStream<String, String> stream2 = builder.stream("input-2", Consumed.as("source-2"));

        stream1.leftJoin(
                stream2,
                MockValueJoiner.TOSTRING_JOINER,
                JoinWindows.ofTimeDifferenceAndGrace(Duration.ofDays(1), Duration.ofDays(1)),
                StreamJoined.as("ss-join"))
            .to("output", Produced.as("sink"));

        builder.build();

        // TODO: fix these names once we address https://issues.apache.org/jira/browse/KAFKA-18191
        assertThat(counter.wrappedProcessorNames(), Matchers.containsInAnyOrder(
            "KSTREAM-JOINTHIS-0000000004", "KSTREAM-OUTEROTHER-0000000005",
            "KSTREAM-WINDOWED-0000000003", "KSTREAM-WINDOWED-0000000002",
            "KSTREAM-MERGE-0000000006"
        ));
        assertThat(counter.numWrappedProcessors(), CoreMatchers.is(5));

        // 1 additional store due to spurious results fix for left/outer joins
        assertThat(counter.numUniqueStateStores(), CoreMatchers.is(3));
        assertThat(counter.numConnectedStateStores(), CoreMatchers.is(6));
    }

    @Test
    public void shouldWrapProcessorsForStreamStreamOuterJoin() {
        final Map<Object, Object> props = dummyStreamsConfigMap();
        props.put(PROCESSOR_WRAPPER_CLASS_CONFIG, RecordingProcessorWrapper.class);

        final WrapperRecorder counter = new WrapperRecorder();
        props.put(PROCESSOR_WRAPPER_COUNTER_CONFIG, counter);

        final StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(props)));

        final KStream<String, String> stream1 = builder.stream("input-1", Consumed.as("source-1"));
        final KStream<String, String> stream2 = builder.stream("input-2", Consumed.as("source-2"));

        stream1.outerJoin(
                stream2,
                MockValueJoiner.TOSTRING_JOINER,
                JoinWindows.ofTimeDifferenceAndGrace(Duration.ofDays(1), Duration.ofDays(1)),
                StreamJoined.as("ss-join"))
            .to("output", Produced.as("sink"));

        builder.build();

        // TODO: fix these names once we address https://issues.apache.org/jira/browse/KAFKA-18191
        assertThat(counter.wrappedProcessorNames(), Matchers.containsInAnyOrder(
            "KSTREAM-OUTERTHIS-0000000004", "KSTREAM-OUTEROTHER-0000000005",
            "KSTREAM-WINDOWED-0000000003", "KSTREAM-WINDOWED-0000000002",
            "KSTREAM-MERGE-0000000006"
        ));
        assertThat(counter.numWrappedProcessors(), CoreMatchers.is(5));

        // 1 additional store due to spurious results fix for left/outer joins
        assertThat(counter.numUniqueStateStores(), CoreMatchers.is(3));
        assertThat(counter.numConnectedStateStores(), CoreMatchers.is(6));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldWrapProcessorsForStreamStreamOuterJoinWithoutSpuriousResultsFix() {
        final Map<Object, Object> props = dummyStreamsConfigMap();
        props.put(PROCESSOR_WRAPPER_CLASS_CONFIG, RecordingProcessorWrapper.class);

        final WrapperRecorder counter = new WrapperRecorder();
        props.put(PROCESSOR_WRAPPER_COUNTER_CONFIG, counter);

        final StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(props)));

        final KStream<String, String> stream1 = builder.stream("input-1", Consumed.as("source-1"));
        final KStream<String, String> stream2 = builder.stream("input-2", Consumed.as("source-2"));

        stream1.outerJoin(
                stream2,
                MockValueJoiner.TOSTRING_JOINER,
                JoinWindows.of(Duration.ofDays(1)), // intentionally uses deprecated version of this API!
                StreamJoined.as("ss-join"))
            .to("output", Produced.as("sink"));

        builder.build();

        // TODO: fix these names once we address https://issues.apache.org/jira/browse/KAFKA-18191
        assertThat(counter.wrappedProcessorNames(), Matchers.containsInAnyOrder(
            "KSTREAM-OUTERTHIS-0000000004", "KSTREAM-OUTEROTHER-0000000005",
            "KSTREAM-WINDOWED-0000000003", "KSTREAM-WINDOWED-0000000002",
            "KSTREAM-MERGE-0000000006"
        ));
        assertThat(counter.numWrappedProcessors(), CoreMatchers.is(5));
        assertThat(counter.numUniqueStateStores(), CoreMatchers.is(2));
        assertThat(counter.numConnectedStateStores(), CoreMatchers.is(4));
    }

    @Test
    public void shouldWrapProcessorsForStreamStreamSelfJoin() {
        final Map<Object, Object> props = dummyStreamsConfigMap();
        props.put(PROCESSOR_WRAPPER_CLASS_CONFIG, RecordingProcessorWrapper.class);

        final WrapperRecorder counter = new WrapperRecorder();
        props.put(PROCESSOR_WRAPPER_COUNTER_CONFIG, counter);

        final StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(props)));

        final KStream<String, String> stream1 = builder.stream("input", Consumed.as("source"));

        stream1.join(
                stream1,
                MockValueJoiner.TOSTRING_JOINER,
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofDays(1)),
                StreamJoined.as("ss-join"))
            .to("output", Produced.as("sink"));

        builder.build();

        // TODO: fix these names once we address https://issues.apache.org/jira/browse/KAFKA-18191
        assertThat(counter.wrappedProcessorNames(), Matchers.containsInAnyOrder(
            "KSTREAM-JOINTHIS-0000000003", "KSTREAM-JOINOTHER-0000000004",
            "KSTREAM-WINDOWED-0000000001", "KSTREAM-WINDOWED-0000000002",
            "KSTREAM-MERGE-0000000005"
        ));
        assertThat(counter.numWrappedProcessors(), CoreMatchers.is(5));
        assertThat(counter.numUniqueStateStores(), CoreMatchers.is(2));
        assertThat(counter.numConnectedStateStores(), CoreMatchers.is(4));
    }

    @Test
    public void shouldWrapProcessorsForStreamStreamSelfJoinWithSharedStoreOptimization() {
        final Map<Object, Object> props = dummyStreamsConfigMap();
        props.put(PROCESSOR_WRAPPER_CLASS_CONFIG, RecordingProcessorWrapper.class);
        props.put(TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);

        final WrapperRecorder counter = new WrapperRecorder();
        props.put(PROCESSOR_WRAPPER_COUNTER_CONFIG, counter);

        final StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(props)));

        final KStream<String, String> stream1 = builder.stream("input", Consumed.as("source"));

        stream1.join(
                stream1,
                MockValueJoiner.TOSTRING_JOINER,
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofDays(1)),
                StreamJoined.as("ss-join"))
            .to("output", Produced.as("sink"));

        final Properties properties = new Properties();
        properties.putAll(props);
        builder.build(properties);

        // TODO: fix these names once we address https://issues.apache.org/jira/browse/KAFKA-18191
        assertThat(counter.wrappedProcessorNames(), Matchers.containsInAnyOrder(
            "KSTREAM-WINDOWED-0000000001", "KSTREAM-MERGE-0000000005"
        ));
        assertThat(counter.numWrappedProcessors(), CoreMatchers.is(2));
        // only 1 store when topology optimizations enabled due to sharing self-join store
        assertThat(counter.numUniqueStateStores(), CoreMatchers.is(1));
        assertThat(counter.numConnectedStateStores(), CoreMatchers.is(2));
    }

    @Test
    public void shouldWrapProcessorsForStreamTableJoin() {
        final Map<Object, Object> props = dummyStreamsConfigMap();
        props.put(PROCESSOR_WRAPPER_CLASS_CONFIG, RecordingProcessorWrapper.class);

        final WrapperRecorder counter = new WrapperRecorder();
        props.put(PROCESSOR_WRAPPER_COUNTER_CONFIG, counter);

        final StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(props)));

        final KStream<String, String> stream = builder.stream("input", Consumed.as("source-stream"));
        final KTable<String, String> table = builder.table("input-table", Consumed.as("source-table"));

        stream.join(
                table,
                MockValueJoiner.TOSTRING_JOINER,
                Joined.as("st-join"))
            .to("output", Produced.as("sink"));

        builder.build();

        assertThat(counter.wrappedProcessorNames(), Matchers.containsInAnyOrder(
            "source-table", "st-join"
        ));
        assertThat(counter.numWrappedProcessors(), CoreMatchers.is(2));
        assertThat(counter.numUniqueStateStores(), CoreMatchers.is(1));
        assertThat(counter.numConnectedStateStores(), CoreMatchers.is(1));
    }

    @Test
    public void shouldWrapProcessorsForStreamTableJoinWithGracePeriod() {
        final Map<Object, Object> props = dummyStreamsConfigMap();
        props.put(PROCESSOR_WRAPPER_CLASS_CONFIG, RecordingProcessorWrapper.class);

        final WrapperRecorder counter = new WrapperRecorder();
        props.put(PROCESSOR_WRAPPER_COUNTER_CONFIG, counter);

        final StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(props)));

        final KStream<String, String> stream = builder.stream("input", Consumed.as("source-stream"));
        final KTable<String, String> table = builder.table(
            "input-table",
            Consumed.as("versioned-source-table"),
            Materialized.as(Stores.persistentVersionedKeyValueStore("table-store", Duration.ofDays(1)))
        );

        stream.join(
            table,
            MockValueJoiner.TOSTRING_JOINER,
            Joined.<String, String, String>as("st-join").withGracePeriod(Duration.ofDays(1)))
            .to("output", Produced.as("sink"));

        builder.build();
        assertThat(counter.wrappedProcessorNames(), Matchers.containsInAnyOrder(
            "versioned-source-table", "st-join"
        ));
        assertThat(counter.numWrappedProcessors(), CoreMatchers.is(2));
        assertThat(counter.numUniqueStateStores(), CoreMatchers.is(2));
        assertThat(counter.numConnectedStateStores(), CoreMatchers.is(2));
    }

    @Test
    public void shouldWrapProcessorsForTableTableInnerJoin() {
        final Map<Object, Object> props = dummyStreamsConfigMap();
        props.put(PROCESSOR_WRAPPER_CLASS_CONFIG, RecordingProcessorWrapper.class);

        final WrapperRecorder counter = new WrapperRecorder();
        props.put(PROCESSOR_WRAPPER_COUNTER_CONFIG, counter);

        final StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(props)));

        final KTable<String, String> t1 = builder.table("input1", Consumed.as("input1")); // 1
        final KTable<String, String> t2 = builder.table("input2", Consumed.as("input2")); // 2

        t1.join(t2, (v1, v2) -> v1 + v2, Named.as("join-processor"), Materialized.as("the_join")) // 3 (this), 4 (other), 5 (merger)
                .toStream(Named.as("toStream")) // 6
                .to("output", Produced.as("sink"));

        builder.build();
        assertThat(counter.numWrappedProcessors(), CoreMatchers.is(6));
        assertThat(counter.wrappedProcessorNames().toString(), counter.wrappedProcessorNames(), Matchers.containsInAnyOrder(
                "input1",
                "input2",
                "join-processor-join-this",
                "join-processor-join-other",
                "join-processor",
                "toStream"
        ));

        assertThat(counter.numUniqueStateStores(), CoreMatchers.is(3)); // one for join this, one for join that
        assertThat(counter.numConnectedStateStores(), CoreMatchers.is(3));
    }

    @Test
    public void shouldWrapProcessorsForTableTableLeftJoin() {
        final Map<Object, Object> props = dummyStreamsConfigMap();
        props.put(PROCESSOR_WRAPPER_CLASS_CONFIG, RecordingProcessorWrapper.class);

        final WrapperRecorder counter = new WrapperRecorder();
        props.put(PROCESSOR_WRAPPER_COUNTER_CONFIG, counter);

        final StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(props)));

        final KTable<String, String> t1 = builder.table("input1", Consumed.as("input1")); // 1
        final KTable<String, String> t2 = builder.table("input2", Consumed.as("input2")); // 2

        t1.leftJoin(t2, (v1, v2) -> v1 + v2, Named.as("join-processor"), Materialized.as("the_join")) // 3 (this), 4 (other), 5 (merger)
                .toStream(Named.as("toStream")) // 6
                .to("output", Produced.as("sink"));

        builder.build();
        assertThat(counter.numWrappedProcessors(), CoreMatchers.is(6));
        assertThat(counter.wrappedProcessorNames().toString(), counter.wrappedProcessorNames(), Matchers.containsInAnyOrder(
                "input1",
                "input2",
                "join-processor-join-this",
                "join-processor-join-other",
                "join-processor",
                "toStream"
        ));

        assertThat(counter.numUniqueStateStores(), CoreMatchers.is(3)); // table1, table2, join materialized
        assertThat(counter.numConnectedStateStores(), CoreMatchers.is(3));
    }

    @Test
    public void shouldWrapProcessorsForTableTableOuterJoin() {
        final Map<Object, Object> props = dummyStreamsConfigMap();
        props.put(PROCESSOR_WRAPPER_CLASS_CONFIG, RecordingProcessorWrapper.class);

        final WrapperRecorder counter = new WrapperRecorder();
        props.put(PROCESSOR_WRAPPER_COUNTER_CONFIG, counter);

        final StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(props)));

        final KTable<String, String> t1 = builder.table("input1", Consumed.as("input1")); // 1
        final KTable<String, String> t2 = builder.table("input2", Consumed.as("input2")); // 2

        t1.outerJoin(t2, (v1, v2) -> v1 + v2, Named.as("join-processor"), Materialized.as("the_join")) // 3 (this), 4 (other), 5 (merger)
                .toStream(Named.as("toStream")) // 6
                .to("output", Produced.as("sink"));

        builder.build();
        assertThat(counter.numWrappedProcessors(), CoreMatchers.is(6));
        assertThat(counter.wrappedProcessorNames().toString(), counter.wrappedProcessorNames(), Matchers.containsInAnyOrder(
                "input1",
                "input2",
                "join-processor-join-this",
                "join-processor-join-other",
                "join-processor",
                "toStream"
        ));

        assertThat(counter.numUniqueStateStores(), CoreMatchers.is(3)); // table1, table2, join materialized
        assertThat(counter.numConnectedStateStores(), CoreMatchers.is(3));
    }

    @Test
    public void shouldAllowStreamsFromSameTopic() {
        builder.stream("topic");
        builder.stream("topic");
        assertBuildDoesNotThrow(builder);
    }

    @Test
    public void shouldAllowSubscribingToSamePattern() {
        builder.stream(Pattern.compile("some-regex"));
        builder.stream(Pattern.compile("some-regex"));
        assertBuildDoesNotThrow(builder);
    }

    @Test
    public void shouldAllowReadingFromSameCollectionOfTopics() {
        builder.stream(asList("topic1", "topic2"));
        builder.stream(asList("topic2", "topic1"));
        assertBuildDoesNotThrow(builder);
    }

    @Test
    public void shouldNotAllowReadingFromOverlappingAndUnequalCollectionOfTopics() {
        builder.stream(Collections.singletonList("topic"));
        builder.stream(asList("topic", "anotherTopic"));
        assertThrows(TopologyException.class, builder::build);
    }

    @Test
    public void shouldThrowWhenSubscribedToATopicWithDifferentResetPolicies() {
        builder.stream("topic", Consumed.with(AutoOffsetReset.EARLIEST));
        builder.stream("topic", Consumed.with(AutoOffsetReset.LATEST));
        assertThrows(TopologyException.class, builder::build);
    }

    @Test
    public void shouldThrowWhenSubscribedToATopicWithSetAndUnsetResetPolicies() {
        builder.stream("topic", Consumed.with(AutoOffsetReset.EARLIEST));
        builder.stream("topic");
        assertThrows(TopologyException.class, builder::build);
    }

    @Test
    public void shouldThrowWhenSubscribedToATopicWithUnsetAndSetResetPolicies() {
        builder.stream("another-topic");
        builder.stream("another-topic", Consumed.with(AutoOffsetReset.LATEST));
        assertThrows(TopologyException.class, builder::build);
    }

    @Test
    public void shouldThrowWhenSubscribedToAPatternWithDifferentResetPolicies() {
        builder.stream(Pattern.compile("some-regex"), Consumed.with(AutoOffsetReset.EARLIEST));
        builder.stream(Pattern.compile("some-regex"), Consumed.with(AutoOffsetReset.LATEST));
        assertThrows(TopologyException.class, builder::build);
    }

    @Test
    public void shouldThrowWhenSubscribedToAPatternWithSetAndUnsetResetPolicies() {
        builder.stream(Pattern.compile("some-regex"), Consumed.with(AutoOffsetReset.EARLIEST));
        builder.stream(Pattern.compile("some-regex"));
        assertThrows(TopologyException.class, builder::build);
    }

    @Test
    public void shouldNotAllowTablesFromSameTopic() {
        builder.table("topic");
        builder.table("topic");
        assertThrows(TopologyException.class, builder::build);
    }

    @Test
    public void shouldNowAllowStreamAndTableFromSameTopic() {
        builder.stream("topic");
        builder.table("topic");
        assertThrows(TopologyException.class, builder::build);
    }

    private static void assertBuildDoesNotThrow(final StreamsBuilder builder) {
        try {
            builder.build();
        } catch (final TopologyException topologyException) {
            fail("TopologyException not expected");
        }
    }

    private static void assertNamesForOperation(final ProcessorTopology topology, final String... expected) {
        final List<ProcessorNode<?, ?, ?, ?>> processors = topology.processors();
        assertEquals(expected.length, processors.size(), "Invalid number of expected processors");
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], processors.get(i).name());
        }
    }

    private static void assertNamesForStateStore(final List<StateStore> stores, final String... expected) {
        assertEquals(expected.length, stores.size(), "Invalid number of expected state stores");
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], stores.get(i).name());
        }
    }

    private static void assertTypesForStateStore(final List<StateStore> stores, final Class<?>... expected) {
        assertEquals(expected.length, stores.size(), "Invalid number of expected state stores");
        for (int i = 0; i < expected.length; i++) {
            StateStore store = stores.get(i);
            while (store instanceof WrappedStateStore && !(expected[i].isInstance(store))) {
                store = ((WrappedStateStore<?, ?, ?>) store).wrapped();
            }
            assertThat(store, instanceOf(expected[i]));
        }
    }
}
