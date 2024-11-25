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

import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.ForeignTableJoinProcessorSupplier;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionSendProcessorSupplier;
import org.apache.kafka.streams.kstream.internals.graph.ForeignJoinSubscriptionSendNode;
import org.apache.kafka.streams.kstream.internals.graph.ForeignTableJoinNode;
import org.apache.kafka.streams.kstream.internals.graph.GraphNode;
import org.apache.kafka.streams.kstream.internals.graph.KTableKTableJoinNode;
import org.apache.kafka.streams.kstream.internals.graph.StreamStreamJoinNode;
import org.apache.kafka.streams.kstream.internals.graph.TableFilterNode;
import org.apache.kafka.streams.kstream.internals.graph.TableRepartitionMapNode;
import org.apache.kafka.streams.kstream.internals.graph.WindowedStreamProcessorNode;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.MockMapper;
import org.apache.kafka.test.MockTimestampExtractor;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.StreamsTestUtils;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import static java.time.Duration.ofMillis;
import static java.util.Arrays.asList;
import static org.apache.kafka.streams.Topology.AutoOffsetReset;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class InternalStreamsBuilderTest {

    private static final String APP_ID = "app-id";

    private final InternalStreamsBuilder builder = new InternalStreamsBuilder(new InternalTopologyBuilder());
    private final ConsumedInternal<String, String> consumed = new ConsumedInternal<>();
    private final String storePrefix = "prefix-";
    private final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materialized = new MaterializedInternal<>(Materialized.as("test-store"), builder, storePrefix);
    private final Properties props = StreamsTestUtils.getStreamsConfig();

    @Test
    public void testNewName() {
        assertEquals("X-0000000000", builder.newProcessorName("X-"));
        assertEquals("Y-0000000001", builder.newProcessorName("Y-"));
        assertEquals("Z-0000000002", builder.newProcessorName("Z-"));

        final InternalStreamsBuilder newBuilder = new InternalStreamsBuilder(new InternalTopologyBuilder());

        assertEquals("X-0000000000", newBuilder.newProcessorName("X-"));
        assertEquals("Y-0000000001", newBuilder.newProcessorName("Y-"));
        assertEquals("Z-0000000002", newBuilder.newProcessorName("Z-"));
    }

    @Test
    public void testNewStoreName() {
        assertEquals("X-STATE-STORE-0000000000", builder.newStoreName("X-"));
        assertEquals("Y-STATE-STORE-0000000001", builder.newStoreName("Y-"));
        assertEquals("Z-STATE-STORE-0000000002", builder.newStoreName("Z-"));

        final InternalStreamsBuilder newBuilder = new InternalStreamsBuilder(new InternalTopologyBuilder());

        assertEquals("X-STATE-STORE-0000000000", newBuilder.newStoreName("X-"));
        assertEquals("Y-STATE-STORE-0000000001", newBuilder.newStoreName("Y-"));
        assertEquals("Z-STATE-STORE-0000000002", newBuilder.newStoreName("Z-"));
    }

    @Test
    public void shouldHaveCorrectSourceTopicsForTableFromMergedStream() {
        final String topic1 = "topic-1";
        final String topic2 = "topic-2";
        final String topic3 = "topic-3";
        final KStream<String, String> source1 = builder.stream(Collections.singleton(topic1), consumed);
        final KStream<String, String> source2 = builder.stream(Collections.singleton(topic2), consumed);
        final KStream<String, String> source3 = builder.stream(Collections.singleton(topic3), consumed);
        final KStream<String, String> processedSource1 =
                source1.mapValues(v -> v)
                .filter((k, v) -> true);
        final KStream<String, String> processedSource2 = source2.filter((k, v) -> true);

        final KStream<String, String> merged = processedSource1.merge(processedSource2).merge(source3);
        merged.groupByKey().count(Materialized.as("my-table"));
        builder.buildAndOptimizeTopology();
        final Map<String, List<String>> actual = builder.internalTopologyBuilder.stateStoreNameToFullSourceTopicNames();
        assertEquals(asList("topic-1", "topic-2", "topic-3"), actual.get("my-table"));
    }

    @Test
    public void shouldNotMaterializeSourceKTableIfNotRequired() {
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(Materialized.with(null, null), builder, storePrefix);
        final KTable<String, String> table1 = builder.table("topic2", consumed, materializedInternal);

        builder.buildAndOptimizeTopology();
        final ProcessorTopology topology = builder.internalTopologyBuilder
            .rewriteTopology(new StreamsConfig(StreamsTestUtils.getStreamsConfig(APP_ID)))
            .buildTopology();

        assertEquals(0, topology.stateStores().size());
        assertEquals(0, topology.storeToChangelogTopic().size());
        assertNull(table1.queryableStoreName());
    }
    
    @Test
    public void shouldBuildGlobalTableWithNonQueryableStoreName() {
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(Materialized.with(null, null), builder, storePrefix);

        final GlobalKTable<String, String> table1 = builder.globalTable("topic2", consumed, materializedInternal);

        assertNull(table1.queryableStoreName());
    }

    @Test
    public void shouldBuildGlobalTableWithQueryaIbleStoreName() {
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(Materialized.as("globalTable"), builder, storePrefix);
        final GlobalKTable<String, String> table1 = builder.globalTable("topic2", consumed, materializedInternal);

        assertEquals("globalTable", table1.queryableStoreName());
    }

    @Test
    public void shouldBuildSimpleGlobalTableTopology() {
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(Materialized.as("globalTable"), builder, storePrefix);
        builder.globalTable("table",
                            consumed,
            materializedInternal);

        builder.buildAndOptimizeTopology();
        final ProcessorTopology topology = builder.internalTopologyBuilder
            .rewriteTopology(new StreamsConfig(StreamsTestUtils.getStreamsConfig(APP_ID)))
            .buildGlobalStateTopology();
        final List<StateStore> stateStores = topology.globalStateStores();

        assertEquals(1, stateStores.size());
        assertEquals("globalTable", stateStores.get(0).name());
    }

    @Test
    public void shouldThrowOnVersionedStoreSupplierForGlobalTable() {
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materializedInternal =
                new MaterializedInternal<>(
                        Materialized.as(Stores.persistentVersionedKeyValueStore("store", Duration.ZERO)),
                        builder,
                        storePrefix
                );

        assertThrows(
            TopologyException.class,
            () -> builder.globalTable(
                "table",
                consumed,
                materializedInternal)
        );
    }

    private void doBuildGlobalTopologyWithAllGlobalTables() {
        final ProcessorTopology topology = builder.internalTopologyBuilder
            .rewriteTopology(new StreamsConfig(StreamsTestUtils.getStreamsConfig(APP_ID)))
            .buildGlobalStateTopology();

        final List<StateStore> stateStores = topology.globalStateStores();
        final Set<String> sourceTopics = topology.sourceTopics();

        assertEquals(Set.of("table", "table2"), sourceTopics);
        assertEquals(2, stateStores.size());
    }

    @Test
    public void shouldBuildGlobalTopologyWithAllGlobalTables() {
        {
            final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materializedInternal =
                new MaterializedInternal<>(Materialized.as("global1"), builder, storePrefix);
            builder.globalTable("table", consumed, materializedInternal);
        }
        {
            final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materializedInternal =
                new MaterializedInternal<>(Materialized.as("global2"), builder, storePrefix);
            builder.globalTable("table2", consumed, materializedInternal);
        }

        builder.buildAndOptimizeTopology();
        doBuildGlobalTopologyWithAllGlobalTables();
    }

    @Test
    public void shouldAddGlobalTablesToEachGroup() {
        final String one = "globalTable";
        final String two = "globalTable2";

        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(Materialized.as(one), builder, storePrefix);
        final GlobalKTable<String, String> globalTable = builder.globalTable("table", consumed, materializedInternal);

        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materializedInternal2 =
            new MaterializedInternal<>(Materialized.as(two), builder, storePrefix);
        final GlobalKTable<String, String> globalTable2 = builder.globalTable("table2", consumed, materializedInternal2);

        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materializedInternalNotGlobal =
            new MaterializedInternal<>(Materialized.as("not-global"), builder, storePrefix);
        builder.table("not-global", consumed, materializedInternalNotGlobal);

        final KeyValueMapper<String, String, String> kvMapper = (key, value) -> value;

        final KStream<String, String> stream = builder.stream(Collections.singleton("t1"), consumed);
        stream.leftJoin(globalTable, kvMapper, MockValueJoiner.TOSTRING_JOINER);
        final KStream<String, String> stream2 = builder.stream(Collections.singleton("t2"), consumed);
        stream2.leftJoin(globalTable2, kvMapper, MockValueJoiner.TOSTRING_JOINER);

        final Map<Integer, Set<String>> nodeGroups = builder.internalTopologyBuilder.nodeGroups();
        for (final Integer groupId : nodeGroups.keySet()) {
            final ProcessorTopology topology = builder.internalTopologyBuilder.buildSubtopology(groupId);
            final List<StateStore> stateStores = topology.globalStateStores();
            final Set<String> names = new HashSet<>();
            for (final StateStore stateStore : stateStores) {
                names.add(stateStore.name());
            }

            assertEquals(2, stateStores.size());
            assertTrue(names.contains(one));
            assertTrue(names.contains(two));
        }
    }

    @Test
    public void shouldMapStateStoresToCorrectSourceTopics() {
        final KStream<String, String> playEvents = builder.stream(Collections.singleton("events"), consumed);

        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(Materialized.as("table-store"), builder, storePrefix);
        final KTable<String, String> table = builder.table("table-topic", consumed, materializedInternal);

        final KStream<String, String> mapped = playEvents.map(MockMapper.selectValueKeyValueMapper());
        mapped.leftJoin(table, MockValueJoiner.TOSTRING_JOINER).groupByKey().count(Materialized.as("count"));
        builder.buildAndOptimizeTopology();
        builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(StreamsTestUtils.getStreamsConfig(APP_ID)));
        assertEquals(Collections.singletonList("table-topic"), builder.internalTopologyBuilder.sourceTopicsForStore("table-store"));
        assertEquals(Collections.singletonList(APP_ID + "-KSTREAM-MAP-0000000003-repartition"), builder.internalTopologyBuilder.sourceTopicsForStore("count"));
    }

    @Test
    public void shouldAddTopicToEarliestAutoOffsetResetList() {
        final String topicName = "topic-1";
        final ConsumedInternal<String, String> consumed = new ConsumedInternal<>(Consumed.with(AutoOffsetReset.EARLIEST));
        builder.stream(Collections.singleton(topicName), consumed);
        builder.buildAndOptimizeTopology();

        assertThat(builder.internalTopologyBuilder.offsetResetStrategy(topicName), equalTo(AutoOffsetResetStrategy.EARLIEST));
    }

    @Test
    public void shouldAddTopicToLatestAutoOffsetResetList() {
        final String topicName = "topic-1";

        final ConsumedInternal<String, String> consumed = new ConsumedInternal<>(Consumed.with(AutoOffsetReset.LATEST));
        builder.stream(Collections.singleton(topicName), consumed);
        builder.buildAndOptimizeTopology();
        assertThat(builder.internalTopologyBuilder.offsetResetStrategy(topicName), equalTo(AutoOffsetResetStrategy.LATEST));
    }

    @Test
    public void shouldAddTableToEarliestAutoOffsetResetList() {
        final String topicName = "topic-1";
        builder.table(topicName, new ConsumedInternal<>(Consumed.with(AutoOffsetReset.EARLIEST)), materialized);
        builder.buildAndOptimizeTopology();
        assertThat(builder.internalTopologyBuilder.offsetResetStrategy(topicName), equalTo(AutoOffsetResetStrategy.EARLIEST));
    }

    @Test
    public void shouldAddTableToLatestAutoOffsetResetList() {
        final String topicName = "topic-1";
        builder.table(topicName, new ConsumedInternal<>(Consumed.with(AutoOffsetReset.LATEST)), materialized);
        builder.buildAndOptimizeTopology();
        assertThat(builder.internalTopologyBuilder.offsetResetStrategy(topicName), equalTo(AutoOffsetResetStrategy.LATEST));
    }

    @Test
    public void shouldNotAddTableToOffsetResetLists() {
        final String topicName = "topic-1";

        builder.table(topicName, consumed, materialized);
        builder.buildAndOptimizeTopology();

        assertThat(builder.internalTopologyBuilder.offsetResetStrategy(topicName), equalTo(AutoOffsetResetStrategy.NONE));
    }

    @Test
    public void shouldNotAddRegexTopicsToOffsetResetLists() {
        final Pattern topicPattern = Pattern.compile("topic-\\d");
        final String topic = "topic-5";

        builder.stream(topicPattern, consumed);
        builder.buildAndOptimizeTopology();

        assertThat(builder.internalTopologyBuilder.offsetResetStrategy(topic), equalTo(AutoOffsetResetStrategy.NONE));
    }

    @Test
    public void shouldAddRegexTopicToEarliestAutoOffsetResetList() {
        final Pattern topicPattern = Pattern.compile("topic-\\d+");
        final String topicTwo = "topic-500000";

        builder.stream(topicPattern, new ConsumedInternal<>(Consumed.with(AutoOffsetReset.EARLIEST)));
        builder.buildAndOptimizeTopology();

        assertThat(builder.internalTopologyBuilder.offsetResetStrategy(topicTwo), equalTo(AutoOffsetResetStrategy.EARLIEST));
    }

    @Test
    public void shouldAddRegexTopicToLatestAutoOffsetResetList() {
        final Pattern topicPattern = Pattern.compile("topic-\\d+");
        final String topicTwo = "topic-1000000";

        builder.stream(topicPattern, new ConsumedInternal<>(Consumed.with(AutoOffsetReset.LATEST)));
        builder.buildAndOptimizeTopology();

        assertThat(builder.internalTopologyBuilder.offsetResetStrategy(topicTwo), equalTo(AutoOffsetResetStrategy.LATEST));
    }

    @Test
    public void shouldHaveNullTimestampExtractorWhenNoneSupplied() {
        builder.stream(Collections.singleton("topic"), consumed);
        builder.buildAndOptimizeTopology();
        builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(StreamsTestUtils.getStreamsConfig(APP_ID)));
        final ProcessorTopology processorTopology = builder.internalTopologyBuilder.buildTopology();
        assertNull(processorTopology.source("topic").timestampExtractor());
    }

    @Test
    public void shouldUseProvidedTimestampExtractor() {
        final ConsumedInternal<String, String> consumed = new ConsumedInternal<>(Consumed.with(new MockTimestampExtractor()));
        builder.stream(Collections.singleton("topic"), consumed);
        builder.buildAndOptimizeTopology();
        final ProcessorTopology processorTopology = builder.internalTopologyBuilder
            .rewriteTopology(new StreamsConfig(StreamsTestUtils.getStreamsConfig(APP_ID)))
            .buildTopology();
        assertThat(processorTopology.source("topic").timestampExtractor(), instanceOf(MockTimestampExtractor.class));
    }

    @Test
    public void ktableShouldHaveNullTimestampExtractorWhenNoneSupplied() {
        builder.table("topic", consumed, materialized);
        builder.buildAndOptimizeTopology();
        final ProcessorTopology processorTopology = builder.internalTopologyBuilder
            .rewriteTopology(new StreamsConfig(StreamsTestUtils.getStreamsConfig(APP_ID)))
            .buildTopology();
        assertNull(processorTopology.source("topic").timestampExtractor());
    }

    @Test
    public void ktableShouldUseProvidedTimestampExtractor() {
        final ConsumedInternal<String, String> consumed = new ConsumedInternal<>(Consumed.with(new MockTimestampExtractor()));
        builder.table("topic", consumed, materialized);
        builder.buildAndOptimizeTopology();
        final ProcessorTopology processorTopology = builder.internalTopologyBuilder
            .rewriteTopology(new StreamsConfig(StreamsTestUtils.getStreamsConfig(APP_ID)))
            .buildTopology();
        assertThat(processorTopology.source("topic").timestampExtractor(), instanceOf(MockTimestampExtractor.class));
    }

    @Test
    public void shouldMarkStreamStreamJoinAsSelfJoinSingleStream() {
        // Given:
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        final KStream<String, String> stream = builder.stream(Collections.singleton("t1"), consumed);
        stream.join(stream, MockValueJoiner.TOSTRING_JOINER, JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100)));

        // When:
        builder.buildAndOptimizeTopology(props);

        // Then:
        final GraphNode join = getNodeByType(builder.root, StreamStreamJoinNode.class, new HashSet<>());
        assertNotNull(join);
        assertTrue(((StreamStreamJoinNode) join).getSelfJoin());
        final GraphNode parent = join.parentNodes().stream().findFirst().get();
        final AtomicInteger count = new AtomicInteger();
        countJoinWindowNodes(count, builder.root, new HashSet<>());
        assertEquals(count.get(), 1);
    }

    @Test
    public void shouldMarkStreamStreamJoinAsSelfJoinTwoStreams() {
        // Given:
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        final KStream<String, String> stream1 = builder.stream(Collections.singleton("t1"), consumed);
        final KStream<String, String> stream2 = builder.stream(Collections.singleton("t1"), consumed);
        stream1.join(stream2, MockValueJoiner.TOSTRING_JOINER, JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100)));

        // When:
        builder.buildAndOptimizeTopology(props);

        // Then:
        final GraphNode join = getNodeByType(builder.root, StreamStreamJoinNode.class, new HashSet<>());
        assertNotNull(join);
        assertTrue(((StreamStreamJoinNode) join).getSelfJoin());
        final AtomicInteger count = new AtomicInteger();
        countJoinWindowNodes(count, builder.root, new HashSet<>());
        assertEquals(count.get(), 1);
    }

    @Test
    public void shouldMarkStreamStreamJoinAsSelfJoinMergeTwoStreams() {
        // Given:
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        final KStream<String, String> stream1 = builder.stream(Collections.singleton("t1"), consumed);
        final KStream<String, String> stream2 = builder.stream(Collections.singleton("t2"), consumed);
        final KStream<String, String> stream3 = stream1.merge(stream2);
        stream3.join(stream3, MockValueJoiner.TOSTRING_JOINER, JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100)));

        // When:
        builder.buildAndOptimizeTopology(props);

        // Then:
        final GraphNode join = getNodeByType(builder.root, StreamStreamJoinNode.class, new HashSet<>());
        assertNotNull(join);
        assertTrue(((StreamStreamJoinNode) join).getSelfJoin());
        final AtomicInteger count = new AtomicInteger();
        countJoinWindowNodes(count, builder.root, new HashSet<>());
        assertEquals(count.get(), 1);
    }

    @Test
    public void shouldMarkFirstStreamStreamJoinAsSelfJoin3WayJoin() {
        // Given:
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        final KStream<String, String> stream1 = builder.stream(Collections.singleton("t1"), consumed);
        final KStream<String, String> stream2 = builder.stream(Collections.singleton("t1"), consumed);
        final KStream<String, String> stream3 = builder.stream(Collections.singleton("t3"), consumed);
        stream1
            .join(stream2, MockValueJoiner.TOSTRING_JOINER, JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100)))
            .join(stream3, MockValueJoiner.TOSTRING_JOINER, JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100)));

        // When:
        builder.buildAndOptimizeTopology(props);

        // Then:
        final List<GraphNode> result = new ArrayList<>();
        getNodesByType(builder.root, StreamStreamJoinNode.class, new HashSet<>(), result);
        assertEquals(result.size(), 2);
        assertTrue(((StreamStreamJoinNode) result.get(0)).getSelfJoin());
        assertFalse(((StreamStreamJoinNode) result.get(1)).getSelfJoin());
        final AtomicInteger count = new AtomicInteger();
        countJoinWindowNodes(count, builder.root, new HashSet<>());
        assertEquals(count.get(), 3);
    }

    @Test
    public void shouldMarkAllStreamStreamJoinsAsSelfJoin() {
        // Given:
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        final KStream<String, String> stream1 = builder.stream(Collections.singleton("t1"), consumed);
        final KStream<String, String> stream2 = builder.stream(Collections.singleton("t1"), consumed);
        final KStream<String, String> stream3 = builder.stream(Collections.singleton("t2"), consumed);
        final KStream<String, String> stream4 = builder.stream(Collections.singleton("t2"), consumed);

        final KStream<String, String> firstResult =
            stream1.join(stream2, MockValueJoiner.TOSTRING_JOINER, JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100)));
        final KStream<String, String> secondResult =
            stream3.join(stream4, MockValueJoiner.TOSTRING_JOINER, JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100)));
        firstResult.merge(secondResult);

        // When:
        builder.buildAndOptimizeTopology(props);

        // Then:
        final List<GraphNode> result = new ArrayList<>();
        getNodesByType(builder.root, StreamStreamJoinNode.class, new HashSet<>(), result);
        assertEquals(result.size(), 2);
        assertTrue(((StreamStreamJoinNode) result.get(0)).getSelfJoin());
        assertTrue(((StreamStreamJoinNode) result.get(1)).getSelfJoin());
        final AtomicInteger count = new AtomicInteger();
        countJoinWindowNodes(count, builder.root, new HashSet<>());
        assertEquals(count.get(), 2);
    }

    @Test
    public void shouldMarkFirstStreamStreamJoinAsSelfJoinNwaySameSource() {
        // Given:
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        final KStream<String, String> stream1 = builder.stream(Collections.singleton("t1"), consumed);
        final KStream<String, String> stream2 = builder.stream(Collections.singleton("t1"), consumed);
        final KStream<String, String> stream3 = builder.stream(Collections.singleton("t1"), consumed);

        stream1
            .join(stream2, MockValueJoiner.TOSTRING_JOINER, JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100)))
            .join(stream3, MockValueJoiner.TOSTRING_JOINER, JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100)));

        // When:
        builder.buildAndOptimizeTopology(props);

        // Then:
        final List<GraphNode> joinNodes = new ArrayList<>();
        getNodesByType(builder.root, StreamStreamJoinNode.class, new HashSet<>(), joinNodes);
        assertEquals(joinNodes.size(), 2);
        assertTrue(((StreamStreamJoinNode) joinNodes.get(0)).getSelfJoin());
        assertFalse(((StreamStreamJoinNode) joinNodes.get(1)).getSelfJoin());
        final AtomicInteger count = new AtomicInteger();
        countJoinWindowNodes(count, builder.root, new HashSet<>());
        assertEquals(count.get(), 3);
    }

    @Test
    public void shouldMarkFirstStreamStreamJoinAsSelfJoinNway() {
        // Given:
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        final KStream<String, String> stream1 = builder.stream(Collections.singleton("t1"), consumed);
        final KStream<String, String> stream2 = builder.stream(Collections.singleton("t1"), consumed);
        final KStream<String, String> stream3 = builder.stream(Collections.singleton("t2"), consumed);

        stream1
            .join(stream2, MockValueJoiner.TOSTRING_JOINER, JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100)))
            .join(stream3, MockValueJoiner.TOSTRING_JOINER, JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100)));

        // When:
        builder.buildAndOptimizeTopology(props);

        // Then:
        final List<GraphNode> joinNodes = new ArrayList<>();
        getNodesByType(builder.root, StreamStreamJoinNode.class, new HashSet<>(), joinNodes);
        assertEquals(joinNodes.size(), 2);
        assertTrue(((StreamStreamJoinNode) joinNodes.get(0)).getSelfJoin());
        assertFalse(((StreamStreamJoinNode) joinNodes.get(1)).getSelfJoin());
        final AtomicInteger count = new AtomicInteger();
        countJoinWindowNodes(count, builder.root, new HashSet<>());
        assertEquals(count.get(), 3);
    }


    @Test
    public void shouldMarkStreamStreamJoinAsSelfJoinTwoStreamsWithNoOpFilter() {
        // Given:
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        final KStream<String, String> stream1 = builder.stream(Collections.singleton("t1"), consumed);
        final KStream<String, String> stream2 = builder.stream(Collections.singleton("t1"), consumed);
        stream1.filter((key, value) -> value != null);
        stream1.join(stream2, MockValueJoiner.TOSTRING_JOINER, JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100)));

        // When:
        builder.buildAndOptimizeTopology(props);

        // Then:
        final GraphNode join = getNodeByType(builder.root, StreamStreamJoinNode.class, new HashSet<>());
        assertNotNull(join);
        assertTrue(((StreamStreamJoinNode) join).getSelfJoin());
        final AtomicInteger count = new AtomicInteger();
        countJoinWindowNodes(count, builder.root, new HashSet<>());
        assertEquals(count.get(), 1);
    }

    @Test
    public void shouldMarkStreamStreamJoinAsSelfJoinTwoJoinsSameSource() {
        // Given:
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        final KStream<String, String> stream1 = builder.stream(Collections.singleton("t1"), consumed);
        final KStream<String, String> stream2 = builder.stream(Collections.singleton("t1"), consumed);
        final KStream<String, String> stream3 = builder.stream(Collections.singleton("t1"), consumed);
        final KStream<String, String> stream4 = builder.stream(Collections.singleton("t1"), consumed);
        stream1.join(stream2, MockValueJoiner.TOSTRING_JOINER, JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100)));
        stream3.join(stream4, MockValueJoiner.TOSTRING_JOINER, JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100)));

        // When:
        builder.buildAndOptimizeTopology(props);

        // Then:
        final List<GraphNode> joinNodes = new ArrayList<>();
        getNodesByType(builder.root, StreamStreamJoinNode.class, new HashSet<>(), joinNodes);
        assertEquals(joinNodes.size(), 2);
        assertTrue(((StreamStreamJoinNode) joinNodes.get(0)).getSelfJoin());
        assertTrue(((StreamStreamJoinNode) joinNodes.get(1)).getSelfJoin());
        final AtomicInteger count = new AtomicInteger();
        countJoinWindowNodes(count, builder.root, new HashSet<>());
        assertEquals(count.get(), 2);
    }

    /**
     * The join node has two parents and the graph looks like:
     * root ---> source (t1)
     * source ---> filter , windowed-4, join
     * filter ---> windowed-3, join
     */
    @Test
    public void shouldNotMarkStreamStreamJoinAsSelfJoinTwoStreamsWithFilter() {
        // Given:
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        final KStream<String, String> stream1 = builder.stream(Collections.singleton("t1"), consumed);
        final KStream<String, String> stream2 = builder.stream(Collections.singleton("t1"), consumed);
        stream1
            .filter((key, value) -> value != null)
            .join(stream2, MockValueJoiner.TOSTRING_JOINER, JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100)));

        // When:
        builder.buildAndOptimizeTopology(props);

        // Then:
        final GraphNode join = getNodeByType(builder.root, StreamStreamJoinNode.class, new HashSet<>());
        assertNotNull(join);
        assertFalse(((StreamStreamJoinNode) join).getSelfJoin());
        final AtomicInteger count = new AtomicInteger();
        countJoinWindowNodes(count, builder.root, new HashSet<>());
        assertEquals(count.get(), 2);
    }

    /**
     * The join node has two parents and the graph looks like:
     * root ---> source (t1)
     * source ---> map , windowed-4, join
     * map ---> windowed-3, join
     */
    @Test
    public void shouldNotMarkStreamStreamJoinAsSelfJoinOneStreamWithMap() {
        // Given:
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        final KStream<String, String> stream1 = builder.stream(Collections.singleton("t1"), consumed);
        final KStream<String, String> stream3 = stream1.mapValues(v -> v);
        final KStream<String, String> stream2 = builder.stream(Collections.singleton("t1"), consumed);
        stream3.join(stream2, MockValueJoiner.TOSTRING_JOINER, JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100)));

        // When:
        builder.buildAndOptimizeTopology(props);

        // Then:
        final GraphNode join = getNodeByType(builder.root, StreamStreamJoinNode.class, new HashSet<>());
        assertNotNull(join);
        assertFalse(((StreamStreamJoinNode) join).getSelfJoin());
        final AtomicInteger count = new AtomicInteger();
        countJoinWindowNodes(count, builder.root, new HashSet<>());
        assertEquals(count.get(), 2);
    }

    @Test
    public void shouldNotMarkStreamStreamJoinAsSelfJoinMultipleSources() {
        // Given:
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        final KStream<String, String> stream1 = builder.stream(Collections.singleton("t1"), consumed);
        final KStream<String, String> stream2 = builder.stream(Collections.singleton("t2"), consumed);
        stream1.join(stream2, MockValueJoiner.TOSTRING_JOINER, JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100)));

        // When:
        builder.buildAndOptimizeTopology(props);

        // Then:
        final GraphNode join = getNodeByType(builder.root, StreamStreamJoinNode.class, new HashSet<>());
        assertNotNull(join);
        assertFalse(((StreamStreamJoinNode) join).getSelfJoin());
        final AtomicInteger count = new AtomicInteger();
        countJoinWindowNodes(count, builder.root, new HashSet<>());
        assertEquals(count.get(), 2);
    }

    @Test
    public void shouldOptimizeJoinWhenInConfig() {
        // Given:
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.SINGLE_STORE_SELF_JOIN);
        final KStream<String, String> stream1 = builder.stream(Collections.singleton("t1"), consumed);
        stream1.join(stream1, MockValueJoiner.TOSTRING_JOINER, JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100)));

        // When:
        builder.buildAndOptimizeTopology(props);

        // Then:
        final GraphNode join = getNodeByType(builder.root, StreamStreamJoinNode.class, new HashSet<>());
        assertNotNull(join);
        assertTrue(((StreamStreamJoinNode) join).getSelfJoin());
        final AtomicInteger count = new AtomicInteger();
        countJoinWindowNodes(count, builder.root, new HashSet<>());
        assertEquals(count.get(), 1);
    }

    @Test
    public void shouldNotOptimizeJoinWhenNotInConfig() {
        // Given:
        final String value = String.join(",",
                                         StreamsConfig.REUSE_KTABLE_SOURCE_TOPICS,
                                         StreamsConfig.MERGE_REPARTITION_TOPICS);
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, value);
        final KStream<String, String> stream1 = builder.stream(Collections.singleton("t1"), consumed);
        stream1.join(stream1, MockValueJoiner.TOSTRING_JOINER, JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100)));

        // When:
        builder.buildAndOptimizeTopology(props);

        // Then:
        final GraphNode join = getNodeByType(builder.root, StreamStreamJoinNode.class, new HashSet<>());
        assertNotNull(join);
        assertFalse(((StreamStreamJoinNode) join).getSelfJoin());
        final AtomicInteger count = new AtomicInteger();
        countJoinWindowNodes(count, builder.root, new HashSet<>());
        assertEquals(count.get(), 2);
    }

    @Test
    public void shouldSetUseVersionedSemanticsOnTableFilter() {
        // Given:
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(Materialized.as(Stores.persistentVersionedKeyValueStore("store", Duration.ofMinutes(5))), builder, storePrefix);
        final KTable<String, String> table1 = builder.table("t1", consumed, materializedInternal);
        table1.filter((k, v) -> v != null);

        // When:
        builder.buildAndOptimizeTopology();

        // Then:
        final GraphNode filter = getNodeByType(builder.root, TableFilterNode.class, new HashSet<>());
        assertNotNull(filter);
        verifyVersionedSemantics((TableFilterNode<?, ?>) filter, true);
    }

    @Test
    public void shouldSetUseVersionedSemanticsWithIntermediateNode() {
        // Given:
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> versionedMaterialize =
            new MaterializedInternal<>(Materialized.as(Stores.persistentVersionedKeyValueStore("versioned", Duration.ofMinutes(5))), builder, storePrefix);
        final KTable<String, String> table1 = builder.table("t1", consumed, versionedMaterialize);
        final KTable<String, String> table2 = table1.mapValues(v -> v != null ? v + v : null);
        table2.filter((k, v) -> v != null);

        // When:
        builder.buildAndOptimizeTopology();

        // Then:
        final GraphNode filter = getNodeByType(builder.root, TableFilterNode.class, new HashSet<>());
        assertNotNull(filter);
        verifyVersionedSemantics((TableFilterNode<?, ?>) filter, true);
    }

    @Test
    public void shouldNotSetUseVersionedSemanticsWithMaterializedIntermediateNode() {
        // Given:
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> versionedMaterialize =
            new MaterializedInternal<>(Materialized.as(Stores.persistentVersionedKeyValueStore("versioned", Duration.ofMinutes(5))), builder, storePrefix);
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> unversionedMaterialize =
            new MaterializedInternal<>(Materialized.as("unversioned"), builder, storePrefix);
        final KTable<String, String> table1 = builder.table("t1", consumed, versionedMaterialize);
        final KTable<String, String> table2 = table1.mapValues(v -> v != null ? v + v : null, unversionedMaterialize);
        table2.filter((k, v) -> v != null);

        // When:
        builder.buildAndOptimizeTopology();

        // Then:
        final GraphNode filter = getNodeByType(builder.root, TableFilterNode.class, new HashSet<>());
        assertNotNull(filter);
        verifyVersionedSemantics((TableFilterNode<?, ?>) filter, false);
    }

    @Test
    public void shouldSetUseVersionedSemanticsWithIntermediateNodeMaterializedAsVersioned() {
        // Given:
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> versionedMaterialize =
            new MaterializedInternal<>(Materialized.as(Stores.persistentVersionedKeyValueStore("versioned", Duration.ofMinutes(5))), builder, storePrefix);
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> versionedMaterialize2 =
            new MaterializedInternal<>(Materialized.as(Stores.persistentVersionedKeyValueStore("versioned2", Duration.ofMinutes(5))), builder, storePrefix);
        final KTable<String, String> table1 = builder.table("t1", consumed, versionedMaterialize);
        final KTable<String, String> table2 = table1.mapValues(v -> v != null ? v + v : null, versionedMaterialize2);
        table2.filter((k, v) -> v != null);

        // When:
        builder.buildAndOptimizeTopology();

        // Then:
        final GraphNode filter = getNodeByType(builder.root, TableFilterNode.class, new HashSet<>());
        assertNotNull(filter);
        verifyVersionedSemantics((TableFilterNode<?, ?>) filter, true);
    }

    @Test
    public void shouldNotSetUseVersionedSemanticsWithIntermediateAggregation() {
        // Given:
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> versionedMaterialize =
            new MaterializedInternal<>(Materialized.as(Stores.persistentVersionedKeyValueStore("versioned", Duration.ofMinutes(5))), builder, storePrefix);
        final KTable<String, String> table1 = builder.table("t1", consumed, versionedMaterialize);
        final KTable<String, Long> table2 = table1.groupBy(KeyValue::new).count();
        table2.filter((k, v) -> v != null);

        // When:
        builder.buildAndOptimizeTopology();

        // Then:
        final GraphNode filter = getNodeByType(builder.root, TableFilterNode.class, new HashSet<>());
        assertNotNull(filter);
        verifyVersionedSemantics((TableFilterNode<?, ?>) filter, false);
    }

    @Test
    public void shouldSetUseVersionedSemanticsWithIntermediateAggregationMaterializedAsVersioned() {
        // Given:
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> versionedMaterialize =
            new MaterializedInternal<>(Materialized.as(Stores.persistentVersionedKeyValueStore("versioned", Duration.ofMinutes(5))), builder, storePrefix);
        final MaterializedInternal<String, Long, KeyValueStore<Bytes, byte[]>> versionedMaterialize2 =
            new MaterializedInternal<>(Materialized.as(Stores.persistentVersionedKeyValueStore("versioned2", Duration.ofMinutes(5))), builder, storePrefix);
        final KTable<String, String> table1 = builder.table("t1", consumed, versionedMaterialize);
        final KTable<String, Long> table2 = table1.groupBy(KeyValue::new).count(versionedMaterialize2);
        table2.filter((k, v) -> v != null);

        // When:
        builder.buildAndOptimizeTopology();

        // Then:
        final GraphNode filter = getNodeByType(builder.root, TableFilterNode.class, new HashSet<>());
        assertNotNull(filter);
        verifyVersionedSemantics((TableFilterNode<?, ?>) filter, true);
    }

    @Test
    public void shouldNotSetUseVersionedSemanticsWithIntermediateJoin() {
        // Given:
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> versionedMaterialize =
            new MaterializedInternal<>(Materialized.as(Stores.persistentVersionedKeyValueStore("versioned", Duration.ofMinutes(5))), builder, storePrefix);
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> versionedMaterialize2 =
            new MaterializedInternal<>(Materialized.as(Stores.persistentVersionedKeyValueStore("versioned2", Duration.ofMinutes(5))), builder, storePrefix);
        final KTable<String, String> table1 = builder.table("t1", consumed, versionedMaterialize);
        final KTable<String, String> table2 = builder.table("t2", consumed, versionedMaterialize2);
        final KTable<String, String> table3 = table1.join(table2, (v1, v2) -> v1 + v2);
        table3.filter((k, v) -> v != null);

        // When:
        builder.buildAndOptimizeTopology();

        // Then:
        final GraphNode filter = getNodeByType(builder.root, TableFilterNode.class, new HashSet<>());
        assertNotNull(filter);
        verifyVersionedSemantics((TableFilterNode<?, ?>) filter, false);
    }

    // not recommended to materialize join result as versioned since semantics are not correct,
    // but this test is included anyway for completeness
    @Test
    public void shouldSetUseVersionedSemanticsWithIntermediateJoinMaterializedAsVersioned() {
        // Given:
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> versionedMaterialize =
            new MaterializedInternal<>(Materialized.as(Stores.persistentVersionedKeyValueStore("versioned", Duration.ofMinutes(5))), builder, storePrefix);
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> versionedMaterialize2 =
            new MaterializedInternal<>(Materialized.as(Stores.persistentVersionedKeyValueStore("versioned2", Duration.ofMinutes(5))), builder, storePrefix);
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> versionedMaterialize3 =
            new MaterializedInternal<>(Materialized.as(Stores.persistentVersionedKeyValueStore("versioned3", Duration.ofMinutes(5))), builder, storePrefix);
        final KTable<String, String> table1 = builder.table("t1", consumed, versionedMaterialize);
        final KTable<String, String> table2 = builder.table("t2", consumed, versionedMaterialize2);
        final KTable<String, String> table3 = table1.join(table2, (v1, v2) -> v1 + v2, versionedMaterialize3);
        table3.filter((k, v) -> v != null);

        // When:
        builder.buildAndOptimizeTopology();

        // Then:
        final GraphNode filter = getNodeByType(builder.root, TableFilterNode.class, new HashSet<>());
        assertNotNull(filter);
        verifyVersionedSemantics((TableFilterNode<?, ?>) filter, true);
    }

    @Test
    public void shouldNotSetUseVersionedSemanticsWithIntermediateForeignKeyJoin() {
        // Given:
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> versionedMaterialize =
            new MaterializedInternal<>(Materialized.as(Stores.persistentVersionedKeyValueStore("versioned", Duration.ofMinutes(5))), builder, storePrefix);
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> versionedMaterialize2 =
            new MaterializedInternal<>(Materialized.as(Stores.persistentVersionedKeyValueStore("versioned2", Duration.ofMinutes(5))), builder, storePrefix);
        final KTable<String, String> table1 = builder.table("t1", consumed, versionedMaterialize);
        final KTable<String, String> table2 = builder.table("t2", consumed, versionedMaterialize2);
        final KTable<String, String> table3 = table1.join(table2, v -> v, (v1, v2) -> v1 + v2);
        table3.filter((k, v) -> v != null);

        // When:
        builder.buildAndOptimizeTopology();

        // Then:
        final GraphNode filter = getNodeByType(builder.root, TableFilterNode.class, new HashSet<>());
        assertNotNull(filter);
        verifyVersionedSemantics((TableFilterNode<?, ?>) filter, false);
    }

    // not recommended to materialize join result as versioned since semantics are not correct,
    // but this test is included anyway for completeness
    @Test
    public void shouldSetUseVersionedSemanticsWithIntermediateForeignKeyJoinMaterializedAsVersioned() {
        // Given:
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> versionedMaterialize =
            new MaterializedInternal<>(Materialized.as(Stores.persistentVersionedKeyValueStore("versioned", Duration.ofMinutes(5))), builder, storePrefix);
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> versionedMaterialize2 =
            new MaterializedInternal<>(Materialized.as(Stores.persistentVersionedKeyValueStore("versioned2", Duration.ofMinutes(5))), builder, storePrefix);
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> versionedMaterialize3 =
            new MaterializedInternal<>(Materialized.as(Stores.persistentVersionedKeyValueStore("versioned3", Duration.ofMinutes(5))), builder, storePrefix);
        final KTable<String, String> table1 = builder.table("t1", consumed, versionedMaterialize);
        final KTable<String, String> table2 = builder.table("t2", consumed, versionedMaterialize2);
        final KTable<String, String> table3 = table1.join(table2, v -> v, (v1, v2) -> v1 + v2, versionedMaterialize3);
        table3.filter((k, v) -> v != null);

        // When:
        builder.buildAndOptimizeTopology();

        // Then:
        final GraphNode filter = getNodeByType(builder.root, TableFilterNode.class, new HashSet<>());
        assertNotNull(filter);
        verifyVersionedSemantics((TableFilterNode<?, ?>) filter, true);
    }

    @Test
    public void shouldNotSetUseVersionedSemanticsWithToStreamAndBack() {
        // Given:
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> versionedMaterialize =
            new MaterializedInternal<>(Materialized.as(Stores.persistentVersionedKeyValueStore("versioned", Duration.ofMinutes(5))), builder, storePrefix);
        final KTable<String, String> table1 = builder.table("t1", consumed, versionedMaterialize);
        final KTable<String, String> table2 = table1.toStream().toTable();
        table2.filter((k, v) -> v != null);

        // When:
        builder.buildAndOptimizeTopology();

        // Then:
        final GraphNode filter = getNodeByType(builder.root, TableFilterNode.class, new HashSet<>());
        assertNotNull(filter);
        verifyVersionedSemantics((TableFilterNode<?, ?>) filter, false);
    }

    @Test
    public void shouldSetUseVersionedSemanticsWithToStreamAndBackIfMaterializedAsVersioned() {
        // Given:
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> versionedMaterialize =
            new MaterializedInternal<>(Materialized.as(Stores.persistentVersionedKeyValueStore("versioned", Duration.ofMinutes(5))), builder, storePrefix);
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> versionedMaterialize2 =
            new MaterializedInternal<>(Materialized.as(Stores.persistentVersionedKeyValueStore("versioned2", Duration.ofMinutes(5))), builder, storePrefix);
        final KTable<String, String> table1 = builder.table("t1", consumed, versionedMaterialize);
        final KTable<String, String> table2 = table1.toStream().toTable(versionedMaterialize2);
        table2.filter((k, v) -> v != null);

        // When:
        builder.buildAndOptimizeTopology();

        // Then:
        final GraphNode filter = getNodeByType(builder.root, TableFilterNode.class, new HashSet<>());
        assertNotNull(filter);
        verifyVersionedSemantics((TableFilterNode<?, ?>) filter, true);
    }

    @Test
    public void shouldSetUseVersionedSemanticsOnTableRepartitionMap() {
        // Given:
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> versionedMaterialize =
            new MaterializedInternal<>(Materialized.as(Stores.persistentVersionedKeyValueStore("versioned", Duration.ofMinutes(5))), builder, storePrefix);
        final KTable<String, String> table1 = builder.table("t1", consumed, versionedMaterialize);
        table1.groupBy(KeyValue::new).count();

        // When:
        builder.buildAndOptimizeTopology();

        // Then:
        final GraphNode repartitionMap = getNodeByType(builder.root, TableRepartitionMapNode.class, new HashSet<>());
        assertNotNull(repartitionMap);
        verifyVersionedSemantics((TableRepartitionMapNode<?, ?>) repartitionMap, true);
    }

    @Test
    public void shouldSetUseVersionedSemanticsOnTableRepartitionMapWithIntermediateNodes() {
        // Given:
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> versionedMaterialize =
            new MaterializedInternal<>(Materialized.as(Stores.persistentVersionedKeyValueStore("versioned", Duration.ofMinutes(5))), builder, storePrefix);
        final KTable<String, String> table1 = builder.table("t1", consumed, versionedMaterialize);
        final KTable<String, String> table2 = table1.filter((k, v) -> v != null).mapValues(v -> v + v);
        table2.groupBy(KeyValue::new).count();

        // When:
        builder.buildAndOptimizeTopology();

        // Then:
        final GraphNode repartitionMap = getNodeByType(builder.root, TableRepartitionMapNode.class, new HashSet<>());
        assertNotNull(repartitionMap);
        verifyVersionedSemantics((TableRepartitionMapNode<?, ?>) repartitionMap, true);
    }

    @Test
    public void shouldSetUseVersionedSemanticsOnTableJoin() {
        // Given:
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> versionedMaterialize =
            new MaterializedInternal<>(Materialized.as(Stores.persistentVersionedKeyValueStore("versioned", Duration.ofMinutes(5))), builder, storePrefix);
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> versionedMaterialize2 =
            new MaterializedInternal<>(Materialized.as(Stores.persistentVersionedKeyValueStore("versioned2", Duration.ofMinutes(5))), builder, storePrefix);
        final KTable<String, String> table1 = builder.table("t1", consumed, versionedMaterialize);
        final KTable<String, String> table2 = builder.table("t2", consumed, versionedMaterialize2);
        table1.join(table2, (v1, v2) -> v1 + v2);

        // When:
        builder.buildAndOptimizeTopology();

        // Then:
        final GraphNode join = getNodeByType(builder.root, KTableKTableJoinNode.class, new HashSet<>());
        assertNotNull(join);
        verifyVersionedSemantics((KTableKTableJoinNode<?, ?, ?, ?>) join, true, true);
    }

    @Test
    public void shouldSetUseVersionedSemanticsOnTableJoinLeftOnly() {
        // Given:
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> versionedMaterialize =
            new MaterializedInternal<>(Materialized.as(Stores.persistentVersionedKeyValueStore("versioned", Duration.ofMinutes(5))), builder, storePrefix);
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> unversionedMaterialize =
            new MaterializedInternal<>(Materialized.as("unversioned"), builder, storePrefix);
        final KTable<String, String> table1 = builder.table("t1", consumed, versionedMaterialize);
        final KTable<String, String> table2 = builder.table("t2", consumed, unversionedMaterialize);
        table1.join(table2, (v1, v2) -> v1 + v2);

        // When:
        builder.buildAndOptimizeTopology();

        // Then:
        final GraphNode join = getNodeByType(builder.root, KTableKTableJoinNode.class, new HashSet<>());
        assertNotNull(join);
        verifyVersionedSemantics((KTableKTableJoinNode<?, ?, ?, ?>) join, true, false);
    }

    @Test
    public void shouldSetUseVersionedSemanticsOnTableJoinRightOnly() {
        // Given:
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> versionedMaterialize =
            new MaterializedInternal<>(Materialized.as(Stores.persistentVersionedKeyValueStore("versioned", Duration.ofMinutes(5))), builder, storePrefix);
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> unversionedMaterialize =
            new MaterializedInternal<>(Materialized.as("unversioned"), builder, storePrefix);
        final KTable<String, String> table1 = builder.table("t1", consumed, unversionedMaterialize);
        final KTable<String, String> table2 = builder.table("t2", consumed, versionedMaterialize);
        table1.join(table2, (v1, v2) -> v1 + v2);

        // When:
        builder.buildAndOptimizeTopology();

        // Then:
        final GraphNode join = getNodeByType(builder.root, KTableKTableJoinNode.class, new HashSet<>());
        assertNotNull(join);
        verifyVersionedSemantics((KTableKTableJoinNode<?, ?, ?, ?>) join, false, true);
    }

    @Test
    public void shouldSetUseVersionedSemanticsOnTableSelfJoin() {
        // Given:
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> versionedMaterialize =
            new MaterializedInternal<>(Materialized.as(Stores.persistentVersionedKeyValueStore("versioned", Duration.ofMinutes(5))), builder, storePrefix);
        final KTable<String, String> table1 = builder.table("t1", consumed, versionedMaterialize);
        table1.join(table1, (v1, v2) -> v1 + v2);

        // When:
        builder.buildAndOptimizeTopology();

        // Then:
        final GraphNode join = getNodeByType(builder.root, KTableKTableJoinNode.class, new HashSet<>());
        assertNotNull(join);
        verifyVersionedSemantics((KTableKTableJoinNode<?, ?, ?, ?>) join, true, true);
    }

    @Test
    public void shouldSetUseVersionedSemanticsOnTableForeignJoin() {
        // Given:
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> versionedMaterialize =
            new MaterializedInternal<>(Materialized.as(Stores.persistentVersionedKeyValueStore("versioned", Duration.ofMinutes(5))), builder, storePrefix);
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> versionedMaterialize2 =
            new MaterializedInternal<>(Materialized.as(Stores.persistentVersionedKeyValueStore("versioned2", Duration.ofMinutes(5))), builder, storePrefix);
        final KTable<String, String> table1 = builder.table("t1", consumed, versionedMaterialize);
        final KTable<String, String> table2 = builder.table("t2", consumed, versionedMaterialize2);
        table1.join(table2, v -> v, (v1, v2) -> v1 + v2);

        // When:
        builder.buildAndOptimizeTopology();

        // Then:
        final GraphNode joinThis = getNodeByType(builder.root, ForeignJoinSubscriptionSendNode.class, new HashSet<>());
        assertNotNull(joinThis);
        verifyVersionedSemantics((ForeignJoinSubscriptionSendNode<?, ?>) joinThis, true);

        final GraphNode joinOther = getNodeByType(builder.root, ForeignTableJoinNode.class, new HashSet<>());
        assertNotNull(joinOther);
        verifyVersionedSemantics((ForeignTableJoinNode<?, ?>) joinOther, true);
    }

    @Test
    public void shouldSetUseVersionedSemanticsOnTableForeignJoinLeftOnly() {
        // Given:
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> versionedMaterialize =
            new MaterializedInternal<>(Materialized.as(Stores.persistentVersionedKeyValueStore("versioned", Duration.ofMinutes(5))), builder, storePrefix);
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> unversionedMaterialize =
            new MaterializedInternal<>(Materialized.as("unversioned"), builder, storePrefix);
        final KTable<String, String> table1 = builder.table("t1", consumed, versionedMaterialize);
        final KTable<String, String> table2 = builder.table("t2", consumed, unversionedMaterialize);
        table1.join(table2, v -> v, (v1, v2) -> v1 + v2);

        // When:
        builder.buildAndOptimizeTopology();

        // Then:
        final GraphNode joinThis = getNodeByType(builder.root, ForeignJoinSubscriptionSendNode.class, new HashSet<>());
        assertNotNull(joinThis);
        verifyVersionedSemantics((ForeignJoinSubscriptionSendNode<?, ?>) joinThis, true);

        final GraphNode joinOther = getNodeByType(builder.root, ForeignTableJoinNode.class, new HashSet<>());
        assertNotNull(joinOther);
        verifyVersionedSemantics((ForeignTableJoinNode<?, ?>) joinOther, false);
    }

    @Test
    public void shouldSetUseVersionedSemanticsOnTableForeignJoinRightOnly() {
        // Given:
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> versionedMaterialize =
            new MaterializedInternal<>(Materialized.as(Stores.persistentVersionedKeyValueStore("versioned", Duration.ofMinutes(5))), builder, storePrefix);
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> unversionedMaterialize =
            new MaterializedInternal<>(Materialized.as("unversioned"), builder, storePrefix);
        final KTable<String, String> table1 = builder.table("t1", consumed, unversionedMaterialize);
        final KTable<String, String> table2 = builder.table("t2", consumed, versionedMaterialize);
        table1.join(table2, v -> v, (v1, v2) -> v1 + v2);

        // When:
        builder.buildAndOptimizeTopology();

        // Then:
        final GraphNode joinThis = getNodeByType(builder.root, ForeignJoinSubscriptionSendNode.class, new HashSet<>());
        assertNotNull(joinThis);
        verifyVersionedSemantics((ForeignJoinSubscriptionSendNode<?, ?>) joinThis, false);

        final GraphNode joinOther = getNodeByType(builder.root, ForeignTableJoinNode.class, new HashSet<>());
        assertNotNull(joinOther);
        verifyVersionedSemantics((ForeignTableJoinNode<?, ?>) joinOther, true);
    }

    @Test
    public void shouldSetUseVersionedSemanticsOnTableForeignSelfJoin() {
        // Given:
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> versionedMaterialize =
            new MaterializedInternal<>(Materialized.as(Stores.persistentVersionedKeyValueStore("versioned", Duration.ofMinutes(5))), builder, storePrefix);
        final KTable<String, String> table1 = builder.table("t1", consumed, versionedMaterialize);
        table1.join(table1, v -> v, (v1, v2) -> v1 + v2);

        // When:
        builder.buildAndOptimizeTopology();

        // Then:
        final GraphNode joinThis = getNodeByType(builder.root, ForeignJoinSubscriptionSendNode.class, new HashSet<>());
        assertNotNull(joinThis);
        verifyVersionedSemantics((ForeignJoinSubscriptionSendNode<?, ?>) joinThis, true);

        final GraphNode joinOther = getNodeByType(builder.root, ForeignTableJoinNode.class, new HashSet<>());
        assertNotNull(joinOther);
        verifyVersionedSemantics((ForeignTableJoinNode<?, ?>) joinOther, true);
    }

    private void verifyVersionedSemantics(final TableFilterNode<?, ?> filterNode, final boolean expectedValue) {
        final ProcessorSupplier<?, ?, ?, ?> processorSupplier = filterNode.processorParameters().processorSupplier();
        assertInstanceOf(KTableFilter.class, processorSupplier);
        final KTableFilter<?, ?> tableFilter = (KTableFilter<?, ?>) processorSupplier;
        assertEquals(expectedValue, tableFilter.isUseVersionedSemantics());
    }

    private void verifyVersionedSemantics(final TableRepartitionMapNode<?, ?> repartitionMapNode, final boolean expectedValue) {
        final ProcessorSupplier<?, ?, ?, ?> processorSupplier = repartitionMapNode.processorParameters().processorSupplier();
        assertInstanceOf(KTableRepartitionMap.class, processorSupplier);
        final KTableRepartitionMap<?, ?, ?, ?> repartitionMap = (KTableRepartitionMap<?, ?, ?, ?>) processorSupplier;
        assertEquals(expectedValue, repartitionMap.isUseVersionedSemantics());
    }

    private void verifyVersionedSemantics(final KTableKTableJoinNode<?, ?, ?, ?> joinNode, final boolean expectedValueLeft, final boolean expectedValueRight) {
        final ProcessorSupplier<?, ?, ?, ?> thisProcessorSupplier = joinNode.thisProcessorParameters().processorSupplier();
        assertInstanceOf(KTableKTableAbstractJoin.class, thisProcessorSupplier);
        final KTableKTableAbstractJoin<?, ?, ?, ?> thisJoin = (KTableKTableAbstractJoin<?, ?, ?, ?>) thisProcessorSupplier;
        assertEquals(expectedValueLeft, thisJoin.isUseVersionedSemantics());

        final ProcessorSupplier<?, ?, ?, ?> otherProcessorSupplier = joinNode.otherProcessorParameters().processorSupplier();
        assertInstanceOf(KTableKTableAbstractJoin.class, otherProcessorSupplier);
        final KTableKTableAbstractJoin<?, ?, ?, ?> otherJoin = (KTableKTableAbstractJoin<?, ?, ?, ?>) otherProcessorSupplier;
        assertEquals(expectedValueRight, otherJoin.isUseVersionedSemantics());
    }

    private void verifyVersionedSemantics(final ForeignJoinSubscriptionSendNode<?, ?> joinThisNode, final boolean expectedValue) {
        final ProcessorSupplier<?, ?, ?, ?> thisProcessorSupplier = joinThisNode.processorParameters().processorSupplier();
        assertInstanceOf(SubscriptionSendProcessorSupplier.class, thisProcessorSupplier);
        final SubscriptionSendProcessorSupplier<?, ?, ?> joinThis = (SubscriptionSendProcessorSupplier<?, ?, ?>) thisProcessorSupplier;
        assertEquals(expectedValue, joinThis.isUseVersionedSemantics());
    }

    private void verifyVersionedSemantics(final ForeignTableJoinNode<?, ?> joinOtherNode, final boolean expectedValue) {
        final ProcessorSupplier<?, ?, ?, ?> otherProcessorSupplier = joinOtherNode.processorParameters().processorSupplier();
        assertInstanceOf(ForeignTableJoinProcessorSupplier.class, otherProcessorSupplier);
        final ForeignTableJoinProcessorSupplier<?, ?, ?> joinThis = (ForeignTableJoinProcessorSupplier<?, ?, ?>) otherProcessorSupplier;
        assertEquals(expectedValue, joinThis.isUseVersionedSemantics());
    }

    private GraphNode getNodeByType(
        final GraphNode currentNode,
        final Class<? extends GraphNode> clazz,
        final Set<GraphNode> visited) {

        if (clazz.isAssignableFrom(currentNode.getClass())) {
            return currentNode;
        }
        for (final GraphNode child: currentNode.children()) {
            visited.add(child);
            final GraphNode result = getNodeByType(child, clazz, visited);
            if (result != null) {
                return result;
            }
        }
        return null;
    }

    private void getNodesByType(
        final GraphNode currentNode,
        final Class<? extends GraphNode> clazz,
        final Set<GraphNode> visited,
        final List<GraphNode> result) {

        if (clazz.isAssignableFrom(currentNode.getClass())) {
            result.add(currentNode);
        }
        for (final GraphNode child: currentNode.children()) {
            if (!visited.contains(child)) {
                visited.add(child);
                getNodesByType(child, clazz, visited, result);
            }
        }
    }

    private void countJoinWindowNodes(
        final AtomicInteger count,
        final GraphNode currentNode,
        final Set<GraphNode> visited) {

        if (currentNode instanceof WindowedStreamProcessorNode) {
            count.incrementAndGet();
        }

        for (final GraphNode child: currentNode.children()) {
            if (!visited.contains(child)) {
                visited.add(child);
                countJoinWindowNodes(count, child, visited);
            }
        }
    }
}
