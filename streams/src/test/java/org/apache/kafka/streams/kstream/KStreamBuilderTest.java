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
package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.TopologyBuilderException;
import org.apache.kafka.streams.kstream.internals.KStreamImpl;
import org.apache.kafka.streams.kstream.internals.KTableImpl;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;
import org.apache.kafka.streams.processor.internals.SourceNode;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockKeyValueMapper;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockTimestampExtractor;
import org.apache.kafka.test.MockValueJoiner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("deprecation")
public class KStreamBuilderTest {

    private static final String APP_ID = "app-id";

    private final KStreamBuilder builder = new KStreamBuilder();
    @Rule
    public final KStreamTestDriver driver = new KStreamTestDriver();

    @Before
    public void setUp() {
        builder.setApplicationId(APP_ID);
    }

    @Test(expected = TopologyBuilderException.class)
    public void testFrom() {
        builder.stream("topic-1", "topic-2");

        builder.addSource(KStreamImpl.SOURCE_NAME + "0000000000", "topic-3");
    }

    @Test
    public void testNewName() {
        assertEquals("X-0000000000", builder.newName("X-"));
        assertEquals("Y-0000000001", builder.newName("Y-"));
        assertEquals("Z-0000000002", builder.newName("Z-"));

        final KStreamBuilder newBuilder = new KStreamBuilder();

        assertEquals("X-0000000000", newBuilder.newName("X-"));
        assertEquals("Y-0000000001", newBuilder.newName("Y-"));
        assertEquals("Z-0000000002", newBuilder.newName("Z-"));
    }


    @Test
    public void shouldProcessFromSinkTopic() {
        final KStream<String, String> source = builder.stream("topic-source");
        source.to("topic-sink");

        final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();

        source.process(processorSupplier);

        driver.setUp(builder);
        driver.setTime(0L);

        driver.process("topic-source", "A", "aa");

        // no exception was thrown
        assertEquals(Utils.mkList("A:aa"), processorSupplier.processed);
    }

    @Test
    public void shouldProcessViaThroughTopic() {
        final KStream<String, String> source = builder.stream("topic-source");
        final KStream<String, String> through = source.through("topic-sink");

        final MockProcessorSupplier<String, String> sourceProcessorSupplier = new MockProcessorSupplier<>();
        final MockProcessorSupplier<String, String> throughProcessorSupplier = new MockProcessorSupplier<>();

        source.process(sourceProcessorSupplier);
        through.process(throughProcessorSupplier);

        driver.setUp(builder);
        driver.setTime(0L);

        driver.process("topic-source", "A", "aa");

        assertEquals(Utils.mkList("A:aa"), sourceProcessorSupplier.processed);
        assertEquals(Utils.mkList("A:aa"), throughProcessorSupplier.processed);
    }

    @Test
    public void testNewStoreName() {
        assertEquals("X-STATE-STORE-0000000000", builder.newStoreName("X-"));
        assertEquals("Y-STATE-STORE-0000000001", builder.newStoreName("Y-"));
        assertEquals("Z-STATE-STORE-0000000002", builder.newStoreName("Z-"));

        KStreamBuilder newBuilder = new KStreamBuilder();

        assertEquals("X-STATE-STORE-0000000000", newBuilder.newStoreName("X-"));
        assertEquals("Y-STATE-STORE-0000000001", newBuilder.newStoreName("Y-"));
        assertEquals("Z-STATE-STORE-0000000002", newBuilder.newStoreName("Z-"));
    }

    @Test
    public void testMerge() {
        final String topic1 = "topic-1";
        final String topic2 = "topic-2";

        final KStream<String, String> source1 = builder.stream(topic1);
        final KStream<String, String> source2 = builder.stream(topic2);
        final KStream<String, String> merged = builder.merge(source1, source2);

        final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();
        merged.process(processorSupplier);

        driver.setUp(builder);
        driver.setTime(0L);

        driver.process(topic1, "A", "aa");
        driver.process(topic2, "B", "bb");
        driver.process(topic2, "C", "cc");
        driver.process(topic1, "D", "dd");

        assertEquals(Utils.mkList("A:aa", "B:bb", "C:cc", "D:dd"), processorSupplier.processed);
    }

    @Test
    public void shouldHaveCorrectSourceTopicsForTableFromMergedStream() {
        final String topic1 = "topic-1";
        final String topic2 = "topic-2";
        final String topic3 = "topic-3";
        final KStream<String, String> source1 = builder.stream(topic1);
        final KStream<String, String> source2 = builder.stream(topic2);
        final KStream<String, String> source3 = builder.stream(topic3);
        final KStream<String, String> processedSource1 =
                source1.mapValues(new ValueMapper<String, String>() {
                    @Override
                    public String apply(final String value) {
                        return value;
                    }
                }).filter(new Predicate<String, String>() {
                    @Override
                    public boolean test(final String key, final String value) {
                        return true;
                    }
                });
        final KStream<String, String> processedSource2 = source2.filter(new Predicate<String, String>() {
            @Override
            public boolean test(final String key, final String value) {
                return true;
            }
        });

        final KStream<String, String> merged = processedSource1.merge(processedSource2).merge(source3);
        merged.groupByKey().count("my-table");
        final Map<String, List<String>> actual = builder.stateStoreNameToSourceTopics();
        assertEquals(Utils.mkList("topic-1", "topic-2", "topic-3"), actual.get("my-table"));
    }

    @Test(expected = TopologyBuilderException.class)
    public void shouldThrowExceptionWhenNoTopicPresent() {
        builder.stream();
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenTopicNamesAreNull() {
        builder.stream(Serdes.String(), Serdes.String(), null, null);
    }

    @Test
    public void shouldStillMaterializeSourceKTableIfStateNameNotSpecified() {
        KTable table1 = builder.table("topic1", "table1");
        KTable table2 = builder.table("topic2", (String) null);

        final ProcessorTopology topology = builder.build(null);

        assertEquals(2, topology.stateStores().size());
        assertEquals("table1", topology.stateStores().get(0).name());

        final String internalStoreName = topology.stateStores().get(1).name();
        assertTrue(internalStoreName.contains(KTableImpl.STATE_STORE_NAME));
        assertEquals(2, topology.storeToChangelogTopic().size());
        assertEquals("topic1", topology.storeToChangelogTopic().get("table1"));
        assertEquals("topic2", topology.storeToChangelogTopic().get(internalStoreName));
        assertEquals(table1.queryableStoreName(), "table1");
        assertNull(table2.queryableStoreName());
    }

    @Test
    public void shouldBuildSimpleGlobalTableTopology() {
        builder.globalTable("table", "globalTable");

        final ProcessorTopology topology = builder.buildGlobalStateTopology();
        final List<StateStore> stateStores = topology.globalStateStores();

        assertEquals(1, stateStores.size());
        assertEquals("globalTable", stateStores.get(0).name());
    }

    private void doBuildGlobalTopologyWithAllGlobalTables() {
        final ProcessorTopology topology = builder.buildGlobalStateTopology();

        final List<StateStore> stateStores = topology.globalStateStores();
        final Set<String> sourceTopics = topology.sourceTopics();

        assertEquals(Utils.mkSet("table", "table2"), sourceTopics);
        assertEquals(2, stateStores.size());
    }

    @Test
    public void shouldBuildGlobalTopologyWithAllGlobalTables() {
        builder.globalTable("table", "globalTable");
        builder.globalTable("table2", "globalTable2");

        doBuildGlobalTopologyWithAllGlobalTables();
    }

    @Test
    public void shouldBuildGlobalTopologyWithAllGlobalTablesWithInternalStoreName() {
        builder.globalTable("table");
        builder.globalTable("table2");

        doBuildGlobalTopologyWithAllGlobalTables();
    }

    @Test
    public void shouldAddGlobalTablesToEachGroup() {
        final String one = "globalTable";
        final String two = "globalTable2";
        final GlobalKTable<String, String> globalTable = builder.globalTable("table", one);
        final GlobalKTable<String, String> globalTable2 = builder.globalTable("table2", two);

        builder.table("not-global", "not-global");

        final KeyValueMapper<String, String, String> kvMapper = new KeyValueMapper<String, String, String>() {
            @Override
            public String apply(final String key, final String value) {
                return value;
            }
        };

        final KStream<String, String> stream = builder.stream("t1");
        stream.leftJoin(globalTable, kvMapper, MockValueJoiner.TOSTRING_JOINER);
        final KStream<String, String> stream2 = builder.stream("t2");
        stream2.leftJoin(globalTable2, kvMapper, MockValueJoiner.TOSTRING_JOINER);

        final Map<Integer, Set<String>> nodeGroups = builder.nodeGroups();
        for (Integer groupId : nodeGroups.keySet()) {
            final ProcessorTopology topology = builder.build(groupId);
            final List<StateStore> stateStores = topology.globalStateStores();
            final Set<String> names = new HashSet<>();
            for (StateStore stateStore : stateStores) {
                names.add(stateStore.name());
            }

            assertEquals(2, stateStores.size());
            assertTrue(names.contains(one));
            assertTrue(names.contains(two));
        }
    }

    @Test
    public void shouldMapStateStoresToCorrectSourceTopics() {
        final KStream<String, String> playEvents = builder.stream("events");

        final KTable<String, String> table = builder.table("table-topic", "table-store");
        assertEquals(Collections.singletonList("table-topic"), builder.stateStoreNameToSourceTopics().get("table-store"));

        final KStream<String, String> mapped = playEvents.map(MockKeyValueMapper.<String, String>SelectValueKeyValueMapper());
        mapped.leftJoin(table, MockValueJoiner.TOSTRING_JOINER).groupByKey().count("count");
        assertEquals(Collections.singletonList("table-topic"), builder.stateStoreNameToSourceTopics().get("table-store"));
        assertEquals(Collections.singletonList(APP_ID + "-KSTREAM-MAP-0000000003-repartition"), builder.stateStoreNameToSourceTopics().get("count"));
    }

    @Test
    public void shouldAddTopicToEarliestAutoOffsetResetList() {
        final String topicName = "topic-1";

        builder.stream(TopologyBuilder.AutoOffsetReset.EARLIEST, topicName);

        assertTrue(builder.earliestResetTopicsPattern().matcher(topicName).matches());
        assertFalse(builder.latestResetTopicsPattern().matcher(topicName).matches());
    }

    @Test
    public void shouldAddTopicToLatestAutoOffsetResetList() {
        final String topicName = "topic-1";

        builder.stream(TopologyBuilder.AutoOffsetReset.LATEST, topicName);

        assertTrue(builder.latestResetTopicsPattern().matcher(topicName).matches());
        assertFalse(builder.earliestResetTopicsPattern().matcher(topicName).matches());
    }

    @Test
    public void shouldAddTableToEarliestAutoOffsetResetList() {
        final String topicName = "topic-1";
        final String storeName = "test-store";

        builder.table(TopologyBuilder.AutoOffsetReset.EARLIEST, topicName, storeName);

        assertTrue(builder.earliestResetTopicsPattern().matcher(topicName).matches());
        assertFalse(builder.latestResetTopicsPattern().matcher(topicName).matches());
    }

    @Test
    public void shouldAddTableToLatestAutoOffsetResetList() {
        final String topicName = "topic-1";
        final String storeName = "test-store";

        builder.table(TopologyBuilder.AutoOffsetReset.LATEST, topicName, storeName);

        assertTrue(builder.latestResetTopicsPattern().matcher(topicName).matches());
        assertFalse(builder.earliestResetTopicsPattern().matcher(topicName).matches());
    }

    @Test
    public void shouldNotAddTableToOffsetResetLists() {
        final String topicName = "topic-1";
        final String storeName = "test-store";
        final Serde<String> stringSerde = Serdes.String();

        builder.table(stringSerde, stringSerde, topicName, storeName);

        assertFalse(builder.latestResetTopicsPattern().matcher(topicName).matches());
        assertFalse(builder.earliestResetTopicsPattern().matcher(topicName).matches());
    }

    @Test
    public void shouldNotAddRegexTopicsToOffsetResetLists() {
        final Pattern topicPattern = Pattern.compile("topic-\\d");
        final String topic = "topic-5";

        builder.stream(topicPattern);

        assertFalse(builder.latestResetTopicsPattern().matcher(topic).matches());
        assertFalse(builder.earliestResetTopicsPattern().matcher(topic).matches());

    }

    @Test
    public void shouldAddRegexTopicToEarliestAutoOffsetResetList() {
        final Pattern topicPattern = Pattern.compile("topic-\\d+");
        final String topicTwo = "topic-500000";

        builder.stream(TopologyBuilder.AutoOffsetReset.EARLIEST, topicPattern);

        assertTrue(builder.earliestResetTopicsPattern().matcher(topicTwo).matches());
        assertFalse(builder.latestResetTopicsPattern().matcher(topicTwo).matches());
    }

    @Test
    public void shouldAddRegexTopicToLatestAutoOffsetResetList() {
        final Pattern topicPattern = Pattern.compile("topic-\\d+");
        final String topicTwo = "topic-1000000";

        builder.stream(TopologyBuilder.AutoOffsetReset.LATEST, topicPattern);

        assertTrue(builder.latestResetTopicsPattern().matcher(topicTwo).matches());
        assertFalse(builder.earliestResetTopicsPattern().matcher(topicTwo).matches());
    }

    @Test
    public void kStreamTimestampExtractorShouldBeNull() {
        builder.stream("topic");
        final ProcessorTopology processorTopology = builder.build(null);
        assertNull(processorTopology.source("topic").getTimestampExtractor());
    }

    @Test
    public void shouldAddTimestampExtractorToStreamWithKeyValSerdePerSource() {
        builder.stream(new MockTimestampExtractor(), null, null, "topic");
        final ProcessorTopology processorTopology = builder.build(null);
        for (final SourceNode sourceNode: processorTopology.sources()) {
            assertThat(sourceNode.getTimestampExtractor(), instanceOf(MockTimestampExtractor.class));
        }
    }

    @Test
    public void shouldAddTimestampExtractorToStreamWithOffsetResetPerSource() {
        builder.stream(null, new MockTimestampExtractor(), null, null, "topic");
        final ProcessorTopology processorTopology = builder.build(null);
        assertThat(processorTopology.source("topic").getTimestampExtractor(), instanceOf(MockTimestampExtractor.class));
    }

    @Test
    public void shouldAddTimestampExtractorToTablePerSource() {
        builder.table("topic", "store");
        final ProcessorTopology processorTopology = builder.build(null);
        assertNull(processorTopology.source("topic").getTimestampExtractor());
    }

    @Test
    public void kTableTimestampExtractorShouldBeNull() {
        builder.table("topic", "store");
        final ProcessorTopology processorTopology = builder.build(null);
        assertNull(processorTopology.source("topic").getTimestampExtractor());
    }

    @Test
    public void shouldAddTimestampExtractorToTableWithKeyValSerdePerSource() {
        builder.table(null, new MockTimestampExtractor(), null, null, "topic", "store");
        final ProcessorTopology processorTopology = builder.build(null);
        assertThat(processorTopology.source("topic").getTimestampExtractor(), instanceOf(MockTimestampExtractor.class));
    }
}