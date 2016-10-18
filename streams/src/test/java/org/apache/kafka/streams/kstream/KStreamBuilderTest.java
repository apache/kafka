/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.kstream.internals.KStreamImpl;
import org.apache.kafka.streams.errors.TopologyBuilderException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockValueJoiner;
import org.junit.After;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KStreamBuilderTest {

    private KStreamTestDriver driver = null;

    @After
    public void cleanup() {
        if (driver != null) {
            driver.close();
        }
        driver = null;
    }

    @Test(expected = TopologyBuilderException.class)
    public void testFrom() {
        final KStreamBuilder builder = new KStreamBuilder();

        builder.stream("topic-1", "topic-2");

        builder.addSource(KStreamImpl.SOURCE_NAME + "0000000000", "topic-3");
    }

    @Test
    public void testNewName() {
        KStreamBuilder builder = new KStreamBuilder();

        assertEquals("X-0000000000", builder.newName("X-"));
        assertEquals("Y-0000000001", builder.newName("Y-"));
        assertEquals("Z-0000000002", builder.newName("Z-"));

        builder = new KStreamBuilder();

        assertEquals("X-0000000000", builder.newName("X-"));
        assertEquals("Y-0000000001", builder.newName("Y-"));
        assertEquals("Z-0000000002", builder.newName("Z-"));
    }

    @Test
    public void testMerge() {
        String topic1 = "topic-1";
        String topic2 = "topic-2";

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> source1 = builder.stream(topic1);
        KStream<String, String> source2 = builder.stream(topic2);
        KStream<String, String> merged = builder.merge(source1, source2);

        MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();
        merged.process(processorSupplier);

        driver = new KStreamTestDriver(builder);
        driver.setTime(0L);

        driver.process(topic1, "A", "aa");
        driver.process(topic2, "B", "bb");
        driver.process(topic2, "C", "cc");
        driver.process(topic1, "D", "dd");

        assertEquals(Utils.mkList("A:aa", "B:bb", "C:cc", "D:dd"), processorSupplier.processed);
    }

    @Test
    public void shouldHaveCorrectSourceTopicsForTableFromMergedStream() throws Exception {
        final String topic1 = "topic-1";
        final String topic2 = "topic-2";
        final String topic3 = "topic-3";
        final KStreamBuilder builder = new KStreamBuilder();
        final KStream<String, String> source1 = builder.stream(topic1);
        final KStream<String, String> source2 = builder.stream(topic2);
        final KStream<String, String> source3 = builder.stream(topic3);

        final KStream<String, String> merged = builder.merge(source1, source2, source3);
        merged.groupByKey().count("my-table");
        final Map<String, Set<String>> actual = builder.stateStoreNameToSourceTopics();
        assertEquals(Utils.mkSet("topic-1", "topic-2", "topic-3"), actual.get("my-table"));
    }

    @Test
    public void shouldHaveCorrectSourceTopicsForTableFromMergedStreamWithProcessors() throws Exception {
        final String topic1 = "topic-1";
        final String topic2 = "topic-2";
        final KStreamBuilder builder = new KStreamBuilder();
        final KStream<String, String> source1 = builder.stream(topic1);
        final KStream<String, String> source2 = builder.stream(topic2);
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

        final KStream<String, String> merged = builder.merge(processedSource1, processedSource2);
        merged.groupByKey().count("my-table");
        final Map<String, Set<String>> actual = builder.stateStoreNameToSourceTopics();
        assertEquals(Utils.mkSet("topic-1", "topic-2"), actual.get("my-table"));
    }

    @Test(expected = TopologyBuilderException.class)
    public void shouldThrowExceptionWhenNoTopicPresent() throws Exception {
        new KStreamBuilder().stream();
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenTopicNamesAreNull() throws Exception {
        new KStreamBuilder().stream(Serdes.String(), Serdes.String(), null, null);
    }

    @Test
    public void shouldNotGroupGlobalTableWithOtherStreams() throws Exception {
        final KStreamBuilder builder = new KStreamBuilder();
        final GlobalKTable<String, String> globalTable = builder.globalTable("table", "globalTable");
        final KStream<String, String> stream = builder.stream("t1");
        final KeyValueMapper<String, String, String> kvMapper = new KeyValueMapper<String, String, String>() {
            @Override
            public String apply(final String key, final String value) {
                return value;
            }
        };
        stream.leftJoin(globalTable, kvMapper, MockValueJoiner.STRING_JOINER);
        builder.stream("t2");
        builder.setApplicationId("app-id");
        final Map<Integer, Set<String>> nodeGroups = builder.nodeGroups();
        assertEquals(Utils.mkSet("KTABLE-SOURCE-0000000001", "KSTREAM-SOURCE-0000000000"), nodeGroups.get(0));
    }

    @Test
    public void shouldBuildSimpleGlobalTableTopology() throws Exception {
        final KStreamBuilder builder = new KStreamBuilder();
        builder.globalTable("table", "globalTable");
        final ProcessorTopology topology = builder.buildGlobalStateTopology();
        final Map<StateStore, ProcessorNode> stateStoreProcessorNodeMap = topology.storeToProcessorNodeMap();
        assertEquals(1, stateStoreProcessorNodeMap.size());
        final StateStore store = stateStoreProcessorNodeMap.keySet().iterator().next();
        assertEquals("globalTable", store.name());
        assertEquals("KTABLE-SOURCE-0000000001", stateStoreProcessorNodeMap.get(store).name());
    }

    @Test
    public void shouldBuildGlobalTopologyWithAllGlobalTables() throws Exception {
        final KStreamBuilder builder = new KStreamBuilder();
        builder.globalTable("table", "globalTable");
        builder.globalTable("table2", "globalTable2");
        final ProcessorTopology topology = builder.buildGlobalStateTopology();
        final List<StateStore> stateStores = topology.globalStateStores();
        assertEquals(Utils.mkSet("table", "table2"), topology.sourceTopics());
        assertEquals(2, stateStores.size());
    }

    @Test
    public void shouldAddGlobalTablesToEachGroup() throws Exception {
        final String one = "globalTable";
        final String two = "globalTable2";
        final KStreamBuilder builder = new KStreamBuilder();
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
        stream.leftJoin(globalTable, kvMapper, MockValueJoiner.STRING_JOINER);
        final KStream<String, String> stream2 = builder.stream("t2");
        stream2.leftJoin(globalTable2, kvMapper, MockValueJoiner.STRING_JOINER);
        builder.setApplicationId("app-id");
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
}
