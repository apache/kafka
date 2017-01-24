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
import org.apache.kafka.streams.processor.internals.ProcessorTopology;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockKeyValueMapper;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockValueJoiner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KStreamBuilderTest {

    private static final String APP_ID = "app-id";

    private final KStreamBuilder builder = new KStreamBuilder();

    private KStreamTestDriver driver = null;

    @Before
    public void setUp() {
        builder.setApplicationId(APP_ID);
    }

    @After
    public void cleanup() {
        if (driver != null) {
            driver.close();
        }
        driver = null;
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

        KStreamBuilder newBuilder = new KStreamBuilder();

        assertEquals("X-0000000000", newBuilder.newName("X-"));
        assertEquals("Y-0000000001", newBuilder.newName("Y-"));
        assertEquals("Z-0000000002", newBuilder.newName("Z-"));
    }

    @Test
    public void testMerge() {
        String topic1 = "topic-1";
        String topic2 = "topic-2";

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

        final KStream<String, String> merged = builder.merge(processedSource1, processedSource2, source3);
        merged.groupByKey().count("my-table");
        final Map<String, List<String>> actual = builder.stateStoreNameToSourceTopics();
        assertEquals(Utils.mkList("topic-1", "topic-2", "topic-3"), actual.get("my-table"));
    }

    @Test(expected = TopologyBuilderException.class)
    public void shouldThrowExceptionWhenNoTopicPresent() throws Exception {
        builder.stream();
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenTopicNamesAreNull() throws Exception {
        builder.stream(Serdes.String(), Serdes.String(), null, null);
    }

    @Test
    public void shouldNotMaterializeSourceKTableIfStateNameNotSpecified() throws Exception {
        builder.table("topic1", "table1");
        builder.table("topic2", null);

        final ProcessorTopology topology = builder.build(null);

        assertEquals(1, topology.stateStores().size());
        assertEquals("table1", topology.stateStores().get(0).name());
        assertEquals(1, topology.storeToChangelogTopic().size());
        assertEquals("topic1", topology.storeToChangelogTopic().get("table1"));
    }

    @Test
    public void shouldBuildSimpleGlobalTableTopology() throws Exception {
        builder.globalTable("table", "globalTable");

        final ProcessorTopology topology = builder.buildGlobalStateTopology();
        final List<StateStore> stateStores = topology.globalStateStores();

        assertEquals(1, stateStores.size());
        assertEquals("globalTable", stateStores.get(0).name());
    }

    @Test
    public void shouldBuildGlobalTopologyWithAllGlobalTables() throws Exception {
        builder.globalTable("table", "globalTable");
        builder.globalTable("table2", "globalTable2");

        final ProcessorTopology topology = builder.buildGlobalStateTopology();

        final List<StateStore> stateStores = topology.globalStateStores();
        final Set<String> sourceTopics = topology.sourceTopics();

        assertEquals(Utils.mkSet("table", "table2"), sourceTopics);
        assertEquals(2, stateStores.size());
    }

    @Test
    public void shouldAddGlobalTablesToEachGroup() throws Exception {
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
    public void shouldMapStateStoresToCorrectSourceTopics() throws Exception {
        final KStream<String, String> playEvents = builder.stream("events");

        final KTable<String, String> table = builder.table("table-topic", "table-store");
        assertEquals(Collections.singletonList("table-topic"), builder.stateStoreNameToSourceTopics().get("table-store"));

        final KStream<String, String> mapped = playEvents.map(MockKeyValueMapper.<String, String>SelectValueKeyValueMapper());
        mapped.leftJoin(table, MockValueJoiner.TOSTRING_JOINER).groupByKey().count("count");
        assertEquals(Collections.singletonList("table-topic"), builder.stateStoreNameToSourceTopics().get("table-store"));
        assertEquals(Collections.singletonList(APP_ID + "-KSTREAM-MAP-0000000003-repartition"), builder.stateStoreNameToSourceTopics().get("count"));
    }
}
