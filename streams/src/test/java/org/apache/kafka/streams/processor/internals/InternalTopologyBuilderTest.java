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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.MockApiProcessor;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.MockKeyValueStoreBuilder;
import org.apache.kafka.test.MockTimestampExtractor;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static java.time.Duration.ofSeconds;
import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class InternalTopologyBuilderTest {

    private final Serde<String> stringSerde = Serdes.String();
    private final InternalTopologyBuilder builder = new InternalTopologyBuilder();
    private final StoreBuilder<?> storeBuilder = new MockKeyValueStoreBuilder("testStore", false);

    @Test
    public void shouldAddSourceWithOffsetReset() {
        final String earliestTopic = "earliestTopic";
        final String latestTopic = "latestTopic";

        builder.addSource(Topology.AutoOffsetReset.EARLIEST, "source", null, null, null, earliestTopic);
        builder.addSource(Topology.AutoOffsetReset.LATEST, "source2", null, null, null, latestTopic);
        builder.initializeSubscription();

        assertTrue(builder.earliestResetTopicsPattern().matcher(earliestTopic).matches());
        assertTrue(builder.latestResetTopicsPattern().matcher(latestTopic).matches());
    }

    @Test
    public void shouldAddSourcePatternWithOffsetReset() {
        final String earliestTopicPattern = "earliest.*Topic";
        final String latestTopicPattern = "latest.*Topic";

        builder.addSource(Topology.AutoOffsetReset.EARLIEST, "source", null, null, null, Pattern.compile(earliestTopicPattern));
        builder.addSource(Topology.AutoOffsetReset.LATEST, "source2", null, null, null,  Pattern.compile(latestTopicPattern));
        builder.initializeSubscription();

        assertTrue(builder.earliestResetTopicsPattern().matcher("earliestTestTopic").matches());
        assertTrue(builder.latestResetTopicsPattern().matcher("latestTestTopic").matches());
    }

    @Test
    public void shouldAddSourceWithoutOffsetReset() {
        builder.addSource(null, "source", null, stringSerde.deserializer(), stringSerde.deserializer(), "test-topic");
        builder.initializeSubscription();

        assertEquals(Collections.singletonList("test-topic"), builder.sourceTopicCollection());
        assertEquals(builder.earliestResetTopicsPattern().pattern(), "");
        assertEquals(builder.latestResetTopicsPattern().pattern(), "");
    }

    @Test
    public void shouldAddPatternSourceWithoutOffsetReset() {
        final Pattern expectedPattern = Pattern.compile("test-.*");

        builder.addSource(null, "source", null, stringSerde.deserializer(), stringSerde.deserializer(), Pattern.compile("test-.*"));
        builder.initializeSubscription();

        assertEquals(expectedPattern.pattern(), builder.sourceTopicPattern().pattern());
        assertEquals(builder.earliestResetTopicsPattern().pattern(), "");
        assertEquals(builder.latestResetTopicsPattern().pattern(), "");
    }

    @Test(expected = TopologyException.class)
    public void shouldNotAllowOffsetResetSourceWithoutTopics() {
        builder.addSource(Topology.AutoOffsetReset.EARLIEST, "source", null, stringSerde.deserializer(), stringSerde.deserializer());
    }

    @Test
    public void shouldNotAllowOffsetResetSourceWithDuplicateSourceName() {
        builder.addSource(Topology.AutoOffsetReset.EARLIEST, "source", null, stringSerde.deserializer(), stringSerde.deserializer(), "topic-1");
        try {
            builder.addSource(Topology.AutoOffsetReset.LATEST, "source", null, stringSerde.deserializer(), stringSerde.deserializer(), "topic-2");
            fail("Should throw TopologyException for duplicate source name");
        } catch (final TopologyException expected) { /* ok */ }
    }

    @Test
    public void testAddSourceWithSameName() {
        builder.addSource(null, "source", null, null, null, "topic-1");
        try {
            builder.addSource(null, "source", null, null, null, "topic-2");
            fail("Should throw TopologyException with source name conflict");
        } catch (final TopologyException expected) { /* ok */ }
    }

    @Test
    public void testAddSourceWithSameTopic() {
        builder.addSource(null, "source", null, null, null, "topic-1");
        try {
            builder.addSource(null, "source-2", null, null, null, "topic-1");
            fail("Should throw TopologyException with topic conflict");
        } catch (final TopologyException expected) { /* ok */ }
    }

    @Test
    public void testAddProcessorWithSameName() {
        builder.addSource(null, "source", null, null, null, "topic-1");
        builder.addProcessor("processor", new MockApiProcessorSupplier<>(), "source");
        try {
            builder.addProcessor("processor", new MockApiProcessorSupplier<>(), "source");
            fail("Should throw TopologyException with processor name conflict");
        } catch (final TopologyException expected) { /* ok */ }
    }

    @Test(expected = TopologyException.class)
    public void testAddProcessorWithWrongParent() {
        builder.addProcessor("processor", new MockApiProcessorSupplier<>(), "source");
    }

    @Test(expected = TopologyException.class)
    public void testAddProcessorWithSelfParent() {
        builder.addProcessor("processor", new MockApiProcessorSupplier<>(), "processor");
    }

    @Test(expected = TopologyException.class)
    public void testAddProcessorWithEmptyParents() {
        builder.addProcessor("processor", new MockApiProcessorSupplier<>());
    }

    @Test(expected = NullPointerException.class)
    public void testAddProcessorWithNullParents() {
        builder.addProcessor("processor", new MockApiProcessorSupplier<>(), (String) null);
    }

    @Test
    public void testAddProcessorWithBadSupplier() {
        final Processor<Object, Object, Object, Object> processor = new MockApiProcessor<>();
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
            () -> builder.addProcessor("processor", () -> processor, (String) null)
        );
        assertThat(exception.getMessage(), containsString("#get() must return a new object each time it is called."));
    }

    @Test
    public void testAddGlobalStoreWithBadSupplier() {
        final org.apache.kafka.streams.processor.api.Processor<?, ?, Void, Void> processor = new MockApiProcessorSupplier<Object, Object, Void, Void>().get();
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
            () -> builder.addGlobalStore(
                        new MockKeyValueStoreBuilder("global-store", false).withLoggingDisabled(),
                        "globalSource",
                        null,
                        null,
                        null,
                        "globalTopic",
                        "global-processor",
                () -> processor)
        );
        assertThat(exception.getMessage(), containsString("#get() must return a new object each time it is called."));
    }

    @Test
    public void testAddSinkWithSameName() {
        builder.addSource(null, "source", null, null, null, "topic-1");
        builder.addSink("sink", "topic-2", null, null, null, "source");
        try {
            builder.addSink("sink", "topic-3", null, null, null, "source");
            fail("Should throw TopologyException with sink name conflict");
        } catch (final TopologyException expected) { /* ok */ }
    }

    @Test(expected = TopologyException.class)
    public void testAddSinkWithWrongParent() {
        builder.addSink("sink", "topic-2", null, null, null, "source");
    }

    @Test(expected = TopologyException.class)
    public void testAddSinkWithSelfParent() {
        builder.addSink("sink", "topic-2", null, null, null, "sink");
    }


    @Test(expected = TopologyException.class)
    public void testAddSinkWithEmptyParents() {
        builder.addSink("sink", "topic", null, null, null);
    }

    @Test(expected = NullPointerException.class)
    public void testAddSinkWithNullParents() {
        builder.addSink("sink", "topic", null, null, null, (String) null);
    }

    @Test
    public void testAddSinkConnectedWithParent() {
        builder.addSource(null, "source", null, null, null, "source-topic");
        builder.addSink("sink", "dest-topic", null, null, null, "source");

        final Map<Integer, Set<String>> nodeGroups = builder.nodeGroups();
        final Set<String> nodeGroup = nodeGroups.get(0);

        assertTrue(nodeGroup.contains("sink"));
        assertTrue(nodeGroup.contains("source"));
    }

    @Test
    public void testAddSinkConnectedWithMultipleParent() {
        builder.addSource(null, "source", null, null, null, "source-topic");
        builder.addSource(null, "sourceII", null, null, null, "source-topicII");
        builder.addSink("sink", "dest-topic", null, null, null, "source", "sourceII");

        final Map<Integer, Set<String>> nodeGroups = builder.nodeGroups();
        final Set<String> nodeGroup = nodeGroups.get(0);

        assertTrue(nodeGroup.contains("sink"));
        assertTrue(nodeGroup.contains("source"));
        assertTrue(nodeGroup.contains("sourceII"));
    }

    @Test
    public void testOnlyTopicNameSourceTopics() {
        builder.setApplicationId("X");
        builder.addSource(null, "source-1", null, null, null, "topic-1");
        builder.addSource(null, "source-2", null, null, null, "topic-2");
        builder.addSource(null, "source-3", null, null, null, "topic-3");
        builder.addInternalTopic("topic-3", InternalTopicProperties.empty());
        builder.initializeSubscription();

        assertFalse(builder.usesPatternSubscription());
        assertEquals(Arrays.asList("X-topic-3", "topic-1", "topic-2"), builder.sourceTopicCollection());
    }

    @Test
    public void testPatternAndNameSourceTopics() {
        final Pattern sourcePattern = Pattern.compile("topic-4|topic-5");

        builder.setApplicationId("X");
        builder.addSource(null, "source-1", null, null, null, "topic-1");
        builder.addSource(null, "source-2", null, null, null, "topic-2");
        builder.addSource(null, "source-3", null, null, null, "topic-3");
        builder.addSource(null, "source-4", null, null, null, sourcePattern);

        builder.addInternalTopic("topic-3", InternalTopicProperties.empty());
        builder.initializeSubscription();

        final Pattern expectedPattern = Pattern.compile("X-topic-3|topic-1|topic-2|topic-4|topic-5");

        assertEquals(expectedPattern.pattern(), builder.sourceTopicPattern().pattern());
    }

    @Test
    public void testPatternSourceTopicsWithGlobalTopics() {
        builder.setApplicationId("X");
        builder.addSource(null, "source-1", null, null, null, Pattern.compile("topic-1"));
        builder.addSource(null, "source-2", null, null, null, Pattern.compile("topic-2"));
        builder.addGlobalStore(
            new MockKeyValueStoreBuilder("global-store", false).withLoggingDisabled(),
            "globalSource",
            null,
            null,
            null,
            "globalTopic",
            "global-processor",
            new MockApiProcessorSupplier<>()
        );
        builder.initializeSubscription();

        final Pattern expectedPattern = Pattern.compile("topic-1|topic-2");

        assertThat(builder.sourceTopicPattern().pattern(), equalTo(expectedPattern.pattern()));
    }

    @Test
    public void testNameSourceTopicsWithGlobalTopics() {
        builder.setApplicationId("X");
        builder.addSource(null, "source-1", null, null, null, "topic-1");
        builder.addSource(null, "source-2", null, null, null, "topic-2");
        builder.addGlobalStore(
            new MockKeyValueStoreBuilder("global-store", false).withLoggingDisabled(),
            "globalSource",
            null,
            null,
            null,
            "globalTopic",
            "global-processor",
            new MockApiProcessorSupplier<>()
        );
        builder.initializeSubscription();

        assertThat(builder.sourceTopicCollection(), equalTo(asList("topic-1", "topic-2")));
    }

    @Test
    public void testPatternSourceTopic() {
        final Pattern expectedPattern = Pattern.compile("topic-\\d");
        builder.addSource(null, "source-1", null, null, null, expectedPattern);
        builder.initializeSubscription();
        assertEquals(expectedPattern.pattern(), builder.sourceTopicPattern().pattern());
    }

    @Test
    public void testAddMoreThanOnePatternSourceNode() {
        final Pattern expectedPattern = Pattern.compile("topics[A-Z]|.*-\\d");
        builder.addSource(null, "source-1", null, null, null, Pattern.compile("topics[A-Z]"));
        builder.addSource(null, "source-2", null, null, null, Pattern.compile(".*-\\d"));
        builder.initializeSubscription();
        assertEquals(expectedPattern.pattern(), builder.sourceTopicPattern().pattern());
    }

    @Test
    public void testSubscribeTopicNameAndPattern() {
        final Pattern expectedPattern = Pattern.compile("topic-bar|topic-foo|.*-\\d");
        builder.addSource(null, "source-1", null, null, null, "topic-foo", "topic-bar");
        builder.addSource(null, "source-2", null, null, null, Pattern.compile(".*-\\d"));
        builder.initializeSubscription();
        assertEquals(expectedPattern.pattern(), builder.sourceTopicPattern().pattern());
    }

    @Test
    public void testPatternMatchesAlreadyProvidedTopicSource() {
        builder.addSource(null, "source-1", null, null, null, "foo");
        try {
            builder.addSource(null, "source-2", null, null, null, Pattern.compile("f.*"));
            fail("Should throw TopologyException with topic name/pattern conflict");
        } catch (final TopologyException expected) { /* ok */ }
    }

    @Test
    public void testNamedTopicMatchesAlreadyProvidedPattern() {
        builder.addSource(null, "source-1", null, null, null, Pattern.compile("f.*"));
        try {
            builder.addSource(null, "source-2", null, null, null, "foo");
            fail("Should throw TopologyException with topic name/pattern conflict");
        } catch (final TopologyException expected) { /* ok */ }
    }

    @Test(expected = TopologyException.class)
    public void testAddStateStoreWithNonExistingProcessor() {
        builder.addStateStore(storeBuilder, "no-such-processor");
    }

    @Test
    public void testAddStateStoreWithSource() {
        builder.addSource(null, "source-1", null, null, null, "topic-1");
        try {
            builder.addStateStore(storeBuilder, "source-1");
            fail("Should throw TopologyException with store cannot be added to source");
        } catch (final TopologyException expected) { /* ok */ }
    }

    @Test
    public void testAddStateStoreWithSink() {
        builder.addSource(null, "source-1", null, null, null, "topic-1");
        builder.addSink("sink-1", "topic-1", null, null, null, "source-1");
        try {
            builder.addStateStore(storeBuilder, "sink-1");
            fail("Should throw TopologyException with store cannot be added to sink");
        } catch (final TopologyException expected) { /* ok */ }
    }

    @Test
    public void shouldNotAllowToAddStoresWithSameName() {
        final StoreBuilder<KeyValueStore<Object, Object>> otherBuilder =
            new MockKeyValueStoreBuilder("testStore", false);

        builder.addStateStore(storeBuilder);

        final TopologyException exception = assertThrows(
            TopologyException.class,
            () -> builder.addStateStore(otherBuilder)
        );

        assertThat(
            exception.getMessage(),
            equalTo("Invalid topology: A different StateStore has already been added with the name testStore")
        );
    }

    @Test
    public void shouldNotAllowToAddStoresWithSameNameWhenFirstStoreIsGlobal() {
        final StoreBuilder<KeyValueStore<Object, Object>> globalBuilder =
            new MockKeyValueStoreBuilder("testStore", false).withLoggingDisabled();

        builder.addGlobalStore(
            globalBuilder,
            "global-store",
            null,
            null,
            null,
            "global-topic",
            "global-processor",
            new MockApiProcessorSupplier<>()
        );

        final TopologyException exception = assertThrows(
            TopologyException.class,
            () -> builder.addStateStore(storeBuilder)
        );

        assertThat(
            exception.getMessage(),
            equalTo("Invalid topology: A different GlobalStateStore has already been added with the name testStore")
        );
    }

    @Test
    public void shouldNotAllowToAddStoresWithSameNameWhenSecondStoreIsGlobal() {
        final StoreBuilder<KeyValueStore<Object, Object>> globalBuilder =
            new MockKeyValueStoreBuilder("testStore", false).withLoggingDisabled();

        builder.addStateStore(storeBuilder);

        final TopologyException exception = assertThrows(
            TopologyException.class,
            () -> builder.addGlobalStore(
                globalBuilder,
                "global-store",
                null,
                null,
                null,
                "global-topic",
                "global-processor",
                new MockApiProcessorSupplier<>()
            )
        );

        assertThat(
            exception.getMessage(),
            equalTo("Invalid topology: A different StateStore has already been added with the name testStore")
        );
    }

    @Test
    public void shouldNotAllowToAddGlobalStoresWithSameName() {
        final StoreBuilder<KeyValueStore<Object, Object>> firstGlobalBuilder =
            new MockKeyValueStoreBuilder("testStore", false).withLoggingDisabled();
        final StoreBuilder<KeyValueStore<Object, Object>> secondGlobalBuilder =
            new MockKeyValueStoreBuilder("testStore", false).withLoggingDisabled();

        builder.addGlobalStore(
            firstGlobalBuilder,
            "global-store",
            null,
            null,
            null,
            "global-topic",
            "global-processor",
            new MockApiProcessorSupplier<>()
        );

        final TopologyException exception = assertThrows(
            TopologyException.class,
            () -> builder.addGlobalStore(
                secondGlobalBuilder,
                "global-store-2",
                null,
                null,
                null,
                "global-topic",
                "global-processor-2",
                new MockApiProcessorSupplier<>()
            )
        );

        assertThat(
            exception.getMessage(),
            equalTo("Invalid topology: A different GlobalStateStore has already been added with the name testStore")
        );
    }

    @Test
    public void testAddStateStore() {
        builder.addStateStore(storeBuilder);
        builder.setApplicationId("X");
        builder.addSource(null, "source-1", null, null, null, "topic-1");
        builder.addProcessor("processor-1", new MockApiProcessorSupplier<>(), "source-1");

        assertEquals(0, builder.buildTopology().stateStores().size());

        builder.connectProcessorAndStateStores("processor-1", storeBuilder.name());

        final List<StateStore> suppliers = builder.buildTopology().stateStores();
        assertEquals(1, suppliers.size());
        assertEquals(storeBuilder.name(), suppliers.get(0).name());
    }

    @Test
    public void shouldAllowAddingSameStoreBuilderMultipleTimes() {
        builder.setApplicationId("X");
        builder.addSource(null, "source-1", null, null, null, "topic-1");

        builder.addStateStore(storeBuilder);
        builder.addProcessor("processor-1", new MockApiProcessorSupplier<>(), "source-1");
        builder.connectProcessorAndStateStores("processor-1", storeBuilder.name());

        builder.addStateStore(storeBuilder);
        builder.addProcessor("processor-2", new MockApiProcessorSupplier<>(), "source-1");
        builder.connectProcessorAndStateStores("processor-2", storeBuilder.name());

        assertEquals(1, builder.buildTopology().stateStores().size());
    }

    @Test
    public void testTopicGroups() {
        builder.setApplicationId("X");
        builder.addInternalTopic("topic-1x", InternalTopicProperties.empty());
        builder.addSource(null, "source-1", null, null, null, "topic-1", "topic-1x");
        builder.addSource(null, "source-2", null, null, null, "topic-2");
        builder.addSource(null, "source-3", null, null, null, "topic-3");
        builder.addSource(null, "source-4", null, null, null, "topic-4");
        builder.addSource(null, "source-5", null, null, null, "topic-5");

        builder.addProcessor("processor-1", new MockApiProcessorSupplier<>(), "source-1");

        builder.addProcessor("processor-2", new MockApiProcessorSupplier<>(), "source-2", "processor-1");
        builder.copartitionSources(asList("source-1", "source-2"));

        builder.addProcessor("processor-3", new MockApiProcessorSupplier<>(), "source-3", "source-4");

        final Map<Integer, InternalTopologyBuilder.TopicsInfo> topicGroups = builder.topicGroups();

        final Map<Integer, InternalTopologyBuilder.TopicsInfo> expectedTopicGroups = new HashMap<>();
        expectedTopicGroups.put(0, new InternalTopologyBuilder.TopicsInfo(Collections.emptySet(), mkSet("topic-1", "X-topic-1x", "topic-2"), Collections.emptyMap(), Collections.emptyMap()));
        expectedTopicGroups.put(1, new InternalTopologyBuilder.TopicsInfo(Collections.emptySet(), mkSet("topic-3", "topic-4"), Collections.emptyMap(), Collections.emptyMap()));
        expectedTopicGroups.put(2, new InternalTopologyBuilder.TopicsInfo(Collections.emptySet(), mkSet("topic-5"), Collections.emptyMap(), Collections.emptyMap()));

        assertEquals(3, topicGroups.size());
        assertEquals(expectedTopicGroups, topicGroups);

        final Collection<Set<String>> copartitionGroups = builder.copartitionGroups();

        assertEquals(mkSet(mkSet("topic-1", "X-topic-1x", "topic-2")), new HashSet<>(copartitionGroups));
    }

    @Test
    public void testTopicGroupsByStateStore() {
        builder.setApplicationId("X");
        builder.addSource(null, "source-1", null, null, null, "topic-1", "topic-1x");
        builder.addSource(null, "source-2", null, null, null, "topic-2");
        builder.addSource(null, "source-3", null, null, null, "topic-3");
        builder.addSource(null, "source-4", null, null, null, "topic-4");
        builder.addSource(null, "source-5", null, null, null, "topic-5");

        builder.addProcessor("processor-1", new MockApiProcessorSupplier<>(), "source-1");
        builder.addProcessor("processor-2", new MockApiProcessorSupplier<>(), "source-2");
        builder.addStateStore(new MockKeyValueStoreBuilder("store-1", false), "processor-1", "processor-2");

        builder.addProcessor("processor-3", new MockApiProcessorSupplier<>(), "source-3");
        builder.addProcessor("processor-4", new MockApiProcessorSupplier<>(), "source-4");
        builder.addStateStore(new MockKeyValueStoreBuilder("store-2", false), "processor-3", "processor-4");

        builder.addProcessor("processor-5", new MockApiProcessorSupplier<>(), "source-5");
        builder.addStateStore(new MockKeyValueStoreBuilder("store-3", false));
        builder.connectProcessorAndStateStores("processor-5", "store-3");
        builder.buildTopology();

        final Map<Integer, InternalTopologyBuilder.TopicsInfo> topicGroups = builder.topicGroups();

        final Map<Integer, InternalTopologyBuilder.TopicsInfo> expectedTopicGroups = new HashMap<>();
        final String store1 = ProcessorStateManager.storeChangelogTopic("X", "store-1");
        final String store2 = ProcessorStateManager.storeChangelogTopic("X", "store-2");
        final String store3 = ProcessorStateManager.storeChangelogTopic("X", "store-3");
        expectedTopicGroups.put(0, new InternalTopologyBuilder.TopicsInfo(
            Collections.emptySet(), mkSet("topic-1", "topic-1x", "topic-2"),
            Collections.emptyMap(),
            Collections.singletonMap(store1, new UnwindowedChangelogTopicConfig(store1, Collections.emptyMap()))));
        expectedTopicGroups.put(1, new InternalTopologyBuilder.TopicsInfo(
            Collections.emptySet(), mkSet("topic-3", "topic-4"),
            Collections.emptyMap(),
            Collections.singletonMap(store2, new UnwindowedChangelogTopicConfig(store2, Collections.emptyMap()))));
        expectedTopicGroups.put(2, new InternalTopologyBuilder.TopicsInfo(
            Collections.emptySet(), mkSet("topic-5"),
            Collections.emptyMap(),
            Collections.singletonMap(store3, new UnwindowedChangelogTopicConfig(store3, Collections.emptyMap()))));

        assertEquals(3, topicGroups.size());
        assertEquals(expectedTopicGroups, topicGroups);
    }

    @Test
    public void testBuild() {
        builder.addSource(null, "source-1", null, null, null, "topic-1", "topic-1x");
        builder.addSource(null, "source-2", null, null, null, "topic-2");
        builder.addSource(null, "source-3", null, null, null, "topic-3");
        builder.addSource(null, "source-4", null, null, null, "topic-4");
        builder.addSource(null, "source-5", null, null, null, "topic-5");

        builder.addProcessor("processor-1", new MockApiProcessorSupplier<>(), "source-1");
        builder.addProcessor("processor-2", new MockApiProcessorSupplier<>(), "source-2", "processor-1");
        builder.addProcessor("processor-3", new MockApiProcessorSupplier<>(), "source-3", "source-4");

        builder.setApplicationId("X");
        final ProcessorTopology topology0 = builder.buildSubtopology(0);
        final ProcessorTopology topology1 = builder.buildSubtopology(1);
        final ProcessorTopology topology2 = builder.buildSubtopology(2);

        assertEquals(mkSet("source-1", "source-2", "processor-1", "processor-2"), nodeNames(topology0.processors()));
        assertEquals(mkSet("source-3", "source-4", "processor-3"), nodeNames(topology1.processors()));
        assertEquals(mkSet("source-5"), nodeNames(topology2.processors()));
    }

    @Test
    public void shouldAllowIncrementalBuilds() {
        Map<Integer, Set<String>> oldNodeGroups, newNodeGroups;

        oldNodeGroups = builder.nodeGroups();
        builder.addSource(null, "source-1", null, null, null, "topic-1");
        builder.addSource(null, "source-2", null, null, null, "topic-2");
        newNodeGroups = builder.nodeGroups();
        assertNotEquals(oldNodeGroups, newNodeGroups);

        oldNodeGroups = newNodeGroups;
        builder.addSource(null, "source-3", null, null, null, Pattern.compile(""));
        builder.addSource(null, "source-4", null, null, null, Pattern.compile(""));
        newNodeGroups = builder.nodeGroups();
        assertNotEquals(oldNodeGroups, newNodeGroups);

        oldNodeGroups = newNodeGroups;
        builder.addProcessor("processor-1", new MockApiProcessorSupplier<>(), "source-1");
        builder.addProcessor("processor-2", new MockApiProcessorSupplier<>(), "source-2");
        builder.addProcessor("processor-3", new MockApiProcessorSupplier<>(), "source-3");
        newNodeGroups = builder.nodeGroups();
        assertNotEquals(oldNodeGroups, newNodeGroups);

        oldNodeGroups = newNodeGroups;
        builder.addSink("sink-1", "sink-topic", null, null, null, "processor-1");
        newNodeGroups = builder.nodeGroups();
        assertNotEquals(oldNodeGroups, newNodeGroups);

        oldNodeGroups = newNodeGroups;
        builder.addSink("sink-2", (k, v, ctx) -> "sink-topic", null, null, null, "processor-2");
        newNodeGroups = builder.nodeGroups();
        assertNotEquals(oldNodeGroups, newNodeGroups);

        oldNodeGroups = newNodeGroups;
        builder.addStateStore(new MockKeyValueStoreBuilder("store-1", false), "processor-1", "processor-2");
        newNodeGroups = builder.nodeGroups();
        assertNotEquals(oldNodeGroups, newNodeGroups);

        oldNodeGroups = newNodeGroups;
        builder.addStateStore(new MockKeyValueStoreBuilder("store-2", false));
        builder.connectProcessorAndStateStores("processor-2", "store-2");
        builder.connectProcessorAndStateStores("processor-3", "store-2");
        newNodeGroups = builder.nodeGroups();
        assertNotEquals(oldNodeGroups, newNodeGroups);

        oldNodeGroups = newNodeGroups;
        builder.addGlobalStore(
            new MockKeyValueStoreBuilder("global-store", false).withLoggingDisabled(),
            "globalSource",
            null,
            null,
            null,
            "globalTopic",
            "global-processor",
            new MockApiProcessorSupplier<>()
        );
        newNodeGroups = builder.nodeGroups();
        assertNotEquals(oldNodeGroups, newNodeGroups);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullNameWhenAddingSink() {
        builder.addSink(null, "topic", null, null, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullTopicWhenAddingSink() {
        builder.addSink("name", (String) null, null, null, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullTopicChooserWhenAddingSink() {
        builder.addSink("name", (TopicNameExtractor<Object, Object>) null, null, null, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullNameWhenAddingProcessor() {
        builder.addProcessor(null, () -> null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullProcessorSupplier() {
        builder.addProcessor("name", null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullNameWhenAddingSource() {
        builder.addSource(null, null, null, null, null, Pattern.compile(".*"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullProcessorNameWhenConnectingProcessorAndStateStores() {
        builder.connectProcessorAndStateStores(null, "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullStateStoreNameWhenConnectingProcessorAndStateStores() {
        builder.connectProcessorAndStateStores("processor", new String[]{null});
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAddNullInternalTopic() {
        builder.addInternalTopic(null, InternalTopicProperties.empty());
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAddNullInternalTopicProperties() {
        builder.addInternalTopic("topic", null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotSetApplicationIdToNull() {
        builder.setApplicationId(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAddNullStateStoreSupplier() {
        builder.addStateStore(null);
    }

    private Set<String> nodeNames(final Collection<ProcessorNode<?, ?, ?, ?>> nodes) {
        final Set<String> nodeNames = new HashSet<>();
        for (final ProcessorNode<?, ?, ?, ?> node : nodes) {
            nodeNames.add(node.name());
        }
        return nodeNames;
    }

    @Test
    public void shouldAssociateStateStoreNameWhenStateStoreSupplierIsInternal() {
        builder.addSource(null, "source", null, null, null, "topic");
        builder.addProcessor("processor", new MockApiProcessorSupplier<>(), "source");
        builder.addStateStore(storeBuilder, "processor");
        final Map<String, List<String>> stateStoreNameToSourceTopic = builder.stateStoreNameToSourceTopics();
        assertEquals(1, stateStoreNameToSourceTopic.size());
        assertEquals(Collections.singletonList("topic"), stateStoreNameToSourceTopic.get("testStore"));
    }

    @Test
    public void shouldAssociateStateStoreNameWhenStateStoreSupplierIsExternal() {
        builder.addSource(null, "source", null, null, null, "topic");
        builder.addProcessor("processor", new MockApiProcessorSupplier<>(), "source");
        builder.addStateStore(storeBuilder, "processor");
        final Map<String, List<String>> stateStoreNameToSourceTopic = builder.stateStoreNameToSourceTopics();
        assertEquals(1, stateStoreNameToSourceTopic.size());
        assertEquals(Collections.singletonList("topic"), stateStoreNameToSourceTopic.get("testStore"));
    }

    @Test
    public void shouldCorrectlyMapStateStoreToInternalTopics() {
        builder.setApplicationId("appId");
        builder.addInternalTopic("internal-topic", InternalTopicProperties.empty());
        builder.addSource(null, "source", null, null, null, "internal-topic");
        builder.addProcessor("processor", new MockApiProcessorSupplier<>(), "source");
        builder.addStateStore(storeBuilder, "processor");
        final Map<String, List<String>> stateStoreNameToSourceTopic = builder.stateStoreNameToSourceTopics();
        assertEquals(1, stateStoreNameToSourceTopic.size());
        assertEquals(Collections.singletonList("appId-internal-topic"), stateStoreNameToSourceTopic.get("testStore"));
    }

    @Test
    public void shouldAddInternalTopicConfigForWindowStores() {
        builder.setApplicationId("appId");
        builder.addSource(null, "source", null, null, null, "topic");
        builder.addProcessor("processor", new MockApiProcessorSupplier<>(), "source");
        builder.addStateStore(
            Stores.windowStoreBuilder(
                Stores.persistentWindowStore("store1", ofSeconds(30L), ofSeconds(10L), false),
                Serdes.String(),
                Serdes.String()
            ),
            "processor"
        );
        builder.addStateStore(
                Stores.sessionStoreBuilder(
                        Stores.persistentSessionStore("store2", ofSeconds(30)), Serdes.String(), Serdes.String()
                ),
                "processor"
        );
        builder.buildTopology();
        final Map<Integer, InternalTopologyBuilder.TopicsInfo> topicGroups = builder.topicGroups();
        final InternalTopologyBuilder.TopicsInfo topicsInfo = topicGroups.values().iterator().next();
        final InternalTopicConfig topicConfig1 = topicsInfo.stateChangelogTopics.get("appId-store1-changelog");
        final Map<String, String> properties1 = topicConfig1.getProperties(Collections.emptyMap(), 10000);
        assertEquals(3, properties1.size());
        assertEquals(TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE, properties1.get(TopicConfig.CLEANUP_POLICY_CONFIG));
        assertEquals("40000", properties1.get(TopicConfig.RETENTION_MS_CONFIG));
        assertEquals("appId-store1-changelog", topicConfig1.name());
        assertTrue(topicConfig1 instanceof WindowedChangelogTopicConfig);
        final InternalTopicConfig topicConfig2 = topicsInfo.stateChangelogTopics.get("appId-store2-changelog");
        final Map<String, String> properties2 = topicConfig2.getProperties(Collections.emptyMap(), 10000);
        assertEquals(3, properties2.size());
        assertEquals(TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE, properties2.get(TopicConfig.CLEANUP_POLICY_CONFIG));
        assertEquals("40000", properties2.get(TopicConfig.RETENTION_MS_CONFIG));
        assertEquals("appId-store2-changelog", topicConfig2.name());
        assertTrue(topicConfig2 instanceof WindowedChangelogTopicConfig);
    }

    @Test
    public void shouldAddInternalTopicConfigForNonWindowStores() {
        builder.setApplicationId("appId");
        builder.addSource(null, "source", null, null, null, "topic");
        builder.addProcessor("processor", new MockApiProcessorSupplier<>(), "source");
        builder.addStateStore(storeBuilder, "processor");
        builder.buildTopology();
        final Map<Integer, InternalTopologyBuilder.TopicsInfo> topicGroups = builder.topicGroups();
        final InternalTopologyBuilder.TopicsInfo topicsInfo = topicGroups.values().iterator().next();
        final InternalTopicConfig topicConfig = topicsInfo.stateChangelogTopics.get("appId-testStore-changelog");
        final Map<String, String> properties = topicConfig.getProperties(Collections.emptyMap(), 10000);
        assertEquals(2, properties.size());
        assertEquals(TopicConfig.CLEANUP_POLICY_COMPACT, properties.get(TopicConfig.CLEANUP_POLICY_CONFIG));
        assertEquals("appId-testStore-changelog", topicConfig.name());
        assertTrue(topicConfig instanceof UnwindowedChangelogTopicConfig);
    }

    @Test
    public void shouldAddInternalTopicConfigForRepartitionTopics() {
        builder.setApplicationId("appId");
        builder.addInternalTopic("foo", InternalTopicProperties.empty());
        builder.addSource(null, "source", null, null, null, "foo");
        builder.buildTopology();
        final InternalTopologyBuilder.TopicsInfo topicsInfo = builder.topicGroups().values().iterator().next();
        final InternalTopicConfig topicConfig = topicsInfo.repartitionSourceTopics.get("appId-foo");
        final Map<String, String> properties = topicConfig.getProperties(Collections.emptyMap(), 10000);
        assertEquals(4, properties.size());
        assertEquals(String.valueOf(-1), properties.get(TopicConfig.RETENTION_MS_CONFIG));
        assertEquals(TopicConfig.CLEANUP_POLICY_DELETE, properties.get(TopicConfig.CLEANUP_POLICY_CONFIG));
        assertEquals("appId-foo", topicConfig.name());
        assertTrue(topicConfig instanceof RepartitionTopicConfig);
    }

    @Test
    public void shouldSetCorrectSourceNodesWithRegexUpdatedTopics() {
        builder.addSource(null, "source-1", null, null, null, "topic-foo");
        builder.addSource(null, "source-2", null, null, null, Pattern.compile("topic-[A-C]"));
        builder.addSource(null, "source-3", null, null, null, Pattern.compile("topic-\\d"));

        final Set<String> updatedTopics = new HashSet<>();

        updatedTopics.add("topic-B");
        updatedTopics.add("topic-3");
        updatedTopics.add("topic-A");

        builder.addSubscribedTopicsFromMetadata(updatedTopics, null);
        builder.setApplicationId("test-id");

        final Map<Integer, InternalTopologyBuilder.TopicsInfo> topicGroups = builder.topicGroups();
        assertTrue(topicGroups.get(0).sourceTopics.contains("topic-foo"));
        assertTrue(topicGroups.get(1).sourceTopics.contains("topic-A"));
        assertTrue(topicGroups.get(1).sourceTopics.contains("topic-B"));
        assertTrue(topicGroups.get(2).sourceTopics.contains("topic-3"));
    }

    @Test
    public void shouldAddTimestampExtractorPerSource() {
        builder.addSource(null, "source", new MockTimestampExtractor(), null, null, "topic");
        final ProcessorTopology processorTopology = builder.rewriteTopology(new StreamsConfig(StreamsTestUtils.getStreamsConfig())).buildTopology();
        assertThat(processorTopology.source("topic").getTimestampExtractor(), instanceOf(MockTimestampExtractor.class));
    }

    @Test
    public void shouldAddTimestampExtractorWithPatternPerSource() {
        final Pattern pattern = Pattern.compile("t.*");
        builder.addSource(null, "source", new MockTimestampExtractor(), null, null, pattern);
        final ProcessorTopology processorTopology = builder.rewriteTopology(new StreamsConfig(StreamsTestUtils.getStreamsConfig())).buildTopology();
        assertThat(processorTopology.source(pattern.pattern()).getTimestampExtractor(), instanceOf(MockTimestampExtractor.class));
    }

    @Test
    public void shouldSortProcessorNodesCorrectly() {
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addSource(null, "source2", null, null, null, "topic2");
        builder.addProcessor("processor1", new MockApiProcessorSupplier<>(), "source1");
        builder.addProcessor("processor2", new MockApiProcessorSupplier<>(), "source1", "source2");
        builder.addProcessor("processor3", new MockApiProcessorSupplier<>(), "processor2");
        builder.addSink("sink1", "topic2", null, null, null, "processor1", "processor3");

        assertEquals(1, builder.describe().subtopologies().size());

        final Iterator<TopologyDescription.Node> iterator = ((InternalTopologyBuilder.Subtopology) builder.describe().subtopologies().iterator().next()).nodesInOrder();

        assertTrue(iterator.hasNext());
        InternalTopologyBuilder.AbstractNode node = (InternalTopologyBuilder.AbstractNode) iterator.next();
        assertEquals("source1", node.name);
        assertEquals(6, node.size);

        assertTrue(iterator.hasNext());
        node = (InternalTopologyBuilder.AbstractNode) iterator.next();
        assertEquals("source2", node.name);
        assertEquals(4, node.size);

        assertTrue(iterator.hasNext());
        node = (InternalTopologyBuilder.AbstractNode) iterator.next();
        assertEquals("processor2", node.name);
        assertEquals(3, node.size);

        assertTrue(iterator.hasNext());
        node = (InternalTopologyBuilder.AbstractNode) iterator.next();
        assertEquals("processor1", node.name);
        assertEquals(2, node.size);

        assertTrue(iterator.hasNext());
        node = (InternalTopologyBuilder.AbstractNode) iterator.next();
        assertEquals("processor3", node.name);
        assertEquals(2, node.size);

        assertTrue(iterator.hasNext());
        node = (InternalTopologyBuilder.AbstractNode) iterator.next();
        assertEquals("sink1", node.name);
        assertEquals(1, node.size);
    }

    @Test
    public void shouldConnectRegexMatchedTopicsToStateStore() {
        builder.addSource(null, "ingest", null, null, null, Pattern.compile("topic-\\d+"));
        builder.addProcessor("my-processor", new MockApiProcessorSupplier<>(), "ingest");
        builder.addStateStore(storeBuilder, "my-processor");

        final Set<String> updatedTopics = new HashSet<>();

        updatedTopics.add("topic-2");
        updatedTopics.add("topic-3");
        updatedTopics.add("topic-A");

        builder.addSubscribedTopicsFromMetadata(updatedTopics, "test-thread");
        builder.setApplicationId("test-app");

        final Map<String, List<String>> stateStoreAndTopics = builder.stateStoreNameToSourceTopics();
        final List<String> topics = stateStoreAndTopics.get(storeBuilder.name());

        assertEquals("Expected to contain two topics", 2, topics.size());

        assertTrue(topics.contains("topic-2"));
        assertTrue(topics.contains("topic-3"));
        assertFalse(topics.contains("topic-A"));
    }

    @Test(expected = TopologyException.class)
    public void shouldNotAllowToAddGlobalStoreWithSourceNameEqualsProcessorName() {
        final String sameNameForSourceAndProcessor = "sameName";
        builder.addGlobalStore(
            storeBuilder,
            sameNameForSourceAndProcessor,
            null,
            null,
            null,
            "anyTopicName",
            sameNameForSourceAndProcessor,
            new MockApiProcessorSupplier<>()
        );
    }

    @Test
    public void shouldThrowIfNameIsNull() {
        final Exception e = assertThrows(NullPointerException.class, () -> new InternalTopologyBuilder.Source(null, Collections.emptySet(), null));
        assertEquals("name cannot be null", e.getMessage());
    }

    @Test
    public void shouldThrowIfTopicAndPatternAreNull() {
        final Exception e = assertThrows(IllegalArgumentException.class, () -> new InternalTopologyBuilder.Source("name", null, null));
        assertEquals("Either topics or pattern must be not-null, but both are null.", e.getMessage());
    }

    @Test
    public void shouldThrowIfBothTopicAndPatternAreNotNull() {
        final Exception e = assertThrows(IllegalArgumentException.class, () -> new InternalTopologyBuilder.Source("name", Collections.emptySet(), Pattern.compile("")));
        assertEquals("Either topics or pattern must be null, but both are not null.", e.getMessage());
    }

    @Test
    public void sourceShouldBeEqualIfNameAndTopicListAreTheSame() {
        final InternalTopologyBuilder.Source base = new InternalTopologyBuilder.Source("name", Collections.singleton("topic"), null);
        final InternalTopologyBuilder.Source sameAsBase = new InternalTopologyBuilder.Source("name", Collections.singleton("topic"), null);

        assertThat(base, equalTo(sameAsBase));
    }

    @Test
    public void sourceShouldBeEqualIfNameAndPatternAreTheSame() {
        final InternalTopologyBuilder.Source base = new InternalTopologyBuilder.Source("name", null, Pattern.compile("topic"));
        final InternalTopologyBuilder.Source sameAsBase = new InternalTopologyBuilder.Source("name", null, Pattern.compile("topic"));

        assertThat(base, equalTo(sameAsBase));
    }

    @Test
    public void sourceShouldNotBeEqualForDifferentNamesWithSameTopicList() {
        final InternalTopologyBuilder.Source base = new InternalTopologyBuilder.Source("name", Collections.singleton("topic"), null);
        final InternalTopologyBuilder.Source differentName = new InternalTopologyBuilder.Source("name2", Collections.singleton("topic"), null);

        assertThat(base, not(equalTo(differentName)));
    }

    @Test
    public void sourceShouldNotBeEqualForDifferentNamesWithSamePattern() {
        final InternalTopologyBuilder.Source base = new InternalTopologyBuilder.Source("name", null, Pattern.compile("topic"));
        final InternalTopologyBuilder.Source differentName = new InternalTopologyBuilder.Source("name2", null, Pattern.compile("topic"));

        assertThat(base, not(equalTo(differentName)));
    }

    @Test
    public void sourceShouldNotBeEqualForDifferentTopicList() {
        final InternalTopologyBuilder.Source base = new InternalTopologyBuilder.Source("name", Collections.singleton("topic"), null);
        final InternalTopologyBuilder.Source differentTopicList = new InternalTopologyBuilder.Source("name", Collections.emptySet(), null);
        final InternalTopologyBuilder.Source differentTopic = new InternalTopologyBuilder.Source("name", Collections.singleton("topic2"), null);

        assertThat(base, not(equalTo(differentTopicList)));
        assertThat(base, not(equalTo(differentTopic)));
    }

    @Test
    public void sourceShouldNotBeEqualForDifferentPattern() {
        final InternalTopologyBuilder.Source base = new InternalTopologyBuilder.Source("name", null, Pattern.compile("topic"));
        final InternalTopologyBuilder.Source differentPattern = new InternalTopologyBuilder.Source("name", null, Pattern.compile("topic2"));
        final InternalTopologyBuilder.Source overlappingPattern = new InternalTopologyBuilder.Source("name", null, Pattern.compile("top*"));

        assertThat(base, not(equalTo(differentPattern)));
        assertThat(base, not(equalTo(overlappingPattern)));
    }

    @Test
    public void shouldHaveCorrectInternalTopicConfigWhenInternalTopicPropertiesArePresent() {
        final int numberOfPartitions = 10;
        builder.setApplicationId("Z");
        builder.addInternalTopic("topic-1z", new InternalTopicProperties(numberOfPartitions));
        builder.addSource(null, "source-1", null, null, null, "topic-1z");

        final Map<Integer, InternalTopologyBuilder.TopicsInfo> topicGroups = builder.topicGroups();

        final Map<String, InternalTopicConfig> repartitionSourceTopics = topicGroups.get(0).repartitionSourceTopics;

        assertEquals(
            repartitionSourceTopics.get("Z-topic-1z"),
            new RepartitionTopicConfig(
                "Z-topic-1z",
                Collections.emptyMap(),
                numberOfPartitions,
                true
            )
        );
    }

    @Test
    public void shouldHandleWhenTopicPropertiesNumberOfPartitionsIsNull() {
        builder.setApplicationId("T");
        builder.addInternalTopic("topic-1t", InternalTopicProperties.empty());
        builder.addSource(null, "source-1", null, null, null, "topic-1t");

        final Map<Integer, InternalTopologyBuilder.TopicsInfo> topicGroups = builder.topicGroups();

        final Map<String, InternalTopicConfig> repartitionSourceTopics = topicGroups.get(0).repartitionSourceTopics;

        assertEquals(
            repartitionSourceTopics.get("T-topic-1t"),
            new RepartitionTopicConfig(
                "T-topic-1t",
                Collections.emptyMap()
            )
        );
    }

    @Test
    public void shouldHaveCorrectInternalTopicConfigWhenInternalTopicPropertiesAreNotPresent() {
        builder.setApplicationId("Y");
        builder.addInternalTopic("topic-1y", InternalTopicProperties.empty());
        builder.addSource(null, "source-1", null, null, null, "topic-1y");

        final Map<Integer, InternalTopologyBuilder.TopicsInfo> topicGroups = builder.topicGroups();

        final Map<String, InternalTopicConfig> repartitionSourceTopics = topicGroups.get(0).repartitionSourceTopics;

        assertEquals(
            repartitionSourceTopics.get("Y-topic-1y"),
            new RepartitionTopicConfig("Y-topic-1y", Collections.emptyMap())
        );
    }

    @Test
    public void shouldConnectGlobalStateStoreToInputTopic() {
        final String globalStoreName = "global-store";
        final String globalTopic = "global-topic";
        builder.setApplicationId("X");
        builder.addGlobalStore(
            new MockKeyValueStoreBuilder(globalStoreName, false).withLoggingDisabled(),
            "globalSource",
            null,
            null,
            null,
            globalTopic,
            "global-processor",
            new MockApiProcessorSupplier<>()
        );
        builder.initializeSubscription();

        builder.rewriteTopology(new StreamsConfig(mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "asdf"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "asdf")
        ))));

        assertThat(builder.buildGlobalStateTopology().storeToChangelogTopic().get(globalStoreName), is(globalTopic));
    }
}
