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
import org.apache.kafka.streams.TopologyTestDriverWrapper;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.RocksDBWindowStoreSupplier;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockStateStoreSupplier;
import org.apache.kafka.test.MockTimestampExtractor;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.kafka.common.utils.Utils.mkList;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class InternalTopologyBuilderTest {

    private final InternalTopologyBuilder builder = new InternalTopologyBuilder();
    private final Serde<String> stringSerde = Serdes.String();

    @Test
    public void shouldAddSourceWithOffsetReset() {
        final String earliestTopic = "earliestTopic";
        final String latestTopic = "latestTopic";

        builder.addSource(Topology.AutoOffsetReset.EARLIEST, "source", null, null, null, earliestTopic);
        builder.addSource(Topology.AutoOffsetReset.LATEST, "source2", null, null, null, latestTopic);

        assertTrue(builder.earliestResetTopicsPattern().matcher(earliestTopic).matches());
        assertTrue(builder.latestResetTopicsPattern().matcher(latestTopic).matches());
    }

    @Test
    public void shouldAddSourcePatternWithOffsetReset() {
        final String earliestTopicPattern = "earliest.*Topic";
        final String latestTopicPattern = "latest.*Topic";

        builder.addSource(Topology.AutoOffsetReset.EARLIEST, "source", null, null, null, Pattern.compile(earliestTopicPattern));
        builder.addSource(Topology.AutoOffsetReset.LATEST, "source2", null, null, null,  Pattern.compile(latestTopicPattern));

        assertTrue(builder.earliestResetTopicsPattern().matcher("earliestTestTopic").matches());
        assertTrue(builder.latestResetTopicsPattern().matcher("latestTestTopic").matches());
    }

    @Test
    public void shouldAddSourceWithoutOffsetReset() {
        final Pattern expectedPattern = Pattern.compile("test-topic");

        builder.addSource(null, "source", null, stringSerde.deserializer(), stringSerde.deserializer(), "test-topic");

        assertEquals(expectedPattern.pattern(), builder.sourceTopicPattern().pattern());
        assertEquals(builder.earliestResetTopicsPattern().pattern(), "");
        assertEquals(builder.latestResetTopicsPattern().pattern(), "");
    }

    @Test
    public void shouldAddPatternSourceWithoutOffsetReset() {
        final Pattern expectedPattern = Pattern.compile("test-.*");
        
        builder.addSource(null, "source", null, stringSerde.deserializer(), stringSerde.deserializer(), Pattern.compile("test-.*"));

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
        builder.addProcessor("processor", new MockProcessorSupplier(), "source");
        try {
            builder.addProcessor("processor", new MockProcessorSupplier(), "source");
            fail("Should throw TopologyException with processor name conflict");
        } catch (final TopologyException expected) { /* ok */ }
    }

    @Test(expected = TopologyException.class)
    public void testAddProcessorWithWrongParent() {
        builder.addProcessor("processor", new MockProcessorSupplier(), "source");
    }

    @Test(expected = TopologyException.class)
    public void testAddProcessorWithSelfParent() {
        builder.addProcessor("processor", new MockProcessorSupplier(), "processor");
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
    public void testSourceTopics() {
        builder.setApplicationId("X");
        builder.addSource(null, "source-1", null, null, null, "topic-1");
        builder.addSource(null, "source-2", null, null, null, "topic-2");
        builder.addSource(null, "source-3", null, null, null, "topic-3");
        builder.addInternalTopic("topic-3");

        final Pattern expectedPattern = Pattern.compile("X-topic-3|topic-1|topic-2");

        assertEquals(expectedPattern.pattern(), builder.sourceTopicPattern().pattern());
    }

    @Test
    public void testPatternSourceTopic() {
        final Pattern expectedPattern = Pattern.compile("topic-\\d");
        builder.addSource(null, "source-1", null, null, null, expectedPattern);
        assertEquals(expectedPattern.pattern(), builder.sourceTopicPattern().pattern());
    }

    @Test
    public void testAddMoreThanOnePatternSourceNode() {
        final Pattern expectedPattern = Pattern.compile("topics[A-Z]|.*-\\d");
        builder.addSource(null, "source-1", null, null, null, Pattern.compile("topics[A-Z]"));
        builder.addSource(null, "source-2", null, null, null, Pattern.compile(".*-\\d"));
        assertEquals(expectedPattern.pattern(), builder.sourceTopicPattern().pattern());
    }

    @Test
    public void testSubscribeTopicNameAndPattern() {
        final Pattern expectedPattern = Pattern.compile("topic-bar|topic-foo|.*-\\d");
        builder.addSource(null, "source-1", null, null, null, "topic-foo", "topic-bar");
        builder.addSource(null, "source-2", null, null, null, Pattern.compile(".*-\\d"));
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
        builder.addStateStore(new MockStateStoreSupplier("store", false), "no-such-processsor");
    }

    @Test
    public void testAddStateStoreWithSource() {
        builder.addSource(null, "source-1", null, null, null, "topic-1");
        try {
            builder.addStateStore(new MockStateStoreSupplier("store", false), "source-1");
            fail("Should throw TopologyException with store cannot be added to source");
        } catch (final TopologyException expected) { /* ok */ }
    }

    @Test
    public void testAddStateStoreWithSink() {
        builder.addSink("sink-1", "topic-1", null, null, null);
        try {
            builder.addStateStore(new MockStateStoreSupplier("store", false), "sink-1");
            fail("Should throw TopologyException with store cannot be added to sink");
        } catch (final TopologyException expected) { /* ok */ }
    }

    @Test
    public void testAddStateStoreWithDuplicates() {
        builder.addStateStore(new MockStateStoreSupplier("store", false));
        try {
            builder.addStateStore(new MockStateStoreSupplier("store", false));
            fail("Should throw TopologyException with store name conflict");
        } catch (final TopologyException expected) { /* ok */ }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testAddStateStore() {
        final StateStoreSupplier supplier = new MockStateStoreSupplier("store-1", false);
        builder.addStateStore(supplier);
        builder.setApplicationId("X");
        builder.addSource(null, "source-1", null, null, null, "topic-1");
        builder.addProcessor("processor-1", new MockProcessorSupplier(), "source-1");

        assertEquals(0, builder.build(null).stateStores().size());

        builder.connectProcessorAndStateStores("processor-1", "store-1");

        final List<StateStore> suppliers = builder.build(null).stateStores();
        assertEquals(1, suppliers.size());
        assertEquals(supplier.name(), suppliers.get(0).name());
    }

    @Test
    public void testTopicGroups() {
        builder.setApplicationId("X");
        builder.addInternalTopic("topic-1x");
        builder.addSource(null, "source-1", null, null, null, "topic-1", "topic-1x");
        builder.addSource(null, "source-2", null, null, null, "topic-2");
        builder.addSource(null, "source-3", null, null, null, "topic-3");
        builder.addSource(null, "source-4", null, null, null, "topic-4");
        builder.addSource(null, "source-5", null, null, null, "topic-5");

        builder.addProcessor("processor-1", new MockProcessorSupplier(), "source-1");

        builder.addProcessor("processor-2", new MockProcessorSupplier(), "source-2", "processor-1");
        builder.copartitionSources(mkList("source-1", "source-2"));

        builder.addProcessor("processor-3", new MockProcessorSupplier(), "source-3", "source-4");

        final Map<Integer, InternalTopologyBuilder.TopicsInfo> topicGroups = builder.topicGroups();

        final Map<Integer, InternalTopologyBuilder.TopicsInfo> expectedTopicGroups = new HashMap<>();
        expectedTopicGroups.put(0, new InternalTopologyBuilder.TopicsInfo(Collections.<String>emptySet(), mkSet("topic-1", "X-topic-1x", "topic-2"), Collections.<String, InternalTopicConfig>emptyMap(), Collections.<String, InternalTopicConfig>emptyMap()));
        expectedTopicGroups.put(1, new InternalTopologyBuilder.TopicsInfo(Collections.<String>emptySet(), mkSet("topic-3", "topic-4"), Collections.<String, InternalTopicConfig>emptyMap(), Collections.<String, InternalTopicConfig>emptyMap()));
        expectedTopicGroups.put(2, new InternalTopologyBuilder.TopicsInfo(Collections.<String>emptySet(), mkSet("topic-5"), Collections.<String, InternalTopicConfig>emptyMap(), Collections.<String, InternalTopicConfig>emptyMap()));

        assertEquals(3, topicGroups.size());
        assertEquals(expectedTopicGroups, topicGroups);

        final Collection<Set<String>> copartitionGroups = builder.copartitionGroups();

        assertEquals(mkSet(mkSet("topic-1", "X-topic-1x", "topic-2")), new HashSet<>(copartitionGroups));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testTopicGroupsByStateStore() {
        builder.setApplicationId("X");
        builder.addSource(null, "source-1", null, null, null, "topic-1", "topic-1x");
        builder.addSource(null, "source-2", null, null, null, "topic-2");
        builder.addSource(null, "source-3", null, null, null, "topic-3");
        builder.addSource(null, "source-4", null, null, null, "topic-4");
        builder.addSource(null, "source-5", null, null, null, "topic-5");

        builder.addProcessor("processor-1", new MockProcessorSupplier(), "source-1");
        builder.addProcessor("processor-2", new MockProcessorSupplier(), "source-2");
        builder.addStateStore(new MockStateStoreSupplier("store-1", false), "processor-1", "processor-2");

        builder.addProcessor("processor-3", new MockProcessorSupplier(), "source-3");
        builder.addProcessor("processor-4", new MockProcessorSupplier(), "source-4");
        builder.addStateStore(new MockStateStoreSupplier("store-2", false), "processor-3", "processor-4");

        builder.addProcessor("processor-5", new MockProcessorSupplier(), "source-5");
        final StateStoreSupplier supplier = new MockStateStoreSupplier("store-3", false);
        builder.addStateStore(supplier);
        builder.connectProcessorAndStateStores("processor-5", "store-3");

        final Map<Integer, InternalTopologyBuilder.TopicsInfo> topicGroups = builder.topicGroups();

        final Map<Integer, InternalTopologyBuilder.TopicsInfo> expectedTopicGroups = new HashMap<>();
        final String store1 = ProcessorStateManager.storeChangelogTopic("X", "store-1");
        final String store2 = ProcessorStateManager.storeChangelogTopic("X", "store-2");
        final String store3 = ProcessorStateManager.storeChangelogTopic("X", "store-3");
        expectedTopicGroups.put(0, new InternalTopologyBuilder.TopicsInfo(
            Collections.<String>emptySet(), mkSet("topic-1", "topic-1x", "topic-2"),
            Collections.<String, InternalTopicConfig>emptyMap(),
            Collections.singletonMap(store1, (InternalTopicConfig) new UnwindowedChangelogTopicConfig(store1, Collections.<String, String>emptyMap()))));
        expectedTopicGroups.put(1, new InternalTopologyBuilder.TopicsInfo(
            Collections.<String>emptySet(), mkSet("topic-3", "topic-4"),
            Collections.<String, InternalTopicConfig>emptyMap(),
            Collections.singletonMap(store2, (InternalTopicConfig) new UnwindowedChangelogTopicConfig(store2, Collections.<String, String>emptyMap()))));
        expectedTopicGroups.put(2, new InternalTopologyBuilder.TopicsInfo(
            Collections.<String>emptySet(), mkSet("topic-5"),
            Collections.<String, InternalTopicConfig>emptyMap(),
            Collections.singletonMap(store3, (InternalTopicConfig) new UnwindowedChangelogTopicConfig(store3, Collections.<String, String>emptyMap()))));

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

        builder.addProcessor("processor-1", new MockProcessorSupplier(), "source-1");
        builder.addProcessor("processor-2", new MockProcessorSupplier(), "source-2", "processor-1");
        builder.addProcessor("processor-3", new MockProcessorSupplier(), "source-3", "source-4");

        builder.setApplicationId("X");
        final ProcessorTopology topology0 = builder.build(0);
        final ProcessorTopology topology1 = builder.build(1);
        final ProcessorTopology topology2 = builder.build(2);

        assertEquals(mkSet("source-1", "source-2", "processor-1", "processor-2"), nodeNames(topology0.processors()));
        assertEquals(mkSet("source-3", "source-4", "processor-3"), nodeNames(topology1.processors()));
        assertEquals(mkSet("source-5"), nodeNames(topology2.processors()));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullNameWhenAddingSink() throws Exception {
        builder.addSink(null, "topic", null, null, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullTopicWhenAddingSink() throws Exception {
        builder.addSink("name", null, null, null, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullNameWhenAddingProcessor() throws Exception {
        builder.addProcessor(null, new ProcessorSupplier() {
            @Override
            public Processor get() {
                return null;
            }
        });
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullProcessorSupplier() throws Exception {
        builder.addProcessor("name", null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullNameWhenAddingSource() throws Exception {
        builder.addSource(null, null, null, null, null, Pattern.compile(".*"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullProcessorNameWhenConnectingProcessorAndStateStores() throws Exception {
        builder.connectProcessorAndStateStores(null, "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullStateStoreNameWhenConnectingProcessorAndStateStores() throws Exception {
        builder.connectProcessorAndStateStores("processor", new String[]{null});
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAddNullInternalTopic() throws Exception {
        builder.addInternalTopic(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotSetApplicationIdToNull() throws Exception {
        builder.setApplicationId(null);
    }

    @SuppressWarnings("deprecation")
    @Test(expected = NullPointerException.class)
    public void shouldNotAddNullStateStoreSupplier() throws Exception {
        builder.addStateStore((StateStoreSupplier) null);
    }

    private Set<String> nodeNames(final Collection<ProcessorNode> nodes) {
        final Set<String> nodeNames = new HashSet<>();
        for (final ProcessorNode node : nodes) {
            nodeNames.add(node.name());
        }
        return nodeNames;
    }

    @Test
    public void shouldAssociateStateStoreNameWhenStateStoreSupplierIsInternal() throws Exception {
        builder.addSource(null, "source", null, null, null, "topic");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source");
        builder.addStateStore(new MockStateStoreSupplier("store", false), "processor");
        final Map<String, List<String>> stateStoreNameToSourceTopic = builder.stateStoreNameToSourceTopics();
        assertEquals(1, stateStoreNameToSourceTopic.size());
        assertEquals(Collections.singletonList("topic"), stateStoreNameToSourceTopic.get("store"));
    }

    @Test
    public void shouldAssociateStateStoreNameWhenStateStoreSupplierIsExternal() throws Exception {
        builder.addSource(null, "source", null, null, null, "topic");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source");
        builder.addStateStore(new MockStateStoreSupplier("store", false), "processor");
        final Map<String, List<String>> stateStoreNameToSourceTopic = builder.stateStoreNameToSourceTopics();
        assertEquals(1, stateStoreNameToSourceTopic.size());
        assertEquals(Collections.singletonList("topic"), stateStoreNameToSourceTopic.get("store"));
    }

    @Test
    public void shouldCorrectlyMapStateStoreToInternalTopics() throws Exception {
        builder.setApplicationId("appId");
        builder.addInternalTopic("internal-topic");
        builder.addSource(null, "source", null, null, null, "internal-topic");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source");
        builder.addStateStore(new MockStateStoreSupplier("store", false), "processor");
        final Map<String, List<String>> stateStoreNameToSourceTopic = builder.stateStoreNameToSourceTopics();
        assertEquals(1, stateStoreNameToSourceTopic.size());
        assertEquals(Collections.singletonList("appId-internal-topic"), stateStoreNameToSourceTopic.get("store"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldAddInternalTopicConfigForWindowStores() throws Exception {
        builder.setApplicationId("appId");
        builder.addSource(null, "source", null, null, null, "topic");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source");
        builder.addStateStore(new RocksDBWindowStoreSupplier("store", 30000, 3, false, null, null, 10000, true, Collections.<String, String>emptyMap(), false), "processor");
        final Map<Integer, InternalTopologyBuilder.TopicsInfo> topicGroups = builder.topicGroups();
        final InternalTopologyBuilder.TopicsInfo topicsInfo = topicGroups.values().iterator().next();
        final InternalTopicConfig topicConfig = topicsInfo.stateChangelogTopics.get("appId-store-changelog");
        final Map<String, String> properties = topicConfig.getProperties(Collections.<String, String>emptyMap(), 10000);
        assertEquals(2, properties.size());
        assertEquals(TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE, properties.get(TopicConfig.CLEANUP_POLICY_CONFIG));
        assertEquals("40000", properties.get(TopicConfig.RETENTION_MS_CONFIG));
        assertEquals("appId-store-changelog", topicConfig.name());
        assertTrue(topicConfig instanceof WindowedChangelogTopicConfig);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldAddInternalTopicConfigForNonWindowStores() throws Exception {
        builder.setApplicationId("appId");
        builder.addSource(null, "source", null, null, null, "topic");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source");
        builder.addStateStore(new MockStateStoreSupplier("store", true), "processor");
        final Map<Integer, InternalTopologyBuilder.TopicsInfo> topicGroups = builder.topicGroups();
        final InternalTopologyBuilder.TopicsInfo topicsInfo = topicGroups.values().iterator().next();
        final InternalTopicConfig topicConfig = topicsInfo.stateChangelogTopics.get("appId-store-changelog");
        final Map<String, String> properties = topicConfig.getProperties(Collections.<String, String>emptyMap(), 10000);
        assertEquals(1, properties.size());
        assertEquals(TopicConfig.CLEANUP_POLICY_COMPACT, properties.get(TopicConfig.CLEANUP_POLICY_CONFIG));
        assertEquals("appId-store-changelog", topicConfig.name());
        assertTrue(topicConfig instanceof UnwindowedChangelogTopicConfig);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldAddInternalTopicConfigForRepartitionTopics() throws Exception {
        builder.setApplicationId("appId");
        builder.addInternalTopic("foo");
        builder.addSource(null, "source", null, null, null, "foo");
        final InternalTopologyBuilder.TopicsInfo topicsInfo = builder.topicGroups().values().iterator().next();
        final InternalTopicConfig topicConfig = topicsInfo.repartitionSourceTopics.get("appId-foo");
        final Map<String, String> properties = topicConfig.getProperties(Collections.<String, String>emptyMap(), 10000);
        assertEquals(5, properties.size());
        assertEquals(String.valueOf(Long.MAX_VALUE), properties.get(TopicConfig.RETENTION_MS_CONFIG));
        assertEquals(TopicConfig.CLEANUP_POLICY_DELETE, properties.get(TopicConfig.CLEANUP_POLICY_CONFIG));
        assertEquals("appId-foo", topicConfig.name());
        assertTrue(topicConfig instanceof RepartitionTopicConfig);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldThrowOnUnassignedStateStoreAccess() {
        final String sourceNodeName = "source";
        final String goodNodeName = "goodGuy";
        final String badNodeName = "badGuy";

        final Properties config = new Properties();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "host:1");
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
        config.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        final StreamsConfig streamsConfig = new StreamsConfig(config);

        builder.addSource(null, sourceNodeName, null, null, null, "topic");
        builder.addProcessor(goodNodeName, new LocalMockProcessorSupplier(), sourceNodeName);
        builder.addStateStore(
            Stores.create(LocalMockProcessorSupplier.STORE_NAME).withStringKeys().withStringValues().inMemory().build(),
            goodNodeName);
        builder.addProcessor(badNodeName, new LocalMockProcessorSupplier(), sourceNodeName);
        
        try {
            new TopologyTestDriverWrapper(builder, config);
            fail("Should have throw StreamsException");
        } catch (final StreamsException e) {
            final String error = e.toString();
            final String expectedMessage = "org.apache.kafka.streams.errors.StreamsException: failed to initialize processor " + badNodeName;
            
            assertThat(error, equalTo(expectedMessage));
        }
    }

    private static class LocalMockProcessorSupplier implements ProcessorSupplier {
        final static String STORE_NAME = "store";

        @Override
        public Processor get() {
            return new Processor() {
                @Override
                public void init(final ProcessorContext context) {
                    context.getStateStore(STORE_NAME);
                }

                @Override
                public void process(final Object key, final Object value) { }

                @Override
                public void punctuate(final long timestamp) { }

                @Override
                public void close() {
                }
            };
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldSetCorrectSourceNodesWithRegexUpdatedTopics() throws Exception {
        builder.addSource(null, "source-1", null, null, null, "topic-foo");
        builder.addSource(null, "source-2", null, null, null, Pattern.compile("topic-[A-C]"));
        builder.addSource(null, "source-3", null, null, null, Pattern.compile("topic-\\d"));

        final InternalTopologyBuilder.SubscriptionUpdates subscriptionUpdates = new InternalTopologyBuilder.SubscriptionUpdates();
        final Field updatedTopicsField  = subscriptionUpdates.getClass().getDeclaredField("updatedTopicSubscriptions");
        updatedTopicsField.setAccessible(true);

        final Set<String> updatedTopics = (Set<String>) updatedTopicsField.get(subscriptionUpdates);

        updatedTopics.add("topic-B");
        updatedTopics.add("topic-3");
        updatedTopics.add("topic-A");

        builder.updateSubscriptions(subscriptionUpdates, null);
        builder.setApplicationId("test-id");

        final Map<Integer, InternalTopologyBuilder.TopicsInfo> topicGroups = builder.topicGroups();
        assertTrue(topicGroups.get(0).sourceTopics.contains("topic-foo"));
        assertTrue(topicGroups.get(1).sourceTopics.contains("topic-A"));
        assertTrue(topicGroups.get(1).sourceTopics.contains("topic-B"));
        assertTrue(topicGroups.get(2).sourceTopics.contains("topic-3"));

    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldAddTimestampExtractorPerSource() throws Exception {
        builder.addSource(null, "source", new MockTimestampExtractor(), null, null, "topic");
        final ProcessorTopology processorTopology = builder.build(null);
        assertThat(processorTopology.source("topic").getTimestampExtractor(), instanceOf(MockTimestampExtractor.class));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldAddTimestampExtractorWithPatternPerSource() throws Exception {
        final Pattern pattern = Pattern.compile("t.*");
        builder.addSource(null, "source", new MockTimestampExtractor(), null, null, pattern);
        final ProcessorTopology processorTopology = builder.build(null);
        assertThat(processorTopology.source(pattern.pattern()).getTimestampExtractor(), instanceOf(MockTimestampExtractor.class));
    }

    @Test
    public void shouldSortProcessorNodesCorrectly() throws Exception {
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addSource(null, "source2", null, null, null, "topic2");
        builder.addProcessor("processor1", new MockProcessorSupplier(), "source1");
        builder.addProcessor("processor2", new MockProcessorSupplier(), "source1", "source2");
        builder.addProcessor("processor3", new MockProcessorSupplier(), "processor2");
        builder.addSink("sink1", "topic2", null, null, null, "processor1", "processor3");

        assertEquals(1, builder.describe().subtopologies().size());

        final Iterator<TopologyDescription.Node> iterator = ((InternalTopologyBuilder.Subtopology) builder.describe().subtopologies().iterator().next()).nodesInOrder();

        assertTrue(iterator.hasNext());
        InternalTopologyBuilder.AbstractNode node = (InternalTopologyBuilder.AbstractNode) iterator.next();
        assertTrue(node.name.equals("source1"));
        assertEquals(6, node.size);

        assertTrue(iterator.hasNext());
        node = (InternalTopologyBuilder.AbstractNode) iterator.next();
        assertTrue(node.name.equals("source2"));
        assertEquals(4, node.size);

        assertTrue(iterator.hasNext());
        node = (InternalTopologyBuilder.AbstractNode) iterator.next();
        assertTrue(node.name.equals("processor2"));
        assertEquals(3, node.size);

        assertTrue(iterator.hasNext());
        node = (InternalTopologyBuilder.AbstractNode) iterator.next();
        assertTrue(node.name.equals("processor1"));
        assertEquals(2, node.size);

        assertTrue(iterator.hasNext());
        node = (InternalTopologyBuilder.AbstractNode) iterator.next();
        assertTrue(node.name.equals("processor3"));
        assertEquals(2, node.size);

        assertTrue(iterator.hasNext());
        node = (InternalTopologyBuilder.AbstractNode) iterator.next();
        assertTrue(node.name.equals("sink1"));
        assertEquals(1, node.size);
    }

    @Test
    public void shouldConnectRegexMatchedTopicsToStateStore() throws Exception {
        builder.addSource(null, "ingest", null, null, null, Pattern.compile("topic-\\d+"));
        builder.addProcessor("my-processor", new MockProcessorSupplier(), "ingest");
        builder.addStateStore(new MockStateStoreSupplier("testStateStore", false), "my-processor");

        final InternalTopologyBuilder.SubscriptionUpdates subscriptionUpdates = new InternalTopologyBuilder.SubscriptionUpdates();
        final Field updatedTopicsField  = subscriptionUpdates.getClass().getDeclaredField("updatedTopicSubscriptions");
        updatedTopicsField.setAccessible(true);

        final Set<String> updatedTopics = (Set<String>) updatedTopicsField.get(subscriptionUpdates);

        updatedTopics.add("topic-2");
        updatedTopics.add("topic-3");
        updatedTopics.add("topic-A");

        builder.updateSubscriptions(subscriptionUpdates, "test-thread");
        builder.setApplicationId("test-app");

        final Map<String, List<String>> stateStoreAndTopics = builder.stateStoreNameToSourceTopics();
        final List<String> topics = stateStoreAndTopics.get("testStateStore");

        assertTrue("Expected to contain two topics", topics.size() == 2);

        assertTrue(topics.contains("topic-2"));
        assertTrue(topics.contains("topic-3"));
        assertFalse(topics.contains("topic-A"));
    }

    @Test(expected = TopologyException.class)
    public void shouldNotAllowToAddGlobalStoreWithSourceNameEqualsProcessorName() {
        final String sameNameForSourceAndProcessor = "sameName";
        builder.addGlobalStore(
            new MockStateStoreSupplier("anyName", false, false),
            sameNameForSourceAndProcessor,
            null,
            null,
            null,
            "anyTopicName",
            sameNameForSourceAndProcessor,
            new MockProcessorSupplier());
    }
}
