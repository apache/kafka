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
package org.apache.kafka.streams.processor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TopologyTestDriverWrapper;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TopologyBuilderException;
import org.apache.kafka.streams.processor.TopologyBuilder.TopicsInfo;
import org.apache.kafka.streams.processor.internals.InternalTopicConfig;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;
import org.apache.kafka.streams.processor.internals.StreamsPartitionAssignor;
import org.apache.kafka.streams.processor.internals.UnwindowedChangelogTopicConfig;
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

@SuppressWarnings("deprecation")
public class TopologyBuilderTest {

    @Test
    public void shouldAddSourceWithOffsetReset() {
        final TopologyBuilder builder = new TopologyBuilder();

        final String earliestTopic = "earliestTopic";
        final String latestTopic = "latestTopic";

        builder.addSource(TopologyBuilder.AutoOffsetReset.EARLIEST, "source", earliestTopic);
        builder.addSource(TopologyBuilder.AutoOffsetReset.LATEST, "source2", latestTopic);

        assertTrue(builder.earliestResetTopicsPattern().matcher(earliestTopic).matches());
        assertTrue(builder.latestResetTopicsPattern().matcher(latestTopic).matches());

    }

    @Test
    public void shouldAddSourcePatternWithOffsetReset() {
        final TopologyBuilder builder = new TopologyBuilder();

        final String earliestTopicPattern = "earliest.*Topic";
        final String latestTopicPattern = "latest.*Topic";

        builder.addSource(TopologyBuilder.AutoOffsetReset.EARLIEST, "source", Pattern.compile(earliestTopicPattern));
        builder.addSource(TopologyBuilder.AutoOffsetReset.LATEST, "source2", Pattern.compile(latestTopicPattern));

        assertTrue(builder.earliestResetTopicsPattern().matcher("earliestTestTopic").matches());
        assertTrue(builder.latestResetTopicsPattern().matcher("latestTestTopic").matches());
    }

    @Test
    public void shouldAddSourceWithoutOffsetReset() {
        final TopologyBuilder builder = new TopologyBuilder();
        final Serde<String> stringSerde = Serdes.String();
        final Pattern expectedPattern = Pattern.compile("test-topic");

        builder.addSource("source", stringSerde.deserializer(), stringSerde.deserializer(), "test-topic");

        assertEquals(expectedPattern.pattern(), builder.sourceTopicPattern().pattern());
        assertEquals(builder.earliestResetTopicsPattern().pattern(), "");
        assertEquals(builder.latestResetTopicsPattern().pattern(), "");
    }

    @Test
    public void shouldAddPatternSourceWithoutOffsetReset() {
        final TopologyBuilder builder = new TopologyBuilder();
        final Serde<String> stringSerde = Serdes.String();
        final Pattern expectedPattern = Pattern.compile("test-.*");

        builder.addSource("source", stringSerde.deserializer(), stringSerde.deserializer(), Pattern.compile("test-.*"));

        assertEquals(expectedPattern.pattern(), builder.sourceTopicPattern().pattern());
        assertEquals(builder.earliestResetTopicsPattern().pattern(), "");
        assertEquals(builder.latestResetTopicsPattern().pattern(), "");
    }

    @Test
    public void shouldNotAllowOffsetResetSourceWithoutTopics() {
        final TopologyBuilder builder = new TopologyBuilder();
        final Serde<String> stringSerde = Serdes.String();

        try {
            builder.addSource(TopologyBuilder.AutoOffsetReset.EARLIEST, "source", null, stringSerde.deserializer(), stringSerde.deserializer());
            fail("Should throw TopologyBuilderException with no topics");
        } catch (TopologyBuilderException tpe) {
            //no-op
        }
    }

    @Test
    public void shouldNotAllowOffsetResetSourceWithDuplicateSourceName() {
        final TopologyBuilder builder = new TopologyBuilder();
        final Serde<String> stringSerde = Serdes.String();

        builder.addSource(TopologyBuilder.AutoOffsetReset.EARLIEST, "source", null, stringSerde.deserializer(), stringSerde.deserializer(), "topic-1");
        try {
            builder.addSource(TopologyBuilder.AutoOffsetReset.LATEST, "source", null, stringSerde.deserializer(), stringSerde.deserializer(), "topic-2");
            fail("Should throw TopologyBuilderException for duplicate source name");
        } catch (TopologyBuilderException tpe) {
            //no-op
        }
    }



    @Test(expected = TopologyBuilderException.class)
    public void testAddSourceWithSameName() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("source", "topic-1");
        builder.addSource("source", "topic-2");
    }

    @Test(expected = TopologyBuilderException.class)
    public void testAddSourceWithSameTopic() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("source", "topic-1");
        builder.addSource("source-2", "topic-1");
    }

    @Test(expected = TopologyBuilderException.class)
    public void testAddProcessorWithSameName() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("source", "topic-1");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source");
    }

    @Test(expected = TopologyBuilderException.class)
    public void testAddProcessorWithWrongParent() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addProcessor("processor", new MockProcessorSupplier(), "source");
    }

    @Test(expected = TopologyBuilderException.class)
    public void testAddProcessorWithSelfParent() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addProcessor("processor", new MockProcessorSupplier(), "processor");
    }

    @Test(expected = TopologyBuilderException.class)
    public void testAddSinkWithSameName() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("source", "topic-1");
        builder.addSink("sink", "topic-2", "source");
        builder.addSink("sink", "topic-3", "source");
    }

    @Test(expected = TopologyBuilderException.class)
    public void testAddSinkWithWrongParent() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addSink("sink", "topic-2", "source");
    }

    @Test(expected = TopologyBuilderException.class)
    public void testAddSinkWithSelfParent() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addSink("sink", "topic-2", "sink");
    }

    @Test
    public void testAddSinkConnectedWithParent() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("source", "source-topic");
        builder.addSink("sink", "dest-topic", "source");

        Map<Integer, Set<String>> nodeGroups = builder.nodeGroups();
        Set<String> nodeGroup = nodeGroups.get(0);

        assertTrue(nodeGroup.contains("sink"));
        assertTrue(nodeGroup.contains("source"));

    }

    @Test
    public void testAddSinkConnectedWithMultipleParent() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("source", "source-topic");
        builder.addSource("sourceII", "source-topicII");
        builder.addSink("sink", "dest-topic", "source", "sourceII");

        Map<Integer, Set<String>> nodeGroups = builder.nodeGroups();
        Set<String> nodeGroup = nodeGroups.get(0);

        assertTrue(nodeGroup.contains("sink"));
        assertTrue(nodeGroup.contains("source"));
        assertTrue(nodeGroup.contains("sourceII"));

    }

    @Test
    public void testSourceTopics() {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.setApplicationId("X");
        builder.addSource("source-1", "topic-1");
        builder.addSource("source-2", "topic-2");
        builder.addSource("source-3", "topic-3");
        builder.addInternalTopic("topic-3");

        Pattern expectedPattern = Pattern.compile("X-topic-3|topic-1|topic-2");

        assertEquals(expectedPattern.pattern(), builder.sourceTopicPattern().pattern());
    }

    @Test
    public void testPatternSourceTopic() {
        final TopologyBuilder builder = new TopologyBuilder();
        Pattern expectedPattern = Pattern.compile("topic-\\d");
        builder.addSource("source-1", expectedPattern);
        assertEquals(expectedPattern.pattern(), builder.sourceTopicPattern().pattern());
    }

    @Test
    public void testAddMoreThanOnePatternSourceNode() {
        final TopologyBuilder builder = new TopologyBuilder();
        Pattern expectedPattern = Pattern.compile("topics[A-Z]|.*-\\d");
        builder.addSource("source-1", Pattern.compile("topics[A-Z]"));
        builder.addSource("source-2", Pattern.compile(".*-\\d"));
        assertEquals(expectedPattern.pattern(), builder.sourceTopicPattern().pattern());
    }

    @Test
    public void testSubscribeTopicNameAndPattern() {
        final TopologyBuilder builder = new TopologyBuilder();
        Pattern expectedPattern = Pattern.compile("topic-bar|topic-foo|.*-\\d");
        builder.addSource("source-1", "topic-foo", "topic-bar");
        builder.addSource("source-2", Pattern.compile(".*-\\d"));
        assertEquals(expectedPattern.pattern(), builder.sourceTopicPattern().pattern());
    }

    @Test(expected = TopologyBuilderException.class)
    public void testPatternMatchesAlreadyProvidedTopicSource() {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("source-1", "foo");
        builder.addSource("source-2", Pattern.compile("f.*"));
    }

    @Test(expected = TopologyBuilderException.class)
    public void testNamedTopicMatchesAlreadyProvidedPattern() {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("source-1", Pattern.compile("f.*"));
        builder.addSource("source-2", "foo");
    }

    @Test(expected = TopologyBuilderException.class)
    public void testAddStateStoreWithNonExistingProcessor() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addStateStore(new MockStateStoreSupplier("store", false), "no-such-processsor");
    }

    @Test(expected = TopologyBuilderException.class)
    public void testAddStateStoreWithSource() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("source-1", "topic-1");
        builder.addStateStore(new MockStateStoreSupplier("store", false), "source-1");
    }

    @Test(expected = TopologyBuilderException.class)
    public void testAddStateStoreWithSink() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addSink("sink-1", "topic-1");
        builder.addStateStore(new MockStateStoreSupplier("store", false), "sink-1");
    }

    @Test(expected = TopologyBuilderException.class)
    public void testAddStateStoreWithDuplicates() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addStateStore(new MockStateStoreSupplier("store", false));
        builder.addStateStore(new MockStateStoreSupplier("store", false));
    }

    @Test
    public void testAddStateStore() {
        final TopologyBuilder builder = new TopologyBuilder();

        StateStoreSupplier supplier = new MockStateStoreSupplier("store-1", false);
        builder.addStateStore(supplier);
        builder.setApplicationId("X");
        builder.addSource("source-1", "topic-1");
        builder.addProcessor("processor-1", new MockProcessorSupplier(), "source-1");

        assertEquals(0, builder.build(null).stateStores().size());

        builder.connectProcessorAndStateStores("processor-1", "store-1");

        List<StateStore> suppliers = builder.build(null).stateStores();
        assertEquals(1, suppliers.size());
        assertEquals(supplier.name(), suppliers.get(0).name());
    }

    @Test
    public void testTopicGroups() {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.setApplicationId("X");
        builder.addInternalTopic("topic-1x");
        builder.addSource("source-1", "topic-1", "topic-1x");
        builder.addSource("source-2", "topic-2");
        builder.addSource("source-3", "topic-3");
        builder.addSource("source-4", "topic-4");
        builder.addSource("source-5", "topic-5");

        builder.addProcessor("processor-1", new MockProcessorSupplier(), "source-1");

        builder.addProcessor("processor-2", new MockProcessorSupplier(), "source-2", "processor-1");
        builder.copartitionSources(mkList("source-1", "source-2"));

        builder.addProcessor("processor-3", new MockProcessorSupplier(), "source-3", "source-4");

        Map<Integer, TopicsInfo> topicGroups = builder.topicGroups();

        Map<Integer, TopicsInfo> expectedTopicGroups = new HashMap<>();
        expectedTopicGroups.put(0, new TopicsInfo(Collections.<String>emptySet(), mkSet("topic-1", "X-topic-1x", "topic-2"), Collections.<String, InternalTopicConfig>emptyMap(), Collections.<String, InternalTopicConfig>emptyMap()));
        expectedTopicGroups.put(1, new TopicsInfo(Collections.<String>emptySet(), mkSet("topic-3", "topic-4"), Collections.<String, InternalTopicConfig>emptyMap(), Collections.<String, InternalTopicConfig>emptyMap()));
        expectedTopicGroups.put(2, new TopicsInfo(Collections.<String>emptySet(), mkSet("topic-5"), Collections.<String, InternalTopicConfig>emptyMap(), Collections.<String, InternalTopicConfig>emptyMap()));

        assertEquals(3, topicGroups.size());
        assertEquals(expectedTopicGroups, topicGroups);

        Collection<Set<String>> copartitionGroups = builder.copartitionGroups();

        assertEquals(mkSet(mkSet("topic-1", "X-topic-1x", "topic-2")), new HashSet<>(copartitionGroups));
    }

    @Test
    public void testTopicGroupsByStateStore() {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.setApplicationId("X");
        builder.addSource("source-1", "topic-1", "topic-1x");
        builder.addSource("source-2", "topic-2");
        builder.addSource("source-3", "topic-3");
        builder.addSource("source-4", "topic-4");
        builder.addSource("source-5", "topic-5");

        builder.addProcessor("processor-1", new MockProcessorSupplier(), "source-1");
        builder.addProcessor("processor-2", new MockProcessorSupplier(), "source-2");
        builder.addStateStore(new MockStateStoreSupplier("store-1", false), "processor-1", "processor-2");

        builder.addProcessor("processor-3", new MockProcessorSupplier(), "source-3");
        builder.addProcessor("processor-4", new MockProcessorSupplier(), "source-4");
        builder.addStateStore(new MockStateStoreSupplier("store-2", false), "processor-3", "processor-4");

        builder.addProcessor("processor-5", new MockProcessorSupplier(), "source-5");
        StateStoreSupplier supplier = new MockStateStoreSupplier("store-3", false);
        builder.addStateStore(supplier);
        builder.connectProcessorAndStateStores("processor-5", "store-3");

        Map<Integer, TopicsInfo> topicGroups = builder.topicGroups();

        Map<Integer, TopicsInfo> expectedTopicGroups = new HashMap<>();
        final String store1 = ProcessorStateManager.storeChangelogTopic("X", "store-1");
        final String store2 = ProcessorStateManager.storeChangelogTopic("X", "store-2");
        final String store3 = ProcessorStateManager.storeChangelogTopic("X", "store-3");
        expectedTopicGroups.put(0, new TopicsInfo(Collections.<String>emptySet(), mkSet("topic-1", "topic-1x", "topic-2"),
                                                  Collections.<String, InternalTopicConfig>emptyMap(),
                                                  Collections.singletonMap(store1, (InternalTopicConfig)  new UnwindowedChangelogTopicConfig(store1, Collections.<String, String>emptyMap()))));
        expectedTopicGroups.put(1, new TopicsInfo(Collections.<String>emptySet(), mkSet("topic-3", "topic-4"),
                                                  Collections.<String, InternalTopicConfig>emptyMap(),
                                                  Collections.singletonMap(store2, (InternalTopicConfig)  new UnwindowedChangelogTopicConfig(store2, Collections.<String, String>emptyMap()))));
        expectedTopicGroups.put(2, new TopicsInfo(Collections.<String>emptySet(), mkSet("topic-5"),
                                                  Collections.<String, InternalTopicConfig>emptyMap(),
                                                  Collections.singletonMap(store3, (InternalTopicConfig)  new UnwindowedChangelogTopicConfig(store3, Collections.<String, String>emptyMap()))));

        assertEquals(3, topicGroups.size());
        assertEquals(expectedTopicGroups, topicGroups);
    }

    @Test
    public void testBuild() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("source-1", "topic-1", "topic-1x");
        builder.addSource("source-2", "topic-2");
        builder.addSource("source-3", "topic-3");
        builder.addSource("source-4", "topic-4");
        builder.addSource("source-5", "topic-5");

        builder.addProcessor("processor-1", new MockProcessorSupplier(), "source-1");
        builder.addProcessor("processor-2", new MockProcessorSupplier(), "source-2", "processor-1");
        builder.addProcessor("processor-3", new MockProcessorSupplier(), "source-3", "source-4");

        builder.setApplicationId("X");
        ProcessorTopology topology0 = builder.build(0);
        ProcessorTopology topology1 = builder.build(1);
        ProcessorTopology topology2 = builder.build(2);

        assertEquals(mkSet("source-1", "source-2", "processor-1", "processor-2"), nodeNames(topology0.processors()));
        assertEquals(mkSet("source-3", "source-4", "processor-3"), nodeNames(topology1.processors()));
        assertEquals(mkSet("source-5"), nodeNames(topology2.processors()));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullNameWhenAddingSink() {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.addSink(null, "topic");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullTopicWhenAddingSink() {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.addSink("name", null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullNameWhenAddingProcessor() {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.addProcessor(null, new ProcessorSupplier() {
            @Override
            public Processor get() {
                return null;
            }
        });
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullProcessorSupplier() {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.addProcessor("name", null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullNameWhenAddingSource() {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.addSource(null, Pattern.compile(".*"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullProcessorNameWhenConnectingProcessorAndStateStores() {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.connectProcessorAndStateStores(null, "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAddNullInternalTopic() {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.addInternalTopic(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotSetApplicationIdToNull() {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.setApplicationId(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAddNullStateStoreSupplier() {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.addStateStore(null);
    }

    private Set<String> nodeNames(Collection<ProcessorNode> nodes) {
        Set<String> nodeNames = new HashSet<>();
        for (ProcessorNode node : nodes) {
            nodeNames.add(node.name());
        }
        return nodeNames;
    }

    @Test
    public void shouldAssociateStateStoreNameWhenStateStoreSupplierIsInternal() {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("source", "topic");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source");
        builder.addStateStore(new MockStateStoreSupplier("store", false), "processor");
        final Map<String, List<String>> stateStoreNameToSourceTopic = builder.stateStoreNameToSourceTopics();
        assertEquals(1, stateStoreNameToSourceTopic.size());
        assertEquals(Collections.singletonList("topic"), stateStoreNameToSourceTopic.get("store"));
    }

    @Test
    public void shouldAssociateStateStoreNameWhenStateStoreSupplierIsExternal() {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("source", "topic");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source");
        builder.addStateStore(new MockStateStoreSupplier("store", false), "processor");
        final Map<String, List<String>> stateStoreNameToSourceTopic = builder.stateStoreNameToSourceTopics();
        assertEquals(1, stateStoreNameToSourceTopic.size());
        assertEquals(Collections.singletonList("topic"), stateStoreNameToSourceTopic.get("store"));
    }

    @Test
    public void shouldCorrectlyMapStateStoreToInternalTopics() {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.setApplicationId("appId");
        builder.addInternalTopic("internal-topic");
        builder.addSource("source", "internal-topic");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source");
        builder.addStateStore(new MockStateStoreSupplier("store", false), "processor");
        final Map<String, List<String>> stateStoreNameToSourceTopic = builder.stateStoreNameToSourceTopics();
        assertEquals(1, stateStoreNameToSourceTopic.size());
        assertEquals(Collections.singletonList("appId-internal-topic"), stateStoreNameToSourceTopic.get("store"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldAddInternalTopicConfigForWindowStores() {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.setApplicationId("appId");
        builder.addSource("source", "topic");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source");
        builder.addStateStore(new RocksDBWindowStoreSupplier("store", 30000, 3, false, null, null, 10000, true, Collections.<String, String>emptyMap(), false), "processor");
        final Map<Integer, TopicsInfo> topicGroups = builder.topicGroups();
        final TopicsInfo topicsInfo = topicGroups.values().iterator().next();
        final InternalTopicConfig topicConfig = topicsInfo.stateChangelogTopics.get("appId-store-changelog");
        final Map<String, String> properties = topicConfig.getProperties(Collections.<String, String>emptyMap(), 10000);
        assertEquals(2, properties.size());
        assertEquals("40000", properties.get(TopicConfig.RETENTION_MS_CONFIG));
        assertEquals("appId-store-changelog", topicConfig.name());
    }

    @Test
    public void shouldAddInternalTopicConfigForNonWindowStores() {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.setApplicationId("appId");
        builder.addSource("source", "topic");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source");
        builder.addStateStore(new MockStateStoreSupplier("store", true), "processor");
        final Map<Integer, TopicsInfo> topicGroups = builder.topicGroups();
        final TopicsInfo topicsInfo = topicGroups.values().iterator().next();
        final InternalTopicConfig topicConfig = topicsInfo.stateChangelogTopics.get("appId-store-changelog");
        final Map<String, String> properties = topicConfig.getProperties(Collections.<String, String>emptyMap(), 10000);
        assertEquals(1, properties.size());
        assertEquals("appId-store-changelog", topicConfig.name());
    }

    @Test
    public void shouldAddInternalTopicConfigForRepartitionTopics() {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.setApplicationId("appId");
        builder.addInternalTopic("foo");
        builder.addSource("source", "foo");
        final TopicsInfo topicsInfo = builder.topicGroups().values().iterator().next();
        final InternalTopicConfig topicConfig = topicsInfo.repartitionSourceTopics.get("appId-foo");
        final Map<String, String> properties = topicConfig.getProperties(Collections.<String, String>emptyMap(), 10000);
        assertEquals(5, properties.size());
        assertEquals(String.valueOf(Long.MAX_VALUE), properties.get(TopicConfig.RETENTION_MS_CONFIG));
        assertEquals("appId-foo", topicConfig.name());
    }

    @Test
    public void shouldThroughOnUnassignedStateStoreAccess() throws Exception {
        final String sourceNodeName = "source";
        final String goodNodeName = "goodGuy";
        final String badNodeName = "badGuy";

        final Properties config = new Properties();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "host:1");
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
        config.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        final StreamsConfig streamsConfig = new StreamsConfig(config);

        final TopologyBuilder builder = new TopologyBuilder();
        builder.addSource(sourceNodeName, "topic")
                .addProcessor(goodNodeName, new LocalMockProcessorSupplier(), sourceNodeName)
                .addStateStore(Stores.create(LocalMockProcessorSupplier.STORE_NAME).withStringKeys().withStringValues().inMemory().build(), goodNodeName)
                .addProcessor(badNodeName, new LocalMockProcessorSupplier(), sourceNodeName);
        try {
            final TopologyTestDriverWrapper driver = new TopologyTestDriverWrapper(builder.internalTopologyBuilder, config);
            driver.pipeInput(new ConsumerRecord<>("topic", 0, 0L, new byte[] {}, new byte[] {}));
            fail("Should have thrown StreamsException");
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
                public void init(ProcessorContext context) {
                    context.getStateStore(STORE_NAME);
                }

                @Override
                public void process(Object key, Object value) {
                }

                @Override
                public void punctuate(long timestamp) {
                }

                @Override
                public void close() {
                }
            };
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldSetCorrectSourceNodesWithRegexUpdatedTopics() throws Exception {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("source-1", "topic-foo");
        builder.addSource("source-2", Pattern.compile("topic-[A-C]"));
        builder.addSource("source-3", Pattern.compile("topic-\\d"));

        StreamsPartitionAssignor.SubscriptionUpdates subscriptionUpdates = new StreamsPartitionAssignor.SubscriptionUpdates();
        Field updatedTopicsField  = subscriptionUpdates.getClass().getDeclaredField("updatedTopicSubscriptions");
        updatedTopicsField.setAccessible(true);

        Set<String> updatedTopics = (Set<String>) updatedTopicsField.get(subscriptionUpdates);

        updatedTopics.add("topic-B");
        updatedTopics.add("topic-3");
        updatedTopics.add("topic-A");

        builder.updateSubscriptions(subscriptionUpdates, null);
        builder.setApplicationId("test-id");

        Map<Integer, TopicsInfo> topicGroups = builder.topicGroups();
        assertTrue(topicGroups.get(0).sourceTopics.contains("topic-foo"));
        assertTrue(topicGroups.get(1).sourceTopics.contains("topic-A"));
        assertTrue(topicGroups.get(1).sourceTopics.contains("topic-B"));
        assertTrue(topicGroups.get(2).sourceTopics.contains("topic-3"));

    }

    @Test
    public void shouldAddTimestampExtractorPerSource() {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.addSource(new MockTimestampExtractor(), "source", "topic");
        final ProcessorTopology processorTopology = builder.build(null);
        assertThat(processorTopology.source("topic").getTimestampExtractor(), instanceOf(MockTimestampExtractor.class));
    }

    @Test
    public void shouldAddTimestampExtractorWithOffsetResetPerSource() {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.addSource(null, new MockTimestampExtractor(), "source", "topic");
        final ProcessorTopology processorTopology = builder.build(null);
        assertThat(processorTopology.source("topic").getTimestampExtractor(), instanceOf(MockTimestampExtractor.class));
    }

    @Test
    public void shouldAddTimestampExtractorWithPatternPerSource() {
        final TopologyBuilder builder = new TopologyBuilder();
        final Pattern pattern = Pattern.compile("t.*");
        builder.addSource(new MockTimestampExtractor(), "source", pattern);
        final ProcessorTopology processorTopology = builder.build(null);
        assertThat(processorTopology.source(pattern.pattern()).getTimestampExtractor(), instanceOf(MockTimestampExtractor.class));
    }

    @Test
    public void shouldAddTimestampExtractorWithOffsetResetAndPatternPerSource() {
        final TopologyBuilder builder = new TopologyBuilder();
        final Pattern pattern = Pattern.compile("t.*");
        builder.addSource(null, new MockTimestampExtractor(), "source", pattern);
        final ProcessorTopology processorTopology = builder.build(null);
        assertThat(processorTopology.source(pattern.pattern()).getTimestampExtractor(), instanceOf(MockTimestampExtractor.class));
    }

    @Test
    public void shouldAddTimestampExtractorWithOffsetResetAndKeyValSerdesPerSource() {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.addSource(null, "source", new MockTimestampExtractor(), null, null, "topic");
        final ProcessorTopology processorTopology = builder.build(null);
        assertThat(processorTopology.source("topic").getTimestampExtractor(), instanceOf(MockTimestampExtractor.class));
    }

    @Test
    public void shouldAddTimestampExtractorWithOffsetResetAndKeyValSerdesAndPatternPerSource() {
        final TopologyBuilder builder = new TopologyBuilder();
        final Pattern pattern = Pattern.compile("t.*");
        builder.addSource(null, "source", new MockTimestampExtractor(), null, null, pattern);
        final ProcessorTopology processorTopology = builder.build(null);
        assertThat(processorTopology.source(pattern.pattern()).getTimestampExtractor(), instanceOf(MockTimestampExtractor.class));
    }

    @Test
    public void shouldConnectRegexMatchedTopicsToStateStore() throws Exception {

        final TopologyBuilder topologyBuilder = new TopologyBuilder()
                .addSource("ingest", Pattern.compile("topic-\\d+"))
                .addProcessor("my-processor", new MockProcessorSupplier(), "ingest")
                .addStateStore(new MockStateStoreSupplier("testStateStore", false), "my-processor");

        final StreamsPartitionAssignor.SubscriptionUpdates subscriptionUpdates = new StreamsPartitionAssignor.SubscriptionUpdates();
        final Field updatedTopicsField  = subscriptionUpdates.getClass().getDeclaredField("updatedTopicSubscriptions");
        updatedTopicsField.setAccessible(true);

        final Set<String> updatedTopics = (Set<String>) updatedTopicsField.get(subscriptionUpdates);

        updatedTopics.add("topic-2");
        updatedTopics.add("topic-3");
        updatedTopics.add("topic-A");

        topologyBuilder.updateSubscriptions(subscriptionUpdates, "test-thread");
        topologyBuilder.setApplicationId("test-app");

        Map<String, List<String>> stateStoreAndTopics = topologyBuilder.stateStoreNameToSourceTopics();
        List<String> topics = stateStoreAndTopics.get("testStateStore");

        assertTrue("Expected to contain two topics", topics.size() == 2);

        assertTrue(topics.contains("topic-2"));
        assertTrue(topics.contains("topic-3"));
        assertFalse(topics.contains("topic-A"));
    }

    @Test(expected = TopologyBuilderException.class)
    public void shouldNotAllowToAddGlobalStoreWithSourceNameEqualsProcessorName() {
        final String sameNameForSourceAndProcessor = "sameName";
        final TopologyBuilder topologyBuilder = new TopologyBuilder()
            .addGlobalStore(new MockStateStoreSupplier("anyName", false, false),
                sameNameForSourceAndProcessor,
                null,
                null,
                "anyTopicName",
                sameNameForSourceAndProcessor,
                new MockProcessorSupplier());
    }
}
