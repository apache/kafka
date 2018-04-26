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

import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockStateStore;
import org.apache.kafka.test.TestUtils;
import org.easymock.EasyMock;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

public class TopologyTest {

    private final StoreBuilder storeBuilder = EasyMock.createNiceMock(StoreBuilder.class);
    private final KeyValueStoreBuilder globalStoreBuilder = EasyMock.createNiceMock(KeyValueStoreBuilder.class);
    private final Topology topology = new Topology();
    private final InternalTopologyBuilder.TopologyDescription expectedDescription = new InternalTopologyBuilder.TopologyDescription();

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullNameWhenAddingSourceWithTopic() {
        topology.addSource((String) null, "topic");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullNameWhenAddingSourceWithPattern() {
        topology.addSource(null, Pattern.compile(".*"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullTopicsWhenAddingSoureWithTopic() {
        topology.addSource("source", (String[]) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullTopicsWhenAddingSourceWithPattern() {
        topology.addSource("source", (Pattern) null);
    }

    @Test(expected = TopologyException.class)
    public void shouldNotAllowZeroTopicsWhenAddingSource() {
        topology.addSource("source");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullNameWhenAddingProcessor() {
        topology.addProcessor(null, new ProcessorSupplier() {
            @Override
            public Processor get() {
                return new MockProcessorSupplier().get();
            }
        });
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullProcessorSupplierWhenAddingProcessor() {
        topology.addProcessor("name", null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullNameWhenAddingSink() {
        topology.addSink(null, "topic");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullTopicWhenAddingSink() {
        topology.addSink("name", null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullProcessorNameWhenConnectingProcessorAndStateStores() {
        topology.connectProcessorAndStateStores(null, "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullStoreNameWhenConnectingProcessorAndStateStores() {
        topology.connectProcessorAndStateStores("processor", (String[]) null);
    }

    @Test(expected = TopologyException.class)
    public void shouldNotAllowZeroStoreNameWhenConnectingProcessorAndStateStores() {
        topology.connectProcessorAndStateStores("processor");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAddNullStateStoreSupplier() {
        topology.addStateStore(null);
    }

    @Test
    public void shouldNotAllowToAddSourcesWithSameName() {
        topology.addSource("source", "topic-1");
        try {
            topology.addSource("source", "topic-2");
            fail("Should throw TopologyException for duplicate source name");
        } catch (TopologyException expected) { }
    }

    @Test
    public void shouldNotAllowToAddTopicTwice() {
        topology.addSource("source", "topic-1");
        try {
            topology.addSource("source-2", "topic-1");
            fail("Should throw TopologyException for already used topic");
        } catch (TopologyException expected) { }
    }

    @Test
    public void testPatternMatchesAlreadyProvidedTopicSource() {
        topology.addSource("source-1", "foo");
        try {
            topology.addSource("source-2", Pattern.compile("f.*"));
            fail("Should have thrown TopologyException for overlapping pattern with already registered topic");
        } catch (final TopologyException expected) { }
    }

    @Test
    public void testNamedTopicMatchesAlreadyProvidedPattern() {
        topology.addSource("source-1", Pattern.compile("f.*"));
        try {
            topology.addSource("source-2", "foo");
            fail("Should have thrown TopologyException for overlapping topic with already registered pattern");
        } catch (final TopologyException expected) { }
    }

    @Test
    public void shouldNotAllowToAddProcessorWithSameName() {
        topology.addSource("source", "topic-1");
        topology.addProcessor("processor", new MockProcessorSupplier(), "source");
        try {
            topology.addProcessor("processor", new MockProcessorSupplier(), "source");
            fail("Should throw TopologyException for duplicate processor name");
        } catch (TopologyException expected) { }
    }

    @Test(expected = TopologyException.class)
    public void shouldFailOnUnknownSource() {
        topology.addProcessor("processor", new MockProcessorSupplier(), "source");
    }

    @Test(expected = TopologyException.class)
    public void shouldFailIfNodeIsItsOwnParent() {
        topology.addProcessor("processor", new MockProcessorSupplier(), "processor");
    }

    @Test
    public void shouldNotAllowToAddSinkWithSameName() {
        topology.addSource("source", "topic-1");
        topology.addSink("sink", "topic-2", "source");
        try {
            topology.addSink("sink", "topic-3", "source");
            fail("Should throw TopologyException for duplicate sink name");
        } catch (TopologyException expected) { }
    }

    @Test(expected = TopologyException.class)
    public void shouldFailWithUnknownParent() {
        topology.addSink("sink", "topic-2", "source");
    }

    @Test(expected = TopologyException.class)
    public void shouldFailIfSinkIsItsOwnParent() {
        topology.addSink("sink", "topic-2", "sink");
    }

    @Test
    public void shouldFailIfSinkIsParent() {
        topology.addSource("source", "topic-1");
        topology.addSink("sink-1", "topic-2", "source");
        try {
            topology.addSink("sink-2", "topic-3", "sink-1");
            fail("Should throw TopologyException for using sink as parent");
        } catch (final TopologyException expected) { }
    }

    @Test(expected = TopologyException.class)
    public void shouldNotAllowToAddStateStoreToNonExistingProcessor() {
        mockStoreBuilder();
        EasyMock.replay(storeBuilder);
        topology.addStateStore(storeBuilder, "no-such-processsor");
    }

    @Test
    public void shouldNotAllowToAddStateStoreToSource() {
        mockStoreBuilder();
        EasyMock.replay(storeBuilder);
        topology.addSource("source-1", "topic-1");
        try {
            topology.addStateStore(storeBuilder, "source-1");
            fail("Should have thrown TopologyException for adding store to source node");
        } catch (final TopologyException expected) { }
    }

    @Test
    public void shouldNotAllowToAddStateStoreToSink() {
        mockStoreBuilder();
        EasyMock.replay(storeBuilder);
        topology.addSink("sink-1", "topic-1");
        try {
            topology.addStateStore(storeBuilder, "sink-1");
            fail("Should have thrown TopologyException for adding store to sink node");
        } catch (final TopologyException expected) { }
    }

    private void mockStoreBuilder() {
        EasyMock.expect(storeBuilder.name()).andReturn("store").anyTimes();
        EasyMock.expect(storeBuilder.logConfig()).andReturn(Collections.emptyMap());
        EasyMock.expect(storeBuilder.loggingEnabled()).andReturn(false);
    }

    @Test
    public void shouldNotAllowToAddStoreWithSameName() {
        mockStoreBuilder();
        EasyMock.replay(storeBuilder);
        topology.addStateStore(storeBuilder);
        try {
            topology.addStateStore(storeBuilder);
            fail("Should have thrown TopologyException for duplicate store name");
        } catch (final TopologyException expected) { }
    }

    @Test
    public void shouldThrowOnUnassignedStateStoreAccess() throws Exception {
        final String sourceNodeName = "source";
        final String goodNodeName = "goodGuy";
        final String badNodeName = "badGuy";

        final Properties config = new Properties();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "host:1");
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
        config.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        mockStoreBuilder();
        EasyMock.expect(storeBuilder.build()).andReturn(new MockStateStore("store", false));
        EasyMock.replay(storeBuilder);
        topology
            .addSource(sourceNodeName, "topic")
            .addProcessor(goodNodeName, new LocalMockProcessorSupplier(), sourceNodeName)
            .addStateStore(
                storeBuilder,
                goodNodeName)
            .addProcessor(badNodeName, new LocalMockProcessorSupplier(), sourceNodeName);

        try {
            new TopologyTestDriver(topology, config);
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
                public void process(Object key, Object value) { }

                @SuppressWarnings("deprecation")
                @Override
                public void punctuate(long timestamp) { }

                @Override
                public void close() { }
            };
        }
    }

    @Test(expected = TopologyException.class)
    public void shouldNotAllowToAddGlobalStoreWithSourceNameEqualsProcessorName() {
        EasyMock.expect(globalStoreBuilder.name()).andReturn("anyName").anyTimes();
        EasyMock.replay(globalStoreBuilder);
        topology.addGlobalStore(
            globalStoreBuilder,
            "sameName",
            null,
            null,
            "anyTopicName",
            "sameName",
            new MockProcessorSupplier());
    }

    @Test
    public void shouldDescribeEmptyTopology() {
        assertThat(topology.describe(), equalTo((TopologyDescription) expectedDescription));
    }

    @Test
    public void singleSourceShouldHaveSingleSubtopology() {
        final TopologyDescription.Source expectedSourceNode = addSource("source", "topic");

        expectedDescription.addSubtopology(
            new InternalTopologyBuilder.Subtopology(0,
                Collections.<TopologyDescription.Node>singleton(expectedSourceNode)));

        assertThat(topology.describe(), equalTo((TopologyDescription) expectedDescription));
    }

    @Test
    public void singleSourceWithListOfTopicsShouldHaveSingleSubtopology() {
        final TopologyDescription.Source expectedSourceNode = addSource("source", "topic1", "topic2", "topic3");

        expectedDescription.addSubtopology(
            new InternalTopologyBuilder.Subtopology(0,
                Collections.<TopologyDescription.Node>singleton(expectedSourceNode)));

        assertThat(topology.describe(), equalTo((TopologyDescription) expectedDescription));
    }

    @Test
    public void singleSourcePatternShouldHaveSingleSubtopology() {
        final TopologyDescription.Source expectedSourceNode = addSource("source", Pattern.compile("topic[0-9]"));

        expectedDescription.addSubtopology(
            new InternalTopologyBuilder.Subtopology(0,
                Collections.<TopologyDescription.Node>singleton(expectedSourceNode)));

        assertThat(topology.describe(), equalTo((TopologyDescription) expectedDescription));
    }

    @Test
    public void multipleSourcesShouldHaveDistinctSubtopologies() {
        final TopologyDescription.Source expectedSourceNode1 = addSource("source1", "topic1");
        expectedDescription.addSubtopology(
            new InternalTopologyBuilder.Subtopology(0,
                Collections.<TopologyDescription.Node>singleton(expectedSourceNode1)));

        final TopologyDescription.Source expectedSourceNode2 = addSource("source2", "topic2");
        expectedDescription.addSubtopology(
            new InternalTopologyBuilder.Subtopology(1,
                Collections.<TopologyDescription.Node>singleton(expectedSourceNode2)));

        final TopologyDescription.Source expectedSourceNode3 = addSource("source3", "topic3");
        expectedDescription.addSubtopology(
            new InternalTopologyBuilder.Subtopology(2,
                Collections.<TopologyDescription.Node>singleton(expectedSourceNode3)));

        assertThat(topology.describe(), equalTo((TopologyDescription) expectedDescription));
    }

    @Test
    public void sourceAndProcessorShouldHaveSingleSubtopology() {
        final TopologyDescription.Source expectedSourceNode = addSource("source", "topic");
        final TopologyDescription.Processor expectedProcessorNode = addProcessor("processor", expectedSourceNode);

        final Set<TopologyDescription.Node> allNodes = new HashSet<>();
        allNodes.add(expectedSourceNode);
        allNodes.add(expectedProcessorNode);
        expectedDescription.addSubtopology(new InternalTopologyBuilder.Subtopology(0, allNodes));

        assertThat(topology.describe(), equalTo((TopologyDescription) expectedDescription));
    }

    @Test
    public void sourceAndProcessorWithStateShouldHaveSingleSubtopology() {
        final TopologyDescription.Source expectedSourceNode = addSource("source", "topic");
        final String[] store = new String[] {"store"};
        final TopologyDescription.Processor expectedProcessorNode
            = addProcessorWithNewStore("processor", store, expectedSourceNode);

        final Set<TopologyDescription.Node> allNodes = new HashSet<>();
        allNodes.add(expectedSourceNode);
        allNodes.add(expectedProcessorNode);
        expectedDescription.addSubtopology(new InternalTopologyBuilder.Subtopology(0, allNodes));

        assertThat(topology.describe(), equalTo((TopologyDescription) expectedDescription));
    }


    @Test
    public void sourceAndProcessorWithMultipleStatesShouldHaveSingleSubtopology() {
        final TopologyDescription.Source expectedSourceNode = addSource("source", "topic");
        final String[] stores = new String[] {"store1", "store2"};
        final TopologyDescription.Processor expectedProcessorNode
            = addProcessorWithNewStore("processor", stores, expectedSourceNode);

        final Set<TopologyDescription.Node> allNodes = new HashSet<>();
        allNodes.add(expectedSourceNode);
        allNodes.add(expectedProcessorNode);
        expectedDescription.addSubtopology(new InternalTopologyBuilder.Subtopology(0, allNodes));

        assertThat(topology.describe(), equalTo((TopologyDescription) expectedDescription));
    }

    @Test
    public void sourceWithMultipleProcessorsShouldHaveSingleSubtopology() {
        final TopologyDescription.Source expectedSourceNode = addSource("source", "topic");
        final TopologyDescription.Processor expectedProcessorNode1 = addProcessor("processor1", expectedSourceNode);
        final TopologyDescription.Processor expectedProcessorNode2 = addProcessor("processor2", expectedSourceNode);

        final Set<TopologyDescription.Node> allNodes = new HashSet<>();
        allNodes.add(expectedSourceNode);
        allNodes.add(expectedProcessorNode1);
        allNodes.add(expectedProcessorNode2);
        expectedDescription.addSubtopology(new InternalTopologyBuilder.Subtopology(0, allNodes));

        assertThat(topology.describe(), equalTo((TopologyDescription) expectedDescription));
    }

    @Test
    public void processorWithMultipleSourcesShouldHaveSingleSubtopology() {
        final TopologyDescription.Source expectedSourceNode1 = addSource("source1", "topic0");
        final TopologyDescription.Source expectedSourceNode2 = addSource("source2", Pattern.compile("topic[1-9]"));
        final TopologyDescription.Processor expectedProcessorNode = addProcessor("processor", expectedSourceNode1, expectedSourceNode2);

        final Set<TopologyDescription.Node> allNodes = new HashSet<>();
        allNodes.add(expectedSourceNode1);
        allNodes.add(expectedSourceNode2);
        allNodes.add(expectedProcessorNode);
        expectedDescription.addSubtopology(new InternalTopologyBuilder.Subtopology(0, allNodes));

        assertThat(topology.describe(), equalTo((TopologyDescription) expectedDescription));
    }

    @Test
    public void multipleSourcesWithProcessorsShouldHaveDistinctSubtopologies() {
        final TopologyDescription.Source expectedSourceNode1 = addSource("source1", "topic1");
        final TopologyDescription.Processor expectedProcessorNode1 = addProcessor("processor1", expectedSourceNode1);

        final TopologyDescription.Source expectedSourceNode2 = addSource("source2", "topic2");
        final TopologyDescription.Processor expectedProcessorNode2 = addProcessor("processor2", expectedSourceNode2);

        final TopologyDescription.Source expectedSourceNode3 = addSource("source3", "topic3");
        final TopologyDescription.Processor expectedProcessorNode3 = addProcessor("processor3", expectedSourceNode3);

        final Set<TopologyDescription.Node> allNodes1 = new HashSet<>();
        allNodes1.add(expectedSourceNode1);
        allNodes1.add(expectedProcessorNode1);
        expectedDescription.addSubtopology(new InternalTopologyBuilder.Subtopology(0, allNodes1));

        final Set<TopologyDescription.Node> allNodes2 = new HashSet<>();
        allNodes2.add(expectedSourceNode2);
        allNodes2.add(expectedProcessorNode2);
        expectedDescription.addSubtopology(new InternalTopologyBuilder.Subtopology(1, allNodes2));

        final Set<TopologyDescription.Node> allNodes3 = new HashSet<>();
        allNodes3.add(expectedSourceNode3);
        allNodes3.add(expectedProcessorNode3);
        expectedDescription.addSubtopology(new InternalTopologyBuilder.Subtopology(2, allNodes3));

        assertThat(topology.describe(), equalTo((TopologyDescription) expectedDescription));
    }

    @Test
    public void multipleSourcesWithSinksShouldHaveDistinctSubtopologies() {
        final TopologyDescription.Source expectedSourceNode1 = addSource("source1", "topic1");
        final TopologyDescription.Sink expectedSinkNode1 = addSink("sink1", "sinkTopic1", expectedSourceNode1);

        final TopologyDescription.Source expectedSourceNode2 = addSource("source2", "topic2");
        final TopologyDescription.Sink expectedSinkNode2 = addSink("sink2", "sinkTopic2", expectedSourceNode2);

        final TopologyDescription.Source expectedSourceNode3 = addSource("source3", "topic3");
        final TopologyDescription.Sink expectedSinkNode3 = addSink("sink3", "sinkTopic3", expectedSourceNode3);

        final Set<TopologyDescription.Node> allNodes1 = new HashSet<>();
        allNodes1.add(expectedSourceNode1);
        allNodes1.add(expectedSinkNode1);
        expectedDescription.addSubtopology(new InternalTopologyBuilder.Subtopology(0, allNodes1));

        final Set<TopologyDescription.Node> allNodes2 = new HashSet<>();
        allNodes2.add(expectedSourceNode2);
        allNodes2.add(expectedSinkNode2);
        expectedDescription.addSubtopology(new InternalTopologyBuilder.Subtopology(1, allNodes2));

        final Set<TopologyDescription.Node> allNodes3 = new HashSet<>();
        allNodes3.add(expectedSourceNode3);
        allNodes3.add(expectedSinkNode3);
        expectedDescription.addSubtopology(new InternalTopologyBuilder.Subtopology(2, allNodes3));

        assertThat(topology.describe(), equalTo((TopologyDescription) expectedDescription));
    }

    @Test
    public void processorsWithSameSinkShouldHaveSameSubtopology() {
        final TopologyDescription.Source expectedSourceNode1 = addSource("source", "topic");
        final TopologyDescription.Processor expectedProcessorNode1 = addProcessor("processor1", expectedSourceNode1);

        final TopologyDescription.Source expectedSourceNode2 = addSource("source2", "topic2");
        final TopologyDescription.Processor expectedProcessorNode2 = addProcessor("processor2", expectedSourceNode2);

        final TopologyDescription.Source expectedSourceNode3 = addSource("source3", "topic3");
        final TopologyDescription.Processor expectedProcessorNode3 = addProcessor("processor3", expectedSourceNode3);

        final TopologyDescription.Sink expectedSinkNode = addSink(
            "sink",
            "sinkTopic",
            expectedProcessorNode1,
            expectedProcessorNode2,
            expectedProcessorNode3);

        final Set<TopologyDescription.Node> allNodes = new HashSet<>();
        allNodes.add(expectedSourceNode1);
        allNodes.add(expectedProcessorNode1);
        allNodes.add(expectedSourceNode2);
        allNodes.add(expectedProcessorNode2);
        allNodes.add(expectedSourceNode3);
        allNodes.add(expectedProcessorNode3);
        allNodes.add(expectedSinkNode);
        expectedDescription.addSubtopology(new InternalTopologyBuilder.Subtopology(0, allNodes));

        assertThat(topology.describe(), equalTo((TopologyDescription) expectedDescription));
    }

    @Test
    public void processorsWithSharedStateShouldHaveSameSubtopology() {
        final String[] store1 = new String[] {"store1"};
        final String[] store2 = new String[] {"store2"};
        final String[] bothStores = new String[] {store1[0], store2[0]};

        final TopologyDescription.Source expectedSourceNode1 = addSource("source", "topic");
        final TopologyDescription.Processor expectedProcessorNode1
            = addProcessorWithNewStore("processor1", store1, expectedSourceNode1);

        final TopologyDescription.Source expectedSourceNode2 = addSource("source2", "topic2");
        final TopologyDescription.Processor expectedProcessorNode2
            = addProcessorWithNewStore("processor2", store2, expectedSourceNode2);

        final TopologyDescription.Source expectedSourceNode3 = addSource("source3", "topic3");
        final TopologyDescription.Processor expectedProcessorNode3
            = addProcessorWithExistingStore("processor3", bothStores, expectedSourceNode3);

        final Set<TopologyDescription.Node> allNodes = new HashSet<>();
        allNodes.add(expectedSourceNode1);
        allNodes.add(expectedProcessorNode1);
        allNodes.add(expectedSourceNode2);
        allNodes.add(expectedProcessorNode2);
        allNodes.add(expectedSourceNode3);
        allNodes.add(expectedProcessorNode3);
        expectedDescription.addSubtopology(new InternalTopologyBuilder.Subtopology(0, allNodes));

        assertThat(topology.describe(), equalTo((TopologyDescription) expectedDescription));
    }

    @Test
    public void shouldDescribeGlobalStoreTopology() {
        addGlobalStoreToTopologyAndExpectedDescription("globalStore", "source", "globalTopic", "processor", 0);
        assertThat(topology.describe(), equalTo((TopologyDescription) expectedDescription));
    }

    @Test
    public void shouldDescribeMultipleGlobalStoreTopology() {
        addGlobalStoreToTopologyAndExpectedDescription("globalStore1", "source1", "globalTopic1", "processor1", 0);
        addGlobalStoreToTopologyAndExpectedDescription("globalStore2", "source2", "globalTopic2", "processor2", 1);
        assertThat(topology.describe(), equalTo((TopologyDescription) expectedDescription));
    }

    private TopologyDescription.Source addSource(final String sourceName,
                                                 final String... sourceTopic) {
        topology.addSource(null, sourceName, null, null, null, sourceTopic);
        String allSourceTopics = sourceTopic[0];
        for (int i = 1; i < sourceTopic.length; ++i) {
            allSourceTopics += ", " + sourceTopic[i];
        }
        return new InternalTopologyBuilder.Source(sourceName, allSourceTopics);
    }

    private TopologyDescription.Source addSource(final String sourceName,
                                                 final Pattern sourcePattern) {
        topology.addSource(null, sourceName, null, null, null, sourcePattern);
        return new InternalTopologyBuilder.Source(sourceName, sourcePattern.toString());
    }

    private TopologyDescription.Processor addProcessor(final String processorName,
                                                       final TopologyDescription.Node... parents) {
        return addProcessorWithNewStore(processorName, new String[0], parents);
    }

    private TopologyDescription.Processor addProcessorWithNewStore(final String processorName,
                                                                   final String[] storeNames,
                                                                   final TopologyDescription.Node... parents) {
        return addProcessorWithStore(processorName, storeNames, true, parents);
    }

    private TopologyDescription.Processor addProcessorWithExistingStore(final String processorName,
                                                                        final String[] storeNames,
                                                                        final TopologyDescription.Node... parents) {
        return addProcessorWithStore(processorName, storeNames, false, parents);
    }

    private TopologyDescription.Processor addProcessorWithStore(final String processorName,
                                                                final String[] storeNames,
                                                                final boolean newStores,
                                                                final TopologyDescription.Node... parents) {
        final String[] parentNames = new String[parents.length];
        for (int i = 0; i < parents.length; ++i) {
            parentNames[i] = parents[i].name();
        }

        topology.addProcessor(processorName, new MockProcessorSupplier(), parentNames);
        if (newStores) {
            for (final String store : storeNames) {
                final StoreBuilder storeBuilder = EasyMock.createNiceMock(StoreBuilder.class);
                EasyMock.expect(storeBuilder.name()).andReturn(store).anyTimes();
                EasyMock.replay(storeBuilder);
                topology.addStateStore(storeBuilder, processorName);
            }
        } else {
            topology.connectProcessorAndStateStores(processorName, storeNames);
        }
        final TopologyDescription.Processor expectedProcessorNode
            = new InternalTopologyBuilder.Processor(processorName, new HashSet<>(Arrays.asList(storeNames)));

        for (final TopologyDescription.Node parent : parents) {
            ((InternalTopologyBuilder.AbstractNode) parent).addSuccessor(expectedProcessorNode);
            ((InternalTopologyBuilder.AbstractNode) expectedProcessorNode).addPredecessor(parent);
        }

        return expectedProcessorNode;
    }

    private TopologyDescription.Sink addSink(final String sinkName,
                                             final String sinkTopic,
                                             final TopologyDescription.Node... parents) {
        final String[] parentNames = new String[parents.length];
        for (int i = 0; i < parents.length; ++i) {
            parentNames[i] = parents[i].name();
        }

        topology.addSink(sinkName, sinkTopic, null, null, null, parentNames);
        final TopologyDescription.Sink expectedSinkNode
            = new InternalTopologyBuilder.Sink(sinkName, sinkTopic);

        for (final TopologyDescription.Node parent : parents) {
            ((InternalTopologyBuilder.AbstractNode) parent).addSuccessor(expectedSinkNode);
            ((InternalTopologyBuilder.AbstractNode) expectedSinkNode).addPredecessor(parent);
        }

        return expectedSinkNode;
    }

    private void addGlobalStoreToTopologyAndExpectedDescription(final String globalStoreName,
                                                                final String sourceName,
                                                                final String globalTopicName,
                                                                final String processorName,
                                                                final int id) {
        final KeyValueStoreBuilder globalStoreBuilder = EasyMock.createNiceMock(KeyValueStoreBuilder.class);
        EasyMock.expect(globalStoreBuilder.name()).andReturn(globalStoreName).anyTimes();
        EasyMock.replay(globalStoreBuilder);
        topology.addGlobalStore(
            globalStoreBuilder,
            sourceName,
            null,
            null,
            null,
            globalTopicName,
            processorName,
            new MockProcessorSupplier());

        final TopologyDescription.GlobalStore expectedGlobalStore = new InternalTopologyBuilder.GlobalStore(
            sourceName,
            processorName,
            globalStoreName,
            globalTopicName,
            id);

        expectedDescription.addGlobalStore(expectedGlobalStore);
    }
}
