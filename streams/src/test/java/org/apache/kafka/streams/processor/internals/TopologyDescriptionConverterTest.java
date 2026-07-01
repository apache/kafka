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

import org.apache.kafka.common.message.StreamsGroupTopologyDescriptionUpdateRequestData;
import org.apache.kafka.streams.TopologyDescription;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TopologyDescriptionConverterTest {

    /**
     * Test the situation when the topology has multiple subtopologies, including one with two
     * source nodes feeding the same processor: subtopologies should be sorted by id and each node
     * should be emitted with its correct type, topics, and successors.
     */
    @Test
    public void shouldConvertMultipleSubtopologiesWithMultiSourceProcessor() {
        final TopologyDescription.Source source0 = mock(TopologyDescription.Source.class);
        final TopologyDescription.Sink sink0 = mock(TopologyDescription.Sink.class);
        when(source0.name()).thenReturn("source-0");
        when(source0.topicSet()).thenReturn(Set.of("topic-0-in"));
        when(source0.successors()).thenReturn(Set.of(sink0));
        when(sink0.name()).thenReturn("sink-0");
        when(sink0.topic()).thenReturn("topic-0-out");
        when(sink0.successors()).thenReturn(Set.of());

        final TopologyDescription.Subtopology subtopology0 = mock(TopologyDescription.Subtopology.class);
        when(subtopology0.id()).thenReturn(0);
        when(subtopology0.nodes()).thenReturn(Set.of(source0, sink0));

        final TopologyDescription.Source source1a = mock(TopologyDescription.Source.class);
        final TopologyDescription.Source source1b = mock(TopologyDescription.Source.class);
        final TopologyDescription.Processor processor1 = mock(TopologyDescription.Processor.class);
        final TopologyDescription.Sink sink1 = mock(TopologyDescription.Sink.class);

        when(source1a.name()).thenReturn("source-1-a");
        when(source1a.topicSet()).thenReturn(Set.of("topic-1-z", "topic-1-a"));
        when(source1a.successors()).thenReturn(Set.of(processor1));

        when(source1b.name()).thenReturn("source-1-b");
        when(source1b.topicSet()).thenReturn(Set.of("topic-1-m"));
        when(source1b.successors()).thenReturn(Set.of(processor1));

        when(processor1.name()).thenReturn("processor-1");
        when(processor1.stores()).thenReturn(Set.of());
        when(processor1.successors()).thenReturn(Set.of(sink1));

        when(sink1.name()).thenReturn("sink-1");
        when(sink1.topic()).thenReturn("topic-1-out");
        when(sink1.successors()).thenReturn(Set.of());

        final TopologyDescription.Subtopology subtopology1 = mock(TopologyDescription.Subtopology.class);
        when(subtopology1.id()).thenReturn(1);
        when(subtopology1.nodes()).thenReturn(Set.of(source1a, source1b, processor1, sink1));

        final TopologyDescription description = mock(TopologyDescription.class);
        when(description.subtopologies()).thenReturn(Set.of(subtopology1, subtopology0));
        when(description.globalStores()).thenReturn(Set.of());

        final StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription wire =
            TopologyDescriptionConverter.toWire(description, Function.identity());

        assertTrue(wire.globalStores().isEmpty());
        assertEquals(2, wire.subtopologies().size());

        final StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionSubtopology wireSub0 = wire.subtopologies().get(0);
        assertEquals("0", wireSub0.subtopologyId());
        assertEquals(2, wireSub0.nodes().size());
        assertEquals("sink-0", wireSub0.nodes().get(0).name());
        assertEquals("source-0", wireSub0.nodes().get(1).name());

        final StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionSubtopology wireSub1 = wire.subtopologies().get(1);
        assertEquals("1", wireSub1.subtopologyId());
        assertEquals(4, wireSub1.nodes().size());

        final List<StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode> nodes = wireSub1.nodes();

        final StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode wireProcessor1 = nodes.get(0);
        assertEquals("processor-1", wireProcessor1.name());
        assertEquals((byte) 2, wireProcessor1.nodeType());
        assertEquals(List.of(), wireProcessor1.stores());
        assertEquals(List.of("sink-1"), wireProcessor1.successors());

        final StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode wireSink1 = nodes.get(1);
        assertEquals("sink-1", wireSink1.name());
        assertEquals((byte) 3, wireSink1.nodeType());
        assertEquals("topic-1-out", wireSink1.sinkTopic());
        assertEquals(List.of(), wireSink1.successors());

        final StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode wireSource1a = nodes.get(2);
        assertEquals("source-1-a", wireSource1a.name());
        assertEquals((byte) 1, wireSource1a.nodeType());
        assertEquals(List.of("topic-1-a", "topic-1-z"), wireSource1a.sourceTopics());
        assertEquals(List.of("processor-1"), wireSource1a.successors());
        assertNull(wireSource1a.sinkTopic());

        final StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode wireSource1b = nodes.get(3);
        assertEquals("source-1-b", wireSource1b.name());
        assertEquals((byte) 1, wireSource1b.nodeType());
        assertEquals(List.of("topic-1-m"), wireSource1b.sourceTopics());
        assertEquals(List.of("processor-1"), wireSource1b.successors());
    }

    /**
     * Test the situation when a single source node branches into multiple processors: the source's
     * successors list should contain all branch processors sorted alphabetically.
     */
    @Test
    public void shouldConvertBranchTopology() {
        final TopologyDescription.Source source = mock(TopologyDescription.Source.class);
        final TopologyDescription.Processor processor1 = mock(TopologyDescription.Processor.class);
        final TopologyDescription.Processor processor2 = mock(TopologyDescription.Processor.class);
        final TopologyDescription.Sink sink = mock(TopologyDescription.Sink.class);

        when(source.name()).thenReturn("source");
        when(source.topicSet()).thenReturn(Set.of("input-topic"));
        when(source.successors()).thenReturn(Set.of(processor1, processor2));

        when(processor1.name()).thenReturn("processor-1");
        when(processor1.stores()).thenReturn(Set.of());
        when(processor1.successors()).thenReturn(Set.of(sink));

        when(processor2.name()).thenReturn("processor-2");
        when(processor2.stores()).thenReturn(Set.of());
        when(processor2.successors()).thenReturn(Set.of(sink));

        when(sink.name()).thenReturn("sink");
        when(sink.topic()).thenReturn("output-topic");
        when(sink.successors()).thenReturn(Set.of());

        final TopologyDescription.Subtopology subtopology = mock(TopologyDescription.Subtopology.class);
        when(subtopology.id()).thenReturn(0);
        when(subtopology.nodes()).thenReturn(Set.of(source, processor1, processor2, sink));

        final TopologyDescription description = mock(TopologyDescription.class);
        when(description.subtopologies()).thenReturn(Set.of(subtopology));
        when(description.globalStores()).thenReturn(Set.of());

        final StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription wire =
            TopologyDescriptionConverter.toWire(description, Function.identity());

        assertTrue(wire.globalStores().isEmpty());
        assertEquals(1, wire.subtopologies().size());

        final StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionSubtopology wireSub = wire.subtopologies().get(0);
        assertEquals("0", wireSub.subtopologyId());
        assertEquals(4, wireSub.nodes().size());

        final List<StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode> nodes = wireSub.nodes();

        final StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode wireProcessor1 = nodes.get(0);
        assertEquals("processor-1", wireProcessor1.name());
        assertEquals((byte) 2, wireProcessor1.nodeType());
        assertEquals(List.of(), wireProcessor1.stores());
        assertEquals(List.of("sink"), wireProcessor1.successors());

        final StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode wireProcessor2 = nodes.get(1);
        assertEquals("processor-2", wireProcessor2.name());
        assertEquals((byte) 2, wireProcessor2.nodeType());
        assertEquals(List.of(), wireProcessor2.stores());
        assertEquals(List.of("sink"), wireProcessor2.successors());

        final StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode wireSink = nodes.get(2);
        assertEquals("sink", wireSink.name());
        assertEquals((byte) 3, wireSink.nodeType());
        assertEquals("output-topic", wireSink.sinkTopic());
        assertEquals(List.of(), wireSink.successors());

        final StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode wireSource = nodes.get(3);
        assertEquals("source", wireSource.name());
        assertEquals((byte) 1, wireSource.nodeType());
        assertEquals(List.of("input-topic"), wireSource.sourceTopics());
        assertEquals(List.of("processor-1", "processor-2"), wireSource.successors());
        assertNull(wireSource.sinkTopic());
    }

    /**
     * Test the situation when the topology contains a stateful processor and a global store: state
     * store names should appear on the processor, and the global store should be emitted under
     * globalStores rather than subtopologies.
     */
    @Test
    public void shouldConvertTopologyWithStatefulProcessorAndGlobalStore() {
        final TopologyDescription.Source source = mock(TopologyDescription.Source.class);
        final TopologyDescription.Processor processor = mock(TopologyDescription.Processor.class);
        final TopologyDescription.Sink sink = mock(TopologyDescription.Sink.class);

        when(source.name()).thenReturn("source");
        when(source.topicSet()).thenReturn(Set.of("input-topic"));
        when(source.successors()).thenReturn(Set.of(processor));

        when(processor.name()).thenReturn("processor");
        when(processor.stores()).thenReturn(Set.of("store-1", "store-2"));
        when(processor.successors()).thenReturn(Set.of(sink));

        when(sink.name()).thenReturn("sink");
        when(sink.topic()).thenReturn("output-topic");
        when(sink.successors()).thenReturn(Set.of());

        final TopologyDescription.Source globalSource = mock(TopologyDescription.Source.class);
        when(globalSource.name()).thenReturn("global-source");
        when(globalSource.topicSet()).thenReturn(Set.of("global-topic"));
        when(globalSource.successors()).thenReturn(Set.of());

        final TopologyDescription.Processor globalProcessor = mock(TopologyDescription.Processor.class);
        when(globalProcessor.name()).thenReturn("global-processor");
        when(globalProcessor.stores()).thenReturn(Set.of("global-store"));
        when(globalProcessor.successors()).thenReturn(Set.of());

        final TopologyDescription.GlobalStore globalStore = mock(TopologyDescription.GlobalStore.class);
        when(globalStore.id()).thenReturn(0);
        when(globalStore.source()).thenReturn(globalSource);
        when(globalStore.processor()).thenReturn(globalProcessor);

        final TopologyDescription.Subtopology subtopology = mock(TopologyDescription.Subtopology.class);
        when(subtopology.id()).thenReturn(0);
        when(subtopology.nodes()).thenReturn(Set.of(source, processor, sink));

        final TopologyDescription description = mock(TopologyDescription.class);
        when(description.subtopologies()).thenReturn(Set.of(subtopology));
        when(description.globalStores()).thenReturn(Set.of(globalStore));

        final StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription wire =
            TopologyDescriptionConverter.toWire(description, Function.identity());

        assertEquals(1, wire.subtopologies().size());
        final StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionSubtopology wireSub = wire.subtopologies().get(0);
        assertEquals("0", wireSub.subtopologyId());
        assertEquals(3, wireSub.nodes().size());

        final List<StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode> nodes = wireSub.nodes();

        final StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode wireProcessor = nodes.get(0);
        assertEquals("processor", wireProcessor.name());
        assertEquals((byte) 2, wireProcessor.nodeType());
        assertEquals(List.of("store-1", "store-2"), wireProcessor.stores());
        assertEquals(List.of("sink"), wireProcessor.successors());

        final StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode wireSink = nodes.get(1);
        assertEquals("sink", wireSink.name());
        assertEquals((byte) 3, wireSink.nodeType());
        assertEquals("output-topic", wireSink.sinkTopic());
        assertEquals(List.of(), wireSink.successors());

        final StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode wireSource = nodes.get(2);
        assertEquals("source", wireSource.name());
        assertEquals((byte) 1, wireSource.nodeType());
        assertEquals(List.of("input-topic"), wireSource.sourceTopics());
        assertEquals(List.of("processor"), wireSource.successors());
        assertNull(wireSource.sinkTopic());

        assertEquals(1, wire.globalStores().size());
        final StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionGlobalStore wireGlobal = wire.globalStores().get(0);
        assertEquals("global-source", wireGlobal.source().name());
        assertEquals((byte) 1, wireGlobal.source().nodeType());
        assertEquals(List.of("global-topic"), wireGlobal.source().sourceTopics());
        assertEquals(List.of(), wireGlobal.source().successors());
        assertEquals("global-processor", wireGlobal.processor().name());
        assertEquals((byte) 2, wireGlobal.processor().nodeType());
        assertEquals(List.of("global-store"), wireGlobal.processor().stores());
        assertEquals(List.of(), wireGlobal.processor().successors());
    }

    /**
     * Test the situation when a sink uses a TopicNameExtractor (dynamic topic): the wire
     * SinkTopic field should be null since the schema has no field for the extractor itself.
     */
    @Test
    public void shouldConvertSinkWithDynamicTopic() {
        final TopologyDescription.Source source = mock(TopologyDescription.Source.class);
        final TopologyDescription.Sink sink = mock(TopologyDescription.Sink.class);

        when(source.name()).thenReturn("source");
        when(source.topicSet()).thenReturn(Set.of("input-topic"));
        when(source.successors()).thenReturn(Set.of(sink));

        when(sink.name()).thenReturn("sink");
        when(sink.topic()).thenReturn(null);
        when(sink.successors()).thenReturn(Set.of());

        final TopologyDescription.Subtopology subtopology = mock(TopologyDescription.Subtopology.class);
        when(subtopology.id()).thenReturn(0);
        when(subtopology.nodes()).thenReturn(Set.of(source, sink));

        final TopologyDescription description = mock(TopologyDescription.class);
        when(description.subtopologies()).thenReturn(Set.of(subtopology));
        when(description.globalStores()).thenReturn(Set.of());

        final StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription wire =
            TopologyDescriptionConverter.toWire(description, Function.identity());

        final List<StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode> nodes =
            wire.subtopologies().get(0).nodes();

        final StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode wireSink = nodes.get(0);
        assertEquals("sink", wireSink.name());
        assertEquals((byte) 3, wireSink.nodeType());
        assertNull(wireSink.sinkTopic());
    }

    /**
     * Test the situation when a topic-name decorator is supplied: internal topic names should be
     * decorated with the applicationId prefix on the wire output, while external (user-provided)
     * topic names should pass through unchanged.
     */
    @Test
    public void shouldApplyTopicNameDecorator() {
        final TopologyDescription.Source source = mock(TopologyDescription.Source.class);
        final TopologyDescription.Sink sink = mock(TopologyDescription.Sink.class);
        when(source.name()).thenReturn("source");
        when(source.topicSet()).thenReturn(Set.of("external-input-topic", "repartition-topic"));
        when(source.successors()).thenReturn(Set.of(sink));
        when(sink.name()).thenReturn("sink");
        when(sink.topic()).thenReturn("changelog-topic");
        when(sink.successors()).thenReturn(Set.of());

        final TopologyDescription.Subtopology subtopology = mock(TopologyDescription.Subtopology.class);
        when(subtopology.id()).thenReturn(0);
        when(subtopology.nodes()).thenReturn(Set.of(source, sink));

        final TopologyDescription description = mock(TopologyDescription.class);
        when(description.subtopologies()).thenReturn(Set.of(subtopology));
        when(description.globalStores()).thenReturn(Set.of());

        final Set<String> internalTopics = Set.of("repartition-topic", "changelog-topic");
        final Function<String, String> topicNameDecorator = name ->
            internalTopics.contains(name) ? "my-app-" + name : name;

        final StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription wire =
            TopologyDescriptionConverter.toWire(description, topicNameDecorator);

        final List<StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode> nodes =
            wire.subtopologies().get(0).nodes();

        final StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode wireSink = nodes.get(0);
        assertEquals("my-app-changelog-topic", wireSink.sinkTopic());

        final StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode wireSource = nodes.get(1);
        assertEquals(List.of("external-input-topic", "my-app-repartition-topic"), wireSource.sourceTopics());
    }
}
