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
package org.apache.kafka.coordinator.group.streams;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.StreamsGroupTopologyDescriptionUpdateRequestData;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescription;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StreamsGroupTopologyDescriptionConverterTest {

    @Test
    public void testConvertsAllThreeNodeKindsAndPreservesOrder() {
        StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode source =
            new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode()
                .setName("src")
                .setNodeType((byte) 1)
                .setSourceTopics(List.of("topic-a", "topic-b"))
                .setSuccessors(List.of("proc-1", "proc-2"));

        StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode processor =
            new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode()
                .setName("proc")
                .setNodeType((byte) 2)
                .setStores(List.of("store-x", "store-y"))
                .setSuccessors(List.of("sink"));

        StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode sink =
            new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode()
                .setName("sink")
                .setNodeType((byte) 3)
                .setSinkTopic("out-topic")
                .setSuccessors(List.of());

        StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionSubtopology subtopology =
            new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionSubtopology()
                .setSubtopologyId("0")
                .setNodes(List.of(source, processor, sink));

        StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription wire =
            new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription()
                .setSubtopologies(List.of(subtopology))
                .setGlobalStores(List.of());

        StreamsGroupTopologyDescription pojo = StreamsGroupTopologyDescriptionConverter.fromRequest(wire);

        assertEquals(1, pojo.subtopologies().size());
        StreamsGroupTopologyDescription.Subtopology st = pojo.subtopologies().iterator().next();
        assertEquals("0", st.id());

        Iterator<StreamsGroupTopologyDescription.Node> nodes = st.nodes().iterator();
        StreamsGroupTopologyDescription.Source src = assertInstanceOf(StreamsGroupTopologyDescription.Source.class, nodes.next());
        assertEquals("src", src.name());
        assertEquals(List.of("topic-a", "topic-b"), new ArrayList<>(src.topics()));
        assertEquals(List.of("proc-1", "proc-2"), new ArrayList<>(src.successors()));

        StreamsGroupTopologyDescription.Processor proc = assertInstanceOf(StreamsGroupTopologyDescription.Processor.class, nodes.next());
        assertEquals("proc", proc.name());
        assertEquals(List.of("store-x", "store-y"), new ArrayList<>(proc.stores()));

        StreamsGroupTopologyDescription.Sink sk = assertInstanceOf(StreamsGroupTopologyDescription.Sink.class, nodes.next());
        assertEquals("sink", sk.name());
        assertTrue(sk.topic().isPresent());
        assertEquals("out-topic", sk.topic().get());
    }

    @Test
    public void testSinkWithoutTopicYieldsEmptyOptional() {
        StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode sink =
            new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode()
                .setName("sink")
                .setNodeType((byte) 3)
                .setSinkTopic(null)
                .setSuccessors(List.of());

        StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription wire =
            new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription()
                .setSubtopologies(List.of(
                    new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionSubtopology()
                        .setSubtopologyId("0")
                        .setNodes(List.of(sink))))
                .setGlobalStores(List.of());

        StreamsGroupTopologyDescription pojo = StreamsGroupTopologyDescriptionConverter.fromRequest(wire);
        StreamsGroupTopologyDescription.Sink sk = (StreamsGroupTopologyDescription.Sink)
            pojo.subtopologies().iterator().next().nodes().iterator().next();
        assertTrue(sk.topic().isEmpty());
    }

    @Test
    public void testGlobalStoreIsConverted() {
        StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode source =
            new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode()
                .setName("g-src")
                .setNodeType((byte) 1)
                .setSourceTopics(List.of("global-topic"))
                .setSuccessors(List.of("g-proc"));

        StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode processor =
            new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode()
                .setName("g-proc")
                .setNodeType((byte) 2)
                .setStores(List.of("global-store"))
                .setSuccessors(List.of());

        StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionGlobalStore gs =
            new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionGlobalStore()
                .setSource(source)
                .setProcessor(processor);

        StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription wire =
            new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription()
                .setSubtopologies(List.of())
                .setGlobalStores(List.of(gs));

        StreamsGroupTopologyDescription pojo = StreamsGroupTopologyDescriptionConverter.fromRequest(wire);
        assertEquals(1, pojo.globalStores().size());
        StreamsGroupTopologyDescription.GlobalStore converted = pojo.globalStores().iterator().next();
        assertEquals("g-src", converted.source().name());
        assertEquals("g-proc", converted.processor().name());
    }

    @Test
    public void testUnknownNodeTypeIsRejected() {
        StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode unknown =
            new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode()
                .setName("?")
                .setNodeType((byte) 99)
                .setSuccessors(List.of());

        StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription wire =
            new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription()
                .setSubtopologies(List.of(
                    new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionSubtopology()
                        .setSubtopologyId("0")
                        .setNodes(List.of(unknown))))
                .setGlobalStores(List.of());

        assertThrows(InvalidRequestException.class,
            () -> StreamsGroupTopologyDescriptionConverter.fromRequest(wire));
    }

    @Test
    public void testGlobalStoreWithMismatchedNodesIsRejected() {
        StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode sourceA =
            new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode()
                .setName("a").setNodeType((byte) 1).setSuccessors(List.of());
        StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode sourceB =
            new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode()
                .setName("b").setNodeType((byte) 1).setSuccessors(List.of());
        StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionGlobalStore gs =
            new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionGlobalStore()
                .setSource(sourceA).setProcessor(sourceB);

        StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription wire =
            new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription()
                .setSubtopologies(List.of()).setGlobalStores(List.of(gs));

        assertThrows(InvalidRequestException.class,
            () -> StreamsGroupTopologyDescriptionConverter.fromRequest(wire));
    }

    @Test
    public void testEmptyTopologyIsAccepted() {
        StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription wire =
            new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription()
                .setSubtopologies(List.of())
                .setGlobalStores(List.of());
        StreamsGroupTopologyDescription pojo = StreamsGroupTopologyDescriptionConverter.fromRequest(wire);
        assertTrue(pojo.subtopologies().isEmpty());
        assertTrue(pojo.globalStores().isEmpty());
    }
}
