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
package org.apache.kafka.tools.streams;

import org.apache.kafka.clients.admin.StreamsGroupTopologyDescription;
import org.apache.kafka.clients.admin.StreamsGroupTopologyDescription.GlobalStore;
import org.apache.kafka.clients.admin.StreamsGroupTopologyDescription.Node;
import org.apache.kafka.clients.admin.StreamsGroupTopologyDescription.Processor;
import org.apache.kafka.clients.admin.StreamsGroupTopologyDescription.Sink;
import org.apache.kafka.clients.admin.StreamsGroupTopologyDescription.Source;
import org.apache.kafka.clients.admin.StreamsGroupTopologyDescription.Subtopology;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TopologyDescriptionFormatterTest {

    @Test
    public void testFormatSimpleSubtopology() {
        Source source = new Source("source", Set.of("input"), Set.of("processor"), Set.of());
        Processor processor = new Processor("processor", Set.of("store"), Set.of("sink"), Set.of("source"));
        Sink sink = new Sink("sink", Optional.of("output"), Set.of(), Set.of("processor"));
        StreamsGroupTopologyDescription topology = new StreamsGroupTopologyDescription(
            List.of(new Subtopology("0", List.<Node>of(source, processor, sink))),
            List.of());

        String expected = "Topologies:\n" +
            "   Sub-topology: 0\n" +
            "    Source: source (topics: [input])\n" +
            "      --> processor\n" +
            "    Processor: processor (stores: [store])\n" +
            "      --> sink\n" +
            "      <-- source\n" +
            "    Sink: sink (topic: output)\n" +
            "      <-- processor\n" +
            "\n";

        assertEquals(expected, TopologyDescriptionFormatter.format(topology));
    }

    @Test
    public void testFormatGlobalStore() {
        Source source = new Source("global-source", Set.of("global-topic"), Set.of("global-processor"), Set.of());
        Processor processor = new Processor("global-processor", Set.of("global-store"), Set.of(), Set.of("global-source"));
        StreamsGroupTopologyDescription topology = new StreamsGroupTopologyDescription(
            List.of(),
            List.of(new GlobalStore(source, processor)));

        String expected = "Topologies:\n" +
            "   Sub-topology: 0 for global store (will not generate tasks)\n" +
            "    Source: global-source (topics: [global-topic])\n" +
            "      --> global-processor\n" +
            "    Processor: global-processor (stores: [global-store])\n" +
            "      --> none\n" +
            "      <-- global-source\n" +
            "\n";

        assertEquals(expected, TopologyDescriptionFormatter.format(topology));
    }

    @Test
    public void testFormatMultipleGlobalStoresAreSeparatedByBlankLine() {
        Source source0 = new Source("global-source-0", Set.of("topic-0"), Set.of("global-processor-0"), Set.of());
        Processor processor0 = new Processor("global-processor-0", Set.of("store-0"), Set.of(), Set.of("global-source-0"));
        Source source1 = new Source("global-source-1", Set.of("topic-1"), Set.of("global-processor-1"), Set.of());
        Processor processor1 = new Processor("global-processor-1", Set.of("store-1"), Set.of(), Set.of("global-source-1"));
        StreamsGroupTopologyDescription topology = new StreamsGroupTopologyDescription(
            List.of(),
            List.of(new GlobalStore(source0, processor0), new GlobalStore(source1, processor1)));

        assertEquals("Topologies:\n" +
            "   Sub-topology: 0 for global store (will not generate tasks)\n" +
            "    Source: global-source-0 (topics: [topic-0])\n" +
            "      --> global-processor-0\n" +
            "    Processor: global-processor-0 (stores: [store-0])\n" +
            "      --> none\n" +
            "      <-- global-source-0\n" +
            "\n" +
            "   Sub-topology: 1 for global store (will not generate tasks)\n" +
            "    Source: global-source-1 (topics: [topic-1])\n" +
            "      --> global-processor-1\n" +
            "    Processor: global-processor-1 (stores: [store-1])\n" +
            "      --> none\n" +
            "      <-- global-source-1\n" +
            "\n", TopologyDescriptionFormatter.format(topology));
    }

    @Test
    public void testFormatMultipleSubtopologiesAreIndentedConsistently() {
        Source source0 = new Source("source-0", Set.of("input-0"), Set.of(), Set.of());
        Source source1 = new Source("source-1", Set.of("input-1"), Set.of(), Set.of());
        StreamsGroupTopologyDescription topology = new StreamsGroupTopologyDescription(
            List.of(
                new Subtopology("0", List.<Node>of(source0)),
                new Subtopology("1", List.<Node>of(source1))),
            List.of());

        assertEquals("Topologies:\n" +
            "   Sub-topology: 0\n" +
            "    Source: source-0 (topics: [input-0])\n" +
            "      --> none\n" +
            "\n" +
            "   Sub-topology: 1\n" +
            "    Source: source-1 (topics: [input-1])\n" +
            "      --> none\n" +
            "\n", TopologyDescriptionFormatter.format(topology));
    }

    @Test
    public void testFormatRendersEmptySuccessorsAndPredecessorsAsNone() {
        Processor processor = new Processor("processor", Set.of("store"), Set.of(), Set.of());
        StreamsGroupTopologyDescription topology = new StreamsGroupTopologyDescription(
            List.of(new Subtopology("0", List.<Node>of(processor))),
            List.of());

        assertEquals("Topologies:\n" +
            "   Sub-topology: 0\n" +
            "    Processor: processor (stores: [store])\n" +
            "      --> none\n" +
            "      <-- none\n" +
            "\n", TopologyDescriptionFormatter.format(topology));
    }

    @Test
    public void testFormatEmptyTopologyHasNoTrailingSpace() {
        StreamsGroupTopologyDescription topology = new StreamsGroupTopologyDescription(List.of(), List.of());
        assertEquals("Topologies:\n", TopologyDescriptionFormatter.format(topology));
    }

    @Test
    public void testFormatSinkWithoutTopicAndMultipleSuccessors() {
        Source source = new Source("source", Set.of("t2", "t1"), Set.of("p2", "p1"), Set.of());
        Sink sink = new Sink("sink", Optional.empty(), Set.of(), Set.of("source"));
        StreamsGroupTopologyDescription topology = new StreamsGroupTopologyDescription(
            List.of(new Subtopology("0", List.<Node>of(source, sink))),
            List.of());

        String formatted = TopologyDescriptionFormatter.format(topology);

        assertEquals("Topologies:\n" +
            "   Sub-topology: 0\n" +
            "    Source: source (topics: [t1, t2])\n" +
            "      --> p1, p2\n" +
            "    Sink: sink (topic: null)\n" +
            "      <-- source\n" +
            "\n", formatted);
    }
}
