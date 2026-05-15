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

import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TopologyDescriptionFormatterTest {

    @Test
    public void testFormatSimpleSourceProcessorSink() {
        StreamsGroupTopologyDescription.Source source = new StreamsGroupTopologyDescription.Source(
            "KSTREAM-SOURCE-0000000000",
            new LinkedHashSet<>(List.of("input-topic")),
            Set.of(),
            new LinkedHashSet<>(List.of("my-processor"))
        );
        StreamsGroupTopologyDescription.Processor processor = new StreamsGroupTopologyDescription.Processor(
            "my-processor",
            new LinkedHashSet<>(List.of("my-store")),
            new LinkedHashSet<>(List.of("KSTREAM-SOURCE-0000000000")),
            new LinkedHashSet<>(List.of("KSTREAM-SINK-0000000002"))
        );
        StreamsGroupTopologyDescription.Sink sink = new StreamsGroupTopologyDescription.Sink(
            "KSTREAM-SINK-0000000002",
            Optional.of("output-topic"),
            new LinkedHashSet<>(List.of("my-processor")),
            Set.of()
        );
        StreamsGroupTopologyDescription topology = new StreamsGroupTopologyDescription(
            List.of(new StreamsGroupTopologyDescription.Subtopology(
                "0",
                List.of(source, processor, sink)
            )),
            List.of()
        );

        String output = TopologyDescriptionFormatter.format(topology);
        String expected =
            "Topologies:\n" +
            "   Sub-topology: 0\n" +
            "     Source: KSTREAM-SOURCE-0000000000 (topics: [input-topic])\n" +
            "       --> my-processor\n" +
            "     Processor: my-processor (stores: [my-store])\n" +
            "       <-- KSTREAM-SOURCE-0000000000\n" +
            "       --> KSTREAM-SINK-0000000002\n" +
            "     Sink: KSTREAM-SINK-0000000002 (topic: output-topic)\n" +
            "       <-- my-processor\n";
        assertEquals(expected, output);
    }

    @Test
    public void testFormatEmptyTopology() {
        StreamsGroupTopologyDescription topology = new StreamsGroupTopologyDescription(List.of(), List.of());
        assertEquals("Topologies:\n", TopologyDescriptionFormatter.format(topology));
    }

    @Test
    public void testFormatWithGlobalStore() {
        StreamsGroupTopologyDescription.Source globalSource = new StreamsGroupTopologyDescription.Source(
            "global-source",
            new LinkedHashSet<>(List.of("global-topic")),
            Set.of(),
            Set.of()
        );
        StreamsGroupTopologyDescription.Processor globalProcessor = new StreamsGroupTopologyDescription.Processor(
            "global-processor",
            new LinkedHashSet<>(List.of("global-store")),
            Set.of(),
            Set.of()
        );
        StreamsGroupTopologyDescription topology = new StreamsGroupTopologyDescription(
            List.of(),
            List.of(new StreamsGroupTopologyDescription.GlobalStore(globalSource, globalProcessor))
        );

        String output = TopologyDescriptionFormatter.format(topology);
        assertTrue(output.contains("Global Stores:"));
        assertTrue(output.contains("Source: global-source"));
        assertTrue(output.contains("Processor: global-processor"));
    }
}
