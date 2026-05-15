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

import java.util.Collection;

/**
 * Formats a {@link StreamsGroupTopologyDescription} using the same layout as
 * {@code Topology#describe().toString()} in the Kafka Streams API. This keeps the
 * {@code kafka-streams-groups.sh --describe --topology} output familiar to users who
 * have seen the client-side topology description before.
 */
final class TopologyDescriptionFormatter {

    private TopologyDescriptionFormatter() {
    }

    static String format(StreamsGroupTopologyDescription description) {
        StringBuilder sb = new StringBuilder("Topologies:\n");
        for (StreamsGroupTopologyDescription.Subtopology subtopology : description.subtopologies()) {
            sb.append("   Sub-topology: ").append(subtopology.id()).append('\n');
            for (StreamsGroupTopologyDescription.Node node : subtopology.nodes()) {
                appendNode(sb, node, "     ");
            }
        }
        if (!description.globalStores().isEmpty()) {
            sb.append("  Global Stores:\n");
            for (StreamsGroupTopologyDescription.GlobalStore globalStore : description.globalStores()) {
                appendNode(sb, globalStore.source(), "     ");
                appendNode(sb, globalStore.processor(), "     ");
            }
        }
        return sb.toString();
    }

    private static void appendNode(StringBuilder sb, StreamsGroupTopologyDescription.Node node, String indent) {
        if (node instanceof StreamsGroupTopologyDescription.Source) {
            StreamsGroupTopologyDescription.Source source = (StreamsGroupTopologyDescription.Source) node;
            sb.append(indent).append("Source: ").append(source.name())
                .append(" (topics: ").append(source.topics()).append(")\n");
        } else if (node instanceof StreamsGroupTopologyDescription.Processor) {
            StreamsGroupTopologyDescription.Processor processor = (StreamsGroupTopologyDescription.Processor) node;
            sb.append(indent).append("Processor: ").append(processor.name())
                .append(" (stores: ").append(processor.stores()).append(")\n");
        } else if (node instanceof StreamsGroupTopologyDescription.Sink) {
            StreamsGroupTopologyDescription.Sink sink = (StreamsGroupTopologyDescription.Sink) node;
            sb.append(indent).append("Sink: ").append(sink.name())
                .append(" (topic: ").append(sink.topic().orElse("<dynamic>")).append(")\n");
        }
        appendNeighbours(sb, "<--", node.predecessors(), indent);
        appendNeighbours(sb, "-->", node.successors(), indent);
    }

    private static void appendNeighbours(StringBuilder sb, String arrow, Collection<String> names, String indent) {
        if (names.isEmpty()) {
            return;
        }
        sb.append(indent).append("  ").append(arrow).append(' ').append(String.join(", ", names)).append('\n');
    }
}
