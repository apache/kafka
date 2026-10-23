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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Formats a {@link StreamsGroupTopologyDescription} as human-readable text, mirroring the output of
 * {@code org.apache.kafka.streams.Topology#describe()} so that users see a familiar representation. Node names, topics,
 * stores and the successor/predecessor relations are sorted for stable, readable output.
 */
public final class TopologyDescriptionFormatter {

    private TopologyDescriptionFormatter() {
    }

    public static String format(final StreamsGroupTopologyDescription topology) {
        final StringBuilder sb = new StringBuilder();
        sb.append("Topologies:\n");
        // Kafka Streams numbers subtopologies and global stores from a single counter, but the wire format carries no
        // global store id. We assume both lists arrive sorted by id, which holds because
        // org.apache.kafka.streams.processor.internals.TopologyDescriptionConverter#toWire sorts them before sending;
        // neither the protocol nor the topology description plugin guarantees it. Under that assumption the global
        // store ids are exactly the ids not taken by a subtopology, in ascending order.
        final List<GlobalStore> globalStores = new ArrayList<>(topology.globalStores());
        final List<Integer> globalStoreIds = globalStoreIds(topology.subtopologies(), globalStores.size());
        int next = 0;
        // Interleave subtopologies and global stores by id, like Topology#describe().
        for (final Subtopology subtopology : topology.subtopologies()) {
            final Integer subtopologyId = numericId(subtopology);
            while (subtopologyId != null && next < globalStores.size() && globalStoreIds.get(next) < subtopologyId) {
                sb.append("   ");
                appendGlobalStore(sb, globalStores.get(next), globalStoreIds.get(next));
                next++;
            }
            sb.append("   ");
            appendSubtopology(sb, subtopology);
        }
        for (; next < globalStores.size(); next++) {
            sb.append("   ");
            appendGlobalStore(sb, globalStores.get(next), globalStoreIds.get(next));
        }
        return sb.toString();
    }

    private static List<Integer> globalStoreIds(final Collection<Subtopology> subtopologies, final int count) {
        final Set<Integer> subtopologyIds = new HashSet<>();
        for (final Subtopology subtopology : subtopologies) {
            final Integer id = numericId(subtopology);
            if (id != null) {
                subtopologyIds.add(id);
            }
        }
        final List<Integer> ids = new ArrayList<>(count);
        int id = 0;
        while (ids.size() < count) {
            if (!subtopologyIds.contains(id)) {
                ids.add(id);
            }
            id++;
        }
        return ids;
    }

    private static Integer numericId(final Subtopology subtopology) {
        try {
            return Integer.parseInt(subtopology.id());
        } catch (final NumberFormatException e) {
            // a non-numeric id cannot collide with a numeric global store label
            return null;
        }
    }

    private static void appendSubtopology(final StringBuilder sb, final Subtopology subtopology) {
        sb.append("Sub-topology: ").append(subtopology.id()).append('\n');
        for (final Node node : subtopology.nodes()) {
            sb.append("    ");
            appendNode(sb, node);
            sb.append('\n');
        }
        sb.append('\n');
    }

    private static void appendGlobalStore(final StringBuilder sb, final GlobalStore globalStore, final int id) {
        sb.append("Sub-topology: ").append(id).append(" for global store (will not generate tasks)\n");
        sb.append("    ");
        appendNode(sb, globalStore.source());
        sb.append('\n');
        sb.append("    ");
        appendNode(sb, globalStore.processor());
        sb.append('\n');
        sb.append('\n');
    }

    private static void appendNode(final StringBuilder sb, final Node node) {
        if (node instanceof Source) {
            final Source source = (Source) node;
            sb.append("Source: ").append(source.name())
                .append(" (topics: ").append(sorted(source.topics())).append(")")
                .append("\n      --> ").append(nodeNames(source.successors()));
        } else if (node instanceof Processor) {
            final Processor processor = (Processor) node;
            sb.append("Processor: ").append(processor.name())
                .append(" (stores: ").append(sorted(processor.stores())).append(")")
                .append("\n      --> ").append(nodeNames(processor.successors()))
                .append("\n      <-- ").append(nodeNames(processor.predecessors()));
        } else if (node instanceof Sink) {
            final Sink sink = (Sink) node;
            sb.append("Sink: ").append(sink.name())
                .append(" (topic: ").append(sink.topic().orElse(null)).append(")")
                .append("\n      <-- ").append(nodeNames(sink.predecessors()));
        } else {
            throw new IllegalStateException("Unknown topology node type: " + node.getClass().getName());
        }
    }

    private static TreeSet<String> sorted(final Collection<String> values) {
        return new TreeSet<>(values);
    }

    private static String nodeNames(final Collection<String> names) {
        if (names.isEmpty()) {
            return "none";
        }
        return String.join(", ", sorted(names));
    }
}
