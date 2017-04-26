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

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.internals.StreamTask;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A meta representation of a {@link Topology topology}.
 * <p>
 * The nodes of a topology are grouped into {@link Subtopology sub-topologies} if they are connected.
 * In contrast, two sub-topologies are not connected but can be linked to each other via topics, i.e., if one
 * sub-topology {@link Topology#addSink(String, String, String...) writes} into a topic and another sub-topology
 * {@link Topology#addSource(String, String...) reads} from the same topic.
 * <p>
 * For {@link KafkaStreams#start() execution} sub-topologies are translated into {@link StreamTask tasks}.
 */
// TODO make public (hide until KIP-120 if fully implemented)
final class TopologyDescription {
    /** All sub-topologies of the represented topology. */
    public final Set<Subtopology> subtopologies = new HashSet<>();
    /** All global stores of the represented topology. */
    public final Set<GlobalStore> globalStores = new HashSet<>();

    /**
     * A connect sub-graph of a {@link Topology}.
     * <p>
     * Nodes of a {@code Subtopology} are connected {@link Topology#addProcessor(String, ProcessorSupplier, String...)
     * directly} or indirectly via {@link Topology#connectProcessorAndStateStores(String, String...) state stores}
     * (i.e., if multiple processors share the same state).
     */
    public final static class Subtopology {
        /** Internally assigned unique ID. */
        public final int id;
        /** All nodes of this sub-topology. */
        public final Set<Node> nodes;

        Subtopology(final int id,
                    final Set<Node> nodes) {
            this.id = id;
            this.nodes = nodes;
        }

        @Override
        public String toString() {
            return "Sub-topology: " + id + "\n" + nodes();
        }

        private String nodes() {
            final StringBuilder sb = new StringBuilder();
            for (final Node node : nodes) {
                sb.append("    ");
                sb.append(node);
                sb.append('\n');
            }
            return sb.toString();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final Subtopology that = (Subtopology) o;
            return id == that.id
                && nodes.equals(that.nodes);
        }

        @Override
        public int hashCode() {
            int result = id;
            result = 31 * result + nodes.hashCode();
            return result;
        }
    }

    public final static class GlobalStore {
        /** The name of the global store. */
        public final Source source;
        /** The source topic of the global store. */
        public final Processor processor;

        GlobalStore(final String sourceName,
                    final String processorName,
                    final String storeName,
                    final String topicName) {
            source = new Source(sourceName, topicName);
            processor = new Processor(processorName, Collections.singleton(storeName));
            source.successors.add(processor);
            processor.predecessors.add(source);
        }

        @Override
        public String toString() {
            return "GlobalStore: " + source.name + "(topic: " + source.topics + ") -> "
                + processor.name + "(store: " + processor.stores.iterator().next() + ")\n";
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final GlobalStore that = (GlobalStore) o;
            return source.equals(that.source)
                && processor.equals(that.processor);
        }

        @Override
        public int hashCode() {
            int result = source.hashCode();
            result = 31 * result + processor.hashCode();
            return result;
        }
    }

    /**
     * A node of a topology. Can be a source, sink, or processor node.
     */
    public interface Node {
        /** The name of the node. */
        String name();
        /**
         * The predecessors of this node within a sub-topology.
         * Note, sources do not have any predecessors.
         */
        Set<Node> predecessors();
        /**
         * The successor of this node within a sub-topology.
         * Note, sinks do not have any successors.
         */
        Set<Node> successors();
    }

    /**
     * A source node of a topology.
     */
    public final static class Source implements Node {
        /** The name of this source node. */
        final String name;
        /**
         * The topics name this source node is reading from.
         * This can be comma separated list of topic names or pattern (as String).
         */
        public final String topics;
        /** All successors of this source node. */
        final Set<Node> successors = new HashSet<>();

        Source(final String name,
               final String topics) {
            this.name = name;
            this.topics = topics;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Set<Node> predecessors() {
            return Collections.emptySet();
        }

        @Override
        public Set<Node> successors() {
            return successors;
        }

        @Override
        public String toString() {
            return "Source: " + name + "(topics: " + topics + ") --> " + nodeNames(successors);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final Source source = (Source) o;
            // oit successor to avoid infinite loops
            return name.equals(source.name)
                && topics.equals(source.topics);
        }

        @Override
        public int hashCode() {
            int result = name.hashCode();
            result = 31 * result + topics.hashCode();
            // omit successor to avoid infinite loops
            return result;
        }
    }

    public final static class Processor implements Node {
        /** The name of this processor node. */
        final String name;
        /** The names of all connected stores. */
        public final Set<String> stores;
        /** All predecessors of this processor node. */
        final Set<Node> predecessors = new HashSet<>();
        /** All successors of this processor node. */
        final Set<Node> successors = new HashSet<>();

        Processor(final String name,
                  final Set<String> stores) {
            this.name = name;
            this.stores = stores;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Set<Node> predecessors() {
            return predecessors;
        }

        @Override
        public Set<Node> successors() {
            return successors;
        }

        @Override
        public String toString() {
            return "Processor: " + name + "(stores: " + stores + ") --> " + nodeNames(successors) + " <-- " + nodeNames(predecessors);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final Processor processor = (Processor) o;
            // oit successor to avoid infinite loops
            return name.equals(processor.name)
                && stores.equals(processor.stores)
                && predecessors.equals(processor.predecessors);
        }

        @Override
        public int hashCode() {
            int result = name.hashCode();
            result = 31 * result + stores.hashCode();
            result = 31 * result + predecessors.hashCode();
            // omit successor to avoid infinite loops
            return result;
        }
    }

    public final static class Sink implements Node {
        /** The name of this sink node. */
        final String name;
        /** The topic name this sink node is writing to. */
        public final String topic;
        /** All predecessors of this processor node. */
        final Set<Node> predecessors = new HashSet<>();

        Sink(final String name,
             final String topic) {
            this.name = name;
            this.topic = topic;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Set<Node> predecessors() {
            return predecessors;
        }

        @Override
        public Set<Node> successors() {
            return Collections.emptySet();
        }

        @Override
        public String toString() {
            return "Sink: " + name + "(topic: " + topic + ") <-- " + nodeNames(predecessors);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final Sink sink = (Sink) o;
            return name.equals(sink.name)
                && topic.equals(sink.topic)
                && predecessors.equals(sink.predecessors);
        }

        @Override
        public int hashCode() {
            int result = name.hashCode();
            result = 31 * result + topic.hashCode();
            result = 31 * result + predecessors.hashCode();
            return result;
        }
    }

    private static String nodeNames(final Set<Node> nodes) {
        final StringBuilder sb = new StringBuilder();
        if (!nodes.isEmpty()) {
            for (final Node n : nodes) {
                sb.append(n.name());
                sb.append(", ");
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return subtopologies() + globalStores();
    }

    private String subtopologies() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Sub-topologies: \n");
        if (subtopologies.isEmpty()) {
            sb.append("  none\n");
        } else {
            for (final Subtopology st : subtopologies) {
                sb.append("  ");
                sb.append(st);
            }
        }
        return sb.toString();
    }

    private String globalStores() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Global Stores:\n");
        if (globalStores.isEmpty()) {
            sb.append("  none\n");
        } else {
            for (final GlobalStore gs : globalStores) {
                sb.append("  ");
                sb.append(gs);
            }
        }
        return sb.toString();
    }
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final TopologyDescription that = (TopologyDescription) o;
        return subtopologies.equals(that.subtopologies)
            && globalStores.equals(that.globalStores);
    }

    @Override
    public int hashCode() {
        int result = subtopologies.hashCode();
        result = 31 * result + globalStores.hashCode();
        return result;
    }

}

