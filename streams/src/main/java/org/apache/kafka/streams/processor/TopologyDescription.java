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
import java.util.Objects;
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
    private final Set<Subtopology> subtopologies = new HashSet<>();
    private final Set<GlobalStore> globalStores = new HashSet<>();

    /**
     * A connected sub-graph of a {@link Topology}.
     * <p>
     * Nodes of a {@code Subtopology} are connected {@link Topology#addProcessor(String, ProcessorSupplier, String...)
     * directly} or indirectly via {@link Topology#connectProcessorAndStateStores(String, String...) state stores}
     * (i.e., if multiple processors share the same state).
     */
    public final static class Subtopology {
        private final int id;
        private final Set<Node> nodes;

        Subtopology(final int id,
                    final Set<Node> nodes) {
            this.id = id;
            this.nodes = nodes;
        }

        /**
         * Internally assigned unique ID.
         * @return the ID of the sub-topology
         */
        public int id() {
            return id;
        }

        /**
         * All nodes of this sub-topology.
         * @return set of all nodes within the sub-topology
         */
        public Set<Node> nodes() {
            return Collections.unmodifiableSet(nodes);
        }

        @Override
        public String toString() {
            return "Sub-topology: " + id + "\n" + nodesAsString();
        }

        private String nodesAsString() {
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
            return Objects.hash(id, nodes);
        }
    }

    /**
     * Represents a {@link Topology#addGlobalStore(StateStoreSupplier, String,
     * org.apache.kafka.common.serialization.Deserializer, org.apache.kafka.common.serialization.Deserializer, String,
     * String, ProcessorSupplier)} global store}.
     * Adding a global store results in adding a source node and one stateful processor node.
     * Note, that all added global stores form a single unit (similar to a {@link Subtopology}) even if different
     * global stores are not connected to each other.
     * Furthermore, global stores are available to all processors without connecting them explicitly, and thus global
     * stores will never be part of any {@link Subtopology}.
     */
    public final static class GlobalStore {
        private final Source source;
        private final Processor processor;

        GlobalStore(final String sourceName,
                    final String processorName,
                    final String storeName,
                    final String topicName) {
            source = new Source(sourceName, topicName);
            processor = new Processor(processorName, Collections.singleton(storeName));
            source.successors.add(processor);
            processor.predecessors.add(source);
        }

        /**
         * The source node reading from a "global" topic.
         * @return the "global" source node
         */
        public Source source() {
            return source;
        }

        /**
         * The processor node maintaining the global store.
         * @return the "global" processor node
         */
        public Processor processor() {
            return processor;
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
            return Objects.hash(source, processor);
        }
    }

    /**
     * A node of a topology. Can be a source, sink, or processor node.
     */
    public interface Node {
        /**
         * The name of the node. Will never be {@code null}.
         * @return the name of the node
         */
        String name();
        /**
         * The predecessors of this node within a sub-topology.
         * Note, sources do not have any predecessors.
         * Will never be {@code null}.
         * @return set of all predecessors
         */
        Set<Node> predecessors();
        /**
         * The successor of this node within a sub-topology.
         * Note, sinks do not have any successors.
         * Will never be {@code null}.
         * @return set of all successor
         */
        Set<Node> successors();
    }

    abstract static class AbstractNode implements Node {
        final String name;
        final Set<Node> predecessors = new HashSet<>();
        final Set<Node> successors = new HashSet<>();

        AbstractNode(final String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Set<Node> predecessors() {
            return Collections.unmodifiableSet(predecessors);
        }

        @Override
        public Set<Node> successors() {
            return Collections.unmodifiableSet(successors);
        }

        void addPredecessor(final Node predecessor) {
            predecessors.add(predecessor);
        }

        void addSuccessor(final Node successor) {
            successors.add(successor);
        }
    }

    /**
     * A source node of a topology.
     */
    public final static class Source extends AbstractNode {
        private final String topics;

        Source(final String name,
               final String topics) {
            super(name);
            this.topics = topics;
        }

        /**
         * The topic names this source node is reading from.
         * @return comma separated list of topic names or pattern (as String)
         */
        public String topics() {
            return topics;
        }

        @Override
        void addPredecessor(final Node predecessor) {
            throw new UnsupportedOperationException("Sources don't have predecessors.");
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
            // omit successor to avoid infinite loops
            return name.equals(source.name)
                && topics.equals(source.topics);
        }

        @Override
        public int hashCode() {
            // omit successor as it might change and alter the hash code
            return Objects.hash(name, topics);
        }
    }

    /**
     * A processor node of a topology.
     */
    public final static class Processor extends AbstractNode {
        private final Set<String> stores;

        Processor(final String name,
                  final Set<String> stores) {
            super(name);
            this.stores = stores;
        }

        /**
         * The names of all connected stores.
         * @return set of store names
         */
        public Set<String> stores() {
            return Collections.unmodifiableSet(stores);
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
            // omit successor to avoid infinite loops
            return name.equals(processor.name)
                && stores.equals(processor.stores)
                && predecessors.equals(processor.predecessors);
        }

        @Override
        public int hashCode() {
            // omit successor as it might change and alter the hash code
            return Objects.hash(name, stores);
        }
    }

    /**
     * A sink node of a topology.
     */
    public final static class Sink extends AbstractNode {
        private final String topic;

        Sink(final String name,
             final String topic) {
            super(name);
            this.topic = topic;
        }

        /**
         * The topic name this sink node is writing to.
         * @return a topic name
         */
        public String topic() {
            return topic;
        }

        @Override
        void addSuccessor(final Node successor) {
            throw new UnsupportedOperationException("Sinks don't have successors.");
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
            // omit predecessors as it might change and alter the hash code
            return Objects.hash(name, topic);
        }
    }

    void addSubtopology(final Subtopology subtopology) {
        subtopologies.add(subtopology);
    }

    void addGlobalStore(final GlobalStore globalStore) {
        globalStores.add(globalStore);
    }

    /**
     * All sub-topologies of the represented topology.
     * @return set of all sub-topologies
     */
    public Set<Subtopology> subtopologies() {
        return Collections.unmodifiableSet(subtopologies);
    }

    /**
     * All global stores of the represented topology.
     * @return set of all global stores
     */
    public Set<GlobalStore> globalStores() {
        return Collections.unmodifiableSet(globalStores);
    }

    @Override
    public String toString() {
        return subtopologiesAsString() + globalStoresAsString();
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

    private String subtopologiesAsString() {
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

    private String globalStoresAsString() {
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
        return Objects.hash(subtopologies, globalStores);
    }

}

