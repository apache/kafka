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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.annotation.InterfaceAudience;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * A description of a Kafka Streams topology, as recorded by the topology description plugin configured on the broker.
 * <p>
 * This type mirrors {@code org.apache.kafka.streams.TopologyDescription} in shape but lives in the admin client so that
 * callers do not need to depend on {@code kafka-streams}. The wire format only carries the successor relation between
 * nodes; the {@link Node#predecessors() predecessors} are reconstructed from the successors when this description is built.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class StreamsGroupTopologyDescription {

    private final Collection<Subtopology> subtopologies;
    private final Collection<GlobalStore> globalStores;

    public StreamsGroupTopologyDescription(
        final Collection<Subtopology> subtopologies,
        final Collection<GlobalStore> globalStores
    ) {
        this.subtopologies = List.copyOf(Objects.requireNonNull(subtopologies, "subtopologies must be non-null"));
        this.globalStores = List.copyOf(Objects.requireNonNull(globalStores, "globalStores must be non-null"));
    }

    /**
     * The subtopologies that make up this topology.
     */
    public Collection<Subtopology> subtopologies() {
        return subtopologies;
    }

    /**
     * The global state stores used by this topology.
     */
    public Collection<GlobalStore> globalStores() {
        return globalStores;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final StreamsGroupTopologyDescription that = (StreamsGroupTopologyDescription) o;
        return Objects.equals(subtopologies, that.subtopologies)
            && Objects.equals(globalStores, that.globalStores);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subtopologies, globalStores);
    }

    @Override
    public String toString() {
        return "StreamsGroupTopologyDescription(" +
            "subtopologies=" + subtopologies +
            ", globalStores=" + globalStores +
            ')';
    }

    /**
     * A connected sub-graph of a topology.
     */
    public static class Subtopology {

        private final String id;
        private final Collection<Node> nodes;

        public Subtopology(final String id, final Collection<Node> nodes) {
            this.id = Objects.requireNonNull(id, "id must be non-null");
            this.nodes = List.copyOf(Objects.requireNonNull(nodes, "nodes must be non-null"));
        }

        /**
         * The subtopology identifier, unique within the topology.
         */
        public String id() {
            return id;
        }

        /**
         * The processing nodes in this subtopology.
         */
        public Collection<Node> nodes() {
            return nodes;
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
            // Nodes within a subtopology are unordered, so compare them set-wise rather than by wire order.
            return Objects.equals(id, that.id) && Objects.equals(new HashSet<>(nodes), new HashSet<>(that.nodes));
        }

        @Override
        public int hashCode() {
            // Mirror the order-insensitive equality above; HashSet.hashCode() is independent of iteration order.
            return Objects.hash(id, new HashSet<>(nodes));
        }

        @Override
        public String toString() {
            return "Subtopology(id=" + id + ", nodes=" + nodes + ')';
        }
    }

    /**
     * A node of a topology. Can be a {@link Source}, {@link Sink}, or {@link Processor} node.
     */
    public interface Node {

        /**
         * The name of this node.
         */
        String name();

        /**
         * The names of the successor nodes within the subtopology.
         */
        Set<String> successors();

        /**
         * The names of the predecessor nodes within the subtopology, reconstructed from the successor relation.
         */
        Set<String> predecessors();
    }

    /**
     * A source node of a topology.
     */
    public static final class Source implements Node {

        private final String name;
        private final Set<String> topics;
        private final Set<String> successors;
        private final Set<String> predecessors;

        public Source(
            final String name,
            final Set<String> topics,
            final Set<String> successors,
            final Set<String> predecessors
        ) {
            this.name = Objects.requireNonNull(name, "name must be non-null");
            this.topics = Set.copyOf(Objects.requireNonNull(topics, "topics must be non-null"));
            this.successors = Set.copyOf(Objects.requireNonNull(successors, "successors must be non-null"));
            this.predecessors = Set.copyOf(Objects.requireNonNull(predecessors, "predecessors must be non-null"));
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Set<String> successors() {
            return successors;
        }

        @Override
        public Set<String> predecessors() {
            return predecessors;
        }

        /**
         * The topics this source node reads from. May be empty if the source topics are dynamically determined.
         */
        public Set<String> topics() {
            return topics;
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
            return Objects.equals(name, source.name)
                && Objects.equals(topics, source.topics)
                && Objects.equals(successors, source.successors)
                && Objects.equals(predecessors, source.predecessors);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, topics, successors, predecessors);
        }

        @Override
        public String toString() {
            return "Source(name=" + name + ", topics=" + topics + ", successors=" + successors + ", predecessors=" + predecessors + ')';
        }
    }

    /**
     * A processor node of a topology.
     */
    public static final class Processor implements Node {

        private final String name;
        private final Set<String> stores;
        private final Set<String> successors;
        private final Set<String> predecessors;

        public Processor(
            final String name,
            final Set<String> stores,
            final Set<String> successors,
            final Set<String> predecessors
        ) {
            this.name = Objects.requireNonNull(name, "name must be non-null");
            this.stores = Set.copyOf(Objects.requireNonNull(stores, "stores must be non-null"));
            this.successors = Set.copyOf(Objects.requireNonNull(successors, "successors must be non-null"));
            this.predecessors = Set.copyOf(Objects.requireNonNull(predecessors, "predecessors must be non-null"));
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Set<String> successors() {
            return successors;
        }

        @Override
        public Set<String> predecessors() {
            return predecessors;
        }

        /**
         * The names of the state stores accessed by this processor node.
         */
        public Set<String> stores() {
            return stores;
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
            return Objects.equals(name, processor.name)
                && Objects.equals(stores, processor.stores)
                && Objects.equals(successors, processor.successors)
                && Objects.equals(predecessors, processor.predecessors);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, stores, successors, predecessors);
        }

        @Override
        public String toString() {
            return "Processor(name=" + name + ", stores=" + stores + ", successors=" + successors + ", predecessors=" + predecessors + ')';
        }
    }

    /**
     * A sink node of a topology.
     */
    public static final class Sink implements Node {

        private final String name;
        private final Optional<String> topic;
        private final Set<String> successors;
        private final Set<String> predecessors;

        public Sink(
            final String name,
            final Optional<String> topic,
            final Set<String> successors,
            final Set<String> predecessors
        ) {
            this.name = Objects.requireNonNull(name, "name must be non-null");
            this.topic = Objects.requireNonNull(topic, "topic must be non-null");
            this.successors = Set.copyOf(Objects.requireNonNull(successors, "successors must be non-null"));
            this.predecessors = Set.copyOf(Objects.requireNonNull(predecessors, "predecessors must be non-null"));
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Set<String> successors() {
            return successors;
        }

        @Override
        public Set<String> predecessors() {
            return predecessors;
        }

        /**
         * The topic this sink node writes to. Empty if the topic is dynamically determined.
         */
        public Optional<String> topic() {
            return topic;
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
            return Objects.equals(name, sink.name)
                && Objects.equals(topic, sink.topic)
                && Objects.equals(successors, sink.successors)
                && Objects.equals(predecessors, sink.predecessors);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, topic, successors, predecessors);
        }

        @Override
        public String toString() {
            return "Sink(name=" + name + ", topic=" + topic.orElse(null) + ", successors=" + successors + ", predecessors=" + predecessors + ')';
        }
    }

    /**
     * A global state store, made up of a {@link Source} node and the {@link Processor} node that maintains the store.
     */
    public static class GlobalStore {

        private final Source source;
        private final Processor processor;

        public GlobalStore(final Source source, final Processor processor) {
            this.source = Objects.requireNonNull(source, "source must be non-null");
            this.processor = Objects.requireNonNull(processor, "processor must be non-null");
        }

        /**
         * The source node providing data to the global store.
         */
        public Source source() {
            return source;
        }

        /**
         * The processor node that maintains the global store.
         */
        public Processor processor() {
            return processor;
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
            return Objects.equals(source, that.source) && Objects.equals(processor, that.processor);
        }

        @Override
        public int hashCode() {
            return Objects.hash(source, processor);
        }

        @Override
        public String toString() {
            return "GlobalStore(source=" + source + ", processor=" + processor + ')';
        }
    }
}
