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

package org.apache.kafka.coordinator.group.api.streams;

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Full topology description of a Kafka Streams application, as exchanged between the
 * broker and {@link StreamsGroupTopologyDescriptionPlugin} implementations.
 *
 * <p>Mirrors {@link org.apache.kafka.streams.TopologyDescription} but is defined here so
 * plugin implementations need only depend on {@code group-coordinator-api}.
 */
@InterfaceStability.Evolving
public class StreamsGroupTopologyDescription {

    private final Collection<Subtopology> subtopologies;
    private final Collection<GlobalStore> globalStores;

    public StreamsGroupTopologyDescription(
        final Collection<Subtopology> subtopologies,
        final Collection<GlobalStore> globalStores
    ) {
        this.subtopologies = List.copyOf(Objects.requireNonNull(subtopologies));
        this.globalStores = List.copyOf(Objects.requireNonNull(globalStores));
    }

    public Collection<Subtopology> subtopologies() {
        return subtopologies;
    }

    public Collection<GlobalStore> globalStores() {
        return globalStores;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof StreamsGroupTopologyDescription)) return false;
        StreamsGroupTopologyDescription that = (StreamsGroupTopologyDescription) o;
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
     * A logical grouping of processing nodes that operate on a set of co-partitioned topics.
     */
    public static final class Subtopology {
        private final String id;
        private final Collection<Node> nodes;

        public Subtopology(final String id, final Collection<Node> nodes) {
            this.id = Objects.requireNonNull(id);
            this.nodes = List.copyOf(Objects.requireNonNull(nodes));
        }

        public String id() {
            return id;
        }

        public Collection<Node> nodes() {
            return nodes;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (!(o instanceof Subtopology)) return false;
            Subtopology that = (Subtopology) o;
            return Objects.equals(id, that.id) && Objects.equals(nodes, that.nodes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, nodes);
        }

        @Override
        public String toString() {
            return "Subtopology(id=" + id + ", nodes=" + nodes + ')';
        }
    }

    /**
     * A processing node in the topology. Implementations are {@link Source}, {@link Processor},
     * and {@link Sink}.
     *
     * <p>Note that this broker-side POJO exposes only successor edges and not predecessor
     * edges, even though predecessor edges are conceptually present in any topology
     * description. The broker does not reconstruct the predecessor relation, because the
     * primary purpose of this POJO is to ferry the description through the plugin layer:
     * predecessors are derivable from successors and any plugin that needs both can compute
     * them once. The matching admin-side POJO ({@code o.a.k.clients.admin.StreamsGroupTopologyDescription})
     * does reconstruct predecessors, since it is user-facing and traversal in both directions
     * is convenient for tooling.
     */
    public sealed interface Node permits Source, Processor, Sink {
        String name();

        /**
         * The direct successor nodes of this node in the processing graph (immediate
         * children only — transitive descendants are not included). Matches the
         * "{@code -->}" edges from {@code o.a.k.streams.TopologyDescription.Node}.
         */
        Set<String> successors();
    }

    /**
     * A source node — reads from one or more source topics.
     */
    public static final class Source implements Node {
        private final String name;
        private final Set<String> topics;
        private final Set<String> successors;

        public Source(
            final String name,
            final Set<String> topics,
            final Set<String> successors
        ) {
            this.name = Objects.requireNonNull(name);
            this.topics = Collections.unmodifiableSet(Objects.requireNonNull(topics));
            this.successors = Collections.unmodifiableSet(Objects.requireNonNull(successors));
        }

        @Override
        public String name() {
            return name;
        }

        public Set<String> topics() {
            return topics;
        }

        @Override
        public Set<String> successors() {
            return successors;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (!(o instanceof Source)) return false;
            Source that = (Source) o;
            return Objects.equals(name, that.name)
                && Objects.equals(topics, that.topics)
                && Objects.equals(successors, that.successors);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, topics, successors);
        }

        @Override
        public String toString() {
            return "Source(name=" + name + ", topics=" + topics + ')';
        }
    }

    /**
     * A processor node — applies user logic and may access state stores.
     */
    public static final class Processor implements Node {
        private final String name;
        private final Set<String> stores;
        private final Set<String> successors;

        public Processor(
            final String name,
            final Set<String> stores,
            final Set<String> successors
        ) {
            this.name = Objects.requireNonNull(name);
            this.stores = Collections.unmodifiableSet(Objects.requireNonNull(stores));
            this.successors = Collections.unmodifiableSet(Objects.requireNonNull(successors));
        }

        @Override
        public String name() {
            return name;
        }

        public Set<String> stores() {
            return stores;
        }

        @Override
        public Set<String> successors() {
            return successors;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (!(o instanceof Processor)) return false;
            Processor that = (Processor) o;
            return Objects.equals(name, that.name)
                && Objects.equals(stores, that.stores)
                && Objects.equals(successors, that.successors);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, stores, successors);
        }

        @Override
        public String toString() {
            return "Processor(name=" + name + ", stores=" + stores + ')';
        }
    }

    /**
     * A sink node — writes to a single topic.
     */
    public static final class Sink implements Node {
        private final String name;
        private final Optional<String> topic;
        private final Set<String> successors;

        public Sink(
            final String name,
            final Optional<String> topic,
            final Set<String> successors
        ) {
            this.name = Objects.requireNonNull(name);
            this.topic = Objects.requireNonNull(topic);
            this.successors = Collections.unmodifiableSet(Objects.requireNonNull(successors));
        }

        @Override
        public String name() {
            return name;
        }

        /**
         * The topic this sink writes to. Empty if the sink resolves its topic dynamically
         * (e.g. via a topic-name extractor).
         */
        public Optional<String> topic() {
            return topic;
        }

        @Override
        public Set<String> successors() {
            return successors;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (!(o instanceof Sink)) return false;
            Sink that = (Sink) o;
            return Objects.equals(name, that.name)
                && Objects.equals(topic, that.topic)
                && Objects.equals(successors, that.successors);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, topic, successors);
        }

        @Override
        public String toString() {
            return "Sink(name=" + name + ", topic=" + topic + ')';
        }
    }

    /**
     * A global state store with its source and processor nodes.
     */
    public static final class GlobalStore {
        private final Source source;
        private final Processor processor;

        public GlobalStore(final Source source, final Processor processor) {
            this.source = Objects.requireNonNull(source);
            this.processor = Objects.requireNonNull(processor);
        }

        public Source source() {
            return source;
        }

        public Processor processor() {
            return processor;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (!(o instanceof GlobalStore)) return false;
            GlobalStore that = (GlobalStore) o;
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
