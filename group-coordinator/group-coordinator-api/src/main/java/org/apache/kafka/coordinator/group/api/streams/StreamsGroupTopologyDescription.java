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

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Broker-side description of a Kafka Streams topology, as pushed by clients via
 * {@code StreamsGroupTopologyDescriptionUpdate} and consumed by
 * {@link StreamsGroupTopologyDescriptionPlugin} implementations.
 *
 * <p>This type mirrors {@code org.apache.kafka.streams.TopologyDescription} in shape
 * but lives in {@code group-coordinator-api} so plugin implementations do not need
 * to depend on {@code kafka-streams}. The wire schema only carries the successor
 * relation; plugins that need both directions reconstruct predecessors in a single
 * pass over the nodes.
 */
public class StreamsGroupTopologyDescription {

    /**
     * A processing node in the topology. Predecessor nodes can be inferred from
     * the {@link #successors()} relation.
     */
    public sealed interface Node {
        String name();
        Set<String> successors();
    }

    public static final class Source implements Node {
        private final String name;
        private final Set<String> topics;
        private final Set<String> successors;

        public Source(String name, Set<String> topics, Set<String> successors) {
            this.name = Objects.requireNonNull(name, "name");
            this.topics = Set.copyOf(Objects.requireNonNull(topics, "topics"));
            this.successors = Set.copyOf(Objects.requireNonNull(successors, "successors"));
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
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Source other)) return false;
            return name.equals(other.name)
                && topics.equals(other.topics)
                && successors.equals(other.successors);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, topics, successors);
        }

        @Override
        public String toString() {
            return "Source(name=" + name + ", topics=" + topics + ", successors=" + successors + ")";
        }
    }

    public static final class Processor implements Node {
        private final String name;
        private final Set<String> stores;
        private final Set<String> successors;

        public Processor(String name, Set<String> stores, Set<String> successors) {
            this.name = Objects.requireNonNull(name, "name");
            this.stores = Set.copyOf(Objects.requireNonNull(stores, "stores"));
            this.successors = Set.copyOf(Objects.requireNonNull(successors, "successors"));
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
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Processor other)) return false;
            return name.equals(other.name)
                && stores.equals(other.stores)
                && successors.equals(other.successors);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, stores, successors);
        }

        @Override
        public String toString() {
            return "Processor(name=" + name + ", stores=" + stores + ", successors=" + successors + ")";
        }
    }

    public static final class Sink implements Node {
        private final String name;
        private final Optional<String> topic;
        private final Set<String> successors;

        public Sink(String name, Optional<String> topic, Set<String> successors) {
            this.name = Objects.requireNonNull(name, "name");
            this.topic = Objects.requireNonNull(topic, "topic");
            this.successors = Set.copyOf(Objects.requireNonNull(successors, "successors"));
        }

        @Override
        public String name() {
            return name;
        }

        public Optional<String> topic() {
            return topic;
        }

        @Override
        public Set<String> successors() {
            return successors;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Sink other)) return false;
            return name.equals(other.name)
                && topic.equals(other.topic)
                && successors.equals(other.successors);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, topic, successors);
        }

        @Override
        public String toString() {
            return "Sink(name=" + name + ", topic=" + topic + ", successors=" + successors + ")";
        }
    }

    public static final class Subtopology {
        private final String id;
        private final Collection<Node> nodes;

        public Subtopology(String id, Collection<Node> nodes) {
            this.id = Objects.requireNonNull(id, "id");
            this.nodes = List.copyOf(Objects.requireNonNull(nodes, "nodes"));
        }

        public String id() {
            return id;
        }

        public Collection<Node> nodes() {
            return nodes;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Subtopology other)) return false;
            return id.equals(other.id) && nodes.equals(other.nodes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, nodes);
        }

        @Override
        public String toString() {
            return "Subtopology(id=" + id + ", nodes=" + nodes + ")";
        }
    }

    public static final class GlobalStore {
        private final Source source;
        private final Processor processor;

        public GlobalStore(Source source, Processor processor) {
            this.source = Objects.requireNonNull(source, "source");
            this.processor = Objects.requireNonNull(processor, "processor");
        }

        public Source source() {
            return source;
        }

        public Processor processor() {
            return processor;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof GlobalStore other)) return false;
            return source.equals(other.source) && processor.equals(other.processor);
        }

        @Override
        public int hashCode() {
            return Objects.hash(source, processor);
        }

        @Override
        public String toString() {
            return "GlobalStore(source=" + source + ", processor=" + processor + ")";
        }
    }

    private final Collection<Subtopology> subtopologies;
    private final Collection<GlobalStore> globalStores;

    public StreamsGroupTopologyDescription(Collection<Subtopology> subtopologies,
                                           Collection<GlobalStore> globalStores) {
        this.subtopologies = List.copyOf(Objects.requireNonNull(subtopologies, "subtopologies"));
        this.globalStores = List.copyOf(Objects.requireNonNull(globalStores, "globalStores"));
    }

    public Collection<Subtopology> subtopologies() {
        return subtopologies;
    }

    public Collection<GlobalStore> globalStores() {
        return globalStores;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StreamsGroupTopologyDescription other)) return false;
        return subtopologies.equals(other.subtopologies) && globalStores.equals(other.globalStores);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subtopologies, globalStores);
    }

    @Override
    public String toString() {
        return "StreamsGroupTopologyDescription(subtopologies=" + subtopologies
            + ", globalStores=" + globalStores + ")";
    }
}
