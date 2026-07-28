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

import org.apache.kafka.common.annotation.InterfaceAudience;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collection;
import java.util.Collections;
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
@InterfaceAudience.Public
@InterfaceStability.Evolving
public record StreamsGroupTopologyDescription(
    Collection<Subtopology> subtopologies,
    Collection<GlobalStore> globalStores
) {

    public StreamsGroupTopologyDescription {
        subtopologies = List.copyOf(Objects.requireNonNull(subtopologies, "subtopologies"));
        globalStores = List.copyOf(Objects.requireNonNull(globalStores, "globalStores"));
    }

    /**
     * A processing node in the topology. Predecessor nodes can be inferred from
     * the {@link #successors()} relation.
     */
    public sealed interface Node {
        String name();
        Set<String> successors();
    }

    public record Source(String name, Set<String> topics, Set<String> successors) implements Node {
        public Source {
            Objects.requireNonNull(name, "name");
            topics = Collections.unmodifiableSet(Objects.requireNonNull(topics, "topics"));
            successors = Collections.unmodifiableSet(Objects.requireNonNull(successors, "successors"));
        }
    }

    public record Processor(String name, Set<String> stores, Set<String> successors) implements Node {
        public Processor {
            Objects.requireNonNull(name, "name");
            stores = Collections.unmodifiableSet(Objects.requireNonNull(stores, "stores"));
            successors = Collections.unmodifiableSet(Objects.requireNonNull(successors, "successors"));
        }
    }

    public record Sink(String name, Optional<String> topic, Set<String> successors) implements Node {
        public Sink {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(topic, "topic");
            successors = Collections.unmodifiableSet(Objects.requireNonNull(successors, "successors"));
        }
    }

    public record Subtopology(String id, Collection<Node> nodes) {
        public Subtopology {
            Objects.requireNonNull(id, "id");
            nodes = List.copyOf(Objects.requireNonNull(nodes, "nodes"));
        }
    }

    public record GlobalStore(Source source, Processor processor) {
        public GlobalStore {
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(processor, "processor");
        }
    }
}
