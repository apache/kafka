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
package org.apache.kafka.streams.processor.internals.namedtopology;

import org.apache.kafka.common.annotation.InterfaceStability.Unstable;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.apache.kafka.streams.processor.internals.TopologyMetadata;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * This is currently an internal and experimental feature for enabling certain kinds of topology upgrades. Use at
 * your own risk.
 *
 * Status: basic architecture implemented but no actual upgrades are supported yet
 *
 * Note: some standard features of Kafka Streams are not yet supported with NamedTopologies. These include:
 *       - global state stores
 *       - interactive queries (IQ) -- partially working, requires all stores to be given a unique name
 *       - TopologyTestDriver (TTD)
 */
@Unstable
public class KafkaStreamsNamedTopologyWrapper extends KafkaStreams {

    final Map<String, NamedTopology> nameToTopology = new HashMap<>();

    /**
     * A Kafka Streams application with a single initial NamedTopology
     */
    public KafkaStreamsNamedTopologyWrapper(final NamedTopology topology, final Properties props) {
        this(Collections.singleton(topology), new StreamsConfig(props), new DefaultKafkaClientSupplier());
    }

    /**
     * A Kafka Streams application with a single initial NamedTopology
     */
    public KafkaStreamsNamedTopologyWrapper(final NamedTopology topology, final Properties props, final KafkaClientSupplier clientSupplier) {
        this(Collections.singleton(topology), new StreamsConfig(props), clientSupplier);
    }

    /**
     * An empty Kafka Streams application that allows NamedTopologies to be added at a later point
     */
    public KafkaStreamsNamedTopologyWrapper(final Properties props) {
        this(Collections.emptyList(), new StreamsConfig(props), new DefaultKafkaClientSupplier());
    }

    /**
     * An empty Kafka Streams application that allows NamedTopologies to be added at a later point
     */
    public KafkaStreamsNamedTopologyWrapper(final Properties props, final KafkaClientSupplier clientSupplier) {
        this(Collections.emptyList(), new StreamsConfig(props), clientSupplier);
    }

    /**
     * A Kafka Streams application with a multiple initial NamedTopologies
     *
     * @throws IllegalArgumentException if any of the named topologies have the same name
     * @throws TopologyException        if multiple NamedTopologies subscribe to the same input topics or pattern
     */
    public KafkaStreamsNamedTopologyWrapper(final Collection<NamedTopology> topologies, final Properties props) {
        this(topologies, new StreamsConfig(props), new DefaultKafkaClientSupplier());
    }

    /**
     * A Kafka Streams application with a multiple initial NamedTopologies
     *
     * @throws IllegalArgumentException if any of the named topologies have the same name
     * @throws TopologyException        if multiple NamedTopologies subscribe to the same input topics or pattern
     */
    public KafkaStreamsNamedTopologyWrapper(final Collection<NamedTopology> topologies, final Properties props, final KafkaClientSupplier clientSupplier) {
        this(topologies, new StreamsConfig(props), clientSupplier);
    }

    private KafkaStreamsNamedTopologyWrapper(final Collection<NamedTopology> topologies, final StreamsConfig config, final KafkaClientSupplier clientSupplier) {
        super(
            new TopologyMetadata(
                topologies.stream().collect(Collectors.toMap(
                    NamedTopology::name,
                    NamedTopology::internalTopologyBuilder,
                    (v1, v2) -> {
                        throw new IllegalArgumentException("Topology names must be unique");
                    },
                    () -> new TreeMap<>())),
                config),
            config,
            clientSupplier
        );
        for (final NamedTopology topology : topologies) {
            nameToTopology.put(topology.name(), topology);
        }
    }

    public NamedTopology getTopologyByName(final String name) {
        if (nameToTopology.containsKey(name)) {
            return nameToTopology.get(name);
        } else {
            throw new IllegalArgumentException("Unable to locate a NamedTopology called " + name);
        }
    }

    public void addNamedTopology(final NamedTopology topology) {
        nameToTopology.put(topology.name(), topology);
        throw new UnsupportedOperationException();
    }

    public void removeNamedTopology(final String namedTopology) {
        throw new UnsupportedOperationException();
    }

    public String getFullTopologyDescription() {
        return topologyMetadata.topologyDescriptionString();
    }
}
