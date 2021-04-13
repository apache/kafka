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

import org.apache.kafka.common.annotation.InterfaceStability.Evolving;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.internals.TopologyMetadata;

import java.util.HashMap;
import java.util.List;
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
 *       - interactive queries (IQ)
 */
@Evolving
public class KafkaStreamsNamedTopologyWrapper extends KafkaStreams {

    final Map<String, NamedTopology> nameToTopology = new HashMap<>();

    public KafkaStreamsNamedTopologyWrapper(final NamedTopology topology, final Properties props, final KafkaClientSupplier clientSupplier) {
        super(new TopologyMetadata(topology.internalTopologyBuilder()), new StreamsConfig(props), clientSupplier);
        nameToTopology.put(topology.name(), topology);
    }

    public KafkaStreamsNamedTopologyWrapper(final Properties props, final KafkaClientSupplier clientSupplier) {
        super(new TopologyMetadata(), new StreamsConfig(props), clientSupplier);
    }

    public KafkaStreamsNamedTopologyWrapper(final List<NamedTopology> topologies, final Properties props, final KafkaClientSupplier clientSupplier) {
        super(
            new TopologyMetadata(topologies.stream().collect(Collectors.toMap(
                NamedTopology::name,
                NamedTopology::internalTopologyBuilder,
                (v1, v2) -> {
                    throw new IllegalArgumentException("Topology names must be unique");
                },
                () -> new TreeMap<>()))),
            new StreamsConfig(props),
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
        return topologyMetadata.topologyDescription();
    }
}
