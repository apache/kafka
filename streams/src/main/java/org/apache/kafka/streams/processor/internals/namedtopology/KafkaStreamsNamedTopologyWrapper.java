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
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.TopologyMetadata;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

/**
 * This is currently an internal and experimental feature for enabling certain kinds of topology upgrades. Use at
 * your own risk.
 *
 * Status: additive upgrades possible, removal of NamedTopologies not yet supported
 *
 * Note: some standard features of Kafka Streams are not yet supported with NamedTopologies. These include:
 *       - global state stores
 *       - interactive queries (IQ)
 *       - TopologyTestDriver (TTD)
 */
@Unstable
public class KafkaStreamsNamedTopologyWrapper extends KafkaStreams {

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
                    () -> new ConcurrentSkipListMap<>())),
                config),
            config,
            clientSupplier
        );
    }

    /**
     * @return the NamedTopology for the specific name, or Optional.empty() if the application has no NamedTopology of that name
     */
    public Optional<NamedTopology> getTopologyByName(final String name) {
        return Optional.ofNullable(topologyMetadata.lookupBuilderForNamedTopology(name)).map(InternalTopologyBuilder::namedTopology);
    }

    /**
     * Add a new NamedTopology to a running Kafka Streams app. If multiple instances of the application are running,
     * you should inform all of them by calling {@link #addNamedTopology(NamedTopology)} on each client in order for
     * it to begin processing the new topology.
     *
     * @throws IllegalArgumentException if this topology name is already in use
     * @throws IllegalStateException    if streams has not been started or has already shut down
     * @throws TopologyException        if this topology subscribes to any input topics or pattern already in use
     */
    public void addNamedTopology(final NamedTopology newTopology) {
        if (hasStartedOrFinishedShuttingDown()) {
            throw new IllegalStateException("Cannot add a NamedTopology while the state is " + super.state);
        } else if (getTopologyByName(newTopology.name()).isPresent()) {
            throw new IllegalArgumentException("Unable to add the new NamedTopology " + newTopology.name() +
                                                   " as another of the same name already exists");
        }
        topologyMetadata.registerAndBuildNewTopology(newTopology.internalTopologyBuilder());
    }

    /**
     * Remove an existing NamedTopology from a running Kafka Streams app. If multiple instances of the application are
     * running, you should inform all of them by calling {@link #removeNamedTopology(String)} on each client to ensure
     * it stops processing the old topology.
     *
     * @throws IllegalArgumentException if this topology name cannot be found
     * @throws IllegalStateException    if streams has not been started or has already shut down
     * @throws TopologyException        if this topology subscribes to any input topics or pattern already in use
     */
    public void removeNamedTopology(final String topologyToRemove) {
        if (!isRunningOrRebalancing()) {
            throw new IllegalStateException("Cannot remove a NamedTopology while the state is " + super.state);
        } else if (!getTopologyByName(topologyToRemove).isPresent()) {
            throw new IllegalArgumentException("Unable to locate for removal a NamedTopology called " + topologyToRemove);
        }

        topologyMetadata.unregisterTopology(topologyToRemove);
    }

    /**
     * Do a clean up of the local state directory for this NamedTopology by deleting all data with regard to the
     * @link StreamsConfig#APPLICATION_ID_CONFIG application ID} in the ({@link StreamsConfig#STATE_DIR_CONFIG})
     * <p>
     * May be called while the Streams is in any state, but only on a {@link NamedTopology} that has already been
     * removed via {@link #removeNamedTopology(String)}.
     * <p>
     * Calling this method triggers a restore of local {@link StateStore}s for this {@link NamedTopology} if it is
     * ever re-added via {@link #addNamedTopology(NamedTopology)}.
     *
     * @throws IllegalStateException if this {@code NamedTopology} hasn't been removed
     * @throws StreamsException if cleanup failed
     */
    public void cleanUpNamedTopology(final String name) {
        if (getTopologyByName(name).isPresent()) {
            throw new IllegalStateException("Can't clean up local state for an active NamedTopology: " + name);
        }
        stateDirectory.clearLocalStateForNamedTopology(name);
    }

    public String getFullTopologyDescription() {
        return topologyMetadata.topologyDescriptionString();
    }
}
