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

import org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsResult;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability.Unstable;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupSubscribedToTopicException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.LagInfo;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.TaskMetadata;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.errors.UnknownStateStoreException;
import org.apache.kafka.streams.errors.UnknownTopologyException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.Task;
import org.apache.kafka.streams.processor.internals.TopologyMetadata;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * This is currently an internal and experimental feature for enabling certain kinds of topology upgrades. Use at
 * your own risk.
 *
 * Status: additive upgrades possible, removal of NamedTopologies not yet supported
 *
 * Note: some standard features of Kafka Streams are not yet supported with NamedTopologies. These include:
 *       - global state stores
 *       - TopologyTestDriver (TTD)
 */
@Unstable
public class KafkaStreamsNamedTopologyWrapper extends KafkaStreams {

    private final Logger log;

    /**
     * An empty Kafka Streams application that allows NamedTopologies to be added at a later point
     */
    public KafkaStreamsNamedTopologyWrapper(final Properties props) {
        this(new StreamsConfig(props), new DefaultKafkaClientSupplier());
    }

    public KafkaStreamsNamedTopologyWrapper(final Properties props, final KafkaClientSupplier clientSupplier) {
        this(new StreamsConfig(props), clientSupplier);
    }

    private KafkaStreamsNamedTopologyWrapper(final StreamsConfig config, final KafkaClientSupplier clientSupplier) {
        super(new TopologyMetadata(new ConcurrentSkipListMap<>(), config), config, clientSupplier);
        final LogContext logContext = new LogContext(String.format("stream-client [%s] ", clientId));
        this.log = logContext.logger(getClass());
    }

    /**
     * Start up Streams with a single initial NamedTopology
     */
    public void start(final NamedTopology initialTopology) {
        start(Collections.singleton(initialTopology));
    }

    /**
     * Start up Streams with a collection of initial NamedTopologies (may be empty)
     *
     * Note: this is synchronized to ensure that the application state cannot change while we add topologies
     */
    public synchronized void start(final Collection<NamedTopology> initialTopologies) {
        log.info("Starting Streams with topologies: {}", initialTopologies);
        for (final NamedTopology topology : initialTopologies) {
            final AddNamedTopologyResult addNamedTopologyResult = addNamedTopology(topology);
            if (addNamedTopologyResult.all().isCompletedExceptionally()) {
                final StreamsException e = addNamedTopologyResult.exceptionNow();
                log.error("Failed to start Streams when adding topology " + topology.name() + " due to", e);
                throw e;
            }
        }
        super.start();
    }

    /**
     * Provides a high-level DSL for specifying the processing logic of your application and building it into an
     * independent topology that can be executed by this {@link KafkaStreams}.
     *
     * @param topologyName              The name for this topology
     * @param topologyOverrides         The properties and any config overrides for this topology
     *
     * @throws IllegalArgumentException if the name contains the character sequence "__"
     */
    public NamedTopologyBuilder newNamedTopologyBuilder(final String topologyName, final Properties topologyOverrides) {
        if (topologyName.contains(TaskId.NAMED_TOPOLOGY_DELIMITER)) {
            throw new IllegalArgumentException("The character sequence '__' is not allowed in a NamedTopology, please select a new name");
        }
        return new NamedTopologyBuilder(topologyName, applicationConfigs, topologyOverrides);
    }

    /**
     * Provides a high-level DSL for specifying the processing logic of your application and building it into an
     * independent topology that can be executed by this {@link KafkaStreams}. This method will use the global
     * application {@link StreamsConfig} passed in to the constructor for all topology-level configs. To override
     * any of these for this specific Topology, use {@link #newNamedTopologyBuilder(String, Properties)}.
     * @param topologyName              The name for this topology
     *
     * @throws IllegalArgumentException if the name contains the character sequence "__"
     */
    public NamedTopologyBuilder newNamedTopologyBuilder(final String topologyName) {
        return newNamedTopologyBuilder(topologyName, new Properties());
    }

    /**
     * @return the NamedTopology for the specific name, or Optional.empty() if the application has no NamedTopology of that name
     */
    public synchronized Optional<NamedTopology> getTopologyByName(final String name) {
        return Optional.ofNullable(topologyMetadata.lookupBuilderForNamedTopology(name)).map(InternalTopologyBuilder::namedTopology);
    }

    public Collection<NamedTopology> getAllTopologies() {
        return topologyMetadata.getAllNamedTopologies();
    }

    /**
     * Add a new NamedTopology to a running Kafka Streams app. If multiple instances of the application are running,
     * you should inform all of them by calling {@code #addNamedTopology(NamedTopology)} on each client in order for
     * it to begin processing the new topology.
     * This method is not purely Async.
     *
     * May complete exceptionally (does not actually directly throw) with:
     *
     * @throws IllegalArgumentException if this topology name is already in use
     * @throws IllegalStateException    if streams has not been started or has already shut down
     * @throws TopologyException        if this topology subscribes to any input topics or pattern already in use
     */
    public AddNamedTopologyResult addNamedTopology(final NamedTopology newTopology) {
        log.info("Adding new NamedTopology: {}", newTopology.name());
        final KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();

        if (hasStartedOrFinishedShuttingDown()) {
            future.completeExceptionally(
                new IllegalStateException("Cannot add a NamedTopology while the state is " + super.state)
            );
        } else if (getTopologyByName(newTopology.name()).isPresent()) {
            future.completeExceptionally(
                new IllegalArgumentException("Unable to add the new NamedTopology " + newTopology.name() +
                                                   " as another of the same name already exists")
            );
        } else {
            topologyMetadata.registerAndBuildNewTopology(future, newTopology.internalTopologyBuilder());
            maybeCompleteFutureIfStillInCREATED(future, "adding topology " + newTopology.name());
        }

        return new AddNamedTopologyResult(future);
    }

    /**
     * Remove an existing NamedTopology from a running Kafka Streams app. If multiple instances of the application are
     * running, you should inform all of them by calling {@code #removeNamedTopology(String)} on each client to ensure
     * it stops processing the old topology.
     * This method is not purely Async.
     *
     * @param topologyToRemove          name of the topology to be removed
     * @param resetOffsets              whether to reset the committed offsets for any source topics
     *
     * May complete exceptionally (does not actually directly throw) with:
     *
     * @throws IllegalArgumentException if this topology name cannot be found
     * @throws IllegalStateException    if streams has not been started or has already shut down
     */
    public RemoveNamedTopologyResult removeNamedTopology(final String topologyToRemove, final boolean resetOffsets) {
        log.info("Informed to remove topology {} with resetOffsets={} ", topologyToRemove, resetOffsets);

        final KafkaFutureImpl<Void> removeTopologyFuture = new KafkaFutureImpl<>();

        if (hasStartedOrFinishedShuttingDown()) {
            log.error("Attempted to remove topology {} from while the Kafka Streams was in state {}, "
                          + "topologies cannot be modified if the application has begun or completed shutting down.",
                      topologyToRemove, state
            );
            removeTopologyFuture.completeExceptionally(
                new IllegalStateException("Cannot remove a NamedTopology while the state is " + super.state)
            );
        } else if (!getTopologyByName(topologyToRemove).isPresent()) {
            log.error("Attempted to remove unknown topology {}. This application currently contains the"
                          + "following topologies: {}.", topologyToRemove, topologyMetadata.namedTopologiesView()
            );
            removeTopologyFuture.completeExceptionally(
                new UnknownTopologyException("Unable to remove topology", topologyToRemove)
            );
        }

        final Set<TopicPartition> partitionsToReset = metadataForLocalThreads()
            .stream()
            .flatMap(t -> {
                final Set<TaskMetadata> tasks = new HashSet<>(t.activeTasks());
                return tasks.stream();
            })
            .flatMap(t -> t.topicPartitions().stream())
            .filter(t -> topologyMetadata.sourceTopicsForTopology(topologyToRemove).contains(t.topic()))
            .collect(Collectors.toSet());

        topologyMetadata.unregisterTopology(removeTopologyFuture, topologyToRemove);

        final boolean skipResetForUnstartedApplication =
            maybeCompleteFutureIfStillInCREATED(removeTopologyFuture, "removing topology " + topologyToRemove);

        if (resetOffsets && !skipResetForUnstartedApplication && !partitionsToReset.isEmpty()) {
            log.info("Resetting offsets for the following partitions of {} removed NamedTopology {}: {}",
                     removeTopologyFuture.isCompletedExceptionally() ? "unsuccessfully" : "successfully",
                     topologyToRemove, partitionsToReset
            );
            return new RemoveNamedTopologyResult(
                removeTopologyFuture,
                topologyToRemove,
                () -> resetOffsets(partitionsToReset)
            );
        } else {
            return new RemoveNamedTopologyResult(removeTopologyFuture);
        }
    }

    /**
     * @return  true iff the application is still in CREATED and the future was completed
     */
    private boolean maybeCompleteFutureIfStillInCREATED(final KafkaFutureImpl<Void> updateTopologyFuture,
                                                        final String operation) {
        if (state == State.CREATED && !updateTopologyFuture.isDone()) {
            updateTopologyFuture.complete(null);
            log.info("Completed {} since application has not been started", operation);
            return true;
        } else {
            return false;
        }
    }

    private void resetOffsets(final Set<TopicPartition> partitionsToReset) throws StreamsException {
        // The number of times to retry upon failure
        int retries = 100;
        while (true) {
            try {
                final DeleteConsumerGroupOffsetsResult deleteOffsetsResult = adminClient.deleteConsumerGroupOffsets(
                    applicationConfigs.getString(StreamsConfig.APPLICATION_ID_CONFIG),
                    partitionsToReset);
                deleteOffsetsResult.all().get();
                log.info("Successfully completed resetting offsets.");
                break;
            } catch (final InterruptedException ex) {
                ex.printStackTrace();
                log.error("Offset reset failed.", ex);
                throw new StreamsException(ex);
            } catch (final ExecutionException ex) {
                final Throwable error = ex.getCause() != null ? ex.getCause() : ex;

                if (error instanceof GroupSubscribedToTopicException &&
                    error.getMessage()
                        .equals("Deleting offsets of a topic is forbidden while the consumer group is actively subscribed to it.")) {
                    log.debug("Offset reset failed, there may be other nodes which have not yet finished removing this topology", error);
                } else if (error instanceof GroupIdNotFoundException) {
                    log.info("The offsets have been reset by another client or the group has been deleted, no need to retry further.");
                    break;
                } else {
                    if (--retries > 0) {
                        log.error("Offset reset failed, retries remaining: " + retries, error);
                    } else {
                        log.error("Offset reset failed, no retries remaining.", error);
                        throw new StreamsException(error);
                    }
                }
            }
            try {
                Thread.sleep(100);
            } catch (final InterruptedException ex) {
                ex.printStackTrace();
            }
        }
    }

    /**
     * Remove an existing NamedTopology from a running Kafka Streams app. If multiple instances of the application are
     * running, you should inform all of them by calling {@code #removeNamedTopology(String)} on each client to ensure
     * it stops processing the old topology.
     * This method is not purely Async.
     *
     * @param topologyToRemove          name of the topology to be removed
     *
     * @throws IllegalArgumentException if this topology name cannot be found
     * @throws IllegalStateException    if streams has not been started or has already shut down
     * @throws TopologyException        if this topology subscribes to any input topics or pattern already in use
     */
    public RemoveNamedTopologyResult removeNamedTopology(final String topologyToRemove) {
        return removeNamedTopology(topologyToRemove, false);
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

    private void verifyTopologyStateStore(final String topologyName, final String storeName) {
        final InternalTopologyBuilder builder = topologyMetadata.lookupBuilderForNamedTopology(topologyName);
        if (builder == null) {
            throw new UnknownTopologyException("Cannot get state store " + storeName, topologyName);
        } else if (!builder.hasStore(storeName)) {
            throw new UnknownStateStoreException(
                "Cannot get state store " + storeName + " from NamedTopology " + topologyName +
                    " because no such state store exists in this topology."
            );
        }
    }

    /**
     * See {@link KafkaStreams#store(StoreQueryParameters)}
     */
    public <T> T store(final NamedTopologyStoreQueryParameters<T> storeQueryParameters) {
        final String topologyName = storeQueryParameters.topologyName;
        final String storeName = storeQueryParameters.storeName();
        verifyTopologyStateStore(topologyName, storeName);
        return super.store(storeQueryParameters);
    }

    /**
     * See {@link KafkaStreams#streamsMetadataForStore(String)}
     */
    public Collection<StreamsMetadata> streamsMetadataForStore(final String storeName, final String topologyName) {
        verifyTopologyStateStore(topologyName, storeName);
        validateIsRunningOrRebalancing();
        return streamsMetadataState.getAllMetadataForStore(storeName, topologyName);
    }

    /**
     * See {@link KafkaStreams#metadataForAllStreamsClients()}
     */
    public Collection<StreamsMetadata> allStreamsClientsMetadataForTopology(final String topologyName) {
        validateIsRunningOrRebalancing();
        return streamsMetadataState.getAllMetadataForTopology(topologyName);
    }

    /**
     * See {@link KafkaStreams#queryMetadataForKey(String, Object, Serializer)}
     */
    public <K> KeyQueryMetadata queryMetadataForKey(final String storeName,
                                                    final K key,
                                                    final Serializer<K> keySerializer,
                                                    final String topologyName) {
        verifyTopologyStateStore(topologyName, storeName);
        validateIsRunningOrRebalancing();
        return streamsMetadataState.getKeyQueryMetadataForKey(storeName, key, keySerializer, topologyName);
    }

    /**
     * See {@link KafkaStreams#allLocalStorePartitionLags()}
     */
    public Map<String, Map<Integer, LagInfo>> allLocalStorePartitionLagsForTopology(final String topologyName) {
        if (!getTopologyByName(topologyName).isPresent()) {
            log.error("Can't get local store partition lags since topology {} does not exist in this application",
                      topologyName);
            throw new UnknownTopologyException("Can't get local store partition lags", topologyName);
        }
        final List<Task> allTopologyTasks = new ArrayList<>();
        processStreamThread(thread -> allTopologyTasks.addAll(
            thread.allTasks().values().stream()
                .filter(t -> topologyName.equals(t.id().topologyName()))
                .collect(Collectors.toList())));
        return allLocalStorePartitionLags(allTopologyTasks);
    }
}
