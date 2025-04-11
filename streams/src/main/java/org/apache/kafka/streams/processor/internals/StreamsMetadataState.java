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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.internals.StreamsMetadataImpl;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.clients.producer.RecordMetadata.UNKNOWN_PARTITION;

/**
 * Provides access to the {@link StreamsMetadata} in a KafkaStreams application. This can be used
 * to discover the locations of {@link org.apache.kafka.streams.processor.StateStore}s
 * in a KafkaStreams application
 */
public class StreamsMetadataState {
    private final Logger log;
    public static final HostInfo UNKNOWN_HOST = HostInfo.unavailable();
    private final TopologyMetadata topologyMetadata;
    private final Set<String> globalStores;
    private final HostInfo thisHost;
    private List<StreamsMetadata> allMetadata = Collections.emptyList();
    private Map<String, List<PartitionInfo>> partitionsByTopic;
    private final AtomicReference<StreamsMetadata> localMetadata = new AtomicReference<>(null);

    public StreamsMetadataState(final TopologyMetadata topologyMetadata,
                                final HostInfo thisHost,
                                final LogContext logContext) {
        this.topologyMetadata = topologyMetadata;
        this.globalStores = this.topologyMetadata.globalStateStores().keySet();
        this.thisHost = thisHost;
        this.log = logContext.logger(getClass());
    }

    @Override
    public String toString() {
        return toString("");
    }

    public String toString(final String indent) {
        return indent + "GlobalMetadata: " + allMetadata + "\n" +
                indent + "GlobalStores: " + globalStores + "\n" +
                indent + "My HostInfo: " + thisHost + "\n" +
                indent + "PartitionsByTopic: " + partitionsByTopic + "\n";
    }

    /**
     * Get the {@link StreamsMetadata}s for the local instance in a {@link KafkaStreams application}
     *
     * @return the {@link StreamsMetadata}s for the local instance in a {@link KafkaStreams} application
     */
    public StreamsMetadata getLocalMetadata() {
        return localMetadata.get();
    }

    /**
     * Find all of the {@link StreamsMetadata}s in a
     * {@link KafkaStreams application}
     *
     * @return all the {@link StreamsMetadata}s in a {@link KafkaStreams} application
     */
    public Collection<StreamsMetadata> allMetadata() {
        return Collections.unmodifiableList(allMetadata);
    }

    /**
     * Find all of the {@link StreamsMetadata}s for a given storeName
     *
     * @param storeName the storeName to find metadata for
     * @return A collection of {@link StreamsMetadata} that have the provided storeName
     */
    public synchronized Collection<StreamsMetadata> allMetadataForStore(final String storeName) {
        Objects.requireNonNull(storeName, "storeName cannot be null");
        if (topologyMetadata.hasNamedTopologies()) {
            throw new IllegalArgumentException("Cannot invoke the allMetadataForStore(storeName) method when"
                                                   + "using named topologies, please use the overload that accepts"
                                                   + "a topologyName parameter to identify the correct store");
        }

        if (!isInitialized()) {
            return Collections.emptyList();
        }

        if (globalStores.contains(storeName)) {
            return allMetadata;
        }

        final Collection<String> sourceTopics = topologyMetadata.sourceTopicsForStore(storeName, null);
        if (sourceTopics.isEmpty()) {
            return Collections.emptyList();
        }

        final ArrayList<StreamsMetadata> results = new ArrayList<>();
        for (final StreamsMetadata metadata : allMetadata) {
            if (metadata.stateStoreNames().contains(storeName) || metadata.standbyStateStoreNames().contains(storeName)) {
                results.add(metadata);
            }
        }
        return results;
    }

    /**
     * Find all of the {@link StreamsMetadata}s for a given storeName in the given topology
     *
     * @param storeName the storeName to find metadata for
     * @param topologyName the storeName to find metadata for
     * @return A collection of {@link StreamsMetadata} that have the provided storeName
     */
    public synchronized Collection<StreamsMetadata> allMetadataForStore(final String storeName, final String topologyName) {
        Objects.requireNonNull(storeName, "storeName cannot be null");
        Objects.requireNonNull(topologyName, "topologyName cannot be null");

        if (!isInitialized()) {
            return Collections.emptyList();
        }

        final Collection<String> sourceTopics = topologyMetadata.sourceTopicsForStore(storeName, topologyName);
        if (sourceTopics.isEmpty()) {
            return Collections.emptyList();
        }

        final ArrayList<StreamsMetadata> results = new ArrayList<>();
        for (final StreamsMetadata metadata : allMetadata) {
            final String metadataTopologyName = ((StreamsMetadataImpl) metadata).topologyName();
            if (metadataTopologyName != null && metadataTopologyName.equals(topologyName)
                && metadata.stateStoreNames().contains(storeName) || metadata.standbyStateStoreNames().contains(storeName)) {
                results.add(metadata);
            }
        }
        return results;
    }

    public synchronized Collection<StreamsMetadata> allMetadataForTopology(final String topologyName) {
        Objects.requireNonNull(topologyName, "topologyName cannot be null");

        if (!isInitialized()) {
            return Collections.emptyList();
        }

        final ArrayList<StreamsMetadata> results = new ArrayList<>();
        for (final StreamsMetadata metadata : allMetadata) {
            final String metadataTopologyName = ((StreamsMetadataImpl) metadata).topologyName();
            if (metadataTopologyName != null && metadataTopologyName.equals(topologyName)) {
                results.add(metadata);
            }
        }
        return results;
    }

    /**
     * Find the {@link KeyQueryMetadata}s for a given storeName and key. This method will use the
     * {@link DefaultStreamPartitioner} to locate the store. If a custom partitioner has been used
     * please use {@link StreamsMetadataState#keyQueryMetadataForKey(String, Object, StreamPartitioner)} instead.
     *
     * Note: the key may not exist in the {@link org.apache.kafka.streams.processor.StateStore},
     * this method provides a way of finding which {@link KeyQueryMetadata} it would exist on.
     *
     * @param storeName     Name of the store
     * @param key           Key to use
     * @param keySerializer Serializer for the key
     * @param <K>           key type
     * @return The {@link KeyQueryMetadata} for the storeName and key or {@link KeyQueryMetadata#NOT_AVAILABLE}
     * if streams is (re-)initializing or {@code null} if the corresponding topic cannot be found,
     * or null if no matching metadata could be found.
     */
    public synchronized <K> KeyQueryMetadata keyQueryMetadataForKey(final String storeName,
                                                                    final K key,
                                                                    final Serializer<K> keySerializer) {
        Objects.requireNonNull(keySerializer, "keySerializer can't be null");
        if (topologyMetadata.hasNamedTopologies()) {
            throw new IllegalArgumentException("Cannot invoke the KeyQueryMetadataForKey(storeName, key, keySerializer)"
                                                   + "method when using named topologies, please use the overload that"
                                                   + "accepts a topologyName parameter to identify the correct store");
        }
        return keyQueryMetadataForKey(storeName,
                                      key,
                                      new DefaultStreamPartitioner<>(keySerializer));
    }

    /**
     * See {@link StreamsMetadataState#keyQueryMetadataForKey(String, Object, Serializer)}
     */
    public synchronized <K> KeyQueryMetadata keyQueryMetadataForKey(final String storeName,
                                                                    final K key,
                                                                    final Serializer<K> keySerializer,
                                                                    final String topologyName) {
        Objects.requireNonNull(keySerializer, "keySerializer can't be null");
        return keyQueryMetadataForKey(storeName,
                                      key,
                                      new DefaultStreamPartitioner<>(keySerializer),
                                      topologyName);
    }

    /**
     * Find the {@link KeyQueryMetadata}s for a given storeName and key
     *
     * Note: the key may not exist in the {@link StateStore},this method provides a way of finding which
     * {@link StreamsMetadata} it would exist on.
     *
     * @param storeName   Name of the store
     * @param key         Key to use
     * @param partitioner partitioner to use to find correct partition for key
     * @param <K>         key type
     * @return The {@link KeyQueryMetadata} for the storeName and key or {@link KeyQueryMetadata#NOT_AVAILABLE}
     * if streams is (re-)initializing, or {@code null} if no matching metadata could be found.
     */
    public synchronized <K> KeyQueryMetadata keyQueryMetadataForKey(final String storeName,
                                                                    final K key,
                                                                    final StreamPartitioner<? super K, ?> partitioner) {
        Objects.requireNonNull(storeName, "storeName can't be null");
        Objects.requireNonNull(key, "key can't be null");
        Objects.requireNonNull(partitioner, "partitioner can't be null");
        if (topologyMetadata.hasNamedTopologies()) {
            throw new IllegalArgumentException("Cannot invoke the keyQueryMetadataForKey(storeName, key, partitioner)"
                                                   + "method when using named topologies, please use the overload that"
                                                   + "accepts a topologyName parameter to identify the correct store");
        }

        if (!isInitialized()) {
            return KeyQueryMetadata.NOT_AVAILABLE;
        }

        if (globalStores.contains(storeName)) {
            // global stores are on every node. if we don't have the host info
            // for this host then just pick the first metadata
            if (thisHost.equals(UNKNOWN_HOST)) {
                return new KeyQueryMetadata(allMetadata.get(0).hostInfo(), Collections.emptySet(), UNKNOWN_PARTITION);
            }
            return new KeyQueryMetadata(localMetadata.get().hostInfo(), Collections.emptySet(), UNKNOWN_PARTITION);
        }

        final SourceTopicsInfo sourceTopicsInfo = getSourceTopicsInfo(storeName);
        if (sourceTopicsInfo == null) {
            return null;
        }
        return keyQueryMetadataForKey(storeName, key, partitioner, sourceTopicsInfo);
    }

    /**
     * See {@link StreamsMetadataState#keyQueryMetadataForKey(String, Object, StreamPartitioner)}
     */
    public synchronized <K> KeyQueryMetadata keyQueryMetadataForKey(final String storeName,
                                                                    final K key,
                                                                    final StreamPartitioner<? super K, ?> partitioner,
                                                                    final String topologyName) {
        Objects.requireNonNull(storeName, "storeName can't be null");
        Objects.requireNonNull(key, "key can't be null");
        Objects.requireNonNull(partitioner, "partitioner can't be null");
        Objects.requireNonNull(topologyName, "topologyName can't be null");


        if (!isInitialized()) {
            return KeyQueryMetadata.NOT_AVAILABLE;
        }

        final SourceTopicsInfo sourceTopicsInfo = getSourceTopicsInfo(storeName, topologyName);
        if (sourceTopicsInfo == null) {
            return null;
        }
        return keyQueryMetadataForKey(storeName, key, partitioner, sourceTopicsInfo, topologyName);
    }

    /**
     * Respond to changes to the HostInfo -> TopicPartition mapping. Will rebuild the
     * metadata
     *
     * @param activePartitionHostMap  the current mapping of {@link HostInfo} -> {@link TopicPartition}s for active partitions
     * @param standbyPartitionHostMap the current mapping of {@link HostInfo} -> {@link TopicPartition}s for standby partitions
     * @param topicPartitionInfo      the current mapping of {@link TopicPartition} -> {@Link PartitionInfo}
     */
    synchronized void onChange(final Map<HostInfo, Set<TopicPartition>> activePartitionHostMap,
                               final Map<HostInfo, Set<TopicPartition>> standbyPartitionHostMap,
                               final Map<TopicPartition, PartitionInfo> topicPartitionInfo) {
        this.partitionsByTopic = new HashMap<>();
        topicPartitionInfo.forEach((key, value) -> this.partitionsByTopic
                .computeIfAbsent(key.topic(), topic -> new ArrayList<>())
                .add(value));

        rebuildMetadata(activePartitionHostMap, standbyPartitionHostMap);
    }

    private boolean hasPartitionsForAnyTopics(final List<String> topicNames, final Set<TopicPartition> partitionForHost) {
        for (final TopicPartition topicPartition : partitionForHost) {
            if (topicNames.contains(topicPartition.topic())) {
                return true;
            }
        }
        return false;
    }

    private Set<String> getStoresOnHost(final Map<String, List<String>> storeToSourceTopics,
                                        final Set<TopicPartition> sourceTopicPartitions) {
        final Set<String> storesOnHost = new HashSet<>();
        for (final Map.Entry<String, List<String>> storeTopicEntry : storeToSourceTopics.entrySet()) {
            final List<String> topicsForStore = storeTopicEntry.getValue();
            if (hasPartitionsForAnyTopics(topicsForStore, sourceTopicPartitions)) {
                storesOnHost.add(storeTopicEntry.getKey());
            }
        }
        return storesOnHost;
    }

    private void rebuildMetadata(final Map<HostInfo, Set<TopicPartition>> activePartitionHostMap,
                                 final Map<HostInfo, Set<TopicPartition>> standbyPartitionHostMap) {
        if (activePartitionHostMap.isEmpty() && standbyPartitionHostMap.isEmpty()) {
            allMetadata = Collections.emptyList();
            localMetadata.set(new StreamsMetadataImpl(
                thisHost,
                Collections.emptySet(),
                Collections.emptySet(),
                Collections.emptySet(),
                Collections.emptySet()
            ));
            return;
        }

        allMetadata = topologyMetadata.hasNamedTopologies() ?
            rebuildMetadataForNamedTopologies(activePartitionHostMap, standbyPartitionHostMap) :
            rebuildMetadataForSingleTopology(activePartitionHostMap, standbyPartitionHostMap);
    }

    private List<StreamsMetadata> rebuildMetadataForNamedTopologies(final Map<HostInfo, Set<TopicPartition>> activePartitionHostMap,
                                                                    final Map<HostInfo, Set<TopicPartition>> standbyPartitionHostMap) {
        final List<StreamsMetadata> rebuiltMetadata = new ArrayList<>();
        Stream.concat(activePartitionHostMap.keySet().stream(), standbyPartitionHostMap.keySet().stream())
            .distinct()
            .sorted(Comparator.comparing(HostInfo::host).thenComparingInt(HostInfo::port))
            .forEach(hostInfo -> {
                for (final String topologyName : topologyMetadata.namedTopologiesView()) {
                    final Map<String, List<String>> storeToSourceTopics =
                        topologyMetadata.stateStoreNameToSourceTopicsForTopology(topologyName);

                    final Set<TopicPartition> activePartitionsOnHost = new HashSet<>();
                    final Set<String> activeStoresOnHost = new HashSet<>();
                    if (activePartitionHostMap.containsKey(hostInfo)) {
                        // filter out partitions for topics that are not connected to this topology
                        activePartitionsOnHost.addAll(
                            activePartitionHostMap.get(hostInfo).stream()
                                .filter(tp -> topologyMetadata.fullSourceTopicNamesForTopology(topologyName).contains(tp.topic()))
                                .collect(Collectors.toSet())
                        );
                        activeStoresOnHost.addAll(getStoresOnHost(storeToSourceTopics, activePartitionsOnHost));
                    }

                    final Set<TopicPartition> standbyPartitionsOnHost = new HashSet<>();
                    final Set<String> standbyStoresOnHost = new HashSet<>();
                    if (standbyPartitionHostMap.containsKey(hostInfo)) {
                        standbyPartitionsOnHost.addAll(
                            standbyPartitionHostMap.get(hostInfo).stream()
                                .filter(tp -> topologyMetadata.fullSourceTopicNamesForTopology(topologyName).contains(tp.topic()))
                                .collect(Collectors.toSet()));
                        standbyStoresOnHost.addAll(getStoresOnHost(storeToSourceTopics, standbyPartitionsOnHost));
                    }

                    final StreamsMetadata metadata = new StreamsMetadataImpl(
                        hostInfo,
                        activeStoresOnHost,
                        activePartitionsOnHost,
                        standbyStoresOnHost,
                        standbyPartitionsOnHost,
                        topologyName
                    );

                    rebuiltMetadata.add(metadata);
                    if (hostInfo.equals(thisHost)) {
                        localMetadata.set(metadata);
                    }
                }

                // Construct metadata across all topologies on this host for the `localMetadata` field
                final Map<String, List<String>> storeToSourceTopics = topologyMetadata.stateStoreNameToSourceTopics();
                final Set<TopicPartition> localActivePartitions = activePartitionHostMap.get(thisHost);
                final Set<TopicPartition> localStandbyPartitions = standbyPartitionHostMap.get(thisHost);

                localMetadata.set(
                    new StreamsMetadataImpl(thisHost,
                                            getStoresOnHost(storeToSourceTopics, localActivePartitions),
                                            localActivePartitions,
                                            getStoresOnHost(storeToSourceTopics, localStandbyPartitions),
                                            localStandbyPartitions)
                );
            });
        return rebuiltMetadata;
    }

    private List<StreamsMetadata> rebuildMetadataForSingleTopology(final Map<HostInfo, Set<TopicPartition>> activePartitionHostMap,
                                                                   final Map<HostInfo, Set<TopicPartition>> standbyPartitionHostMap) {
        final List<StreamsMetadata> rebuiltMetadata = new ArrayList<>();
        final Map<String, List<String>> storeToSourceTopics = topologyMetadata.stateStoreNameToSourceTopics();
        Stream.concat(activePartitionHostMap.keySet().stream(), standbyPartitionHostMap.keySet().stream())
            .distinct()
            .sorted(Comparator.comparing(HostInfo::host).thenComparingInt(HostInfo::port))
            .forEach(hostInfo -> {
                final Set<TopicPartition> activePartitionsOnHost = new HashSet<>();
                final Set<String> activeStoresOnHost = new HashSet<>();
                if (activePartitionHostMap.containsKey(hostInfo)) {
                    activePartitionsOnHost.addAll(activePartitionHostMap.get(hostInfo));
                    activeStoresOnHost.addAll(getStoresOnHost(storeToSourceTopics, activePartitionsOnHost));
                }
                activeStoresOnHost.addAll(globalStores);

                final Set<TopicPartition> standbyPartitionsOnHost = new HashSet<>();
                final Set<String> standbyStoresOnHost = new HashSet<>();
                if (standbyPartitionHostMap.containsKey(hostInfo)) {
                    standbyPartitionsOnHost.addAll(standbyPartitionHostMap.get(hostInfo));
                    standbyStoresOnHost.addAll(getStoresOnHost(storeToSourceTopics, standbyPartitionsOnHost));
                }

                final StreamsMetadata metadata = new StreamsMetadataImpl(
                    hostInfo,
                    activeStoresOnHost,
                    activePartitionsOnHost,
                    standbyStoresOnHost,
                    standbyPartitionsOnHost
                );

                rebuiltMetadata.add(metadata);
                if (hostInfo.equals(thisHost)) {
                    localMetadata.set(metadata);
                }
            });
        return rebuiltMetadata;
    }

    private final Function<Optional<Set<Integer>>, Integer> getPartition = maybeMulticastPartitions -> {
        if (maybeMulticastPartitions.isEmpty()) {
            return null;
        }
        if (maybeMulticastPartitions.get().size() != 1) {
            throw new IllegalArgumentException("The partitions returned by StreamPartitioner#partitions method when used for fetching KeyQueryMetadata for key should be a singleton set");
        }
        return maybeMulticastPartitions.get().iterator().next();
    };

    private <K> KeyQueryMetadata keyQueryMetadataForKey(final String storeName,
                                                        final K key,
                                                        final StreamPartitioner<? super K, ?> partitioner,
                                                        final SourceTopicsInfo sourceTopicsInfo) {

        final Integer partition = getPartition.apply(partitioner.partitions(sourceTopicsInfo.topicWithMostPartitions, key, null, sourceTopicsInfo.maxPartitions));
        final Set<TopicPartition> matchingPartitions = new HashSet<>();
        for (final String sourceTopic : sourceTopicsInfo.sourceTopics) {
            matchingPartitions.add(new TopicPartition(sourceTopic, partition));
        }

        HostInfo activeHost = UNKNOWN_HOST;
        final Set<HostInfo> standbyHosts = new HashSet<>();
        for (final StreamsMetadata streamsMetadata : allMetadata) {
            final Set<String> activeStateStoreNames = streamsMetadata.stateStoreNames();
            final Set<TopicPartition> topicPartitions = new HashSet<>(streamsMetadata.topicPartitions());
            final Set<String> standbyStateStoreNames = streamsMetadata.standbyStateStoreNames();
            final Set<TopicPartition> standbyTopicPartitions = new HashSet<>(streamsMetadata.standbyTopicPartitions());

            topicPartitions.retainAll(matchingPartitions);
            if (activeStateStoreNames.contains(storeName) && !topicPartitions.isEmpty()) {
                activeHost = streamsMetadata.hostInfo();
            }

            standbyTopicPartitions.retainAll(matchingPartitions);
            if (standbyStateStoreNames.contains(storeName) && !standbyTopicPartitions.isEmpty()) {
                standbyHosts.add(streamsMetadata.hostInfo());
            }
        }

        return new KeyQueryMetadata(activeHost, standbyHosts, partition);
    }

    private <K> KeyQueryMetadata keyQueryMetadataForKey(final String storeName,
                                                        final K key,
                                                        final StreamPartitioner<? super K, ?> partitioner,
                                                        final SourceTopicsInfo sourceTopicsInfo,
                                                        final String topologyName) {
        Objects.requireNonNull(topologyName, "topology name must not be null");
        final Integer partition = getPartition.apply(partitioner.partitions(sourceTopicsInfo.topicWithMostPartitions, key, null, sourceTopicsInfo.maxPartitions));
        final Set<TopicPartition> matchingPartitions = new HashSet<>();
        for (final String sourceTopic : sourceTopicsInfo.sourceTopics) {
            matchingPartitions.add(new TopicPartition(sourceTopic, partition));
        }

        HostInfo activeHost = UNKNOWN_HOST;
        final Set<HostInfo> standbyHosts = new HashSet<>();
        for (final StreamsMetadata streamsMetadata : allMetadata) {
            final String metadataTopologyName = ((StreamsMetadataImpl) streamsMetadata).topologyName();
            if (metadataTopologyName != null && metadataTopologyName.equals(topologyName)) {

                final Set<String> activeStateStoreNames = streamsMetadata.stateStoreNames();
                final Set<TopicPartition> topicPartitions = new HashSet<>(streamsMetadata.topicPartitions());
                final Set<String> standbyStateStoreNames = streamsMetadata.standbyStateStoreNames();
                final Set<TopicPartition> standbyTopicPartitions = new HashSet<>(streamsMetadata.standbyTopicPartitions());

                topicPartitions.retainAll(matchingPartitions);
                if (activeStateStoreNames.contains(storeName) && !topicPartitions.isEmpty()) {
                    activeHost = streamsMetadata.hostInfo();
                }

                standbyTopicPartitions.retainAll(matchingPartitions);
                if (standbyStateStoreNames.contains(storeName) && !standbyTopicPartitions.isEmpty()) {
                    standbyHosts.add(streamsMetadata.hostInfo());
                }
            }
        }

        return new KeyQueryMetadata(activeHost, standbyHosts, partition);
    }

    private SourceTopicsInfo getSourceTopicsInfo(final String storeName) {
        return getSourceTopicsInfo(storeName, null);
    }

    private SourceTopicsInfo getSourceTopicsInfo(final String storeName, final String topologyName) {
        final List<String> sourceTopics = new ArrayList<>(topologyMetadata.sourceTopicsForStore(storeName, topologyName));

        if (sourceTopics.isEmpty()) {
            return null;
        }
        return new SourceTopicsInfo(sourceTopics);
    }

    private boolean isInitialized() {
        return partitionsByTopic != null && !partitionsByTopic.isEmpty() && localMetadata.get() != null;
    }

    public String storeForChangelogTopic(final String topicName) {
        return topologyMetadata.storeForChangelogTopic(topicName);
    }

    private class SourceTopicsInfo {
        private final List<String> sourceTopics;
        private int maxPartitions;
        private String topicWithMostPartitions;

        private SourceTopicsInfo(final List<String> sourceTopics) {
            this.sourceTopics = sourceTopics;
            for (final String topic : sourceTopics) {
                final List<PartitionInfo> partitions = partitionsByTopic.getOrDefault(topic, Collections.emptyList());
                if (partitions.size() > maxPartitions) {
                    maxPartitions = partitions.size();
                    topicWithMostPartitions = topic;
                }
            }
        }
    }
}
