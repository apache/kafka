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

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.StreamsMetadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;


/**
 * Provides access to the {@link StreamsMetadata} in a KafkaStreams application. This can be used
 * to discover the locations of {@link org.apache.kafka.streams.processor.StateStore}s
 * in a KafkaStreams application
 */
public class StreamsMetadataState {
    public static final HostInfo UNKNOWN_HOST = new HostInfo("unknown", -1);
    private final InternalTopologyBuilder builder;
    private final List<StreamsMetadata> allMetadata = new ArrayList<>();
    private final Set<String> globalStores;
    private final HostInfo thisHost;
    private Cluster clusterMetadata;
    private StreamsMetadata myMetadata;

    public StreamsMetadataState(final InternalTopologyBuilder builder, final HostInfo thisHost) {
        this.builder = builder;
        this.globalStores = builder.globalStateStores().keySet();
        this.thisHost = thisHost;
    }

    @Override
    public String toString() {
        return toString("");
    }

    public String toString(final String indent) {
        final StringBuilder builder = new StringBuilder();
        builder.append(indent).append("GlobalMetadata: ").append(allMetadata).append("\n");
        builder.append(indent).append("GlobalStores: ").append(globalStores).append("\n");
        builder.append(indent).append("My HostInfo: ").append(thisHost).append("\n");
        builder.append(indent).append(clusterMetadata).append("\n");

        return builder.toString();
    }

    /**
     * Find all of the {@link StreamsMetadata}s in a
     * {@link KafkaStreams application}
     *
     * @return all the {@link StreamsMetadata}s in a {@link KafkaStreams} application
     */
    public synchronized Collection<StreamsMetadata> getAllMetadata() {
        return allMetadata;
    }

    /**
     * Find all of the {@link StreamsMetadata}s for a given storeName
     *
     * @param storeName the storeName to find metadata for
     * @return A collection of {@link StreamsMetadata} that have the provided storeName
     */
    public synchronized Collection<StreamsMetadata> getAllMetadataForStore(final String storeName) {
        Objects.requireNonNull(storeName, "storeName cannot be null");

        if (!isInitialized()) {
            return Collections.emptyList();
        }

        if (globalStores.contains(storeName)) {
            return allMetadata;
        }

        final List<String> sourceTopics = builder.stateStoreNameToSourceTopics().get(storeName);
        if (sourceTopics == null) {
            return Collections.emptyList();
        }

        final ArrayList<StreamsMetadata> results = new ArrayList<>();
        for (final StreamsMetadata metadata : allMetadata) {
            if (metadata.stateStoreNames().contains(storeName) || metadata.getStandbyStateStoreNames().contains(storeName)) {
                results.add(metadata);
            }
        }
        return results;
    }

    /**
     * Find the {@link StreamsMetadata}s for a given storeName and key. This method will use the
     * {@link DefaultStreamPartitioner} to locate the store. If a custom partitioner has been used
     * please use {@link StreamsMetadataState#getMetadataWithKey(String, Object, StreamPartitioner)}
     * Note: the key may not exist in the {@link org.apache.kafka.streams.processor.StateStore},
     * this method provides a way of finding which {@link StreamsMetadata} it would exist on.
     *
     * @param storeName     Name of the store
     * @param key           Key to use
     * @param keySerializer Serializer for the key
     * @param <K>           key type
     * @return The {@link StreamsMetadata} for the storeName and key or {@link StreamsMetadata#NOT_AVAILABLE}
     * if streams is (re-)initializing
     */
    @Deprecated
    public synchronized <K> StreamsMetadata getMetadataWithKey(final String storeName,
                                                               final K key,
                                                               final Serializer<K> keySerializer) {
        Objects.requireNonNull(keySerializer, "keySerializer can't be null");
        Objects.requireNonNull(storeName, "storeName can't be null");
        Objects.requireNonNull(key, "key can't be null");

        if (!isInitialized()) {
            return StreamsMetadata.NOT_AVAILABLE;
        }

        if (globalStores.contains(storeName)) {
            // global stores are on every node. if we dont' have the host info
            // for this host then just pick the first metadata
            if (thisHost == UNKNOWN_HOST) {
                return allMetadata.get(0);
            }
            return myMetadata;
        }

        final SourceTopicsInfo sourceTopicsInfo = getSourceTopicsInfo(storeName);
        if (sourceTopicsInfo == null) {
            return null;
        }

        return getStreamsMetadataForKey(storeName,
                key,
                new DefaultStreamPartitioner<>(keySerializer, clusterMetadata),
                sourceTopicsInfo);
    }

    /**
     * Find the {@link KeyQueryMetadata}s for a given storeName and key.
     *
     * Note: the key may not exist in the {@link StateStore},
     * this method provides a way of finding which {@link StreamsMetadata} it would exist on.
     *
     * @param storeName   Name of the store
     * @param key         Key to use
     * @param partitioner partitioner to use to find correct partition for key
     * @param <K>         key type
     * @return The {@link KeyQueryMetadata} for the storeName and key or {@link KeyQueryMetadata#NOT_AVAILABLE}
     * if streams is (re-)initializing
     */
    public synchronized <K> KeyQueryMetadata getKeyQueryMetadataWithKey(final String storeName,
                                                                        final K key,
                                                                        final StreamPartitioner<? super K, ?> partitioner) {
        Objects.requireNonNull(storeName, "storeName can't be null");
        Objects.requireNonNull(key, "key can't be null");
        Objects.requireNonNull(partitioner, "partitioner can't be null");

        if (!isInitialized()) {
            return KeyQueryMetadata.NOT_AVAILABLE;
        }

        if (globalStores.contains(storeName)) {
            // global stores are on every node. if we dont' have the host info
            // for this host then just pick the first metadata
            if (thisHost == UNKNOWN_HOST) {
                return new KeyQueryMetadata(allMetadata.get(0).hostInfo(), Collections.emptySet(), -1);
            }
            return new KeyQueryMetadata(myMetadata.hostInfo(), Collections.emptySet(), -1);
        }

        final SourceTopicsInfo sourceTopicsInfo = getSourceTopicsInfo(storeName);
        if (sourceTopicsInfo == null) {
            return null;
        }
        return getKeyQueryMetadataForKey(storeName, key, partitioner, sourceTopicsInfo);
    }

    /**
     * Find the {@link KeyQueryMetadata}s for a given storeName and key. This method will use the
     * {@link DefaultStreamPartitioner} to locate the store. If a custom partitioner has been used
     * please use {@link StreamsMetadataState#getKeyQueryMetadataWithKey(String, Object, Serializer)}
     *
     * Note: the key may not exist in the {@link org.apache.kafka.streams.processor.StateStore},
     * this method provides a way of finding which {@link KeyQueryMetadata} it would exist on.
     *
     * @param storeName     Name of the store
     * @param key           Key to use
     * @param keySerializer Serializer for the key
     * @param <K>           key type
     * @return The {@link KeyQueryMetadata} for the storeName and key or {@link KeyQueryMetadata#NOT_AVAILABLE}
     * if streams is (re-)initializing
     */
    public synchronized <K> KeyQueryMetadata getKeyQueryMetadataWithKey(final String storeName,
                                                                        final K key,
                                                                        final Serializer<K> keySerializer) {
        Objects.requireNonNull(keySerializer, "keySerializer can't be null");
        Objects.requireNonNull(storeName, "storeName can't be null");
        Objects.requireNonNull(key, "key can't be null");

        if (!isInitialized()) {
            return KeyQueryMetadata.NOT_AVAILABLE;
        }

        if (globalStores.contains(storeName)) {
            // global stores are on every node. if we dont' have the host info
            // for this host then just pick the first metadata
            if (thisHost == UNKNOWN_HOST) {
                return new KeyQueryMetadata(allMetadata.get(0).hostInfo(), Collections.emptySet(), -1);
            }
            return new KeyQueryMetadata(myMetadata.hostInfo(), Collections.emptySet(), -1);
        }

        final SourceTopicsInfo sourceTopicsInfo = getSourceTopicsInfo(storeName);
        if (sourceTopicsInfo == null) {
            return null;
        }

        return getKeyQueryMetadataForKey(storeName,
                key,
                new DefaultStreamPartitioner<>(keySerializer, clusterMetadata),
                sourceTopicsInfo);
    }

    /**
     * Find the {@link StreamsMetadata}s for a given storeName and key.
     * <p>
     * Note: the key may not exist in the {@link StateStore},
     * this method provides a way of finding which {@link StreamsMetadata} it would exist on.
     *
     * @param storeName   Name of the store
     * @param key         Key to use
     * @param partitioner partitioner to use to find correct partition for key
     * @param <K>         key type
     * @return The {@link StreamsMetadata} for the storeName and key or {@link StreamsMetadata#NOT_AVAILABLE}
     * if streams is (re-)initializing
     */
    @Deprecated
    public synchronized <K> StreamsMetadata getMetadataWithKey(final String storeName,
                                                               final K key,
                                                               final StreamPartitioner<? super K, ?> partitioner) {
        Objects.requireNonNull(storeName, "storeName can't be null");
        Objects.requireNonNull(key, "key can't be null");
        Objects.requireNonNull(partitioner, "partitioner can't be null");

        if (!isInitialized()) {
            return StreamsMetadata.NOT_AVAILABLE;
        }

        if (globalStores.contains(storeName)) {
            // global stores are on every node. if we dont' have the host info
            // for this host then just pick the first metadata
            if (thisHost == UNKNOWN_HOST) {
                return allMetadata.get(0);
            }
            return myMetadata;
        }

        final SourceTopicsInfo sourceTopicsInfo = getSourceTopicsInfo(storeName);
        if (sourceTopicsInfo == null) {
            return null;
        }
        return getStreamsMetadataForKey(storeName, key, partitioner, sourceTopicsInfo);
    }

    /**
     * Respond to changes to the HostInfo -> TopicPartition mapping. Will rebuild the
     * metadata
     *
     * @param activePartitionHostMap  the current mapping of {@link HostInfo} -> {@link TopicPartition}s for active partitions
     * @param standbyPartitionHostMap the current mapping of {@link HostInfo} -> {@link TopicPartition}s for standby partitions
     * @param clusterMetadata         the current clusterMetadata {@link Cluster}
     */
    synchronized void onChange(final Map<HostInfo, Set<TopicPartition>> activePartitionHostMap,
                               final Map<HostInfo, Set<TopicPartition>> standbyPartitionHostMap, final Cluster clusterMetadata) {
        this.clusterMetadata = clusterMetadata;
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

    private Set<String> getStoresOnHost(final Map<String, List<String>> stores, final Set<TopicPartition> partitionsForHost) {
        final Set<String> storesOnHost = new HashSet<>();
        for (final Map.Entry<String, List<String>> storeTopicEntry : stores.entrySet()) {
            final List<String> topicsForStore = storeTopicEntry.getValue();
            if (hasPartitionsForAnyTopics(topicsForStore, partitionsForHost)) {
                storesOnHost.add(storeTopicEntry.getKey());
            }
        }
        return storesOnHost;
    }


    private void rebuildMetadata(final Map<HostInfo, Set<TopicPartition>> activePartitionHostMap,
                                 final Map<HostInfo, Set<TopicPartition>> standbyPartitionHostMap) {
        allMetadata.clear();
        if (activePartitionHostMap.isEmpty()) {
            return;
        }
        final Map<String, List<String>> stores = builder.stateStoreNameToSourceTopics();
        for (final Map.Entry<HostInfo, Set<TopicPartition>> entry : activePartitionHostMap.entrySet()) {
            final HostInfo key = entry.getKey();
            final Set<TopicPartition> activepartitionsForHost = new HashSet<>(entry.getValue());
            final Set<String> activeStoresOnHost = getStoresOnHost(stores, activepartitionsForHost);
            activeStoresOnHost.addAll(globalStores);
            final Set<TopicPartition> standbyPartitionsForHost = new HashSet<>();
            final Set<String> standbyStoresOnHost = new HashSet<>();
            if (!standbyPartitionHostMap.isEmpty() && standbyPartitionHostMap.containsKey(key)) {
                standbyPartitionsForHost.addAll(standbyPartitionHostMap.get(key));
                standbyStoresOnHost.addAll(getStoresOnHost(stores, standbyPartitionsForHost));
            }
            final StreamsMetadata metadata = new StreamsMetadata(key, activeStoresOnHost, activepartitionsForHost, standbyPartitionsForHost, standbyStoresOnHost);
            if (activepartitionsForHost != null) {
                allMetadata.add(metadata);
            }
            if (key.equals(thisHost)) {
                myMetadata = metadata;
            }
        }
    }

    private <K> KeyQueryMetadata getKeyQueryMetadataForKey(final String storeName,
                                                           final K key,
                                                           final StreamPartitioner<? super K, ?> partitioner,
                                                           final SourceTopicsInfo sourceTopicsInfo) {

        final Integer partition = partitioner.partition(sourceTopicsInfo.topicWithMostPartitions, key, null, sourceTopicsInfo.maxPartitions);
        final Set<TopicPartition> matchingPartitions = new HashSet<>();
        for (final String sourceTopic : sourceTopicsInfo.sourceTopics) {
            matchingPartitions.add(new TopicPartition(sourceTopic, partition));
        }

        HostInfo activeHost = new HostInfo("unavailable", -1);
        final Set<HostInfo> standbyHosts = new HashSet<>();
        for (final StreamsMetadata streamsMetadata : allMetadata) {
            final Set<String> activeStateStoreNames = streamsMetadata.stateStoreNames();
            final Set<String> standbyStateStoreNames = streamsMetadata.getStandbyStateStoreNames();
            final Set<TopicPartition> topicPartitions = new HashSet<>(streamsMetadata.topicPartitions());
            topicPartitions.retainAll(matchingPartitions);
            if (activeStateStoreNames.contains(storeName)
                    && !topicPartitions.isEmpty()) {
                activeHost = streamsMetadata.hostInfo();
            } else if (standbyStateStoreNames.contains(storeName)
                    && !topicPartitions.isEmpty()) {
                standbyHosts.add(streamsMetadata.hostInfo());
            }
        }

        // would need these below changes to send TopicGroupId instead of partition in KeyQueryMetadata
        int myTopicGroupId;
        final Map<Integer, InternalTopologyBuilder.TopicsInfo> topicGroups = builder.topicGroups();
        for (Map.Entry<Integer, InternalTopologyBuilder.TopicsInfo> topicGroup : topicGroups.entrySet()) {
                if (topicGroup.getValue().sourceTopics.contains(sourceTopicsInfo.sourceTopics)) {
                    myTopicGroupId = topicGroup.getKey();
            }
        }

        return new KeyQueryMetadata(activeHost, standbyHosts, partition.intValue());
    }

    @Deprecated
    private <K> StreamsMetadata getStreamsMetadataForKey(final String storeName,
                                                         final K key,
                                                         final StreamPartitioner<? super K, ?> partitioner,
                                                         final SourceTopicsInfo sourceTopicsInfo) {

        final Integer partition = partitioner.partition(sourceTopicsInfo.topicWithMostPartitions, key, null, sourceTopicsInfo.maxPartitions);
        final Set<TopicPartition> matchingPartitions = new HashSet<>();
        for (final String sourceTopic : sourceTopicsInfo.sourceTopics) {
            matchingPartitions.add(new TopicPartition(sourceTopic, partition));
        }

        for (final StreamsMetadata streamsMetadata : allMetadata) {
            final Set<String> stateStoreNames = streamsMetadata.stateStoreNames();
            final Set<TopicPartition> topicPartitions = new HashSet<>(streamsMetadata.topicPartitions());
            topicPartitions.retainAll(matchingPartitions);
            if (stateStoreNames.contains(storeName)
                    && !topicPartitions.isEmpty()) {
                return streamsMetadata;
            }
        }
        return null;
    }

    private SourceTopicsInfo getSourceTopicsInfo(final String storeName) {
        final List<String> sourceTopics = builder.stateStoreNameToSourceTopics().get(storeName);
        if (sourceTopics == null || sourceTopics.isEmpty()) {
            return null;
        }
        return new SourceTopicsInfo(sourceTopics);
    }

    private boolean isInitialized() {
        return clusterMetadata != null && !clusterMetadata.topics().isEmpty();
    }

    private class SourceTopicsInfo {
        private final List<String> sourceTopics;
        private int maxPartitions;
        private String topicWithMostPartitions;

        private SourceTopicsInfo(final List<String> sourceTopics) {
            this.sourceTopics = sourceTopics;
            for (final String topic : sourceTopics) {
                final List<PartitionInfo> partitions = clusterMetadata.partitionsForTopic(topic);
                if (partitions.size() > maxPartitions) {
                    maxPartitions = partitions.size();
                    topicWithMostPartitions = partitions.get(0).topic();
                }
            }
        }
    }
}
