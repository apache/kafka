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
        for (StreamsMetadata metadata : allMetadata) {
            if (metadata.stateStoreNames().contains(storeName)) {
                results.add(metadata);
            }
        }
        return results;
    }

    /**
     * Find the {@link StreamsMetadata}s for a given storeName and key. This method will use the
     * {@link DefaultStreamPartitioner} to locate the store. If a custom partitioner has been used
     * please use {@link StreamsMetadataState#getMetadataWithKey(String, Object, StreamPartitioner)}
     *
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
     * Find the {@link StreamsMetadata}s for a given storeName and key.
     *
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

        SourceTopicsInfo sourceTopicsInfo = getSourceTopicsInfo(storeName);
        if (sourceTopicsInfo == null) {
            return null;
        }
        return getStreamsMetadataForKey(storeName, key, partitioner, sourceTopicsInfo);
    }

    /**
     * Respond to changes to the HostInfo -> TopicPartition mapping. Will rebuild the
     * metadata
     * @param currentState       the current mapping of {@link HostInfo} -> {@link TopicPartition}s
     * @param clusterMetadata    the current clusterMetadata {@link Cluster}
     */
    synchronized void onChange(final Map<HostInfo, Set<TopicPartition>> currentState, final Cluster clusterMetadata) {
        this.clusterMetadata = clusterMetadata;
        rebuildMetadata(currentState);
    }

    private boolean hasPartitionsForAnyTopics(final List<String> topicNames, final Set<TopicPartition> partitionForHost) {
        for (TopicPartition topicPartition : partitionForHost) {
            if (topicNames.contains(topicPartition.topic())) {
                return true;
            }
        }
        return false;
    }

    private void rebuildMetadata(final Map<HostInfo, Set<TopicPartition>> currentState) {
        allMetadata.clear();
        if (currentState.isEmpty()) {
            return;
        }
        final Map<String, List<String>> stores = builder.stateStoreNameToSourceTopics();
        for (Map.Entry<HostInfo, Set<TopicPartition>> entry : currentState.entrySet()) {
            final HostInfo key = entry.getKey();
            final Set<TopicPartition> partitionsForHost = new HashSet<>(entry.getValue());
            final Set<String> storesOnHost = new HashSet<>();
            for (Map.Entry<String, List<String>> storeTopicEntry : stores.entrySet()) {
                final List<String> topicsForStore = storeTopicEntry.getValue();
                if (hasPartitionsForAnyTopics(topicsForStore, partitionsForHost)) {
                    storesOnHost.add(storeTopicEntry.getKey());
                }
            }
            storesOnHost.addAll(globalStores);
            final StreamsMetadata metadata = new StreamsMetadata(key, storesOnHost, partitionsForHost);
            allMetadata.add(metadata);
            if (key.equals(thisHost)) {
                myMetadata = metadata;
            }
        }
    }

    private <K> StreamsMetadata getStreamsMetadataForKey(final String storeName,
                                                         final K key,
                                                         final StreamPartitioner<? super K, ?> partitioner,
                                                         final SourceTopicsInfo sourceTopicsInfo) {

        final Integer partition = partitioner.partition(sourceTopicsInfo.topicWithMostPartitions, key, null, sourceTopicsInfo.maxPartitions);
        final Set<TopicPartition> matchingPartitions = new HashSet<>();
        for (String sourceTopic : sourceTopicsInfo.sourceTopics) {
            matchingPartitions.add(new TopicPartition(sourceTopic, partition));
        }

        for (StreamsMetadata streamsMetadata : allMetadata) {
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
            for (String topic : sourceTopics) {
                final List<PartitionInfo> partitions = clusterMetadata.partitionsForTopic(topic);
                if (partitions.size() > maxPartitions) {
                    maxPartitions = partitions.size();
                    topicWithMostPartitions = partitions.get(0).topic();
                }
            }
        }
    }
}
