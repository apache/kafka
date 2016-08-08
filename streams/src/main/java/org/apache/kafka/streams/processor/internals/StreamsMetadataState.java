/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.StreamsMetadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.toPositive;

/**
 * Provides access to the {@link StreamsMetadata} in a KafkaStreams application. This can be used
 * to discover the locations of {@link org.apache.kafka.streams.processor.StateStore}s
 * in a KafkaStreams application
 */
public class StreamsMetadataState {
    private final TopologyBuilder builder;
    private final List<StreamsMetadata> allMetadata = new ArrayList<>();
    private Cluster clusterMetadata;

    public StreamsMetadataState(final TopologyBuilder builder) {
        this.builder = builder;
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
        if (storeName == null) {
            throw new IllegalArgumentException("storeName cannot be null");
        }
        final Set<String> sourceTopics = builder.stateStoreNameToSourceTopics().get(storeName);
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
     * Find the {@link StreamsMetadata}s for a given storeName and key.
     * Note: the key may not exist in the {@link org.apache.kafka.streams.processor.StateStore},
     * this method provides a way of finding which {@link StreamsMetadata} it would exist on.
     *
     * @param storeName     Name of the store
     * @param key           Key to use to for partition
     * @param keySerializer Serializer for the key
     * @param <K>           key type
     * @return The {@link StreamsMetadata} for the storeName and key
     */
    public synchronized <K> StreamsMetadata getMetadataWithKey(final String storeName,
                                                               final K key,
                                                               final Serializer<K> keySerializer) {
        if (keySerializer == null) {
            throw new IllegalArgumentException("keySerializer cannot be null");
        }

        // To find the correct instance for a given key we need to use the same partitioning
        // strategy that was used when producing the messages. In this case we create a StreamPartitioner
        // that works the same as org.apache.kafka.clients.producer.internals.DefaultPartitioner
        return getMetadataWithKey(storeName, key, new StreamPartitioner<K, Object>() {
            @Override
            public Integer partition(final K key, final Object value, final int numPartitions) {
                final String sourceTopic = builder.stateStoreNameToSourceTopics().get(storeName).iterator().next();
                final byte[] bytes = keySerializer.serialize(sourceTopic, key);
                return toPositive(Utils.murmur2(bytes)) % numPartitions;
            }
        });
    }

    /**
     * Find the {@link StreamsMetadata}s for a given storeName and key.
     * Note: the key may not exist in the {@link StateStore},
     * this method provides a way of finding which {@link StreamsMetadata} it would exist on.
     *
     * @param storeName   Name of the store
     * @param key         Key to use to for partition
     * @param partitioner partitioner to use to find correct partition for key
     * @param <K>         key type
     * @return The {@link StreamsMetadata} for the storeName and key
     */
    public synchronized <K> StreamsMetadata getMetadataWithKey(final String storeName,
                                                               final K key,
                                                               final StreamPartitioner<K, ?> partitioner) {
        if (storeName == null) {
            throw new IllegalArgumentException("storeName cannot be null");
        }

        if (key == null) {
            throw new IllegalArgumentException("key cannot be null");
        }

        if (partitioner == null) {
            throw new IllegalArgumentException("partitioner cannot be null");
        }

        final Set<String> sourceTopics = builder.stateStoreNameToSourceTopics().get(storeName);
        if (sourceTopics == null) {
            return null;
        }


        int numPartitions = 0;
        for (String topic : sourceTopics) {
            final List<PartitionInfo> partitions = clusterMetadata.partitionsForTopic(topic);
            if (partitions != null && partitions.size() > numPartitions) {
                numPartitions = partitions.size();
            }
        }

        final Integer partition = partitioner.partition(key, null, numPartitions);
        final Set<TopicPartition> matchingPartitions = new HashSet<>();
        for (String sourceTopic : sourceTopics) {
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

    /**
     * Respond to changes to the HostInfo -> TopicPartition mapping. Will rebuild the
     * metadata
     * @param currentState  the current mapping of {@link HostInfo} -> {@link TopicPartition}s
     * @param clusterMetadata    the current clusterMetadata {@link Cluster}
     */
    public synchronized void onChange(final Map<HostInfo, Set<TopicPartition>> currentState, final Cluster clusterMetadata) {
        this.clusterMetadata = clusterMetadata;
        rebuildMetadata(currentState);
    }

    private boolean hasPartitionsForAnyTopics(final Set<String> topicNames, final Set<TopicPartition> partitionForHost) {
        for (String topic : topicNames) {
            for (PartitionInfo partitionInfo : clusterMetadata.partitionsForTopic(topic)) {
                final TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
                if (partitionForHost.contains(topicPartition)) {
                    return true;
                }
            }
        }
        return false;
    }

    private void rebuildMetadata(final Map<HostInfo, Set<TopicPartition>> currentState) {
        allMetadata.clear();
        if (currentState.isEmpty()) {
            return;
        }
        final Map<String, Set<String>> stores = builder.stateStoreNameToSourceTopics();
        for (Map.Entry<HostInfo, Set<TopicPartition>> entry : currentState.entrySet()) {
            final HostInfo key = entry.getKey();
            final Set<TopicPartition> partitionsForHost = new HashSet<>(entry.getValue());
            final Set<String> storesOnHost = new HashSet<>();
            for (Map.Entry<String, Set<String>> storeTopicEntry : stores.entrySet()) {
                final Set<String> topicsForStore = storeTopicEntry.getValue();
                if (hasPartitionsForAnyTopics(topicsForStore,   partitionsForHost)) {
                    storesOnHost.add(storeTopicEntry.getKey());
                }
            }
            allMetadata.add(new StreamsMetadata(key, storesOnHost, partitionsForHost));
        }
    }
}
