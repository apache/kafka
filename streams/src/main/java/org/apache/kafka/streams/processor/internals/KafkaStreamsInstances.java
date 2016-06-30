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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KafkaStreamsInstance;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.toPositive;

/**
 * Provides access to the {@link KafkaStreamsInstance} in a KafkaStreams application. This can be used
 * to discover the locations of {@link org.apache.kafka.streams.processor.StateStore}s
 * in a KafkaStreams application
 */
public class KafkaStreamsInstances {
    private final Map<HostInfo, Set<TopicPartition>> hostToTopicPartition;
    private final TopologyBuilder builder;
    private final Map<String, List<TopicPartition>> partitionsByTopic;

    public KafkaStreamsInstances(final Map<HostInfo, Set<TopicPartition>> hostToTopicPartition,
                                 final TopologyBuilder builder) {
        this.hostToTopicPartition = hostToTopicPartition;
        this.partitionsByTopic = getPartitionsByTopic();
        this.builder = builder;
    }

    /**
     * Find all of the {@link KafkaStreamsInstance}s in a
     * {@link KafkaStreams application}
     *
     * @return all the {@link KafkaStreamsInstance}s in a {@link KafkaStreams} application
     */
    public Collection<KafkaStreamsInstance> getAllStreamsInstances() {
        final Map<String, String> stateStoreNameToSourceTopics = builder.getStateStoreNameToSourceTopics();
        final Map<HostInfo, KafkaStreamsInstance> results = new HashMap<>();

        for (final Map.Entry<String, String> entry : stateStoreNameToSourceTopics.entrySet()) {
            updateInstancesWithStore(entry.getKey(), results, partitionsByTopic.get(entry.getValue()));
        }

        /**
         * Add hosts that don't have any stores
         */
        for (HostInfo hostInfo : hostToTopicPartition.keySet()) {
            if (!results.containsKey(hostInfo)) {
                results.put(hostInfo, new KafkaStreamsInstance(hostInfo,
                        Collections.<String>emptySet(),
                        hostToTopicPartition.get(hostInfo)));
            }
        }
        return new ArrayList<>(results.values());
    }

    /**
     * Find all of the {@link KafkaStreamsInstance}s for a given storeName
     *
     * @param storeName the storeName to find metadata for
     * @return A collection of {@link KafkaStreamsInstance} that have the provided storeName
     */
    public Collection<KafkaStreamsInstance> getAllStreamsInstancesWithStore(final String storeName) {
        if (storeName == null) {
            throw new IllegalArgumentException("storeName cannot be null");
        }
        final String sourceTopic = builder.getStateStoreNameToSourceTopics().get(storeName);
        if (sourceTopic == null) {
            return Collections.emptyList();
        }
        final Map<HostInfo, KafkaStreamsInstance> results = new HashMap<>();
        updateInstancesWithStore(storeName, results, partitionsByTopic.get(sourceTopic));
        return new ArrayList<>(results.values());

    }

    /**
     * Find all of the {@link KafkaStreamsInstance}s for a given storeName and key.
     * Note: the key may not exist in the {@link org.apache.kafka.streams.processor.StateStore},
     * this method provides a way of finding which {@link KafkaStreamsInstance} it would exist on.
     *
     * @param storeName     Name of the store
     * @param key           Key to use to for partition
     * @param keySerializer Serializer for the key
     * @param <K>           key type
     * @return The {@link HostInfo} with its associated {@link TaskMetadata} for the storeName and key
     */
    public <K> KafkaStreamsInstance getStreamsInstanceWithKey(final String storeName,
                                                              final K key,
                                                              final Serializer<K> keySerializer) {
        if (keySerializer == null) {
            throw new IllegalArgumentException("keySerializer cannot be null");
        }

        return getStreamsInstanceWithKey(storeName, key, new StreamPartitioner<K, Object>() {
            @Override
            public Integer partition(final K key, final Object value, final int numPartitions) {
                final String sourceTopic = builder.getStateStoreNameToSourceTopics().get(storeName);
                final byte[] bytes = keySerializer.serialize(sourceTopic, key);
                return toPositive(Utils.murmur2(bytes)) % numPartitions;
            }
        });
    }

    /**
     * Find all of the {@link KafkaStreamsInstance}s for a given storeName and key.
     * Note: the key may not exist in the {@link org.apache.kafka.streams.processor.StateStore},
     * this method provides a way of finding which {@link KafkaStreamsInstance} it would exist on.
     *
     * @param storeName   Name of the store
     * @param key         Key to use to for partition
     * @param partitioner partitioner to use to find correct partition for key
     * @param <K>         key type
     * @return The {@link HostInfo} with its associated {@link TaskMetadata} for the storeName and key
     */
    public <K> KafkaStreamsInstance getStreamsInstanceWithKey(final String storeName,
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

        final String sourceTopic = builder.getStateStoreNameToSourceTopics().get(storeName);
        if (sourceTopic == null) {
            return null;
        }
        final List<TopicPartition> allPartitions = partitionsByTopic.get(sourceTopic);
        final Integer partition = partitioner.partition(key, null, allPartitions.size());
        final Map<HostInfo, KafkaStreamsInstance> results = new HashMap<>();
        updateInstancesWithStore(storeName, results, Collections.singletonList(new TopicPartition(sourceTopic, partition)));
        if (results.isEmpty()) {
            return null;
        }
        return results.values().iterator().next();
    }


    private void updateInstancesWithStore(final String storeName,
                                          final Map<HostInfo, KafkaStreamsInstance> results,
                                          final List<TopicPartition> partitionsForTopic) {
        for (final Map.Entry<HostInfo, Set<TopicPartition>> hostToPartitions : hostToTopicPartition.entrySet()) {
            final Set<TopicPartition> partitionsForHost = new HashSet<>(hostToPartitions.getValue());
            partitionsForHost.retainAll(partitionsForTopic);
            if (partitionsForHost.isEmpty()) {
                continue;
            }

            final HostInfo hostInfo = hostToPartitions.getKey();
            if (!results.containsKey(hostInfo)) {
                results.put(hostInfo, new KafkaStreamsInstance(hostInfo,
                        Collections.<String>emptySet(), Collections.<TopicPartition>emptySet()));
            }
            final KafkaStreamsInstance instance = results.get(hostInfo);
            partitionsForHost.addAll(instance.getTopicPartitions());
            final HashSet<String> stateStoreNames = new HashSet<>();
            stateStoreNames.add(storeName);
            stateStoreNames.addAll(instance.getStateStoreNames());
            results.put(hostInfo, new KafkaStreamsInstance(hostInfo, stateStoreNames, partitionsForHost));
        }
    }

    private Map<String, List<TopicPartition>> getPartitionsByTopic() {
        final Map<String, List<TopicPartition>> topicPartitionByTopic = new HashMap<>();
        for (final Set<TopicPartition> topicPartitions : hostToTopicPartition.values()) {
            for (TopicPartition topicPartition : topicPartitions) {
                if (!topicPartitionByTopic.containsKey(topicPartition.topic())) {
                    topicPartitionByTopic.put(topicPartition.topic(), new ArrayList<TopicPartition>());
                }
                topicPartitionByTopic.get(topicPartition.topic()).add(topicPartition);
            }
        }
        return topicPartitionByTopic;
    }


}
