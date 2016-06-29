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
import org.apache.kafka.streams.state.HostState;
import org.apache.kafka.streams.state.TaskMetadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.toPositive;

/**
 * Provides a Mapping from {@link HostState} to {@link TaskMetadata}. This can be used
 * to discovery the locations of {@link org.apache.kafka.streams.processor.StateStore}s
 * in a KafkaStreams application
 */
public class HostStateToTaskMetadata {
    private final Map<HostState, Set<TopicPartition>> hostToTopicPartition;
    private final TopologyBuilder builder;
    private final Map<String, List<TopicPartition>> partitionsByTopic;

    public HostStateToTaskMetadata(final Map<HostState, Set<TopicPartition>> hostToTopicPartition,
                                   final TopologyBuilder builder) {
        this.hostToTopicPartition = hostToTopicPartition;
        this.partitionsByTopic = getPartitionsByTopic();
        this.builder = builder;
    }

    /**
     * Find all of the {@link HostState}s and their associated {@link TaskMetadata} in a
     * {@link KafkaStreams application}
     * @return  all the {@link HostState}s in a {@link KafkaStreams} application and their corresponding
     *          {@link TaskMetadata}
     */
    public Map<HostState, TaskMetadata> getAllTasks() {
        final Map<HostState, TaskMetadata> results = new HashMap<>();
        final Map<String, String> stateStoreNameToSourceTopics = builder.getStateStoreNameToSourceTopics();
        for (final Map.Entry<String, String> entry : stateStoreNameToSourceTopics.entrySet()) {
            createTasksForStore(entry.getKey(), results, partitionsByTopic.get(entry.getValue()));
        }
        return results;
    }

    /**
     * Find all of the {@link HostState}s and their associated {@link TaskMetadata} for a given storeName
     * @param storeName the storeName to find metadata for
     * @return  A map containing {@link HostState} and {@link TaskMetadata} for the provided storeName
     */
    public Map<HostState, TaskMetadata> getAllTasksWithStore(final String storeName) {
        if (storeName == null) {
            throw new IllegalArgumentException("storeName cannot be null");
        }
        final String sourceTopic = builder.getStateStoreNameToSourceTopics().get(storeName);
        if (sourceTopic == null) {
            return Collections.emptyMap();
        }
        final Map<HostState, TaskMetadata> results = new HashMap<>();
        createTasksForStore(storeName, results, partitionsByTopic.get(sourceTopic));
        return results;

    }

    /**
     * Find the {@link HostState} and its associated {@link TaskMetadata} for the
     * given storeName and key. Note: the key may not exist in the {@link org.apache.kafka.streams.processor.StateStore},
     * this method provides a way of finding which host it would exist on.
     * @param storeName         Name of the store
     * @param key               Key to use to for partition
     * @param keySerializer     Serializer for the key
     * @param <K>               key type
     * @return  The {@link HostState} with its associated {@link TaskMetadata} for the storeName and key
     */
    public <K> Map<HostState, TaskMetadata> getTaskWithKey(final String storeName,
                                                    final K key,
                                                    final Serializer<K> keySerializer) {
        if (keySerializer == null) {
            throw new IllegalArgumentException("keySerializer cannot be null");
        }

        return getTaskWithKey(storeName, key, new StreamPartitioner<K, Object>() {
            @Override
            public Integer partition(final K key, final Object value, final int numPartitions) {
                final String sourceTopic = builder.getStateStoreNameToSourceTopics().get(storeName);
                final byte[] bytes = keySerializer.serialize(sourceTopic, key);
                return toPositive(Utils.murmur2(bytes)) % numPartitions;
            }
        });
    }

    /**
     * Find the {@link HostState} and its associated {@link TaskMetadata} for the
     * given storeName and key. Note: the key may not exist in the {@link org.apache.kafka.streams.processor.StateStore},
     * this method provides a way of finding which host it would exist on.
     * @param storeName     Name of the store
     * @param key           Key to use to for partition
     * @param partitioner   partitioner to use to find correct partition for key
     * @param <K>           key type
     * @return The {@link HostState} with its associated {@link TaskMetadata} for the storeName and key
     */
    public <K> Map<HostState, TaskMetadata> getTaskWithKey(final String storeName,
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
            return Collections.emptyMap();
        }
        final List<TopicPartition> allPartitions = partitionsByTopic.get(sourceTopic);
        final Integer partition = partitioner.partition(key, null, allPartitions.size());
        final Map<HostState, TaskMetadata> results = new HashMap<>();
        createTasksForStore(storeName, results, Collections.singletonList(new TopicPartition(sourceTopic, partition)));
        return results;
    }


    private void createTasksForStore(final String storeName,
                                     final Map<HostState, TaskMetadata> results,
                                     final List<TopicPartition> partitionsForTopic) {
        for (final Map.Entry<HostState, Set<TopicPartition>> hostToPartitions : hostToTopicPartition.entrySet()) {
            final Set<TopicPartition> partitionsForHost = new HashSet<>(hostToPartitions.getValue());
            partitionsForHost.retainAll(partitionsForTopic);
            if (partitionsForHost.isEmpty()) {
                continue;
            }

            if (!results.containsKey(hostToPartitions.getKey())) {
                results.put(hostToPartitions.getKey(), new TaskMetadata(Collections.<String>emptySet(), Collections.<TopicPartition>emptySet()));
            }
            final TaskMetadata taskMetadata = results.get(hostToPartitions.getKey());
            partitionsForHost.addAll(taskMetadata.getTopicPartitions());
            final HashSet<String> stateStoreNames = new HashSet<>();
            stateStoreNames.add(storeName);
            stateStoreNames.addAll(taskMetadata.getStateStoreNames());
            results.put(hostToPartitions.getKey(), new TaskMetadata(stateStoreNames, partitionsForHost));
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
