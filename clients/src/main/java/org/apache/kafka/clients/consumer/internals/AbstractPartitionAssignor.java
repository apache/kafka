/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.PartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Abstract assignor implementation which uses the following metadata format:
 *
 * ProtocolMetadata => Topics TopicPattern MetadataHash
 *   Topics         => [String]
 *   TopicPattern   => String
 *   MetadataHash   => Bytes
 *
 * This class handles metadata consistency checking between group members. Custom assignors will
 * usually extend this class unless they need to provide their own metadata format.
 */
public abstract class AbstractPartitionAssignor implements PartitionAssignor {
    private final Logger log = LoggerFactory.getLogger(getClass());

    public abstract Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                             Map<String, List<String>> subscriptions);

    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, List<String>> subscriptions,
                                                    Cluster metadata) {
        Set<String> allSubscribedTopics = new HashSet<>();
        for (List<String> topics : subscriptions.values())
            allSubscribedTopics.addAll(topics);

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        for (String topic : allSubscribedTopics) {
            Integer numPartitions = metadata.partitionCountForTopic(topic);
            if (numPartitions != null)
                partitionsPerTopic.put(topic, numPartitions);
            else
                log.debug("Skipping assignment for topic {} since no metadata is available", topic);
        }

        return assign(partitionsPerTopic, subscriptions);
    }

    protected static <K, V> void put(Map<K, List<V>> map, K key, V value) {
        List<V> list = map.get(key);
        if (list == null) {
            list = new ArrayList<>();
            map.put(key, list);
        }
        list.add(value);
    }

    protected static <K, V> void putAll(Map<K, List<V>> map, K key, List<V> values) {
        List<V> list = map.get(key);
        if (list == null) {
            list = new ArrayList<>();
            map.put(key, list);
        }
        list.addAll(values);
    }


}
