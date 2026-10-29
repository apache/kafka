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
package kafka.server.share;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.server.share.SharePartitionKey;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * The SharePartitionCache is used to cache the SharePartition objects. The cache is thread-safe.
 */
public class SharePartitionCache {

    /**
     * The map to store the share group id and the set of topic-partitions for that group.
     */
    private final Map<String, Set<TopicIdPartition>> groups;

    /**
     * The map is used to store the SharePartition objects for each share group topic-partition.
     */
    private final Map<SharePartitionKey, SharePartition> partitions;

    SharePartitionCache() {
        this.groups = new HashMap<>();
        this.partitions = new ConcurrentHashMap<>();
    }

    /**
     * Returns the share partition for the given key.
     *
     * @param partitionKey The key to get the share partition for.
     * @return The share partition for the key or null if not found.
     */
    public SharePartition get(SharePartitionKey partitionKey) {
        return partitions.get(partitionKey);
    }

    /**
     * Returns the set of topic-partitions for the given group id.
     *
     * @param groupId The group id to get the topic-partitions for.
     * @return The set of topic-partitions for the group id.
     */
    public synchronized Set<TopicIdPartition> topicIdPartitionsForGroup(String groupId) {
        return groups.containsKey(groupId) ? Set.copyOf(groups.get(groupId)) : Set.of();
    }

    /**
     * Removes the share partition from the cache. The method also removes the topic-partition from
     * the group map.
     *
     * @param partitionKey The key to remove.
     * @return The removed value or null if not found.
     */
    public synchronized SharePartition remove(SharePartitionKey partitionKey) {
        groups.computeIfPresent(partitionKey.groupId(), (k, v) -> {
            v.remove(partitionKey.topicIdPartition());
            return v.isEmpty() ? null : v;
        });
        return partitions.remove(partitionKey);
    }

    /**
     * Computes the value for the given key if it is not already present in the cache. Method also
     * updates the group map with the topic-partition for the group id.
     *
     * @param partitionKey The key to compute the value for.
     * @param mappingFunction The function to compute the value.
     * @return The computed or existing value.
     */
    public synchronized SharePartition computeIfAbsent(SharePartitionKey partitionKey, Function<SharePartitionKey, SharePartition> mappingFunction) {
        groups.computeIfAbsent(partitionKey.groupId(), k -> new HashSet<>()).add(partitionKey.topicIdPartition());
        return partitions.computeIfAbsent(partitionKey, mappingFunction);
    }

    /**
     * Returns the set of all share partition keys in the cache. As the cache can't be cleaned without
     * marking the share partitions fenced and detaching the partition listener in the replica manager,
     * hence rather providing a method to clean the cache directly, this method is provided to fetch
     * all the keys in the cache.
     *
     * @return The set of all share partition keys.
     */
    public Set<SharePartitionKey> cachedSharePartitionKeys() {
        return partitions.keySet();
    }

    // Visible for testing. Should not be used outside the test classes.
    void put(SharePartitionKey partitionKey, SharePartition sharePartition) {
        partitions.put(partitionKey, sharePartition);
    }

    // Visible for testing.
    int size() {
        return partitions.size();
    }

    // Visible for testing.
    boolean containsKey(SharePartitionKey partitionKey) {
        return partitions.containsKey(partitionKey);
    }

    // Visible for testing.
    boolean isEmpty() {
        return partitions.isEmpty();
    }

    // Visible for testing.
    synchronized Map<String, Set<TopicIdPartition>> groups() {
        return Map.copyOf(groups);
    }
}
