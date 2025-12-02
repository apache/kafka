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

package org.apache.kafka.server.share.session;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.utils.ImplicitLinkedHashCollection;
import org.apache.kafka.server.share.CachedSharePartition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShareSession {

    // Helper enum to return the possible type of modified list of TopicIdPartitions in cache
    public enum ModifiedTopicIdPartitionType {
        ADDED,
        UPDATED,
        REMOVED
    }

    private final ShareSessionKey key;
    private final ImplicitLinkedHashCollection<CachedSharePartition> partitionMap;
    private final String connectionId;

    // visible for testing
    public int epoch;
    // This is used by the ShareSessionCache to store the last known size of this session.
    // If this is -1, the Session is not in the cache.
    private int cachedSize = -1;

    /**
     * The share session.
     * Each share session is protected by its own lock, which must be taken before mutable
     * fields are read or modified. This includes modification of the share session partition map.
     *
     * @param key                The share session key to identify the share session uniquely.
     * @param partitionMap       The CachedPartitionMap.
     * @param epoch              The share session sequence number.
     * @param connectionId       The connection id associated with this share session.
     */
    public ShareSession(
        ShareSessionKey key,
        ImplicitLinkedHashCollection<CachedSharePartition> partitionMap,
        int epoch,
        String connectionId
    ) {
        this.key = key;
        this.partitionMap = partitionMap;
        this.epoch = epoch;
        this.connectionId = connectionId;
    }

    public ShareSessionKey key() {
        return key;
    }

    public synchronized int cachedSize() {
        return cachedSize;
    }

    public synchronized ImplicitLinkedHashCollection<CachedSharePartition> partitionMap() {
        return partitionMap;
    }

    // Visible for testing
    public synchronized int epoch() {
        return epoch;
    }

    public synchronized int size() {
        return partitionMap.size();
    }

    public synchronized Boolean isEmpty() {
        return partitionMap.isEmpty();
    }

    public String connectionId() {
        return connectionId;
    }

    // Update the cached partition data based on the request.
    public synchronized Map<ModifiedTopicIdPartitionType, List<TopicIdPartition>> update(
        List<TopicIdPartition> shareFetchData,
        List<TopicIdPartition> toForget
    ) {
        List<TopicIdPartition> added = new ArrayList<>();
        List<TopicIdPartition> updated = new ArrayList<>();
        List<TopicIdPartition> removed = new ArrayList<>();
        shareFetchData.forEach(topicIdPartition -> {
            CachedSharePartition cachedSharePartitionKey = new CachedSharePartition(topicIdPartition, true);
            CachedSharePartition cachedPart = partitionMap.find(cachedSharePartitionKey);
            if (cachedPart == null) {
                partitionMap.mustAdd(cachedSharePartitionKey);
                added.add(topicIdPartition);
            } else {
                updated.add(topicIdPartition);
            }
        });
        toForget.forEach(topicIdPartition -> {
            if (partitionMap.remove(new CachedSharePartition(topicIdPartition)))
                removed.add(topicIdPartition);
        });
        Map<ModifiedTopicIdPartitionType, List<TopicIdPartition>> result = new HashMap<>();
        result.put(ModifiedTopicIdPartitionType.ADDED, added);
        result.put(ModifiedTopicIdPartitionType.UPDATED, updated);
        result.put(ModifiedTopicIdPartitionType.REMOVED, removed);
        return result;
    }

    /**
     * Updates the cached size of the session to represent the current partitionMap size.
     * @return The difference between the current cached size and the previously stored cached size. This is required to
     *         update the total number of share partitions stored in the share session cache.
     */
    public synchronized int updateCachedSize() {
        var previousSize = cachedSize;
        cachedSize = partitionMap.size();
        return previousSize != -1 ? cachedSize - previousSize : cachedSize;
    }

    public static String partitionsToLogString(Collection<TopicIdPartition> partitions, Boolean traceEnabled) {
        if (traceEnabled) {
            return String.format("( %s )", String.join(", ", partitions.toString()));
        }
        return String.format("%s partition(s)", partitions.size());
    }

    public String toString() {
        return "ShareSession(" +
                "key=" + key +
                ", partitionMap=" + partitionMap +
                ", epoch=" + epoch +
                ", cachedSize=" + cachedSize +
                ", connectionId=" + connectionId +
                ")";
    }
}
