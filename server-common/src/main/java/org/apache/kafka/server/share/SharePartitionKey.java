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
package org.apache.kafka.server.share;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;

import java.util.Objects;

/**
 * The SharePartitionKey is used to uniquely identify a share partition. The key is made up of the
 * share group id, the topic id and the partition id. The key is used to store the SharePartition
 * objects in the partition cache map.
 */
public class SharePartitionKey {

    protected final String groupId;
    protected final TopicIdPartition topicIdPartition;

    public SharePartitionKey(String groupId, TopicIdPartition topicIdPartition) {
        this.groupId = Objects.requireNonNull(groupId);
        this.topicIdPartition = Objects.requireNonNull(topicIdPartition);
    }

    private SharePartitionKey(String groupId, Uuid topicId, int partition) {
        this(groupId, topicId, null, partition);
    }

    private SharePartitionKey(String groupId, Uuid topicId, String topic, int partition) {
        this(groupId, new TopicIdPartition(Objects.requireNonNull(topicId), new TopicPartition(topic, partition)));
    }

    public String groupId() {
        return groupId;
    }

    public TopicIdPartition topicIdPartition() {
        return topicIdPartition;
    }

    public Uuid topicId() {
        return topicIdPartition.topicId();
    }

    public int partition() {
        return topicIdPartition.partition();
    }

    public static SharePartitionKey getInstance(String groupId, TopicIdPartition topicIdPartition) {
        return getInstance(groupId, topicIdPartition.topicId(), topicIdPartition.partition());
    }


    /**
     * Returns a SharePartitionKey from input string of format - groupId:topicId:partition
     * @param key - String in format groupId:topicId:partition
     * @return object representing SharePartitionKey
     * @throws IllegalArgumentException if the key is empty or has invalid format
     */
    public static SharePartitionKey getInstance(String key) {
        validate(key);
        String[] tokens = key.split(":");
        return new SharePartitionKey(
                tokens[0].trim(),
                Uuid.fromString(tokens[1]),
                Integer.parseInt(tokens[2])
        );
    }

    /**
     * Validates whether the String argument has a valid SharePartitionKey format - groupId:topicId:partition
     * @param key - String in format groupId:topicId:partition
     * @throws IllegalArgumentException if the key is empty or has invalid format
     */
    public static void validate(String key) {
        Objects.requireNonNull(key, "Share partition key cannot be null");
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Share partition key cannot be empty");
        }

        String[] tokens = key.split(":");
        if (tokens.length != 3) {
            throw new IllegalArgumentException("Invalid key format: expected - groupId:topicId:partition, found -  " + key);
        }

        if (tokens[0].trim().isEmpty()) {
            throw new IllegalArgumentException("GroupId must be alphanumeric string");
        }

        try {
            Uuid.fromString(tokens[1]);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid topic ID: " + tokens[1], e);
        }

        try {
            Integer.parseInt(tokens[2]);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid partition: " + tokens[2], e);
        }
    }

    public static SharePartitionKey getInstance(String groupId, Uuid topicId, int partition) {
        return new SharePartitionKey(groupId, topicId, partition);
    }

    public String asCoordinatorKey() {
        return asCoordinatorKey(groupId(), topicId(), partition());
    }

    public static String asCoordinatorKey(String groupId, Uuid topicId, int partition) {
        return String.format("%s:%s:%d", groupId, topicId, partition);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj)
            return true;
        else if (obj == null || getClass() != obj.getClass())
            return false;
        else {
            SharePartitionKey that = (SharePartitionKey) obj;
            return groupId.equals(that.groupId) && Objects.equals(topicIdPartition, that.topicIdPartition);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId, topicIdPartition);
    }

    @Override
    public String toString() {
        return "SharePartitionKey{" +
            "groupId=" + groupId +
            ", topicIdPartition=" + topicIdPartition +
            '}';
    }
}
