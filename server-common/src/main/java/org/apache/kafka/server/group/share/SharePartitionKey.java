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
package org.apache.kafka.server.group.share;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;

import java.util.Objects;

/**
 * Common immutable share partition key class. This class is
 * placed in server-common so that it can be freely used across
 * various modules.
 */
public class SharePartitionKey {
    private final String groupId;
    private final Uuid topicId;
    private final int partition;

    private SharePartitionKey(String groupId, Uuid topicId, int partition) {
        this.groupId = groupId;
        this.topicId = topicId;
        this.partition = partition;
    }

    public String groupId() {
        return groupId;
    }

    public Uuid topicId() {
        return topicId;
    }

    public int partition() {
        return partition;
    }

    public static SharePartitionKey getInstance(String groupId, TopicIdPartition topicIdPartition) {
        return getInstance(groupId, topicIdPartition.topicId(), topicIdPartition.partition());
    }

    public static SharePartitionKey getInstance(String groupId, Uuid topicId, int partition) {
        return new SharePartitionKey(groupId, topicId, partition);
    }

    public String asCoordinatorKey() {
        return asCoordinatorKey(groupId, topicId, partition);
    }

    public static String asCoordinatorKey(String groupId, Uuid topicId, int partition) {
        return String.format("%s:%s:%d", groupId, topicId, partition);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SharePartitionKey)) return false;
        SharePartitionKey that = (SharePartitionKey) o;
        return partition == that.partition && Objects.equals(groupId, that.groupId) && Objects.equals(topicId, that.topicId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId, topicId, partition);
    }

    @Override
    public String toString() {
        return "SharePartitionKey{" +
            "groupId=" + groupId +
            ",topicId=" + topicId +
            ",partition=" + partition +
            "}";
    }
}
