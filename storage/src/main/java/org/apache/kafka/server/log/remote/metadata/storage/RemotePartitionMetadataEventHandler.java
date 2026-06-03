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
package org.apache.kafka.server.log.remote.metadata.storage;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteMetadata;

public abstract class RemotePartitionMetadataEventHandler {

    public void handleRemoteLogMetadata(RemoteLogMetadata remoteLogMetadata) {
        if (remoteLogMetadata instanceof RemoteLogSegmentMetadata) {
            handleRemoteLogSegmentMetadata((RemoteLogSegmentMetadata) remoteLogMetadata);
        } else if (remoteLogMetadata instanceof RemoteLogSegmentMetadataUpdate) {
            handleRemoteLogSegmentMetadataUpdate((RemoteLogSegmentMetadataUpdate) remoteLogMetadata);
        } else if (remoteLogMetadata instanceof RemotePartitionDeleteMetadata) {
            handleRemotePartitionDeleteMetadata((RemotePartitionDeleteMetadata) remoteLogMetadata);
        } else {
            throw new IllegalArgumentException("remoteLogMetadata: " + remoteLogMetadata + " is not supported.");
        }
    }

    protected abstract void handleRemoteLogSegmentMetadata(RemoteLogSegmentMetadata remoteLogSegmentMetadata);

    protected abstract void handleRemoteLogSegmentMetadataUpdate(RemoteLogSegmentMetadataUpdate remoteLogSegmentMetadataUpdate);

    protected abstract void handleRemotePartitionDeleteMetadata(RemotePartitionDeleteMetadata remotePartitionDeleteMetadata);

    public abstract void clearTopicPartition(TopicIdPartition topicIdPartition);

    public abstract void markInitialized(TopicIdPartition partition);

    public abstract boolean isInitialized(TopicIdPartition partition);

    public abstract void maybeLoadPartition(TopicIdPartition partition);

    /**
     * Handle a tombstone event for a remote log segment.
     * This is called when a tombstone message is consumed from the metadata topic.
     *
     * @param topicId the topic UUID
     * @param topicName the topic name
     * @param partition the partition number
     * @param endOffset the end offset of the segment
     * @param brokerLeaderEpoch the broker leader epoch
     */
    public abstract void handleTombstoneEvent(Uuid topicId, String topicName, int partition, long endOffset, int brokerLeaderEpoch);
}