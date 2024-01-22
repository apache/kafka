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
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteMetadata;

import java.io.IOException;

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

    public abstract void syncLogMetadataSnapshot(TopicIdPartition topicIdPartition,
                                                 int metadataPartition,
                                                 Long metadataPartitionOffset) throws IOException;

    public abstract void clearTopicPartition(TopicIdPartition topicIdPartition);

    public abstract void markInitialized(TopicIdPartition partition);

    public abstract boolean isInitialized(TopicIdPartition partition);

    public abstract void maybeLoadPartition(TopicIdPartition partition);
}