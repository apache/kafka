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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;

import java.io.IOException;
import java.nio.file.Path;

/**
 * This is a wrapper around {@link RemoteLogMetadataCache} providing a file based snapshot of
 * {@link RemoteLogMetadataCache} for the given {@code topicIdPartition}. Snapshot is stored in the given
 * {@code partitionDir}.
 */
public class FileBasedRemoteLogMetadataCache extends RemoteLogMetadataCache {

    private final RemoteLogMetadataSnapshotFile snapshotFile;
    private final TopicIdPartition topicIdPartition;

    public FileBasedRemoteLogMetadataCache(TopicIdPartition topicIdPartition,
                                           Path partitionDir) {
        if (!partitionDir.toFile().exists() || !partitionDir.toFile().isDirectory()) {
            throw new KafkaException("Given partition directory:" + partitionDir + " must be an existing directory.");
        }

        this.topicIdPartition = topicIdPartition;
        snapshotFile = new RemoteLogMetadataSnapshotFile(partitionDir);

        try {
            snapshotFile.read().ifPresent(snapshot -> loadRemoteLogSegmentMetadata(snapshot.remoteLogMetadatas()));
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }

    /**
     * Flushes the in-memory state to the snapshot file.
     *
     * @param metadataPartition       remote log metadata partition from which the messages have been consumed for the given
     *                                user topic partition.
     * @param metadataPartitionOffset remote log metadata partition offset up to which the messages have been consumed.
     * @throws IOException if any errors occurred while writing the snapshot to the file.
     */
    public void flushToFile(int metadataPartition,
                            Long metadataPartitionOffset) throws IOException {
        snapshotFile.write(new RemoteLogMetadataSnapshotFile.Snapshot(topicIdPartition.topicId(), metadataPartition, metadataPartitionOffset,
                                                                      idToSegmentMetadata.values()));
    }
}
