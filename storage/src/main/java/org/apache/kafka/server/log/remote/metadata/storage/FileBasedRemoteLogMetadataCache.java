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
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * This is a wrapper around {@link RemoteLogMetadataCache} providing a file based snapshot of
 * {@link RemoteLogMetadataCache} for the given {@code topicIdPartition}. Snapshot is stored in the given
 * {@code partitionDir}.
 */
public class FileBasedRemoteLogMetadataCache extends RemoteLogMetadataCache {
    private static final Logger log = LoggerFactory.getLogger(FileBasedRemoteLogMetadataCache.class);
    private final RemoteLogMetadataSnapshotFile snapshotFile;
    private final TopicIdPartition topicIdPartition;

    @SuppressWarnings("this-escape")
    public FileBasedRemoteLogMetadataCache(TopicIdPartition topicIdPartition,
                                           Path partitionDir) {
        if (!partitionDir.toFile().exists() || !partitionDir.toFile().isDirectory()) {
            throw new KafkaException("Given partition directory:" + partitionDir + " must be an existing directory.");
        }

        this.topicIdPartition = topicIdPartition;
        snapshotFile = new RemoteLogMetadataSnapshotFile(partitionDir);

        try {
            snapshotFile.read().ifPresent(snapshot -> loadRemoteLogSegmentMetadata(snapshot));
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }

    protected final void loadRemoteLogSegmentMetadata(RemoteLogMetadataSnapshotFile.Snapshot snapshot) {
        log.info("Loading snapshot for partition {} is: {}", topicIdPartition, snapshot);
        for (RemoteLogSegmentMetadataSnapshot metadataSnapshot : snapshot.remoteLogSegmentMetadataSnapshots()) {
            switch (metadataSnapshot.state()) {
                case COPY_SEGMENT_STARTED:
                    addCopyInProgressSegment(createRemoteLogSegmentMetadata(metadataSnapshot));
                    break;
                case COPY_SEGMENT_FINISHED:
                    handleSegmentWithCopySegmentFinishedState(createRemoteLogSegmentMetadata(metadataSnapshot));
                    break;
                case DELETE_SEGMENT_STARTED:
                    handleSegmentWithDeleteSegmentStartedState(createRemoteLogSegmentMetadata(metadataSnapshot));
                    break;
                case DELETE_SEGMENT_FINISHED:
                default:
                    throw new IllegalArgumentException("Given remoteLogSegmentMetadata has invalid state: " + metadataSnapshot);
            }
        }
    }

    private RemoteLogSegmentMetadata createRemoteLogSegmentMetadata(RemoteLogSegmentMetadataSnapshot snapshot) {
        return new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, snapshot.segmentId()), snapshot.startOffset(),
                                            snapshot.endOffset(), snapshot.maxTimestampMs(), snapshot.brokerId(), snapshot.eventTimestampMs(),
                                            snapshot.segmentSizeInBytes(), snapshot.customMetadata(), snapshot.state(), snapshot.segmentLeaderEpochs()
        );
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
        List<RemoteLogSegmentMetadataSnapshot> snapshots = new ArrayList<>(idToSegmentMetadata.size());
        for (RemoteLogLeaderEpochState state : leaderEpochEntries.values()) {
            // Add unreferenced segments first, as to maintain the order when these segments are again read from
            // the snapshot to build RemoteLogMetadataCache.
            for (RemoteLogSegmentId id : state.unreferencedSegmentIds()) {
                snapshots.add(RemoteLogSegmentMetadataSnapshot.create(idToSegmentMetadata.get(id)));
            }
            
            // Add referenced segments.
            for (RemoteLogSegmentId id : state.referencedSegmentIds()) {
                snapshots.add(RemoteLogSegmentMetadataSnapshot.create(idToSegmentMetadata.get(id)));
            }
        }

        snapshotFile.write(new RemoteLogMetadataSnapshotFile.Snapshot(metadataPartition, metadataPartitionOffset, snapshots));
    }
}
