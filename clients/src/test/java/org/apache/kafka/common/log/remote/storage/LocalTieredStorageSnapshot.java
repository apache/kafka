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
package org.apache.kafka.common.log.remote.storage;

import org.apache.kafka.common.*;
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentFileset.*;

import java.io.*;
import java.util.*;
import java.util.stream.*;

import static java.lang.String.*;

/**
 * A capture of the content of the local tiered storage at a point in time.
 */
public final class LocalTieredStorageSnapshot {

    public static LocalTieredStorageSnapshot takeSnapshot(final LocalTieredStorage storage) {
        Snapshot snapshot = new Snapshot();
        storage.traverse(snapshot);
        return new LocalTieredStorageSnapshot(snapshot);
    }

    public List<TopicPartition> getTopicPartitions() {
        return Collections.unmodifiableList(snapshot.topicPartitions);
    }

    public List<RemoteLogSegmentFileset> getFilesets(final TopicPartition topicPartition) {
        return snapshot.records.values().stream()
                .filter(fileset -> fileset.getRemoteLogSegmentId().topicPartition().equals(topicPartition))
                .collect(Collectors.toList());
    }

    public int size() {
        return snapshot.records.size();
    }

    public File getFile(final RemoteLogSegmentId id, final RemoteLogSegmentFileType type) {
        final RemoteLogSegmentFileset fileset = snapshot.records.get(id);
        if (fileset == null) {
            throw new IllegalArgumentException(String.format("No file found for id: %s", id));
        }

        return fileset.getFile(type);
    }

    private final Snapshot snapshot;

    private LocalTieredStorageSnapshot(final Snapshot snapshot) {
        Objects.requireNonNull(this.snapshot = snapshot);
    }

    private static final class Snapshot implements LocalTieredStorageTraverser {
        private final Map<RemoteLogSegmentId, RemoteLogSegmentFileset> records = new HashMap<>();
        private final List<TopicPartition> topicPartitions = new ArrayList<>();

        @Override
        public void visitTopicPartition(TopicPartition topicPartition) {
            if (topicPartitions.contains(topicPartition)) {
                throw new IllegalStateException(format("Topic-partition %s was already visited", topicPartition));
            }

            this.topicPartitions.add(topicPartition);
        }

        @Override
        public void visitSegment(RemoteLogSegmentFileset fileset) {
            if (records.containsKey(fileset)) {
                throw new IllegalStateException(format("Segment with id %s was already visited", fileset));
            }

            records.put(fileset.getRemoteLogSegmentId(), fileset);
        }
    }
}
