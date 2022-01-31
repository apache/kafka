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
package org.apache.kafka.server.log.remote.storage;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentFileset.RemoteLogSegmentFileType;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.lang.String.format;

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
        final List<TopicPartition> topicPartitions = snapshot.topicIdPartitions.stream()
                .map(TopicIdPartition::topicPartition)
                .collect(Collectors.toList());
        return Collections.unmodifiableList(topicPartitions);
    }

    public List<RemoteLogSegmentFileset> getFilesets(final TopicPartition topicPartition) {
        return snapshot.records.values().stream()
                .filter(fileset -> fileset.getRemoteLogSegmentId().topicIdPartition().topicPartition().equals(topicPartition))
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

    public String toString() {
        return snapshot.records.values().stream().map(Object::toString).reduce("", (s1, s2) -> s1 + s2);
    }

    private final Snapshot snapshot;

    private LocalTieredStorageSnapshot(final Snapshot snapshot) {
        Objects.requireNonNull(this.snapshot = snapshot);
    }

    private static final class Snapshot implements LocalTieredStorageTraverser {
        private final Map<RemoteLogSegmentId, RemoteLogSegmentFileset> records = new HashMap<>();
        private final List<TopicIdPartition> topicIdPartitions = new ArrayList<>();

        @Override
        public void visitTopicIdPartition(TopicIdPartition topicIdPartition) {
            if (topicIdPartitions.contains(topicIdPartition)) {
                throw new IllegalStateException(format("Topic-partition %s was already visited", topicIdPartition));
            }

            this.topicIdPartitions.add(topicIdPartition);
        }

        @Override
        public void visitSegment(RemoteLogSegmentFileset fileset) {
            if (records.containsKey(fileset.getRemoteLogSegmentId())) {
                throw new IllegalStateException(format("Segment with id %s was already visited", fileset.getRemoteLogSegmentId()));
            }

            records.put(fileset.getRemoteLogSegmentId(), fileset);
        }
    }
}
