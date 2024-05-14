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
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata.CustomMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;

import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.Optional;

/**
 * This class represents the entry containing the metadata about a remote log segment. This is similar to
 * {@link RemoteLogSegmentMetadata} but it does not contain topic partition information. This class keeps
 * only remote log segment ID but not the topic partition. This class is used in storing the snapshot of
 * remote log metadata for a specific topic partition.
 */
public class RemoteLogSegmentMetadataSnapshot extends RemoteLogMetadata {

    /**
     * Universally unique remote log segment id.
     */
    private final Uuid segmentId;

    /**
     * Start offset of this segment.
     */
    private final long startOffset;

    /**
     * End offset of this segment.
     */
    private final long endOffset;

    /**
     * Maximum timestamp in milliseconds in the segment
     */
    private final long maxTimestampMs;

    /**
     * LeaderEpoch vs offset for messages within this segment.
     */
    private final NavigableMap<Integer, Long> segmentLeaderEpochs;

    /**
     * Size of the segment in bytes.
     */
    private final int segmentSizeInBytes;

    /**
     * Custom metadata.
     */
    private final Optional<CustomMetadata> customMetadata;

    /**
     * It indicates the state in which the action is executed on this segment.
     */
    private final RemoteLogSegmentState state;

    /**
     * Creates an instance with the given metadata of remote log segment.
     * <p>
     * {@code segmentLeaderEpochs} can not be empty. If all the records in this segment belong to the same leader epoch
     * then it should have an entry with epoch mapping to start-offset of this segment.
     *
     * @param segmentId           Universally unique remote log segment id.
     * @param startOffset         Start offset of this segment (inclusive).
     * @param endOffset           End offset of this segment (inclusive).
     * @param maxTimestampMs      Maximum timestamp in milliseconds in this segment.
     * @param brokerId            Broker id from which this event is generated.
     * @param eventTimestampMs    Epoch time in milliseconds at which the remote log segment is copied to the remote tier storage.
     * @param segmentSizeInBytes  Size of this segment in bytes.
     * @param customMetadata      Custom metadata.
     * @param state               State of the respective segment of remoteLogSegmentId.
     * @param segmentLeaderEpochs leader epochs occurred within this segment.
     */
    public RemoteLogSegmentMetadataSnapshot(Uuid segmentId,
                                            long startOffset,
                                            long endOffset,
                                            long maxTimestampMs,
                                            int brokerId,
                                            long eventTimestampMs,
                                            int segmentSizeInBytes,
                                            Optional<CustomMetadata> customMetadata,
                                            RemoteLogSegmentState state,
                                            Map<Integer, Long> segmentLeaderEpochs) {
        super(brokerId, eventTimestampMs);
        this.segmentId = Objects.requireNonNull(segmentId, "remoteLogSegmentId can not be null");
        this.state = Objects.requireNonNull(state, "state can not be null");

        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.maxTimestampMs = maxTimestampMs;
        this.segmentSizeInBytes = segmentSizeInBytes;
        this.customMetadata = Objects.requireNonNull(customMetadata, "customMetadata can not be null");

        if (segmentLeaderEpochs == null || segmentLeaderEpochs.isEmpty()) {
            throw new IllegalArgumentException("segmentLeaderEpochs can not be null or empty");
        }

        this.segmentLeaderEpochs = Collections.unmodifiableNavigableMap(new TreeMap<>(segmentLeaderEpochs));
    }

    public static RemoteLogSegmentMetadataSnapshot create(RemoteLogSegmentMetadata metadata) {
        return new RemoteLogSegmentMetadataSnapshot(metadata.remoteLogSegmentId().id(), metadata.startOffset(), metadata.endOffset(),
                                                    metadata.maxTimestampMs(), metadata.brokerId(), metadata.eventTimestampMs(),
                                                    metadata.segmentSizeInBytes(), metadata.customMetadata(), metadata.state(), metadata.segmentLeaderEpochs()
        );
    }

    /**
     * @return unique id of this segment.
     */
    public Uuid segmentId() {
        return segmentId;
    }

    /**
     * @return Start offset of this segment (inclusive).
     */
    public long startOffset() {
        return startOffset;
    }

    /**
     * @return End offset of this segment (inclusive).
     */
    public long endOffset() {
        return endOffset;
    }

    /**
     * @return Total size of this segment in bytes.
     */
    public int segmentSizeInBytes() {
        return segmentSizeInBytes;
    }

    /**
     * @return Maximum timestamp in milliseconds of a record within this segment.
     */
    public long maxTimestampMs() {
        return maxTimestampMs;
    }

    /**
     * @return Map of leader epoch vs offset for the records available in this segment.
     */
    public NavigableMap<Integer, Long> segmentLeaderEpochs() {
        return segmentLeaderEpochs;
    }

    /**
     * @return Custom metadata.
     */
    public Optional<CustomMetadata> customMetadata() {
        return customMetadata;
    }

    /**
     * Returns the current state of this remote log segment. It can be any of the below
     * <ul>
     *     {@link RemoteLogSegmentState#COPY_SEGMENT_STARTED}
     *     {@link RemoteLogSegmentState#COPY_SEGMENT_FINISHED}
     *     {@link RemoteLogSegmentState#DELETE_SEGMENT_STARTED}
     *     {@link RemoteLogSegmentState#DELETE_SEGMENT_FINISHED}
     * </ul>
     */
    public RemoteLogSegmentState state() {
        return state;
    }

    @Override
    public TopicIdPartition topicIdPartition() {
        throw new UnsupportedOperationException("This metadata does not have topic partition with it.");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RemoteLogSegmentMetadataSnapshot)) return false;
        RemoteLogSegmentMetadataSnapshot that = (RemoteLogSegmentMetadataSnapshot) o;
        return startOffset == that.startOffset
                && endOffset == that.endOffset
                && maxTimestampMs == that.maxTimestampMs
                && segmentSizeInBytes == that.segmentSizeInBytes
                && Objects.equals(customMetadata, that.customMetadata)
                && Objects.equals(segmentId, that.segmentId)
                && Objects.equals(segmentLeaderEpochs, that.segmentLeaderEpochs)
                && state == that.state;
    }

    @Override
    public int hashCode() {
        return Objects.hash(segmentId, startOffset, endOffset, maxTimestampMs, segmentLeaderEpochs, segmentSizeInBytes, customMetadata, state);
    }

    @Override
    public String toString() {
        return "RemoteLogSegmentMetadataSnapshot{" +
                "segmentId=" + segmentId +
                ", startOffset=" + startOffset +
                ", endOffset=" + endOffset +
                ", maxTimestampMs=" + maxTimestampMs +
                ", segmentLeaderEpochs=" + segmentLeaderEpochs +
                ", segmentSizeInBytes=" + segmentSizeInBytes +
                ", customMetadata=" + customMetadata +
                ", state=" + state +
                '}';
    }
}
