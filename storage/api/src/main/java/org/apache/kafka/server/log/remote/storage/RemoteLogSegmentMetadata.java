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

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

/**
 * It describes the metadata about a topic partition's remote log segment in the remote storage. This is uniquely
 * represented with {@link RemoteLogSegmentId}.
 * <p>
 * New instance is always created with the state as {@link RemoteLogSegmentState#COPY_SEGMENT_STARTED}. This can be
 * updated by applying {@link RemoteLogSegmentMetadataUpdate} for the respective {@link RemoteLogSegmentId} of the
 * {@code RemoteLogSegmentMetadata}.
 */
@InterfaceStability.Evolving
public class RemoteLogSegmentMetadata {

    /**
     * Universally unique remote log segment id.
     */
    private final RemoteLogSegmentId remoteLogSegmentId;

    /**
     * Start offset of this segment.
     */
    private final long startOffset;

    /**
     * End offset of this segment.
     */
    private final long endOffset;

    /**
     * Broker id from which this event is generated.
     */
    private final int brokerId;

    /**
     * Maximum timestamp in milli seconds in the segment
     */
    private final long maxTimestampMs;

    /**
     * Epoch time in milli seconds at which the respective {@link #state} is set.
     */
    private final long eventTimestampMs;

    /**
     * LeaderEpoch vs offset for messages within this segment.
     */
    private final NavigableMap<Integer, Long> segmentLeaderEpochs;

    /**
     * Size of the segment in bytes.
     */
    private final int segmentSizeInBytes;

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
     * @param remoteLogSegmentId  Universally unique remote log segment id.
     * @param startOffset         Start offset of this segment (inclusive).
     * @param endOffset           End offset of this segment (inclusive).
     * @param maxTimestampMs      Maximum timestamp in milli seconds in this segment.
     * @param brokerId            Broker id from which this event is generated.
     * @param eventTimestampMs    Epoch time in milli seconds at which the remote log segment is copied to the remote tier storage.
     * @param segmentSizeInBytes  Size of this segment in bytes.
     * @param state               State of the respective segment of remoteLogSegmentId.
     * @param segmentLeaderEpochs leader epochs occurred within this segment.
     */
    private RemoteLogSegmentMetadata(RemoteLogSegmentId remoteLogSegmentId,
                                     long startOffset,
                                     long endOffset,
                                     long maxTimestampMs,
                                     int brokerId,
                                     long eventTimestampMs,
                                     int segmentSizeInBytes,
                                     RemoteLogSegmentState state,
                                     Map<Integer, Long> segmentLeaderEpochs) {
        this.remoteLogSegmentId = Objects.requireNonNull(remoteLogSegmentId, "remoteLogSegmentId can not be null");
        this.state = Objects.requireNonNull(state, "state can not be null");

        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.brokerId = brokerId;
        this.maxTimestampMs = maxTimestampMs;
        this.eventTimestampMs = eventTimestampMs;
        this.segmentSizeInBytes = segmentSizeInBytes;

        if (segmentLeaderEpochs == null || segmentLeaderEpochs.isEmpty()) {
            throw new IllegalArgumentException("segmentLeaderEpochs can not be null or empty");
        }

        this.segmentLeaderEpochs = Collections.unmodifiableNavigableMap(new TreeMap<>(segmentLeaderEpochs));
    }

    /**
     * Creates an instance with the given metadata of remote log segment and its state as {@link RemoteLogSegmentState#COPY_SEGMENT_STARTED}.
     * <p>
     * {@code segmentLeaderEpochs} can not be empty. If all the records in this segment belong to the same leader epoch
     * then it should have an entry with epoch mapping to start-offset of this segment.
     *
     * @param remoteLogSegmentId  Universally unique remote log segment id.
     * @param startOffset         Start offset of this segment (inclusive).
     * @param endOffset           End offset of this segment (inclusive).
     * @param maxTimestampMs      Maximum timestamp in this segment
     * @param brokerId            Broker id from which this event is generated.
     * @param eventTimestampMs    Epoch time in milli seconds at which the remote log segment is copied to the remote tier storage.
     * @param segmentSizeInBytes  Size of this segment in bytes.
     * @param segmentLeaderEpochs leader epochs occurred within this segment
     */
    public RemoteLogSegmentMetadata(RemoteLogSegmentId remoteLogSegmentId,
                                    long startOffset,
                                    long endOffset,
                                    long maxTimestampMs,
                                    int brokerId,
                                    long eventTimestampMs,
                                    int segmentSizeInBytes,
                                    Map<Integer, Long> segmentLeaderEpochs) {
        this(remoteLogSegmentId,
                startOffset,
                endOffset,
                maxTimestampMs,
                brokerId,
                eventTimestampMs, segmentSizeInBytes,
                RemoteLogSegmentState.COPY_SEGMENT_STARTED,
                segmentLeaderEpochs);
    }


    /**
     * @return unique id of this segment.
     */
    public RemoteLogSegmentId remoteLogSegmentId() {
        return remoteLogSegmentId;
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
     * @return Epoch time in milli seconds at which this event is occurred.
     */
    public long eventTimestampMs() {
        return eventTimestampMs;
    }

    /**
     * @return Total size of this segment in bytes.
     */
    public int segmentSizeInBytes() {
        return segmentSizeInBytes;
    }

    /**
     * @return Maximum timestamp in milli seconds of a record within this segment.
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
     * @return Broker id from which this event is generated.
     */
    public int brokerId() {
        return brokerId;
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

    /**
     * Creates a new RemoteLogSegmentMetadata applying the given {@code rlsmUpdate} on this instance. This method will
     * not update this instance.
     *
     * @param rlsmUpdate update to be applied.
     * @return a new instance created by applying the given update on this instance.
     */
    public RemoteLogSegmentMetadata createWithUpdates(RemoteLogSegmentMetadataUpdate rlsmUpdate) {
        if (!remoteLogSegmentId.equals(rlsmUpdate.remoteLogSegmentId())) {
            throw new IllegalArgumentException("Given rlsmUpdate does not have this instance's remoteLogSegmentId.");
        }

        return new RemoteLogSegmentMetadata(remoteLogSegmentId, startOffset,
                endOffset, maxTimestampMs, rlsmUpdate.brokerId(), rlsmUpdate.eventTimestampMs(),
                segmentSizeInBytes, rlsmUpdate.state(), segmentLeaderEpochs);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RemoteLogSegmentMetadata that = (RemoteLogSegmentMetadata) o;
        return startOffset == that.startOffset && endOffset == that.endOffset && brokerId == that.brokerId
               && maxTimestampMs == that.maxTimestampMs && eventTimestampMs == that.eventTimestampMs
               && segmentSizeInBytes == that.segmentSizeInBytes
               && Objects.equals(remoteLogSegmentId, that.remoteLogSegmentId)
               && Objects.equals(segmentLeaderEpochs, that.segmentLeaderEpochs) && state == that.state;
    }

    @Override
    public int hashCode() {
        return Objects.hash(remoteLogSegmentId, startOffset, endOffset, brokerId, maxTimestampMs,
                eventTimestampMs, segmentLeaderEpochs, segmentSizeInBytes, state);
    }

    @Override
    public String toString() {
        return "RemoteLogSegmentMetadata{" +
               "remoteLogSegmentId=" + remoteLogSegmentId +
               ", startOffset=" + startOffset +
               ", endOffset=" + endOffset +
               ", brokerId=" + brokerId +
               ", maxTimestampMs=" + maxTimestampMs +
               ", eventTimestampMs=" + eventTimestampMs +
               ", segmentLeaderEpochs=" + segmentLeaderEpochs +
               ", segmentSizeInBytes=" + segmentSizeInBytes +
               ", state=" + state +
               '}';
    }

}
