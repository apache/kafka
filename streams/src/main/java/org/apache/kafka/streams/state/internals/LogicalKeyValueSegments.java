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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.ProcessorContextUtils;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecorder;

import java.util.HashMap;
import java.util.Map;

/**
 * A {@link Segments} implementation which uses a single underlying RocksDB instance.
 * Regular segments with {@code segmentId >= 0} expire according to the specified
 * retention period. "Reserved" segments with {@code segmentId < 0} do not expire
 * and are completely separate from regular segments in that methods such as
 * {@link #segmentForTimestamp(long)}, {@link #getOrCreateSegment(long, StateStoreContext)},
 * {@link #getOrCreateSegmentIfLive(long, StateStoreContext, long)},
 * {@link #segments(long, long, boolean)}, and {@link #allSegments(boolean)}
 * only return regular segments and not reserved segments. The methods {@link #flush()}
 * and {@link #close()} flush and close both regular and reserved segments, due to
 * the fact that both types of segments share the same physical RocksDB instance.
 * To create a reserved segment, use {@link #createReservedSegment(long, String)} instead.
 */
public class LogicalKeyValueSegments extends AbstractSegments<LogicalKeyValueSegment> {

    private final RocksDBMetricsRecorder metricsRecorder;
    private final RocksDBStore physicalStore;

    // reserved segments do not expire, and are tracked here separately from regular segments
    private final Map<Long, LogicalKeyValueSegment> reservedSegments = new HashMap<>();

    LogicalKeyValueSegments(final String name,
                            final String parentDir,
                            final long retentionPeriod,
                            final long segmentInterval,
                            final RocksDBMetricsRecorder metricsRecorder) {
        super(name, retentionPeriod, segmentInterval);
        this.metricsRecorder = metricsRecorder;
        this.physicalStore = new RocksDBStore(name, parentDir, metricsRecorder, false);
    }

    @Override
    public void setPosition(final Position position) {
        this.physicalStore.position = position;
    }

    @Override
    public LogicalKeyValueSegment getOrCreateSegment(final long segmentId,
                                                     final StateStoreContext context) {
        if (segments.containsKey(segmentId)) {
            return segments.get(segmentId);
        } else {
            if (segmentId < 0) {
                throw new IllegalArgumentException(
                    "Negative segment IDs are reserved for reserved segments, "
                        + "and should be created through createReservedSegment() instead");
            }

            final LogicalKeyValueSegment newSegment = new LogicalKeyValueSegment(segmentId, segmentName(segmentId), physicalStore);

            if (segments.put(segmentId, newSegment) != null) {
                throw new IllegalStateException("LogicalKeyValueSegment already exists. Possible concurrent access.");
            }

            return newSegment;
        }
    }

    LogicalKeyValueSegment createReservedSegment(final long segmentId,
                                                 final String segmentName) {
        if (segmentId >= 0) {
            throw new IllegalArgumentException("segmentId for a reserved segment must be negative");
        }

        final LogicalKeyValueSegment newSegment = new LogicalKeyValueSegment(segmentId, segmentName, physicalStore);

        if (reservedSegments.put(segmentId, newSegment) != null) {
            throw new IllegalStateException("LogicalKeyValueSegment already exists.");
        }

        return newSegment;
    }

    // VisibleForTesting
    LogicalKeyValueSegment getReservedSegment(final long segmentId) {
        return reservedSegments.get(segmentId);
    }

    @Override
    public void openExisting(final StateStoreContext context, final long streamTime) {
        metricsRecorder.init(ProcessorContextUtils.metricsImpl(context), context.taskId());
        physicalStore.openDB(context.appConfigs(), context.stateDir());
    }

    @Override
    public void cleanupExpiredSegments(final long streamTime) {
        super.cleanupExpiredSegments(streamTime);
    }

    @Override
    public void flush() {
        physicalStore.flush();
    }

    @Override
    public void close() {
        // close the logical segments first to close any open iterators
        super.close();

        // same for reserved segments
        for (final LogicalKeyValueSegment segment : reservedSegments.values()) {
            segment.close();
        }
        reservedSegments.clear();

        physicalStore.close();
    }

    @Override
    public String segmentName(final long segmentId) {
        if (segmentId < 0) {
            throw new IllegalArgumentException(
                "Negative segment IDs are reserved for reserved segments, "
                    + "which have custom names that should not be accessed from this method");
        }

        return super.segmentName(segmentId);
    }
}