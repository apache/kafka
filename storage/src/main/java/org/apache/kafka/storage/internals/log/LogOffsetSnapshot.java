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
package org.apache.kafka.storage.internals.log;

import java.util.Objects;

/**
 * Container class which represents a snapshot of the significant offsets for a partition. This allows fetching
 * of these offsets atomically without the possibility of a leader change affecting their consistency relative
 * to each other. See {@link UnifiedLog#fetchOffsetSnapshot()}.
 */
public class LogOffsetSnapshot {

    public final long logStartOffset;
    public final LogOffsetMetadata logEndOffset;
    public final LogOffsetMetadata highWatermark;
    public final LogOffsetMetadata lastStableOffset;

    public LogOffsetSnapshot(long logStartOffset,
                             LogOffsetMetadata logEndOffset,
                             LogOffsetMetadata highWatermark,
                             LogOffsetMetadata lastStableOffset) {

        this.logStartOffset = logStartOffset;
        this.logEndOffset = logEndOffset;
        this.highWatermark = highWatermark;
        this.lastStableOffset = lastStableOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LogOffsetSnapshot that = (LogOffsetSnapshot) o;

        return logStartOffset == that.logStartOffset &&
                Objects.equals(logEndOffset, that.logEndOffset) &&
                Objects.equals(highWatermark, that.highWatermark) &&
                Objects.equals(lastStableOffset, that.lastStableOffset);
    }

    @Override
    public int hashCode() {
        int result = (int) (logStartOffset ^ (logStartOffset >>> 32));
        result = 31 * result + (logEndOffset != null ? logEndOffset.hashCode() : 0);
        result = 31 * result + (highWatermark != null ? highWatermark.hashCode() : 0);
        result = 31 * result + (lastStableOffset != null ? lastStableOffset.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "LogOffsetSnapshot(" +
                "logStartOffset=" + logStartOffset +
                ", logEndOffset=" + logEndOffset +
                ", highWatermark=" + highWatermark +
                ", lastStableOffset=" + lastStableOffset +
                ')';
    }
}
