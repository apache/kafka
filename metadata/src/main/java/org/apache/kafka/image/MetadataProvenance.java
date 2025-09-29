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

package org.apache.kafka.image;

import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.snapshot.Snapshots;

import java.util.Objects;


/**
 * Information about the source of a metadata image.
 */
public final class MetadataProvenance {
    public static final MetadataProvenance EMPTY = new MetadataProvenance(-1L, -1, -1L, false);

    private final long lastContainedOffset;
    private final int lastContainedEpoch;
    private final long lastContainedLogTimeMs;
    private final boolean isOffsetBatchAligned;

    public MetadataProvenance(
        long lastContainedOffset,
        int lastContainedEpoch,
        long lastContainedLogTimeMs,
        boolean isOffsetBatchAligned
    ) {
        this.lastContainedOffset = lastContainedOffset;
        this.lastContainedEpoch = lastContainedEpoch;
        this.lastContainedLogTimeMs = lastContainedLogTimeMs;
        this.isOffsetBatchAligned = isOffsetBatchAligned;
    }

    public OffsetAndEpoch snapshotId() {
        return new OffsetAndEpoch(lastContainedOffset + 1, lastContainedEpoch);
    }

    public long lastContainedOffset() {
        return lastContainedOffset;
    }

    public int lastContainedEpoch() {
        return lastContainedEpoch;
    }

    public long lastContainedLogTimeMs() {
        return lastContainedLogTimeMs;
    }

    /**
     * Returns whether lastContainedOffset is the last offset in a record batch
     */
    public boolean isOffsetBatchAligned() {
        return isOffsetBatchAligned;
    }

    /**
     * Returns the name that a snapshot with this provenance would have.
     */
    public String snapshotName() {
        return String.format("snapshot %s", Snapshots.filenameFromSnapshotId(snapshotId()));
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !o.getClass().equals(this.getClass())) return false;
        MetadataProvenance other = (MetadataProvenance) o;
        return lastContainedOffset == other.lastContainedOffset &&
            lastContainedEpoch == other.lastContainedEpoch &&
            lastContainedLogTimeMs == other.lastContainedLogTimeMs &&
            isOffsetBatchAligned == other.isOffsetBatchAligned;
    }

    @Override
    public int hashCode() {
        return Objects.hash(lastContainedOffset,
            lastContainedEpoch,
            lastContainedLogTimeMs,
            isOffsetBatchAligned);
    }

    @Override
    public String toString() {
        return "MetadataProvenance(" +
            "lastContainedOffset=" + lastContainedOffset +
            ", lastContainedEpoch=" + lastContainedEpoch +
            ", lastContainedLogTimeMs=" + lastContainedLogTimeMs +
            ", isOffsetBatchAligned=" + isOffsetBatchAligned +
            ")";
    }
}
