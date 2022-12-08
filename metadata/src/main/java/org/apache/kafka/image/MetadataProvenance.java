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

import java.util.Objects;


/**
 * Information about the source of a metadata image.
 */
public final class MetadataProvenance {
    public static final MetadataProvenance EMPTY = new MetadataProvenance(-1L, -1, -1L);

    private final long offset;
    private final int epoch;
    private final long lastContainedLogTimeMs;

    public MetadataProvenance(
        long offset,
        int epoch,
        long lastContainedLogTimeMs
    ) {
        this.offset = offset;
        this.epoch = epoch;
        this.lastContainedLogTimeMs = lastContainedLogTimeMs;
    }

    public OffsetAndEpoch offsetAndEpoch() {
        return new OffsetAndEpoch(offset, epoch);
    }

    public long offset() {
        return offset;
    }

    public int epoch() {
        return epoch;
    }

    public long lastContainedLogTimeMs() {
        return lastContainedLogTimeMs;
    }

    /**
     * Returns the name that a snapshot with this provenance would have.
     */
    public String snapshotName() {
        return String.format("snapshot %020d-%010d", offset, epoch);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !o.getClass().equals(this.getClass())) return false;
        MetadataProvenance other = (MetadataProvenance) o;
        return offset == other.offset &&
            epoch == other.epoch &&
            lastContainedLogTimeMs == other.lastContainedLogTimeMs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset,
            epoch,
            lastContainedLogTimeMs);
    }

    @Override
    public String toString() {
        return "MetadataProvenance(" +
            "offset=" + offset +
            ", epoch=" + epoch +
            ", lastContainedLogTimeMs=" + lastContainedLogTimeMs +
            ")";
    }
}
