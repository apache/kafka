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

package org.apache.kafka.metadata.loader;

import java.util.Objects;


/**
 * Contains information about a MetadataDelta that has been loaded by the MetadataLoader.
 */
public class LoadInformation {
    /**
     * The timestamp of the last batch that was part of the delta.
     */
    private final long lastContainedTimestamp;

    /**
     * The base offset that the delta was laoded from, or the snapshot offset if we loaded a
     * snapshot.
     */
    private final long baseOffset;

    /**
     * The last offset contained in the delta, or the snapshot offset if we loaded a snapshot.
     */
    private final long lastContainedOffset;

    /**
     * The number of batches that were loaded in the delta.
     */
    private final int numBatches;

    /**
     * The number of records that were loaded in the delta.
     */
    private final int numRecords;

    /**
     * The time in microseconds that it took to load the delta.
     */
    private final long elapsedUs;

    /**
     * The total size of the records in bytes that we read while creating the delta.
     */
    private final long numBytes;

    /**
     * True only if we loaded the delta from a snapshot.
     */
    private final boolean fromSnapshot;

    public LoadInformation(
        long lastContainedTimestamp,
        long baseOffset,
        long lastContainedOffset,
        int numBatches,
        int numRecords,
        long elapsedUs,
        long numBytes,
        boolean fromSnapshot
    ) {
        this.lastContainedTimestamp = lastContainedTimestamp;
        this.baseOffset = baseOffset;
        this.lastContainedOffset = lastContainedOffset;
        this.numBatches = numBatches;
        this.numRecords = numRecords;
        this.elapsedUs = elapsedUs;
        this.numBytes = numBytes;
        this.fromSnapshot = fromSnapshot;
    }

    public long lastContainedTimestamp() {
        return lastContainedTimestamp;
    }

    public long baseOffset() {
        return baseOffset;
    }

    public long lastContainedOffset() {
        return lastContainedOffset;
    }

    public int numBatches() {
        return numBatches;
    }

    public int numRecords() {
        return numRecords;
    }

    public long elapsedUs() {
        return elapsedUs;
    }

    public long numBytes() {
        return numBytes;
    }

    public boolean fromSnapshot() {
        return fromSnapshot;
    }

    @Override
    public int hashCode() {
        return Objects.hash(lastContainedTimestamp,
                baseOffset,
                lastContainedOffset,
                numBatches,
                numRecords,
                elapsedUs,
                numBytes,
                fromSnapshot);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !o.getClass().equals(this.getClass())) return false;
        LoadInformation other = (LoadInformation) o;
        return lastContainedTimestamp == other.lastContainedTimestamp &&
                baseOffset == other.baseOffset &&
                lastContainedOffset == other.lastContainedOffset &&
                numBatches == other.numBatches &&
                numRecords == other.numRecords &&
                elapsedUs == other.elapsedUs &&
                numBytes == other.numBytes &&
                fromSnapshot == other.fromSnapshot;
    }

    @Override
    public String toString() {
        return "LoadInformation(" +
                "lastContainedTimestamp=" + lastContainedTimestamp +
                ", baseOffset=" + baseOffset +
                ", lastContainedOffset=" + lastContainedOffset +
                ", numBatches=" + numBatches +
                ", numRecords=" + numRecords +
                ", elapsedUs=" + elapsedUs +
                ", numBytes=" + numBytes +
                ", fromSnapshot=" + fromSnapshot +
                ")";
    }
}
