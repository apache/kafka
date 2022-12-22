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

package org.apache.kafka.image.loader;

import org.apache.kafka.image.MetadataProvenance;

import java.util.Objects;


/**
 * Contains information about a set of changes that were loaded from the metadata log.
 */
public class LogDeltaManifest {
    /**
     * The highest offset and epoch included in this delta, inclusive.
     */
    private final MetadataProvenance provenance;

    /**
     * The number of batches that were loaded.
     */
    private final int numBatches;

    /**
     * The time in nanoseconds that it took to load the changes.
     */
    private final long elapsedNs;

    /**
     * The total size of the records in bytes that we read while creating the delta.
     */
    private final long numBytes;

    public LogDeltaManifest(
        MetadataProvenance provenance,
        int numBatches,
        long elapsedNs,
        long numBytes
    ) {
        this.provenance = provenance;
        this.numBatches = numBatches;
        this.elapsedNs = elapsedNs;
        this.numBytes = numBytes;
    }


    public MetadataProvenance provenance() {
        return provenance;
    }

    public int numBatches() {
        return numBatches;
    }

    public long elapsedNs() {
        return elapsedNs;
    }

    public long numBytes() {
        return numBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                provenance,
                numBatches,
                elapsedNs,
                numBytes);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !o.getClass().equals(this.getClass())) return false;
        LogDeltaManifest other = (LogDeltaManifest) o;
        return provenance.equals(other.provenance) &&
                numBatches == other.numBatches &&
                elapsedNs == other.elapsedNs &&
                numBytes == other.numBytes;
    }

    @Override
    public String toString() {
        return "LogDeltaManifest(" +
                "provenance=" + provenance +
                ", numBatches=" + numBatches +
                ", elapsedNs=" + elapsedNs +
                ", numBytes=" + numBytes +
                ")";
    }
}
