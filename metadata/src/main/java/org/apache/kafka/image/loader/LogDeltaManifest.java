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
import org.apache.kafka.raft.LeaderAndEpoch;

import java.util.Objects;


/**
 * Contains information about a set of changes that were loaded from the metadata log.
 */
public class LogDeltaManifest implements LoaderManifest {

    public static class Builder {
        private MetadataProvenance provenance;
        private LeaderAndEpoch leaderAndEpoch;
        private int numBatches = -1;
        private long elapsedNs = -1L;
        private long numBytes = -1L;

        public Builder provenance(MetadataProvenance provenance) {
            this.provenance = provenance;
            return this;
        }

        public Builder leaderAndEpoch(LeaderAndEpoch leaderAndEpoch) {
            this.leaderAndEpoch = leaderAndEpoch;
            return this;
        }

        public Builder numBatches(int numBatches) {
            this.numBatches = numBatches;
            return this;
        }

        public Builder elapsedNs(long elapsedNs) {
            this.elapsedNs = elapsedNs;
            return this;
        }

        public Builder numBytes(long numBytes) {
            this.numBytes = numBytes;
            return this;
        }

        public LogDeltaManifest build() {
            if (provenance == null) {
                throw new RuntimeException("provenance must not be null");
            }
            if (leaderAndEpoch == null) {
                throw new RuntimeException("leaderAndEpoch must not be null");
            }
            if (numBatches == -1) {
                throw new RuntimeException("numBatches must not be null");
            }
            if (elapsedNs == -1L) {
                throw new RuntimeException("elapsedNs must not be null");
            }
            if (numBytes == -1L) {
                throw new RuntimeException("numBytes must not be null");
            }
            return new LogDeltaManifest(provenance, leaderAndEpoch, numBatches, elapsedNs, numBytes);
        }
    }

    /**
     * The highest offset and epoch included in this delta, inclusive.
     */
    private final MetadataProvenance provenance;

    /**
     * The current leader and epoch at the end of this delta.
     */
    private final LeaderAndEpoch leaderAndEpoch;

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

    LogDeltaManifest(
        MetadataProvenance provenance,
        LeaderAndEpoch leaderAndEpoch,
        int numBatches,
        long elapsedNs,
        long numBytes
    ) {
        this.provenance = provenance;
        this.leaderAndEpoch = leaderAndEpoch;
        this.numBatches = numBatches;
        this.elapsedNs = elapsedNs;
        this.numBytes = numBytes;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public LoaderManifestType type() {
        return LoaderManifestType.LOG_DELTA;
    }

    @Override
    public MetadataProvenance provenance() {
        return provenance;
    }

    public LeaderAndEpoch leaderAndEpoch() {
        return leaderAndEpoch;
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
                leaderAndEpoch,
                numBatches,
                elapsedNs,
                numBytes);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !o.getClass().equals(this.getClass())) return false;
        LogDeltaManifest other = (LogDeltaManifest) o;
        return provenance.equals(other.provenance) &&
                leaderAndEpoch == other.leaderAndEpoch &&
                numBatches == other.numBatches &&
                elapsedNs == other.elapsedNs &&
                numBytes == other.numBytes;
    }

    @Override
    public String toString() {
        return "LogDeltaManifest(" +
                "provenance=" + provenance +
                ", leaderAndEpoch=" + leaderAndEpoch +
                ", numBatches=" + numBatches +
                ", elapsedNs=" + elapsedNs +
                ", numBytes=" + numBytes +
                ")";
    }
}
