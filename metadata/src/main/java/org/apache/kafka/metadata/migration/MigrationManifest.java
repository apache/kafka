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

package org.apache.kafka.metadata.migration;

import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * Holds the results of a ZK to KRaft metadata migration. The {@link #toString()} can be used to provide a
 * human-readable summary of the migration.
 */
public class MigrationManifest {

    public static class Builder {
        private final Time time;
        private final long startTimeNanos;
        private final Map<MetadataRecordType, Integer> counts = new HashMap<>();
        private int batches = 0;
        private int total = 0;
        private long endTimeNanos = 0;

        Builder(Time time) {
            this.time = time;
            this.startTimeNanos = time.nanoseconds();
        }

        public void acceptBatch(List<ApiMessageAndVersion> recordBatch) {
            batches++;
            recordBatch.forEach(apiMessageAndVersion -> {
                MetadataRecordType type = MetadataRecordType.fromId(apiMessageAndVersion.message().apiKey());
                counts.merge(type, 1, Integer::sum);
                total++;
            });
        }

        public MigrationManifest build() {
            if (endTimeNanos == 0) {
                endTimeNanos = time.nanoseconds();
            }
            Map<MetadataRecordType, Integer> orderedCounts = new TreeMap<>(counts);
            return new MigrationManifest(total, batches, endTimeNanos - startTimeNanos, orderedCounts);
        }
    }

    private final int totalRecords;
    private final int totalBatches;
    private final long durationNanos;
    private final Map<MetadataRecordType, Integer> recordTypeCounts;

    MigrationManifest(
        int totalRecords,
        int totalBatches,
        long durationNanos,
        Map<MetadataRecordType, Integer> recordTypeCounts
    ) {
        this.totalRecords = totalRecords;
        this.totalBatches = totalBatches;
        this.durationNanos = durationNanos;
        this.recordTypeCounts = Collections.unmodifiableMap(recordTypeCounts);
    }

    public static Builder newBuilder(Time time) {
        return new Builder(time);
    }

    public long durationMs() {
        return TimeUnit.NANOSECONDS.toMillis(durationNanos);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MigrationManifest that = (MigrationManifest) o;
        return totalRecords == that.totalRecords &&
            totalBatches == that.totalBatches &&
            durationNanos == that.durationNanos &&
            recordTypeCounts.equals(that.recordTypeCounts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(totalRecords, totalBatches, durationNanos, recordTypeCounts);
    }

    public String toString() {
        return String.format("%d records were generated in %d ms across %d batches. The record types were %s",
            totalRecords, durationMs(), totalBatches, recordTypeCounts);
    }
}