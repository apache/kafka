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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tracks the records generated as part of a ZK migration and creates a textual summary.
 */
public class MigrationSummary {
    private final Time time;
    private final long startTimeNanos;
    private final Map<MetadataRecordType, Integer> counts = new HashMap<>();
    private int batches = 0;
    private int total = 0;
    private long endTimeNanos = 0;

    MigrationSummary(Time time) {
        this.time = time;
        this.startTimeNanos = time.nanoseconds();
    }

    public void acceptBatch(List<ApiMessageAndVersion> recordBatch) {
        batches ++;
        recordBatch.forEach(apiMessageAndVersion -> {
            MetadataRecordType type = MetadataRecordType.fromId(apiMessageAndVersion.message().apiKey());
            counts.merge(type, 1, (__, count) -> count + 1);
            total ++;
        });
    }

    public void close() {
        if (endTimeNanos == 0) {
            endTimeNanos = time.nanoseconds();
        }
    }

    public long durationMs() {
        if (endTimeNanos == 0) {
            return -1;
        } else {
            return TimeUnit.NANOSECONDS.toMillis(endTimeNanos - startTimeNanos);
        }
    }

    public String toString() {
        return String.format("%d records were generated in %d ms across %d batches. The record types were %s", total, durationMs(), batches, counts);
    }
}
