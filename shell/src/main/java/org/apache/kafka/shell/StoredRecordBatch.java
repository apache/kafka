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

package org.apache.kafka.shell;

import org.apache.kafka.raft.Batch;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;


/**
 * Maintains the in-memory metadata for the metadata tool.
 */
public final class StoredRecordBatch {
    private final long baseOffset;
    private final List<ApiMessageAndVersion> records;
    private final long appendTimestamp;

    public static StoredRecordBatch fromRaftBatch(Batch<ApiMessageAndVersion> batch) {
        return new StoredRecordBatch(batch.baseOffset(),
            batch.records(),
            batch.appendTimestamp());
    }

    public StoredRecordBatch(
            long baseOffset,
            List<ApiMessageAndVersion> records,
            long appendTimestamp
    ) {
        this.baseOffset = baseOffset;
        this.records = records;
        this.appendTimestamp = appendTimestamp;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("[offset ").append(baseOffset).append(" at ");
        builder.append(DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(appendTimestamp)));
        builder.append(String.format("]%n"));
        for (ApiMessageAndVersion messageAndVersion : records) {
            builder.append("    [").append((int) messageAndVersion.version()).append("] ");
            builder.append(messageAndVersion.message()).append(String.format("%n"));
        }
        return builder.toString();
    }
}
