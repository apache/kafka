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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;

import java.util.Objects;

/**
 * A cache entry
 */
class LRUCacheEntry {
    private final ContextualRecord record;
    private final long sizeBytes;
    private boolean isDirty;


    LRUCacheEntry(final byte[] value) {
        this(value, new RecordHeaders(), false, -1, -1, -1, "");
    }

    LRUCacheEntry(final byte[] value,
                  final Headers headers,
                  final boolean isDirty,
                  final long offset,
                  final long timestamp,
                  final int partition,
                  final String topic) {
        final ProcessorRecordContext context = new ProcessorRecordContext(timestamp, offset, partition, topic, headers);

        this.record = new ContextualRecord(
            value,
            context
        );

        this.isDirty = isDirty;
        this.sizeBytes = 1 + // isDirty
            record.residentMemorySizeEstimate();
    }

    void markClean() {
        isDirty = false;
    }

    boolean isDirty() {
        return isDirty;
    }

    long size() {
        return sizeBytes;
    }

    byte[] value() {
        return record.value();
    }

    public ProcessorRecordContext context() {
        return record.recordContext();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final LRUCacheEntry that = (LRUCacheEntry) o;
        return sizeBytes == that.sizeBytes &&
            isDirty() == that.isDirty() &&
            Objects.equals(record, that.record);
    }

    @Override
    public int hashCode() {
        return Objects.hash(record, sizeBytes, isDirty());
    }
}
