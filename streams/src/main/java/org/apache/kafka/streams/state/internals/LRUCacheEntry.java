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
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;

import java.util.Arrays;
import java.util.Objects;

/**
 * A cache entry
 */
class LRUCacheEntry extends ProcessorRecordContext {

    private final byte[] value;
    private final long sizeBytes;
    private boolean isDirty;

    LRUCacheEntry(final byte[] value) {
        this(value, null, false, -1, -1, -1, "");
    }

    LRUCacheEntry(final byte[] value,
                  final Headers headers,
                  final boolean isDirty,
                  final long offset,
                  final long timestamp,
                  final int partition,
                  final String topic) {
        super(timestamp, offset, partition, topic, headers);
        this.value = value;
        this.isDirty = isDirty;
        this.sizeBytes = (value == null ? 0 : value.length) +
                1 + // isDirty
                8 + // timestamp
                8 + // offset
                4 + // partition
                (topic == null ? 0 : topic.length());
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
        return value;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final LRUCacheEntry that = (LRUCacheEntry) o;
        return timestamp() == that.timestamp() &&
                offset() == that.offset() &&
                partition() == that.partition() &&
                Objects.equals(topic(), that.topic()) &&
                Objects.equals(headers(), that.headers()) &&
                Arrays.equals(this.value, that.value()) &&
                this.isDirty == that.isDirty();
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp(), offset(), topic(), partition(), headers(), value, isDirty);
    }
}
