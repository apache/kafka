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
import org.apache.kafka.streams.processor.internals.RecordContext;

/**
 * A cache entry
 */
class LRUCacheEntry implements RecordContext {

    public final byte[] value;
    private final Headers headers;
    private final long offset;
    private final String topic;
    private final int partition;
    private long timestamp;

    private long sizeBytes;
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
        this.value = value;
        this.headers = headers;
        this.partition = partition;
        this.topic = topic;
        this.offset = offset;
        this.isDirty = isDirty;
        this.timestamp = timestamp;
        this.sizeBytes = (value == null ? 0 : value.length) +
                1 + // isDirty
                8 + // timestamp
                8 + // offset
                4 + // partition
                (topic == null ? 0 : topic.length());
    }

    @Override
    public long offset() {
        return offset;
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    @Override
    public void setTimestamp(final long timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public int partition() {
        return partition;
    }

    @Override
    public Headers headers() {
        return headers;
    }

    void markClean() {
        isDirty = false;
    }

    boolean isDirty() {
        return isDirty;
    }

    public long size() {
        return sizeBytes;
    }


}
