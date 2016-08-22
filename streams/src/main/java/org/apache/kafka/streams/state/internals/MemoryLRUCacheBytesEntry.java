/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.processor.RecordContext;

/**
 * A cache entry
 */
public class MemoryLRUCacheBytesEntry<K, V> implements RecordContext {

    public final V value;

    public final K key;
    private final long offset;
    private final long timestamp;
    private final String topic;
    public boolean isDirty;
    private final int partition;
    private long sizeBytes = 0;


    public MemoryLRUCacheBytesEntry(final K key, final V value, long sizeBytes) {
        this(key, value, sizeBytes, false, -1, -1, -1, null);
    }

    public MemoryLRUCacheBytesEntry(final K key, final V value, final long sizeBytes, final boolean isDirty,
                                    final long offset, final long timestamp, final int partition,
                                    final String topic) {
        this.key = key;
        this.value = value;
        this.sizeBytes = sizeBytes;
        this.isDirty = isDirty;
        this.partition = partition;
        this.topic = topic;
        this.offset = offset;
        this.timestamp = timestamp;
    }



    public void markClean() {
        isDirty = false;
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
    public String topic() {
        return topic;
    }

    @Override
    public int partition() {
        return partition;
    }

    public boolean isDirty() {
        return isDirty;
    }

    public long size() {
        return sizeBytes;
    }


}
