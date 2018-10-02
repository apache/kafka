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
package org.apache.kafka.streams.kstream.internals.suppress;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.internals.ContextualRecord;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

class InMemoryTimeOrderedKeyValueBuffer implements TimeOrderedKeyValueBuffer {
    private final Map<Bytes, TimeKey> index = new HashMap<>();
    private final TreeMap<TimeKey, ContextualRecord> sortedMap = new TreeMap<>();
    private long memBufferSize = 0L;
    private long minTimestamp = Long.MAX_VALUE;

    @Override
    public void evictWhile(final Supplier<Boolean> predicate,
                           final Consumer<KeyValue<Bytes, ContextualRecord>> callback) {
        final Iterator<Map.Entry<TimeKey, ContextualRecord>> delegate = sortedMap.entrySet().iterator();

        if (predicate.get()) {
            Map.Entry<TimeKey, ContextualRecord> next = null;
            if (delegate.hasNext()) {
                next = delegate.next();
            }

            // predicate being true means we read one record, call the callback, and then remove it
            while (next != null && predicate.get()) {
                callback.accept(new KeyValue<>(next.getKey().key(), next.getValue()));

                delegate.remove();
                index.remove(next.getKey().key());

                memBufferSize = memBufferSize - computeRecordSize(next.getKey().key(), next.getValue());

                // peek at the next record so we can update the minTimestamp
                if (delegate.hasNext()) {
                    next = delegate.next();
                    minTimestamp = next == null ? Long.MAX_VALUE : next.getKey().time();
                } else {
                    next = null;
                    minTimestamp = Long.MAX_VALUE;
                }
            }
        }
    }

    @Override
    public void put(final long time,
                    final Bytes key,
                    final ContextualRecord value) {
        // non-resetting semantics:
        // if there was a previous version of the same record,
        // then insert the new record in the same place in the priority queue

        final TimeKey previousKey = index.get(key);
        if (previousKey == null) {
            final TimeKey nextKey = new TimeKey(time, key);
            index.put(key, nextKey);
            sortedMap.put(nextKey, value);
            minTimestamp = Math.min(minTimestamp, time);
            memBufferSize = memBufferSize + computeRecordSize(key, value);
        } else {
            final ContextualRecord removedValue = sortedMap.put(previousKey, value);
            memBufferSize =
                memBufferSize
                    + computeRecordSize(key, value)
                    - (removedValue == null ? 0 : computeRecordSize(key, removedValue));
        }
    }

    @Override
    public int numRecords() {
        return index.size();
    }

    @Override
    public long bufferSize() {
        return memBufferSize;
    }

    @Override
    public long minTimestamp() {
        return minTimestamp;
    }

    private long computeRecordSize(final Bytes key, final ContextualRecord value) {
        long size = 0L;
        size += 8; // buffer time
        size += key.get().length;
        if (value != null) {
            size += value.sizeBytes();
        }
        return size;
    }
}
