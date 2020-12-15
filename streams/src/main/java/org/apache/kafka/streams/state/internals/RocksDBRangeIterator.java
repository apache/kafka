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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.rocksdb.RocksIterator;

import java.util.Comparator;
import java.util.Set;

class RocksDBRangeIterator extends RocksDbIterator {
    // RocksDB's JNI interface does not expose getters/setters that allow the
    // comparator to be pluggable, and the default is lexicographic, so it's
    // safe to just force lexicographic comparator here for now.
    private final Comparator<byte[]> comparator = Bytes.BYTES_LEXICO_COMPARATOR;
    private final byte[] rawLastKey;
    private final boolean forward;

    RocksDBRangeIterator(final String storeName,
                         final RocksIterator iter,
                         final Set<KeyValueIterator<Bytes, byte[]>> openIterators,
                         final Bytes from,
                         final Bytes to,
                         final boolean forward) {
        super(storeName, iter, openIterators, forward);
        this.forward = forward;
        if (forward) {
            iter.seek(from.get());
            rawLastKey = to.get();
            if (rawLastKey == null) {
                throw new NullPointerException("RocksDBRangeIterator: RawLastKey is null for key " + to);
            }
        } else {
            iter.seekForPrev(to.get());
            rawLastKey = from.get();
            if (rawLastKey == null) {
                throw new NullPointerException("RocksDBRangeIterator: RawLastKey is null for key " + from);
            }
        }
    }

    @Override
    public KeyValue<Bytes, byte[]> makeNext() {
        final KeyValue<Bytes, byte[]> next = super.makeNext();
        if (next == null) {
            return allDone();
        } else {
            if (forward) {
                if (comparator.compare(next.key.get(), rawLastKey) <= 0) {
                    return next;
                } else {
                    return allDone();
                }
            } else {
                if (comparator.compare(next.key.get(), rawLastKey) >= 0) {
                    return next;
                } else {
                    return allDone();
                }
            }
        }
    }
}

