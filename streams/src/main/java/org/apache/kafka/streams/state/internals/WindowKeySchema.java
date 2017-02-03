/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;

import java.util.List;

class WindowKeySchema implements RocksDBSegmentedBytesStore.KeySchema {
    private static final HasNextCondition ITERATOR_HAS_NEXT = new HasNextCondition() {
        @Override
        public boolean hasNext(final KeyValueIterator<Bytes, ?> iterator) {
            return iterator.hasNext();
        }
    };
    private final StateSerdes<Bytes, byte[]> serdes = new StateSerdes<>("window-store-key-schema", Serdes.Bytes(), Serdes.ByteArray());

    @Override
    public Bytes upperRange(final Bytes key, final long to) {
        return WindowStoreUtils.toBinaryKey(key, to, Integer.MAX_VALUE, serdes);
    }

    @Override
    public Bytes lowerRange(final Bytes key, final long from) {
        return WindowStoreUtils.toBinaryKey(key, Math.max(0, from), 0, serdes);
    }

    @Override
    public long segmentTimestamp(final Bytes key) {
        return WindowStoreUtils.timestampFromBinaryKey(key.get());
    }

    @Override
    public HasNextCondition hasNextCondition(final Bytes binaryKey, final long from, final long to) {
        return ITERATOR_HAS_NEXT;
    }

    @Override
    public List<Segment> segmentsToSearch(final Segments segments, final long from, final long to) {
        return segments.segments(from, to);
    }
}
