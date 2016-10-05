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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.List;

public interface SegmentedBytesStore extends StateStore {
    @SuppressWarnings("unchecked")
    KeyValueIterator<Bytes, byte[]> fetch(Bytes key, long from, long to);

    void remove(Bytes key);

    void put(Bytes key, byte[] value);

    @Override
    boolean isOpen();

    byte[] get(Bytes key);

    interface KeySchema {
        Bytes upperRange(final Bytes key, final long to);

        Bytes lowerRange(final Bytes key, final long from);

        long segmentTimestamp(final Bytes key);

        HasNextCondition hasNextCondition(final Bytes binaryKey, long from, long to);

        List<Segment> segmentsToSearch(Segments segments, long from, long to);
    }
}
