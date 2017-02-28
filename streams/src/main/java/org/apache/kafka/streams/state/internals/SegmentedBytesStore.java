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

/**
 * The interface representing a StateStore that has 1 or more segments that are based
 * on time.
 * @see RocksDBSegmentedBytesStore
 */
public interface SegmentedBytesStore extends StateStore {

    /**
     * Fetch all records from the segmented store with the provided key and time range
     * from all existing segments
     * @param key       the key to match
     * @param from      earliest time to match
     * @param to        latest time to match
     * @return  an iterator over key-value pairs
     */
    KeyValueIterator<Bytes, byte[]> fetch(Bytes key, long from, long to);

    /**
     * Remove the record with the provided key. The key
     * should be a composite of the record key, and the timestamp information etc
     * as described by the {@link KeySchema}
     * @param key   the segmented key to remove
     */
    void remove(Bytes key);

    /**
     * Write a new value to the store with the provided key. The key
     * should be a composite of the record key, and the timestamp information etc
     * as described by the {@link KeySchema}
     * @param key
     * @param value
     */
    void put(Bytes key, byte[] value);

    /**
     * Get the record from the store with the given key. The key
     * should be a composite of the record key, and the timestamp information etc
     * as described by the {@link KeySchema}
     * @param key
     * @return
     */
    byte[] get(Bytes key);

    interface KeySchema {
        /**
         * Given a record-key and a time, construct a Segmented key that represents
         * the upper range of keys to search when performing range queries.
         * @see SessionKeySchema#upperRange
         * @see WindowKeySchema#upperRange
         * @param key
         * @param to
         * @return      The key that represents the upper range to search for in the store
         */
        Bytes upperRange(final Bytes key, final long to);

        /**
         * Given a record-key and a time, construct a Segmented key that represents
         * the lower range of keys to search when performing range queries.
         * @see SessionKeySchema#lowerRange
         * @see WindowKeySchema#lowerRange
         * @param key
         * @param from
         * @return      The key that represents the lower range to search for in the store
         */
        Bytes lowerRange(final Bytes key, final long from);

        /**
         * Extract the timestamp of the segment from the key. The key is a composite of
         * the record-key, any timestamps, plus any additional information.
         * @see SessionKeySchema#lowerRange
         * @see WindowKeySchema#lowerRange
         * @param key
         * @return
         */
        long segmentTimestamp(final Bytes key);

        /**
         * Create an implementation of {@link HasNextCondition} that knows when
         * to stop iterating over the Segments. Used during {@link SegmentedBytesStore#fetch(Bytes, long, long)} operations
         * @param binaryKey     the record-key
         * @param from          starting time range
         * @param to            ending time range
         * @return
         */
        HasNextCondition hasNextCondition(final Bytes binaryKey, long from, long to);

        /**
         * Used during {@link SegmentedBytesStore#fetch(Bytes, long, long)} operations to determine
         * which segments should be scanned.
         * @param segments
         * @param from
         * @param to
         * @return  List of segments to search
         */
        List<Segment> segmentsToSearch(Segments segments, long from, long to);
    }
}
