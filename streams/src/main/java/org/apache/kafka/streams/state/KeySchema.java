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
package org.apache.kafka.streams.state;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.internals.SessionKeySchema;
import org.apache.kafka.streams.state.internals.WindowKeySchema;

public interface KeySchema {

    /**
     * Initialize the schema with a topic. The topic may need to be used in any Serdes
     * uses by the implementation
     *
     * @param topic a topic name
     */
    void init(final String topic);

    /**
     * Given a range of record keys and a time, construct a key that represents
     * the upper range of keys to search when performing range queries.
     *
     * @param key
     * @param to
     * @return The key that represents the upper range to search for in the store
     * @see SessionKeySchema#upperRange
     * @see WindowKeySchema#upperRange
     */
    Bytes upperRange(final Bytes key, final long to);

    /**
     * Given a range of record keys and a time, construct a key that represents
     * the lower range of keys to search when performing range queries.
     *
     * @param key
     * @param from
     * @return The key that represents the lower range to search for in the store
     * @see SessionKeySchema#lowerRange
     * @see WindowKeySchema#lowerRange
     */
    Bytes lowerRange(final Bytes key, final long from);

    /**
     * Given a range of fixed size record keys and a time, construct a key that represents
     * the upper range of keys to search when performing range queries.
     *
     * @param key the last key in the range
     * @param to  the last timestamp in the range
     * @return The key that represents the upper range to search for in the store
     * @see SessionKeySchema#upperRange
     * @see WindowKeySchema#upperRange
     */
    Bytes upperRangeFixedSize(final Bytes key, final long to);

    /**
     * Given a range of fixed size record keys and a time, construct a key that represents
     * the lower range of keys to search when performing range queries.
     *
     * @param key  the first key in the range
     * @param from the first timestamp in the range
     * @return The key that represents the lower range to search for in the store
     * @see SessionKeySchema#lowerRange
     * @see WindowKeySchema#lowerRange
     */
    Bytes lowerRangeFixedSize(final Bytes key, final long from);

    /**
     * Create an implementation of {@link HasNextCondition} that knows when
     * to stop iterating.
     *
     * @param binaryKeyFrom the first key in the range
     * @param binaryKeyTo   the last key in the range
     * @param from          starting time range
     * @param to            ending time range
     * @return
     */
    HasNextCondition hasNextCondition(final Bytes binaryKeyFrom, final Bytes binaryKeyTo, final long from, final long to);

    /**
     * Convert the storeKey into the cacheKey
     * @param storeKey
     * @return  cacheKey
     */
    Bytes toCacheKey(final Bytes storeKey);

    /**
     * Convert the cacheKey into a storeKey
     * @param cacheKey
     * @return storeKey
     */
    Bytes toStoreKey(final Bytes cacheKey);

    /**
     * Extract the record key from the binaryKey
     * @param binaryKey
     * @return  recordKey
     */
    Bytes extractRecordKey(final Bytes binaryKey);

    /**
     * Convert the Windowed key to binary
     * @param storeKey
     * @return binaryKey
     */
    Bytes toBinaryKey(final Windowed<Bytes> storeKey);

    /**
     * Extract the Window from the binary key
     * @param binaryKey
     * @return Window
     */
    Window extractWindow(final Bytes binaryKey);

    /**
     * Compare the cachekey against the storeKey
     * @param cacheKey
     * @param storeKey
     * @return  0 if they are equal.
     *          A positive integer if cacheKey &gt; storeKey.
     *          A negative integer if storeKey &gt; cacheKey
     */
    int compareKeys(final Bytes cacheKey, final Bytes storeKey);

    Bytes toBinaryKey(final Bytes key, final long timestamp, int sequenceNumber);
}

