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

/**
 * A store supplier that can be used to create one or more {@link WindowStore WindowStore<Bytes, byte[]>} instances of type &lt;Byte, byte[]&gt;.
 *
 * For any stores implementing the {@link WindowStore WindowStore<Bytes, byte[]>} interface, null value bytes are considered as "not exist". This means:
 *
 * 1. Null value bytes in put operations should be treated as delete.
 * 2. Null value bytes should never be returned in range query results.
 */
public interface WindowBytesStoreSupplier extends StoreSupplier<WindowStore<Bytes, byte[]>> {
    /**
     * The number of segments the store has. If your store is segmented then this should be the number of segments
     * in the underlying store.
     * It is also used to reduce the amount of data that is scanned when caching is enabled.
     *
     * @return number of segments
     * @deprecated since 2.1. Use {@link WindowBytesStoreSupplier#segmentIntervalMs()} instead.
     */
    @Deprecated
    int segments();

    /**
     * The size of the segments (in milliseconds) the store has.
     * If your store is segmented then this should be the size of segments in the underlying store.
     * It is also used to reduce the amount of data that is scanned when caching is enabled.
     *
     * @return size of the segments (in milliseconds)
     */
    long segmentIntervalMs();

    /**
     * The size of the windows (in milliseconds) any store created from this supplier is creating.
     *
     * @return window size
     */
    long windowSize();

    /**
     * Whether or not this store is retaining duplicate keys.
     * Usually only true if the store is being used for joins.
     * Note this should return false if caching is enabled.
     *
     * @return true if duplicates should be retained
     */
    boolean retainDuplicates();

    /**
     * The time period for which the {@link WindowStore} will retain historic data.
     *
     * @return retentionPeriod
     */
    long retentionPeriod();
}
