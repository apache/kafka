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

import org.apache.kafka.streams.state.HeadersBytesStore;
import org.apache.kafka.streams.state.TimestampedBytesStore;

/**
 * RocksDB-backed timestamped window store with support for record headers.
 * <p>
 * This store extends {@link RocksDBWindowStore} and implements both
 * {@link TimestampedBytesStore} (for timestamp support) and {@link HeadersBytesStore}
 * (for header support) marker interfaces.
 * <p>
 * IQv2 query handling is inherited (via {@code StoreQueryUtils}); header-aware value
 * (de)serialization is performed at the metered layer. There is deliberately no {@code query()}
 * override to gate query types: the metered wrapper ({@code MeteredTimestampedWindowStoreWithHeaders})
 * intercepts and deserializes every query that would otherwise be served from this store's raw
 * header-format bytes, delegating downward only queries that {@code StoreQueryUtils} rejects as
 * {@code UNKNOWN_QUERY_TYPE}. So raw header-format value bytes are never returned directly to a
 * caller; teaching {@code StoreQueryUtils} to serve a new query type from window stores would
 * require matching interception at the metered layer.
 * <p>
 * The storage format for values is: [headersSize(varint)][headersBytes][timestamp(8)][value]
 * <p>
 * This implementation uses segment-level versioning for backward compatibility:
 * <ul>
 * <li>Old segments continue to use the legacy format without headers</li>
 * <li>New segments use the header-embedded format</li>
 * <li>Legacy values are served with empty headers on read</li>
 * <li>All new writes use the new format</li>
 * </ul>
 *
 * @see RocksDBWindowStore
 * @see HeadersBytesStore
 * @see TimestampedBytesStore
 */
class RocksDBTimestampedWindowStoreWithHeaders extends RocksDBWindowStore implements TimestampedBytesStore, HeadersBytesStore {

    RocksDBTimestampedWindowStoreWithHeaders(final AbstractRocksDBSegmentedBytesStore<?> bytesStore,
                                             final boolean retainDuplicates,
                                             final long windowSize) {
        super(bytesStore, retainDuplicates, windowSize);
    }
}
