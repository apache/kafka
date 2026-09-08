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

/**
 * RocksDB-backed time-ordered session store with support for record headers.
 * <p>
 * This store extends {@link RocksDBTimeOrderedSessionStore} and relies on its inherited IQv2 query
 * handling (via {@code StoreQueryUtils}); header-aware value (de)serialization is performed at the
 * metered layer.
 * <p>
 * There is deliberately no {@code query()} override to gate query types: the metered wrapper
 * ({@code MeteredSessionStoreWithHeaders}) intercepts and deserializes every query that would
 * otherwise be served from this store's raw header-format bytes, delegating downward only queries
 * that {@code StoreQueryUtils} rejects as {@code UNKNOWN_QUERY_TYPE}. So raw header-format value
 * bytes are never returned directly to a caller; teaching {@code StoreQueryUtils} to serve a new
 * query type from session stores would require matching interception at the metered layer.
 * <p>
 * The storage format for values is: [headersSize(varint)][headersBytes][aggregationBytes]
 *
 * @see RocksDBTimeOrderedSessionStore
 */
class RocksDBTimeOrderedSessionStoreWithHeaders extends RocksDBTimeOrderedSessionStore implements HeadersBytesStore {

    RocksDBTimeOrderedSessionStoreWithHeaders(final RocksDBTimeOrderedSessionSegmentedBytesStoreWithHeaders store) {
        super(store);
    }
}
