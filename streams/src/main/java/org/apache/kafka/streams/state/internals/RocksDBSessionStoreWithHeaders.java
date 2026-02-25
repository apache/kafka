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

import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.HeadersBytesStore;

/**
 * RocksDB-backed session store with support for record headers.
 * <p>
 * This store extends {@link RocksDBSessionStore} and overrides
 * {@code query()} to disable IQv2 for header-aware stores.
 * <p>
 * The storage format for values is: [headersSize(varint)][headersBytes][aggregationBytes]
 *
 * @see RocksDBSessionStore
 */
class RocksDBSessionStoreWithHeaders extends RocksDBSessionStore implements HeadersBytesStore {

    RocksDBSessionStoreWithHeaders(final SegmentedBytesStore bytesStore) {
        super(bytesStore);
    }

    @Override
    public <R> QueryResult<R> query(final Query<R> query, final PositionBound positionBound, final QueryConfig config) {
        throw new UnsupportedOperationException("Querying stores with headers is not supported");
    }
}
