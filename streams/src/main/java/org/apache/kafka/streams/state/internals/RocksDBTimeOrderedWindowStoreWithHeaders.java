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

import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.HeadersBytesStore;
import org.apache.kafka.streams.state.TimestampedBytesStore;

/**
 * RocksDB-backed time-ordered window store with support for record headers.
 * <p>
 * This store extends {@link RocksDBTimeOrderedWindowStore} and implements both
 * {@link TimestampedBytesStore} (for timestamp support) and {@link HeadersBytesStore}
 * (for header support) marker interfaces.
 * <p>
 * This store returns {@link QueryResult#forUnknownQueryType(Query, Object)} for all queries,
 * as IQv2 query handling is done at the metered layer.
 * <p>
 * The storage format for values is: [headersSize(varint)][headersBytes][timestamp(8)][value]
 *
 * @see RocksDBTimeOrderedWindowStore
 * @see HeadersBytesStore
 * @see TimestampedBytesStore
 */
class RocksDBTimeOrderedWindowStoreWithHeaders extends RocksDBTimeOrderedWindowStore implements TimestampedBytesStore, HeadersBytesStore {

    RocksDBTimeOrderedWindowStoreWithHeaders(final RocksDBTimeOrderedWindowSegmentedBytesStore<WindowSegmentWithHeaders> store,
                                             final boolean retainDuplicates,
                                             final long windowSize) {
        super(store, retainDuplicates, windowSize);
    }

    @Override
    public <R> QueryResult<R> query(final Query<R> query,
                                    final PositionBound positionBound,
                                    final QueryConfig config) {
        final long start = config.isCollectExecutionInfo() ? System.nanoTime() : -1L;
        final QueryResult<R> result;
        final Position position = getPosition();

        synchronized (position) {
            result = QueryResult.forUnknownQueryType(query, this);

            if (config.isCollectExecutionInfo()) {
                result.addExecutionInfo(
                    "Handled in " + this.getClass() + " in " + (System.nanoTime() - start) + "ns"
                );
            }
            result.setPosition(position.copy());
        }
        return result;
    }
}
