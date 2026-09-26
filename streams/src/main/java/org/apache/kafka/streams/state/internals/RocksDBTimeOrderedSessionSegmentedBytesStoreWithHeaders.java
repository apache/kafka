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

/**
 * A RocksDB-backed time-ordered segmented bytes store with headers support for session key schema.
 * <p>
 * This store extends {@link AbstractRocksDBTimeOrderedSegmentedBytesStore} and uses
 * {@link SessionSegmentsWithHeaders} to manage segments with full header support,
 * including Column Family management and lazy migration from legacy formats.
 * <p>
 * The store maintains a dual-schema architecture:
 * <ul>
 *   <li>Base store: Time-first session key schema for efficient time-range queries</li>
 *   <li>Index store (optional): Key-first session key schema for efficient key-based queries</li>
 * </ul>
 * <p>
 * Headers are managed at the segment level by {@link SessionSegmentWithHeaders}.
 * <p>
 * Value format (timestamps are in the key, not in the value):
 * <ul>
 *   <li>Old format: {@code [aggregationBytes]}</li>
 *   <li>New format: {@code [headersSize(varint)][headersBytes][aggregationBytes]}</li>
 * </ul>
 *
 * @see RocksDBTimeOrderedSessionStore
 * @see SessionSegmentsWithHeaders
 * @see SessionSegmentWithHeaders
 */
class RocksDBTimeOrderedSessionSegmentedBytesStoreWithHeaders
    extends RocksDBTimeOrderedSessionSegmentedBytesStore<SessionSegmentWithHeaders> {

    RocksDBTimeOrderedSessionSegmentedBytesStoreWithHeaders(
        final String name,
        final String metricsScope,
        final long retention,
        final long segmentInterval,
        final boolean withIndex
    ) {
        super(
            name,
            retention,
            withIndex,
            new SessionSegmentsWithHeaders(name, metricsScope, retention, segmentInterval)
        );
    }

    @Override
    public <R> QueryResult<R> query(
        final Query<R> query,
        final PositionBound positionBound,
        final QueryConfig config
    ) {
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
