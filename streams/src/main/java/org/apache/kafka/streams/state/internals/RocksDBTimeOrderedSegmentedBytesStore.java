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

import java.util.List;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.internals.PrefixedWindowKeySchemas.TimeFirstWindowKeySchema;

public class RocksDBTimeOrderedSegmentedBytesStore extends AbstractRocksDBSegmentedBytesStore<KeyValueSegment> {

    RocksDBTimeOrderedSegmentedBytesStore(final String name,
                               final String metricsScope,
                               final long retention,
                               final long segmentInterval) {
        super(name, metricsScope, new TimeFirstWindowKeySchema(), new KeyValueSegments(name, metricsScope, retention, segmentInterval));
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> fetchAll(final long timeFrom,
                                                    final long timeTo) {
        final List<KeyValueSegment> searchSpace = segments.segments(timeFrom, timeTo, true);
        final Bytes binaryFrom = keySchema.lowerRange(null, timeFrom);
        final Bytes binaryTo = keySchema.upperRange(null, timeTo);

        return new SegmentIterator<>(
                searchSpace.iterator(),
                keySchema.hasNextCondition(null, null, timeFrom, timeTo),
                binaryFrom,
                binaryTo,
                true);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> backwardFetchAll(final long timeFrom,
                                                            final long timeTo) {
        final List<KeyValueSegment> searchSpace = segments.segments(timeFrom, timeTo, false);
        final Bytes binaryFrom = keySchema.lowerRange(null, timeFrom);
        final Bytes binaryTo = keySchema.upperRange(null, timeTo);

        return new SegmentIterator<>(
                searchSpace.iterator(),
                keySchema.hasNextCondition(null, null, timeFrom, timeTo),
                binaryFrom,
                binaryTo,
                false);
    }
}