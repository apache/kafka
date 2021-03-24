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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.List;

public class RocksDBTimeOrderedSegmentedBytesStore extends AbstractRocksDBSegmentedBytesStore<KeyValueSegment> {
    private final KeySchema keySchema;
    private final AbstractSegments<KeyValueSegment> segments;

    RocksDBTimeOrderedSegmentedBytesStore(final String name,
                                          final String metricsScope,
                                          final long retention,
                                          final long segmentInterval,
                                          final KeySchema keySchema) {
        this(name, metricsScope, keySchema, new KeyValueSegments(name, metricsScope, retention, segmentInterval));
    }

    private RocksDBTimeOrderedSegmentedBytesStore(final String name,
                                          final String metricsScope,
                                          final KeySchema keySchema,
                                          final AbstractSegments<KeyValueSegment> segments) {
        super(name, metricsScope, keySchema, segments);
        this.keySchema = keySchema;
        this.segments = segments;
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> fetchAll(final long timeFrom,
                                                    final long timeTo) {
        final List<KeyValueSegment> searchSpace = segments.segments(timeFrom, timeTo, true);

        return new SegmentIterator<>(
            searchSpace.iterator(),
            keySchema.hasNextCondition(null, null, timeFrom, timeTo),
            null,
            null,
            true);
    }
}
