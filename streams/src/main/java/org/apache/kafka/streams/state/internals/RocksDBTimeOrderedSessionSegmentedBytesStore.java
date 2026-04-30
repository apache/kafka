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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.internals.PrefixedSessionKeySchemas.KeyFirstSessionKeySchema;
import org.apache.kafka.streams.state.internals.PrefixedSessionKeySchemas.TimeFirstSessionKeySchema;

import org.rocksdb.WriteBatch;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A RocksDB backed time-ordered segmented bytes store for session key schema.
 */
public class RocksDBTimeOrderedSessionSegmentedBytesStore<S extends Segment> extends AbstractRocksDBTimeOrderedSegmentedBytesStore<S> {

    private class SessionKeySchemaIndexToBaseStoreIterator extends IndexToBaseStoreIterator {
        SessionKeySchemaIndexToBaseStoreIterator(final KeyValueIterator<Bytes, byte[]> indexIterator) {
            super(indexIterator);
        }

        @Override
        protected Bytes getBaseKey(final Bytes indexKey) {
            final Window window = KeyFirstSessionKeySchema.extractWindow(indexKey.get());
            final byte[] key = KeyFirstSessionKeySchema.extractKeyBytes(indexKey.get());
            return TimeFirstSessionKeySchema.toBinary(Bytes.wrap(key), window.start(), window.end());
        }
    }

    RocksDBTimeOrderedSessionSegmentedBytesStore(final String name,
                                                 final long retention,
                                                 final boolean withIndex,
                                                 final AbstractSegments<S> segments) {
        super(
            name,
            retention,
            new TimeFirstSessionKeySchema(),
            Optional.ofNullable(withIndex ? new KeyFirstSessionKeySchema() : null),
            segments
        );
    }

    @Override
    public byte[] fetchSession(final Bytes key,
                               final long sessionStartTime,
                               final long sessionEndTime) {
        return get(TimeFirstSessionKeySchema.toBinary(
            key,
            sessionStartTime,
            sessionEndTime
        ));
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> fetchSessions(final long earliestSessionEndTime,
                                                         final long latestSessionEndTime) {
        final List<S> searchSpace = segments.segments(earliestSessionEndTime, latestSessionEndTime, true);

        // here we want [0, latestSE, FF] as the upper bound to cover any possible keys,
        // but since we can only get upper bound based on timestamps, we use a slight larger upper bound as [0, latestSE+1]
        final Bytes binaryFrom = baseKeySchema.lowerRangeFixedSize(null, earliestSessionEndTime);
        final Bytes binaryTo = baseKeySchema.lowerRangeFixedSize(null, latestSessionEndTime + 1);

        return new SegmentIterator<>(
                searchSpace.iterator(),
                iterator -> {
                    while (iterator.hasNext()) {
                        final Bytes bytes = iterator.peekNextKey();

                        final Windowed<Bytes> windowedKey = TimeFirstSessionKeySchema.from(bytes);
                        final long endTime = windowedKey.window().end();

                        if (endTime <= latestSessionEndTime && endTime >= earliestSessionEndTime) {
                            return true;
                        }
                        iterator.next();
                    }
                    return false;
                },
                binaryFrom,
                binaryTo,
                true);
    }

    @Override
    public void remove(final Windowed<Bytes> key) {
        remove(TimeFirstSessionKeySchema.toBinary(key));
    }

    @Override
    public void put(final Windowed<Bytes> sessionKey, final byte[] aggregate) {
        put(TimeFirstSessionKeySchema.toBinary(sessionKey), aggregate);
    }

    @Override
    protected KeyValue<Bytes, byte[]> getIndexKeyValue(final Bytes baseKey, final byte[] baseValue) {
        final Window window = TimeFirstSessionKeySchema.extractWindow(baseKey.get());
        final byte[] key = TimeFirstSessionKeySchema.extractKeyBytes(baseKey.get());
        return KeyValue.pair(KeyFirstSessionKeySchema.toBinary(Bytes.wrap(key), window.start(), window.end()), new byte[0]);
    }

    @Override
    Map<S, WriteBatch> getWriteBatches(final Collection<ConsumerRecord<byte[], byte[]>> records) {
        return getWriteBatches(
            records,
            SessionKeySchema::extractEndTimestamp,
            KeyFirstSessionKeySchema::prefixNonPrefixSessionKey,
            TimeFirstSessionKeySchema::extractWindowBytesFromNonPrefixSessionKey
        );
    }

    @Override
    protected IndexToBaseStoreIterator getIndexToBaseStoreIterator(final SegmentIterator<S> segmentIterator) {
        return new SessionKeySchemaIndexToBaseStoreIterator(segmentIterator);
    }
}