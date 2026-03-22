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
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.internals.PrefixedWindowKeySchemas.KeyFirstWindowKeySchema;
import org.apache.kafka.streams.state.internals.PrefixedWindowKeySchemas.TimeFirstWindowKeySchema;

import org.rocksdb.WriteBatch;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 * A RocksDB backed time-ordered segmented bytes store for window key schema.
 * <p>
 * This class supports different segment implementations via the generic type parameter.
 * It can be used with {@link KeyValueSegment} for regular stores or {@link WindowSegmentWithHeaders}
 * for stores with headers support.
 *
 * @param <S> the segment type
 */
public class RocksDBTimeOrderedWindowSegmentedBytesStore<S extends Segment> extends AbstractRocksDBTimeOrderedSegmentedBytesStore<S> {

    /**
     * Concrete implementation of IndexToBaseStoreIterator for window key schema.
     * Converts index keys (key-first schema) to base store keys (time-first schema).
     * <p>
     * This can be reused by both window store implementations (with and without headers).
     */
    class WindowKeySchemaIndexToBaseStoreIterator extends IndexToBaseStoreIterator {
        WindowKeySchemaIndexToBaseStoreIterator(final KeyValueIterator<Bytes, byte[]> indexIterator) {
            super(indexIterator);
        }

        @Override
        protected Bytes getBaseKey(final Bytes indexKey) {
            final byte[] keyBytes = KeyFirstWindowKeySchema.extractStoreKeyBytes(indexKey.get());
            final long timestamp = KeyFirstWindowKeySchema.extractStoreTimestamp(indexKey.get());
            final int seqnum = KeyFirstWindowKeySchema.extractStoreSequence(indexKey.get());
            return TimeFirstWindowKeySchema.toStoreKeyBinary(keyBytes, timestamp, seqnum);
        }
    }

    RocksDBTimeOrderedWindowSegmentedBytesStore(final String name,
                                                final long retention,
                                                final boolean withIndex,
                                                final AbstractSegments<S> segments) {
        super(
            name,
            retention,
            new TimeFirstWindowKeySchema(),
            Optional.ofNullable(withIndex ? new KeyFirstWindowKeySchema() : null),
            segments
        );
    }

    @Override
    public void put(final Bytes key, final long timestamp, final int seqnum, final byte[] value) {
        final Bytes baseKey = TimeFirstWindowKeySchema.toStoreKeyBinary(key, timestamp, seqnum);
        put(baseKey, value);
    }

    @Override
    public byte[] fetch(final Bytes key, final long timestamp, final int seqnum) {
        return get(TimeFirstWindowKeySchema.toStoreKeyBinary(key, timestamp, seqnum));
    }

    @Override
    protected KeyValue<Bytes, byte[]> getIndexKeyValue(final Bytes baseKey, final byte[] baseValue) {
        final byte[] key = TimeFirstWindowKeySchema.extractStoreKeyBytes(baseKey.get());
        final long timestamp = TimeFirstWindowKeySchema.extractStoreTimestamp(baseKey.get());
        final int seqnum = TimeFirstWindowKeySchema.extractStoreSequence(baseKey.get());
        return KeyValue.pair(KeyFirstWindowKeySchema.toStoreKeyBinary(key, timestamp, seqnum), new byte[0]);
    }

    @Override
    Map<S, WriteBatch> getWriteBatches(final Collection<ConsumerRecord<byte[], byte[]>> records) {
        return getWriteBatches(
            records,
            WindowKeySchema::extractStoreTimestamp,
            KeyFirstWindowKeySchema::fromNonPrefixWindowKey,
            TimeFirstWindowKeySchema::fromNonPrefixWindowKey
        );
    }

    @Override
    protected IndexToBaseStoreIterator getIndexToBaseStoreIterator(final SegmentIterator<S> segmentIterator) {
        return new WindowKeySchemaIndexToBaseStoreIterator(segmentIterator);
    }
}