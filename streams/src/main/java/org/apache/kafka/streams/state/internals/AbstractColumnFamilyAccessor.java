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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.query.Position;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Abstract base class for all ColumnFamilyAccessor.
 * Provides common logic for committing and retrieving offsets,
 * while delegating specific commit behavior to subclasses.
 */
abstract class AbstractColumnFamilyAccessor implements RocksDBStore.ColumnFamilyAccessor {

    private final ColumnFamilyHandle offsetColumnFamilyHandle;
    private final StringSerializer stringSerializer = new StringSerializer();
    private final Serdes.LongSerde longSerde = new Serdes.LongSerde();
    private final byte[] statusKey = stringSerializer.serialize(null, "status");
    private final byte[] positionKey = stringSerializer.serialize(null, "position");
    private final byte[] openState = longSerde.serializer().serialize(null, 1L);
    private final byte[] closedState = longSerde.serializer().serialize(null, 0L);
    private final AtomicBoolean storeOpen;
    // Status marker writes are skipped when true; the check on open still runs.
    private final boolean isTransactional;

    AbstractColumnFamilyAccessor(final ColumnFamilyHandle offsetColumnFamilyHandle,
                                  final AtomicBoolean storeOpen,
                                  final boolean isTransactional) {
        this.offsetColumnFamilyHandle = offsetColumnFamilyHandle;
        this.storeOpen = storeOpen;
        this.isTransactional = isTransactional;
    }

    @Override
    public final void commit(final RocksDBStore.DBAccessor accessor,
                             final Position position,
                             final Map<TopicPartition, Long> changelogOffsets) throws RocksDBException {
        if (changelogOffsets.isEmpty()) {
            // No changelog: wipe any stale changelog-offset keys left from a prior
            // logging configuration, then write the current position below.
            wipeOffsets(accessor);
        } else {
            for (final Map.Entry<TopicPartition, Long> entry : changelogOffsets.entrySet()) {
                final TopicPartition tp = entry.getKey();
                final Long offset = entry.getValue();
                final byte[] key = stringSerializer.serialize(null, tp.toString());
                if (offset != null) {
                    final byte[] value = longSerde.serializer().serialize(null, offset);
                    accessor.put(offsetColumnFamilyHandle, key, value);
                } else {
                    accessor.delete(offsetColumnFamilyHandle, key);
                }
            }
        }
        // Merge committed and uncommitted positions into a temporary copy so that if
        // commitStagedWrites() throws, the in-memory position remains consistent with the
        // last successful commit. Position is written after wipeOffsets so the key is not
        // deleted by the wipe. The in-memory position is updated only on success.
        final Position merged = position.copy();
        accessor.mergeUncommittedPositionInto(merged);
        accessor.put(offsetColumnFamilyHandle, positionKey, PositionSerde.serialize(merged).array());
        accessor.commitStagedWrites();
        position.merge(merged);
    }

    @Override
    public final Position open(final RocksDBStore.DBAccessor accessor, final boolean ignoreInvalidState) throws RocksDBException {
        final byte[] valueBytes = accessor.get(offsetColumnFamilyHandle, statusKey);
        if (ignoreInvalidState || (valueBytes == null || Arrays.equals(valueBytes, closedState))) {
            if (!isTransactional) {
                accessor.put(offsetColumnFamilyHandle, statusKey, openState);
            }
            storeOpen.set(true);
            final byte[] positionBytes = accessor.get(offsetColumnFamilyHandle, positionKey);
            if (positionBytes != null) {
                return PositionSerde.deserialize(ByteBuffer.wrap(positionBytes));
            } else {
                return Position.emptyPosition();
            }
        } else {
            throw new ProcessorStateException("Invalid state during store open. Expected state to be either empty or closed");
        }
    }

    @Override
    public void close(final RocksDBStore.DBAccessor accessor) throws RocksDBException {
        // Only persist the closed state if the store was previously open.
        // After an unclean shutdown, RocksDB may still be running background recovery,
        // causing accessor.put() to block. The put can also throw if RocksDB is in a
        // failed state (e.g. during an EOSv2 fencing cascade); the handle close must
        // still happen, otherwise the native ColumnFamilyHandle leaks every cycle.
        try {
            if (storeOpen.compareAndSet(true, false) && !isTransactional) {
                accessor.put(offsetColumnFamilyHandle, statusKey, closedState);
            }
        } finally {
            offsetColumnFamilyHandle.close();
        }
    }

    @Override
    public final Long getCommittedOffset(final RocksDBStore.DBAccessor accessor, final TopicPartition partition) throws RocksDBException {
        final byte[] valueBytes = accessor.get(offsetColumnFamilyHandle, stringSerializer.serialize(null, partition.toString()));
        if (valueBytes != null) {
            return longSerde.deserializer().deserialize(null, valueBytes);
        }
        return null;
    }


    @Override
    public final ColumnFamilyHandle offsetsColumnFamily() {
        return offsetColumnFamilyHandle;
    }

    // Visible for testing
    ColumnFamilyHandle offsetColumnFamilyHandle() {
        return offsetColumnFamilyHandle;
    }

    private void wipeOffsets(final RocksDBStore.DBAccessor accessor) throws RocksDBException {
        try (final RocksIterator iter = accessor.newIterator(offsetColumnFamilyHandle)) {
            iter.seekToFirst();
            while (iter.isValid()) {
                final byte[] key = iter.key();
                if (!Arrays.equals(key, statusKey)) {
                    accessor.delete(offsetColumnFamilyHandle, key);
                }
                iter.next();
            }
        }
    }
}
