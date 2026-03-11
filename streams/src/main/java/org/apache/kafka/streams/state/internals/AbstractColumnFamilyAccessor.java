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

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

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
    private final byte[] openState = longSerde.serializer().serialize(null, 1L);
    private final byte[] closedState = longSerde.serializer().serialize(null, 0L);
    private final AtomicBoolean storeOpen;

    AbstractColumnFamilyAccessor(final ColumnFamilyHandle offsetColumnFamilyHandle, final AtomicBoolean storeOpen) {
        this.offsetColumnFamilyHandle = offsetColumnFamilyHandle;
        this.storeOpen = storeOpen;
    }

    @Override
    public final void commit(final RocksDBStore.DBAccessor accessor, final Map<TopicPartition, Long> changelogOffsets) throws RocksDBException {
        for (final Map.Entry<TopicPartition, Long> entry : changelogOffsets.entrySet()) {
            final TopicPartition tp = entry.getKey();
            final Long offset = entry.getValue();
            final byte[] key = stringSerializer.serialize(null, tp.toString());
            final byte[] value = longSerde.serializer().serialize(null, offset);
            accessor.put(offsetColumnFamilyHandle, key, value);
        }
        // We need to remove this flush call when implementing KAFKA-19712
        this.flush(accessor, offsetColumnFamilyHandle);
    }

    @Override
    public final void open(final RocksDBStore.DBAccessor accessor, final boolean ignoreInvalidState) throws RocksDBException {
        final byte[] valueBytes = accessor.get(offsetColumnFamilyHandle, statusKey);
        if (ignoreInvalidState || (valueBytes == null || Arrays.equals(valueBytes, closedState))) {
            // If the status key is not present, we initialize it to "OPEN"
            accessor.put(offsetColumnFamilyHandle, statusKey, openState);
            storeOpen.set(true);
        } else {
            throw new ProcessorStateException("Invalid state during store open. Expected state to be either empty or closed");
        }
    }

    @Override
    public void close(final RocksDBStore.DBAccessor accessor) throws RocksDBException {
        accessor.put(offsetColumnFamilyHandle, statusKey, closedState);
        offsetColumnFamilyHandle.close();
        storeOpen.set(false);
    }

    @Override
    public final Long getCommittedOffset(final RocksDBStore.DBAccessor accessor, final TopicPartition partition) throws RocksDBException {
        final byte[] valueBytes = accessor.get(offsetColumnFamilyHandle, stringSerializer.serialize(null, partition.toString()));
        if (valueBytes != null) {
            return longSerde.deserializer().deserialize(null, valueBytes);
        }
        return null;
    }

    /**
     * Invokes commit in the underlying ColumnFamilyAccessor.
     * Subclasses should implement this method to define specific commit behavior.
     * This method will be removed when implementing KAFKA-19712
     *
     * @param accessor the RocksDB accessor used to interact with the database
     * @throws RocksDBException if an error occurs during the commit operation
     */
    protected abstract void flush(final RocksDBStore.DBAccessor accessor, final ColumnFamilyHandle offsetColumnFamilyHandle) throws RocksDBException;
}
