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

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.util.Map;

/**
 * Abstract base class for all ColumnFamilyAccessor.
 * Provides common logic for committing and retrieving offsets,
 * while delegating specific commit behavior to subclasses.
 */
abstract class AbstractColumnFamilyAccessor implements RocksDBStore.ColumnFamilyAccessor {

    private final ColumnFamilyHandle offsetColumnFamilyHandle;
    private final StringSerializer stringSerializer = new StringSerializer();
    private final Serdes.LongSerde longSerde = new Serdes.LongSerde();

    AbstractColumnFamilyAccessor(final ColumnFamilyHandle offsetColumnFamilyHandle) {
        this.offsetColumnFamilyHandle = offsetColumnFamilyHandle;
    }

    @Override
    public final void commit(final RocksDBStore.DBAccessor accessor, final Map<TopicPartition, Long> changelogOffsets) throws RocksDBException {
        this.commit(accessor);
        for (final Map.Entry<TopicPartition, Long> entry : changelogOffsets.entrySet()) {
            final TopicPartition tp = entry.getKey();
            final Long offset = entry.getValue();
            final byte[] key = stringSerializer.serialize(null, tp.toString());
            final byte[] value = longSerde.serializer().serialize(null, offset);
            accessor.put(offsetColumnFamilyHandle, key, value);
        }
        accessor.flush(offsetColumnFamilyHandle);
    }

    @Override
    public void close() {
        offsetColumnFamilyHandle.close();
    }

    @Override
    public final Long getCommitedOffset(final RocksDBStore.DBAccessor accessor, final TopicPartition partition) throws RocksDBException {
        final byte[] valueBytes = accessor.get(offsetColumnFamilyHandle, stringSerializer.serialize(null, partition.toString()));
        if (valueBytes != null) {
            return longSerde.deserializer().deserialize(null, valueBytes);
        }
        return null;
    }

    /**
     * Invokes commit in the underlying ColumnFamilyAccessor.
     * Subclasses should implement this method to define specific commit behavior.
     *
     * @param accessor the RocksDB accessor used to interact with the database
     * @throws RocksDBException if an error occurs during the commit operation
     */
    protected abstract void commit(final RocksDBStore.DBAccessor accessor) throws RocksDBException;
}
