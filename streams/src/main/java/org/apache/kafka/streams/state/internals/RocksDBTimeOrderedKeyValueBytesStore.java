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
import org.apache.kafka.streams.state.internals.PrefixedWindowKeySchemas.TimeFirstWindowKeySchema;

import org.rocksdb.WriteBatch;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 * A RocksDB backed time-ordered segmented bytes store for window key schema.
 */
public class RocksDBTimeOrderedKeyValueBytesStore extends AbstractRocksDBTimeOrderedSegmentedBytesStore<KeyValueSegment> {

    RocksDBTimeOrderedKeyValueBytesStore(final String name,
                                         final String metricsScope) {
        super(name,
            Long.MAX_VALUE,
            new TimeFirstWindowKeySchema(),
            Optional.empty(),
            new KeyValueSegments(name, metricsScope, Long.MAX_VALUE, Long.MAX_VALUE));
    }

    @Override
    protected KeyValue<Bytes, byte[]> getIndexKeyValue(final Bytes baseKey, final byte[] baseValue) {
        throw new UnsupportedOperationException("Do not use for TimeOrderedKeyValueStore");
    }

    @Override
    Map<KeyValueSegment, WriteBatch> getWriteBatches(final Collection<ConsumerRecord<byte[], byte[]>> records) {
        return getWriteBatches(
            records,
            WindowKeySchema::extractStoreTimestamp,
            null, // never an indexed store -- not needed
            TimeFirstWindowKeySchema::fromNonPrefixWindowKey
        );
    }

    @Override
    protected IndexToBaseStoreIterator getIndexToBaseStoreIterator(final SegmentIterator<KeyValueSegment> segmentIterator) {
        throw new UnsupportedOperationException("Do not use for TimeOrderedKeyValueStore");
    }

}