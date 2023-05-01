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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.internals.ChangelogRecordDeserializationHelper;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.internals.PrefixedWindowKeySchemas.KeyFirstWindowKeySchema;
import org.apache.kafka.streams.state.internals.PrefixedWindowKeySchemas.TimeFirstWindowKeySchema;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

/**
 * A RocksDB backed time-ordered segmented bytes store for window key schema.
 */
public class RocksDBTimeOrderedKeyValueSegmentedBytesStore extends AbstractRocksDBTimeOrderedSegmentedBytesStore {

  RocksDBTimeOrderedKeyValueSegmentedBytesStore(final String name,
                                              final String metricsScope,
                                              final long retention,
                                              final long segmentInterval,
                                              final boolean withIndex) {
    super(name, metricsScope, retention, segmentInterval, new TimeFirstWindowKeySchema(),
        Optional.ofNullable(withIndex ? new KeyFirstWindowKeySchema() : null));
  }

  @Override
  protected KeyValue<Bytes, byte[]> getIndexKeyValue(Bytes baseKey, byte[] baseValue) {
    return null;
  }

  @Override
  Map<KeyValueSegment, WriteBatch> getWriteBatches(Collection<ConsumerRecord<byte[], byte[]>> records) {
    return null;
  }

  @Override
  protected IndexToBaseStoreIterator getIndexToBaseStoreIterator(SegmentIterator<KeyValueSegment> segmentIterator) {
    return null;
  }
}