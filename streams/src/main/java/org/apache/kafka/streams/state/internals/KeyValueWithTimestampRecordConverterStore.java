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
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.RecordConverterStore;

public class KeyValueWithTimestampRecordConverterStore<K, V> extends MeteredKeyValueWithTimestampStore<K, V>
                                                             implements RecordConverterStore {

    KeyValueWithTimestampRecordConverterStore(final MeteredKeyValueStore<Bytes, byte[]> innerByteStore,
                                              final String metricScope,
                                              final Time time,
                                              final Serde<K> keySerde,
                                              final Serde<V> valueSerde) {
        super(innerByteStore, metricScope, time, keySerde, valueSerde);
    }

    @Override
    public KeyValue<byte[], byte[]> convert(final ConsumerRecord<byte[], byte[]> oldRecord) {
        return ChangeLoggingKeyValueWithTimestampBytesStore.convertRecord(oldRecord);
    }

}
