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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueStore;

class CachingKeyValueWithTimestampStore<K, V> extends CachingKeyValueStore<K, V> {

    CachingKeyValueWithTimestampStore(final KeyValueStore<Bytes, byte[]> underlying, final Serde<K> keySerde, final KeyValueWithTimestampStoreBuilder.ValueAndTimestampSerde<V> valueSerde) {
        super(underlying, keySerde, valueSerde.valueSerde());
    }

    @Override
    void notifyFlushListener(final ThreadCache.DirtyEntry entry, final byte[] nullableOldRawValue) {
        final byte[] oldRawValue;
        if (nullableOldRawValue == null) {
            oldRawValue = null;
        } else {
            oldRawValue = new byte[nullableOldRawValue.length - 8];
            System.arraycopy(nullableOldRawValue, 8, oldRawValue, 0, oldRawValue.length);
        }

        final byte[] newRawValueAndTimestamp = entry.newValue();
        final byte[] newRawValue = new byte[newRawValueAndTimestamp.length - 8];
        System.arraycopy(newRawValueAndTimestamp, 8, newRawValue, 0, newRawValue.length);

        super.notifyFlushListener(new ThreadCache.DirtyEntry(entry.key(), newRawValue, entry.entry()), oldRawValue);
    }
}
