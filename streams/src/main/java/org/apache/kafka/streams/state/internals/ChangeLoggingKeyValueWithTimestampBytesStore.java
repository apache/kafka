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

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;

public class ChangeLoggingKeyValueWithTimestampBytesStore extends ChangeLoggingKeyValueBytesStore {
    private final LongDeserializer longDeserializer = new LongDeserializer();

    ChangeLoggingKeyValueWithTimestampBytesStore(final KeyValueStore<Bytes, byte[]> inner) {
        super(inner);
    }

    @Override
    public void put(final Bytes key,
                    final byte[] valueAndTimestamp) {
        if (valueAndTimestamp != null) {
            inner.put(key, valueAndTimestamp);
            log(key, valueAndTimestamp);
        } else {
            inner.put(key, null);
            changeLogger.logChange(key, null);
        }
    }

    @Override
    public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
        inner.putAll(entries);
        for (final KeyValue<Bytes, byte[]> entry : entries) {
            log(entry.key, entry.value);
        }
    }

    private void log(final Bytes key,
                     final byte[] valueAndTimestamp) {
        final byte[] rawTimestamp = new byte[8];
        final byte[] rawValue = new byte[valueAndTimestamp.length - 8];

        System.arraycopy(valueAndTimestamp, 0, rawTimestamp, 0, 8);
        System.arraycopy(valueAndTimestamp, 8, rawValue, 0, valueAndTimestamp.length - 8);

        changeLogger.logChange(key, rawValue, longDeserializer.deserialize(null, rawTimestamp));
    }
}