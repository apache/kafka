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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;

import static org.apache.kafka.streams.state.internals.ValueAndTimestampDeserializer.rawValue;
import static org.apache.kafka.streams.state.internals.ValueAndTimestampDeserializer.timestamp;

public class ChangeLoggingTimestampedKeyValueBytesStore extends ChangeLoggingKeyValueBytesStore {

    ChangeLoggingTimestampedKeyValueBytesStore(final KeyValueStore<Bytes, byte[]> inner) {
        super(inner);
    }

    @Override
    public void put(final Bytes key,
                    final byte[] valueAndTimestamp) {
        wrapped().put(key, valueAndTimestamp);
        log(key, rawValue(valueAndTimestamp), valueAndTimestamp == null ? context.timestamp() : timestamp(valueAndTimestamp));
    }

    @Override
    public byte[] putIfAbsent(final Bytes key,
                              final byte[] valueAndTimestamp) {
        final byte[] previous = wrapped().putIfAbsent(key, valueAndTimestamp);
        if (previous == null) {
            // then it was absent
            log(key, rawValue(valueAndTimestamp), valueAndTimestamp == null ? context.timestamp() : timestamp(valueAndTimestamp));
        }
        return previous;
    }

    @Override
    public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
        wrapped().putAll(entries);
        for (final KeyValue<Bytes, byte[]> entry : entries) {
            final byte[] valueAndTimestamp = entry.value;
            log(entry.key, rawValue(valueAndTimestamp), valueAndTimestamp == null ? context.timestamp() : timestamp(valueAndTimestamp));
        }
    }
}