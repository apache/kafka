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

import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

public class KeyAndJoinSideDeserializer<K> implements Deserializer<KeyAndJoinSide<K>> {
    private final Deserializer<K> keyDeserializer;

    KeyAndJoinSideDeserializer(final Deserializer<K> keyDeserializer) {
        this.keyDeserializer = Objects.requireNonNull(keyDeserializer, "keyDeserializer is null");
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        keyDeserializer.configure(configs, isKey);
    }

    @Override
    public KeyAndJoinSide<K> deserialize(final String topic, final byte[] data) {
        final boolean bool = data[0] == 1 ? true : false;
        final K key = keyDeserializer.deserialize(topic, rawKey(data));

        return KeyAndJoinSide.make(bool, key);
    }

    static byte[] rawKey(final byte[] data) {
        final int rawValueLength = data.length - 1;

        return ByteBuffer
            .allocate(rawValueLength)
            .put(data, 1, rawValueLength)
            .array();
    }

    @Override
    public void close() {
        keyDeserializer.close();
    }
}
