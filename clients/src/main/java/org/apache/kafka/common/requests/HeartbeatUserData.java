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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.serialization.ByteBufferSerializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public final class HeartbeatUserData {
    private static final ByteBufferSerializer serializer = new ByteBufferSerializer();

    private static class BytesTuple {

        private final byte[] key;
        private final byte[] value;

        public BytesTuple(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }
    }
    private HeartbeatUserData() {}

    public static byte[] serializeOneUserData(ByteBuffer heartbeatData) {
        if (heartbeatData == null) {
            return null;
        }
        return serializer.serialize(null, heartbeatData);
    }

    public static byte[] serializeUserDatas(Map<String, byte[]> heartbeatData) {
        if (heartbeatData == null) {
            return null;
        }
        int size = Integer.BYTES;
        final List<BytesTuple> serials = new LinkedList<>();
        for (Map.Entry<String, byte[]> stringEntry : heartbeatData.entrySet()) {
            final byte[] serialKey = stringEntry.getKey().getBytes(StandardCharsets.UTF_8);
            final byte[] serialValue = stringEntry.getValue();
            serials.add(new BytesTuple(serialKey, serialValue));
            size += Integer.BYTES + serialKey.length + Integer.BYTES + serialValue.length;
        }
        final ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.putInt(heartbeatData.size());
        for (BytesTuple tuple : serials) {
            buffer.putInt(tuple.key.length);
            buffer.put(tuple.key);
            buffer.putInt(tuple.value.length);
            buffer.put(tuple.value);
        }
        return serializer.serialize(null, buffer);
    }

    public static Map<String, ByteBuffer> deserializeUserDatas(byte[] userDatas) {
        if (userDatas == null) {
            return null;
        }
        final ByteBuffer buffer = ByteBuffer.wrap(userDatas);
        final int numEntries = buffer.getInt();
        final Map<String, ByteBuffer> result = new LinkedHashMap<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            final int keySize = buffer.getInt();
            final byte[] serialKey = new byte[keySize];
            buffer.get(serialKey);
            final String key = new String(serialKey, StandardCharsets.UTF_8);
            final int valueSize = buffer.getInt();
            final byte[] serialValue = new byte[valueSize];
            buffer.get(serialValue);
            result.put(key, ByteBuffer.wrap(serialValue));
        }
        return result;
    }
}
