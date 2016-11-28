/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Windowed;

import java.nio.ByteBuffer;

/**
 * Utility class for the {@link org.apache.kafka.streams.state.SessionStore}.
 * Used to convert to/from the binary & non-binary representations of the
 * {@link Windowed} key used in the store.
 * @param <K>
 */
public class SessionKeyBinaryConverter<K> {
    private static final int TIMESTAMP_SIZE = 8;
    private static final String SESSIONKEY = "sessionkey";

    public static long extractEnd(final byte [] binaryKey) {
        return ByteBuffer.wrap(binaryKey).getLong(binaryKey.length - 2 * TIMESTAMP_SIZE);
    }

    public static long extractStart(final byte [] binaryKey) {
        return ByteBuffer.wrap(binaryKey).getLong(binaryKey.length - TIMESTAMP_SIZE);
    }

    public static byte[] extractKeyBytes(final byte[] binaryKey) {
        final byte[] bytes = new byte[binaryKey.length - 2 * TIMESTAMP_SIZE];
        System.arraycopy(binaryKey, 0, bytes, 0, bytes.length);
        return bytes;
    }

    public static <K> Windowed<K> from(final byte[] binaryKey, final Deserializer<K> keyDeserializer) {
        final K key = extractKey(binaryKey, keyDeserializer);
        final ByteBuffer buffer = ByteBuffer.wrap(binaryKey);
        final long start = buffer.getLong(binaryKey.length - TIMESTAMP_SIZE);
        final long end = buffer.getLong(binaryKey.length - 2 * TIMESTAMP_SIZE);
        return new Windowed<>(key, new TimeWindow(start, end));
    }

    private static <K> K extractKey(final byte[] binaryKey, Deserializer<K> deserializer) {
        return deserializer.deserialize(SESSIONKEY, extractKeyBytes(binaryKey));
    }

    public static <K> Bytes toBinary(final Windowed<K> sessionKey, final Serializer<K> serializer) {
        final byte[] bytes = serializer.serialize(SESSIONKEY, sessionKey.key());
        ByteBuffer buf = ByteBuffer.allocate(bytes.length + 2 * TIMESTAMP_SIZE);
        buf.put(bytes);
        buf.putLong(sessionKey.window().end());
        buf.putLong(sessionKey.window().start());
        return new Bytes(buf.array());
    }
}
