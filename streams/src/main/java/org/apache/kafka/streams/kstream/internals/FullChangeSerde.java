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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

public final class FullChangeSerde<T> {
    private final Serde<T> inner;

    public static <T> FullChangeSerde<T> wrap(final Serde<T> serde) {
        if (serde == null) {
            return null;
        } else {
            return new FullChangeSerde<>(serde);
        }
    }

    private FullChangeSerde(final Serde<T> inner) {
        this.inner = requireNonNull(inner);
    }

    public Serde<T> innerSerde() {
        return inner;
    }

    public Change<byte[]> serializeParts(final String topic, final Change<T> data) {
        if (data == null) {
            return null;
        }
        final Serializer<T> innerSerializer = innerSerde().serializer();
        final byte[] oldBytes = data.oldValue == null ? null : innerSerializer.serialize(topic, data.oldValue);
        final byte[] newBytes = data.newValue == null ? null : innerSerializer.serialize(topic, data.newValue);
        return new Change<>(newBytes, oldBytes);
    }


    public Change<T> deserializeParts(final String topic, final Change<byte[]> serialChange) {
        if (serialChange == null) {
            return null;
        }
        final Deserializer<T> innerDeserializer = innerSerde().deserializer();

        final T oldValue =
            serialChange.oldValue == null ? null : innerDeserializer.deserialize(topic, serialChange.oldValue);
        final T newValue =
            serialChange.newValue == null ? null : innerDeserializer.deserialize(topic, serialChange.newValue);

        return new Change<>(newValue, oldValue);
    }

    /**
     * We used to serialize a Change into a single byte[]. Now, we don't anymore, but we still keep this logic here
     * so that we can produce the legacy format to test that we can still deserialize it.
     */
    public static byte[] composeLegacyFormat(final Change<byte[]> serialChange) {
        if (serialChange == null) {
            return null;
        }

        final int oldSize = serialChange.oldValue == null ? -1 : serialChange.oldValue.length;
        final int newSize = serialChange.newValue == null ? -1 : serialChange.newValue.length;

        final ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES * 2 + Math.max(0, oldSize) + Math.max(0, newSize));


        buffer.putInt(oldSize);
        if (serialChange.oldValue != null) {
            buffer.put(serialChange.oldValue);
        }

        buffer.putInt(newSize);
        if (serialChange.newValue != null) {
            buffer.put(serialChange.newValue);
        }
        return buffer.array();
    }

    /**
     * We used to serialize a Change into a single byte[]. Now, we don't anymore, but we still
     * need to be able to read it (so that we can load the state store from previously-written changelog records).
     */
    public static Change<byte[]> decomposeLegacyFormat(final byte[] data) {
        if (data == null) {
            return null;
        }
        final ByteBuffer buffer = ByteBuffer.wrap(data);

        final int oldSize = buffer.getInt();
        final byte[] oldBytes = oldSize == -1 ? null : new byte[oldSize];
        if (oldBytes != null) {
            buffer.get(oldBytes);
        }

        final int newSize = buffer.getInt();
        final byte[] newBytes = newSize == -1 ? null : new byte[newSize];
        if (newBytes != null) {
            buffer.get(newBytes);
        }

        return new Change<>(newBytes, oldBytes);
    }

}
