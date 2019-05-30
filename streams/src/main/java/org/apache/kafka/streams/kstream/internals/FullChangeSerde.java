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
import java.util.Map;

import static java.util.Objects.requireNonNull;

public final class FullChangeSerde<T> implements Serde<Change<T>> {
    private final Serde<T> inner;

    @SuppressWarnings("unchecked")
    public static <T> FullChangeSerde<T> castOrWrap(final Serde<T> serde) {
        if (serde == null) {
            return null;
        } else if (serde instanceof FullChangeSerde) {
            return (FullChangeSerde<T>) serde;
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

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        inner.configure(configs, isKey);
    }

    @Override
    public void close() {
        inner.close();
    }

    @Override
    public Serializer<Change<T>> serializer() {
        final Serializer<T> innerSerializer = inner.serializer();

        return new Serializer<Change<T>>() {
            @Override
            public void configure(final Map<String, ?> configs, final boolean isKey) {
                innerSerializer.configure(configs, isKey);
            }

            @Override
            public byte[] serialize(final String topic, final Change<T> data) {
                if (data == null) {
                    return null;
                }
                final byte[] oldBytes = data.oldValue == null ? null : innerSerializer.serialize(topic, data.oldValue);
                final int oldSize = oldBytes == null ? -1 : oldBytes.length;
                final byte[] newBytes = data.newValue == null ? null : innerSerializer.serialize(topic, data.newValue);
                final int newSize = newBytes == null ? -1 : newBytes.length;

                final ByteBuffer buffer = ByteBuffer.wrap(
                    new byte[4 + (oldSize == -1 ? 0 : oldSize) + 4 + (newSize == -1 ? 0 : newSize)]
                );
                buffer.putInt(oldSize);
                if (oldBytes != null) {
                    buffer.put(oldBytes);
                }
                buffer.putInt(newSize);
                if (newBytes != null) {
                    buffer.put(newBytes);
                }
                return buffer.array();
            }

            @Override
            public void close() {
                innerSerializer.close();
            }
        };
    }

    @Override
    public Deserializer<Change<T>> deserializer() {
        final Deserializer<T> innerDeserializer = inner.deserializer();
        return new Deserializer<Change<T>>() {
            @Override
            public void configure(final Map<String, ?> configs, final boolean isKey) {
                innerDeserializer.configure(configs, isKey);
            }

            @Override
            public Change<T> deserialize(final String topic, final byte[] data) {
                if (data == null) {
                    return null;
                }
                final ByteBuffer buffer = ByteBuffer.wrap(data);

                final byte[] oldBytes = extractOldValuePart(buffer);
                final T oldValue = oldBytes == null ? null : innerDeserializer.deserialize(topic, oldBytes);

                final int newSize = buffer.getInt();
                final byte[] newBytes = newSize == -1 ? null : new byte[newSize];
                if (newBytes != null) {
                    buffer.get(newBytes);
                }
                final T newValue = newBytes == null ? null : innerDeserializer.deserialize(topic, newBytes);
                return new Change<>(newValue, oldValue);
            }

            @Override
            public void close() {
                innerDeserializer.close();
            }
        };
    }

    public static byte[] extractOldValuePart(final ByteBuffer buffer) {
        final int oldSize = buffer.getInt();
        final byte[] oldBytes = oldSize == -1 ? null : new byte[oldSize];
        if (oldBytes != null) {
            buffer.get(oldBytes);
        }
        return oldBytes;
    }
}
