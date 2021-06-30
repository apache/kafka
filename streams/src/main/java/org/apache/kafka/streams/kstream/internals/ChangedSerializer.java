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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.errors.StreamsException;

import java.nio.ByteBuffer;
import java.util.Objects;

public class ChangedSerializer<T> implements Serializer<Change<T>>, WrappingNullableSerializer<Change<T>, Void, T> {

    private static final int NEW_DATA_LENGTH_BYTES_SIZE = Integer.BYTES;

    private Serializer<T> inner;

    public ChangedSerializer(final Serializer<T> inner) {
        this.inner = inner;
    }

    public Serializer<T> inner() {
        return inner;
    }

    @Override
    public void setIfUnset(final Serializer<Void> defaultKeySerializer, final Serializer<T> defaultValueSerializer) {
        if (inner == null) {
            inner = Objects.requireNonNull(defaultValueSerializer);
        }
    }

    /**
     * @throws StreamsException if both old and new values of data are null.
     */
    @Override
    public byte[] serialize(final String topic, final Headers headers, final Change<T> data) {
        final boolean oldValueIsNull = data.oldValue == null;
        final boolean newValueIsNull = data.newValue == null;

        // both old and new values cannot be null
        if (oldValueIsNull && newValueIsNull) {
            throw new StreamsException("Both old and new values are null in ChangeSerializer, which is not allowed.");
        } else {
            final byte[] newData = newValueIsNull ? new byte[0] : inner.serialize(topic, headers, data.newValue);
            final byte[] oldData = oldValueIsNull ? new byte[0] : inner.serialize(topic, headers, data.oldValue);

            final int newDataLength = newData.length;
            final int capacity = NEW_DATA_LENGTH_BYTES_SIZE + newDataLength + oldData.length;

            return ByteBuffer
                    .allocate(capacity)
                    .putInt(newDataLength)
                    .put(newData)
                    .put(oldData)
                    .array();
        }
    }

    @Override
    public byte[] serialize(final String topic, final Change<T> data) {
        return serialize(topic, null, data);
    }

    @Override
    public void close() {
        inner.close();
    }
}
