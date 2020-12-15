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
import static org.apache.kafka.common.utils.Utils.getNullableSizePrefixedArray;

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
     * We used to serialize a Change into a single byte[]. Now, we don't anymore, but we still
     * need to be able to read it (so that we can load the state store from previously-written changelog records).
     */
    public static Change<byte[]> decomposeLegacyFormattedArrayIntoChangeArrays(final byte[] data) {
        if (data == null) {
            return null;
        }
        final ByteBuffer buffer = ByteBuffer.wrap(data);
        final byte[] oldBytes = getNullableSizePrefixedArray(buffer);
        final byte[] newBytes = getNullableSizePrefixedArray(buffer);
        return new Change<>(newBytes, oldBytes);
    }

}
