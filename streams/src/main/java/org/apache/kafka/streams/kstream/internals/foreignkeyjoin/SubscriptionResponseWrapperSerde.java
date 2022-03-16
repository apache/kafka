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
package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.internals.WrappingNullableDeserializer;
import org.apache.kafka.streams.kstream.internals.WrappingNullableSerializer;
import org.apache.kafka.streams.processor.internals.SerdeGetter;

import java.nio.ByteBuffer;

public class SubscriptionResponseWrapperSerde<V> implements Serde<SubscriptionResponseWrapper<V>> {
    private final SubscriptionResponseWrapperSerializer<V> serializer;
    private final SubscriptionResponseWrapperDeserializer<V> deserializer;

    public SubscriptionResponseWrapperSerde(final Serde<V> foreignValueSerde) {
        serializer = new SubscriptionResponseWrapperSerializer<>(foreignValueSerde == null ? null : foreignValueSerde.serializer());
        deserializer = new SubscriptionResponseWrapperDeserializer<>(foreignValueSerde == null ? null : foreignValueSerde.deserializer());
    }

    @Override
    public Serializer<SubscriptionResponseWrapper<V>> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<SubscriptionResponseWrapper<V>> deserializer() {
        return deserializer;
    }

    private static final class SubscriptionResponseWrapperSerializer<V>
        implements Serializer<SubscriptionResponseWrapper<V>>, WrappingNullableSerializer<SubscriptionResponseWrapper<V>, Void, V> {

        private Serializer<V> serializer;

        private SubscriptionResponseWrapperSerializer(final Serializer<V> serializer) {
            this.serializer = serializer;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void setIfUnset(final SerdeGetter getter) {
            if (serializer == null) {
                serializer = (Serializer<V>) getter.valueSerde().serializer();
            }
        }

        @Override
        public byte[] serialize(final String topic, final SubscriptionResponseWrapper<V> data) {
            //{1-bit-isHashNull}{7-bits-version}{Optional-16-byte-Hash}{n-bytes serialized data}

            //7-bit (0x7F) maximum for data version.
            if (Byte.compare((byte) 0x7F, data.getVersion()) < 0) {
                throw new UnsupportedVersionException("SubscriptionResponseWrapper version is larger than maximum supported 0x7F");
            }

            final byte[] serializedData = data.getForeignValue() == null ? null : serializer.serialize(topic, data.getForeignValue());
            final int serializedDataLength = serializedData == null ? 0 : serializedData.length;
            final long[] originalHash = data.getOriginalValueHash();
            final int hashLength = originalHash == null ? 0 : 2 * Long.BYTES;

            final ByteBuffer buf = ByteBuffer.allocate(1 + hashLength + serializedDataLength);

            if (originalHash != null) {
                buf.put(data.getVersion());
                buf.putLong(originalHash[0]);
                buf.putLong(originalHash[1]);
            } else {
                //Don't store hash as it's null.
                buf.put((byte) (data.getVersion() | (byte) 0x80));
            }

            if (serializedData != null)
                buf.put(serializedData);
            return buf.array();
        }

    }

    private static final class SubscriptionResponseWrapperDeserializer<V>
        implements Deserializer<SubscriptionResponseWrapper<V>>, WrappingNullableDeserializer<SubscriptionResponseWrapper<V>, Void, V> {

        private Deserializer<V> deserializer;

        private SubscriptionResponseWrapperDeserializer(final Deserializer<V> deserializer) {
            this.deserializer = deserializer;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void setIfUnset(final SerdeGetter getter) {
            if (deserializer == null) {
                deserializer = (Deserializer<V>) getter.valueSerde().deserializer();
            }
        }

        @Override
        public SubscriptionResponseWrapper<V> deserialize(final String topic, final byte[] data) {
            //{1-bit-isHashNull}{7-bits-version}{Optional-16-byte-Hash}{n-bytes serialized data}

            final ByteBuffer buf = ByteBuffer.wrap(data);
            final byte versionAndIsHashNull = buf.get();
            final byte version = (byte) (0x7F & versionAndIsHashNull);
            final boolean isHashNull = (0x80 & versionAndIsHashNull) == 0x80;

            final long[] hash;
            int lengthSum = 1; //The first byte
            if (isHashNull) {
                hash = null;
            } else {
                hash = new long[2];
                hash[0] = buf.getLong();
                hash[1] = buf.getLong();
                lengthSum += 2 * Long.BYTES;
            }

            final V value;
            if (data.length - lengthSum > 0) {
                final byte[] serializedValue;
                serializedValue = new byte[data.length - lengthSum];
                buf.get(serializedValue, 0, serializedValue.length);
                value = deserializer.deserialize(topic, serializedValue);
            } else {
                value = null;
            }

            return new SubscriptionResponseWrapper<>(hash, value, version);
        }

    }

}
