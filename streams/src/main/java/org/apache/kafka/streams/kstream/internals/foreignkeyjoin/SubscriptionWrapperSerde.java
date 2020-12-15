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
import org.apache.kafka.streams.kstream.internals.WrappingNullableSerde;
import org.apache.kafka.streams.kstream.internals.WrappingNullableSerializer;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.function.Supplier;

public class SubscriptionWrapperSerde<K> extends WrappingNullableSerde<SubscriptionWrapper<K>, K, Void> {
    public SubscriptionWrapperSerde(final Supplier<String> primaryKeySerializationPseudoTopicSupplier,
                                    final Serde<K> primaryKeySerde) {
        super(
            new SubscriptionWrapperSerializer<>(primaryKeySerializationPseudoTopicSupplier,
                                                primaryKeySerde == null ? null : primaryKeySerde.serializer()),
            new SubscriptionWrapperDeserializer<>(primaryKeySerializationPseudoTopicSupplier,
                                                  primaryKeySerde == null ? null : primaryKeySerde.deserializer())
        );
    }

    private static class SubscriptionWrapperSerializer<K>
        implements Serializer<SubscriptionWrapper<K>>, WrappingNullableSerializer<SubscriptionWrapper<K>, K, Void> {

        private final Supplier<String> primaryKeySerializationPseudoTopicSupplier;
        private String primaryKeySerializationPseudoTopic = null;
        private Serializer<K> primaryKeySerializer;

        SubscriptionWrapperSerializer(final Supplier<String> primaryKeySerializationPseudoTopicSupplier,
                                      final Serializer<K> primaryKeySerializer) {
            this.primaryKeySerializationPseudoTopicSupplier = primaryKeySerializationPseudoTopicSupplier;
            this.primaryKeySerializer = primaryKeySerializer;
        }

        @Override
        public void setIfUnset(final Serializer<K> defaultKeySerializer, final Serializer<Void> defaultValueSerializer) {
            if (primaryKeySerializer == null) {
                primaryKeySerializer = Objects.requireNonNull(defaultKeySerializer);
            }
        }

        @Override
        public byte[] serialize(final String ignored, final SubscriptionWrapper<K> data) {
            //{1-bit-isHashNull}{7-bits-version}{1-byte-instruction}{Optional-16-byte-Hash}{PK-serialized}

            //7-bit (0x7F) maximum for data version.
            if (Byte.compare((byte) 0x7F, data.getVersion()) < 0) {
                throw new UnsupportedVersionException("SubscriptionWrapper version is larger than maximum supported 0x7F");
            }

            if (primaryKeySerializationPseudoTopic == null) {
                primaryKeySerializationPseudoTopic = primaryKeySerializationPseudoTopicSupplier.get();
            }

            final byte[] primaryKeySerializedData = primaryKeySerializer.serialize(
                primaryKeySerializationPseudoTopic,
                data.getPrimaryKey()
            );

            final ByteBuffer buf;
            if (data.getHash() != null) {
                buf = ByteBuffer.allocate(2 + 2 * Long.BYTES + primaryKeySerializedData.length);
                buf.put(data.getVersion());
            } else {
                //Don't store hash as it's null.
                buf = ByteBuffer.allocate(2 + primaryKeySerializedData.length);
                buf.put((byte) (data.getVersion() | (byte) 0x80));
            }

            buf.put(data.getInstruction().getValue());
            final long[] elem = data.getHash();
            if (data.getHash() != null) {
                buf.putLong(elem[0]);
                buf.putLong(elem[1]);
            }
            buf.put(primaryKeySerializedData);
            return buf.array();
        }

    }

    private static class SubscriptionWrapperDeserializer<K>
        implements Deserializer<SubscriptionWrapper<K>>, WrappingNullableDeserializer<SubscriptionWrapper<K>, K, Void> {

        private final Supplier<String> primaryKeySerializationPseudoTopicSupplier;
        private String primaryKeySerializationPseudoTopic = null;
        private Deserializer<K> primaryKeyDeserializer;

        SubscriptionWrapperDeserializer(final Supplier<String> primaryKeySerializationPseudoTopicSupplier,
                                        final Deserializer<K> primaryKeyDeserializer) {
            this.primaryKeySerializationPseudoTopicSupplier = primaryKeySerializationPseudoTopicSupplier;
            this.primaryKeyDeserializer = primaryKeyDeserializer;
        }

        @Override
        public void setIfUnset(final Deserializer<K> defaultKeyDeserializer, final Deserializer<Void> defaultValueDeserializer) {
            if (primaryKeyDeserializer == null) {
                primaryKeyDeserializer = Objects.requireNonNull(defaultKeyDeserializer);
            }
        }

        @Override
        public SubscriptionWrapper<K> deserialize(final String ignored, final byte[] data) {
            //{7-bits-version}{1-bit-isHashNull}{1-byte-instruction}{Optional-16-byte-Hash}{PK-serialized}
            final ByteBuffer buf = ByteBuffer.wrap(data);
            final byte versionAndIsHashNull = buf.get();
            final byte version = (byte) (0x7F & versionAndIsHashNull);
            final boolean isHashNull = (0x80 & versionAndIsHashNull) == 0x80;
            final SubscriptionWrapper.Instruction inst = SubscriptionWrapper.Instruction.fromValue(buf.get());

            final long[] hash;
            int lengthSum = 2; //The first 2 bytes
            if (isHashNull) {
                hash = null;
            } else {
                hash = new long[2];
                hash[0] = buf.getLong();
                hash[1] = buf.getLong();
                lengthSum += 2 * Long.BYTES;
            }

            final byte[] primaryKeyRaw = new byte[data.length - lengthSum]; //The remaining data is the serialized pk
            buf.get(primaryKeyRaw, 0, primaryKeyRaw.length);

            if (primaryKeySerializationPseudoTopic == null) {
                primaryKeySerializationPseudoTopic = primaryKeySerializationPseudoTopicSupplier.get();
            }

            final K primaryKey = primaryKeyDeserializer.deserialize(primaryKeySerializationPseudoTopic,
                                                                    primaryKeyRaw);

            return new SubscriptionWrapper<>(hash, inst, primaryKey, version);
        }

    }

}
