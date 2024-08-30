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
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.internals.UpgradeFromValues;
import org.apache.kafka.streams.kstream.internals.WrappingNullableDeserializer;
import org.apache.kafka.streams.kstream.internals.WrappingNullableSerde;
import org.apache.kafka.streams.kstream.internals.WrappingNullableSerializer;
import org.apache.kafka.streams.processor.internals.SerdeGetter;

import java.nio.ByteBuffer;
import java.util.Map;
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
        private boolean upgradeFromV0 = false;

        SubscriptionWrapperSerializer(final Supplier<String> primaryKeySerializationPseudoTopicSupplier,
                                      final Serializer<K> primaryKeySerializer) {
            this.primaryKeySerializationPseudoTopicSupplier = primaryKeySerializationPseudoTopicSupplier;
            this.primaryKeySerializer = primaryKeySerializer;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void setIfUnset(final SerdeGetter getter) {
            if (primaryKeySerializer == null) {
                primaryKeySerializer = (Serializer<K>) getter.keySerde().serializer();
            }
        }

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {
            this.upgradeFromV0 = upgradeFromV0(configs);
        }

        private static boolean upgradeFromV0(final Map<String, ?> configs) {
            final Object upgradeFrom = configs.get(StreamsConfig.UPGRADE_FROM_CONFIG);
            if (upgradeFrom == null) {
                return false;
            }

            switch (UpgradeFromValues.fromString((String) upgradeFrom)) {
                case UPGRADE_FROM_0100:
                case UPGRADE_FROM_0101:
                case UPGRADE_FROM_0102:
                case UPGRADE_FROM_0110:
                case UPGRADE_FROM_10:
                case UPGRADE_FROM_11:
                case UPGRADE_FROM_20:
                case UPGRADE_FROM_21:
                case UPGRADE_FROM_22:
                case UPGRADE_FROM_23:
                case UPGRADE_FROM_24:
                case UPGRADE_FROM_25:
                case UPGRADE_FROM_26:
                case UPGRADE_FROM_27:
                case UPGRADE_FROM_28:
                case UPGRADE_FROM_30:
                case UPGRADE_FROM_31:
                case UPGRADE_FROM_32:
                case UPGRADE_FROM_33:
                    // there is no need to add new versions here
                    return true;
                default:
                    return false;
            }
        }

        @Override
        public byte[] serialize(final String ignored, final SubscriptionWrapper<K> data) {
            //{1-bit-isHashNull}{7-bits-version}{1-byte-instruction}{Optional-16-byte-Hash}{PK-serialized}{4-bytes-primaryPartition}

            //7-bit (0x7F) maximum for data version.
            if (Byte.compare((byte) 0x7F, data.version()) < 0) {
                throw new UnsupportedVersionException("SubscriptionWrapper version is larger than maximum supported 0x7F");
            }

            final int version = data.version();
            if (upgradeFromV0 || version == 0) {
                return serializeV0(data);
            } else if (version == 1) {
                return serializeV1(data);
            } else {
                throw new UnsupportedVersionException("Unsupported SubscriptionWrapper version " + data.version());
            }
        }

        private byte[] serializePrimaryKey(final SubscriptionWrapper<K> data) {
            if (primaryKeySerializationPseudoTopic == null) {
                primaryKeySerializationPseudoTopic = primaryKeySerializationPseudoTopicSupplier.get();
            }

            return  primaryKeySerializer.serialize(
                primaryKeySerializationPseudoTopic,
                data.primaryKey()
            );
        }

        private ByteBuffer serializeCommon(final SubscriptionWrapper<K> data, final byte version, final int extraLength) {
            final byte[] primaryKeySerializedData = serializePrimaryKey(data);
            final ByteBuffer buf;
            int dataLength = 2 + primaryKeySerializedData.length + extraLength;
            if (data.hash() != null) {
                dataLength += 2 * Long.BYTES;
                buf = ByteBuffer.allocate(dataLength);
                buf.put(version);
            } else {
                //Don't store hash as it's null.
                buf = ByteBuffer.allocate(dataLength);
                buf.put((byte) (version | (byte) 0x80));
            }
            buf.put(data.instruction().value());
            final long[] elem = data.hash();
            if (data.hash() != null) {
                buf.putLong(elem[0]);
                buf.putLong(elem[1]);
            }
            buf.put(primaryKeySerializedData);
            return buf;
        }

        private byte[] serializeV0(final SubscriptionWrapper<K> data) {
            return serializeCommon(data, (byte) 0, 0).array();
        }

        private byte[] serializeV1(final SubscriptionWrapper<K> data) {
            final ByteBuffer buf = serializeCommon(data, data.version(), Integer.BYTES);
            buf.putInt(data.primaryPartition());
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

        @SuppressWarnings("unchecked")
        @Override
        public void setIfUnset(final SerdeGetter getter) {
            if (primaryKeyDeserializer == null) {
                primaryKeyDeserializer = (Deserializer<K>) getter.keySerde().deserializer();
            }
        }

        @Override
        public SubscriptionWrapper<K> deserialize(final String ignored, final byte[] data) {
            //{7-bits-version}{1-bit-isHashNull}{1-byte-instruction}{Optional-16-byte-Hash}{PK-serialized}{4-bytes-primaryPartition}
            final ByteBuffer buf = ByteBuffer.wrap(data);
            final byte versionAndIsHashNull = buf.get();
            final byte version = (byte) (0x7F & versionAndIsHashNull);
            final boolean isHashNull = (0x80 & versionAndIsHashNull) == 0x80;
            final SubscriptionWrapper.Instruction inst = SubscriptionWrapper.Instruction.fromValue(buf.get());

            int lengthSum = 2; //The first 2 bytes
            final long[] hash;
            if (isHashNull) {
                hash = null;
            } else {
                hash = new long[2];
                hash[0] = buf.getLong();
                hash[1] = buf.getLong();
                lengthSum += 2 * Long.BYTES;
            }

            final int primaryKeyLength;
            if (version > 0) {
                primaryKeyLength = data.length - lengthSum - Integer.BYTES;
            } else {
                primaryKeyLength = data.length - lengthSum;
            }
            final byte[] primaryKeyRaw = new byte[primaryKeyLength];
            buf.get(primaryKeyRaw, 0, primaryKeyLength);

            if (primaryKeySerializationPseudoTopic == null) {
                primaryKeySerializationPseudoTopic = primaryKeySerializationPseudoTopicSupplier.get();
            }

            final K primaryKey = primaryKeyDeserializer.deserialize(
                primaryKeySerializationPseudoTopic,
                primaryKeyRaw
            );
            final Integer primaryPartition;
            if (version > 0) {
                primaryPartition = buf.getInt();
            } else {
                primaryPartition = null;
            }

            return new SubscriptionWrapper<>(hash, inst, primaryKey, version, primaryPartition);
        }

    }

}
