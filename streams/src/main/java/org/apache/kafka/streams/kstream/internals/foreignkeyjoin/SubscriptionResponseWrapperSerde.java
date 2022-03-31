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

import java.util.Map;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;
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
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);
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
        private boolean upgradeFromV0 = false;

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
        public void configure(final Map<String, ?> configs, final boolean isKey) {
            this.upgradeFromV0 = upgradeFromV0(configs);
        }

        private static boolean upgradeFromV0(final Map<String, ?> configs) {
            final Object upgradeFrom = configs.get(StreamsConfig.UPGRADE_FROM_CONFIG);
            if (upgradeFrom == null) {
                return false;
            }

            switch ((String) upgradeFrom) {
                case StreamsConfig.UPGRADE_FROM_0100:
                case StreamsConfig.UPGRADE_FROM_0101:
                case StreamsConfig.UPGRADE_FROM_0102:
                case StreamsConfig.UPGRADE_FROM_0110:
                case StreamsConfig.UPGRADE_FROM_10:
                case StreamsConfig.UPGRADE_FROM_11:
                case StreamsConfig.UPGRADE_FROM_20:
                case StreamsConfig.UPGRADE_FROM_21:
                case StreamsConfig.UPGRADE_FROM_22:
                case StreamsConfig.UPGRADE_FROM_23:
                case StreamsConfig.UPGRADE_FROM_24:
                case StreamsConfig.UPGRADE_FROM_25:
                case StreamsConfig.UPGRADE_FROM_26:
                case StreamsConfig.UPGRADE_FROM_27:
                case StreamsConfig.UPGRADE_FROM_28:
                case StreamsConfig.UPGRADE_FROM_30:
                case StreamsConfig.UPGRADE_FROM_31:
                case StreamsConfig.UPGRADE_FROM_32:
                    return true;
                default:
                    return false;
            }
        }

        @Override
        public byte[] serialize(final String topic, final SubscriptionResponseWrapper<V> data) {
            //{1-bit-isHashNull}{7-bits-version}{4-bytes-primaryPartition}{Optional-16-byte-Hash}{n-bytes serialized data}

            //7-bit (0x7F) maximum for data version.
            if (Byte.compare((byte) 0x7F, data.getVersion()) < 0) {
                throw new UnsupportedVersionException("SubscriptionResponseWrapper version is larger than maximum supported 0x7F");
            }

            final int version = data.getVersion();
            if (upgradeFromV0 || version == 0) {
                return serializeV0(topic, data);
            } else if (version == 1) {
                return serializeV1(topic, data);
            } else {
                throw new UnsupportedVersionException("Unsupported SubscriptionWrapper version " + data.getVersion());
            }
        }

        private byte[] serializeV0(final String topic, final SubscriptionResponseWrapper<V> data) {
            final byte[] serializedData = data.getForeignValue() == null ? null : serializer.serialize(topic, data.getForeignValue());
            final int serializedDataLength = serializedData == null ? 0 : serializedData.length;
            final long[] originalHash = data.getOriginalValueHash();
            final int hashLength = originalHash == null ? 0 : 2 * Long.BYTES;
            final int dataLength = 1 + hashLength + serializedDataLength;

            final ByteBuffer buf = ByteBuffer.allocate(dataLength);

            if (originalHash != null) {
                buf.put((byte) 0);
                buf.putLong(originalHash[0]);
                buf.putLong(originalHash[1]);
            } else {
                buf.put((byte) 0x80);
            }

            if (serializedData != null)
                buf.put(serializedData);
            return buf.array();
        }

        private byte[] serializeV1(final String topic, final SubscriptionResponseWrapper<V> data) {
            final byte[] serializedData = data.getForeignValue() == null ? null : serializer.serialize(topic, data.getForeignValue());
            final int serializedDataLength = serializedData == null ? 0 : serializedData.length;
            final long[] originalHash = data.getOriginalValueHash();
            final int hashLength = originalHash == null ? 0 : 2 * Long.BYTES;
            final int primaryPartitionLength = Integer.BYTES;
            final int dataLength = 1 + hashLength + serializedDataLength + primaryPartitionLength;

            final ByteBuffer buf = ByteBuffer.allocate(dataLength);

            if (originalHash != null) {
                buf.put(data.getVersion());
            } else {
                buf.put((byte) (data.getVersion() | (byte) 0x80));
            }
            buf.putInt(data.getPrimaryPartition());

            if (originalHash != null) {
                buf.putLong(originalHash[0]);
                buf.putLong(originalHash[1]);
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
            //{1-bit-isHashNull}{7-bits-version}{4-bytes-primaryPartition}{Optional-16-byte-Hash}{n-bytes serialized data}

            final ByteBuffer buf = ByteBuffer.wrap(data);
            final byte versionAndIsHashNull = buf.get();
            final byte version = (byte) (0x7F & versionAndIsHashNull);
            final boolean isHashNull = (0x80 & versionAndIsHashNull) == 0x80;
            int lengthSum = 1; //The first byte
            final Integer primaryPartition;
            if (version > 0) {
                primaryPartition = buf.getInt();
                lengthSum += Integer.BYTES;
            } else {
                primaryPartition = null;
            }

            final long[] hash;
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

            return new SubscriptionResponseWrapper<>(hash, value, version, primaryPartition);
        }

    }

}
