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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class SubscriptionWrapperSerde<K> implements Serde {
    private final SubscriptionWrapperSerializer<K> serializer;
    private final SubscriptionWrapperDeserializer<K> deserializer;
    public final static int VERSION_BITS = 7;

    public SubscriptionWrapperSerde(final Serde<K> primaryKeySerde) {
        this.serializer = new SubscriptionWrapperSerializer<>(primaryKeySerde.serializer());
        this.deserializer = new SubscriptionWrapperDeserializer<>(primaryKeySerde.deserializer());
    }

    @Override
    public void configure(final Map configs, final boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer serializer() {
        return serializer;
    }

    @Override
    public Deserializer deserializer() {
        return deserializer;
    }

    private static class SubscriptionWrapperSerializer<K> implements Serializer<SubscriptionWrapper<K>> {
        final private Serializer<K> primaryKeySerializer;
        SubscriptionWrapperSerializer(final Serializer<K> primaryKeySerializer) {
            this.primaryKeySerializer = primaryKeySerializer;
        }

        @Override
        public void configure(final Map configs, final boolean isKey) {
            //Do nothing
        }

        @Override
        public byte[] serialize(final String topic, final SubscriptionWrapper<K> data) {
            //{1-bit-isHashNull}{7-bits-version}{1-byte-instruction}{Optional-16-byte-Hash}{PK-serialized}
            final byte[] primaryKeySerializedData = primaryKeySerializer.serialize(topic, data.getPrimaryKey());

            final ByteBuffer buf;
            if (data.getHash() != null) {
                buf = ByteBuffer.allocate(2 + 2 * Long.BYTES + primaryKeySerializedData.length);
                buf.put(data.getVersion());
            } else {
                //Don't store hash as it's null.
                buf = ByteBuffer.allocate(2 + primaryKeySerializedData.length);
                buf.put((byte) (data.getVersion() | (byte) 0x80));
            }

            buf.put(data.getInstruction().getByte());
            final long[] elem = data.getHash();
            if (data.getHash() != null) {
                buf.putLong(elem[0]);
                buf.putLong(elem[1]);
            }
            buf.put(primaryKeySerializedData);
            return buf.array();
        }

        @Override
        public void close() {
            //Do nothing
        }
    }

    private static class SubscriptionWrapperDeserializer<K> implements Deserializer<SubscriptionWrapper> {
        final private Deserializer<K> primaryKeyDeserializer;
        SubscriptionWrapperDeserializer(final Deserializer<K> primaryKeyDeserializer) {
            this.primaryKeyDeserializer = primaryKeyDeserializer;
        }

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {
            //Do nothing
        }

        @Override
        public SubscriptionWrapper deserialize(final String topic, final byte[] data) {
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
            final K primaryKey = primaryKeyDeserializer.deserialize(topic, primaryKeyRaw);

            return new SubscriptionWrapper<>(hash, inst, primaryKey, version);
        }

        @Override
        public void close() {
            //Do nothing
        }
    }

    private static byte[] numToBytes(final int num) {
        final ByteBuffer wrapped = ByteBuffer.allocate(Integer.BYTES);
        wrapped.putInt(num);
        return wrapped.array();
    }
}
