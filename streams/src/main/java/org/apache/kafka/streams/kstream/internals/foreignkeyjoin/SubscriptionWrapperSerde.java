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
            //{1-byte-version}{1-byte-instruction}{16-bytes Hash}{Integer.Bytes-PK-length}{PK-serialized}
            final byte[] primaryKeySerializedData = primaryKeySerializer.serialize(topic, data.getPrimaryKey());
            //Size of Integer.BYTES bytes
            final byte[] primaryKeyByteSize = numToBytes(primaryKeySerializedData.length);

            final ByteBuffer buf = ByteBuffer.allocate(2 + 2* Long.BYTES + Integer.BYTES + primaryKeySerializedData.length);
            buf.put(data.getVersion());
            buf.put(data.getInstruction().getByte());
            final long[] elem = data.getHash();
            buf.putLong(elem[0]);
            buf.putLong(elem[1]);
            buf.put(primaryKeyByteSize);
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
        SubscriptionWrapperDeserializer (Deserializer<K> primaryKeyDeserializer) {
            this.primaryKeyDeserializer = primaryKeyDeserializer;
        }

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {
            //Do nothing
        }

        @Override
        public SubscriptionWrapper deserialize(final String topic, final byte[] data) {
            //{1-byte-version}{1-byte-instruction}{16-bytes Hash}{Integer.Bytes-PK-length}{PK-serialized}
            final ByteBuffer buf = ByteBuffer.wrap(data);
            final byte version = buf.get();
            final SubscriptionWrapper.Instruction inst = SubscriptionWrapper.Instruction.fromValue(buf.get());
            final long[] hash = new long[2];
            hash[0] = buf.getLong();
            hash[1] = buf.getLong();
            final int fkLength = buf.getInt();

            final byte[] primaryKeyRaw = new byte[fkLength];
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
