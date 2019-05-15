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

/**
 * Factory for creating CombinedKey serializers / deserializers.
 */
public class CombinedKeySerde<KO, K> implements Serde<CombinedKey<KO, K>> {
    final private Serializer<K> primaryKeySerializer;
    final private Deserializer<K> primaryKeyDeserializer;
    final private Serializer<KO> foreignKeySerializer;
    final private Deserializer<KO> foreignKeyDeserializer;
    final private Serializer<CombinedKey<KO, K>> serializer;
    final private Deserializer<CombinedKey<KO, K>> deserializer;

    public CombinedKeySerde(final Serde<KO> foreignKeySerde, final Serde<K> primaryKeySerde) {
        this.primaryKeySerializer = primaryKeySerde.serializer();
        this.primaryKeyDeserializer = primaryKeySerde.deserializer();
        this.foreignKeyDeserializer = foreignKeySerde.deserializer();
        this.foreignKeySerializer = foreignKeySerde.serializer();
        this.serializer = new CombinedKeySerializer<>(foreignKeySerializer, primaryKeySerializer);
        this.deserializer = new CombinedKeyDeserializer<>(foreignKeyDeserializer, primaryKeyDeserializer);
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        primaryKeySerializer.configure(configs, isKey);
        foreignKeySerializer.configure(configs, isKey);
        primaryKeyDeserializer.configure(configs, isKey);
        foreignKeyDeserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        primaryKeyDeserializer.close();
        foreignKeyDeserializer.close();
        primaryKeySerializer.close();
        foreignKeySerializer.close();
    }

    @Override
    public Serializer<CombinedKey<KO, K>> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<CombinedKey<KO, K>> deserializer() {
        return deserializer;
    }

    public Serializer<KO> getForeignKeySerializer() {
        return this.foreignKeySerializer;
    }

    public Serializer<K> getPrimaryKeySerializer() {
        return this.primaryKeySerializer;
    }

    class CombinedKeySerializer<KF, KP> implements Serializer<CombinedKey<KF, KP>> {

        private final Serializer<KF> foreignKeySerializer;
        private final Serializer<KP> primaryKeySerializer;

        public CombinedKeySerializer(final Serializer<KF> foreignKeySerializer, final Serializer<KP> primaryKeySerializer) {
            this.foreignKeySerializer = foreignKeySerializer;
            this.primaryKeySerializer = primaryKeySerializer;
        }

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {
            //Don't need to configure, they are already configured. This is just a wrapper.
        }

        @Override
        public byte[] serialize(final String topic, final CombinedKey<KF, KP> data) {
            //{Integer.BYTES foreignKeyLength}{foreignKeySerialized}{primaryKeySerialized}
            final byte[] foreignKeySerializedData = foreignKeySerializer.serialize(topic, data.getForeignKey());
            //Integer.BYTES bytes
            final byte[] foreignKeyByteSize = numToBytes(foreignKeySerializedData.length);

            if (data.getPrimaryKey() != null) {
                //? bytes
                final byte[] primaryKeySerializedData = primaryKeySerializer.serialize(topic, data.getPrimaryKey());

                final ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES + foreignKeySerializedData.length + primaryKeySerializedData.length);
                buf.put(foreignKeyByteSize);
                buf.put(foreignKeySerializedData);
                buf.put(primaryKeySerializedData);
                return buf.array();
            } else {
                final ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES + foreignKeySerializedData.length);
                buf.put(foreignKeyByteSize);
                buf.put(foreignKeySerializedData);
                return buf.array();
            }
        }

        private byte[] numToBytes(final int num) {
            final ByteBuffer wrapped = ByteBuffer.allocate(4);
            wrapped.putInt(num);
            return wrapped.array();
        }

        @Override
        public void close() {
            foreignKeySerializer.close();
            primaryKeySerializer.close();
        }
    }

    class CombinedKeyDeserializer<KF, KP> implements Deserializer<CombinedKey<KF, KP>> {

        private final Deserializer<KF> foreignKeyDeserializer;
        private final Deserializer<KP> primaryKeyDeserializer;


        public CombinedKeyDeserializer(final Deserializer<KF> foreignKeyDeserializer, final Deserializer<KP> primaryKeyDeserializer) {
            this.foreignKeyDeserializer = foreignKeyDeserializer;
            this.primaryKeyDeserializer = primaryKeyDeserializer;
        }

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {
            //Don't need to configure them, as they are already configured. This is only a wrapper.
        }

        @Override
        public CombinedKey<KF, KP> deserialize(final String topic, final byte[] data) {
            final ByteBuffer buf = ByteBuffer.wrap(data);
            final int foreignKeyLength = buf.getInt();
            final byte[] foreignKeyRaw = new byte[foreignKeyLength];
            buf.get(foreignKeyRaw, 0, foreignKeyLength);
            final KF foreignKey = foreignKeyDeserializer.deserialize(topic, foreignKeyRaw);

            if (data.length == Integer.BYTES + foreignKeyLength) {
                return new CombinedKey<>(foreignKey);
            } else {
                final byte[] primaryKeyRaw = new byte[data.length - foreignKeyLength - Integer.BYTES];
                buf.get(primaryKeyRaw, 0, primaryKeyRaw.length);
                final KP primaryKey = primaryKeyDeserializer.deserialize(topic, primaryKeyRaw);
                return new CombinedKey<>(foreignKey, primaryKey);
            }
        }

        @Override
        public void close() {
            foreignKeyDeserializer.close();
            primaryKeyDeserializer.close();
        }
    }


}
