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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.nio.ByteBuffer;
import java.util.function.Supplier;

/**
 * Factory for creating CombinedKey serializers / deserializers.
 */
public class CombinedKeySchema<KO, K> {
    private final Supplier<String> undecoratedPrimaryKeySerdeTopicSupplier;
    private final Supplier<String> undecoratedForeignKeySerdeTopicSupplier;
    private String primaryKeySerdeTopic;
    private String foreignKeySerdeTopic;
    private Serializer<K> primaryKeySerializer;
    private Deserializer<K> primaryKeyDeserializer;
    private Serializer<KO> foreignKeySerializer;
    private Deserializer<KO> foreignKeyDeserializer;

    public CombinedKeySchema(final Supplier<String> foreignKeySerdeTopicSupplier,
                             final Serde<KO> foreignKeySerde,
                             final Supplier<String> primaryKeySerdeTopicSupplier,
                             final Serde<K> primaryKeySerde) {
        undecoratedPrimaryKeySerdeTopicSupplier = primaryKeySerdeTopicSupplier;
        undecoratedForeignKeySerdeTopicSupplier = foreignKeySerdeTopicSupplier;
        primaryKeySerializer = primaryKeySerde == null ? null : primaryKeySerde.serializer();
        primaryKeyDeserializer = primaryKeySerde == null ? null : primaryKeySerde.deserializer();
        foreignKeyDeserializer = foreignKeySerde == null ? null : foreignKeySerde.deserializer();
        foreignKeySerializer = foreignKeySerde == null ? null : foreignKeySerde.serializer();
    }

    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context) {
        primaryKeySerdeTopic = undecoratedPrimaryKeySerdeTopicSupplier.get();
        foreignKeySerdeTopic = undecoratedForeignKeySerdeTopicSupplier.get();
        primaryKeySerializer = primaryKeySerializer == null ? (Serializer<K>) context.keySerde().serializer() : primaryKeySerializer;
        primaryKeyDeserializer = primaryKeyDeserializer == null ? (Deserializer<K>) context.keySerde().deserializer() : primaryKeyDeserializer;
        foreignKeySerializer = foreignKeySerializer == null ? (Serializer<KO>) context.keySerde().serializer() : foreignKeySerializer;
        foreignKeyDeserializer = foreignKeyDeserializer == null ? (Deserializer<KO>) context.keySerde().deserializer() : foreignKeyDeserializer;
    }

    Bytes toBytes(final KO foreignKey, final K primaryKey) {
        //The serialization format - note that primaryKeySerialized may be null, such as when a prefixScan
        //key is being created.
        //{Integer.BYTES foreignKeyLength}{foreignKeySerialized}{Optional-primaryKeySerialized}
        final byte[] foreignKeySerializedData = foreignKeySerializer.serialize(foreignKeySerdeTopic,
                                                                               foreignKey);

        //? bytes
        final byte[] primaryKeySerializedData = primaryKeySerializer.serialize(primaryKeySerdeTopic,
                                                                               primaryKey);

        final ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES + foreignKeySerializedData.length + primaryKeySerializedData.length);
        buf.putInt(foreignKeySerializedData.length);
        buf.put(foreignKeySerializedData);
        buf.put(primaryKeySerializedData);
        return Bytes.wrap(buf.array());
    }


    public CombinedKey<KO, K> fromBytes(final Bytes data) {
        //{Integer.BYTES foreignKeyLength}{foreignKeySerialized}{Optional-primaryKeySerialized}
        final byte[] dataArray = data.get();
        final ByteBuffer dataBuffer = ByteBuffer.wrap(dataArray);
        final int foreignKeyLength = dataBuffer.getInt();
        final byte[] foreignKeyRaw = new byte[foreignKeyLength];
        dataBuffer.get(foreignKeyRaw, 0, foreignKeyLength);
        final KO foreignKey = foreignKeyDeserializer.deserialize(foreignKeySerdeTopic, foreignKeyRaw);

        final byte[] primaryKeyRaw = new byte[dataArray.length - foreignKeyLength - Integer.BYTES];
        dataBuffer.get(primaryKeyRaw, 0, primaryKeyRaw.length);
        final K primaryKey = primaryKeyDeserializer.deserialize(primaryKeySerdeTopic, primaryKeyRaw);
        return new CombinedKey<>(foreignKey, primaryKey);
    }

    Bytes prefixBytes(final KO key) {
        //The serialization format. Note that primaryKeySerialized is not required/used in this function.
        //{Integer.BYTES foreignKeyLength}{foreignKeySerialized}{Optional-primaryKeySerialized}

        final byte[] foreignKeySerializedData = foreignKeySerializer.serialize(foreignKeySerdeTopic, key);

        final ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES + foreignKeySerializedData.length);
        buf.putInt(foreignKeySerializedData.length);
        buf.put(foreignKeySerializedData);
        return Bytes.wrap(buf.array());
    }
}
