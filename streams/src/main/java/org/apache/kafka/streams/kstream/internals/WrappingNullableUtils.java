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

/**
 * If a component's serdes are Wrapping serdes, then they require a little extra setup
 * to be fully initialized at run time.
 */
public class WrappingNullableUtils {

    @SuppressWarnings("unchecked")
    private static <T> Deserializer<T> prepareDeserializer(final Deserializer<T> specificDeserializer, final Deserializer<?> contextKeyDeserializer, final Deserializer<?> contextValueDeserializer, final boolean isKey) {
        Deserializer<T> deserializerToUse = specificDeserializer;
        if (deserializerToUse == null) {
            deserializerToUse = (Deserializer<T>) (isKey ? contextKeyDeserializer : contextValueDeserializer);
        } else {
            initNullableDeserializer(deserializerToUse, contextKeyDeserializer, contextValueDeserializer);
        }
        return deserializerToUse;
    }
    @SuppressWarnings("unchecked")
    private static <T> Serializer<T> prepareSerializer(final Serializer<T> specificSerializer, final Serializer<?> contextKeySerializer, final Serializer<?> contextValueSerializer, final boolean isKey) {
        Serializer<T> serializerToUse = specificSerializer;
        if (serializerToUse == null) {
            serializerToUse = (Serializer<T>) (isKey ? contextKeySerializer : contextValueSerializer);
        } else {
            initNullableSerializer(serializerToUse, contextKeySerializer, contextValueSerializer);
        }
        return serializerToUse;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static <T> Serde<T> prepareSerde(final Serde<T> specificSerde, final Serde<?> contextKeySerde, final Serde<?> contextValueSerde, final boolean isKey) {
        Serde<T> serdeToUse = specificSerde;
        if (serdeToUse == null) {
            serdeToUse = (Serde<T>) (isKey ?  contextKeySerde : contextValueSerde);
        } else if (serdeToUse instanceof WrappingNullableSerde) {
            ((WrappingNullableSerde) serdeToUse).setIfUnset(contextKeySerde, contextValueSerde);
        }
        return serdeToUse;
    }

    public static <K> Deserializer<K> prepareKeyDeserializer(final Deserializer<K> specificDeserializer, final Deserializer<?> contextKeyDeserializer, final Deserializer<?> contextValueDeserializer) {
        return prepareDeserializer(specificDeserializer, contextKeyDeserializer, contextValueDeserializer, true);
    }

    public static <V> Deserializer<V> prepareValueDeserializer(final Deserializer<V> specificDeserializer, final Deserializer<?> contextKeyDeserializer, final Deserializer<?> contextValueDeserializer) {
        return prepareDeserializer(specificDeserializer, contextKeyDeserializer, contextValueDeserializer, false);
    }

    public static <K> Serializer<K> prepareKeySerializer(final Serializer<K> specificSerializer, final Serializer<?> contextKeySerializer, final Serializer<?> contextValueSerializer) {
        return prepareSerializer(specificSerializer, contextKeySerializer, contextValueSerializer, true);
    }

    public static <V> Serializer<V> prepareValueSerializer(final Serializer<V> specificSerializer, final Serializer<?> contextKeySerializer, final Serializer<?> contextValueSerializer) {
        return prepareSerializer(specificSerializer, contextKeySerializer, contextValueSerializer, false);
    }

    public static <K> Serde<K> prepareKeySerde(final Serde<K> specificSerde, final Serde<?> keySerde, final Serde<?> valueSerde) {
        return prepareSerde(specificSerde, keySerde, valueSerde, true);
    }

    public static <V> Serde<V> prepareValueSerde(final Serde<V> specificSerde, final Serde<?> keySerde, final Serde<?> valueSerde) {
        return prepareSerde(specificSerde, keySerde, valueSerde, false);
    }
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static <T> void initNullableSerializer(final Serializer<T> specificSerializer, final Serializer<?> contextKeySerializer, final Serializer<?> contextValueSerializer) {
        if (specificSerializer instanceof WrappingNullableSerializer) {
            ((WrappingNullableSerializer) specificSerializer).setIfUnset(contextKeySerializer, contextValueSerializer);
        }
    }
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static <T> void initNullableDeserializer(final Deserializer<T> specificDeserializer, final Deserializer<?> contextKeyDeserializer, final Deserializer<?> contextValueDeserializer) {
        if (specificDeserializer instanceof WrappingNullableDeserializer) {
            ((WrappingNullableDeserializer) specificDeserializer).setIfUnset(contextKeyDeserializer, contextValueDeserializer);
        }
    }

}
