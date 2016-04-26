/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.kafka.common.serialization;

import org.apache.kafka.common.utils.Bytes;

import java.nio.ByteBuffer;

/**
 * Factory for creating serializers / deserializers.
 */
public class Serdes {

    static public final class LongSerde implements Serde<Long> {
        @Override
        public Serializer<Long> serializer() {
            return new LongSerializer();
        }

        @Override
        public Deserializer<Long> deserializer() {
            return new LongDeserializer();
        }
    }

    static public final class IntegerSerde implements Serde<Integer> {
        @Override
        public Serializer<Integer> serializer() {
            return new IntegerSerializer();
        }

        @Override
        public Deserializer<Integer> deserializer() {
            return new IntegerDeserializer();
        }
    }

    static public final class DoubleSerde implements Serde<Double> {
        @Override
        public Serializer<Double> serializer() {
            return new DoubleSerializer();
        }

        @Override
        public Deserializer<Double> deserializer() {
            return new DoubleDeserializer();
        }
    }

    static public final class StringSerde implements Serde<String> {
        @Override
        public Serializer<String> serializer() {
            return new StringSerializer();
        }

        @Override
        public Deserializer<String> deserializer() {
            return new StringDeserializer();
        }
    }

    static public final class ByteBufferSerde implements Serde<ByteBuffer> {
        @Override
        public Serializer<ByteBuffer> serializer() {
            return new ByteBufferSerializer();
        }

        @Override
        public Deserializer<ByteBuffer> deserializer() {
            return new ByteBufferDeserializer();
        }
    }

    static public final class BytesSerde implements Serde<Bytes> {
        @Override
        public Serializer<Bytes> serializer() {
            return new BytesSerializer();
        }

        @Override
        public Deserializer<Bytes> deserializer() {
            return new BytesDeserializer();
        }
    }

    static public final class ByteArraySerde implements Serde<byte[]> {
        @Override
        public Serializer<byte[]> serializer() {
            return new ByteArraySerializer();
        }

        @Override
        public Deserializer<byte[]> deserializer() {
            return new ByteArrayDeserializer();
        }
    }

    @SuppressWarnings("unchecked")
    static public <T> Serde<T> serdeFrom(Class<T> type) {
        if (String.class.isAssignableFrom(type)) {
            return (Serde<T>) String();
        }

        if (Integer.class.isAssignableFrom(type)) {
            return (Serde<T>) Integer();
        }

        if (Long.class.isAssignableFrom(type)) {
            return (Serde<T>) Long();
        }

        if (Double.class.isAssignableFrom(type)) {
            return (Serde<T>) Double();
        }

        if (byte[].class.isAssignableFrom(type)) {
            return (Serde<T>) ByteArray();
        }

        if (ByteBuffer.class.isAssignableFrom(type)) {
            return (Serde<T>) ByteBuffer();
        }

        if (Bytes.class.isAssignableFrom(type)) {
            return (Serde<T>) Bytes();
        }

        // TODO: we can also serializes objects of type T using generic Java serialization by default
        throw new IllegalArgumentException("Unknown class for built-in serializer");
    }

    /**
     * Construct a serde object from separate serializer and deserializer
     *
     * @param serializer    must not be null.
     * @param deserializer  must not be null.
     */
    static public <T> Serde<T> serdeFrom(final Serializer<T> serializer, final Deserializer<T> deserializer) {
        if (serializer == null) {
            throw new IllegalArgumentException("serializer must not be null");
        }
        if (deserializer == null) {
            throw new IllegalArgumentException("deserializer must not be null");
        }

        return new Serde<T>() {
            @Override
            public Serializer<T> serializer() {
                return serializer;
            }

            @Override
            public Deserializer<T> deserializer() {
                return deserializer;
            }
        };
    }

    /*
     * A serde for nullable {@code Long} type.
     */
    static public Serde<Long> Long() {
        return new LongSerde();
    }

    /*
     * A serde for nullable {@code Integer} type.
     */
    static public Serde<Integer> Integer() {
        return new IntegerSerde();
    }

    /*
     * A serde for nullable {@code Double} type.
     */
    static public Serde<Double> Double() {
        return new DoubleSerde();
    }

    /*
     * A serde for nullable {@code String} type.
     */
    static public Serde<String> String() {
        return new StringSerde();
    }

    /*
     * A serde for nullable {@code ByteBuffer} type.
     */
    static public Serde<ByteBuffer> ByteBuffer() {
        return new ByteBufferSerde();
    }

    /*
     * A serde for nullable {@code Bytes} type.
     */
    static public Serde<Bytes> Bytes() {
        return new BytesSerde();
    }

    /*
     * A serde for nullable {@code byte[]} type.
     */
    static public Serde<byte[]> ByteArray() {
        return new ByteArraySerde();
    }
}
