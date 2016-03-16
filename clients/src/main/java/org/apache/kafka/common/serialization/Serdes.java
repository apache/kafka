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
            return (Serde<T>) STRING();
        }

        if (Integer.class.isAssignableFrom(type)) {
            return (Serde<T>) INTEGER();
        }

        if (Long.class.isAssignableFrom(type)) {
            return (Serde<T>) LONG();
        }

        if (byte[].class.isAssignableFrom(type)) {
            return (Serde<T>) BYTE_ARRAY();
        }

        // TODO: we can also serializes objects of type T using generic Java serialization by default
        throw new IllegalArgumentException("Unknown class for built-in serializer");
    }

    static public <T> Serde<T> serdeFrom(final Serializer<T> serializer, final Deserializer<T> deserializer) {
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
     * A serde for nullable long type.
     */
    static public Serde<Long> LONG() {
        return new LongSerde();
    }

    /*
     * A serde for nullable int type.
     */
    static public Serde<Integer> INTEGER() {
        return new IntegerSerde();
    }

    /*
     * A serde for nullable string type.
     */
    static public Serde<String> STRING() {
        return new StringSerde();
    }

    /*
     * A serde for nullable byte array type.
     */
    static public Serde<byte[]> BYTE_ARRAY() {
        return new ByteArraySerde();
    }
}
