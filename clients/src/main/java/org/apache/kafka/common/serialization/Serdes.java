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
public class SerDes {

    static private final Serializer<Long> longSerializer = new LongSerializer();
    static private final Deserializer<Long> longDeserializer = new LongDeserializer();

    static private final Serializer<Integer> integerSerializer = new IntegerSerializer();
    static private final Deserializer<Integer> integerDeserializer = new IntegerDeserializer();

    static private final Serializer<String> stringSerializer = new StringSerializer();
    static private final Deserializer<String> stringDeserializer = new StringDeserializer();

    static private final Serializer<byte[]> bytesSerializer = new ByteArraySerializer();
    static private final Deserializer<byte[]> bytesDeserializer = new ByteArrayDeserializer();


    static public final class LongSerDe implements SerDe<Long> {

        @Override
        public Serializer<Long> serializer() {
            return longSerializer;
        }

        @Override
        public Deserializer<Long> deserializer() {
            return longDeserializer;
        }
    }

    static public final class IntegerSerDe implements SerDe<Integer> {
        @Override
        public Serializer<Integer> serializer() {
            return integerSerializer;
        }

        @Override
        public Deserializer<Integer> deserializer() {
            return integerDeserializer;
        }
    }

    static public final class StringSerDe implements SerDe<String> {
        @Override
        public Serializer<String> serializer() {
            return stringSerializer;
        }

        @Override
        public Deserializer<String> deserializer() {
            return stringDeserializer;
        }
    }

    static public final class ByteArraySerDe implements SerDe<byte[]> {
        @Override
        public Serializer<byte[]> serializer() {
            return bytesSerializer;
        }

        @Override
        public Deserializer<byte[]> deserializer() {
            return bytesDeserializer;
        }
    }

    @SuppressWarnings("unchecked")
    static public <T> SerDe<T> serialization(Class<T> type) {
        if (String.class.isAssignableFrom(type)) {
            return (SerDe<T>) STRING();
        }

        if (Integer.class.isAssignableFrom(type)) {
            return (SerDe<T>) INTEGER();
        }

        if (Long.class.isAssignableFrom(type)) {
            return (SerDe<T>) LONG();
        }

        if (byte[].class.isAssignableFrom(type)) {
            return (SerDe<T>) BYTE_ARRAY();
        }

        // TODO: we can also serializes objects of type T using generic Java serialization by default
        throw new IllegalArgumentException("Unknown class for built-in serializer");
    }

    static public <T> SerDe<T> serialization(final Serializer<T> serializer, final Deserializer<T> deserializer) {
        return new SerDe<T>() {
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
    static public SerDe<Long> LONG() {
        return new LongSerDe();
    }

    /*
     * A serde for nullable long type.
     */
    static public SerDe<Integer> INTEGER() {
        return new IntegerSerDe();
    }

    /*
     * A serde for nullable string type.
     */
    static public SerDe<String> STRING() {
        return new StringSerDe();
    }

    /*
     * A serde for nullable byte array type.
     */
    static public SerDe<byte[]> BYTE_ARRAY() {
        return new ByteArraySerDe();
    }
}
