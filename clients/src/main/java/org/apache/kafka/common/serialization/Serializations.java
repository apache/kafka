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
public class Serializations {

    static public class LongSerialization implements Serialization<Long> {
        private Serializer<Long> serializer = new LongSerializer();
        private Deserializer<Long> deserializer = new LongDeserializer();

        @Override
        public Serializer<Long> serializer() {
            return serializer;
        }

        @Override
        public Deserializer<Long> deserializer() {
            return deserializer;
        }
    }

    static public class IntegerSerialization implements Serialization<Integer> {
        private Serializer<Integer> serializer = new IntegerSerializer();
        private Deserializer<Integer> deserializer = new IntegerDeserializer();

        @Override
        public Serializer<Integer> serializer() {
            return serializer;
        }

        @Override
        public Deserializer<Integer> deserializer() {
            return deserializer;
        }
    }

    static public class StringSerialization implements Serialization<String> {
        private Serializer<String> serializer = new StringSerializer();
        private Deserializer<String> deserializer = new StringDeserializer();

        @Override
        public Serializer<String> serializer() {
            return serializer;
        }

        @Override
        public Deserializer<String> deserializer() {
            return deserializer;
        }
    }

    static public class ByteArraySerialization implements Serialization<byte[]> {
        private Serializer<byte[]> serializer = new ByteArraySerializer();
        private Deserializer<byte[]> deserializer = new ByteArrayDeserializer();

        @Override
        public Serializer<byte[]> serializer() {
            return serializer;
        }

        @Override
        public Deserializer<byte[]> deserializer() {
            return deserializer;
        }
    }

    @SuppressWarnings("unchecked")
    static public <T> Serialization<T> serialization(Class<T> type) {
        if (String.class.isAssignableFrom(type)) {
            return (Serialization<T>) STRING();
        }

        if (Integer.class.isAssignableFrom(type)) {
            return (Serialization<T>) INT();
        }

        if (Long.class.isAssignableFrom(type)) {
            return (Serialization<T>) LONG();
        }

        if (byte[].class.isAssignableFrom(type)) {
            return (Serialization<T>) BYTEARRAY();
        }

        // TODO: we can also serializes objects of type T using generic Java serialization by default
        throw new IllegalArgumentException("Unknown class for built-in serializer");
    }

    static public <T> Serialization<T> serialization(final Serializer<T> serializer, final Deserializer<T> deserializer) {
        return new Serialization<T>() {
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
    static public Serialization<Long> LONG() {
        return new LongSerialization();
    }

    /*
     * A serde for nullable long type.
     */
    static public Serialization<Integer> INT() {
        return new IntegerSerialization();
    }

    /*
     * A serde for nullable string type.
     */
    static public Serialization<String> STRING() {
        return new StringSerialization();
    }

    /*
     * A serde for nullable byte array type.
     */
    static public Serialization<byte[]> BYTEARRAY() {
        return new ByteArraySerialization();
    }
}
