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
package org.apache.kafka.common.serialization;

import org.apache.kafka.common.utils.Bytes;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Factory for creating serializers / deserializers.
 */
public class Serdes {

    public static class WrapperSerde<T> implements Serde<T> {
        private final Serializer<T> serializer;
        private final Deserializer<T> deserializer;

        public WrapperSerde(Serializer<T> serializer, Deserializer<T> deserializer) {
            this.serializer = serializer;
            this.deserializer = deserializer;
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            serializer.configure(configs, isKey);
            deserializer.configure(configs, isKey);
        }

        @Override
        public void close() {
            serializer.close();
            deserializer.close();
        }

        @Override
        public Serializer<T> serializer() {
            return serializer;
        }

        @Override
        public Deserializer<T> deserializer() {
            return deserializer;
        }
    }

    public static final class VoidSerde extends WrapperSerde<Void> {
        public VoidSerde() {
            super(new VoidSerializer(), new VoidDeserializer());
        }
    }

    public static final class LongSerde extends WrapperSerde<Long> {
        public LongSerde() {
            super(new LongSerializer(), new LongDeserializer());
        }
    }

    public static final class IntegerSerde extends WrapperSerde<Integer> {
        public IntegerSerde() {
            super(new IntegerSerializer(), new IntegerDeserializer());
        }
    }

    public static final class ShortSerde extends WrapperSerde<Short> {
        public ShortSerde() {
            super(new ShortSerializer(), new ShortDeserializer());
        }
    }

    public static final class FloatSerde extends WrapperSerde<Float> {
        public FloatSerde() {
            super(new FloatSerializer(), new FloatDeserializer());
        }
    }

    public static final class DoubleSerde extends WrapperSerde<Double> {
        public DoubleSerde() {
            super(new DoubleSerializer(), new DoubleDeserializer());
        }
    }

    public static final class StringSerde extends WrapperSerde<String> {
        public StringSerde() {
            super(new StringSerializer(), new StringDeserializer());
        }
    }

    public static final class ByteBufferSerde extends WrapperSerde<ByteBuffer> {
        public ByteBufferSerde() {
            super(new ByteBufferSerializer(), new ByteBufferDeserializer());
        }
    }

    public static final class BytesSerde extends WrapperSerde<Bytes> {
        public BytesSerde() {
            super(new BytesSerializer(), new BytesDeserializer());
        }
    }

    public static final class ByteArraySerde extends WrapperSerde<byte[]> {
        public ByteArraySerde() {
            super(new ByteArraySerializer(), new ByteArrayDeserializer());
        }
    }

    public static final class UUIDSerde extends WrapperSerde<UUID> {
        public UUIDSerde() {
            super(new UUIDSerializer(), new UUIDDeserializer());
        }
    }

    public static final class BooleanSerde extends WrapperSerde<Boolean> {
        public BooleanSerde() {
            super(new BooleanSerializer(), new BooleanDeserializer());
        }
    }

    public static final class ListSerde<Inner> extends WrapperSerde<List<Inner>> {

        static final int NULL_ENTRY_VALUE = -1;

        enum SerializationStrategy {
            CONSTANT_SIZE,
            VARIABLE_SIZE;

            public static final SerializationStrategy[] VALUES = SerializationStrategy.values();
        }

        public ListSerde() {
            super(new ListSerializer<>(), new ListDeserializer<>());
        }

        public <L extends List<Inner>> ListSerde(Class<L> listClass, Serde<Inner> serde) {
            super(new ListSerializer<>(serde.serializer()), new ListDeserializer<>(listClass, serde.deserializer()));
        }

    }

    @SuppressWarnings("unchecked")
    public static <T> Serde<T> serdeFrom(Class<T> type) {
        if (String.class.isAssignableFrom(type)) {
            return (Serde<T>) String();
        }

        if (Short.class.isAssignableFrom(type)) {
            return (Serde<T>) Short();
        }

        if (Integer.class.isAssignableFrom(type)) {
            return (Serde<T>) Integer();
        }

        if (Long.class.isAssignableFrom(type)) {
            return (Serde<T>) Long();
        }

        if (Float.class.isAssignableFrom(type)) {
            return (Serde<T>) Float();
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

        if (UUID.class.isAssignableFrom(type)) {
            return (Serde<T>) UUID();
        }

        if (Boolean.class.isAssignableFrom(type)) {
            return (Serde<T>) Boolean();
        }

        // TODO: we can also serializes objects of type T using generic Java serialization by default
        throw new IllegalArgumentException("Unknown class for built-in serializer. Supported types are: " +
            "String, Short, Integer, Long, Float, Double, ByteArray, ByteBuffer, Bytes, UUID, Boolean");
    }

    /**
     * Construct a serde object from separate serializer and deserializer
     *
     * @param serializer    must not be null.
     * @param deserializer  must not be null.
     */
    public static <T> Serde<T> serdeFrom(final Serializer<T> serializer, final Deserializer<T> deserializer) {
        if (serializer == null) {
            throw new IllegalArgumentException("serializer must not be null");
        }
        if (deserializer == null) {
            throw new IllegalArgumentException("deserializer must not be null");
        }

        return new WrapperSerde<>(serializer, deserializer);
    }

    /**
     * A serde for nullable {@code Long} type.
     */
    public static Serde<Long> Long() {
        return new LongSerde();
    }

    /**
     * A serde for nullable {@code Integer} type.
     */
    public static Serde<Integer> Integer() {
        return new IntegerSerde();
    }

    /**
     * A serde for nullable {@code Short} type.
     */
    public static Serde<Short> Short() {
        return new ShortSerde();
    }

    /**
     * A serde for nullable {@code Float} type.
     */
    public static Serde<Float> Float() {
        return new FloatSerde();
    }

    /**
     * A serde for nullable {@code Double} type.
     */
    public static Serde<Double> Double() {
        return new DoubleSerde();
    }

    /**
     * A serde for nullable {@code String} type.
     */
    public static Serde<String> String() {
        return new StringSerde();
    }

    /**
     * A serde for nullable {@code ByteBuffer} type.
     */
    public static Serde<ByteBuffer> ByteBuffer() {
        return new ByteBufferSerde();
    }

    /**
     * A serde for nullable {@code Bytes} type.
     */
    public static Serde<Bytes> Bytes() {
        return new BytesSerde();
    }

    /**
     * A serde for nullable {@code UUID} type
     */
    public static Serde<UUID> UUID() {
        return new UUIDSerde();
    }

    /**
     * A serde for nullable {@code Boolean} type.
     */
    public static Serde<Boolean> Boolean() {
        return new BooleanSerde();
    }

    /**
     * A serde for nullable {@code byte[]} type.
     */
    public static Serde<byte[]> ByteArray() {
        return new ByteArraySerde();
    }

    /**
     * A serde for {@code Void} type.
     */
    public static Serde<Void> Void() {
        return new VoidSerde();
    }

    /*
     * A serde for {@code List} type
     */
    public static <L extends List<Inner>, Inner> Serde<List<Inner>> ListSerde(Class<L> listClass, Serde<Inner> innerSerde) {
        return new ListSerde<>(listClass, innerSerde);
    }

}
