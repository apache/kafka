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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.internals.SerdeGetter;

import java.util.Map;
import java.util.function.Function;

/**
 * A key {@link Serde} for the {@link Windowed} keys produced by a windowed aggregation.
 * <p>
 * If the user did not specify a key serde, the inner (plain) key serde is the default key serde from the
 * {@code StreamsConfig}, which is only known at runtime. Instead of dropping the windowed serde in that case
 * (which makes downstream operators fall back to the unwrapped default serde and fail with a
 * {@link ClassCastException}), the inner serde is left unset and completed later via {@link #setIfUnset(SerdeGetter)},
 * following the same {@link WrappingNullableSerde} contract as the other wrapping serdes.
 * <p>
 * The serializer is a {@link WindowedSerializer}, so sinks keep partitioning windowed records by their base key.
 */
abstract class LateBoundWindowedSerde<T> extends WrappingNullableSerde<Windowed<T>, T, Void> {

    /**
     * @param inner                the plain key serde, or {@code null} to resolve the default key serde at runtime
     * @param serializerFactory    creates the windowed serializer for a given inner serializer
     * @param deserializerFactory  creates the windowed deserializer for a given inner deserializer
     */
    LateBoundWindowedSerde(final Serde<T> inner,
                           final Function<Serializer<T>, WindowedSerializer<T>> serializerFactory,
                           final Function<Deserializer<T>, Deserializer<Windowed<T>>> deserializerFactory) {
        super(
            new LateBoundSerializer<>(inner == null ? null : inner.serializer(), serializerFactory),
            new LateBoundDeserializer<>(inner == null ? null : inner.deserializer(), deserializerFactory)
        );
    }

    private static final class LateBoundSerializer<T>
        implements WindowedSerializer<T>, WrappingNullableSerializer<Windowed<T>, T, Void> {

        private final Function<Serializer<T>, WindowedSerializer<T>> factory;
        // Every thread resolves an equivalent default serde, so racing setIfUnset() calls are harmless.
        private volatile WindowedSerializer<T> delegate;

        LateBoundSerializer(final Serializer<T> inner, final Function<Serializer<T>, WindowedSerializer<T>> factory) {
            this.factory = factory;
            this.delegate = inner == null ? null : factory.apply(inner);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void setIfUnset(final SerdeGetter getter) {
            if (delegate == null) {
                delegate = factory.apply((Serializer<T>) getter.keySerde().serializer());
            }
        }

        private WindowedSerializer<T> delegate() {
            final WindowedSerializer<T> current = delegate;
            if (current == null) {
                throw new IllegalStateException("The inner key serializer of this windowed serializer has not been set yet");
            }
            return current;
        }

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {
            final WindowedSerializer<T> current = delegate;
            if (current != null) {
                current.configure(configs, isKey);
            }
        }

        @Override
        public byte[] serialize(final String topic, final Windowed<T> data) {
            return delegate().serialize(topic, data);
        }

        @Override
        public byte[] serialize(final String topic, final Headers headers, final Windowed<T> data) {
            return delegate().serialize(topic, headers, data);
        }

        @Override
        public byte[] serializeBaseKey(final String topic, final Windowed<T> data) {
            return delegate().serializeBaseKey(topic, data);
        }

        @Override
        public byte[] serializeBaseKey(final String topic, final Headers headers, final Windowed<T> data) {
            return delegate().serializeBaseKey(topic, headers, data);
        }

        @Override
        public void close() {
            final WindowedSerializer<T> current = delegate;
            if (current != null) {
                current.close();
            }
        }
    }

    private static final class LateBoundDeserializer<T>
        implements WrappingNullableDeserializer<Windowed<T>, T, Void> {

        private final Function<Deserializer<T>, Deserializer<Windowed<T>>> factory;
        // Every thread resolves an equivalent default serde, so racing setIfUnset() calls are harmless.
        private volatile Deserializer<Windowed<T>> delegate;

        LateBoundDeserializer(final Deserializer<T> inner, final Function<Deserializer<T>, Deserializer<Windowed<T>>> factory) {
            this.factory = factory;
            this.delegate = inner == null ? null : factory.apply(inner);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void setIfUnset(final SerdeGetter getter) {
            if (delegate == null) {
                delegate = factory.apply((Deserializer<T>) getter.keySerde().deserializer());
            }
        }

        private Deserializer<Windowed<T>> delegate() {
            final Deserializer<Windowed<T>> current = delegate;
            if (current == null) {
                throw new IllegalStateException("The inner key deserializer of this windowed deserializer has not been set yet");
            }
            return current;
        }

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {
            final Deserializer<Windowed<T>> current = delegate;
            if (current != null) {
                current.configure(configs, isKey);
            }
        }

        @Override
        public Windowed<T> deserialize(final String topic, final byte[] data) {
            return delegate().deserialize(topic, data);
        }

        @Override
        public Windowed<T> deserialize(final String topic, final Headers headers, final byte[] data) {
            return delegate().deserialize(topic, headers, data);
        }

        @Override
        public void close() {
            final Deserializer<Windowed<T>> current = delegate;
            if (current != null) {
                current.close();
            }
        }
    }
}
