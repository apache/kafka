/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import org.apache.kafka.streams.kstream.Windowed;

import java.util.Map;

/**
 * Serde for a {@link Windowed} key when working with {@link org.apache.kafka.streams.kstream.SessionWindows}
 *
 * @param <K> sessionId type
 */
public class SessionKeySerde<K> implements Serde<Windowed<K>> {

    private final Serde<K> keySerde;

    public SessionKeySerde(final Serde<K> keySerde) {
        this.keySerde = keySerde;
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<Windowed<K>> serializer() {
        return new SessionKeySerializer(keySerde.serializer());
    }

    @Override
    public Deserializer<Windowed<K>> deserializer() {
        return new SessionKeyDeserializer(keySerde.deserializer());
    }

    private class SessionKeySerializer implements Serializer<Windowed<K>> {

        private final Serializer<K> keySerializer;

        SessionKeySerializer(final Serializer<K> keySerializer) {
            this.keySerializer = keySerializer;
        }

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {

        }

        @Override
        public byte[] serialize(final String topic, final Windowed<K> data) {
            if (data == null) {
                return null;
            }
            return SessionKeyBinaryConverter.toBinary(data, keySerializer).get();
        }

        @Override
        public void close() {

        }
    }

    private class SessionKeyDeserializer implements Deserializer<Windowed<K>> {
        private final Deserializer<K> deserializer;

        SessionKeyDeserializer(final Deserializer<K> deserializer) {
            this.deserializer = deserializer;
        }

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {
        }

        @Override
        public Windowed<K> deserialize(final String topic, final byte[] data) {
            if (data == null || data.length == 0) {
                return null;
            }
            return SessionKeyBinaryConverter.from(data, deserializer);
        }


        @Override
        public void close() {

        }
    }
}
