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

public class SubscriptionResponseWrapperSerde<V> implements Serde<SubscriptionResponseWrapper<V>> {
    private final SubscriptionResponseWrapperSerializer<V> serializer;
    private final SubscriptionResponseWrapperDeserializer<V> deserializer;

    public SubscriptionResponseWrapperSerde(final Serializer<V> serializer,
                                            final Deserializer<V> deserializer) {
        this.serializer = new SubscriptionResponseWrapperSerializer<>(serializer);
        this.deserializer = new SubscriptionResponseWrapperDeserializer<>(deserializer);
    }

    @Override
    public void configure(final Map configs, final boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<SubscriptionResponseWrapper<V>> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<SubscriptionResponseWrapper<V>> deserializer() {
        return deserializer;
    }

    private static class SubscriptionResponseWrapperSerializer<V> implements Serializer<SubscriptionResponseWrapper<V>> {
        private final Serializer<V> serializer;

        public SubscriptionResponseWrapperSerializer(final Serializer<V> serializer) {
            this.serializer = serializer;
        }

        @Override
        public void configure(final Map configs, final boolean isKey) {
            //Do nothing
        }

        @Override
        public byte[] serialize(final String topic, final SubscriptionResponseWrapper<V> data) {
            //{16-bytes Hash}{n-bytes serialized data}
            final byte[] serializedData = serializer.serialize(topic, data.getForeignValue());
            final int length = serializedData == null ? 0 : serializedData.length;
            final ByteBuffer buf = ByteBuffer.allocate(16 + length);
            final long[] elem = data.getOriginalValueHash();
            buf.putLong(elem[0]);
            buf.putLong(elem[1]);
            if (serializedData != null)
                buf.put(serializedData);
            return buf.array();
        }

        @Override
        public void close() {
            //Do nothing
        }
    }

    private static class SubscriptionResponseWrapperDeserializer<V> implements Deserializer<SubscriptionResponseWrapper<V>> {
        final private Deserializer<V> deserializer;

        public SubscriptionResponseWrapperDeserializer(final Deserializer<V> deserializer) {
            this.deserializer = deserializer;
        }

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {
            //Do nothing
        }

        @Override
        public SubscriptionResponseWrapper<V> deserialize(final String topic, final byte[] data) {
            //{16-bytes Hash}{n-bytes serialized data}
            final int size = 16;
            final ByteBuffer buf = ByteBuffer.wrap(data);
            final long[] hash = new long[2];
            hash[0] = buf.getLong();
            hash[1] = buf.getLong();
            final byte[] serializedValue = data.length == size ? null : new byte[data.length - size];
            if (serializedValue != null)
                buf.get(serializedValue, 0, data.length - size);
            final V value = deserializer.deserialize(topic, serializedValue);
            return new SubscriptionResponseWrapper<>(hash, value);
        }

        @Override
        public void close() {
            //Do nothing
        }
    }

}
