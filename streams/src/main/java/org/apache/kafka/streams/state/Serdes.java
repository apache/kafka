/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.processor.ProcessorContext;

public final class Serdes<K, V> {

    public static <K, V> Serdes<K, V> withBuiltinTypes(String topic, Class<K> keyClass, Class<V> valueClass) {
        Serializer<K> keySerializer = serializer(keyClass);
        Deserializer<K> keyDeserializer = deserializer(keyClass);
        Serializer<V> valueSerializer = serializer(valueClass);
        Deserializer<V> valueDeserializer = deserializer(valueClass);
        return new Serdes<>(topic, keySerializer, keyDeserializer, valueSerializer, valueDeserializer);
    }

    @SuppressWarnings("unchecked")
    static <T> Serializer<T> serializer(Class<T> type) {
        if (String.class.isAssignableFrom(type)) return (Serializer<T>) new StringSerializer();
        if (Integer.class.isAssignableFrom(type)) return (Serializer<T>) new IntegerSerializer();
        if (Long.class.isAssignableFrom(type)) return (Serializer<T>) new LongSerializer();
        if (byte[].class.isAssignableFrom(type)) return (Serializer<T>) new ByteArraySerializer();
        throw new IllegalArgumentException("Unknown class for built-in serializer");
    }

    @SuppressWarnings("unchecked")
    static <T> Deserializer<T> deserializer(Class<T> type) {
        if (String.class.isAssignableFrom(type)) return (Deserializer<T>) new StringDeserializer();
        if (Integer.class.isAssignableFrom(type)) return (Deserializer<T>) new IntegerDeserializer();
        if (Long.class.isAssignableFrom(type)) return (Deserializer<T>) new LongDeserializer();
        if (byte[].class.isAssignableFrom(type)) return (Deserializer<T>) new ByteArrayDeserializer();
        throw new IllegalArgumentException("Unknown class for built-in serializer");
    }

    private final String topic;
    private Serializer<K> keySerializer;
    private Serializer<V> valueSerializer;
    private Deserializer<K> keyDeserializer;
    private Deserializer<V> valueDeserializer;

    /**
     * Create a context for serialization using the specified serializers and deserializers, or if any of them are null the
     * corresponding {@link ProcessorContext}'s serializer or deserializer, which
     * <em>must</em> match the key and value types used as parameters for this object.
     *
     * @param topic the name of the topic
     * @param keySerializer the serializer for keys; may be null
     * @param keyDeserializer the deserializer for keys; may be null
     * @param valueSerializer the serializer for values; may be null
     * @param valueDeserializer the deserializer for values; may be null
     */
    @SuppressWarnings("unchecked")
    public Serdes(String topic,
            Serializer<K> keySerializer, Deserializer<K> keyDeserializer,
            Serializer<V> valueSerializer, Deserializer<V> valueDeserializer) {
        this.topic = topic;
        this.keySerializer = keySerializer;
        this.keyDeserializer = keyDeserializer;
        this.valueSerializer = valueSerializer;
        this.valueDeserializer = valueDeserializer;
    }

    /**
     * Create a context for serialization using the {@link ProcessorContext}'s serializers and deserializers, which
     * <em>must</em> match the key and value types used as parameters for this object.
     *
     * @param topic the name of the topic
     */
    @SuppressWarnings("unchecked")
    public Serdes(String topic) {
        this(topic, null, null, null, null);
    }

    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        keySerializer = keySerializer != null ? keySerializer : (Serializer<K>) context.keySerializer();
        keyDeserializer = keyDeserializer != null ? keyDeserializer : (Deserializer<K>) context.keyDeserializer();
        valueSerializer = valueSerializer != null ? valueSerializer : (Serializer<V>) context.valueSerializer();
        valueDeserializer = valueDeserializer != null ? valueDeserializer : (Deserializer<V>) context.valueDeserializer();
    }

    public Deserializer<K> keyDeserializer() {
        return keyDeserializer;
    }

    public Serializer<K> keySerializer() {
        return keySerializer;
    }

    public Deserializer<V> valueDeserializer() {
        return valueDeserializer;
    }

    public Serializer<V> valueSerializer() {
        return valueSerializer;
    }

    public String topic() {
        return topic;
    }

    public K keyFrom(byte[] rawKey) {
        return keyDeserializer.deserialize(topic, rawKey);
    }

    public V valueFrom(byte[] rawValue) {
        return valueDeserializer.deserialize(topic, rawValue);
    }

    public byte[] rawKey(K key) {
        return keySerializer.serialize(topic, key);
    }

    public byte[] rawValue(V value) {
        return valueSerializer.serialize(topic, value);
    }
}
