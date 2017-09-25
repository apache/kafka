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
package org.apache.kafka.streams.state;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Factory for creating serializers / deserializers for state stores in Kafka Streams.
 *
 * @param <K> key type of serde
 * @param <V> value type of serde
 */
public final class StateSerdes<K, V> {

    /**
     * Create a new instance of {@link StateSerdes} for the given state name and key-/value-type classes.
     *
     * @param topic      the topic name
     * @param keyClass   the class of the key type
     * @param valueClass the class of the value type
     * @param <K>        the key type
     * @param <V>        the value type
     * @return a new instance of {@link StateSerdes}
     */
    public static <K, V> StateSerdes<K, V> withBuiltinTypes(
        final String topic,
        final Class<K> keyClass,
        final Class<V> valueClass) {
        return new StateSerdes<>(topic, Serdes.serdeFrom(keyClass), Serdes.serdeFrom(valueClass));
    }

    private final String topic;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;

    /**
     * Create a context for serialization using the specified serializers and deserializers which
     * <em>must</em> match the key and value types used as parameters for this object; the state changelog topic
     * is provided to bind this serde factory to, so that future calls for serialize / deserialize do not
     * need to provide the topic name any more.
     *
     * @param topic         the topic name
     * @param keySerde      the serde for keys; cannot be null
     * @param valueSerde    the serde for values; cannot be null
     * @throws IllegalArgumentException if key or value serde is null
     */
    public StateSerdes(final String topic,
                       final Serde<K> keySerde,
                       final Serde<V> valueSerde) {
        if (topic == null) {
            throw new IllegalArgumentException("topic cannot be null");
        }
        if (keySerde == null) {
            throw new IllegalArgumentException("key serde cannot be null");
        }
        if (valueSerde == null) {
            throw new IllegalArgumentException("value serde cannot be null");
        }

        this.topic = topic;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    /**
     * Return the key serde.
     *
     * @return the key serde
     */
    public Serde<K> keySerde() {
        return keySerde;
    }

    /**
     * Return the value serde.
     *
     * @return the value serde
     */
    public Serde<V> valueSerde() {
        return valueSerde;
    }

    /**
     * Return the key deserializer.
     *
     * @return the key deserializer
     */
    public Deserializer<K> keyDeserializer() {
        return keySerde.deserializer();
    }

    /**
     * Return the key serializer.
     *
     * @return the key serializer
     */
    public Serializer<K> keySerializer() {
        return keySerde.serializer();
    }

    /**
     * Return the value deserializer.
     *
     * @return the value deserializer
     */
    public Deserializer<V> valueDeserializer() {
        return valueSerde.deserializer();
    }

    /**
     * Return the value serializer.
     *
     * @return the value serializer
     */
    public Serializer<V> valueSerializer() {
        return valueSerde.serializer();
    }

    /**
     * Return the topic.
     *
     * @return the topic
     */
    public String topic() {
        return topic;
    }

    /**
     * Deserialize the key from raw bytes.
     *
     * @param rawKey  the key as raw bytes
     * @return        the key as typed object
     */
    public K keyFrom(byte[] rawKey) {
        return keySerde.deserializer().deserialize(topic, rawKey);
    }

    /**
     * Deserialize the value from raw bytes.
     *
     * @param rawValue  the value as raw bytes
     * @return          the value as typed object
     */
    public V valueFrom(byte[] rawValue) {
        return valueSerde.deserializer().deserialize(topic, rawValue);
    }

    /**
     * Serialize the given key.
     *
     * @param key  the key to be serialized
     * @return     the serialized key
     */
    public byte[] rawKey(K key) {
        return keySerde.serializer().serialize(topic, key);
    }

    /**
     * Serialize the given value.
     *
     * @param value  the value to be serialized
     * @return       the serialized value
     */
    public byte[] rawValue(V value) {
        return valueSerde.serializer().serialize(topic, value);
    }
}
