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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serialization;
import org.apache.kafka.common.serialization.Serializations;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Factory for creating serializers / deserializers for state stores in Kafka Streams.
 *
 * @param <K> key type of serdes
 * @param <V> value type of serdes
 */
public final class StateSerdes<K, V> {

    public static <K, V> StateSerdes<K, V> withBuiltinTypes(String topic, Class<K> keyClass, Class<V> valueClass) {
        return new StateSerdes<>(topic, Serializations.serialization(keyClass), Serializations.serialization(valueClass));
    }

    private final String topic;
    private final Serialization<K> keySerialization;
    private final Serialization<V> valueSerialization;

    /**
     * Create a context for serialization using the specified serializers and deserializers which
     * <em>must</em> match the key and value types used as parameters for this object; the state changelog topic
     * is provided to bind this serde factory to, so that future calls for serialize / deserialize do not
     * need to provide the topic name any more.
     *
     * @param topic the name of the topic
     * @param keySerialization the serde for keys; may be null
     * @param valueSerialization the serde for values; may be null
     */
    @SuppressWarnings("unchecked")
    public StateSerdes(String topic,
                       Serialization<K> keySerialization,
                       Serialization<V> valueSerialization) {
        this.topic = topic;

        if (keySerialization == null)
            throw new NullPointerException();
        if (valueSerialization == null)
            throw new NullPointerException();

        this.keySerialization = keySerialization;
        this.valueSerialization = valueSerialization;
    }

    public Deserializer<K> keyDeserializer() {
        return keySerialization.deserializer();
    }

    public Serializer<K> keySerializer() {
        return keySerialization.serializer();
    }

    public Deserializer<V> valueDeserializer() {
        return valueSerialization.deserializer();
    }

    public Serializer<V> valueSerializer() {
        return valueSerialization.serializer();
    }

    public String topic() {
        return topic;
    }

    public K keyFrom(byte[] rawKey) {
        return keySerialization.deserializer().deserialize(topic, rawKey);
    }

    public V valueFrom(byte[] rawValue) {
        return valueSerialization.deserializer().deserialize(topic, rawValue);
    }

    public byte[] rawKey(K key) {
        return keySerialization.serializer().serialize(topic, key);
    }

    public byte[] rawValue(V value) {
        return valueSerialization.serializer().serialize(topic, value);
    }
}
