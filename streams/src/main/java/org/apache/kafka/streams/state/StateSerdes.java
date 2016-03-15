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
import org.apache.kafka.common.serialization.SerDe;
import org.apache.kafka.common.serialization.SerDes;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Factory for creating serializers / deserializers for state stores in Kafka Streams.
 *
 * @param <K> key type of serdes
 * @param <V> value type of serdes
 */
public final class StateSerdes<K, V> {

    public static <K, V> StateSerdes<K, V> withBuiltinTypes(String topic, Class<K> keyClass, Class<V> valueClass) {
        return new StateSerdes<>(topic, SerDes.serialization(keyClass), SerDes.serialization(valueClass));
    }

    private final String topic;
    private final SerDe<K> keySerDe;
    private final SerDe<V> valueSerDe;

    /**
     * Create a context for serialization using the specified serializers and deserializers which
     * <em>must</em> match the key and value types used as parameters for this object; the state changelog topic
     * is provided to bind this serde factory to, so that future calls for serialize / deserialize do not
     * need to provide the topic name any more.
     *
     * @param topic the name of the topic
     * @param keySerDe the serde for keys; may be null
     * @param valueSerDe the serde for values; may be null
     */
    @SuppressWarnings("unchecked")
    public StateSerdes(String topic,
                       SerDe<K> keySerDe,
                       SerDe<V> valueSerDe) {
        this.topic = topic;

        if (keySerDe == null)
            throw new NullPointerException();
        if (valueSerDe == null)
            throw new NullPointerException();

        this.keySerDe = keySerDe;
        this.valueSerDe = valueSerDe;
    }

    public Deserializer<K> keyDeserializer() {
        return keySerDe.deserializer();
    }

    public Serializer<K> keySerializer() {
        return keySerDe.serializer();
    }

    public Deserializer<V> valueDeserializer() {
        return valueSerDe.deserializer();
    }

    public Serializer<V> valueSerializer() {
        return valueSerDe.serializer();
    }

    public String topic() {
        return topic;
    }

    public K keyFrom(byte[] rawKey) {
        return keySerDe.deserializer().deserialize(topic, rawKey);
    }

    public V valueFrom(byte[] rawValue) {
        return valueSerDe.deserializer().deserialize(topic, rawValue);
    }

    public byte[] rawKey(K key) {
        return keySerDe.serializer().serialize(topic, key);
    }

    public byte[] rawValue(V value) {
        return valueSerDe.serializer().serialize(topic, value);
    }
}
