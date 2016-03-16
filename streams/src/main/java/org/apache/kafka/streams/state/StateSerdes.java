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
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Factory for creating serializers / deserializers for state stores in Kafka Streams.
 *
 * @param <K> key type of serdes
 * @param <V> value type of serdes
 */
public final class StateSerdes<K, V> {

    public static <K, V> StateSerdes<K, V> withBuiltinTypes(String topic, Class<K> keyClass, Class<V> valueClass) {
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
     * @param topic the name of the topic
     * @param keySerde the serde for keys; may be null
     * @param valueSerde the serde for values; may be null
     */
    @SuppressWarnings("unchecked")
    public StateSerdes(String topic,
                       Serde<K> keySerde,
                       Serde<V> valueSerde) {
        this.topic = topic;

        if (keySerde == null)
            throw new NullPointerException();
        if (valueSerde == null)
            throw new NullPointerException();

        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    public Deserializer<K> keyDeserializer() {
        return keySerde.deserializer();
    }

    public Serializer<K> keySerializer() {
        return keySerde.serializer();
    }

    public Deserializer<V> valueDeserializer() {
        return valueSerde.deserializer();
    }

    public Serializer<V> valueSerializer() {
        return valueSerde.serializer();
    }

    public String topic() {
        return topic;
    }

    public K keyFrom(byte[] rawKey) {
        return keySerde.deserializer().deserialize(topic, rawKey);
    }

    public V valueFrom(byte[] rawValue) {
        return valueSerde.deserializer().deserialize(topic, rawValue);
    }

    public byte[] rawKey(K key) {
        return keySerde.serializer().serialize(topic, key);
    }

    public byte[] rawValue(V value) {
        return valueSerde.serializer().serialize(topic, value);
    }
}
