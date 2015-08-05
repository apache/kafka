/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.stream.topology.internals;

import org.apache.kafka.stream.KStreamContext;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.stream.topology.KStreamTopology;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class KStreamSource<K, V> extends KStreamImpl<K, V> {

    private Deserializer<K> keyDeserializer;
    private Deserializer<V> valueDeserializer;

    public String[] topics;

    public KStreamSource(String[] topics, KStreamTopology topology) {
        this(topics, null, null, topology);
    }

    public KStreamSource(String[] topics, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, KStreamTopology topology) {
        super(topology);
        this.topics = topics;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void bind(KStreamContext context, KStreamMetadata metadata) {
        if (keyDeserializer == null) keyDeserializer = (Deserializer<K>) context.keyDeserializer();
        if (valueDeserializer == null) valueDeserializer = (Deserializer<V>) context.valueDeserializer();

        super.bind(context, metadata);
    }

    @Override
    public void receive(Object key, Object value, long timestamp) {
        synchronized (this) {
            // KStream needs to forward the topic name since it is directly from the Kafka source
            forward(key, value, timestamp);
        }
    }

    public Deserializer<K> keyDeserializer() {
        return keyDeserializer;
    }

    public Deserializer<V> valueDeserializer() {
        return valueDeserializer;
    }

    public Set<String> topics() {
        return new HashSet<>(Arrays.asList(topics));
    }

}
