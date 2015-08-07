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

package org.apache.kafka.clients.processor;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class PTopology {

    private class KeyValueDeserializers<K, V> {
        public Deserializer<K> keyDeserializer;
        public Deserializer<V> valDeserializer;

        public KeyValueDeserializers(Deserializer<K> keyDeserializer, Deserializer<V> valDeserializer) {
            this.keyDeserializer = keyDeserializer;
            this.valDeserializer = valDeserializer;
        }
    }

    List<KafkaProcessor> sources = new ArrayList<>();
    List<KafkaProcessor> processors = new ArrayList<>();
    Map<String, KeyValueDeserializers> topicDesers = new HashMap<>();

    public List<KafkaProcessor> sources() {
        return sources;
    }

    public Set<String> topics() {
        return topicDesers.keySet();
    }

    public Deserializer keyDeser(String topic) {
        KeyValueDeserializers desers = topicDesers.get(topic);

        if (desers == null)
            throw new IllegalStateException("The topic " + topic + " is unknown.");

        return desers.keyDeserializer;
    }

    public Deserializer valueDeser(String topic) {
        KeyValueDeserializers desers = topicDesers.get(topic);

        if (desers == null)
            throw new IllegalStateException("The topic " + topic + " is unknown.");

        return desers.valDeserializer;
    }

    public final <K, V> void addProcessor(KafkaProcessor<K, V, ?, ?> processor, Deserializer<K> keyDeserializer, Deserializer<V> valDeserializer, String... topics) {
        if (processors.contains(processor))
            throw new IllegalArgumentException("Processor " + processor.name() + " is already added.");

        processors.add(processor);

        for (String topic : topics) {
            if (topicDesers.containsKey(topic))
                throw new IllegalArgumentException("Topic " + topic + " has already been registered by another processor.");

            topicDesers.put(topic, new KeyValueDeserializers<K, V>(keyDeserializer, valDeserializer));
        }

        sources.add(processor);
    }

    public final <K, V> void addProcessor(KafkaProcessor<K, V, ?, ?> processor, KafkaProcessor<?, ?, K, V>... parents) {
        if (processors.contains(processor))
            throw new IllegalArgumentException("Processor " + processor.name() + " is already added.");

        processors.add(processor);

        if (parents != null) {
            for (KafkaProcessor<?, ?, K, V> parent : parents) {
                if (!processors.contains(parent))
                    throw new IllegalArgumentException("Parent processor " + parent.name() + " is not added yet.");

                parent.chain(processor);
            }
        }
    }

    public abstract void build();
}
