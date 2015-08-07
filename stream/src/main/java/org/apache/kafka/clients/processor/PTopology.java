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

import org.apache.kafka.clients.processor.internals.KafkaSource;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

abstract public class PTopology {

    private List<KafkaProcessor> processors = new ArrayList<>();
    private Map<String, KafkaSource> sources = new HashMap<>();

    public Set<KafkaSource> sources() {
        Set<KafkaSource> sources = new HashSet<>();
        for (KafkaSource source : this.sources.values()) {
            sources.add(source);
        }

        return sources;
    }

    public Set<String> topics() {
        return sources.keySet();
    }

    public KafkaSource source(String topic) {
        return sources.get(topic);
    }

    public Deserializer keyDeser(String topic) {
        KafkaSource source = sources.get(topic);

        if (source == null)
            throw new IllegalStateException("The topic " + topic + " is unknown.");

        return source.keyDeserializer;
    }

    public Deserializer valueDeser(String topic) {
        KafkaSource source = sources.get(topic);

        if (source == null)
            throw new IllegalStateException("The topic " + topic + " is unknown.");

        return source.valDeserializer;
    }

    public final void init(ProcessorContext context) {
        for (KafkaProcessor processor : processors) {
            processor.init(context);
        }
    }

    public final <K, V> KafkaSource<K, V> addSource(Deserializer<K> keyDeserializer, Deserializer<V> valDeserializer, String... topics) {
        KafkaSource<K, V> source = new KafkaSource<>(keyDeserializer, valDeserializer);

        processors.add(source);

        for (String topic : topics) {
            if (sources.containsKey(topic))
                throw new IllegalArgumentException("Topic " + topic + " has already been registered by another processor.");

            sources.put(topic, source);
        }

        return source;
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

    public final void close() {
        for (KafkaProcessor processor : processors) {
            processor.close();
        }

        processors.clear();
        sources.clear();
    }

    abstract public void build();
}
