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

package org.apache.kafka.streaming.processor;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streaming.processor.internals.ProcessorTopology;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TopologyBuilder {

    private Map<String, ProcessorClazz> processorClasses = new HashMap<>();
    private Map<String, SourceClazz> sourceClasses = new HashMap<>();
    private Map<String, String> topicsToSourceNames = new HashMap<>();

    private Map<String, List<String>> parents = new HashMap<>();
    private Map<String, List<String>> children = new HashMap<>();

    private class ProcessorClazz {
        public Class<? extends KafkaProcessor> clazz;
        public ProcessorMetadata metadata;

        public ProcessorClazz(Class<? extends KafkaProcessor> clazz, ProcessorMetadata metadata) {
            this.clazz = clazz;
            this.metadata = metadata;
        }
    }

    private class SourceClazz {
        public Deserializer keyDeserializer;
        public Deserializer valDeserializer;

        private SourceClazz(Deserializer keyDeserializer, Deserializer valDeserializer) {
            this.keyDeserializer = keyDeserializer;
            this.valDeserializer = valDeserializer;
        }
    }

    public TopologyBuilder() {}

    @SuppressWarnings("unchecked")
    public final KafkaSource addSource(String name, Deserializer keyDeserializer, Deserializer valDeserializer, String... topics) {
        KafkaSource source = new KafkaSource(name, keyDeserializer, valDeserializer);

        for (String topic : topics) {
            if (topicsToSourceNames.containsKey(topic))
                throw new IllegalArgumentException("Topic " + topic + " has already been registered by another processor.");

            topicsToSourceNames.put(topic, name);
        }

        sourceClasses.put(name, new SourceClazz(keyDeserializer, valDeserializer));

        return source;
    }

    public final void addProcessor(String name, Class<? extends KafkaProcessor> processorClass, ProcessorMetadata config, String... parentNames) {
        if (processorClasses.containsKey(name))
            throw new IllegalArgumentException("Processor " + name + " is already added.");

        processorClasses.put(name, new ProcessorClazz(processorClass, config));

        if (parentNames != null) {
            for (String parent : parentNames) {
                if (!processorClasses.containsKey(parent))
                    throw new IllegalArgumentException("Parent processor " + parent + " is not added yet.");

                // add to parent list
                if (!parents.containsKey(name))
                    parents.put(name, new ArrayList<>());
                parents.get(name).add(parent);

                // add to children list
                if (!children.containsKey(parent))
                    children.put(parent, new ArrayList<>());
                children.get(parent).add(name);
            }
        }
    }

    /**
     * Build the topology by creating the processors
     */
    @SuppressWarnings("unchecked")
    public ProcessorTopology build() {
        Map<String, KafkaProcessor> processorMap = new HashMap<>();
        Map<String, KafkaSource> topicSourceMap = new HashMap<>();

        // create sources
        try {
            for (String name : sourceClasses.keySet()) {
                Deserializer keyDeserializer = sourceClasses.get(name).keyDeserializer;
                Deserializer valDeserializer = sourceClasses.get(name).valDeserializer;
                KafkaSource source = new KafkaSource(name, keyDeserializer, valDeserializer);
                processorMap.put(name, source);
            }
        } catch (Exception e) {
            throw new KafkaException("KafkaProcessor(String) constructor failed: this should not happen.");
        }

        // create processors
        try {
            for (String name : processorClasses.keySet()) {
                ProcessorMetadata metadata = processorClasses.get(name).metadata;
                Class<? extends KafkaProcessor> processorClass = processorClasses.get(name).clazz;
                KafkaProcessor processor = processorClass.getConstructor(String.class, ProcessorMetadata.class).newInstance(name, metadata);
                processorMap.put(name, processor);
            }
        } catch (Exception e) {
            throw new KafkaException("KafkaProcessor(String) constructor failed: this should not happen.");
        }

        // construct topics to sources map
        for (String topic : topicsToSourceNames.keySet()) {
            KafkaSource source = (KafkaSource) processorMap.get(topicsToSourceNames.get(topic));
            topicSourceMap.put(topic, source);
        }

        for (KafkaProcessor processor : processorMap.values()) {
            // chain children to this processor
            for (String child : children.get(processor.name())) {
                KafkaProcessor childProcessor = processorMap.get(child);
                processor.chain(childProcessor);
            }
        }

        return new ProcessorTopology(processorMap, topicSourceMap);
    }
}
