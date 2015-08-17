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

package org.apache.kafka.stream.processor;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.stream.processor.internals.PTopology;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PTopologyBuilder {

    private Map<String, ProcessorClazz> processorClasses = new HashMap<>();
    private Map<String, KafkaSource> sources = new HashMap<>();

    private Map<String, List<String>> parents = new HashMap<>();
    private Map<String, List<String>> children = new HashMap<>();

    private class ProcessorClazz {
        public Class<? extends KafkaProcessor> clazz;
        public ProcessorMetadata config;

        public ProcessorClazz(Class<? extends KafkaProcessor> clazz, ProcessorMetadata config) {
            this.clazz = clazz;
            this.config = config;
        }
    }

    public PTopologyBuilder() {}

    @SuppressWarnings("unchecked")
    public final KafkaSource addSource(String name, Deserializer keyDeserializer, Deserializer valDeserializer, String... topics) {
        KafkaSource source = new KafkaSource(name, keyDeserializer, valDeserializer);

        for (String topic : topics) {
            if (sources.containsKey(topic))
                throw new IllegalArgumentException("Topic " + topic + " has already been registered by another processor.");

            sources.put(name, source);
        }

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

                // add to parent list
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
    public PTopology build() {
        Map<String, KafkaProcessor> processorMap = new HashMap<>();

        // create processors
        try {
            for (String name : processorClasses.keySet()) {
                ProcessorMetadata config = processorClasses.get(name).config;
                Class<? extends KafkaProcessor> processorClass = processorClasses.get(name).clazz;
                KafkaProcessor processor = processorClass.getConstructor(String.class, ProcessorMetadata.class).newInstance(name, config);
                processorMap.put(name, processor);
            }
        } catch (Exception e) {
            throw new KafkaException("KafkaProcessor(String) constructor failed: this should not happen.");
        }

        // construct processor parent-child relationships
        for (String topic : sources.keySet()) {
            KafkaSource source = sources.get(topic);
            processorMap.put(source.name(), source);

            // chain children to this processor
            for (String child : children.get(source.name())) {
                KafkaProcessor childProcessor = processorMap.get(child);
                source.chain(childProcessor);
            }
        }

        for (KafkaProcessor processor : processorMap.values()) {
            // chain children to this processor
            for (String child : children.get(processor.name())) {
                KafkaProcessor childProcessor = processorMap.get(child);
                processor.chain(childProcessor);
            }

        }

        return new PTopology(processorMap, sources);
    }
}
