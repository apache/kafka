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
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streaming.processor.internals.ProcessorNode;
import org.apache.kafka.streaming.processor.internals.ProcessorTopology;
import org.apache.kafka.streaming.processor.internals.SinkNode;
import org.apache.kafka.streaming.processor.internals.SourceNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TopologyBuilder {

    private Map<String, ProcessorClazz> processorClasses = new HashMap<>();
    private Map<String, SourceClazz> sourceClasses = new HashMap<>();
    private Map<String, SinkClazz> sinkClasses = new HashMap<>();
    private Map<String, String> topicsToSourceNames = new HashMap<>();
    private Map<String, String> topicsToSinkNames = new HashMap<>();

    private Map<String, List<String>> parents = new HashMap<>();
    private Map<String, List<String>> children = new HashMap<>();

    private class ProcessorClazz {
        public ProcessorFactory factory;

        public ProcessorClazz(ProcessorFactory factory) {
            this.factory = factory;
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

    private class SinkClazz {
        public Serializer keySerializer;
        public Serializer valSerializer;

        private SinkClazz(Serializer keySerializer, Serializer valSerializer) {
            this.keySerializer = keySerializer;
            this.valSerializer = valSerializer;
        }
    }

    public TopologyBuilder() {}

    @SuppressWarnings("unchecked")
    public final void addSource(String name, Deserializer keyDeserializer, Deserializer valDeserializer, String... topics) {
        for (String topic : topics) {
            if (topicsToSourceNames.containsKey(topic))
                throw new IllegalArgumentException("Topic " + topic + " has already been registered by another processor.");

            topicsToSourceNames.put(topic, name);
        }

        sourceClasses.put(name, new SourceClazz(keyDeserializer, valDeserializer));
    }

    public final void addSink(String name, Serializer keySerializer, Serializer valSerializer, String... topics) {
        for (String topic : topics) {
            if (topicsToSinkNames.containsKey(topic))
                throw new IllegalArgumentException("Topic " + topic + " has already been registered by another processor.");

            topicsToSinkNames.put(topic, name);
        }

        sinkClasses.put(name, new SinkClazz(keySerializer, valSerializer));
    }

    public final void addProcessor(String name, ProcessorFactory factory, String... parentNames) {
        if (processorClasses.containsKey(name))
            throw new IllegalArgumentException("Processor " + name + " is already added.");

        processorClasses.put(name, new ProcessorClazz(factory));

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
        Map<String, ProcessorNode> processorMap = new HashMap<>();
        Map<String, SourceNode> topicSourceMap = new HashMap<>();
        Map<String, SinkNode> topicSinkMap = new HashMap<>();

        // create sources
        for (String name : sourceClasses.keySet()) {
            Deserializer keyDeserializer = sourceClasses.get(name).keyDeserializer;
            Deserializer valDeserializer = sourceClasses.get(name).valDeserializer;
            SourceNode node = new SourceNode(name, keyDeserializer, valDeserializer);
            processorMap.put(name, node);
        }

        // create sinks
        for (String name : sinkClasses.keySet()) {
            Serializer keySerializer = sinkClasses.get(name).keySerializer;
            Serializer valSerializer = sinkClasses.get(name).valSerializer;
            SinkNode node = new SinkNode(name, keySerializer, valSerializer);
            processorMap.put(name, node);
        }

        // create processors
        try {
            for (String name : processorClasses.keySet()) {
                ProcessorFactory processorFactory = processorClasses.get(name).factory;
                Processor processor = processorFactory.build();
                ProcessorNode node = new ProcessorNode(name, processor);
                processorMap.put(name, node);
            }
        } catch (Exception e) {
            throw new KafkaException("Processor(String) constructor failed: this should not happen.");
        }

        // construct topics to sources map
        for (String topic : topicsToSourceNames.keySet()) {
            SourceNode node = (SourceNode) processorMap.get(topicsToSourceNames.get(topic));
            topicSourceMap.put(topic, node);
        }

        // construct topics to sinks map
        for (String topic : topicsToSinkNames.keySet()) {
            SinkNode node = (SinkNode) processorMap.get(topicsToSourceNames.get(topic));
            topicSinkMap.put(topic, node);
            node.addTopic(topic);
        }

        // chain children to parents to build the DAG
        for (ProcessorNode node : processorMap.values()) {
            for (String child : children.get(node.name())) {
                ProcessorNode childNode = processorMap.get(child);
                node.chain(childNode);
            }
        }

        return new ProcessorTopology(processorMap, topicSourceMap, topicSinkMap);
    }
}
