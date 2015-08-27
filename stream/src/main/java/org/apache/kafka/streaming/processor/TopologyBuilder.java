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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TopologyBuilder {

    // list of node factories in a topological order
    private ArrayList<NodeFactory> nodeFactories = new ArrayList<>();

    private Set<String> nodeNames = new HashSet<>();
    private Set<String> sourceTopicNames = new HashSet<>();

    private interface NodeFactory {
        ProcessorNode build();
    }

    private class ProcessorNodeFactory implements NodeFactory {
        public final String[] parents;
        private final String name;
        private final ProcessorDef definition;

        public ProcessorNodeFactory(String name, String[] parents, ProcessorDef definition) {
            this.name = name;
            this.parents = parents.clone();
            this.definition = definition;
        }

        public ProcessorNode build() {
            Processor processor = definition.define();
            return new ProcessorNode(name, processor);
        }
    }

    private class SourceNodeFactory implements NodeFactory {
        public final String[] topics;
        private final String name;
        private Deserializer keyDeserializer;
        private Deserializer valDeserializer;

        private SourceNodeFactory(String name, String[] topics, Deserializer keyDeserializer, Deserializer valDeserializer) {
            this.name = name;
            this.topics = topics.clone();
            this.keyDeserializer = keyDeserializer;
            this.valDeserializer = valDeserializer;
        }

        public ProcessorNode build() {
            return new SourceNode(name, keyDeserializer, valDeserializer);
        }
    }

    private class SinkNodeFactory implements NodeFactory {
        public final String[] parents;
        public final String topic;
        private final String name;
        private Serializer keySerializer;
        private Serializer valSerializer;

        private SinkNodeFactory(String name, String[] parents, String topic, Serializer keySerializer, Serializer valSerializer) {
            this.name = name;
            this.parents = parents.clone();
            this.topic = topic;
            this.keySerializer = keySerializer;
            this.valSerializer = valSerializer;
        }
        public ProcessorNode build() {
            return new SinkNode(name, topic, keySerializer, valSerializer);
        }
    }

    public TopologyBuilder() {}

    public final void addSource(String name, Deserializer keyDeserializer, Deserializer valDeserializer, String... topics) {
        if (nodeNames.contains(name))
            throw new IllegalArgumentException("Processor " + name + " is already added.");

        for (String topic : topics) {
            if (sourceTopicNames.contains(topic))
                throw new IllegalArgumentException("Topic " + topic + " has already been registered by another processor.");

            sourceTopicNames.add(topic);
        }

        nodeNames.add(name);
        nodeFactories.add(new SourceNodeFactory(name, topics, keyDeserializer, valDeserializer));
    }

    public final void addSink(String name, String topic, Serializer keySerializer, Serializer valSerializer, String... parentNames) {
        if (nodeNames.contains(name))
            throw new IllegalArgumentException("Processor " + name + " is already added.");

        if (parentNames != null) {
            for (String parent : parentNames) {
                if (parent.equals(name)) {
                    throw new IllegalArgumentException("Processor " + name + " cannot be a parent of itself");
                }
                if (!nodeNames.contains(parent)) {
                    throw new IllegalArgumentException("Parent processor " + parent + " is not added yet.");
                }
            }
        }

        nodeNames.add(name);
        nodeFactories.add(new SinkNodeFactory(name, parentNames, topic, keySerializer, valSerializer));
    }

    public final void addProcessor(String name, ProcessorDef definition, String... parentNames) {
        if (nodeNames.contains(name))
            throw new IllegalArgumentException("Processor " + name + " is already added.");

        if (parentNames != null) {
            for (String parent : parentNames) {
                if (parent.equals(name)) {
                    throw new IllegalArgumentException("Processor " + name + " cannot be a parent of itself");
                }
                if (!nodeNames.contains(parent)) {
                    throw new IllegalArgumentException("Parent processor " + parent + " is not added yet.");
                }
            }
        }

        nodeNames.add(name);
        nodeFactories.add(new ProcessorNodeFactory(name, parentNames, definition));
    }

    /**
     * Build the topology by creating the processors
     */
    @SuppressWarnings("unchecked")
    public ProcessorTopology build() {
        List<ProcessorNode> processorNodes = new ArrayList<>(nodeFactories.size());
        Map<String, ProcessorNode> processorMap = new HashMap<>();
        Map<String, SourceNode> topicSourceMap = new HashMap<>();
        Map<String, SinkNode> topicSinkMap = new HashMap<>();

        try {
            // create processor nodes in a topological order ("nodeFactories" is already topologically sorted)
            for (NodeFactory factory : nodeFactories) {
                ProcessorNode node = factory.build();
                processorNodes.add(node);
                processorMap.put(node.name(), node);

                if (factory instanceof ProcessorNodeFactory) {
                    for (String parent : ((ProcessorNodeFactory) factory).parents) {
                        processorMap.get(parent).addChild(node);
                    }
                } else if (factory instanceof SourceNodeFactory) {
                    for (String topic : ((SourceNodeFactory)factory).topics) {
                        topicSourceMap.put(topic, (SourceNode) node);
                    }
                } else if (factory instanceof SinkNodeFactory) {
                    String topic = ((SinkNodeFactory) factory).topic;
                    topicSinkMap.put(topic, (SinkNode) node);

                    for (String parent : ((SinkNodeFactory) factory).parents) {
                        processorMap.get(parent).addChild(node);
                    }
                } else {
                    throw new IllegalStateException("unknown definition class: " + factory.getClass().getName());
                }
            }
        } catch (Exception e) {
            throw new KafkaException("ProcessorNode construction failed: this should not happen.");
        }

        return new ProcessorTopology(processorNodes, topicSourceMap, topicSinkMap);
    }
}
