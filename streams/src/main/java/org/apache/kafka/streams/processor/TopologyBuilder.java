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

package org.apache.kafka.streams.processor;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;
import org.apache.kafka.streams.processor.internals.QuickUnion;
import org.apache.kafka.streams.processor.internals.SinkNode;
import org.apache.kafka.streams.processor.internals.SourceNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A component that is used to build a {@link ProcessorTopology}. A topology contains an acyclic graph of sources, processors,
 * and sinks. A {@link SourceNode source} is a node in the graph that consumes one or more Kafka topics and forwards them to
 * its child nodes. A {@link Processor processor} is a node in the graph that receives input messages from upstream nodes,
 * processes that message, and optionally forwarding new messages to one or all of its children. Finally, a {@link SinkNode sink}
 * is a node in the graph that receives messages from upstream nodes and writes them to a Kafka topic. This builder allows you
 * to construct an acyclic graph of these nodes, and the builder is then passed into a new {@link KafkaStreaming} instance
 * that will then {@link KafkaStreaming#start() begin consuming, processing, and producing messages}.
 */
public class TopologyBuilder {

    // list of node factories in a topological order
    private final ArrayList<NodeFactory> nodeFactories = new ArrayList<>();

    private final Set<String> nodeNames = new HashSet<>();
    private final Set<String> sourceTopicNames = new HashSet<>();

    private final PartitionGrouper partitionGrouper;
    private final QuickUnion<String> nodeGroups = new QuickUnion<>();
    private final HashMap<String, String[]> nodeToTopics = new HashMap<>();

    private interface NodeFactory {
        ProcessorNode build();
    }

    private class ProcessorNodeFactory implements NodeFactory {
        public final String[] parents;
        private final String name;
        private final ProcessorSupplier supplier;

        public ProcessorNodeFactory(String name, String[] parents, ProcessorSupplier supplier) {
            this.name = name;
            this.parents = parents.clone();
            this.supplier = supplier;
        }

        @Override
        public ProcessorNode build() {
            return new ProcessorNode(name, supplier.get());
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

        @Override
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
        @Override
        public ProcessorNode build() {
            return new SinkNode(name, topic, keySerializer, valSerializer);
        }
    }

    /**
     * Create a new builder with the default PartitionGrouper.
     */
    public TopologyBuilder() {
        this(new DefaultPartitionGrouper());
    }

    /**
     * Create a new builder with the specified PartitionGrouper
     */
    public TopologyBuilder(PartitionGrouper partitionGrouper) {
        this.partitionGrouper = partitionGrouper;
    }

    /**
     * Add a new source that consumes the named topics and forwards the messages to child processor and/or sink nodes.
     * The source will use the {@link StreamingConfig#KEY_DESERIALIZER_CLASS_CONFIG default key deserializer} and
     * {@link StreamingConfig#VALUE_DESERIALIZER_CLASS_CONFIG default value deserializer} specified in the
     * {@link StreamingConfig streaming configuration}.
     *
     * @param name the unique name of the source used to reference this node when
     * {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}.
     * @param topics the name of one or more Kafka topics that this source is to consume
     * @return this builder instance so methods can be chained together; never null
     */
    public final TopologyBuilder addSource(String name, String... topics) {
        return addSource(name, (Deserializer) null, (Deserializer) null, topics);
    }

    /**
     * Add a new source that consumes the named topics and forwards the messages to child processor and/or sink nodes.
     * The sink will use the specified key and value deserializers.
     *
     * @param name the unique name of the source used to reference this node when
     * {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}.
     * @param keyDeserializer the {@link Deserializer key deserializer} used when consuming messages; may be null if the source
     * should use the {@link StreamingConfig#KEY_DESERIALIZER_CLASS_CONFIG default key deserializer} specified in the
     * {@link StreamingConfig streaming configuration}
     * @param valDeserializer the {@link Deserializer value deserializer} used when consuming messages; may be null if the source
     * should use the {@link StreamingConfig#VALUE_DESERIALIZER_CLASS_CONFIG default value deserializer} specified in the
     * {@link StreamingConfig streaming configuration}
     * @param topics the name of one or more Kafka topics that this source is to consume
     * @return this builder instance so methods can be chained together; never null
     */
    public final TopologyBuilder addSource(String name, Deserializer keyDeserializer, Deserializer valDeserializer, String... topics) {
        if (nodeNames.contains(name))
            throw new TopologyException("Processor " + name + " is already added.");

        for (String topic : topics) {
            if (sourceTopicNames.contains(topic))
                throw new TopologyException("Topic " + topic + " has already been registered by another source.");

            sourceTopicNames.add(topic);
        }

        nodeNames.add(name);
        nodeFactories.add(new SourceNodeFactory(name, topics, keyDeserializer, valDeserializer));
        nodeToTopics.put(name, topics.clone());
        nodeGroups.add(name);

        return this;
    }

    /**
     * Add a new sink that forwards messages from upstream parent processor and/or source nodes to the named Kafka topic.
     * The sink will use the {@link StreamingConfig#KEY_SERIALIZER_CLASS_CONFIG default key serializer} and
     * {@link StreamingConfig#VALUE_SERIALIZER_CLASS_CONFIG default value serializer} specified in the
     * {@link StreamingConfig streaming configuration}.
     *
     * @param name the unique name of the sink
     * @param topic the name of the Kafka topic to which this sink should write its messages
     * @return this builder instance so methods can be chained together; never null
     */
    public final TopologyBuilder addSink(String name, String topic, String... parentNames) {
        return addSink(name, topic, (Serializer) null, (Serializer) null, parentNames);
    }

    /**
     * Add a new sink that forwards messages from upstream parent processor and/or source nodes to the named Kafka topic.
     * The sink will use the specified key and value serializers.
     *
     * @param name the unique name of the sink
     * @param topic the name of the Kafka topic to which this sink should write its messages
     * @param keySerializer the {@link Serializer key serializer} used when consuming messages; may be null if the sink
     * should use the {@link StreamingConfig#KEY_SERIALIZER_CLASS_CONFIG default key serializer} specified in the
     * {@link StreamingConfig streaming configuration}
     * @param valSerializer the {@link Serializer value serializer} used when consuming messages; may be null if the sink
     * should use the {@link StreamingConfig#VALUE_SERIALIZER_CLASS_CONFIG default value serializer} specified in the
     * {@link StreamingConfig streaming configuration}
     * @param parentNames the name of one or more source or processor nodes whose output message this sink should consume
     * and write to its topic
     * @return this builder instance so methods can be chained together; never null
     */
    public final TopologyBuilder addSink(String name, String topic, Serializer keySerializer, Serializer valSerializer, String... parentNames) {
        if (nodeNames.contains(name))
            throw new TopologyException("Processor " + name + " is already added.");

        if (parentNames != null) {
            for (String parent : parentNames) {
                if (parent.equals(name)) {
                    throw new TopologyException("Processor " + name + " cannot be a parent of itself.");
                }
                if (!nodeNames.contains(parent)) {
                    throw new TopologyException("Parent processor " + parent + " is not added yet.");
                }
            }
        }

        nodeNames.add(name);
        nodeFactories.add(new SinkNodeFactory(name, parentNames, topic, keySerializer, valSerializer));
        return this;
    }

    /**
     * Add a new processor node that receives and processes messages output by one or more parent source or processor node.
     * Any new messages output by this processor will be forwarded to its child processor or sink nodes.
     * @param name the unique name of the processor node
     * @param supplier the supplier used to obtain this node's {@link Processor} instance
     * @param parentNames the name of one or more source or processor nodes whose output messages this processor should receive
     * and process
     * @return this builder instance so methods can be chained together; never null
     */
    public final TopologyBuilder addProcessor(String name, ProcessorSupplier supplier, String... parentNames) {
        if (nodeNames.contains(name))
            throw new TopologyException("Processor " + name + " is already added.");

        if (parentNames != null) {
            for (String parent : parentNames) {
                if (parent.equals(name)) {
                    throw new TopologyException("Processor " + name + " cannot be a parent of itself.");
                }
                if (!nodeNames.contains(parent)) {
                    throw new TopologyException("Parent processor " + parent + " is not added yet.");
                }
            }
        }

        nodeNames.add(name);
        nodeFactories.add(new ProcessorNodeFactory(name, parentNames, supplier));
        nodeGroups.add(name);
        return this;
    }

    /**
     * Declares that the streams of the specified processor nodes must be copartitioned.
     *
     * @param nodeName
     * @param otherNodeNames
     */
    public void copartition(String nodeName, String... otherNodeNames) {
        for (String other : otherNodeNames) {
            nodeGroups.unite(nodeName, other);
        }
    }

    /**
     * Returns the grouped topics
     *
     * @return groups of topic names
     */
    public Collection<Set<String>> topicGroups() {
        HashMap<String, Set<String>> topicGroupMap = new HashMap<>();

        // collect topics by node groups
        for (String nodeName : nodeNames) {
            String[] topics = nodeToTopics.get(nodeName);
            if (topics != null) {
                String root = nodeGroups.root(nodeName);
                Set<String> topicGroup = topicGroupMap.get(root);
                if (topicGroup == null) {
                    topicGroup = new HashSet<>();
                    topicGroupMap.put(root, topicGroup);
                }
                topicGroup.addAll(Arrays.asList(topics));
            }
        }

        // make the data unmodifiable, then return
        List<Set<String>> topicGroups = new ArrayList<>(topicGroupMap.size());
        for (Set<String> group : topicGroupMap.values()) {
            topicGroups.add(Collections.unmodifiableSet(group));
        }
        return Collections.unmodifiableList(topicGroups);
    }

    /**
     * Returns the instance of PartitionGrouper for this TopologyBuilder
     * @return
     */
    public PartitionGrouper partitionGrouper() {
        return partitionGrouper;
    }

    /**
     * Build the topology. This is typically called automatically when passing this builder into the
     * {@link KafkaStreaming#KafkaStreaming(TopologyBuilder, StreamingConfig)} constructor.
     *
     * @see KafkaStreaming#KafkaStreaming(TopologyBuilder, StreamingConfig)
     */
    @SuppressWarnings("unchecked")
    public ProcessorTopology build() {
        List<ProcessorNode> processorNodes = new ArrayList<>(nodeFactories.size());
        Map<String, ProcessorNode> processorMap = new HashMap<>();
        Map<String, SourceNode> topicSourceMap = new HashMap<>();

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
                    for (String topic : ((SourceNodeFactory) factory).topics) {
                        topicSourceMap.put(topic, (SourceNode) node);
                    }
                } else if (factory instanceof SinkNodeFactory) {
                    for (String parent : ((SinkNodeFactory) factory).parents) {
                        processorMap.get(parent).addChild(node);
                    }
                } else {
                    throw new TopologyException("Unknown definition class: " + factory.getClass().getName());
                }
            }
        } catch (Exception e) {
            throw new KafkaException("ProcessorNode construction failed: this should not happen.");
        }

        return new ProcessorTopology(processorNodes, topicSourceMap);
    }

    /**
     * Get the names of topics that are to be consumed by the source nodes created by this builder.
     * @return the unmodifiable set of topic names used by source nodes, which changes as new sources are added; never null
     */
    public Set<String> sourceTopics() {
        return Collections.unmodifiableSet(sourceTopicNames);
    }
}
