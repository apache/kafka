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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.TopologyBuilderException;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A component that is used to build a {@link ProcessorTopology}. A topology contains an acyclic graph of sources, processors,
 * and sinks. A {@link SourceNode source} is a node in the graph that consumes one or more Kafka topics and forwards them to
 * its child nodes. A {@link Processor processor} is a node in the graph that receives input records from upstream nodes,
 * processes that records, and optionally forwarding new records to one or all of its children. Finally, a {@link SinkNode sink}
 * is a node in the graph that receives records from upstream nodes and writes them to a Kafka topic. This builder allows you
 * to construct an acyclic graph of these nodes, and the builder is then passed into a new {@link org.apache.kafka.streams.KafkaStreams}
 * instance that will then {@link org.apache.kafka.streams.KafkaStreams#start() begin consuming, processing, and producing records}.
 */
public class TopologyBuilder {

    // node factories in a topological order
    private final LinkedHashMap<String, NodeFactory> nodeFactories = new LinkedHashMap<>();

    // state factories
    private final Map<String, StateStoreFactory> stateFactories = new HashMap<>();

    private final Set<String> sourceTopicNames = new HashSet<>();
    private final Set<String> internalTopicNames = new HashSet<>();
    private final QuickUnion<String> nodeGrouper = new QuickUnion<>();
    private final List<Set<String>> copartitionSourceGroups = new ArrayList<>();
    private final HashMap<String, String[]> nodeToSourceTopics = new HashMap<>();
    private final HashMap<String, String> nodeToSinkTopic = new HashMap<>();
    private Map<Integer, Set<String>> nodeGroups = null;

    private static class StateStoreFactory {
        public final Set<String> users;

        public final boolean isInternal;
        public final StateStoreSupplier supplier;

        StateStoreFactory(boolean isInternal, StateStoreSupplier supplier) {
            this.isInternal = isInternal;
            this.supplier = supplier;
            this.users = new HashSet<>();
        }
    }

    private static abstract class NodeFactory {
        public final String name;

        NodeFactory(String name) {
            this.name = name;
        }

        public abstract ProcessorNode build(String applicationId);
    }

    private static class ProcessorNodeFactory extends NodeFactory {
        public final String[] parents;
        private final ProcessorSupplier supplier;
        private final Set<String> stateStoreNames = new HashSet<>();

        public ProcessorNodeFactory(String name, String[] parents, ProcessorSupplier supplier) {
            super(name);
            this.parents = parents.clone();
            this.supplier = supplier;
        }

        public void addStateStore(String stateStoreName) {
            stateStoreNames.add(stateStoreName);
        }

        @SuppressWarnings("unchecked")
        @Override
        public ProcessorNode build(String applicationId) {
            return new ProcessorNode(name, supplier.get(), stateStoreNames);
        }
    }

    private static class SourceNodeFactory extends NodeFactory {
        public final String[] topics;
        private Deserializer keyDeserializer;
        private Deserializer valDeserializer;

        private SourceNodeFactory(String name, String[] topics, Deserializer keyDeserializer, Deserializer valDeserializer) {
            super(name);
            this.topics = topics.clone();
            this.keyDeserializer = keyDeserializer;
            this.valDeserializer = valDeserializer;
        }

        @SuppressWarnings("unchecked")
        @Override
        public ProcessorNode build(String applicationId) {
            return new SourceNode(name, keyDeserializer, valDeserializer);
        }
    }

    private class SinkNodeFactory extends NodeFactory {
        public final String[] parents;
        public final String topic;
        private Serializer keySerializer;
        private Serializer valSerializer;
        private final StreamPartitioner partitioner;

        private SinkNodeFactory(String name, String[] parents, String topic, Serializer keySerializer, Serializer valSerializer, StreamPartitioner partitioner) {
            super(name);
            this.parents = parents.clone();
            this.topic = topic;
            this.keySerializer = keySerializer;
            this.valSerializer = valSerializer;
            this.partitioner = partitioner;
        }

        @SuppressWarnings("unchecked")
        @Override
        public ProcessorNode build(String applicationId) {
            if (internalTopicNames.contains(topic)) {
                // prefix the internal topic name with the application id
                return new SinkNode(name, applicationId + "-" + topic, keySerializer, valSerializer, partitioner);
            } else {
                return new SinkNode(name, topic, keySerializer, valSerializer, partitioner);
            }
        }
    }

    public static class TopicsInfo {
        public Set<String> sinkTopics;
        public Set<String> sourceTopics;
        public Set<String> interSourceTopics;
        public Set<String> stateChangelogTopics;

        public TopicsInfo(Set<String> sinkTopics, Set<String> sourceTopics, Set<String> interSourceTopics, Set<String> stateChangelogTopics) {
            this.sinkTopics = sinkTopics;
            this.sourceTopics = sourceTopics;
            this.interSourceTopics = interSourceTopics;
            this.stateChangelogTopics = stateChangelogTopics;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof TopicsInfo) {
                TopicsInfo other = (TopicsInfo) o;
                return other.sourceTopics.equals(this.sourceTopics) && other.stateChangelogTopics.equals(this.stateChangelogTopics);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            long n = ((long) sourceTopics.hashCode() << 32) | (long) stateChangelogTopics.hashCode();
            return (int) (n % 0xFFFFFFFFL);
        }
    }

    /**
     * Create a new builder.
     */
    public TopologyBuilder() {}

    /**
     * Add a new source that consumes the named topics and forwards the records to child processor and/or sink nodes.
     * The source will use the {@link org.apache.kafka.streams.StreamsConfig#KEY_SERDE_CLASS_CONFIG default key deserializer} and
     * {@link org.apache.kafka.streams.StreamsConfig#VALUE_SERDE_CLASS_CONFIG default value deserializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}.
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
     * Add a new source that consumes the named topics and forwards the records to child processor and/or sink nodes.
     * The source will use the specified key and value deserializers.
     *
     * @param name the unique name of the source used to reference this node when
     * {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}.
     * @param keyDeserializer the {@link Deserializer key deserializer} used when consuming records; may be null if the source
     * should use the {@link org.apache.kafka.streams.StreamsConfig#KEY_SERDE_CLASS_CONFIG default key deserializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}
     * @param valDeserializer the {@link Deserializer value deserializer} used when consuming records; may be null if the source
     * should use the {@link org.apache.kafka.streams.StreamsConfig#VALUE_SERDE_CLASS_CONFIG default value deserializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}
     * @param topics the name of one or more Kafka topics that this source is to consume
     * @return this builder instance so methods can be chained together; never null
     * @throws TopologyBuilderException if processor is already added or if topics have already been registered by another source
     */
    public final TopologyBuilder addSource(String name, Deserializer keyDeserializer, Deserializer valDeserializer, String... topics) {
        if (nodeFactories.containsKey(name))
            throw new TopologyBuilderException("Processor " + name + " is already added.");

        for (String topic : topics) {
            if (sourceTopicNames.contains(topic))
                throw new TopologyBuilderException("Topic " + topic + " has already been registered by another source.");

            sourceTopicNames.add(topic);
        }

        nodeFactories.put(name, new SourceNodeFactory(name, topics, keyDeserializer, valDeserializer));
        nodeToSourceTopics.put(name, topics.clone());
        nodeGrouper.add(name);

        return this;
    }

    /**
     * Add a new sink that forwards records from upstream parent processor and/or source nodes to the named Kafka topic.
     * The sink will use the {@link org.apache.kafka.streams.StreamsConfig#KEY_SERDE_CLASS_CONFIG default key serializer} and
     * {@link org.apache.kafka.streams.StreamsConfig#VALUE_SERDE_CLASS_CONFIG default value serializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}.
     *
     * @param name the unique name of the sink
     * @param topic the name of the Kafka topic to which this sink should write its records
     * @param parentNames the name of one or more source or processor nodes whose output records this sink should consume
     * and write to its topic
     * @return this builder instance so methods can be chained together; never null
     * @see #addSink(String, String, StreamPartitioner, String...)
     * @see #addSink(String, String, Serializer, Serializer, String...)
     * @see #addSink(String, String, Serializer, Serializer, StreamPartitioner, String...)
     */
    public final TopologyBuilder addSink(String name, String topic, String... parentNames) {
        return addSink(name, topic, (Serializer) null, (Serializer) null, parentNames);
    }

    /**
     * Add a new sink that forwards records from upstream parent processor and/or source nodes to the named Kafka topic, using
     * the supplied partitioner.
     * The sink will use the {@link org.apache.kafka.streams.StreamsConfig#KEY_SERDE_CLASS_CONFIG default key serializer} and
     * {@link org.apache.kafka.streams.StreamsConfig#VALUE_SERDE_CLASS_CONFIG default value serializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}.
     * <p>
     * The sink will also use the specified {@link StreamPartitioner} to determine how records are distributed among
     * the named Kafka topic's partitions. Such control is often useful with topologies that use
     * {@link #addStateStore(StateStoreSupplier, String...) state stores}
     * in its processors. In most other cases, however, a partitioner needs not be specified and Kafka will automatically distribute
     * records among partitions using Kafka's default partitioning logic.
     *
     * @param name the unique name of the sink
     * @param topic the name of the Kafka topic to which this sink should write its records
     * @param partitioner the function that should be used to determine the partition for each record processed by the sink
     * @param parentNames the name of one or more source or processor nodes whose output records this sink should consume
     * and write to its topic
     * @return this builder instance so methods can be chained together; never null
     * @see #addSink(String, String, String...)
     * @see #addSink(String, String, Serializer, Serializer, String...)
     * @see #addSink(String, String, Serializer, Serializer, StreamPartitioner, String...)
     */
    public final TopologyBuilder addSink(String name, String topic, StreamPartitioner partitioner, String... parentNames) {
        return addSink(name, topic, (Serializer) null, (Serializer) null, partitioner, parentNames);
    }

    /**
     * Add a new sink that forwards records from upstream parent processor and/or source nodes to the named Kafka topic.
     * The sink will use the specified key and value serializers.
     *
     * @param name the unique name of the sink
     * @param topic the name of the Kafka topic to which this sink should write its records
     * @param keySerializer the {@link Serializer key serializer} used when consuming records; may be null if the sink
     * should use the {@link org.apache.kafka.streams.StreamsConfig#KEY_SERDE_CLASS_CONFIG default key serializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}
     * @param valSerializer the {@link Serializer value serializer} used when consuming records; may be null if the sink
     * should use the {@link org.apache.kafka.streams.StreamsConfig#VALUE_SERDE_CLASS_CONFIG default value serializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}
     * @param parentNames the name of one or more source or processor nodes whose output records this sink should consume
     * and write to its topic
     * @return this builder instance so methods can be chained together; never null
     * @see #addSink(String, String, String...)
     * @see #addSink(String, String, StreamPartitioner, String...)
     * @see #addSink(String, String, Serializer, Serializer, StreamPartitioner, String...)
     */
    public final TopologyBuilder addSink(String name, String topic, Serializer keySerializer, Serializer valSerializer, String... parentNames) {
        return addSink(name, topic, keySerializer, valSerializer, (StreamPartitioner) null, parentNames);
    }

    /**
     * Add a new sink that forwards records from upstream parent processor and/or source nodes to the named Kafka topic.
     * The sink will use the specified key and value serializers, and the supplied partitioner.
     *
     * @param name the unique name of the sink
     * @param topic the name of the Kafka topic to which this sink should write its records
     * @param keySerializer the {@link Serializer key serializer} used when consuming records; may be null if the sink
     * should use the {@link org.apache.kafka.streams.StreamsConfig#KEY_SERDE_CLASS_CONFIG default key serializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}
     * @param valSerializer the {@link Serializer value serializer} used when consuming records; may be null if the sink
     * should use the {@link org.apache.kafka.streams.StreamsConfig#VALUE_SERDE_CLASS_CONFIG default value serializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}
     * @param partitioner the function that should be used to determine the partition for each record processed by the sink
     * @param parentNames the name of one or more source or processor nodes whose output records this sink should consume
     * and write to its topic
     * @return this builder instance so methods can be chained together; never null
     * @see #addSink(String, String, String...)
     * @see #addSink(String, String, StreamPartitioner, String...)
     * @see #addSink(String, String, Serializer, Serializer, String...)
     * @throws TopologyBuilderException if parent processor is not added yet, or if this processor's name is equal to the parent's name
     */
    public final <K, V> TopologyBuilder addSink(String name, String topic, Serializer<K> keySerializer, Serializer<V> valSerializer, StreamPartitioner<K, V> partitioner, String... parentNames) {
        if (nodeFactories.containsKey(name))
            throw new TopologyBuilderException("Processor " + name + " is already added.");

        if (parentNames != null) {
            for (String parent : parentNames) {
                if (parent.equals(name)) {
                    throw new TopologyBuilderException("Processor " + name + " cannot be a parent of itself.");
                }
                if (!nodeFactories.containsKey(parent)) {
                    throw new TopologyBuilderException("Parent processor " + parent + " is not added yet.");
                }
            }
        }

        nodeFactories.put(name, new SinkNodeFactory(name, parentNames, topic, keySerializer, valSerializer, partitioner));
        nodeToSinkTopic.put(name, topic);
        nodeGrouper.add(name);
        nodeGrouper.unite(name, parentNames);
        return this;
    }

    /**
     * Add a new processor node that receives and processes records output by one or more parent source or processor node.
     * Any new record output by this processor will be forwarded to its child processor or sink nodes.
     * @param name the unique name of the processor node
     * @param supplier the supplier used to obtain this node's {@link Processor} instance
     * @param parentNames the name of one or more source or processor nodes whose output records this processor should receive
     * and process
     * @return this builder instance so methods can be chained together; never null
     * @throws TopologyBuilderException if parent processor is not added yet, or if this processor's name is equal to the parent's name
     */
    public final TopologyBuilder addProcessor(String name, ProcessorSupplier supplier, String... parentNames) {
        if (nodeFactories.containsKey(name))
            throw new TopologyBuilderException("Processor " + name + " is already added.");

        if (parentNames != null) {
            for (String parent : parentNames) {
                if (parent.equals(name)) {
                    throw new TopologyBuilderException("Processor " + name + " cannot be a parent of itself.");
                }
                if (!nodeFactories.containsKey(parent)) {
                    throw new TopologyBuilderException("Parent processor " + parent + " is not added yet.");
                }
            }
        }

        nodeFactories.put(name, new ProcessorNodeFactory(name, parentNames, supplier));
        nodeGrouper.add(name);
        nodeGrouper.unite(name, parentNames);
        return this;
    }

    /**
     * Adds a state store
     *
     * @param supplier the supplier used to obtain this state store {@link StateStore} instance
     * @return this builder instance so methods can be chained together; never null
     * @throws TopologyBuilderException if state store supplier is already added
     */
    public final TopologyBuilder addStateStore(StateStoreSupplier supplier, boolean isInternal, String... processorNames) {
        if (stateFactories.containsKey(supplier.name())) {
            throw new TopologyBuilderException("StateStore " + supplier.name() + " is already added.");
        }

        stateFactories.put(supplier.name(), new StateStoreFactory(isInternal, supplier));

        if (processorNames != null) {
            for (String processorName : processorNames) {
                connectProcessorAndStateStore(processorName, supplier.name());
            }
        }

        return this;
    }

    /**
     * Adds a state store
     *
     * @param supplier the supplier used to obtain this state store {@link StateStore} instance
     * @return this builder instance so methods can be chained together; never null
     */
    public final TopologyBuilder addStateStore(StateStoreSupplier supplier, String... processorNames) {
        return this.addStateStore(supplier, true, processorNames);
    }

    /**
     * Connects the processor and the state stores
     *
     * @param processorName the name of the processor
     * @param stateStoreNames the names of state stores that the processor uses
     * @return this builder instance so methods can be chained together; never null
     */
    public final TopologyBuilder connectProcessorAndStateStores(String processorName, String... stateStoreNames) {
        if (stateStoreNames != null) {
            for (String stateStoreName : stateStoreNames) {
                connectProcessorAndStateStore(processorName, stateStoreName);
            }
        }

        return this;
    }

    /**
     * Connects a list of processors.
     *
     * NOTE this function would not needed by developers working with the processor APIs, but only used
     * for the high-level DSL parsing functionalities.
     *
     * @param processorNames the name of the processors
     * @return this builder instance so methods can be chained together; never null
     * @throws TopologyBuilderException if less than two processors are specified, or if one of the processors is not added yet
     */
    public final TopologyBuilder connectProcessors(String... processorNames) {
        if (processorNames.length < 2)
            throw new TopologyBuilderException("At least two processors need to participate in the connection.");

        for (String processorName : processorNames) {
            if (!nodeFactories.containsKey(processorName))
                throw new TopologyBuilderException("Processor " + processorName + " is not added yet.");

        }

        String firstProcessorName = processorNames[0];

        nodeGrouper.unite(firstProcessorName, Arrays.copyOfRange(processorNames, 1, processorNames.length));

        return this;
    }

    /**
     * Adds an internal topic
     *
     * @param topicName the name of the topic
     * @return this builder instance so methods can be chained together; never null
     */
    public final TopologyBuilder addInternalTopic(String topicName) {
        this.internalTopicNames.add(topicName);

        return this;
    }

    private void connectProcessorAndStateStore(String processorName, String stateStoreName) {
        if (!stateFactories.containsKey(stateStoreName))
            throw new TopologyBuilderException("StateStore " + stateStoreName + " is not added yet.");
        if (!nodeFactories.containsKey(processorName))
            throw new TopologyBuilderException("Processor " + processorName + " is not added yet.");

        StateStoreFactory stateStoreFactory = stateFactories.get(stateStoreName);
        Iterator<String> iter = stateStoreFactory.users.iterator();
        if (iter.hasNext()) {
            String user = iter.next();
            nodeGrouper.unite(user, processorName);
        }
        stateStoreFactory.users.add(processorName);

        NodeFactory nodeFactory = nodeFactories.get(processorName);
        if (nodeFactory instanceof ProcessorNodeFactory) {
            ((ProcessorNodeFactory) nodeFactory).addStateStore(stateStoreName);
        } else {
            throw new TopologyBuilderException("cannot connect a state store " + stateStoreName + " to a source node or a sink node.");
        }
    }

    /**
     * Returns the map of topic groups keyed by the group id.
     * A topic group is a group of topics in the same task.
     *
     * @return groups of topic names
     */
    public Map<Integer, TopicsInfo> topicGroups(String applicationId) {
        Map<Integer, TopicsInfo> topicGroups = new HashMap<>();

        if (nodeGroups == null)
            nodeGroups = makeNodeGroups();

        for (Map.Entry<Integer, Set<String>> entry : nodeGroups.entrySet()) {
            Set<String> sinkTopics = new HashSet<>();
            Set<String> sourceTopics = new HashSet<>();
            Set<String> internalSourceTopics = new HashSet<>();
            Set<String> stateChangelogTopics = new HashSet<>();
            for (String node : entry.getValue()) {
                // if the node is a source node, add to the source topics
                String[] topics = nodeToSourceTopics.get(node);
                if (topics != null) {
                    // if some of the topics are internal, add them to the internal topics
                    for (String topic : topics) {
                        if (this.internalTopicNames.contains(topic)) {
                            // prefix the internal topic name with the application id
                            String internalTopic = applicationId + "-" + topic;
                            internalSourceTopics.add(internalTopic);
                            sourceTopics.add(internalTopic);
                        } else {
                            sourceTopics.add(topic);
                        }
                    }
                }

                // if the node is a sink node, add to the sink topics
                String topic = nodeToSinkTopic.get(node);
                if (topic != null) {
                    if (internalTopicNames.contains(topic)) {
                        // prefix the change log topic name with the application id
                        sinkTopics.add(applicationId + "-" + topic);
                    } else {
                        sinkTopics.add(topic);
                    }
                }

                // if the node is connected to a state, add to the state topics
                for (StateStoreFactory stateFactory : stateFactories.values()) {
                    if (stateFactory.isInternal && stateFactory.users.contains(node)) {
                        // prefix the change log topic name with the application id
                        stateChangelogTopics.add(applicationId + "-" + stateFactory.supplier.name() + ProcessorStateManager.STATE_CHANGELOG_TOPIC_SUFFIX);
                    }
                }
            }
            topicGroups.put(entry.getKey(), new TopicsInfo(
                    Collections.unmodifiableSet(sinkTopics),
                    Collections.unmodifiableSet(sourceTopics),
                    Collections.unmodifiableSet(internalSourceTopics),
                    Collections.unmodifiableSet(stateChangelogTopics)));
        }

        return Collections.unmodifiableMap(topicGroups);
    }

    /**
     * Returns the map of node groups keyed by the topic group id.
     *
     * @return groups of node names
     */
    public Map<Integer, Set<String>> nodeGroups() {
        if (nodeGroups == null)
            nodeGroups = makeNodeGroups();

        return nodeGroups;
    }

    private Map<Integer, Set<String>> makeNodeGroups() {
        HashMap<Integer, Set<String>> nodeGroups = new HashMap<>();
        HashMap<String, Set<String>> rootToNodeGroup = new HashMap<>();

        int nodeGroupId = 0;

        // Go through source nodes first. This makes the group id assignment easy to predict in tests
        for (String nodeName : Utils.sorted(nodeToSourceTopics.keySet())) {
            String root = nodeGrouper.root(nodeName);
            Set<String> nodeGroup = rootToNodeGroup.get(root);
            if (nodeGroup == null) {
                nodeGroup = new HashSet<>();
                rootToNodeGroup.put(root, nodeGroup);
                nodeGroups.put(nodeGroupId++, nodeGroup);
            }
            nodeGroup.add(nodeName);
        }

        // Go through non-source nodes
        for (String nodeName : Utils.sorted(nodeFactories.keySet())) {
            if (!nodeToSourceTopics.containsKey(nodeName)) {
                String root = nodeGrouper.root(nodeName);
                Set<String> nodeGroup = rootToNodeGroup.get(root);
                if (nodeGroup == null) {
                    nodeGroup = new HashSet<>();
                    rootToNodeGroup.put(root, nodeGroup);
                    nodeGroups.put(nodeGroupId++, nodeGroup);
                }
                nodeGroup.add(nodeName);
            }
        }

        return nodeGroups;
    }

    /**
     * Asserts that the streams of the specified source nodes must be copartitioned.
     *
     * @param sourceNodes a set of source node names
     * @return this builder instance so methods can be chained together; never null
     */
    public final TopologyBuilder copartitionSources(Collection<String> sourceNodes) {
        copartitionSourceGroups.add(Collections.unmodifiableSet(new HashSet<>(sourceNodes)));
        return this;
    }

    /**
     * Returns the copartition groups.
     * A copartition group is a group of source topics that are required to be copartitioned.
     *
     * @return groups of topic names
     */
    public Collection<Set<String>> copartitionGroups() {
        List<Set<String>> list = new ArrayList<>(copartitionSourceGroups.size());
        for (Set<String> nodeNames : copartitionSourceGroups) {
            Set<String> copartitionGroup = new HashSet<>();
            for (String node : nodeNames) {
                String[] topics = nodeToSourceTopics.get(node);
                if (topics != null)
                    copartitionGroup.addAll(Arrays.asList(topics));
            }
            list.add(Collections.unmodifiableSet(copartitionGroup));
        }
        return Collections.unmodifiableList(list);
    }

    /**
     * Build the topology for the specified topic group. This is called automatically when passing this builder into the
     * {@link org.apache.kafka.streams.KafkaStreams#KafkaStreams(TopologyBuilder, org.apache.kafka.streams.StreamsConfig)} constructor.
     *
     * @see org.apache.kafka.streams.KafkaStreams#KafkaStreams(TopologyBuilder, org.apache.kafka.streams.StreamsConfig)
     */
    public ProcessorTopology build(String applicationId, Integer topicGroupId) {
        Set<String> nodeGroup;
        if (topicGroupId != null) {
            nodeGroup = nodeGroups().get(topicGroupId);
        } else {
            // when nodeGroup is null, we build the full topology. this is used in some tests.
            nodeGroup = null;
        }
        return build(applicationId, nodeGroup);
    }

    @SuppressWarnings("unchecked")
    private ProcessorTopology build(String applicationId, Set<String> nodeGroup) {
        List<ProcessorNode> processorNodes = new ArrayList<>(nodeFactories.size());
        Map<String, ProcessorNode> processorMap = new HashMap<>();
        Map<String, SourceNode> topicSourceMap = new HashMap<>();
        Map<String, StateStoreSupplier> stateStoreMap = new HashMap<>();

        // create processor nodes in a topological order ("nodeFactories" is already topologically sorted)
        for (NodeFactory factory : nodeFactories.values()) {
            if (nodeGroup == null || nodeGroup.contains(factory.name)) {
                ProcessorNode node = factory.build(applicationId);
                processorNodes.add(node);
                processorMap.put(node.name(), node);

                if (factory instanceof ProcessorNodeFactory) {
                    for (String parent : ((ProcessorNodeFactory) factory).parents) {
                        processorMap.get(parent).addChild(node);
                    }
                    for (String stateStoreName : ((ProcessorNodeFactory) factory).stateStoreNames) {
                        if (!stateStoreMap.containsKey(stateStoreName)) {
                            stateStoreMap.put(stateStoreName, stateFactories.get(stateStoreName).supplier);
                        }
                    }
                } else if (factory instanceof SourceNodeFactory) {
                    for (String topic : ((SourceNodeFactory) factory).topics) {
                        if (internalTopicNames.contains(topic)) {
                            // prefix the internal topic name with the application id
                            topicSourceMap.put(applicationId + "-" + topic, (SourceNode) node);
                        } else {
                            topicSourceMap.put(topic, (SourceNode) node);
                        }
                    }
                } else if (factory instanceof SinkNodeFactory) {
                    for (String parent : ((SinkNodeFactory) factory).parents) {
                        processorMap.get(parent).addChild(node);
                    }
                } else {
                    throw new TopologyBuilderException("Unknown definition class: " + factory.getClass().getName());
                }
            }
        }

        return new ProcessorTopology(processorNodes, topicSourceMap, new ArrayList<>(stateStoreMap.values()));
    }

    /**
     * Get the names of topics that are to be consumed by the source nodes created by this builder.
     * @return the unmodifiable set of topic names used by source nodes, which changes as new sources are added; never null
     */
    public Set<String> sourceTopics(String applicationId) {
        Set<String> topics = new HashSet<>();
        for (String topic : sourceTopicNames) {
            if (internalTopicNames.contains(topic)) {
                topics.add(applicationId + "-" + topic);
            } else {
                topics.add(topic);
            }
        }
        return Collections.unmodifiableSet(topics);
    }
}
