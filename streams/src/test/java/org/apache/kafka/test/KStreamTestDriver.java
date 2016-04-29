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

package org.apache.kafka.test;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;
import org.apache.kafka.streams.processor.internals.RecordCollector;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class KStreamTestDriver {

    private final ProcessorTopology topology;
    private final MockProcessorContext context;
    public final File stateDir;

    private ProcessorNode currNode;

    public KStreamTestDriver(KStreamBuilder builder) {
        this(builder, null, Serdes.ByteArray(), Serdes.ByteArray());
    }

    public KStreamTestDriver(KStreamBuilder builder, File stateDir) {
        this(builder, stateDir, Serdes.ByteArray(), Serdes.ByteArray());
    }

    public KStreamTestDriver(KStreamBuilder builder,
                             File stateDir,
                             Serde<?> keySerde,
                             Serde<?> valSerde) {
        this.topology = builder.build("X", null);
        this.stateDir = stateDir;
        this.context = new MockProcessorContext(this, stateDir, keySerde, valSerde, new MockRecordCollector());
        this.context.setTime(0L);

        for (StateStoreSupplier stateStoreSupplier : topology.stateStoreSuppliers()) {
            StateStore store = stateStoreSupplier.get();
            store.init(context, store);
        }

        for (ProcessorNode node : topology.processors()) {
            currNode = node;
            try {
                node.init(context);
            } finally {
                currNode = null;
            }
        }
    }

    public ProcessorContext context() {
        return context;
    }

    public void process(String topicName, Object key, Object value) {
        currNode = topology.source(topicName);

        // if currNode is null, check if this topic is a changelog topic;
        // if yes, skip
        if (topicName.endsWith(ProcessorStateManager.STATE_CHANGELOG_TOPIC_SUFFIX))
            return;

        try {
            forward(key, value);
        } finally {
            currNode = null;
        }
    }

    public void punctuate(long timestamp) {
        setTime(timestamp);

        for (ProcessorNode processor : topology.processors()) {
            if (processor.processor() != null) {
                currNode = processor;
                try {
                    processor.processor().punctuate(timestamp);
                } finally {
                    currNode = null;
                }
            }
        }
    }

    public void setTime(long timestamp) {
        context.setTime(timestamp);
    }

    @SuppressWarnings("unchecked")
    public <K, V> void forward(K key, V value) {
        ProcessorNode thisNode = currNode;
        for (ProcessorNode childNode : (List<ProcessorNode<K, V>>) thisNode.children()) {
            currNode = childNode;
            try {
                childNode.process(key, value);
            } finally {
                currNode = thisNode;
            }
        }
    }

    @SuppressWarnings("unchecked")
    public <K, V> void forward(K key, V value, int childIndex) {
        ProcessorNode thisNode = currNode;
        ProcessorNode childNode = (ProcessorNode<K, V>) thisNode.children().get(childIndex);
        currNode = childNode;
        try {
            childNode.process(key, value);
        } finally {
            currNode = thisNode;
        }
    }

    @SuppressWarnings("unchecked")
    public <K, V> void forward(K key, V value, String childName) {
        ProcessorNode thisNode = currNode;
        for (ProcessorNode childNode : (List<ProcessorNode<K, V>>) thisNode.children()) {
            if (childNode.name().equals(childName)) {
                currNode = childNode;
                try {
                    childNode.process(key, value);
                } finally {
                    currNode = thisNode;
                }
                break;
            }
        }
    }

    public void close() {
        // close all processors
        for (ProcessorNode node : topology.processors()) {
            currNode = node;
            try {
                node.close();
            } finally {
                currNode = null;
            }
        }

        // close all state stores
        for (StateStore store : context.allStateStores().values()) {
            store.close();
        }
    }

    public Set<String> allProcessorNames() {
        Set<String> names = new HashSet<>();

        List<ProcessorNode> nodes = topology.processors();

        for (ProcessorNode node: nodes) {
            names.add(node.name());
        }

        return names;
    }

    public ProcessorNode processor(String name) {
        List<ProcessorNode> nodes = topology.processors();

        for (ProcessorNode node: nodes) {
            if (node.name().equals(name))
                return node;
        }

        return null;
    }

    public Map<String, StateStore> allStateStores() {
        return context.allStateStores();
    }

    private class MockRecordCollector extends RecordCollector {
        public MockRecordCollector() {
            super(null);
        }

        @Override
        public <K, V> void send(ProducerRecord<K, V> record, Serializer<K> keySerializer, Serializer<V> valueSerializer,
                                StreamPartitioner<K, V> partitioner) {
            // The serialization is skipped.
            process(record.topic(), record.key(), record.value());
        }

        @Override
        public <K, V> void send(ProducerRecord<K, V> record, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
            // The serialization is skipped.
            process(record.topic(), record.key(), record.value());
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    }

}
