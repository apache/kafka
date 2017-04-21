/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;
import org.apache.kafka.streams.processor.internals.RecordCollectorImpl;
import org.apache.kafka.streams.state.internals.ThreadCache;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class KStreamTestDriver {

    private static final long DEFAULT_CACHE_SIZE_BYTES = 1 * 1024 * 1024L;

    private final ProcessorTopology topology;
    private final MockProcessorContext context;
    private final ProcessorTopology globalTopology;

    public KStreamTestDriver(final KStreamBuilder builder) {
        this(builder, null, Serdes.ByteArray(), Serdes.ByteArray());
    }

    public KStreamTestDriver(final KStreamBuilder builder, final File stateDir) {
        this(builder, stateDir, Serdes.ByteArray(), Serdes.ByteArray());
    }

    public KStreamTestDriver(final KStreamBuilder builder, final File stateDir, final long cacheSize) {
        this(builder, stateDir, Serdes.ByteArray(), Serdes.ByteArray(), cacheSize);
    }

    public KStreamTestDriver(final KStreamBuilder builder,
                             final File stateDir,
                             final Serde<?> keySerde,
                             final Serde<?> valSerde) {
        this(builder, stateDir, keySerde, valSerde, DEFAULT_CACHE_SIZE_BYTES);
    }

    public KStreamTestDriver(final KStreamBuilder builder,
                             final File stateDir,
                             final Serde<?> keySerde,
                             final Serde<?> valSerde,
                             final long cacheSize) {
        builder.setApplicationId("TestDriver");
        topology = builder.build(null);
        globalTopology = builder.buildGlobalStateTopology();
        final ThreadCache cache = new ThreadCache("testCache", cacheSize, new MockStreamsMetrics(new Metrics()));
        context = new MockProcessorContext(stateDir, keySerde, valSerde, new MockRecordCollector(), cache);
        context.setRecordContext(new ProcessorRecordContext(0, 0, 0, "topic"));
        // init global topology first as it will add stores to the
        // store map that are required for joins etc.
        if (globalTopology != null) {
            initTopology(globalTopology, globalTopology.globalStateStores());
        }
        initTopology(topology, topology.stateStores());
    }

    private void initTopology(final ProcessorTopology topology, final List<StateStore> stores) {
        for (final StateStore store : stores) {
            store.init(context, store);
        }

        for (final ProcessorNode node : topology.processors()) {
            context.setCurrentNode(node);
            try {
                node.init(context);
            } finally {
                context.setCurrentNode(null);
            }
        }
    }

    public ProcessorTopology topology() {
        return topology;
    }

    public ProcessorContext context() {
        return context;
    }

    public void process(final String topicName, final Object key, final Object value) {
        final ProcessorNode prevNode = context.currentNode();
        ProcessorNode currNode = topology.source(topicName);
        if (currNode == null && globalTopology != null) {
            currNode = globalTopology.source(topicName);
        }

        // if currNode is null, check if this topic is a changelog topic;
        // if yes, skip
        if (topicName.endsWith(ProcessorStateManager.STATE_CHANGELOG_TOPIC_SUFFIX)) {
            return;
        }
        context.setRecordContext(createRecordContext(context.timestamp()));
        context.setCurrentNode(currNode);
        try {
            context.forward(key, value);
        } finally {
            context.setCurrentNode(prevNode);
        }
    }

    public void punctuate(final long timestamp) {
        final ProcessorNode prevNode = context.currentNode();
        for (final ProcessorNode processor : topology.processors()) {
            if (processor.processor() != null) {
                context.setRecordContext(createRecordContext(timestamp));
                context.setCurrentNode(processor);
                try {
                    processor.processor().punctuate(timestamp);
                } finally {
                    context.setCurrentNode(prevNode);
                }
            }
        }
    }

    public void setTime(final long timestamp) {
        context.setTime(timestamp);
    }

    public void close() {
        // close all processors
        for (final ProcessorNode node : topology.processors()) {
            context.setCurrentNode(node);
            try {
                node.close();
            } finally {
                context.setCurrentNode(null);
            }
        }

        context.close();
        closeState();
    }

    public Set<String> allProcessorNames() {
        final Set<String> names = new HashSet<>();

        final List<ProcessorNode> nodes = topology.processors();

        for (final ProcessorNode node: nodes) {
            names.add(node.name());
        }

        return names;
    }

    public ProcessorNode processor(final String name) {
        final List<ProcessorNode> nodes = topology.processors();

        for (final ProcessorNode node: nodes) {
            if (node.name().equals(name)) {
                return node;
            }
        }

        return null;
    }

    public Map<String, StateStore> allStateStores() {
        return context.allStateStores();
    }

    public void flushState() {
        for (final StateStore stateStore : context.allStateStores().values()) {
            stateStore.flush();
        }
    }

    private void closeState() {
        // we need to first flush all stores before trying to close any one
        // of them since the flushing could cause eviction and hence tries to access other stores
        flushState();

        for (final StateStore stateStore : context.allStateStores().values()) {
            stateStore.close();
        }
    }

    private ProcessorRecordContext createRecordContext(final long timestamp) {
        return new ProcessorRecordContext(timestamp, -1, -1, "topic");
    }

    private class MockRecordCollector extends RecordCollectorImpl {
        MockRecordCollector() {
            super(null, "KStreamTestDriver");
        }

        @Override
        public <K, V> void send(final String topic,
                                final K key,
                                final V value,
                                final Long timestamp,
                                final Serializer<K> keySerializer,
                                final Serializer<V> valueSerializer,
                                final StreamPartitioner<? super K, ? super V> partitioner) {
            // The serialization is skipped.
            process(topic, key, value);
        }

        @Override
        public <K, V> void send(final String topic,
                                final K key,
                                final V value,
                                final Integer partition,
                                final Long timestamp,
                                final Serializer<K> keySerializer,
                                final Serializer<V> valueSerializer) {
        // The serialization is skipped.
            process(topic, key, value);
        }

        @Override
        public void flush() {}

        @Override
        public void close() {}
    }
}
