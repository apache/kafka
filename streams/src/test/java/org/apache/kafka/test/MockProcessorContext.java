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

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.RestoreFunc;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.internals.ProcessorNode;

import java.io.File;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockProcessorContext implements ProcessorContext {

    private Serializer serializer;
    private Deserializer deserializer;
    private ProcessorNode node;
    private final ArrayDeque<ProcessorNode> nodeStack = new ArrayDeque<ProcessorNode>();

    private Map<String, StateStore> storeMap = new HashMap<>();

    long timestamp = -1L;

    public MockProcessorContext(Serializer<?> serializer, Deserializer<?> deserializer) {
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    public void setTime(long timestamp) {
        this.timestamp = timestamp;
    }

    public int id() {
        return -1;
    }

    @Override
    public boolean joinable() {
        return true;
    }

    @Override
    public Serializer<?> keySerializer() {
        return serializer;
    }

    @Override
    public Serializer<?> valueSerializer() {
        return serializer;
    }

    @Override
    public Deserializer<?> keyDeserializer() {
        return deserializer;
    }

    @Override
    public Deserializer<?> valueDeserializer() {
        return deserializer;
    }

    @Override
    public File stateDir() {
        throw new UnsupportedOperationException("stateDir() not supported.");
    }

    @Override
    public Metrics metrics() {
        throw new UnsupportedOperationException("metrics() not supported.");
    }

    @Override
    public void register(StateStore store, RestoreFunc func) {
        if (func != null) new UnsupportedOperationException("RestoreFunc not supported.");
        storeMap.put(store.name(), store);
    }

    @Override
    public StateStore getStateStore(String name) {
        return storeMap.get(name);
    }

    @Override
    public void schedule(Processor processor, long interval) {
        throw new UnsupportedOperationException("schedule() not supported");
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> void forward(K key, V value) {
        for (ProcessorNode childNode : (List<ProcessorNode<K, V>>) node().children()) {
            pushNode(childNode);
            try {
                childNode.process(key, value);
            } finally {
                popNode();
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> void forward(K key, V value, int childIndex) {
        ProcessorNode childNode = (ProcessorNode<K, V>) node().children().get(childIndex);
        pushNode(childNode);
        try {
            childNode.process(key, value);
        } finally {
            popNode();
        }
    }

    @Override
    public void commit() {
        throw new UnsupportedOperationException("commit() not supported.");
    }

    @Override
    public String topic() {
        throw new UnsupportedOperationException("topic() not supported.");
    }

    @Override
    public int partition() {
        throw new UnsupportedOperationException("partition() not supported.");
    }

    @Override
    public long offset() {
        throw new UnsupportedOperationException("offset() not supported.");
    }

    @Override
    public long timestamp() {
        return this.timestamp;
    }

    public void pushNode(ProcessorNode node) {
        nodeStack.push(node);
        this.node = node;
    }

    public ProcessorNode popNode() {
        ProcessorNode node = nodeStack.pop();
        this.node = nodeStack.peek();
        return node;
    }

    private ProcessorNode node() {
        return this.node;
    }
}
