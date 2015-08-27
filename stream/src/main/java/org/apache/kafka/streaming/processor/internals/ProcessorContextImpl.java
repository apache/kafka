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

package org.apache.kafka.streaming.processor.internals;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streaming.StreamingConfig;
import org.apache.kafka.streaming.processor.ProcessorContext;
import org.apache.kafka.streaming.processor.StateStore;
import org.apache.kafka.streaming.processor.Processor;
import org.apache.kafka.streaming.processor.RestoreFunc;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ProcessorContextImpl implements ProcessorContext {

    private static final Logger log = LoggerFactory.getLogger(ProcessorContextImpl.class);

    private final int id;
    private final StreamTask task;
    private final Metrics metrics;
    private final RecordCollector collector;
    private final ProcessorStateManager stateMgr;

    private final Serializer<?> keySerializer;
    private final Serializer<?> valSerializer;
    private final Deserializer<?> keyDeserializer;
    private final Deserializer<?> valDeserializer;

    private boolean initialized;

    @SuppressWarnings("unchecked")
    public ProcessorContextImpl(int id,
                                StreamTask task,
                                StreamingConfig config,
                                RecordCollector collector,
                                Metrics metrics) throws IOException {
        this.id = id;
        this.task = task;
        this.metrics = metrics;
        this.collector = collector;

        this.keySerializer = config.getConfiguredInstance(StreamingConfig.KEY_SERIALIZER_CLASS_CONFIG, Serializer.class);
        this.valSerializer = config.getConfiguredInstance(StreamingConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serializer.class);
        this.keyDeserializer = config.getConfiguredInstance(StreamingConfig.KEY_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
        this.valDeserializer = config.getConfiguredInstance(StreamingConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Deserializer.class);

        File stateFile = new File(config.getString(StreamingConfig.STATE_DIR_CONFIG), Integer.toString(id));

        Consumer restoreConsumer = new KafkaConsumer<>(
            config.getConsumerProperties(),
            null /* no callback for restore consumer */,
            new ByteArrayDeserializer(),
            new ByteArrayDeserializer());

        this.stateMgr = new ProcessorStateManager(id, stateFile, restoreConsumer);

        this.initialized = false;
    }

    public ProcessorStateManager stateManager() {
        return this.stateMgr;
    }

    public RecordCollector recordCollector() {
        return this.collector;
    }

    @Override
    public boolean joinable() {
        Set<TopicPartition> partitions = this.task.partitions();
        Map<Integer, List<String>> partitionsById = new HashMap<>();
        int firstId = -1;
        for (TopicPartition partition : partitions) {
            if (!partitionsById.containsKey(partition.partition())) {
                partitionsById.put(partition.partition(), new ArrayList<String>());
            }
            partitionsById.get(partition.partition()).add(partition.topic());

            if (firstId < 0)
                firstId = partition.partition();
        }

        List<String> topics = partitionsById.get(firstId);
        for (List<String> topicsPerPartition : partitionsById.values()) {
            if (topics.size() != topicsPerPartition.size())
                return false;

            for (String topic : topicsPerPartition) {
                if (!topics.contains(topic))
                    return false;
            }
        }

        return true;
    }

    @Override
    public int id() {
        return id;
    }

    @Override
    public Serializer<?> keySerializer() {
        return this.keySerializer;
    }

    @Override
    public Serializer<?> valueSerializer() {
        return this.valSerializer;
    }

    @Override
    public Deserializer<?> keyDeserializer() {
        return this.keyDeserializer;
    }

    @Override
    public Deserializer<?> valueDeserializer() {
        return this.valDeserializer;
    }

    @Override
    public File stateDir() {
        return stateMgr.baseDir();
    }

    @Override
    public Metrics metrics() {
        return metrics;
    }

    @Override
    public void register(StateStore store, RestoreFunc restoreFunc) {
        if (initialized)
            throw new KafkaException("Can only create state stores during initialization.");

        stateMgr.register(store, restoreFunc);
    }

    public StateStore getStateStore(String name) {
        return stateMgr.getStore(name);
    }

    @Override
    public String topic() {
        if (task.record() == null)
            throw new IllegalStateException("this should not happen as topic() should only be called while a record is processed");

        return task.record().topic();
    }

    @Override
    public int partition() {
        if (task.record() == null)
            throw new IllegalStateException("this should not happen as partition() should only be called while a record is processed");

        return task.record().partition();
    }

    @Override
    public long offset() {
        if (this.task.record() == null)
            throw new IllegalStateException("this should not happen as offset() should only be called while a record is processed");

        return this.task.record().offset();
    }

    @Override
    public long timestamp() {
        if (task.record() == null)
            throw new IllegalStateException("this should not happen as timestamp() should only be called while a record is processed");

        return task.record().timestamp;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> void forward(K key, V value) {
        for (ProcessorNode childNode : (List<ProcessorNode<K, V>>) task.node().children()) {
            task.node(childNode);
            childNode.process(key, value);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> void forward(K key, V value, int childIndex) {
        ProcessorNode childNode = (ProcessorNode<K, V>) task.node().children().get(childIndex);
        task.node(childNode);
        childNode.process(key, value);
    }

    @Override
    public void commit() {
        task.needCommit();
    }

    @Override
    public void schedule(Processor processor, long interval) {
        task.schedule(processor, interval);
    }
}
