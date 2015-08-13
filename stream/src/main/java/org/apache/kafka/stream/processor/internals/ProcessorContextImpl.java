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

package org.apache.kafka.stream.processor.internals;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.stream.processor.KafkaSource;
import org.apache.kafka.stream.processor.PTopologyBuilder;
import org.apache.kafka.stream.processor.ProcessorConfig;
import org.apache.kafka.stream.processor.ProcessorContext;
import org.apache.kafka.stream.processor.ProcessorProperties;
import org.apache.kafka.stream.processor.RecordCollector;
import org.apache.kafka.stream.processor.StateStore;
import org.apache.kafka.stream.processor.KafkaProcessor;
import org.apache.kafka.stream.processor.RestoreFunc;
import org.apache.kafka.stream.processor.TimestampExtractor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;


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

    public final int id;
    public final StreamGroup streamGroup;
    public final Ingestor ingestor;

    private final Metrics metrics;
    private final PTopology topology;
    private final RecordCollectorImpl collector;
    private final ProcessorStateManager stateMgr;
    private final ProcessorProperties processorProperties;
    private final ProcessorConfig processorConfig;
    private final TimestampExtractor timestampExtractor;

    private boolean initialized;

    @SuppressWarnings("unchecked")
    public ProcessorContextImpl(int id,
                                Ingestor ingestor,
                                PTopology topology,
                                RecordCollectorImpl collector,
                                ProcessorProperties processorProperties,
                                ProcessorConfig processorConfig,
                                Metrics metrics) throws IOException {
        this.id = id;
        this.metrics = metrics;
        this.ingestor = ingestor;
        this.topology = topology;
        this.collector = collector;
        this.processorProperties = processorProperties;
        this.processorConfig = processorConfig;
        this.timestampExtractor = this.processorProperties.timestampExtractor();

        for (String topic : this.topology.topics()) {
            if (!ingestor.topics().contains(topic))
                throw new IllegalArgumentException("topic not subscribed: " + topic);
        }

        File stateFile = new File(processorConfig.stateDir, Integer.toString(id));
        Consumer restoreConsumer = new KafkaConsumer<>(processorProperties.config(), null, new ByteArrayDeserializer(), new ByteArrayDeserializer());

        this.stateMgr = new ProcessorStateManager(id, stateFile, restoreConsumer);
        this.streamGroup = new StreamGroup(this, this.ingestor, new TimeBasedChooser(), this.timestampExtractor, this.processorConfig.bufferedRecordsPerPartition);

        stateMgr.init();

        initialized = false;
    }

    public void addPartition(TopicPartition partition) {
        // update the partition -> source stream map
        KafkaSource source = topology.source(partition.topic());

        this.streamGroup.addPartition(partition, source);
        this.ingestor.addPartitionStreamToGroup(this.streamGroup, partition);
    }

    @Override
    public boolean joinable(ProcessorContext o) {

        ProcessorContextImpl other = (ProcessorContextImpl) o;

        if (this.streamGroup != other.streamGroup)
            return false;

        Set<TopicPartition> partitions = this.streamGroup.partitions();
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
        return processorProperties.keySerializer();
    }

    @Override
    public Serializer<?> valueSerializer() {
        return processorProperties.valueSerializer();
    }

    @Override
    public Deserializer<?> keyDeserializer() {
        return processorProperties.keyDeserializer();
    }

    @Override
    public Deserializer<?> valueDeserializer() {
        return processorProperties.valueDeserializer();
    }

    @Override
    public RecordCollector recordCollector() {
        return collector;
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

    @Override
    public void flush() {
        stateMgr.flush();
    }

    public String topic() {
        if (streamGroup.record() == null)
            throw new IllegalStateException("this should not happen as topic() should only be called while a record is processed");

        return streamGroup.record().topic();
    }

    @Override
    public int partition() {
        if (streamGroup.record() == null)
            throw new IllegalStateException("this should not happen as partition() should only be called while a record is processed");

        return streamGroup.record().partition();
    }

    @Override
    public long offset() {
        if (this.streamGroup.record() == null)
            throw new IllegalStateException("this should not happen as offset() should only be called while a record is processed");

        return this.streamGroup.record().offset();
    }

    @Override
    public long timestamp() {
        if (streamGroup.record() == null)
            throw new IllegalStateException("this should not happen as timestamp() should only be called while a record is processed");

        return streamGroup.record().timestamp;
    }

    @Override
    public void send(String topic, Object key, Object value) {
        collector.send(new ProducerRecord<>(topic, key, value));
    }

    @Override
    public void send(String topic, Object key, Object value, Serializer<Object> keySerializer, Serializer<Object> valSerializer) {
        if (keySerializer == null || valSerializer == null)
            throw new IllegalStateException("key and value serializers must be specified");

        collector.send(new ProducerRecord<>(topic, key, value), keySerializer, valSerializer);
    }

    @Override
    public void commit() {
        streamGroup.commitOffset();
    }

    @Override
    public void schedule(KafkaProcessor processor, long interval) {
        streamGroup.schedule(processor, interval);
    }

    public void initialized() {
        initialized = true;
    }

    public Map<TopicPartition, Long> consumedOffsets() {
        return streamGroup.consumedOffsets();
    }

    public void close() throws Exception {
        topology.close();
        stateMgr.close(collector.offsets());
        streamGroup.close();
    }

}
