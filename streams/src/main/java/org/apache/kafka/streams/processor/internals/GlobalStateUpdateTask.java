/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Updates the state for all Global State Stores.
 */
public class GlobalStateUpdateTask implements GlobalStateMaintainer {

    private class SourceNodeAndDeserializer {
        private final SourceNode sourceNode;
        private final RecordDeserializer deserializer;

        SourceNodeAndDeserializer(final SourceNode sourceNode,
                                  final RecordDeserializer deserializer) {
            this.sourceNode = sourceNode;
            this.deserializer = deserializer;
        }
    }

    private final ProcessorTopology topology;
    private final InternalProcessorContext processorContext;
    private final Map<TopicPartition, Long> offsets = new HashMap<>();
    private final Map<String, SourceNodeAndDeserializer> deserializers = new HashMap<>();
    private final GlobalStateManager stateMgr;


    public GlobalStateUpdateTask(final ProcessorTopology topology,
                                 final InternalProcessorContext processorContext,
                                 final GlobalStateManager stateMgr) {

        this.topology = topology;
        this.stateMgr = stateMgr;
        this.processorContext = processorContext;
    }

    @SuppressWarnings("unchecked")
    public Map<TopicPartition, Long> initialize() {
        final Set<String> storeNames = stateMgr.initialize(processorContext);
        final Map<String, String> storeNameToTopic = topology.storeToChangelogTopic();
        for (final String storeName : storeNames) {
            final String sourceTopic = storeNameToTopic.get(storeName);
            final SourceNode source = topology.source(sourceTopic);
            deserializers.put(sourceTopic, new SourceNodeAndDeserializer(source, new SourceNodeRecordDeserializer(source)));
        }
        initTopology();
        processorContext.initialized();
        return stateMgr.checkpointedOffsets();
    }


    @SuppressWarnings("unchecked")
    @Override
    public void update(final ConsumerRecord<byte[], byte[]> record) {
        final SourceNodeAndDeserializer sourceNodeAndDeserializer = deserializers.get(record.topic());
        final ConsumerRecord<Object, Object> deserialized = sourceNodeAndDeserializer.deserializer.deserialize(record);
        final ProcessorRecordContext recordContext =
                new ProcessorRecordContext(deserialized.timestamp(),
                                           deserialized.offset(),
                                           deserialized.partition(),
                                           deserialized.topic());
        processorContext.setRecordContext(recordContext);
        processorContext.setCurrentNode(sourceNodeAndDeserializer.sourceNode);
        sourceNodeAndDeserializer.sourceNode.process(deserialized.key(), deserialized.value());
        offsets.put(new TopicPartition(record.topic(), record.partition()), deserialized.offset() + 1);
    }

    public void flushState() {
        stateMgr.flush(processorContext);
    }

    public void close() throws IOException {
        stateMgr.close(offsets);
    }

    private void initTopology() {
        for (ProcessorNode node : this.topology.processors()) {
            processorContext.setCurrentNode(node);
            try {
                node.init(this.processorContext);
            } finally {
                processorContext.setCurrentNode(null);
            }
        }
    }


}
