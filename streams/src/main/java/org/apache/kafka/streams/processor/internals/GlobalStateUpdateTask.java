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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.LockException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.StateStore;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensorOrSkippedRecordsSensor;

/**
 * Updates the state for all Global State Stores.
 */
public class GlobalStateUpdateTask implements GlobalTask {

    private final ProcessorTopology topology;
    private final InternalProcessorContext processorContext;
    private final Map<TopicPartition, Long> offsets = new HashMap<>();
    private final Map<String, RecordDeserializer> deserializers = new HashMap<>();
    private final Consumer<byte[], byte[]> globalConsumer;

    private final GlobalStateManager stateMgr;
    private final StateDirectory stateDirectory;
    private final DeserializationExceptionHandler deserializationExceptionHandler;
    private final LogContext logContext;

    public GlobalStateUpdateTask(final StreamsConfig config,
                                 final LogContext logContext,
                                 final ProcessorTopology topology,
                                 final GlobalStateManager stateMgr,
                                 final StateDirectory stateDirectory,
                                 final Consumer<byte[], byte[]> globalConsumer,
                                 final InternalProcessorContext processorContext) {
        this.topology = topology;
        this.stateMgr = stateMgr;
        this.logContext = logContext;
        this.globalConsumer = globalConsumer;
        this.stateDirectory = stateDirectory;
        this.processorContext = processorContext;
        this.deserializationExceptionHandler = config.defaultDeserializationExceptionHandler();
    }

    /**
     * @throws IllegalStateException If store gets registered after initialized is already finished
     * @throws StreamsException      If the store's change log does not contain the partition
     */
    @Override
    public Map<TopicPartition, Long> initialize() {

        final Set<String> storeNames = stateMgr.initialize();
        final Map<String, String> storeNameToTopic = topology.storeToChangelogTopic();
        for (final String storeName : storeNames) {
            final String sourceTopic = storeNameToTopic.get(storeName);
            final SourceNode source = topology.source(sourceTopic);
            deserializers.put(
                sourceTopic,
                new RecordDeserializer(
                    source,
                    deserializationExceptionHandler,
                    logContext,
                    droppedRecordsSensorOrSkippedRecordsSensor(
                        Thread.currentThread().getName(),
                        processorContext.taskId().toString(),
                        processorContext.metrics()
                    )
                )
            );
        }
        initializeTopology();
        processorContext.initialize();
        return stateMgr.changelogOffsets();
    }

    public void initializeTopology() {
        for (final ProcessorNode node : this.topology.processors()) {
            processorContext.setCurrentNode(node);
            try {
                node.init(this.processorContext);
            } finally {
                processorContext.setCurrentNode(null);
            }
        }

        processorContext.initialize();
    }

    public boolean initializeStateStores() {
        try {
            if (!stateDirectory.lockGlobalState()) {
                throw new LockException(String.format("Failed to lock the global state directory: %s", stateDirectory.globalStateDir()));
            }
        } catch (final IOException e) {
            throw new LockException(String.format("Failed to lock the global state directory: %s", stateDirectory.globalStateDir()), e);
        }

        final List<StateStore> stateStores = topology.globalStateStores();
        for (final StateStore stateStore : stateStores) {
            stateStore.init(processorContext, stateStore);

            // also initialize the deserializers for all source nodes
            final String sourceTopic = topology.storeToChangelogTopic().get(stateStore.name());
            final SourceNode source = topology.source(sourceTopic);
            deserializers.put(
                sourceTopic,
                new RecordDeserializer(
                    source,
                    deserializationExceptionHandler,
                    logContext,
                    droppedRecordsSensorOrSkippedRecordsSensor(
                        Thread.currentThread().getName(),
                        processorContext.taskId().toString(),
                        processorContext.metrics()
                    )
                )
            );
        }

        // global update task always have a global state store
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void update(final ConsumerRecord<byte[], byte[]> record) {
        final RecordDeserializer recordDeserializer = deserializers.get(record.topic());
        final ConsumerRecord<Object, Object> deserialized = recordDeserializer.deserialize(processorContext, record);

        if (deserialized != null) {
            final ProcessorRecordContext recordContext =
                new ProcessorRecordContext(deserialized.timestamp(),
                    deserialized.offset(),
                    deserialized.partition(),
                    deserialized.topic(),
                    deserialized.headers());
            processorContext.setRecordContext(recordContext);
            processorContext.setCurrentNode(recordDeserializer.sourceNode());
            recordDeserializer.sourceNode().process(deserialized.key(), deserialized.value());
        }

        offsets.put(new TopicPartition(record.topic(), record.partition()), record.offset());
    }

    public void flushState() {
        // this could theoretically throw a ProcessorStateException caused by a ProducerFencedException,
        // but in practice this shouldn't happen for global state update tasks, since the stores are not
        // logged and there are no downstream operators after global stores.
        stateMgr.flush();
        stateMgr.checkpoint(offsets);
    }

    public void close() throws IOException {
        stateMgr.close(true);
    }
}
