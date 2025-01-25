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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.api.Record;

import org.slf4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensor;

/**
 * Updates the state for all Global State Stores.
 */
public class GlobalStateUpdateTask implements GlobalStateMaintainer {
    private final Logger log;
    private final LogContext logContext;

    private final ProcessorTopology topology;
    private final InternalProcessorContext processorContext;
    private final Map<TopicPartition, Long> offsets = new HashMap<>();
    private final Map<String, RecordDeserializer> deserializers = new HashMap<>();
    private final GlobalStateManager stateMgr;
    private final DeserializationExceptionHandler deserializationExceptionHandler;
    private final Time time;
    private final long flushInterval;
    private long lastFlush;

    public GlobalStateUpdateTask(final LogContext logContext,
                                 final ProcessorTopology topology,
                                 final InternalProcessorContext processorContext,
                                 final GlobalStateManager stateMgr,
                                 final DeserializationExceptionHandler deserializationExceptionHandler,
                                 final Time time,
                                 final long flushInterval
                                 ) {
        this.logContext = logContext;
        this.log = logContext.logger(getClass());
        this.topology = topology;
        this.stateMgr = stateMgr;
        this.processorContext = processorContext;
        this.deserializationExceptionHandler = deserializationExceptionHandler;
        this.time = time;
        this.flushInterval = flushInterval;
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
            final SourceNode<?, ?> source = topology.source(sourceTopic);
            deserializers.put(
                sourceTopic,
                new RecordDeserializer(
                    source,
                    deserializationExceptionHandler,
                    logContext,
                    droppedRecordsSensor(
                        Thread.currentThread().getName(),
                        processorContext.taskId().toString(),
                        processorContext.metrics()
                    )
                )
            );
        }
        initTopology();
        processorContext.initialize();
        lastFlush = time.milliseconds();
        return stateMgr.changelogOffsets();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void update(final ConsumerRecord<byte[], byte[]> record) {
        final RecordDeserializer sourceNodeAndDeserializer = deserializers.get(record.topic());
        final ConsumerRecord<Object, Object> deserialized = sourceNodeAndDeserializer.deserialize(processorContext, record);

        if (deserialized != null) {
            final ProcessorRecordContext recordContext =
                new ProcessorRecordContext(
                    deserialized.timestamp(),
                    deserialized.offset(),
                    deserialized.partition(),
                    deserialized.topic(),
                    deserialized.headers());
            processorContext.setRecordContext(recordContext);
            processorContext.setCurrentNode(sourceNodeAndDeserializer.sourceNode());
            final Record<Object, Object> toProcess = new Record<>(
                deserialized.key(),
                deserialized.value(),
                processorContext.recordContext().timestamp(),
                processorContext.recordContext().headers()
            );
            ((SourceNode<Object, Object>) sourceNodeAndDeserializer.sourceNode()).process(toProcess);
        }

        offsets.put(new TopicPartition(record.topic(), record.partition()), record.offset() + 1);
    }

    public void flushState() {
        // this could theoretically throw a ProcessorStateException caused by a ProducerFencedException,
        // but in practice this shouldn't happen for global state update tasks, since the stores are not
        // logged and there are no downstream operators after global stores.
        stateMgr.flush();
        stateMgr.updateChangelogOffsets(offsets);
        stateMgr.checkpoint();
    }

    public void close(final boolean wipeStateStore) throws IOException {
        stateMgr.close();
        if (wipeStateStore) {
            try {
                log.info("Deleting global task directory after detecting corruption.");
                Utils.delete(stateMgr.baseDir());
            } catch (final IOException e) {
                log.error("Failed to delete global task directory after detecting corruption.", e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void initTopology() {
        for (final ProcessorNode<?, ?, ?, ?> node : this.topology.processors()) {
            processorContext.setCurrentNode(node);
            try {
                node.init(this.processorContext);
            } finally {
                processorContext.setCurrentNode(null);
            }
        }
    }

    @Override
    public void maybeCheckpoint() {
        final long now = time.milliseconds();
        if (now - flushInterval >= lastFlush && StateManagerUtil.checkpointNeeded(false, stateMgr.changelogOffsets(), offsets)) {
            flushState();
            lastFlush = now;
        }
    }

}
