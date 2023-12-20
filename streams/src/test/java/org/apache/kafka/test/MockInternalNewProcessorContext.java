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

import java.util.Objects;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.CommitCallback;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorMetadata;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.processor.internals.StreamTask;
import org.apache.kafka.streams.processor.internals.Task.TaskType;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.streams.state.internals.ThreadCache.DirtyEntryFlushListener;

import java.io.File;
import java.util.Properties;

public class MockInternalNewProcessorContext<KOut, VOut> extends MockProcessorContext<KOut, VOut> implements InternalProcessorContext<KOut, VOut> {

    private ProcessorNode currentNode;
    private long currentSystemTimeMs;
    private TaskType taskType = TaskType.ACTIVE;

    private long timestamp = 0;
    private Headers headers = new RecordHeaders();
    private ProcessorMetadata processorMetadata;

    public MockInternalNewProcessorContext() {
        processorMetadata = new ProcessorMetadata();
    }

    public MockInternalNewProcessorContext(final Properties config, final TaskId taskId, final File stateDir) {
        super(config, taskId, stateDir);
        processorMetadata = new ProcessorMetadata();
    }

    @Override
    public void setSystemTimeMs(long timeMs) {
        currentSystemTimeMs = timeMs;
    }

    @Override
    public long currentSystemTimeMs() {
        return currentSystemTimeMs;
    }

    @Override
    public long currentStreamTimeMs() {
        return 0;
    }

    @Override
    public StreamsMetricsImpl metrics() {
        return (StreamsMetricsImpl) super.metrics();
    }

    @Override
    public ProcessorRecordContext recordContext() {
        return new ProcessorRecordContext(timestamp(), offset(), partition(), topic(), headers());
    }

    @Override
    public void setRecordContext(final ProcessorRecordContext recordContext) {
        setRecordMetadata(
            recordContext.topic(),
            recordContext.partition(),
            recordContext.offset()
        );
        this.headers = recordContext.headers();
        this.timestamp = recordContext.timestamp();
    }

    public void setTimestamp(final long timestamp) {
        this.timestamp = timestamp;
    }

    public void setHeaders(final Headers headers) {
        this.headers = headers;
    }

    @Override
    public void setCurrentNode(final ProcessorNode currentNode) {
        this.currentNode = currentNode;
    }

    @Override
    public ProcessorNode currentNode() {
        return currentNode;
    }

    @Override
    public ThreadCache cache() {
        return null;
    }

    @Override
    public void initialize() {}

    @Override
    public void uninitialize() {}

    @Override
    public void register(final StateStore store,
                         final StateRestoreCallback stateRestoreCallback) {
        addStateStore(store);
    }

    @Override
    public void register(final StateStore store,
                         final StateRestoreCallback stateRestoreCallback,
                         final CommitCallback checkpoint) {
        addStateStore(store);
    }

    @Override
    public <K, V> void forward(K key, V value) {
        throw new UnsupportedOperationException("Migrate to new implementation");
    }

    @Override
    public <K, V> void forward(K key, V value, To to) {
        throw new UnsupportedOperationException("Migrate to new implementation");
    }

    @Override
    public String topic() {
        if (recordMetadata().isPresent()) return recordMetadata().get().topic();
        else return null;
    }

    @Override
    public int partition() {
        if (recordMetadata().isPresent()) return recordMetadata().get().partition();
        else return 0;
    }

    @Override
    public long offset() {
        if (recordMetadata().isPresent()) return recordMetadata().get().offset();
        else return 0;
    }

    @Override
    public Headers headers() {
        return headers;
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    @Override
    public TaskType taskType() {
        return taskType;
    }

    @Override
    public void logChange(final String storeName,
                          final Bytes key,
                          final byte[] value,
                          final long timestamp,
                          final Position position) {
    }

    @Override
    public void transitionToActive(final StreamTask streamTask, final RecordCollector recordCollector, final ThreadCache newCache) {
    }

    @Override
    public void transitionToStandby(final ThreadCache newCache) {
    }

    @Override
    public void registerCacheFlushListener(final String namespace, final DirtyEntryFlushListener listener) {
    }

    @Override
    public <T extends StateStore> T getStateStore(StoreBuilder<T> builder) {
        return getStateStore(builder.name());
    }

    @Override
    public String changelogFor(final String storeName) {
        return "mock-changelog";
    }

    @Override
    public void addProcessorMetadataKeyValue(final String key, final long value) {
        processorMetadata.put(key, value);
    }

    @Override
    public Long processorMetadataForKey(final String key) {
        return processorMetadata.get(key);
    }

    @Override
    public void setProcessorMetadata(final ProcessorMetadata metadata) {
        Objects.requireNonNull(metadata);
        processorMetadata = metadata;
    }

    @Override
    public ProcessorMetadata getProcessorMetadata() {
        return processorMetadata;
    }

    @Override
    public <K extends KOut, V extends VOut> void forward(final FixedKeyRecord<K, V> record) {
        forward(new Record<>(record.key(), record.value(), record.timestamp(), record.headers()));
    }

    @Override
    public <K extends KOut, V extends VOut> void forward(final FixedKeyRecord<K, V> record,
                                                         final String childName) {
        forward(
            new Record<>(record.key(), record.value(), record.timestamp(), record.headers()),
            childName
        );
    }
}