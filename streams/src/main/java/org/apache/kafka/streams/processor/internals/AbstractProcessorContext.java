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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.CommitCallback;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.Task.TaskType;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.ThreadCache;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public abstract class AbstractProcessorContext<KOut, VOut> implements InternalProcessorContext<KOut, VOut> {

    private final TaskId taskId;
    private final String applicationId;
    private final StreamsConfig config;
    private final StreamsMetricsImpl metrics;
    private final Serde<?> keySerde;
    private final Serde<?> valueSerde;
    private boolean initialized;
    protected ProcessorRecordContext recordContext;
    protected ProcessorNode<?, ?, ?, ?> currentNode;
    private long cachedSystemTimeMs;
    protected ThreadCache cache;
    private ProcessorMetadata processorMetadata;

    public AbstractProcessorContext(final TaskId taskId,
                                    final StreamsConfig config,
                                    final StreamsMetricsImpl metrics,
                                    final ThreadCache cache) {
        this.taskId = taskId;
        this.applicationId = config.getString(StreamsConfig.APPLICATION_ID_CONFIG);
        this.config = config;
        this.metrics = metrics;
        valueSerde = null;
        keySerde = null;
        this.cache = cache;
        processorMetadata = new ProcessorMetadata();
    }

    protected abstract StateManager stateManager();

    @Override
    public void setSystemTimeMs(final long timeMs) {
        cachedSystemTimeMs = timeMs;
    }

    @Override
    public long currentSystemTimeMs() {
        return cachedSystemTimeMs;
    }

    @Override
    public String applicationId() {
        return applicationId;
    }

    @Override
    public TaskId taskId() {
        return taskId;
    }

    @Override
    public Serde<?> keySerde() {
        if (keySerde == null) {
            return config.defaultKeySerde();
        }
        return keySerde;
    }

    @Override
    public Serde<?> valueSerde() {
        if (valueSerde == null) {
            return config.defaultValueSerde();
        }
        return valueSerde;
    }

    @Override
    public File stateDir() {
        return stateManager().baseDir();
    }

    @Override
    public StreamsMetricsImpl metrics() {
        return metrics;
    }

    @Override
    public void register(final StateStore store,
                         final StateRestoreCallback stateRestoreCallback) {
        register(store, stateRestoreCallback, () -> { });
    }

    @Override
    public void register(final StateStore store,
                         final StateRestoreCallback stateRestoreCallback,
                         final CommitCallback checkpoint) {
        if (initialized) {
            throw new IllegalStateException("Can only create state stores during initialization.");
        }
        Objects.requireNonNull(store, "store must not be null");
        stateManager().registerStore(store, stateRestoreCallback, checkpoint);
    }

    @Override
    public String topic() {
        if (recordContext == null) {
            // This is only exposed via the deprecated ProcessorContext,
            // in which case, we're preserving the pre-existing behavior
            // of returning dummy values when the record context is undefined.
            // For topic, the dummy value is `null`.
            return null;
        } else {
            return recordContext.topic();
        }
    }

    @Override
    public int partition() {
        if (recordContext == null) {
            // This is only exposed via the deprecated ProcessorContext,
            // in which case, we're preserving the pre-existing behavior
            // of returning dummy values when the record context is undefined.
            // For partition, the dummy value is `-1`.
            return -1;
        } else {
            return recordContext.partition();
        }
    }

    @Override
    public long offset() {
        if (recordContext == null) {
            // This is only exposed via the deprecated ProcessorContext,
            // in which case, we're preserving the pre-existing behavior
            // of returning dummy values when the record context is undefined.
            // For offset, the dummy value is `-1L`.
            return -1L;
        } else {
            return recordContext.offset();
        }
    }

    @Override
    public Headers headers() {
        if (recordContext == null) {
            // This is only exposed via the deprecated ProcessorContext,
            // in which case, we're preserving the pre-existing behavior
            // of returning dummy values when the record context is undefined.
            // For headers, the dummy value is an empty headers collection.
            return new RecordHeaders();
        } else {
            return recordContext.headers();
        }
    }

    @Override
    public long timestamp() {
        if (recordContext == null) {
            // This is only exposed via the deprecated ProcessorContext,
            // in which case, we're preserving the pre-existing behavior
            // of returning dummy values when the record context is undefined.
            // For timestamp, the dummy value is `0L`.
            return 0L;
        } else {
            return recordContext.timestamp();
        }
    }

    @Override
    public Map<String, Object> appConfigs() {
        final Map<String, Object> combined = new HashMap<>();
        combined.putAll(config.originals());
        combined.putAll(config.values());
        return combined;
    }

    @Override
    public Map<String, Object> appConfigsWithPrefix(final String prefix) {
        return config.originalsWithPrefix(prefix);
    }

    @Override
    public void setRecordContext(final ProcessorRecordContext recordContext) {
        this.recordContext = recordContext;
    }

    @Override
    public ProcessorRecordContext recordContext() {
        return recordContext;
    }

    @Override
    public Optional<RecordMetadata> recordMetadata() {
        return Optional.ofNullable(recordContext);
    }

    @Override
    public void setCurrentNode(final ProcessorNode<?, ?, ?, ?> currentNode) {
        this.currentNode = currentNode;
    }

    @Override
    public ProcessorNode<?, ?, ?, ?> currentNode() {
        return currentNode;
    }

    @Override
    public ThreadCache cache() {
        return cache;
    }

    @Override
    public void initialize() {
        initialized = true;
    }

    @Override
    public void uninitialize() {
        initialized = false;
    }

    @Override
    public TaskType taskType() {
        return stateManager().taskType();
    }

    @Override
    public String changelogFor(final String storeName) {
        return stateManager().changelogFor(storeName);
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
}