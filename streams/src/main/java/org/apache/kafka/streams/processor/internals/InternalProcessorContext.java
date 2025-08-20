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

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.Task.TaskType;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.streams.state.internals.ThreadCache.DirtyEntryFlushListener;

/**
 * For internal use, so we can update the {@link RecordContext} and current
 * {@link ProcessorNode} when we are forwarding items that have been evicted or flushed from
 * {@link ThreadCache}
 */
public interface InternalProcessorContext<KOut, VOut>
    extends ProcessorContext,
    org.apache.kafka.streams.processor.api.ProcessorContext<KOut, VOut>,
    org.apache.kafka.streams.processor.api.FixedKeyProcessorContext<KOut, VOut>,
    StateStoreContext {

    BytesSerializer BYTES_KEY_SERIALIZER = new BytesSerializer();
    ByteArraySerializer BYTEARRAY_VALUE_SERIALIZER = new ByteArraySerializer();

    @Override
    StreamsMetricsImpl metrics();

    /**
     * @param timeMs current wall-clock system timestamp in milliseconds
     */
    void setSystemTimeMs(long timeMs);

    /**
     * Returns the current {@link RecordContext}
     * @return the current {@link RecordContext}
     */
    ProcessorRecordContext recordContext();

    /**
     * @param recordContext the {@link ProcessorRecordContext} for the record about to be processes
     */
    void setRecordContext(ProcessorRecordContext recordContext);

    /**
     * @param currentNode the current {@link ProcessorNode}
     */
    void setCurrentNode(ProcessorNode<?, ?, ?, ?> currentNode);

    /**
     * Get the current {@link ProcessorNode}
     */
    ProcessorNode<?, ?, ?, ?> currentNode();

    /**
     * Get the thread-global cache
     */
    ThreadCache cache();

    /**
     * Mark this context as being initialized
     */
    void initialize();

    /**
     * Mark this context as being uninitialized
     */
    void uninitialize();

    /**
     * @return the type of task (active/standby/global) that this context corresponds to
     */
    TaskType taskType();

    /**
     * Transition to active task and register a new task and cache to this processor context
     */
    void transitionToActive(final StreamTask streamTask, final RecordCollector recordCollector, final ThreadCache newCache);

    /**
     * Transition to standby task and register a dummy cache to this processor context
     */
    void transitionToStandby(final ThreadCache newCache);

    /**
     * Register a dirty entry flush listener for a particular namespace
     */
    void registerCacheFlushListener(final String namespace, final DirtyEntryFlushListener listener);

    /**
     * Get a correctly typed state store, given a handle on the original builder.
     */
    @SuppressWarnings("unchecked")
    default <T extends StateStore> T getStateStore(final StoreBuilder<T> builder) {
        return (T) getStateStore(builder.name());
    }

    void logChange(final String storeName,
                   final Bytes key,
                   final byte[] value,
                   final long timestamp,
                   final Position position);

    String changelogFor(final String storeName);

    void addProcessorMetadataKeyValue(final String key, final long value);

    Long processorMetadataForKey(final String key);

    void setProcessorMetadata(final ProcessorMetadata metadata);

    ProcessorMetadata processorMetadata();

}
