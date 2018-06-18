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

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.ThreadCache;

/**
 * For internal use so we can update the {@link RecordContext} and current
 * {@link ProcessorNode} when we are forwarding items that have been evicted or flushed from
 * {@link ThreadCache}
 */
public interface InternalProcessorContext extends ProcessorContext {

    @Override
    StreamsMetricsImpl metrics();

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
    void setCurrentNode(ProcessorNode currentNode);

    ProcessorNode currentNode();

    /**
     * Get the thread-global cache
     */
    ThreadCache getCache();

    /**
     * Mark this context as being initialized
     */
    void initialized();

    /**
     * Mark this context as being uninitialized
     */
    void uninitialize();

    Long streamTime();
}
