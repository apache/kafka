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

import org.apache.kafka.streams.kstream.internals.CacheFlushListener;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.ProcessorRecordContext;
import org.apache.kafka.streams.processor.RecordContext;

/**
 * A {@link CacheFlushListener} that forwards the change to all child Processors
 */
public class ProcessorNodeCacheFlushListener<K, V> implements CacheFlushListener<K, V> {
    private final ProcessorNode<K, V> node;

    public ProcessorNodeCacheFlushListener(final ProcessorNode<K, V> node) {
        this.node = node;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void forward(final K key, final Change<V> value, final RecordContext recordContext, final InternalProcessorContext context) {
        final ProcessorRecordContext current = context.processorRecordContext();
        try {
            for (ProcessorNode processorNode : node.children()) {
                final ProcessorRecordContextImpl processorRecordContext = new ProcessorRecordContextImpl(recordContext.timestamp(),
                                                                                                         recordContext.offset(),
                                                                                                         recordContext.partition(),
                                                                                                         recordContext.topic(),
                                                                                                         processorNode);
                context.setRecordContext(processorRecordContext);
                processorNode.process(key, value);
            }
        } finally {
            context.setRecordContext(current);
        }

    }
}
