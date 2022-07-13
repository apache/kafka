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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.CacheFlushListener;

import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;

class TimestampedCacheFlushListener<KOut, VOut> implements CacheFlushListener<KOut, ValueAndTimestamp<VOut>> {

    private final InternalProcessorContext<KOut, Change<VOut>> context;

    @SuppressWarnings("rawtypes")
    private final ProcessorNode myNode;

    TimestampedCacheFlushListener(final ProcessorContext<KOut, Change<VOut>> context) {
        this.context = (InternalProcessorContext<KOut, Change<VOut>>) context;
        myNode = this.context.currentNode();
    }

    @Override
    public void apply(final Record<KOut, Change<ValueAndTimestamp<VOut>>> record) {
        @SuppressWarnings("rawtypes") final ProcessorNode prev = context.currentNode();
        context.setCurrentNode(myNode);
        try {
            context.forward(
                record
                    .withValue(
                        new Change<>(
                            getValueOrNull(record.value().newValue),
                            getValueOrNull(record.value().oldValue)))
                    .withTimestamp(
                        record.value().newValue != null ? record.value().newValue.timestamp()
                            : record.timestamp())
            );
        } finally {
            context.setCurrentNode(prev);
        }
    }
}
