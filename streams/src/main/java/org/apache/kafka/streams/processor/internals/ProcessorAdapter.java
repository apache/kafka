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


import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

@SuppressWarnings("deprecation") // Old PAPI compatibility
public final class ProcessorAdapter<KIn, VIn, KOut, VOut> implements Processor<KIn, VIn, KOut, VOut> {
    private final org.apache.kafka.streams.processor.Processor<KIn, VIn> delegate;
    private InternalProcessorContext context;

    public static <KIn, VIn, KOut, VOut> Processor<KIn, VIn, KOut, VOut> adapt(final org.apache.kafka.streams.processor.Processor<KIn, VIn> delegate) {
        if (delegate == null) {
            return null;
        } else {
            return new ProcessorAdapter<>(delegate);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static <KIn, VIn, KOut, VOut> Processor<KIn, VIn, KOut, VOut> adaptRaw(final org.apache.kafka.streams.processor.Processor delegate) {
        if (delegate == null) {
            return null;
        } else {
            return new ProcessorAdapter<>(delegate);
        }
    }

    private ProcessorAdapter(final org.apache.kafka.streams.processor.Processor<KIn, VIn> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void init(final ProcessorContext<KOut, VOut> context) {
        // It only makes sense to use this adapter internally to Streams, in which case
        // all contexts are implementations of InternalProcessorContext.
        // This would fail if someone were to use this adapter in a unit test where
        // the context only implements api.ProcessorContext.
        this.context = (InternalProcessorContext) context;
        delegate.init((org.apache.kafka.streams.processor.ProcessorContext) context);
    }

    @Override
    public void process(final Record<KIn, VIn> record) {
        final ProcessorRecordContext processorRecordContext = context.recordContext();
        try {
            context.setRecordContext(new ProcessorRecordContext(
                record.timestamp(),
                context.offset(),
                context.partition(),
                context.topic(),
                record.headers()
            ));
            delegate.process(record.key(), record.value());
        } finally {
            context.setRecordContext(processorRecordContext);
        }
    }

    @Override
    public void close() {
        delegate.close();
    }
}
