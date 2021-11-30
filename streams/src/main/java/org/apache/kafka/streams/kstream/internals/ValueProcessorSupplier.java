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

import java.util.Set;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.ValueProcessorContext;
import org.apache.kafka.streams.state.StoreBuilder;

public class ValueProcessorSupplier<KIn, VIn, VOut> implements ProcessorSupplier<KIn, VIn, KIn, VOut> {

    private final ProcessorSupplier<KIn, VIn, KIn, VOut> processorSupplier;

    public ValueProcessorSupplier(ProcessorSupplier<KIn, VIn, KIn, VOut> processorSupplier) {
        this.processorSupplier = processorSupplier;
    }

    @Override
    public Processor<KIn, VIn, KIn, VOut> get() {
        return new ValueProcessor<>(processorSupplier.get());
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        return processorSupplier.stores();
    }

    public static class ValueProcessor<KIn, VIn, VOut> extends ContextualProcessor<KIn, VIn, KIn, VOut> {

        private final Processor<KIn, VIn, KIn, VOut> processor;

        private ValueProcessorContext<KIn, VOut> processorContext;

        public ValueProcessor(Processor<KIn, VIn, KIn, VOut> processor) {
            this.processor = processor;
        }

        @Override
        public void init(final ProcessorContext<KIn, VOut> context) {
            super.init(context);
            this.processorContext = new ValueProcessorContext<>(context);
            processor.init(processorContext);
        }

        @Override
        public void process(final Record<KIn, VIn> record) {
            processorContext.setInitialKey(record.key());
            processor.process(record);
            processorContext.unsetInitialKey();
        }

        @Override
        public void close() {
            processor.close();
        }
    }
}
