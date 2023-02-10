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

import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.ForwardingDisabledProcessorContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Set;

public class KStreamTransformValues<KIn, VIn, VOut> implements ProcessorSupplier<KIn, VIn, KIn, VOut> {

    private final ValueTransformerWithKeySupplier<KIn, VIn, VOut> valueTransformerSupplier;

    KStreamTransformValues(final ValueTransformerWithKeySupplier<KIn, VIn, VOut> valueTransformerSupplier) {
        this.valueTransformerSupplier = valueTransformerSupplier;
    }

    @Override
    public Processor<KIn, VIn, KIn, VOut> get() {
        return new KStreamTransformValuesProcessor<>(valueTransformerSupplier.get());
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        return valueTransformerSupplier.stores();
    }

    public static class KStreamTransformValuesProcessor<KIn, VIn, VOut> extends ContextualProcessor<KIn, VIn, KIn, VOut> {

        private final ValueTransformerWithKey<KIn, VIn, VOut> valueTransformer;

        KStreamTransformValuesProcessor(final ValueTransformerWithKey<KIn, VIn, VOut> valueTransformer) {
            this.valueTransformer = valueTransformer;
        }

        @Override
        public void init(final ProcessorContext<KIn, VOut> context) {
            super.init(context);
            valueTransformer.init(new ForwardingDisabledProcessorContext((InternalProcessorContext<KIn, VOut>) context));
        }

        @Override
        public void process(final Record<KIn, VIn> record) {
            context().forward(record.withValue(valueTransformer.transform(record.key(), record.value())));
        }

        @Override
        public void close() {
            valueTransformer.close();
        }
    }
}
