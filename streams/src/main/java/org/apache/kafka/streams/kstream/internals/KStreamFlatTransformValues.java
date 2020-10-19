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
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.internals.ForwardingDisabledProcessorContext;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Set;

public class KStreamFlatTransformValues<KIn, VIn, VOut> implements ProcessorSupplier<KIn, VIn> {

    private final ValueTransformerWithKeySupplier<KIn, VIn, Iterable<VOut>> valueTransformerSupplier;

    public KStreamFlatTransformValues(final ValueTransformerWithKeySupplier<KIn, VIn, Iterable<VOut>> valueTransformerWithKeySupplier) {
        this.valueTransformerSupplier = valueTransformerWithKeySupplier;
    }

    @Override
    public Processor<KIn, VIn> get() {
        return new KStreamFlatTransformValuesProcessor<>(valueTransformerSupplier.get());
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        return valueTransformerSupplier.stores();
    }

    public static class KStreamFlatTransformValuesProcessor<KIn, VIn, VOut> extends AbstractProcessor<KIn, VIn> {

        private final ValueTransformerWithKey<KIn, VIn, Iterable<VOut>> valueTransformer;

        KStreamFlatTransformValuesProcessor(final ValueTransformerWithKey<KIn, VIn, Iterable<VOut>> valueTransformer) {
            this.valueTransformer = valueTransformer;
        }

        @Override
        public void init(final ProcessorContext context) {
            super.init(context);
            valueTransformer.init(new ForwardingDisabledProcessorContext(context));
        }

        @Override
        public void process(final KIn key, final VIn value) {
            final Iterable<VOut> transformedValues = valueTransformer.transform(key, value);
            if (transformedValues != null) {
                for (final VOut transformedValue : transformedValues) {
                    context.forward(key, transformedValue);
                }
            }
        }

        @Override
        public void close() {
            super.close();
            valueTransformer.close();
        }
    }

}
