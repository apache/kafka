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
import org.apache.kafka.streams.processor.TypedProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TypedProcessorSupplier;
import org.apache.kafka.streams.processor.internals.ForwardingDisabledProcessorContext;

public class KStreamFlatTransformValues<K, VIn, VOut> implements TypedProcessorSupplier<K, VIn, K, VOut> {

    private final ValueTransformerWithKeySupplier<? super K, ? super VIn, Iterable<VOut>> valueTransformerSupplier;

    public KStreamFlatTransformValues(final ValueTransformerWithKeySupplier<? super K, ? super VIn, Iterable<VOut>> valueTransformerWithKeySupplier) {
        this.valueTransformerSupplier = valueTransformerWithKeySupplier;
    }

    @Override
    public TypedProcessor<K, VIn, K, VOut> get() {
        return new KStreamFlatTransformValuesProcessor<>(valueTransformerSupplier.get());
    }

    public static class KStreamFlatTransformValuesProcessor<K, VIn, VOut> implements TypedProcessor<K, VIn, K, VOut> {

        private final ValueTransformerWithKey<? super K, ? super VIn, Iterable<VOut>> valueTransformer;
        private ProcessorContext<K, VOut> context;

        KStreamFlatTransformValuesProcessor(final ValueTransformerWithKey<? super K, ? super VIn, Iterable<VOut>> valueTransformer) {
            this.valueTransformer = valueTransformer;
        }

        @Override
        public void init(final ProcessorContext<K, VOut> context) {
            valueTransformer.init(new ForwardingDisabledProcessorContext(context));
            this.context = context;
        }

        @Override
        public void process(final K key, final VIn value) {
            final Iterable<VOut> transformedValues = valueTransformer.transform(key, value);
            if (transformedValues != null) {
                for (final VOut transformedValue : transformedValues) {
                    context.forward(key, transformedValue);
                }
            }
        }

        @Override
        public void close() {
            valueTransformer.close();
        }
    }

}
