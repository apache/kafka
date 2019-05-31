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

public class KStreamTransformValues<K, V, R> implements TypedProcessorSupplier<K, V, K, R> {

    private final ValueTransformerWithKeySupplier<? super K, ? super V, ? extends R> valueTransformerSupplier;

    KStreamTransformValues(final ValueTransformerWithKeySupplier<? super K, ? super V, ? extends R> valueTransformerSupplier) {
        this.valueTransformerSupplier = valueTransformerSupplier;
    }

    @Override
    public TypedProcessor<K, V, K, R> get() {
        return new KStreamTransformValuesProcessor<>(valueTransformerSupplier.get());
    }

    public static class KStreamTransformValuesProcessor<K, V, R> implements TypedProcessor<K, V, K, R> {

        private final ValueTransformerWithKey<? super K, ? super V, ? extends R> valueTransformer;
        private ProcessorContext<K, R> context;

        KStreamTransformValuesProcessor(final ValueTransformerWithKey<? super K, ? super V, ? extends R> valueTransformer) {
            this.valueTransformer = valueTransformer;
        }

        @Override
        public void init(final ProcessorContext<K, R> context) {
            valueTransformer.init(new ForwardingDisabledProcessorContext(context));
            this.context = context;
        }

        @Override
        public void process(final K key, final V value) {
            context.forward(key, valueTransformer.transform(key, value));
        }

        @Override
        public void close() {
            valueTransformer.close();
        }
    }
}
