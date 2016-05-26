/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

public class KStreamTransformValues<K, V, R> implements ProcessorSupplier<K, V> {

    private final ValueTransformerSupplier<V, R> valueTransformerSupplier;

    public KStreamTransformValues(ValueTransformerSupplier<V, R> valueTransformerSupplier) {
        this.valueTransformerSupplier = valueTransformerSupplier;
    }

    @Override
    public Processor<K, V> get() {
        return new KStreamTransformValuesProcessor<>(valueTransformerSupplier.get());
    }

    public static class KStreamTransformValuesProcessor<K, V, R> implements Processor<K, V> {

        private final ValueTransformer<V, R> valueTransformer;
        private ProcessorContext context;

        public KStreamTransformValuesProcessor(ValueTransformer<V, R> valueTransformer) {
            this.valueTransformer = valueTransformer;
        }

        @Override
        public void init(ProcessorContext context) {
            valueTransformer.init(context);
            this.context = context;
        }

        @Override
        public void process(K key, V value) {
            context.forward(key, valueTransformer.transform(value));
        }

        @Override
        public void punctuate(long timestamp) {
            R ret = valueTransformer.punctuate(timestamp);

            if (ret != null)
                context.forward(null, ret);
        }

        @Override
        public void close() {
            valueTransformer.close();
        }
    }
}
