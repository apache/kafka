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

import java.util.Objects;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

public class KStreamFlatTransform<K, V, K1, V1> implements ProcessorSupplier<K, V> {

    private final TransformerSupplier<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends K1, ? extends V1>>> transformerSupplier;

    public KStreamFlatTransform(TransformerSupplier<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends K1, ? extends V1>>> transformerSupplier) {
        this.transformerSupplier = transformerSupplier;
    }

    @Override
    public Processor<K, V> get() {
        return new KStreamFlatTransformProcessor<>(transformerSupplier.get());
    }

    public static class KStreamFlatTransformProcessor<K, V, K1, V1> extends AbstractProcessor<K, V> {

        private final Transformer<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends K1, ? extends V1>>> transformer;

        public KStreamFlatTransformProcessor(Transformer<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends K1, ? extends V1>>> transformer) {
            this.transformer = transformer;
        }

        @Override
        public void init(ProcessorContext context) {
            super.init(context);
            transformer.init(context);
        }

        @Override
        public void process(K key, V value) {
            Iterable<? extends KeyValue<? extends K1, ? extends V1>> pairs = transformer.transform(key, value);
            Objects.requireNonNull(pairs, "result of transform can't be null");

            for (KeyValue<? extends K1, ? extends V1> pair : pairs) {
                context().forward(pair.key, pair.value);
            }
        }

        @Override
        public void close() {
            transformer.close();
        }
    }

}
