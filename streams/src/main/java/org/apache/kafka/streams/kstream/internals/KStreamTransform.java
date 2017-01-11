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

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

public class KStreamTransform<K, V, K1, V1> implements ProcessorSupplier<K, V> {

    private final TransformerSupplier<? super K, ? super V, ? extends KeyValue<? extends K1, ? extends V1>> transformerSupplier;

    public KStreamTransform(TransformerSupplier<? super K, ? super V, ? extends KeyValue<? extends K1, ? extends V1>> transformerSupplier) {
        this.transformerSupplier = transformerSupplier;
    }

    @Override
    public Processor<K, V> get() {
        return new KStreamTransformProcessor<>(transformerSupplier.get());
    }

    public static class KStreamTransformProcessor<K1, V1, K2, V2> extends AbstractProcessor<K1, V1> {

        private final Transformer<? super K1, ? super V1, ? extends KeyValue<? extends K2, ? extends V2>> transformer;

        public KStreamTransformProcessor(Transformer<? super K1, ? super V1, ? extends KeyValue<? extends K2, ? extends V2>> transformer) {
            this.transformer = transformer;
        }

        @Override
        public void init(ProcessorContext context) {
            super.init(context);
            transformer.init(context);
        }

        @Override
        public void process(K1 key, V1 value) {
            KeyValue<? extends K2, ? extends V2> pair = transformer.transform(key, value);

            if (pair != null)
                context().forward(pair.key, pair.value);
        }

        @Override
        public void punctuate(long timestamp) {
            KeyValue<? extends K2, ? extends V2> pair = transformer.punctuate(timestamp);

            if (pair != null)
                context().forward(pair.key, pair.value);
        }

        @Override
        public void close() {
            transformer.close();
        }
    }
}
