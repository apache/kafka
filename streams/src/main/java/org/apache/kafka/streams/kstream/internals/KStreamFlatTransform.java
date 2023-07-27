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

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Set;

public class KStreamFlatTransform<KIn, VIn, KOut, VOut> implements ProcessorSupplier<KIn, VIn, KOut, VOut> {

    private final TransformerSupplier<? super KIn, ? super VIn, Iterable<KeyValue<KOut, VOut>>> transformerSupplier;

    public KStreamFlatTransform(final TransformerSupplier<? super KIn, ? super VIn, Iterable<KeyValue<KOut, VOut>>> transformerSupplier) {
        this.transformerSupplier = transformerSupplier;
    }

    @Override
    public Processor<KIn, VIn, KOut, VOut> get() {
        return new KStreamFlatTransformProcessor<>(transformerSupplier.get());
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        return transformerSupplier.stores();
    }

    public static class KStreamFlatTransformProcessor<KIn, VIn, KOut, VOut> extends ContextualProcessor<KIn, VIn, KOut, VOut> {

        private final Transformer<? super KIn, ? super VIn, Iterable<KeyValue<KOut, VOut>>> transformer;

        public KStreamFlatTransformProcessor(final Transformer<? super KIn, ? super VIn, Iterable<KeyValue<KOut, VOut>>> transformer) {
            this.transformer = transformer;
        }

        @Override
        public void init(final ProcessorContext<KOut, VOut> context) {
            super.init(context);
            transformer.init((InternalProcessorContext<KOut, VOut>) context);
        }

        @Override
        public void process(final Record<KIn, VIn> record) {
            final Iterable<KeyValue<KOut, VOut>> pairs = transformer.transform(record.key(), record.value());
            if (pairs != null) {
                for (final KeyValue<KOut, VOut> pair : pairs) {
                    context().forward(record.withKey(pair.key).withValue(pair.value));
                }
            }
        }

        @Override
        public void close() {
            transformer.close();
        }
    }
}
