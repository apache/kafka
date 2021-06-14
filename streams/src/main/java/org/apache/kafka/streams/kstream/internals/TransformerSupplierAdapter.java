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

import java.util.Collections;
import java.util.Set;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.StoreBuilder;

public class TransformerSupplierAdapter<KIn, VIn, KOut, VOut> implements TransformerSupplier<KIn, VIn, Iterable<KeyValue<KOut, VOut>>> {

    private final TransformerSupplier<KIn, VIn, KeyValue<KOut, VOut>> transformerSupplier;

    public TransformerSupplierAdapter(final TransformerSupplier<KIn, VIn, KeyValue<KOut, VOut>> transformerSupplier) {
        this.transformerSupplier = transformerSupplier;
    }

    @Override
    public Transformer<KIn, VIn, Iterable<KeyValue<KOut, VOut>>> get() {
        return new Transformer<KIn, VIn, Iterable<KeyValue<KOut, VOut>>>() {

            private final Transformer<KIn, VIn, KeyValue<KOut, VOut>> transformer = transformerSupplier.get();

            @Override
            public void init(final ProcessorContext context) {
                transformer.init(context);
            }

            @Override
            public Iterable<KeyValue<KOut, VOut>> transform(final KIn key, final VIn value) {
                final KeyValue<KOut, VOut> pair = transformer.transform(key, value);
                if (pair != null) {
                    return Collections.singletonList(pair);
                }
                return Collections.emptyList();
            }

            @Override
            public void close() {
                transformer.close();
            }
        };
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        return transformerSupplier.stores();
    }
}
