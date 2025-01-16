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
package org.apache.kafka.streams.kstream;


import org.apache.kafka.streams.processor.ConnectedStoreProvider;

import java.util.function.Supplier;

/**
 * A {@code TransformerSupplier} interface which can create one or more {@link Transformer} instances.
 * <p>
 * The supplier should always generate a new instance each time {@link TransformerSupplier#get()} gets called. Creating
 * a single {@link Transformer} object and returning the same object reference in {@link TransformerSupplier#get()} would be
 * a violation of the supplier pattern and leads to runtime exceptions.
 *
 * @param <K> key type
 * @param <V> value type
 * @param <R> {@link org.apache.kafka.streams.KeyValue KeyValue} return type (both key and value type can be set
 *            arbitrarily)
 * @see Transformer
 * @see ValueTransformer
 * @see ValueTransformerSupplier
 * @deprecated Since 4.0. Use {@link org.apache.kafka.streams.processor.api.ProcessorSupplier api.ProcessorSupplier} instead.
 */
@Deprecated
public interface TransformerSupplier<K, V, R> extends ConnectedStoreProvider, Supplier<Transformer<K, V, R>> {

    /**
     * Return a newly constructed {@link Transformer} instance.
     * The supplier should always generate a new instance each time {@link TransformerSupplier#get() gets called}.
     * <p>
     * Creating a single {@link Transformer} object and returning the same object reference in {@link TransformerSupplier#get()}
     * is a violation of the supplier pattern and leads to runtime exceptions.
     *
     * @return a newly constructed {@link Transformer} instance
     */
    Transformer<K, V, R> get();
}
