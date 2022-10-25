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
package org.apache.kafka.streams.processor.api;

import org.apache.kafka.streams.processor.ConnectedStoreProvider;

import java.util.function.Supplier;

/**
 * A processor supplier that can create one or more {@link FixedKeyProcessor} instances.
 * <p>
 * The supplier should always generate a new instance each time {@link FixedKeyProcessorSupplier#get()} gets called. Creating
 * a single {@link FixedKeyProcessor} object and returning the same object reference in {@link FixedKeyProcessorSupplier#get()} would be
 * a violation of the supplier pattern and leads to runtime exceptions.
 *
 * @param <KIn> the type of input keys
 * @param <VIn> the type of input values
 * @param <VOut> the type of output values
 */
@FunctionalInterface
public interface FixedKeyProcessorSupplier<KIn, VIn, VOut>
    extends ConnectedStoreProvider, Supplier<FixedKeyProcessor<KIn, VIn, VOut>> {

    /**
     * Return a newly constructed {@link FixedKeyProcessor} instance.
     * The supplier should always generate a new instance each time {@code FixedKeyProcessorSupplier#get()} gets called.
     * <p>
     * Creating a single {@link FixedKeyProcessor} object and returning the same object reference in {@code FixedKeyProcessorSupplier#get()}
     * is a violation of the supplier pattern and leads to runtime exceptions.
     *
     * @return a new {@link FixedKeyProcessor} instance
     */
    FixedKeyProcessor<KIn, VIn, VOut> get();
}
