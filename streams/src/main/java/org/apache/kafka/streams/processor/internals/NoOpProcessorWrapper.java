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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.ProcessorWrapper;
import org.apache.kafka.streams.processor.api.WrappedFixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.WrappedProcessorSupplier;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Set;

public class NoOpProcessorWrapper implements ProcessorWrapper {

    @Override
    public <KIn, VIn, KOut, VOut> WrappedProcessorSupplier<KIn, VIn, KOut, VOut> wrapProcessorSupplier(final String processorName,
                                                                                                       final ProcessorSupplier<KIn, VIn, KOut, VOut> processorSupplier) {
        return ProcessorWrapper.asWrapped(processorSupplier);
    }

    @Override
    public <KIn, VIn, VOut> WrappedFixedKeyProcessorSupplier<KIn, VIn, VOut> wrapFixedKeyProcessorSupplier(final String processorName,
                                                                                                              final FixedKeyProcessorSupplier<KIn, VIn, VOut> processorSupplier) {
        return ProcessorWrapper.asWrappedFixedKey(processorSupplier);
    }

    public static class WrappedProcessorSupplierImpl<KIn, VIn, KOut, VOut> implements  WrappedProcessorSupplier<KIn, VIn, KOut, VOut> {

        private final ProcessorSupplier<KIn, VIn, KOut, VOut> delegate;

        public WrappedProcessorSupplierImpl(final ProcessorSupplier<KIn, VIn, KOut, VOut> delegate) {
            this.delegate = delegate;
        }

        @Override
        public Set<StoreBuilder<?>> stores() {
            return delegate.stores();
        }

        @Override
        public Processor<KIn, VIn, KOut, VOut> get() {
            return delegate.get();
        }
    }

    public static class WrappedFixedKeyProcessorSupplierImpl<KIn, VIn, VOut> implements WrappedFixedKeyProcessorSupplier<KIn, VIn, VOut> {

        private final FixedKeyProcessorSupplier<KIn, VIn, VOut> delegate;

        public WrappedFixedKeyProcessorSupplierImpl(final FixedKeyProcessorSupplier<KIn, VIn, VOut> delegate) {
            this.delegate = delegate;
        }

        @Override
        public Set<StoreBuilder<?>> stores() {
            return delegate.stores();
        }

        @Override
        public FixedKeyProcessor<KIn, VIn, VOut> get() {
            return delegate.get();
        }
    }
}