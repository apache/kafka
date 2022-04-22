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

/**
 * An abstract implementation of {@link FixedKeyProcessor} that manages the
 * {@link FixedKeyProcessorContext} instance and provides default no-op
 * implementation of {@link #close()}.
 *
 * @param <KIn> the type of input keys
 * @param <VIn> the type of input values
 * @param <VOut> the type of output values
 */
public abstract class ContextualFixedKeyProcessor<KIn, VIn, VOut> implements FixedKeyProcessor<KIn, VIn, VOut> {

    private FixedKeyProcessorContext<KIn, VOut> context;

    protected ContextualFixedKeyProcessor() {}

    @Override
    public void init(final FixedKeyProcessorContext<KIn, VOut> context) {
        this.context = context;
    }

    /**
     * Get the processor's context set during {@link #init(FixedKeyProcessorContext) initialization}.
     *
     * @return the processor context; null only when called prior to {@link #init(FixedKeyProcessorContext) initialization}.
     */
    protected final FixedKeyProcessorContext<KIn, VOut> context() {
        return context;
    }
}
