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

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TypedProcessor;
import org.apache.kafka.streams.processor.TypedProcessorSupplier;

public class ProcessorAdapter<KIn, VIn, KOut, VOut> implements TypedProcessorSupplier<KIn, VIn, KOut, VOut> {
    private final ProcessorSupplier<KIn, VIn> processorSupplier;

    public ProcessorAdapter(final ProcessorSupplier<KIn, VIn> processorSupplier) {
        this.processorSupplier = processorSupplier;
    }

    @Override
    public TypedProcessor<KIn, VIn, KOut, VOut> get() {
        final Processor<? super KIn, ? super VIn> processor = processorSupplier.get();

        return new TypedProcessor<KIn, VIn, KOut, VOut>() {
            @Override
            public void init(final ProcessorContext<KOut, VOut> context) {
                processor.init(context);
            }

            @Override
            public void close() {
                processor.close();
            }

            @Override
            public void process(final KIn key, final VIn value) {
                processor.process(key, value);
            }
        };
    }
}
