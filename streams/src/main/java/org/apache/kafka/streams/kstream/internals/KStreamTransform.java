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
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

public class KStreamTransform<Ki, Vi, Ko, Vo> implements ProcessorSupplier<Ki, Vi> {

    private final TransformerSupplier<? super Ki, ? super Vi, ? extends KeyValue<? extends Ko, ? extends Vo>> transformerSupplier;

    public KStreamTransform(final TransformerSupplier<? super Ki, ? super Vi, ? extends KeyValue<? extends Ko, ? extends Vo>> transformerSupplier) {
        this.transformerSupplier = transformerSupplier;
    }

    @Override
    public Processor<Ki, Vi> get() {
        return new KStreamTransformProcessor<>(transformerSupplier.get());
    }

    public static class KStreamTransformProcessor<Ki, Vi, Ko, Vo> extends AbstractProcessor<Ki, Vi> {

        private final Transformer<? super Ki, ? super Vi, ? extends KeyValue<? extends Ko, ? extends Vo>> transformer;

        public KStreamTransformProcessor(final Transformer<? super Ki, ? super Vi, ? extends KeyValue<? extends Ko, ? extends Vo>> transformer) {
            this.transformer = transformer;
        }

        @Override
        public void init(final ProcessorContext context) {
            super.init(context);
            transformer.init(context);
        }

        @Override
        public void process(final Ki key, final Vi value) {
            KeyValue<? extends Ko, ? extends Vo> pair = transformer.transform(key, value);

            if (pair != null)
                context().forward(pair.key, pair.value);
        }

        @Override
        public void close() {
            transformer.close();
        }
    }
}
