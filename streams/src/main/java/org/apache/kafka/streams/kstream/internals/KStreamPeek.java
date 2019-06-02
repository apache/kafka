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

import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.processor.TypedProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TypedProcessorSupplier;

class KStreamPeek<K, V> implements TypedProcessorSupplier<K, V, K, V> {

    private final boolean forwardDownStream;
    private final ForeachAction<? super K, ? super V> action;

    public KStreamPeek(final ForeachAction<? super K, ? super V> action, final boolean forwardDownStream) {
        this.action = action;
        this.forwardDownStream = forwardDownStream;
    }

    @Override
    public TypedProcessor<K, V, K, V> get() {
        return new KStreamPeekProcessor();
    }

    private class KStreamPeekProcessor implements TypedProcessor<K, V, K, V> {
        private ProcessorContext<K, V> context;

        @Override
        public void init(final ProcessorContext<K, V> context) {
            this.context = context;
        }

        @Override
        public void process(final K key, final V value) {
            action.apply(key, value);
            if (forwardDownStream) {
                context.forward(key, value);
            }
        }
    }

}
