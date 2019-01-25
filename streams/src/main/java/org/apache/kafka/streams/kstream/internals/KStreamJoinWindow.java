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

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.internals.WrappedStateStore;

class KStreamJoinWindow<K, V> implements ProcessorSupplier<K, V> {

    private final String windowName;

    KStreamJoinWindow(final String windowName) {
        this.windowName = windowName;
    }

    @Override
    public Processor<K, V> get() {
        return new KStreamJoinWindowProcessor();
    }

    private class KStreamJoinWindowProcessor extends AbstractProcessor<K, V> {

        private WindowStore<K, ValueAndTimestamp<V>> window;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            super.init(context);

            StateStore store = context.getStateStore(windowName);
            if (store instanceof WrappedStateStore) {
                store = ((WrappedStateStore) store).wrappedStore();
            }
            window = ((KStreamImpl.WindowStoreFacade) store).inner;
        }

        @Override
        public void process(final K key, final V value) {
            // if the key is null, we do not need to put the record into window store
            // since it will never be considered for join operations
            if (key != null) {
                context().forward(key, value);
                // Every record basically starts a new window. We're using a window store mostly for the retention.
                window.put(key, ValueAndTimestamp.make(value, context().timestamp()), context().timestamp());
            }
        }
    }

}
