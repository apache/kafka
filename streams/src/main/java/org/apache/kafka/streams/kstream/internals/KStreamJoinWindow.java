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

import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.StoreFactory;
import org.apache.kafka.streams.processor.internals.StoreFactory.FactoryWrappingStoreBuilder;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.WindowStore;

import java.util.Collections;
import java.util.Set;

class KStreamJoinWindow<K, V> implements ProcessorSupplier<K, V, K, V> {

    private final StoreFactory thisWindowStoreFactory;

    KStreamJoinWindow(final StoreFactory thisWindowStoreFactory) {
        this.thisWindowStoreFactory = thisWindowStoreFactory;
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        return Collections.singleton(new FactoryWrappingStoreBuilder<>(thisWindowStoreFactory));
    }

    @Override
    public Processor<K, V, K, V> get() {
        return new KStreamJoinWindowProcessor();
    }

    private class KStreamJoinWindowProcessor extends ContextualProcessor<K, V, K, V> {

        private WindowStore<K, V> window;

        @Override
        public void init(final ProcessorContext<K, V> context) {
            super.init(context);

            window = context.getStateStore(thisWindowStoreFactory.storeName());
        }

        @Override
        public void process(final Record<K, V> record) {
            // if the key is null, we do not need to put the record into window store
            // since it will never be considered for join operations
            context().forward(record);
            if (record.key() != null) {
                // Every record basically starts a new window. We're using a window store mostly for the retention.
                window.put(record.key(), record.value(), record.timestamp());
            }
        }
    }
}
