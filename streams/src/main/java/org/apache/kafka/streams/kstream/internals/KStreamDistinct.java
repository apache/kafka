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
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.util.Collections;
import java.util.Set;

class KStreamDistinct<K, V, I> implements ProcessorSupplier<K, V> {

    private final StoreBuilder<WindowStore<I, Long>> storeBuilder;
    private final DistinctParametersInternal<K, V, I> params;

    public KStreamDistinct(final StoreBuilder<WindowStore<I, Long>> storeBuilder, final DistinctParametersInternal<K, V, I> params) {
        this.params = params;
        this.storeBuilder = storeBuilder;
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        return Collections.singleton(storeBuilder);
    }

    @Override
    public Processor<K, V> get() {
        return new KStreamDistinctProcessor();
    }

    private class KStreamDistinctProcessor extends AbstractProcessor<K, V> {

        private WindowStore<I, Long> eventIdStore;

        private long leftDurationMs = 1000;
        private long rightDurationMs = 1000;

        @Override
        public void process(final K key, final V value) {
            final I eventId = params.getIdExtractor().apply(key, value);
            if (eventId == null) {
                context().forward(key, value);
            } else {
                final long timestamp = context().timestamp();
                final boolean forward = !isDuplicate(eventId);
                eventIdStore.put(eventId, timestamp, timestamp);
                if (forward) {
                    context().forward(key, value);
                }
            }
        }

        private boolean isDuplicate(final I eventId) {
            final long eventTime = context().timestamp();
            final WindowStoreIterator<Long> timeIterator = eventIdStore.fetch(
                    eventId,
                    eventTime - leftDurationMs,
                    eventTime + rightDurationMs);
            final boolean isDuplicate = timeIterator.hasNext();
            timeIterator.close();
            return isDuplicate;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void init(final ProcessorContext context) {
            super.init(context);
            eventIdStore = (WindowStore<I, Long>)
                    context().getStateStore(storeBuilder.name());
        }
    }
}
