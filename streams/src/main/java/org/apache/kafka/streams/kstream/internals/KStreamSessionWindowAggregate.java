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
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;

import java.util.ArrayList;
import java.util.List;

class KStreamSessionWindowAggregate<K, V, T> implements KStreamAggProcessorSupplier<K, Windowed<K>, V, T> {

    private final String storeName;
    private final SessionWindows windows;
    private final Initializer<T> initializer;
    private final Aggregator<? super K, ? super V, T> aggregator;
    private final Merger<? super K, T> sessionMerger;

    private boolean sendOldValues = false;

    KStreamSessionWindowAggregate(final SessionWindows windows,
                                  final String storeName,
                                  final Initializer<T> initializer,
                                  final Aggregator<? super K, ? super V, T> aggregator,
                                  final Merger<? super K, T> sessionMerger) {
        this.windows = windows;
        this.storeName = storeName;
        this.initializer = initializer;
        this.aggregator = aggregator;
        this.sessionMerger = sessionMerger;
    }

    @Override
    public Processor<K, V> get() {
        return new KStreamSessionWindowAggregateProcessor();
    }

    @Override
    public void enableSendingOldValues() {
        sendOldValues = true;
    }

    private class KStreamSessionWindowAggregateProcessor extends AbstractProcessor<K, V> {

        private SessionStore<K, T> store;
        private TupleForwarder<Windowed<K>, T> tupleForwarder;

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            super.init(context);
            store = (SessionStore<K,  T>) context.getStateStore(storeName);
            tupleForwarder = new TupleForwarder<>(store, context, new ForwardingCacheFlushListener<K, V>(context, sendOldValues), sendOldValues);
        }

        @Override
        public void process(final K key, final V value) {
            // if the key is null, we do not need proceed aggregating
            // the record with the table
            if (key == null) {
                return;
            }

            final long timestamp = context().timestamp();
            final List<KeyValue<Windowed<K>, T>> merged = new ArrayList<>();
            final SessionWindow newSessionWindow = new SessionWindow(timestamp, timestamp);
            SessionWindow mergedWindow = newSessionWindow;
            T agg = initializer.apply();

            try (final KeyValueIterator<Windowed<K>, T> iterator = store.findSessions(key, timestamp - windows.inactivityGap(),
                                                                                      timestamp + windows.inactivityGap())) {
                while (iterator.hasNext()) {
                    final KeyValue<Windowed<K>, T> next = iterator.next();
                    merged.add(next);
                    agg = sessionMerger.apply(key, agg, next.value);
                    mergedWindow = mergeSessionWindow(mergedWindow, (SessionWindow) next.key.window());
                }
            }

            agg = aggregator.apply(key, value, agg);
            final Windowed<K> sessionKey = new Windowed<>(key, mergedWindow);
            if (!mergedWindow.equals(newSessionWindow)) {
                for (final KeyValue<Windowed<K>, T> session : merged) {
                    store.remove(session.key);
                    tupleForwarder.maybeForward(session.key, null, session.value);
                }
            }
            store.put(sessionKey, agg);
            tupleForwarder.maybeForward(sessionKey, agg, null);
        }

    }


    private SessionWindow mergeSessionWindow(final SessionWindow one, final SessionWindow two) {
        final long start = one.start() < two.start() ? one.start() : two.start();
        final long end = one.end() > two.end() ? one.end() : two.end();
        return new SessionWindow(start, end);
    }

    @Override
    public KTableValueGetterSupplier<Windowed<K>, T> view() {
        return new KTableValueGetterSupplier<Windowed<K>, T>() {
            @Override
            public KTableValueGetter<Windowed<K>, T> get() {
                return new KTableSessionWindowValueGetter();
            }

            @Override
            public String[] storeNames() {
                return new String[] {storeName};
            }
        };
    }

    private class KTableSessionWindowValueGetter implements KTableValueGetter<Windowed<K>, T> {
        private SessionStore<K, T> store;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            store = (SessionStore<K, T>) context.getStateStore(storeName);
        }

        @Override
        public T get(final Windowed<K> key) {
            try (KeyValueIterator<Windowed<K>, T> iter = store.findSessions(key.key(), key.window().end(), key.window().end())) {
                if (!iter.hasNext()) {
                    return null;
                }
                final T value = iter.next().value;
                if (iter.hasNext()) {
                    throw new ProcessorStateException(String.format("Iterator for key [%s] on session store has more than one value", key));
                }
                return value;
            }
        }
    }

}
