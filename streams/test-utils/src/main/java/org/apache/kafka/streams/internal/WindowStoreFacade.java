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
package org.apache.kafka.streams.internal;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

public class WindowStoreFacade<K, V> extends StoreFacade implements WindowStore<K, V> {
    private final WindowStore<K, V> inner;
    private final InternalProcessorContext context;

    public WindowStoreFacade(final WindowStore<K, V> inner,
                             final InternalProcessorContext context) {
        super(inner);
        this.inner = inner;
        this.context = context;
    }

    @Override
    public void put(final K key,
                    final V value) {
        setContext();
        inner.put(key, value);
        context.setRecordContext(null);
    }

    @Override
    public void put(final K key,
                    final V value,
                    final long timestamp) {
        setContext();
        inner.put(key, value, timestamp);
        context.setRecordContext(null);
    }

    private void setContext() {
        context.setRecordContext(new ProcessorRecordContext(0L, -1L, -1, null, new RecordHeaders()));
    }

    @Override
    public V fetch(final K key,
                   final long time) {
        return inner.fetch(key, time);
    }

    @Override
    public WindowStoreIterator<V> fetch(final K key,
                                        final long timeFrom,
                                        final long timeTo) {
        return inner.fetch(key, timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(final K from,
                                                  final K to,
                                                  final long timeFrom,
                                                  final long timeTo) {
        return inner.fetch(from, to, timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> all() {
        return inner.all();
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetchAll(final long timeFrom,
                                                     final long timeTo) {
        return inner.fetchAll(timeFrom, timeTo);
    }
}
