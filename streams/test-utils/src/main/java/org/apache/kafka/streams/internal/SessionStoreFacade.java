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
import org.apache.kafka.streams.state.SessionStore;

public class SessionStoreFacade<K, AGG> extends StoreFacade implements SessionStore<K, AGG> {
    private final SessionStore<K, AGG> inner;
    private final InternalProcessorContext context;

    public SessionStoreFacade(final SessionStore<K, AGG> inner,
                              final InternalProcessorContext context) {
        super(inner);
        this.inner = inner;
        this.context = context;
    }

    @Override
    public KeyValueIterator<Windowed<K>, AGG> findSessions(final K key,
                                                           final long earliestSessionEndTime,
                                                           final long latestSessionStartTime) {
        return inner.findSessions(key, earliestSessionEndTime, latestSessionStartTime);
    }

    @Override
    public KeyValueIterator<Windowed<K>, AGG> findSessions(final K keyFrom,
                                                           final K keyTo,
                                                           final long earliestSessionEndTime,
                                                           final long latestSessionStartTime) {
        return inner.findSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime);
    }

    @Override
    public void remove(final Windowed<K> sessionKey) {
        setContext();
        inner.remove(sessionKey);
        context.setRecordContext(null);

    }

    @Override
    public void put(final Windowed<K> sessionKey,
                    final AGG aggregate) {
        setContext();
        inner.put(sessionKey, aggregate);
        context.setRecordContext(null);
    }

    private void setContext() {
        context.setRecordContext(new ProcessorRecordContext(0L, -1L, -1, null, new RecordHeaders()));
    }

    @Override
    public KeyValueIterator<Windowed<K>, AGG> fetch(final K key) {
        return inner.fetch(key);
    }

    @Override
    public KeyValueIterator<Windowed<K>, AGG> fetch(final K from,
                                                    final K to) {
        return inner.fetch(from, to);
    }

}
