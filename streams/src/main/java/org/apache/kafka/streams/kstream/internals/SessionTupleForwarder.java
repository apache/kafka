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

import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.internals.CacheFlushListener;
import org.apache.kafka.streams.state.internals.WrappedStateStore;

/**
 * This class is used to determine if a processor should forward values to child nodes.
 * Forwarding by this class only occurs when caching is not enabled. If caching is enabled,
 * forwarding occurs in the flush listener when the cached store flushes.
 *
 * @param <K>
 * @param <V>
 */
class SessionTupleForwarder<K, V> {
    private final ProcessorContext context;
    private final boolean sendOldValues;
    private final boolean cachingEnabled;

    @SuppressWarnings("unchecked")
    SessionTupleForwarder(final StateStore store,
                          final ProcessorContext context,
                          final CacheFlushListener<Windowed<K>, V> flushListener,
                          final boolean sendOldValues) {
        this.context = context;
        this.sendOldValues = sendOldValues;
        cachingEnabled = ((WrappedStateStore) store).setFlushListener(flushListener, sendOldValues);
    }

    public void maybeForward(final Windowed<K> key,
                             final V newValue,
                             final V oldValue) {
        if (!cachingEnabled) {
            context.forward(key, new Change<>(newValue, sendOldValues ? oldValue : null), To.all().withTimestamp(key.window().end()));
        }
    }
}
