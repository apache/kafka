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

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.internals.CachedStateStore;
import org.apache.kafka.streams.state.internals.WrappedStateStore;

/**
 * This class is used to determine if a processor should forward values to child nodes.
 * Forwarding by this class only occurs when caching is not enabled. If caching is enabled,
 * forwarding occurs in the flush listener when the cached store flushes.
 *
 * @param <K>
 * @param <V>
 */
class TupleForwarder<K, V> {
    private final CachedStateStore cachedStateStore;
    private final ProcessorContext context;

    @SuppressWarnings("unchecked")
    TupleForwarder(final StateStore store,
                   final ProcessorContext context,
                   final ForwardingCacheFlushListener flushListener,
                   final boolean sendOldValues) {
        this.cachedStateStore = cachedStateStore(maybeRemoveStoreFacade(store));
        this.context = context;
        if (this.cachedStateStore != null) {
            cachedStateStore.setFlushListener(flushListener, sendOldValues);
        }
    }

    private StateStore maybeRemoveStoreFacade(final StateStore store) {
        if (store instanceof KeyValueWithTimestampStoreMaterializer.TimestampHidingKeyValueStoreFacade) {
            return ((KeyValueWithTimestampStoreMaterializer.TimestampHidingKeyValueStoreFacade) store).inner;
        }
        if (store instanceof KStreamImpl.WindowStoreFacade) {
            return ((KStreamImpl.WindowStoreFacade) store).inner;
        }
        if (store instanceof SessionWindowedKStreamImpl.SessionStoreFacade) {
            return ((SessionWindowedKStreamImpl.SessionStoreFacade) store).inner;
        }
        return store;
    }

    private CachedStateStore cachedStateStore(final StateStore store) {
        if (store instanceof CachedStateStore) {
            return (CachedStateStore) store;
        } else if (store instanceof WrappedStateStore) {
            StateStore wrapped = ((WrappedStateStore) store).wrappedStore();

            while (wrapped instanceof WrappedStateStore && !(wrapped instanceof CachedStateStore)) {
                wrapped = ((WrappedStateStore) wrapped).wrappedStore();
            }

            if (!(wrapped instanceof CachedStateStore)) {
                return null;
            }

            return (CachedStateStore) wrapped;
        }
        return null;
    }

    public void maybeForward(final K key,
                             final V newValue,
                             final V oldValue) {
        if (cachedStateStore == null) {
            context.forward(key, new Change<>(newValue, oldValue));
        }
    }

    public void maybeForward(final K key,
                             final V newValue,
                             final V oldValue,
                             final long timestamp) {
        if (cachedStateStore == null) {
            context.forward(key, new Change<>(newValue, oldValue), To.all().withTimestamp(timestamp));
        }
    }

}
