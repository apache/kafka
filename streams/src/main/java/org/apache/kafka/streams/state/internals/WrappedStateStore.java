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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.TimestampedBytesStore;

/**
 * A storage engine wrapper for utilities like logging, caching, and metering.
 */
public abstract class WrappedStateStore<S extends StateStore, K, V> implements StateStore, CachedStateStore<K, V> {

    public static boolean isTimestamped(final StateStore stateStore) {
        if (stateStore instanceof TimestampedBytesStore) {
            return true;
        } else if (stateStore instanceof WrappedStateStore) {
            return isTimestamped(((WrappedStateStore) stateStore).wrapped());
        } else {
            return false;
        }
    }

    private final S wrapped;

    public WrappedStateStore(final S wrapped) {
        this.wrapped = wrapped;
    }

    @Deprecated
    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {
        wrapped.init(context, root);
    }

    @Override
    public void init(final StateStoreContext context, final StateStore root) {
        wrapped.init(context, root);
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean setFlushListener(final CacheFlushListener<K, V> listener,
                                    final boolean sendOldValues) {
        if (wrapped instanceof CachedStateStore) {
            return ((CachedStateStore<K, V>) wrapped).setFlushListener(listener, sendOldValues);
        }
        return false;
    }

    @Override
    public void flushCache() {
        if (wrapped instanceof CachedStateStore) {
            ((CachedStateStore) wrapped).flushCache();
        }
    }

    @Override
    public String name() {
        return wrapped.name();
    }

    @Override
    public boolean persistent() {
        return wrapped.persistent();
    }

    @Override
    public boolean isOpen() {
        return wrapped.isOpen();
    }

    void validateStoreOpen() {
        if (!wrapped.isOpen()) {
            throw new InvalidStateStoreException("Store " + wrapped.name() + " is currently closed.");
        }
    }

    @Override
    public void flush() {
        wrapped.flush();
    }

    @Override
    public void close() {
        wrapped.close();
    }

    public S wrapped() {
        return wrapped;
    }
}
