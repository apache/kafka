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
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.TimestampedBytesStore;
import org.apache.kafka.streams.state.VersionedBytesStore;

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

    public static boolean isVersioned(final StateStore stateStore) {
        if (stateStore instanceof VersionedBytesStore) {
            return true;
        } else if (stateStore instanceof WrappedStateStore) {
            return isVersioned(((WrappedStateStore) stateStore).wrapped());
        } else {
            return false;
        }
    }

    private final S wrapped;

    public WrappedStateStore(final S wrapped) {
        this.wrapped = wrapped;
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
    public void clearCache() {
        if (wrapped instanceof CachedStateStore) {
            ((CachedStateStore) wrapped).clearCache();
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

    @Override
    public <R> QueryResult<R> query(final Query<R> query,
        final PositionBound positionBound,
        final QueryConfig config) {

        final long start = config.isCollectExecutionInfo() ? System.nanoTime() : -1L;
        final QueryResult<R> result = wrapped().query(query, positionBound, config);
        if (config.isCollectExecutionInfo()) {
            final long end = System.nanoTime();
            result.addExecutionInfo(
                "Handled in " + getClass() + " via WrappedStateStore" + " in " + (end - start)
                    + "ns");
        }
        return result;
    }

    @Override
    public Position getPosition() {
        return wrapped.getPosition();
    }

    public S wrapped() {
        return wrapped;
    }
}
