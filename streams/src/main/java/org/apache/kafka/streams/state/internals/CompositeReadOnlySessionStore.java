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
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlySessionStore;

import java.util.List;
import java.util.Objects;

/**
 * Wrapper over the underlying {@link ReadOnlySessionStore}s found in a {@link
 * org.apache.kafka.streams.processor.internals.ProcessorTopology}
 */
public class CompositeReadOnlySessionStore<K, V> implements ReadOnlySessionStore<K, V> {
    private final StateStoreProvider storeProvider;
    private final QueryableStoreType<ReadOnlySessionStore<K, V>> queryableStoreType;
    private final String storeName;

    public CompositeReadOnlySessionStore(final StateStoreProvider storeProvider,
                                         final QueryableStoreType<ReadOnlySessionStore<K, V>> queryableStoreType,
                                         final String storeName) {
        this.storeProvider = storeProvider;
        this.queryableStoreType = queryableStoreType;
        this.storeName = storeName;
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> findSessions(final K key,
                                                         final long earliestSessionEndTime,
                                                         final long latestSessionStartTime) {
        Objects.requireNonNull(key, "key can't be null");
        final List<ReadOnlySessionStore<K, V>> stores = storeProvider.stores(storeName, queryableStoreType);
        for (final ReadOnlySessionStore<K, V> store : stores) {
            try {
                final KeyValueIterator<Windowed<K>, V> result =
                    store.findSessions(key, earliestSessionEndTime, latestSessionStartTime);

                if (!result.hasNext()) {
                    result.close();
                } else {
                    return result;
                }
            } catch (final InvalidStateStoreException ise) {
                throw new InvalidStateStoreException(
                    "State store  [" + storeName + "] is not available anymore" +
                        " and may have been migrated to another instance; " +
                        "please re-discover its location from the state metadata.",
                    ise
                );
            }
        }
        return KeyValueIterators.emptyIterator();
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardFindSessions(final K key,
                                                                 final long earliestSessionEndTime,
                                                                 final long latestSessionStartTime) {
        Objects.requireNonNull(key, "key can't be null");
        final List<ReadOnlySessionStore<K, V>> stores = storeProvider.stores(storeName, queryableStoreType);
        for (final ReadOnlySessionStore<K, V> store : stores) {
            try {
                final KeyValueIterator<Windowed<K>, V> result = store.backwardFindSessions(key, earliestSessionEndTime, latestSessionStartTime);
                if (!result.hasNext()) {
                    result.close();
                } else {
                    return result;
                }
            } catch (final InvalidStateStoreException ise) {
                throw new InvalidStateStoreException(
                    "State store  [" + storeName + "] is not available anymore" +
                        " and may have been migrated to another instance; " +
                        "please re-discover its location from the state metadata.",
                    ise
                );
            }
        }
        return KeyValueIterators.emptyIterator();
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> findSessions(final K keyFrom,
                                                         final K keyTo,
                                                         final long earliestSessionEndTime,
                                                         final long latestSessionStartTime) {
        final List<ReadOnlySessionStore<K, V>> stores = storeProvider.stores(storeName, queryableStoreType);
        for (final ReadOnlySessionStore<K, V> store : stores) {
            try {
                final KeyValueIterator<Windowed<K>, V> result =
                    store.findSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime);
                if (!result.hasNext()) {
                    result.close();
                } else {
                    return result;
                }
            } catch (final InvalidStateStoreException ise) {
                throw new InvalidStateStoreException(
                    "State store  [" + storeName + "] is not available anymore" +
                        " and may have been migrated to another instance; " +
                        "please re-discover its location from the state metadata.",
                    ise
                );
            }
        }
        return KeyValueIterators.emptyIterator();
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardFindSessions(final K keyFrom,
                                                                 final K keyTo,
                                                                 final long earliestSessionEndTime,
                                                                 final long latestSessionStartTime) {
        final List<ReadOnlySessionStore<K, V>> stores = storeProvider.stores(storeName, queryableStoreType);
        for (final ReadOnlySessionStore<K, V> store : stores) {
            try {
                final KeyValueIterator<Windowed<K>, V> result =
                    store.backwardFindSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime);
                if (!result.hasNext()) {
                    result.close();
                } else {
                    return result;
                }
            } catch (final InvalidStateStoreException ise) {
                throw new InvalidStateStoreException(
                    "State store  [" + storeName + "] is not available anymore" +
                        " and may have been migrated to another instance; " +
                        "please re-discover its location from the state metadata.",
                    ise
                );
            }
        }
        return KeyValueIterators.emptyIterator();
    }

    @Override
    public V fetchSession(final K key, final long earliestSessionEndTime, final long latestSessionStartTime) {
        Objects.requireNonNull(key, "key can't be null");
        final List<ReadOnlySessionStore<K, V>> stores = storeProvider.stores(storeName, queryableStoreType);
        for (final ReadOnlySessionStore<K, V> store : stores) {
            try {
                return store.fetchSession(key, earliestSessionEndTime, latestSessionStartTime);
            } catch (final InvalidStateStoreException ise) {
                throw new InvalidStateStoreException(
                    "State store  [" + storeName + "] is not available anymore" +
                        " and may have been migrated to another instance; " +
                        "please re-discover its location from the state metadata.",
                    ise
                );
            }
        }
        return null;
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(final K key) {
        Objects.requireNonNull(key, "key can't be null");
        final List<ReadOnlySessionStore<K, V>> stores = storeProvider.stores(storeName, queryableStoreType);
        for (final ReadOnlySessionStore<K, V> store : stores) {
            try {
                final KeyValueIterator<Windowed<K>, V> result = store.fetch(key);
                if (!result.hasNext()) {
                    result.close();
                } else {
                    return result;
                }
            } catch (final InvalidStateStoreException ise) {
                throw new InvalidStateStoreException("State store  [" + storeName + "] is not available anymore" +
                                                             " and may have been migrated to another instance; " +
                                                             "please re-discover its location from the state metadata. " +
                                                             "Original error message: " + ise.toString());
            }
        }
        return KeyValueIterators.emptyIterator();
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardFetch(final K key) {
        Objects.requireNonNull(key, "key can't be null");
        final List<ReadOnlySessionStore<K, V>> stores = storeProvider.stores(storeName, queryableStoreType);
        for (final ReadOnlySessionStore<K, V> store : stores) {
            try {
                final KeyValueIterator<Windowed<K>, V> result = store.backwardFetch(key);
                if (!result.hasNext()) {
                    result.close();
                } else {
                    return result;
                }
            } catch (final InvalidStateStoreException ise) {
                throw new InvalidStateStoreException(
                    "State store  [" + storeName + "] is not available anymore" +
                        " and may have been migrated to another instance; " +
                        "please re-discover its location from the state metadata.",
                    ise
                );
            }
        }
        return KeyValueIterators.emptyIterator();
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(final K keyFrom, final K keyTo) {
        final NextIteratorFunction<Windowed<K>, V, ReadOnlySessionStore<K, V>> nextIteratorFunction =
            store -> store.fetch(keyFrom, keyTo);
        return new DelegatingPeekingKeyValueIterator<>(storeName,
                                                       new CompositeKeyValueIterator<>(
                                                               storeProvider.stores(storeName, queryableStoreType).iterator(),
                                                               nextIteratorFunction));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardFetch(final K keyFrom, final K keyTo) {
        final NextIteratorFunction<Windowed<K>, V, ReadOnlySessionStore<K, V>> nextIteratorFunction =
            store -> store.backwardFetch(keyFrom, keyTo);
        return new DelegatingPeekingKeyValueIterator<>(
            storeName,
            new CompositeKeyValueIterator<>(
                storeProvider.stores(storeName, queryableStoreType).iterator(),
                nextIteratorFunction
            )
        );
    }
}
