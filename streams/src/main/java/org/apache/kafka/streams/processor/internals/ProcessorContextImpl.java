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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.internals.ApiUtils;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.streams.state.internals.WrappedStateStore.AbstractStateStore;

import java.time.Duration;
import java.util.List;

import static org.apache.kafka.streams.internals.ApiUtils.prepareMillisCheckFailMsgPrefix;

public class ProcessorContextImpl extends AbstractProcessorContext implements RecordCollector.Supplier {

    private final StreamTask task;
    private final RecordCollector collector;
    private TimestampSupplier streamTimeSupplier;
    private final ToInternal toInternal = new ToInternal();
    private final static To SEND_TO_ALL = To.all();

    ProcessorContextImpl(final TaskId id,
                         final StreamTask task,
                         final StreamsConfig config,
                         final RecordCollector collector,
                         final ProcessorStateManager stateMgr,
                         final StreamsMetricsImpl metrics,
                         final ThreadCache cache) {
        super(id, config, metrics, stateMgr, cache);
        this.task = task;
        this.collector = collector;
    }

    public ProcessorStateManager getStateMgr() {
        return (ProcessorStateManager) stateManager;
    }

    @Override
    public RecordCollector recordCollector() {
        return collector;
    }

    /**
     * @throws StreamsException if an attempt is made to access this state store from an unknown node
     */
    @SuppressWarnings("unchecked")
    @Override
    public StateStore getStateStore(final String name) {
        if (currentNode() == null) {
            throw new StreamsException("Accessing from an unknown node");
        }

        final StateStore global = stateManager.getGlobalStore(name);
        if (global != null) {
            if (global instanceof StateStoreReadOnlyDecorator) {
                return global;
            } else if (global instanceof KeyValueStore) {
                return new KeyValueStoreReadOnlyDecorator((KeyValueStore) global);
            } else if (global instanceof WindowStore) {
                return new WindowStoreReadOnlyDecorator((WindowStore) global);
            } else if (global instanceof SessionStore) {
                return new SessionStoreReadOnlyDecorator((SessionStore) global);
            }

            return global;
        }

        if (!currentNode().stateStores.contains(name)) {
            throw new StreamsException("Processor " + currentNode().name() + " has no access to StateStore " + name +
                " as the store is not connected to the processor. If you add stores manually via '.addStateStore()' " +
                "make sure to connect the added store to the processor by providing the processor name to " +
                "'.addStateStore()' or connect them via '.connectProcessorAndStateStores()'. " +
                "DSL users need to provide the store name to '.process()', '.transform()', or '.transformValues()' " +
                "to connect the store to the corresponding operator. If you do not add stores manually, " +
                "please file a bug report at https://issues.apache.org/jira/projects/KAFKA.");
        }

        final StateStore store = stateManager.getStore(name);
        if (store instanceof StateStoreReadWriteDecorator) {
            return global;
        } else if (store instanceof KeyValueStore) {
            return new KeyValueStoreReadWriteDecorator((KeyValueStore) store);
        } else if (store instanceof WindowStore) {
            return new WindowStoreReadWriteDecorator((WindowStore) store);
        } else if (store instanceof SessionStore) {
            return new SessionStoreReadWriteDecorator((SessionStore) store);
        }

        return store;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> void forward(final K key,
                               final V value) {
        forward(key, value, SEND_TO_ALL);
    }

    @SuppressWarnings({"unchecked", "deprecation"})
    @Override
    public <K, V> void forward(final K key,
                               final V value,
                               final int childIndex) {
        forward(
            key,
            value,
            To.child(((List<ProcessorNode>) currentNode().children()).get(childIndex).name()));
    }

    @SuppressWarnings({"unchecked", "deprecation"})
    @Override
    public <K, V> void forward(final K key,
                               final V value,
                               final String childName) {
        forward(key, value, To.child(childName));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> void forward(final K key,
                               final V value,
                               final To to) {
        toInternal.update(to);
        if (toInternal.hasTimestamp()) {
            recordContext.setTimestamp(toInternal.timestamp());
        }
        final ProcessorNode previousNode = currentNode();
        try {
            final String sendTo = toInternal.child();
            if (sendTo == null) {
                final List<ProcessorNode<K, V>> children = (List<ProcessorNode<K, V>>) currentNode().children();
                for (final ProcessorNode child : children) {
                    forward(child, key, value);
                }
            } else {
                final ProcessorNode child = currentNode().getChild(sendTo);
                if (child == null) {
                    throw new StreamsException("Unknown downstream node: " + sendTo
                        + " either does not exist or is not connected to this processor.");
                }
                forward(child, key, value);
            }
        } finally {
            setCurrentNode(previousNode);
        }
    }

    @SuppressWarnings("unchecked")
    private <K, V> void forward(final ProcessorNode child,
                                final K key,
                                final V value) {
        setCurrentNode(child);
        child.process(key, value);
    }

    @Override
    public void commit() {
        task.requestCommit();
    }

    @Override
    @Deprecated
    public Cancellable schedule(final long interval,
                                final PunctuationType type,
                                final Punctuator callback) {
        if (interval < 1) {
            throw new IllegalArgumentException("The minimum supported scheduling interval is 1 millisecond.");
        }
        return task.schedule(interval, type, callback);
    }

    @SuppressWarnings("deprecation")
    @Override
    public Cancellable schedule(final Duration interval,
                                final PunctuationType type,
                                final Punctuator callback) throws IllegalArgumentException {
        final String msgPrefix = prepareMillisCheckFailMsgPrefix(interval, "interval");
        return schedule(ApiUtils.validateMillisecondDuration(interval, msgPrefix), type, callback);
    }

    void setStreamTimeSupplier(final TimestampSupplier streamTimeSupplier) {
        this.streamTimeSupplier = streamTimeSupplier;
    }

    @Override
    public long streamTime() {
        return streamTimeSupplier.get();
    }

    private abstract static class StateStoreReadOnlyDecorator<T extends StateStore> extends AbstractStateStore {
        static final String ERROR_MESSAGE = "Global store is read only";

        private StateStoreReadOnlyDecorator(final T inner) {
            super(inner);
        }

        @SuppressWarnings("unchecked")
        T getInner() {
            return (T) wrappedStore();
        }

        @Override
        public void flush() {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @Override
        public void init(final ProcessorContext context,
                         final StateStore root) {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }
    }

    private static class KeyValueStoreReadOnlyDecorator<K, V>
        extends StateStoreReadOnlyDecorator<KeyValueStore<K, V>>
        implements KeyValueStore<K, V> {

        private KeyValueStoreReadOnlyDecorator(final KeyValueStore<K, V> inner) {
            super(inner);
        }

        @Override
        public V get(final K key) {
            return getInner().get(key);
        }

        @Override
        public KeyValueIterator<K, V> range(final K from,
                                            final K to) {
            return getInner().range(from, to);
        }

        @Override
        public KeyValueIterator<K, V> all() {
            return getInner().all();
        }

        @Override
        public long approximateNumEntries() {
            return getInner().approximateNumEntries();
        }

        @Override
        public final void put(final K key,
                              final V value) {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @Override
        public final V putIfAbsent(final K key,
                                   final V value) {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @Override
        public final void putAll(final List entries) {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @Override
        public final V delete(final K key) {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }
    }

    private static class WindowStoreReadOnlyDecorator<K, V>
        extends StateStoreReadOnlyDecorator<WindowStore<K, V>>
        implements WindowStore<K, V> {

        private WindowStoreReadOnlyDecorator(final WindowStore<K, V> inner) {
            super(inner);
        }

        @Override
        public final void put(final K key,
                              final V value) {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @Override
        public final void put(final K key,
                              final V value,
                              final long windowStartTimestamp) {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @Override
        public V fetch(final K key,
                       final long time) {
            return getInner().fetch(key, time);
        }

        @Deprecated
        @Override
        public WindowStoreIterator<V> fetch(final K key,
                                            final long timeFrom,
                                            final long timeTo) {
            return getInner().fetch(key, timeFrom, timeTo);
        }

        @Deprecated
        @Override
        public KeyValueIterator<Windowed<K>, V> fetch(final K from,
                                                      final K to,
                                                      final long timeFrom,
                                                      final long timeTo) {
            return getInner().fetch(from, to, timeFrom, timeTo);
        }

        @Override
        public KeyValueIterator<Windowed<K>, V> all() {
            return getInner().all();
        }

        @Deprecated
        @Override
        public KeyValueIterator<Windowed<K>, V> fetchAll(final long timeFrom,
                                                         final long timeTo) {
            return getInner().fetchAll(timeFrom, timeTo);
        }
    }

    private static class SessionStoreReadOnlyDecorator<K, AGG>
        extends StateStoreReadOnlyDecorator<SessionStore<K, AGG>>
        implements SessionStore<K, AGG> {

        private SessionStoreReadOnlyDecorator(final SessionStore<K, AGG> inner) {
            super(inner);
        }

        @Override
        public KeyValueIterator<Windowed<K>, AGG> findSessions(final K key,
                                                               final long earliestSessionEndTime,
                                                               final long latestSessionStartTime) {
            return getInner().findSessions(key, earliestSessionEndTime, latestSessionStartTime);
        }

        @Override
        public KeyValueIterator<Windowed<K>, AGG> findSessions(final K keyFrom,
                                                               final K keyTo,
                                                               final long earliestSessionEndTime,
                                                               final long latestSessionStartTime) {
            return getInner().findSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime);
        }

        @Override
        public final void remove(final Windowed sessionKey) {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @Override
        public final void put(final Windowed<K> sessionKey,
                              final AGG aggregate) {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @Override
        public KeyValueIterator<Windowed<K>, AGG> fetch(final K key) {
            return getInner().fetch(key);
        }

        @Override
        public KeyValueIterator<Windowed<K>, AGG> fetch(final K from,
                                                        final K to) {
            return getInner().fetch(from, to);
        }
    }

    public abstract static class StateStoreReadWriteDecorator<T extends StateStore> extends AbstractStateStore {
        static final String ERROR_MESSAGE = "This method may only be called by Kafka Streams";

        protected StateStoreReadWriteDecorator(final T inner) {
            super(inner);
        }

        @SuppressWarnings("unchecked")
        protected T wrapped() {
            return (T) super.wrappedStore();
        }

        @Override
        public final void init(final ProcessorContext context,
                               final StateStore root) {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @Override
        public final void close() {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }
    }

    public static class KeyValueStoreReadWriteDecorator<K, V>
        extends StateStoreReadWriteDecorator<KeyValueStore<K, V>>
        implements KeyValueStore<K, V> {

        public KeyValueStoreReadWriteDecorator(final KeyValueStore<K, V> inner) {
            super(inner);
        }

        @Override
        public V get(final K key) {
            return wrapped().get(key);
        }

        @Override
        public KeyValueIterator<K, V> range(final K from,
                                            final K to) {
            return wrapped().range(from, to);
        }

        @Override
        public KeyValueIterator<K, V> all() {
            return wrapped().all();
        }

        @Override
        public long approximateNumEntries() {
            return wrapped().approximateNumEntries();
        }

        @Override
        public void put(final K key,
                        final V value) {
            wrapped().put(key, value);
        }

        @Override
        public V putIfAbsent(final K key,
                             final V value) {
            return wrapped().putIfAbsent(key, value);
        }

        @Override
        public void putAll(final List<KeyValue<K, V>> entries) {
            wrapped().putAll(entries);
        }

        @Override
        public V delete(final K key) {
            return wrapped().delete(key);
        }
    }

    public static class WindowStoreReadWriteDecorator<K, V>
        extends StateStoreReadWriteDecorator<WindowStore<K, V>>
        implements WindowStore<K, V> {

        public WindowStoreReadWriteDecorator(final WindowStore<K, V> inner) {
            super(inner);
        }

        @Override
        public void put(final K key,
                        final V value) {
            wrapped().put(key, value);
        }

        @Override
        public void put(final K key,
                        final V value,
                        final long windowStartTimestamp) {
            wrapped().put(key, value, windowStartTimestamp);
        }

        @Override
        public V fetch(final K key,
                       final long time) {
            return wrapped().fetch(key, time);
        }

        @Deprecated
        @Override
        public WindowStoreIterator<V> fetch(final K key,
                                            final long timeFrom,
                                            final long timeTo) {
            return wrapped().fetch(key, timeFrom, timeTo);
        }

        @Deprecated
        @Override
        public KeyValueIterator<Windowed<K>, V> fetch(final K from,
                                                      final K to,
                                                      final long timeFrom,
                                                      final long timeTo) {
            return wrapped().fetch(from, to, timeFrom, timeTo);
        }

        @Override
        public KeyValueIterator<Windowed<K>, V> all() {
            return wrapped().all();
        }

        @Deprecated
        @Override
        public KeyValueIterator<Windowed<K>, V> fetchAll(final long timeFrom,
                                                         final long timeTo) {
            return wrapped().fetchAll(timeFrom, timeTo);
        }
    }

    public static class SessionStoreReadWriteDecorator<K, AGG>
        extends StateStoreReadWriteDecorator<SessionStore<K, AGG>>
        implements SessionStore<K, AGG> {

        public SessionStoreReadWriteDecorator(final SessionStore<K, AGG> inner) {
            super(inner);
        }

        @Override
        public KeyValueIterator<Windowed<K>, AGG> findSessions(final K key,
                                                               final long earliestSessionEndTime,
                                                               final long latestSessionStartTime) {
            return wrapped().findSessions(key, earliestSessionEndTime, latestSessionStartTime);
        }

        @Override
        public KeyValueIterator<Windowed<K>, AGG> findSessions(final K keyFrom,
                                                               final K keyTo,
                                                               final long earliestSessionEndTime,
                                                               final long latestSessionStartTime) {
            return wrapped().findSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime);
        }

        @Override
        public void remove(final Windowed<K> sessionKey) {
            wrapped().remove(sessionKey);
        }

        @Override
        public void put(final Windowed<K> sessionKey,
                        final AGG aggregate) {
            wrapped().put(sessionKey, aggregate);
        }

        @Override
        public KeyValueIterator<Windowed<K>, AGG> fetch(final K key) {
            return wrapped().fetch(key);
        }

        @Override
        public KeyValueIterator<Windowed<K>, AGG> fetch(final K from,
                                                        final K to) {
            return wrapped().fetch(from, to);
        }
    }

}
