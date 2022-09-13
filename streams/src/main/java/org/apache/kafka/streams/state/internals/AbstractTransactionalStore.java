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

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecorder;

public abstract class AbstractTransactionalStore<T extends KeyValueStore<Bytes, byte[]>> implements KeyValueStore<Bytes, byte[]> {
    private static final byte MODIFICATION = 0x1;
    private static final byte DELETION = 0x2;
    private static final byte[] DELETION_VAL = {DELETION};

    static final String PREFIX = "transactional-";
    //VisibleForTesting
    public static final String TMP_SUFFIX = ".tmp";

    private final Set<MergeKeyValueIterator> openIterators = Collections.synchronizedSet(new HashSet<>());

    Map<String, Object> configs;
    File stateDir;

    KeyValueSegment createTmpStore(final String segmentName,
                                           final String windowName,
                                           final long segmentId,
                                           final RocksDBMetricsRecorder metricsRecorder) {
        return new KeyValueSegment(segmentName + TMP_SUFFIX,
                                    windowName,
                                    segmentId,
                                    metricsRecorder);
    }

    public abstract T mainStore();

    public abstract KeyValueSegment tmpStore();

    @Deprecated
    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        if (context instanceof StateStoreContext) {
            init((StateStoreContext) context, root);
        } else {
            throw new UnsupportedOperationException(
                "Use TransactionalKeyValueStore#init(StateStoreContext, StateStore) instead."
            );
        }
    }

    @Override
    public void init(final StateStoreContext context, final StateStore root) {
        doInit(context.appConfigs(), context.stateDir());
        mainStore().init(context, root);
    }

    void doInit(final Map<String, Object> configs, final File stateDir) {
        this.configs = configs;
        this.stateDir = stateDir;
        tmpStore().openDB(configs, stateDir);
    }

    @Override
    public synchronized void close() {
        final HashSet<KeyValueIterator<Bytes, byte[]>> iterators;
        synchronized (openIterators) {
            iterators = new HashSet<>(openIterators);
        }
        for (final KeyValueIterator<Bytes, byte[]> iterator : iterators) {
            iterator.close();
        }

        tmpStore().close();
        mainStore().close();
    }

    @Override
    public void commit(final Long changelogOffset) {
        tmpStore().commit(changelogOffset);
        doCommit();
    }

    @Override
    public boolean recover(final long changelogOffset) {
        truncateTmpStore();
        return true;
    }

    private void truncateTmpStore() {
        try {
            tmpStore().close();
            tmpStore().destroy();
            tmpStore().openDB(configs, stateDir);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean persistent() {
        return mainStore().persistent();
    }

    @Override
    public boolean isOpen() {
        return tmpStore().isOpen() && mainStore().isOpen();
    }

    @Override
    public void put(final Bytes key, final byte[] value) {
        tmpStore().put(key, toUncommittedValue(value));
    }

    @Override
    public byte[] putIfAbsent(final Bytes key, final byte[] value) {
        final byte[] prev = get(key);
        if (prev == null) {
            tmpStore().put(key, toUncommittedValue(value));
        }
        return prev;
    }

    @Override
    public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
        final List<KeyValue<Bytes, byte[]>> tmpEntries = entries
            .stream()
            .map(e -> new KeyValue<>(e.key, toUncommittedValue(e.value)))
            .collect(Collectors.toList());
        tmpStore().putAll(tmpEntries);
    }

    @Override
    public byte[] delete(final Bytes key) {
        final byte[] value = get(key);
        tmpStore().put(key, DELETION_VAL);
        return value;
    }

    @Override
    public byte[] get(final Bytes key) {
        final byte[] tmpValue = tmpStore().get(key);
        final byte[] mainValue = mainStore().get(key);
        if (tmpValue == null) {
            return mainValue;
        } else if (tmpValue[0] == DELETION) {
            return null;
        } else {
            return fromUncommittedValue(tmpValue);
        }
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {
        final MergeKeyValueIterator iterator = new MergeKeyValueIterator(
            tmpStore().range(from, to), mainStore().range(from, to), openIterators);
        openIterators.add(iterator);
        return iterator;
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseRange(final Bytes from, final Bytes to) {
        final MergeKeyValueIterator iterator = new MergeKeyValueIterator(
            tmpStore().reverseRange(from, to),
            mainStore().reverseRange(from, to),
            true,
            openIterators);
        openIterators.add(iterator);
        return iterator;
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        final MergeKeyValueIterator iterator = new MergeKeyValueIterator(
            tmpStore().all(), mainStore().all(), openIterators);
        openIterators.add(iterator);
        return iterator;
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseAll() {
        final MergeKeyValueIterator iterator = new MergeKeyValueIterator(
            tmpStore().reverseAll(), mainStore().reverseAll(), true, openIterators);
        openIterators.add(iterator);
        return iterator;
    }

    @Override
    public long approximateNumEntries() {
        try {
            return Math.addExact(tmpStore().approximateNumEntries(), mainStore().approximateNumEntries());
        } catch (final ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    private void doCommit() {
        try (final KeyValueIterator<Bytes, byte[]> it = tmpStore().all()) {
            while (it.hasNext()) {
                final KeyValue<Bytes, byte[]> kv = it.next();
                mainStore().put(kv.key, fromUncommittedValue(kv.value));
            }
        }

        truncateTmpStore();
    }

    private static KeyValue<Bytes, byte[]> fromUncommittedKV(final KeyValue<Bytes, byte[]> kv) {
        if (kv.value[0] == DELETION) {
            return null;
        } else {
            final byte[] value = new byte[kv.value.length - 1];
            System.arraycopy(kv.value, 1, value, 0, value.length);
            return new KeyValue<>(kv.key, value);
        }
    }

    private static byte[] fromUncommittedValue(final byte[] value) {
        if (value == null || value[0] == DELETION) {
            return null;
        } else {
            final byte[] val = new byte[value.length - 1];
            System.arraycopy(value, 1, val, 0, val.length);
            return val;
        }
    }

    static byte[] toUncommittedValue(final byte[] value) {
        if (value == null) {
            return DELETION_VAL;
        } else {
            final byte[] val = new byte[value.length + 1];
            val[0] = MODIFICATION;
            System.arraycopy(value, 0, val, 1, value.length);
            return val;
        }
    }

    static class MergeKeyValueIterator implements KeyValueIterator<Bytes, byte[]> {
        private final KeyValueIterator<Bytes, byte[]> uncommittedIterator;
        private final KeyValueIterator<Bytes, byte[]> committedIterator;
        private final boolean reverse;
        private final Set<MergeKeyValueIterator> openIterators;

        private KeyValue<Bytes, byte[]> nextKV;

        MergeKeyValueIterator(final KeyValueIterator<Bytes, byte[]> uncommittedIterator,
                              final KeyValueIterator<Bytes, byte[]> committedIterator,
                              final Set<MergeKeyValueIterator> openIterators) {
            this(uncommittedIterator, committedIterator, false, openIterators);
        }

        MergeKeyValueIterator(final KeyValueIterator<Bytes, byte[]> uncommittedIterator,
                              final KeyValueIterator<Bytes, byte[]> committedIterator,
                              final boolean reverse,
                              final Set<MergeKeyValueIterator> openIterators) {
            this.uncommittedIterator = uncommittedIterator;
            this.committedIterator = committedIterator;
            this.reverse = reverse;
            this.openIterators = openIterators;
        }

        @Override
        public synchronized void close() {
            openIterators.remove(this);
            uncommittedIterator.close();
            committedIterator.close();
            nextKV = null;
        }

        @Override
        public Bytes peekNextKey() {
            setNextKV();
            return nextKV != null ? nextKV.key : null;
        }

        @Override
        public boolean hasNext() {
            setNextKV();
            return nextKV != null;
        }

        @Override
        public KeyValue<Bytes, byte[]> next() {
            setNextKV();
            final KeyValue<Bytes, byte[]> kv = nextKV;
            nextKV = null;
            if (kv == null) {
                throw new NoSuchElementException();
            }
            return kv;
        }

        private void setNextKV() {
            if (nextKV != null) {
                return;
            }

            // a loop to skip over tombstones in the uncommitted store
            while (uncommittedIterator.hasNext() || committedIterator.hasNext()) {
                final Bytes uncommittedNext = uncommittedIterator.hasNext() ? uncommittedIterator.peekNextKey() : null;
                final Bytes committedNext = committedIterator.hasNext() ? committedIterator.peekNextKey() : null;
                final KeyValue<Bytes, byte[]> kv;
                if (committedNext == null) {
                    kv = fromUncommittedKV(uncommittedIterator.next());
                } else if (uncommittedNext == null) {
                    kv = committedIterator.next();
                } else if (uncommittedNext.equals(committedNext)) {
                    committedIterator.next(); // shadowed by uncommittedIterator
                    kv = fromUncommittedKV(uncommittedIterator.next());
                } else {
                    final int cmp = uncommittedNext.compareTo(committedNext);
                    if (reverse) {
                        if (cmp < 0) {
                            kv = committedIterator.next();
                        } else {
                            kv = fromUncommittedKV(uncommittedIterator.next());
                        }
                    } else {
                        if (cmp < 0) {
                            kv = fromUncommittedKV(uncommittedIterator.next());
                        } else {
                            kv = committedIterator.next();
                        }
                    }
                }

                if (kv != null) {
                    nextKV = kv;
                    break;
                }
            }
        }
    }
}
