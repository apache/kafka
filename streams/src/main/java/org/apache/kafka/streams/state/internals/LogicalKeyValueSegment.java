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

import static org.apache.kafka.streams.state.internals.RocksDBStore.incrementWithoutOverflow;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.internals.RocksDBVersionedStore.VersionedStoreSegment;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.Snapshot;
import org.rocksdb.WriteBatchInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This "logical segment" is a segment which shares its underlying physical store with other
 * logical segments. Each segment uses a unique, fixed-length key prefix derived from the
 * segment ID when writing to the shared physical store. In other words, a logical segment
 * stores a key into a shared physical store by prepending the key with a prefix (unique to
 * the specific logical segment), and storing the combined key into the physical store.
 */
class LogicalKeyValueSegment implements Comparable<LogicalKeyValueSegment>, Segment, VersionedStoreSegment {
    private static final Logger log = LoggerFactory.getLogger(LogicalKeyValueSegment.class);

    private final long id;
    private final String name;
    private final RocksDBStore physicalStore;
    private final PrefixKeyFormatter prefixKeyFormatter;

    final Set<KeyValueIterator<Bytes, byte[]>> openIterators = Collections.synchronizedSet(new HashSet<>());

    LogicalKeyValueSegment(final long id,
                           final String name,
                           final RocksDBStore physicalStore) {
        this.id = id;
        this.name = name;
        this.physicalStore = Objects.requireNonNull(physicalStore);

        this.prefixKeyFormatter = new PrefixKeyFormatter(serializeLongToBytes(id));
    }

    @Override
    public long id() {
        return id;
    }

    @Override
    public int compareTo(final LogicalKeyValueSegment segment) {
        return Long.compare(id, segment.id);
    }

    @Override
    public synchronized void destroy() {
        if (id < 0) {
            throw new IllegalStateException("Negative segment ID indicates a reserved segment, "
                + "which should not be destroyed. Reserved segments are cleaned up only when "
                + "an entire store is closed, via the close() method rather than destroy().");
        }

        final Bytes keyPrefix = prefixKeyFormatter.getPrefix();

        // this deleteRange() call deletes all entries with the given prefix, because the
        // deleteRange() implementation calls Bytes.increment() in order to make keyTo inclusive
        physicalStore.deleteRange(keyPrefix, keyPrefix);
    }

    @Override
    public synchronized void deleteRange(final Bytes keyFrom, final Bytes keyTo) {
        physicalStore.deleteRange(
            prefixKeyFormatter.addPrefix(keyFrom),
            prefixKeyFormatter.addPrefix(keyTo));
    }

    @Override
    public synchronized void put(final Bytes key, final byte[] value) {
        physicalStore.put(
            prefixKeyFormatter.addPrefix(key),
            value);
    }

    @Override
    public synchronized byte[] putIfAbsent(final Bytes key, final byte[] value) {
        return physicalStore.putIfAbsent(
            prefixKeyFormatter.addPrefix(key),
            value);
    }

    @Override
    public synchronized void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
        physicalStore.putAll(entries.stream()
            .map(kv -> new KeyValue<>(
                prefixKeyFormatter.addPrefix(kv.key),
                kv.value))
            .collect(Collectors.toList()));
    }

    @Override
    public synchronized byte[] delete(final Bytes key) {
        return physicalStore.delete(prefixKeyFormatter.addPrefix(key));
    }

    @Override
    public String name() {
        return name;
    }

    @Deprecated
    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        throw new UnsupportedOperationException("cannot initialize a logical segment");
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException("nothing to flush for logical segment");
    }

    @Override
    public synchronized void close() {
        // close open iterators
        final HashSet<KeyValueIterator<Bytes, byte[]>> iterators;
        synchronized (openIterators) {
            iterators = new HashSet<>(openIterators);
            openIterators.clear();
        }
        if (iterators.size() != 0) {
            log.warn("Closing {} open iterators for store {}", iterators.size(), name);
            for (final KeyValueIterator<Bytes, byte[]> iterator : iterators) {
                iterator.close();
            }
        }
    }

    @Override
    public boolean persistent() {
        return true;
    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public synchronized byte[] get(final Bytes key) {
        return get(key, Optional.empty());
    }

    public synchronized byte[] get(final Bytes key, final Snapshot snapshot) {
        return get(key, Optional.of(snapshot));
    }

    private synchronized byte[] get(final Bytes key, final Optional<Snapshot> snapshot) {
        if (snapshot.isPresent()) {
            try (ReadOptions readOptions = new ReadOptions()) {
                readOptions.setSnapshot(snapshot.get());
                return physicalStore.get(prefixKeyFormatter.addPrefix(key), readOptions);
            }
        } else {
            return physicalStore.get(prefixKeyFormatter.addPrefix(key));
        }
    }

    public Snapshot getSnapshot() {
        return physicalStore.getSnapshot();
    }

    public void releaseSnapshot(final Snapshot snapshot) {
        physicalStore.releaseSnapshot(snapshot);
    }

    @Override
    public synchronized KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {
        // from bound is inclusive. if the provided bound is null, replace with prefix
        final Bytes fromBound = from == null
            ? prefixKeyFormatter.getPrefix()
            : prefixKeyFormatter.addPrefix(from);
        // to bound is inclusive. if the provided bound is null, replace with the next prefix.
        // this requires potentially filtering out the element corresponding to the next prefix
        // with empty bytes from the returned iterator. this filtering is accomplished by
        // passing the prefix filter into StrippedPrefixKeyValueIteratorAdapter().
        final Bytes toBound = to == null
            ? incrementWithoutOverflow(prefixKeyFormatter.getPrefix())
            : prefixKeyFormatter.addPrefix(to);
        final KeyValueIterator<Bytes, byte[]> iteratorWithKeyPrefixes = physicalStore.range(
            fromBound,
            toBound,
            openIterators);
        return new StrippedPrefixKeyValueIteratorAdapter(
            iteratorWithKeyPrefixes,
            prefixKeyFormatter::removePrefix,
            prefixKeyFormatter::startsWithPrefix);
    }

    @Override
    public synchronized KeyValueIterator<Bytes, byte[]> all() {
        final KeyValueIterator<Bytes, byte[]> iteratorWithKeyPrefixes = physicalStore.prefixScan(
            prefixKeyFormatter.getPrefix(),
            new BytesSerializer(),
            openIterators);
        return new StrippedPrefixKeyValueIteratorAdapter(
            iteratorWithKeyPrefixes,
            prefixKeyFormatter::removePrefix);
    }

    @Override
    public long approximateNumEntries() {
        throw new UnsupportedOperationException("Cannot estimate num entries for logical segment");
    }

    @Override
    public void addToBatch(final KeyValue<byte[], byte[]> record, final WriteBatchInterface batch) throws RocksDBException {
        physicalStore.addToBatch(
            new KeyValue<>(
                prefixKeyFormatter.addPrefix(record.key),
                record.value),
            batch);
    }

    @Override
    public void write(final WriteBatchInterface batch) throws RocksDBException {
        // no key transformations here since they should've already been done as part
        // of adding to the write batch
        physicalStore.write(batch);
    }

    /**
     * Manages translation between raw key and the key to be stored into the physical store.
     * The key for the physical store is the raw key prepended with a fixed-length prefix.
     */
    private static class PrefixKeyFormatter {
        private final byte[] prefix;

        PrefixKeyFormatter(final byte[] prefix) {
            this.prefix = prefix;
        }

        Bytes addPrefix(final Bytes key) {
            return Bytes.wrap(addPrefix(key.get()));
        }

        byte[] addPrefix(final byte[] key) {
            final byte[] keyWithPrefix = new byte[prefix.length + key.length];
            System.arraycopy(prefix, 0, keyWithPrefix, 0, prefix.length);
            System.arraycopy(key, 0, keyWithPrefix, prefix.length, key.length);
            return keyWithPrefix;
        }

        Bytes removePrefix(final Bytes keyWithPrefix) {
            return Bytes.wrap(removePrefix(keyWithPrefix.get()));
        }

        private byte[] removePrefix(final byte[] keyWithPrefix) {
            final int rawKeyLength = keyWithPrefix.length - prefix.length;
            final byte[] rawKey = new byte[rawKeyLength];
            System.arraycopy(keyWithPrefix, prefix.length, rawKey, 0, rawKeyLength);
            return rawKey;
        }

        Bytes getPrefix() {
            return Bytes.wrap(prefix);
        }

        boolean startsWithPrefix(final Bytes maybePrefixed) {
            if (maybePrefixed.get().length < prefix.length) {
                return false;
            }

            final byte[] maybePrefix = new byte[prefix.length];
            System.arraycopy(maybePrefixed.get(), 0, maybePrefix, 0, prefix.length);
            return Arrays.equals(prefix, maybePrefix);
        }
    }

    /**
     * Converts a {@link KeyValueIterator} which returns keys with prefixes to one which
     * returns un-prefixed keys.
     */
    private static class StrippedPrefixKeyValueIteratorAdapter implements KeyValueIterator<Bytes, byte[]> {

        private final KeyValueIterator<Bytes, byte[]> iteratorWithKeyPrefixes;
        private final Function<Bytes, Bytes> prefixRemover;
        private final Function<Bytes, Boolean> prefixChecker;

        StrippedPrefixKeyValueIteratorAdapter(final KeyValueIterator<Bytes, byte[]> iteratorWithKeyPrefixes,
                                              final Function<Bytes, Bytes> prefixRemover) {
            this(iteratorWithKeyPrefixes, prefixRemover, bytes -> true);
        }

        StrippedPrefixKeyValueIteratorAdapter(final KeyValueIterator<Bytes, byte[]> iteratorWithKeyPrefixes,
                                              final Function<Bytes, Bytes> prefixRemover,
                                              final Function<Bytes, Boolean> prefixChecker) {
            this.iteratorWithKeyPrefixes = iteratorWithKeyPrefixes;
            this.prefixRemover = prefixRemover;
            this.prefixChecker = prefixChecker;
            pruneNonPrefixedElements();
        }

        @Override
        public boolean hasNext() {
            return iteratorWithKeyPrefixes.hasNext();
        }

        @Override
        public KeyValue<Bytes, byte[]> next() {
            final KeyValue<Bytes, byte[]> nextWithKeyPrefix = iteratorWithKeyPrefixes.next();
            final KeyValue<Bytes, byte[]> next = new KeyValue<>(
                prefixRemover.apply(nextWithKeyPrefix.key),
                nextWithKeyPrefix.value);
            pruneNonPrefixedElements();
            return next;
        }

        @Override
        public Bytes peekNextKey() {
            return prefixRemover.apply(iteratorWithKeyPrefixes.peekNextKey());
        }

        @Override
        public void remove() {
            iteratorWithKeyPrefixes.remove();
        }

        @Override
        public void close() {
            iteratorWithKeyPrefixes.close();
        }

        private void pruneNonPrefixedElements() {
            while (iteratorWithKeyPrefixes.hasNext()
                && !prefixChecker.apply(iteratorWithKeyPrefixes.peekNextKey())) {
                iteratorWithKeyPrefixes.next();
            }
        }
    }

    private static byte[] serializeLongToBytes(final long l) {
        return ByteBuffer.allocate(Long.BYTES).putLong(l).array();
    }
}