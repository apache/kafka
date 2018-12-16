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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.AbstractNotifyingBatchingRestoreCallback;
import org.apache.kafka.streams.processor.BatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.RecordConverter;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.kafka.streams.state.internals.KeyValueKeyValueWithTimestampProxyStore.getValueWithUnknownTimestamp;

/**
 * A persistent key-value store based on RocksDB.
 *
 * Note that the use of array-typed keys is discouraged because they result in incorrect caching behavior.
 * If you intend to work on byte arrays as key, for example, you may want to wrap them with the {@code Bytes} class,
 * i.e. use {@code RocksDBStore<Bytes, ...>} rather than {@code RocksDBStore<byte[], ...>}.
 */
public class RocksDBWithTimestampStore implements KeyValueStore<Bytes, byte[]>, RecordConverter {

    private static final Pattern SST_FILE_EXTENSION = Pattern.compile(".*\\.sst");

    private static final CompressionType COMPRESSION_TYPE = CompressionType.NO_COMPRESSION;
    private static final CompactionStyle COMPACTION_STYLE = CompactionStyle.UNIVERSAL;
    private static final long WRITE_BUFFER_SIZE = 16 * 1024 * 1024L;
    private static final long BLOCK_CACHE_SIZE = 50 * 1024 * 1024L;
    private static final long BLOCK_SIZE = 4096L;
    private static final int MAX_WRITE_BUFFERS = 3;
    private static final String DB_FILE_DIR = "rocksdb";

    private final String name;
    private final String parentDir;
    private final Set<KeyValueIterator> openIterators = Collections.synchronizedSet(new HashSet<>());

    private File dbDir;
    private RocksDB db;
    private ColumnFamilyHandle noTimestampColumnFamily;
    private ColumnFamilyHandle withTimestampColumnFamily;

    // the following option objects will be created in the constructor and closed in the close() method
    private RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsFacade userSpecifiedOptions;
    private WriteOptions wOptions;
    private FlushOptions fOptions;

    private volatile boolean prepareForBulkload = false;
    private ProcessorContext internalProcessorContext;
    // visible for testing
    volatile BatchingStateRestoreCallback batchingStateRestoreCallback = null;

    protected volatile boolean open = false;

    public RocksDBWithTimestampStore(final String name) {
        this(name, DB_FILE_DIR);
    }

    public RocksDBWithTimestampStore(final String name, final String parentDir) {
        this.name = name;
        this.parentDir = parentDir;
    }

    @SuppressWarnings("unchecked")
    void openDB(final ProcessorContext context) {
        // initialize the default rocksdb options
        final BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
        tableConfig.setBlockCacheSize(BLOCK_CACHE_SIZE);
        tableConfig.setBlockSize(BLOCK_SIZE);

        final DBOptions dbOptions = new DBOptions();
        final ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
        userSpecifiedOptions = new RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsFacade(dbOptions, columnFamilyOptions);
        userSpecifiedOptions.setTableFormatConfig(tableConfig);
        userSpecifiedOptions.setWriteBufferSize(WRITE_BUFFER_SIZE);
        userSpecifiedOptions.setCompressionType(COMPRESSION_TYPE);
        userSpecifiedOptions.setCompactionStyle(COMPACTION_STYLE);
        userSpecifiedOptions.setMaxWriteBufferNumber(MAX_WRITE_BUFFERS);
        userSpecifiedOptions.setCreateIfMissing(true);
        userSpecifiedOptions.setErrorIfExists(false);
        userSpecifiedOptions.setInfoLogLevel(InfoLogLevel.ERROR_LEVEL);
        // this is the recommended way to increase parallelism in RocksDb
        // note that the current implementation of setIncreaseParallelism affects the number
        // of compaction threads but not flush threads (the latter remains one). Also
        // the parallelism value needs to be at least two because of the code in
        // https://github.com/facebook/rocksdb/blob/62ad0a9b19f0be4cefa70b6b32876e764b7f3c11/util/options.cc#L580
        // subtracts one from the value passed to determine the number of compaction threads
        // (this could be a bug in the RocksDB code and their devs have been contacted).
        userSpecifiedOptions.setIncreaseParallelism(Math.max(Runtime.getRuntime().availableProcessors(), 2));

        wOptions = new WriteOptions();
        wOptions.setDisableWAL(true);

        fOptions = new FlushOptions();
        fOptions.setWaitForFlush(true);

        final Map<String, Object> configs = context.appConfigs();
        final Class<RocksDBConfigSetter> configSetterClass =
                (Class<RocksDBConfigSetter>) configs.get(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG);

        if (configSetterClass != null) {
            final RocksDBConfigSetter configSetter = Utils.newInstance(configSetterClass);
            configSetter.setConfig(name, userSpecifiedOptions, configs);
        }

        if (prepareForBulkload) {
            userSpecifiedOptions.prepareForBulkLoad();
        }

        dbDir = new File(new File(context.stateDir(), parentDir), name);

        try {
            final List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<ColumnFamilyDescriptor>(2) {
                {
                    add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions));
                    add(new ColumnFamilyDescriptor("keyValueWithTimestamp".getBytes(StandardCharsets.UTF_8), columnFamilyOptions));
                }
            };
            final List<ColumnFamilyHandle> columnFamilies = new ArrayList<>(columnFamilyDescriptors.size());

            Files.createDirectories(dbDir.getParentFile().toPath());

            try {
                db = RocksDB.open(dbOptions, dbDir.getAbsolutePath(), columnFamilyDescriptors, columnFamilies);
            } catch (final RocksDBException e) {
                if ("Column family not found: : keyValueWithTimestamp".equals(e.getMessage())) {
                    try {
                        db = RocksDB.open(dbOptions, dbDir.getAbsolutePath(), columnFamilyDescriptors.subList(0, 1), columnFamilies);
                        columnFamilies.add(db.createColumnFamily(columnFamilyDescriptors.get(1)));
                    } catch (final RocksDBException fatal) {
                        throw new ProcessorStateException("Error opening store " + name + " at location " + dbDir.toString(), fatal);
                    }
                    columnFamilies.get(0).close();
                    columnFamilies.get(1).close();
                    columnFamilies.clear();
                    db.close();
                    try {
                        db = RocksDB.open(dbOptions, dbDir.getAbsolutePath(), columnFamilyDescriptors, columnFamilies);
                    } catch (final RocksDBException fatal) {
                        throw new ProcessorStateException("Error opening store " + name + " at location " + dbDir.toString(), fatal);
                    }
                } else {
                    throw new ProcessorStateException("Error opening store " + name + " at location " + dbDir.toString(), e);
                }
            }
            noTimestampColumnFamily = columnFamilies.get(0);
            withTimestampColumnFamily = columnFamilies.get(1);
        } catch (final IOException fatal) {
            throw new ProcessorStateException(fatal);
        }

        open = true;
    }

    public void init(final ProcessorContext context,
                     final StateStore root) {
        // open the DB dir
        internalProcessorContext = context;
        openDB(context);
        batchingStateRestoreCallback = new RocksDBBatchingRestoreCallback(this);

        // value getter should always read directly from rocksDB
        // since it is only for values that are already flushed
        context.register(root, batchingStateRestoreCallback);
    }

    // visible for testing
    boolean isPrepareForBulkload() {
        return prepareForBulkload;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean persistent() {
        return true;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public synchronized byte[] get(final Bytes key) {
        validateStoreOpen();
        return getInternal(key.get());
    }

    private void validateStoreOpen() {
        if (!open) {
            throw new InvalidStateStoreException("Store " + name + " is currently closed");
        }
    }

    private byte[] getInternal(final byte[] rawKey) {
        try {
            final byte[] rawValueWithTimestamp = db.get(withTimestampColumnFamily, rawKey);
            if (rawValueWithTimestamp != null) {
                return rawValueWithTimestamp;
            }

            final byte[] rawValue = db.get(noTimestampColumnFamily, rawKey);
            if (rawValue != null) {
                final byte[] rawValueWithUnknownTimestamp = getValueWithUnknownTimestamp(rawValue);
                // this does only work, because the changelog topic contains correct data already
                // for other format changes, we cannot take this short cut and can only migrate data
                // from old to new store on put()
                putInternal(rawKey, rawValueWithUnknownTimestamp);
                return rawValueWithUnknownTimestamp;
            }

            return null;
        } catch (final RocksDBException e) {
            // String format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
            throw new ProcessorStateException("Error while getting value for key %s from store " + name, e);
        }
    }

    private void toggleDbForBulkLoading(final boolean prepareForBulkload) {

        if (prepareForBulkload) {
            // if the store is not empty, we need to compact to get around the num.levels check for bulk loading
            final String[] sstFileNames = dbDir.list((dir, name) -> SST_FILE_EXTENSION.matcher(name).matches());

            if (sstFileNames != null && sstFileNames.length > 0) {
                try {
                    db.compactRange(true, 1, 0);
                    db.compactRange(true, 1, 0);
                } catch (final RocksDBException e) {
                    throw new ProcessorStateException("Error while range compacting during restoring  store " + name, e);
                }
            }
        }

        close();
        this.prepareForBulkload = prepareForBulkload;
        openDB(internalProcessorContext);
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized void put(final Bytes key,
                                 final byte[] rawValueWithTimestamp) {
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();
        putInternal(key.get(), rawValueWithTimestamp);
    }

    @Override
    public synchronized byte[] putIfAbsent(final Bytes key,
                                           final byte[] rawValueWithTimestamp) {
        Objects.requireNonNull(key, "key cannot be null");
        final byte[] originalValueWithTimestamp = get(key);
        if (originalValueWithTimestamp == null) {
            put(key, rawValueWithTimestamp);
        }
        return originalValueWithTimestamp;
    }

    private void restoreAllInternal(final Collection<KeyValue<byte[], byte[]>> records) {
        try (final WriteBatch batch = new WriteBatch()) {
            for (final KeyValue<byte[], byte[]> record : records) {
                if (record.value == null) {
                    batch.delete(noTimestampColumnFamily, record.key);
                    batch.delete(withTimestampColumnFamily, record.key);
                } else {
                    batch.delete(noTimestampColumnFamily, record.key);
                    batch.put(withTimestampColumnFamily, record.key, record.value);
                }
            }
            write(batch);
        } catch (final RocksDBException e) {
            throw new ProcessorStateException("Error restoring batch to store " + this.name, e);
        }
    }

    private void putInternal(final byte[] rawKey,
                             final byte[] rawValueWithTimestamp) {
        if (rawValueWithTimestamp == null) {
            try {
                db.delete(noTimestampColumnFamily, wOptions, rawKey);
                db.delete(withTimestampColumnFamily, wOptions, rawKey);
            } catch (final RocksDBException e) {
                // String format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                throw new ProcessorStateException("Error while removing key %s from store " + name, e);
            }
        } else {
            try {
                db.delete(noTimestampColumnFamily, wOptions, rawKey);
                db.put(withTimestampColumnFamily, wOptions, rawKey, rawValueWithTimestamp);
            } catch (final RocksDBException e) {
                // String format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                throw new ProcessorStateException("Error while putting key %s value %s into store " + name, e);
            }
        }
    }

    private void write(final WriteBatch batch) throws RocksDBException {
        db.write(wOptions, batch);
    }

    @Override
    public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
        try (final WriteBatch batch = new WriteBatch()) {
            for (final KeyValue<Bytes, byte[]> entry : entries) {
                Objects.requireNonNull(entry.key, "key cannot be null");
                if (entry.value == null) {
                    batch.delete(noTimestampColumnFamily, entry.key.get());
                    batch.delete(withTimestampColumnFamily, entry.key.get());
                } else {
                    batch.delete(noTimestampColumnFamily, entry.key.get());
                    batch.put(withTimestampColumnFamily, entry.key.get(), entry.value);
                }
            }
            write(batch);
        } catch (final RocksDBException e) {
            throw new ProcessorStateException("Error while batch writing to store " + name, e);
        }

    }

    @Override
    public synchronized byte[] delete(final Bytes key) {
        Objects.requireNonNull(key, "key cannot be null");
        final byte[] valueWithTimestamp = get(key);
        put(key, null);
        return valueWithTimestamp;
    }

    @Override
    public synchronized KeyValueIterator<Bytes, byte[]> range(final Bytes from,
                                                              final Bytes to) {
        Objects.requireNonNull(from, "from cannot be null");
        Objects.requireNonNull(to, "to cannot be null");
        validateStoreOpen();

        // query rocksdb
        final RocksDBRangeIterator rocksDBRangeIterator = new RocksDBRangeIterator(
            name,
            db.newIterator(withTimestampColumnFamily),
            db.newIterator(noTimestampColumnFamily),
            from,
            to);
        openIterators.add(rocksDBRangeIterator);

        return rocksDBRangeIterator;
    }

    @Override
    public synchronized KeyValueIterator<Bytes, byte[]> all() {
        validateStoreOpen();
        // query rocksdb
        final RocksIterator innerIterWithTimestamp = db.newIterator(withTimestampColumnFamily);
        innerIterWithTimestamp.seekToFirst();
        final RocksIterator innerIterNoTimestamp = db.newIterator(noTimestampColumnFamily);
        innerIterNoTimestamp.seekToFirst();
        final RocksDbIterator rocksDbIterator = new RocksDbIterator(name, innerIterWithTimestamp, innerIterNoTimestamp);
        openIterators.add(rocksDbIterator);
        return rocksDbIterator;
    }

    /**
     * Return an approximate count of key-value mappings in this store.
     *
     * <code>RocksDB</code> cannot return an exact entry count without doing a
     * full scan, so this method relies on the <code>rocksdb.estimate-num-keys</code>
     * property to get an approximate count. The returned size also includes
     * a count of dirty keys in the store's in-memory cache, which may lead to some
     * double-counting of entries and inflate the estimate.
     *
     * @return an approximate count of key-value mappings in the store.
     */
    @Override
    public long approximateNumEntries() {
        validateStoreOpen();
        final long value;
        try {
            value = db.getLongProperty(noTimestampColumnFamily, "rocksdb.estimate-num-keys")
                + db.getLongProperty(withTimestampColumnFamily, "rocksdb.estimate-num-keys");
        } catch (final RocksDBException e) {
            throw new ProcessorStateException("Error fetching property from store " + name, e);
        }
        if (isOverflowing(value)) {
            return Long.MAX_VALUE;
        }
        return value;
    }

    private boolean isOverflowing(final long value) {
        // RocksDB returns an unsigned 8-byte integer, which could overflow long
        // and manifest as a negative value.
        return value < 0;
    }

    @Override
    public synchronized void flush() {
        if (db == null) {
            return;
        }
        // flush RocksDB
        flushInternal();
    }
    /**
     * @throws ProcessorStateException if flushing failed because of any internal store exceptions
     */
    private void flushInternal() {
        try {
            db.flush(fOptions, noTimestampColumnFamily);
            db.flush(fOptions, withTimestampColumnFamily);
        } catch (final RocksDBException e) {
            throw new ProcessorStateException("Error while executing flush from store " + name, e);
        }
    }

    @Override
    public synchronized void close() {
        if (!open) {
            return;
        }

        open = false;
        closeOpenIterators();
        noTimestampColumnFamily.close();
        withTimestampColumnFamily.close();
        userSpecifiedOptions.close();
        wOptions.close();
        fOptions.close();
        db.close();

        userSpecifiedOptions = null;
        wOptions = null;
        fOptions = null;
        db = null;
    }

    private void closeOpenIterators() {
        final HashSet<KeyValueIterator> iterators;
        synchronized (openIterators) {
            iterators = new HashSet<>(openIterators);
        }
        for (final KeyValueIterator iterator : iterators) {
            iterator.close();
        }
    }

    @Override
    public ConsumerRecord<byte[], byte[]> convert(final ConsumerRecord<byte[], byte[]> record) {
        final byte[] rawValue = record.value();
        return new ConsumerRecord<>(
            record.topic(),
            record.partition(),
            record.offset(),
            record.timestamp(),
            record.timestampType(),
            (long) ConsumerRecord.NULL_CHECKSUM,
            record.serializedKeySize(),
            8 + record.serializedValueSize(),
            record.key(),
            ByteBuffer.allocate(8 + rawValue.length)
                .putLong(record.timestamp())
                .put(rawValue).array(),
            record.headers(),
            record.leaderEpoch()
        );
    }

    private class RocksDbIterator extends AbstractIterator<KeyValue<Bytes, byte[]>> implements KeyValueIterator<Bytes, byte[]> {
        // RocksDB's JNI interface does not expose getters/setters that allow the
        // comparator to be pluggable, and the default is lexicographic, so it's
        // safe to just force lexicographic comparator here for now.
        private final Comparator<byte[]> comparator = Bytes.BYTES_LEXICO_COMPARATOR;

        private final String storeName;
        private final RocksIterator iterWithTimestamp;
        private final RocksIterator iterNoTimestamp;

        private volatile boolean open = true;

        private KeyValue<Bytes, byte[]> nextWithTimestamp;
        private KeyValue<Bytes, byte[]> nextNoTimestamp;
        private KeyValue<Bytes, byte[]> next;

        RocksDbIterator(final String storeName,
                        final RocksIterator iterWithTimestamp,
                        final RocksIterator iterNoTimestamp) {
            this.iterWithTimestamp = iterWithTimestamp;
            this.iterNoTimestamp = iterNoTimestamp;
            this.storeName = storeName;
        }

        @Override
        public synchronized boolean hasNext() {
            if (!open) {
                throw new InvalidStateStoreException(String.format("RocksDB store %s has closed", storeName));
            }
            return super.hasNext();
        }

        @Override
        public synchronized KeyValue<Bytes, byte[]> next() {
            return super.next();
        }

        @Override
        public KeyValue<Bytes, byte[]> makeNext() {
            if (nextNoTimestamp == null && iterNoTimestamp.isValid()) {
                nextNoTimestamp = getKeyValueNoTimestamp();
            }

            if (nextWithTimestamp == null && iterWithTimestamp.isValid()) {
                nextWithTimestamp = getKeyValueWithTimestamp();
            }

            if (nextNoTimestamp == null) {
                if (nextWithTimestamp == null) {
                    return allDone();
                } else {
                    next = nextWithTimestamp;
                    nextWithTimestamp = null;
                    iterWithTimestamp.next();
                }
            } else {
                if (nextWithTimestamp == null) {
                    next = nextNoTimestamp;
                    nextNoTimestamp = null;
                    iterNoTimestamp.next();
                } else {
                    if (comparator.compare(nextNoTimestamp.key.get(), nextWithTimestamp.key.get()) <= 0) {
                        next = nextNoTimestamp;
                        nextNoTimestamp = null;
                        iterNoTimestamp.next();
                    } else {
                        next = nextWithTimestamp;
                        nextWithTimestamp = null;
                        iterWithTimestamp.next();
                    }
                }

            }

            return next;
        }

        private KeyValue<Bytes, byte[]> getKeyValueWithTimestamp() {
            return new KeyValue<>(new Bytes(iterWithTimestamp.key()), iterWithTimestamp.value());
        }

        private KeyValue<Bytes, byte[]> getKeyValueNoTimestamp() {
            return new KeyValue<>(new Bytes(iterNoTimestamp.key()), getValueWithUnknownTimestamp(iterNoTimestamp.value()));
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("RocksDB iterator does not support remove()");
        }

        @Override
        public synchronized void close() {
            openIterators.remove(this);
            iterNoTimestamp.close();
            iterWithTimestamp.close();
            open = false;
        }

        @Override
        public Bytes peekNextKey() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return next.key;
        }
    }

    private class RocksDBRangeIterator extends RocksDbIterator {
        // RocksDB's JNI interface does not expose getters/setters that allow the
        // comparator to be pluggable, and the default is lexicographic, so it's
        // safe to just force lexicographic comparator here for now.
        private final Comparator<byte[]> comparator = Bytes.BYTES_LEXICO_COMPARATOR;
        private final byte[] rawToKey;

        RocksDBRangeIterator(final String storeName,
                             final RocksIterator iterWithTimestamp,
                             final RocksIterator iterNoTimestamp,
                             final Bytes from,
                             final Bytes to) {
            super(storeName, iterWithTimestamp, iterNoTimestamp);
            iterWithTimestamp.seek(from.get());
            iterNoTimestamp.seek(from.get());
            rawToKey = to.get();
            if (rawToKey == null) {
                throw new NullPointerException("RocksDBRangeIterator: RawToKey is null for key " + to);
            }
        }

        @Override
        public KeyValue<Bytes, byte[]> makeNext() {
            final KeyValue<Bytes, byte[]> next = super.makeNext();

            if (next == null) {
                return allDone();
            } else {
                if (comparator.compare(next.key.get(), rawToKey) <= 0)
                    return next;
                else
                    return allDone();
            }
        }
    }

    // not private for testing
    static class RocksDBBatchingRestoreCallback extends AbstractNotifyingBatchingRestoreCallback {

        private final RocksDBWithTimestampStore rocksDBStore;

        RocksDBBatchingRestoreCallback(final RocksDBWithTimestampStore rocksDBStore) {
            this.rocksDBStore = rocksDBStore;
        }

        @Override
        public void restoreAll(final Collection<KeyValue<byte[], byte[]>> records) {
            rocksDBStore.restoreAllInternal(records);
        }

        @Override
        public void onRestoreStart(final TopicPartition topicPartition,
                                   final String storeName,
                                   final long startingOffset,
                                   final long endingOffset) {
            rocksDBStore.toggleDbForBulkLoading(true);
        }

        @Override
        public void onRestoreEnd(final TopicPartition topicPartition,
                                 final String storeName,
                                 final long totalRestored) {
            rocksDBStore.toggleDbForBulkLoading(false);
        }
    }

    // for testing
    public Options getOptions() {
        return userSpecifiedOptions;
    }
}
