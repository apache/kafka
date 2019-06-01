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

import org.apache.kafka.common.TopicPartition;
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
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A persistent key-value store based on RocksDB.
 */
public class RocksDBStore implements KeyValueStore<Bytes, byte[]>, BulkLoadingStore {
    private static final Logger log = LoggerFactory.getLogger(RocksDBStore.class);

    private static final Pattern SST_FILE_EXTENSION = Pattern.compile(".*\\.sst");

    private static final CompressionType COMPRESSION_TYPE = CompressionType.NO_COMPRESSION;
    private static final CompactionStyle COMPACTION_STYLE = CompactionStyle.UNIVERSAL;
    private static final long WRITE_BUFFER_SIZE = 16 * 1024 * 1024L;
    private static final long BLOCK_CACHE_SIZE = 50 * 1024 * 1024L;
    private static final long BLOCK_SIZE = 4096L;
    private static final int MAX_WRITE_BUFFERS = 3;
    private static final String DB_FILE_DIR = "rocksdb";

    final String name;
    private final String parentDir;
    final Set<KeyValueIterator<Bytes, byte[]>> openIterators = Collections.synchronizedSet(new HashSet<>());

    File dbDir;
    RocksDB db;
    RocksDBAccessor dbAccessor;

    // the following option objects will be created in openDB and closed in the close() method
    private RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsAdapter userSpecifiedOptions;
    WriteOptions wOptions;
    FlushOptions fOptions;
    private Cache cache;
    private BloomFilter filter;

    private RocksDBConfigSetter configSetter;

    private volatile boolean prepareForBulkload = false;
    ProcessorContext internalProcessorContext;
    // visible for testing
    volatile BatchingStateRestoreCallback batchingStateRestoreCallback = null;

    protected volatile boolean open = false;

    RocksDBStore(final String name) {
        this(name, DB_FILE_DIR);
    }

    RocksDBStore(final String name,
                 final String parentDir) {
        this.name = name;
        this.parentDir = parentDir;
    }


    @SuppressWarnings("unchecked")
    void openDB(final ProcessorContext context) {
        // initialize the default rocksdb options

        final DBOptions dbOptions = new DBOptions();
        final ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
        userSpecifiedOptions = new RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsAdapter(dbOptions, columnFamilyOptions);

        final BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
        cache = new LRUCache(BLOCK_CACHE_SIZE);
        tableConfig.setBlockCache(cache);
        tableConfig.setBlockSize(BLOCK_SIZE);

        filter = new BloomFilter();
        tableConfig.setFilter(filter);

        userSpecifiedOptions.optimizeFiltersForHits();
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
            configSetter = Utils.newInstance(configSetterClass);
            configSetter.setConfig(name, userSpecifiedOptions, configs);
        }

        if (prepareForBulkload) {
            userSpecifiedOptions.prepareForBulkLoad();
        }

        dbDir = new File(new File(context.stateDir(), parentDir), name);

        try {
            Files.createDirectories(dbDir.getParentFile().toPath());
            Files.createDirectories(dbDir.getAbsoluteFile().toPath());
        } catch (final IOException fatal) {
            throw new ProcessorStateException(fatal);
        }

        openRocksDB(dbOptions, columnFamilyOptions);
        open = true;
    }

    void openRocksDB(final DBOptions dbOptions,
                     final ColumnFamilyOptions columnFamilyOptions) {
        final List<ColumnFamilyDescriptor> columnFamilyDescriptors
            = Collections.singletonList(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions));
        final List<ColumnFamilyHandle> columnFamilies = new ArrayList<>(columnFamilyDescriptors.size());

        try {
            db = RocksDB.open(dbOptions, dbDir.getAbsolutePath(), columnFamilyDescriptors, columnFamilies);
            dbAccessor = new SingleColumnFamilyAccessor(columnFamilies.get(0));
        } catch (final RocksDBException e) {
            throw new ProcessorStateException("Error opening store " + name + " at location " + dbDir.toString(), e);
        }
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

    private void validateStoreOpen() {
        if (!open) {
            throw new InvalidStateStoreException("Store " + name + " is currently closed");
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized void put(final Bytes key,
                                 final byte[] value) {
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();
        dbAccessor.put(key.get(), value);
    }

    @Override
    public synchronized byte[] putIfAbsent(final Bytes key,
                                           final byte[] value) {
        Objects.requireNonNull(key, "key cannot be null");
        final byte[] originalValue = get(key);
        if (originalValue == null) {
            put(key, value);
        }
        return originalValue;
    }

    @Override
    public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
        try (final WriteBatch batch = new WriteBatch()) {
            dbAccessor.prepareBatch(entries, batch);
            write(batch);
        } catch (final RocksDBException e) {
            throw new ProcessorStateException("Error while batch writing to store " + name, e);
        }
    }

    @Override
    public synchronized byte[] get(final Bytes key) {
        validateStoreOpen();
        try {
            return dbAccessor.get(key.get());
        } catch (final RocksDBException e) {
            // String format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
            throw new ProcessorStateException("Error while getting value for key from store " + name, e);
        }
    }

    @Override
    public synchronized byte[] delete(final Bytes key) {
        Objects.requireNonNull(key, "key cannot be null");
        final byte[] oldValue;
        try {
            oldValue = dbAccessor.getOnly(key.get());
        } catch (final RocksDBException e) {
            // String format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
            throw new ProcessorStateException("Error while getting value for key from store " + name, e);
        }
        put(key, null);
        return oldValue;
    }

    @Override
    public synchronized KeyValueIterator<Bytes, byte[]> range(final Bytes from,
                                                              final Bytes to) {
        Objects.requireNonNull(from, "from cannot be null");
        Objects.requireNonNull(to, "to cannot be null");

        if (from.compareTo(to) > 0) {
            log.warn("Returning empty iterator for fetch with invalid key range: from > to. "
                + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                "Note that the built-in numerical serdes do not follow this for negative numbers");
            return KeyValueIterators.emptyIterator();
        }

        validateStoreOpen();

        final KeyValueIterator<Bytes, byte[]> rocksDBRangeIterator = dbAccessor.range(from, to);
        openIterators.add(rocksDBRangeIterator);

        return rocksDBRangeIterator;
    }

    @Override
    public synchronized KeyValueIterator<Bytes, byte[]> all() {
        validateStoreOpen();
        final KeyValueIterator<Bytes, byte[]> rocksDbIterator = dbAccessor.all();
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
        final long numEntries;
        try {
            numEntries = dbAccessor.approximateNumEntries();
        } catch (final RocksDBException e) {
            throw new ProcessorStateException("Error fetching property from store " + name, e);
        }
        if (isOverflowing(numEntries)) {
            return Long.MAX_VALUE;
        }
        return numEntries;
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
        try {
            dbAccessor.flush();
        } catch (final RocksDBException e) {
            throw new ProcessorStateException("Error while executing flush from store " + name, e);
        }
    }

    @Override
    public void toggleDbForBulkLoading(final boolean prepareForBulkload) {
        if (prepareForBulkload) {
            // if the store is not empty, we need to compact to get around the num.levels check for bulk loading
            final String[] sstFileNames = dbDir.list((dir, name) -> SST_FILE_EXTENSION.matcher(name).matches());

            if (sstFileNames != null && sstFileNames.length > 0) {
                dbAccessor.toggleDbForBulkLoading();
            }
        }

        close();
        this.prepareForBulkload = prepareForBulkload;
        openDB(internalProcessorContext);
    }

    @Override
    public void addToBatch(final KeyValue<byte[], byte[]> record,
                           final WriteBatch batch) throws RocksDBException {
        dbAccessor.addToBatch(record.key, record.value, batch);
    }

    @Override
    public void write(final WriteBatch batch) throws RocksDBException {
        db.write(wOptions, batch);
    }

    @Override
    public synchronized void close() {
        if (!open) {
            return;
        }

        open = false;
        closeOpenIterators();

        if (configSetter != null) {
            configSetter.close(name, userSpecifiedOptions);
            configSetter = null;
        }

        dbAccessor.close();
        userSpecifiedOptions.close();
        wOptions.close();
        fOptions.close();
        db.close();
        filter.close();
        cache.close();

        dbAccessor = null;
        userSpecifiedOptions = null;
        wOptions = null;
        fOptions = null;
        db = null;
        filter = null;
        cache = null;
    }

    private void closeOpenIterators() {
        final HashSet<KeyValueIterator<Bytes, byte[]>> iterators;
        synchronized (openIterators) {
            iterators = new HashSet<>(openIterators);
        }
        if (iterators.size() != 0) {
            log.warn("Closing {} open iterators for store {}", iterators.size(), name);
            for (final KeyValueIterator<Bytes, byte[]> iterator : iterators) {
                iterator.close();
            }
        }
    }



    interface RocksDBAccessor {

        void put(final byte[] key,
                 final byte[] value);

        void prepareBatch(final List<KeyValue<Bytes, byte[]>> entries,
                          final WriteBatch batch) throws RocksDBException;

        byte[] get(final byte[] key) throws RocksDBException;

        /**
         * In contrast to get(), we don't migrate the key to new CF.
         * <p>
         * Use for get() within delete() -- no need to migrate, as it's deleted anyway
         */
        byte[] getOnly(final byte[] key) throws RocksDBException;

        KeyValueIterator<Bytes, byte[]> range(final Bytes from,
                                              final Bytes to);

        KeyValueIterator<Bytes, byte[]> all();

        long approximateNumEntries() throws RocksDBException;

        void flush() throws RocksDBException;

        void prepareBatchForRestore(final Collection<KeyValue<byte[], byte[]>> records,
                                    final WriteBatch batch) throws RocksDBException;

        void addToBatch(final byte[] key,
                        final byte[] value,
                        final WriteBatch batch) throws RocksDBException;

        void close();

        void toggleDbForBulkLoading();
    }

    class SingleColumnFamilyAccessor implements RocksDBAccessor {
        private final ColumnFamilyHandle columnFamily;

        SingleColumnFamilyAccessor(final ColumnFamilyHandle columnFamily) {
            this.columnFamily = columnFamily;
        }

        @Override
        public void put(final byte[] key,
                        final byte[] value) {
            if (value == null) {
                try {
                    db.delete(columnFamily, wOptions, key);
                } catch (final RocksDBException e) {
                    // String format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                    throw new ProcessorStateException("Error while removing key from store " + name, e);
                }
            } else {
                try {
                    db.put(columnFamily, wOptions, key, value);
                } catch (final RocksDBException e) {
                    // String format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                    throw new ProcessorStateException("Error while putting key/value into store " + name, e);
                }
            }
        }

        @Override
        public void prepareBatch(final List<KeyValue<Bytes, byte[]>> entries,
                                 final WriteBatch batch) throws RocksDBException {
            for (final KeyValue<Bytes, byte[]> entry : entries) {
                Objects.requireNonNull(entry.key, "key cannot be null");
                addToBatch(entry.key.get(), entry.value, batch);
            }
        }

        @Override
        public byte[] get(final byte[] key) throws RocksDBException {
            return db.get(columnFamily, key);
        }

        @Override
        public byte[] getOnly(final byte[] key) throws RocksDBException {
            return db.get(columnFamily, key);
        }

        @Override
        public KeyValueIterator<Bytes, byte[]> range(final Bytes from,
                                                     final Bytes to) {
            return new RocksDBRangeIterator(
                name,
                db.newIterator(columnFamily),
                openIterators,
                from,
                to);
        }

        @Override
        public KeyValueIterator<Bytes, byte[]> all() {
            final RocksIterator innerIterWithTimestamp = db.newIterator(columnFamily);
            innerIterWithTimestamp.seekToFirst();
            return new RocksDbIterator(name, innerIterWithTimestamp, openIterators);
        }

        @Override
        public long approximateNumEntries() throws RocksDBException {
            return db.getLongProperty(columnFamily, "rocksdb.estimate-num-keys");
        }

        @Override
        public void flush() throws RocksDBException {
            db.flush(fOptions, columnFamily);
        }

        @Override
        public void prepareBatchForRestore(final Collection<KeyValue<byte[], byte[]>> records,
                                           final WriteBatch batch) throws RocksDBException {
            for (final KeyValue<byte[], byte[]> record : records) {
                addToBatch(record.key, record.value, batch);
            }
        }

        @Override
        public void addToBatch(final byte[] key,
                               final byte[] value,
                               final WriteBatch batch) throws RocksDBException {
            if (value == null) {
                batch.delete(columnFamily, key);
            } else {
                batch.put(columnFamily, key, value);
            }
        }

        @Override
        public void close() {
            columnFamily.close();
        }

        @Override
        @SuppressWarnings("deprecation")
        public void toggleDbForBulkLoading() {
            try {
                db.compactRange(columnFamily, true, 1, 0);
            } catch (final RocksDBException e) {
                throw new ProcessorStateException("Error while range compacting during restoring  store " + name, e);
            }
        }
    }

    // not private for testing
    static class RocksDBBatchingRestoreCallback extends AbstractNotifyingBatchingRestoreCallback {

        private final RocksDBStore rocksDBStore;

        RocksDBBatchingRestoreCallback(final RocksDBStore rocksDBStore) {
            this.rocksDBStore = rocksDBStore;
        }

        @Override
        public void restoreAll(final Collection<KeyValue<byte[], byte[]>> records) {
            try (final WriteBatch batch = new WriteBatch()) {
                rocksDBStore.dbAccessor.prepareBatchForRestore(records, batch);
                rocksDBStore.write(batch);
            } catch (final RocksDBException e) {
                throw new ProcessorStateException("Error restoring batch to store " + rocksDBStore.name, e);
            }
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
