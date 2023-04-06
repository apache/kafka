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
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.ChangelogRecordDeserializationHelper;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecorder;
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
import org.rocksdb.Statistics;
import org.rocksdb.TableFormatConfig;
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

import static org.apache.kafka.streams.StreamsConfig.InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED;
import static org.apache.kafka.streams.StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG;
import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.getMetricsImpl;

/**
 * A persistent key-value store based on RocksDB.
 */
public class RocksDBStore implements KeyValueStore<Bytes, byte[]>, BatchWritingStore {
    private static final Logger log = LoggerFactory.getLogger(RocksDBStore.class);

    private static final CompressionType COMPRESSION_TYPE = CompressionType.NO_COMPRESSION;
    private static final CompactionStyle COMPACTION_STYLE = CompactionStyle.UNIVERSAL;
    private static final long WRITE_BUFFER_SIZE = 16 * 1024 * 1024L;
    private static final long BLOCK_CACHE_SIZE = 50 * 1024 * 1024L;
    private static final long BLOCK_SIZE = 4096L;
    private static final int MAX_WRITE_BUFFERS = 3;
    static final String DB_FILE_DIR = "rocksdb";

    final String name;
    private final String parentDir;
    final Set<KeyValueIterator<Bytes, byte[]>> openIterators = Collections.synchronizedSet(new HashSet<>());
    private boolean consistencyEnabled = false;

    // VisibleForTesting
    protected File dbDir;
    RocksDB db;
    RocksDBAccessor dbAccessor;

    // the following option objects will be created in openDB and closed in the close() method
    private RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsAdapter userSpecifiedOptions;
    WriteOptions wOptions;
    FlushOptions fOptions;
    private Cache cache;
    private BloomFilter filter;
    private Statistics statistics;

    private RocksDBConfigSetter configSetter;
    private boolean userSpecifiedStatistics = false;

    private final RocksDBMetricsRecorder metricsRecorder;
    // if true, then open iterators (for range, prefix scan, and other operations) will be
    // managed automatically (by this store instance). if false, then these iterators must be
    // managed elsewhere (by the caller of those methods).
    private final boolean autoManagedIterators;

    protected volatile boolean open = false;
    protected StateStoreContext context;
    protected Position position;
    private OffsetCheckpoint positionCheckpoint;

    public RocksDBStore(final String name,
                        final String metricsScope) {
        this(name, DB_FILE_DIR, new RocksDBMetricsRecorder(metricsScope, name));
    }

    RocksDBStore(final String name,
                 final String parentDir,
                 final RocksDBMetricsRecorder metricsRecorder) {
        this(name, parentDir, metricsRecorder, true);
    }

    RocksDBStore(final String name,
                 final String parentDir,
                 final RocksDBMetricsRecorder metricsRecorder,
                 final boolean autoManagedIterators) {
        this.name = name;
        this.parentDir = parentDir;
        this.metricsRecorder = metricsRecorder;
        this.autoManagedIterators = autoManagedIterators;
    }

    @Deprecated
    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {
        if (context instanceof StateStoreContext) {
            init((StateStoreContext) context, root);
        } else {
            throw new UnsupportedOperationException(
                "Use RocksDBStore#init(StateStoreContext, StateStore) instead."
            );
        }
    }

    @Override
    public void init(final StateStoreContext context,
                     final StateStore root) {
        // open the DB dir
        metricsRecorder.init(getMetricsImpl(context), context.taskId());
        openDB(context.appConfigs(), context.stateDir());

        final File positionCheckpointFile = new File(context.stateDir(), name() + ".position");
        this.positionCheckpoint = new OffsetCheckpoint(positionCheckpointFile);
        this.position = StoreQueryUtils.readPositionFromCheckpoint(positionCheckpoint);

        // value getter should always read directly from rocksDB
        // since it is only for values that are already flushed
        this.context = context;
        context.register(
            root,
            (RecordBatchingStateRestoreCallback) this::restoreBatch,
            () -> StoreQueryUtils.checkpointPosition(positionCheckpoint, position)
        );
        consistencyEnabled = StreamsConfig.InternalConfig.getBoolean(
            context.appConfigs(),
            IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED,
            false);
    }

    @SuppressWarnings("unchecked")
    void openDB(final Map<String, Object> configs, final File stateDir) {
        // initialize the default rocksdb options

        final DBOptions dbOptions = new DBOptions();
        final ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
        userSpecifiedOptions = new RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsAdapter(dbOptions, columnFamilyOptions);

        final BlockBasedTableConfigWithAccessibleCache tableConfig = new BlockBasedTableConfigWithAccessibleCache();
        cache = new LRUCache(BLOCK_CACHE_SIZE);
        tableConfig.setBlockCache(cache);
        tableConfig.setBlockSize(BLOCK_SIZE);

        filter = new BloomFilter();
        tableConfig.setFilterPolicy(filter);

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
        // of compaction threads but not flush threads (the latter remains one). Also,
        // the parallelism value needs to be at least two because of the code in
        // https://github.com/facebook/rocksdb/blob/62ad0a9b19f0be4cefa70b6b32876e764b7f3c11/util/options.cc#L580
        // subtracts one from the value passed to determine the number of compaction threads
        // (this could be a bug in the RocksDB code and their devs have been contacted).
        userSpecifiedOptions.setIncreaseParallelism(Math.max(Runtime.getRuntime().availableProcessors(), 2));

        wOptions = new WriteOptions();
        wOptions.setDisableWAL(true);

        fOptions = new FlushOptions();
        fOptions.setWaitForFlush(true);

        final Class<RocksDBConfigSetter> configSetterClass =
                (Class<RocksDBConfigSetter>) configs.get(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG);

        if (configSetterClass != null) {
            configSetter = Utils.newInstance(configSetterClass);
            configSetter.setConfig(name, userSpecifiedOptions, configs);
        }

        dbDir = new File(new File(stateDir, parentDir), name);
        try {
            Files.createDirectories(dbDir.getParentFile().toPath());
            Files.createDirectories(dbDir.getAbsoluteFile().toPath());
        } catch (final IOException fatal) {
            throw new ProcessorStateException(fatal);
        }

        // Setup statistics before the database is opened, otherwise the statistics are not updated
        // with the measurements from Rocks DB
        setupStatistics(configs, dbOptions);
        openRocksDB(dbOptions, columnFamilyOptions);
        open = true;

        addValueProvidersToMetricsRecorder();
    }

    private void setupStatistics(final Map<String, Object> configs, final DBOptions dbOptions) {
        statistics = userSpecifiedOptions.statistics();
        if (statistics == null) {
            if (RecordingLevel.forName((String) configs.get(METRICS_RECORDING_LEVEL_CONFIG)) == RecordingLevel.DEBUG) {
                statistics = new Statistics();
                dbOptions.setStatistics(statistics);
            }
            userSpecifiedStatistics = false;
        } else {
            userSpecifiedStatistics = true;
        }
    }

    private void addValueProvidersToMetricsRecorder() {
        final TableFormatConfig tableFormatConfig = userSpecifiedOptions.tableFormatConfig();
        if (tableFormatConfig instanceof BlockBasedTableConfigWithAccessibleCache) {
            final Cache cache = ((BlockBasedTableConfigWithAccessibleCache) tableFormatConfig).blockCache();
            metricsRecorder.addValueProviders(name, db, cache, userSpecifiedStatistics ? null : statistics);
        } else if (tableFormatConfig instanceof BlockBasedTableConfig) {
            throw new ProcessorStateException("The used block-based table format configuration does not expose the " +
                    "block cache. Use the BlockBasedTableConfig instance provided by Options#tableFormatConfig() to configure " +
                    "the block-based table format of RocksDB. Do not provide a new instance of BlockBasedTableConfig to " +
                    "the RocksDB options.");
        } else {
            metricsRecorder.addValueProviders(name, db, null, userSpecifiedStatistics ? null : statistics);
        }
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

    @Override
    public synchronized void put(final Bytes key,
                                 final byte[] value) {
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();
        dbAccessor.put(key.get(), value);

        StoreQueryUtils.updatePosition(position, context);
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
            StoreQueryUtils.updatePosition(position, context);
        } catch (final RocksDBException e) {
            throw new ProcessorStateException("Error while batch writing to store " + name, e);
        }
    }

    @Override
    public <R> QueryResult<R> query(
        final Query<R> query,
        final PositionBound positionBound,
        final QueryConfig config) {

        return StoreQueryUtils.handleBasicQueries(
            query,
            positionBound,
            config,
            this,
            position,
            context
        );
    }

    @Override
    public <PS extends Serializer<P>, P> KeyValueIterator<Bytes, byte[]> prefixScan(final P prefix,
                                                                                    final PS prefixKeySerializer) {
        if (!autoManagedIterators) {
            throw new IllegalStateException("Must specify openIterators in call to prefixScan()");
        }
        return doPrefixScan(prefix, prefixKeySerializer, openIterators);
    }

    <PS extends Serializer<P>, P> KeyValueIterator<Bytes, byte[]> prefixScan(final P prefix,
                                                                             final PS prefixKeySerializer,
                                                                             final Set<KeyValueIterator<Bytes, byte[]>> openIterators) {
        if (autoManagedIterators) {
            throw new IllegalStateException("Cannot specify openIterators when using auto-managed iterators");
        }
        return doPrefixScan(prefix, prefixKeySerializer, openIterators);
    }

    <PS extends Serializer<P>, P> KeyValueIterator<Bytes, byte[]> doPrefixScan(final P prefix,
                                                                               final PS prefixKeySerializer,
                                                                               final Set<KeyValueIterator<Bytes, byte[]>> openIterators) {
        validateStoreOpen();
        Objects.requireNonNull(prefix, "prefix cannot be null");
        Objects.requireNonNull(prefixKeySerializer, "prefixKeySerializer cannot be null");
        final Bytes prefixBytes = Bytes.wrap(prefixKeySerializer.serialize(null, prefix));

        final ManagedKeyValueIterator<Bytes, byte[]> rocksDbPrefixSeekIterator = dbAccessor.prefixScan(prefixBytes);
        openIterators.add(rocksDbPrefixSeekIterator);
        rocksDbPrefixSeekIterator.onClose(() -> openIterators.remove(rocksDbPrefixSeekIterator));

        return rocksDbPrefixSeekIterator;
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

    void deleteRange(final Bytes keyFrom, final Bytes keyTo) {
        Objects.requireNonNull(keyFrom, "keyFrom cannot be null");
        Objects.requireNonNull(keyTo, "keyTo cannot be null");

        validateStoreOpen();

        // End of key is exclusive, so we increment it by 1 byte to make keyTo inclusive.
        // RocksDB's deleteRange() does not support a null upper bound so in the event
        // of overflow from increment(), the operation cannot be performed and an
        // IndexOutOfBoundsException will be thrown.
        dbAccessor.deleteRange(keyFrom.get(), Bytes.increment(keyTo).get());
    }

    @Override
    public synchronized KeyValueIterator<Bytes, byte[]> range(final Bytes from,
                                                              final Bytes to) {
        if (!autoManagedIterators) {
            throw new IllegalStateException("Must specify openIterators in call to range()");
        }
        return range(from, to, true, openIterators);
    }

    synchronized KeyValueIterator<Bytes, byte[]> range(final Bytes from,
                                                       final Bytes to,
                                                       final Set<KeyValueIterator<Bytes, byte[]>> openIterators) {
        if (autoManagedIterators) {
            throw new IllegalStateException("Cannot specify openIterators when using auto-managed iterators");
        }
        return range(from, to, true, openIterators);
    }

    @Override
    public synchronized KeyValueIterator<Bytes, byte[]> reverseRange(final Bytes from,
                                                                     final Bytes to) {
        if (!autoManagedIterators) {
            throw new IllegalStateException("Must specify openIterators in call to reverseRange()");
        }
        return range(from, to, false, openIterators);
    }

    synchronized KeyValueIterator<Bytes, byte[]> reverseRange(final Bytes from,
                                                              final Bytes to,
                                                              final Set<KeyValueIterator<Bytes, byte[]>> openIterators) {
        if (autoManagedIterators) {
            throw new IllegalStateException("Cannot specify openIterators when using auto-managed iterators");
        }
        return range(from, to, false, openIterators);
    }

    private KeyValueIterator<Bytes, byte[]> range(final Bytes from,
                                                  final Bytes to,
                                                  final boolean forward,
                                                  final Set<KeyValueIterator<Bytes, byte[]>> openIterators) {
        if (Objects.nonNull(from) && Objects.nonNull(to) && from.compareTo(to) > 0) {
            log.warn("Returning empty iterator for fetch with invalid key range: from > to. "
                    + "This may be due to range arguments set in the wrong order, " +
                    "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                    "Note that the built-in numerical serdes do not follow this for negative numbers");
            return KeyValueIterators.emptyIterator();
        }

        validateStoreOpen();

        final ManagedKeyValueIterator<Bytes, byte[]> rocksDBRangeIterator = dbAccessor.range(from, to, forward);
        openIterators.add(rocksDBRangeIterator);
        rocksDBRangeIterator.onClose(() -> openIterators.remove(rocksDBRangeIterator));

        return rocksDBRangeIterator;
    }

    @Override
    public synchronized KeyValueIterator<Bytes, byte[]> all() {
        if (!autoManagedIterators) {
            throw new IllegalStateException("Must specify openIterators in call to all()");
        }
        return all(true, openIterators);
    }

    synchronized KeyValueIterator<Bytes, byte[]> all(final Set<KeyValueIterator<Bytes, byte[]>> openIterators) {
        if (autoManagedIterators) {
            throw new IllegalStateException("Cannot specify openIterators when using auto-managed iterators");
        }
        return all(true, openIterators);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseAll() {
        if (!autoManagedIterators) {
            throw new IllegalStateException("Must specify openIterators in call to reverseAll()");
        }
        return all(false, openIterators);
    }

    KeyValueIterator<Bytes, byte[]> reverseAll(final Set<KeyValueIterator<Bytes, byte[]>> openIterators) {
        if (autoManagedIterators) {
            throw new IllegalStateException("Cannot specify openIterators when using auto-managed iterators");
        }
        return all(false, openIterators);
    }

    private KeyValueIterator<Bytes, byte[]> all(final boolean forward,
                                                final Set<KeyValueIterator<Bytes, byte[]>> openIterators) {
        validateStoreOpen();
        final ManagedKeyValueIterator<Bytes, byte[]> rocksDbIterator = dbAccessor.all(forward);
        openIterators.add(rocksDbIterator);
        rocksDbIterator.onClose(() -> openIterators.remove(rocksDbIterator));
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

        metricsRecorder.removeValueProviders(name);

        // Important: do not rearrange the order in which the below objects are closed!
        // Order of closing must follow: ColumnFamilyHandle > RocksDB > DBOptions > ColumnFamilyOptions
        dbAccessor.close();
        db.close();
        userSpecifiedOptions.close();
        wOptions.close();
        fOptions.close();
        filter.close();
        cache.close();
        if (statistics != null) {
            statistics.close();
        }

        dbAccessor = null;
        userSpecifiedOptions = null;
        wOptions = null;
        fOptions = null;
        db = null;
        filter = null;
        cache = null;
        statistics = null;
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

        ManagedKeyValueIterator<Bytes, byte[]> range(final Bytes from,
                                              final Bytes to,
                                              final boolean forward);

        /**
         * Deletes keys entries in the range ['from', 'to'], including 'from' and excluding 'to'.
         */
        void deleteRange(final byte[] from,
                         final byte[] to);

        ManagedKeyValueIterator<Bytes, byte[]> all(final boolean forward);

        ManagedKeyValueIterator<Bytes, byte[]> prefixScan(final Bytes prefix);

        long approximateNumEntries() throws RocksDBException;

        void flush() throws RocksDBException;

        void addToBatch(final byte[] key,
                        final byte[] value,
                        final WriteBatch batch) throws RocksDBException;

        void close();
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
        public ManagedKeyValueIterator<Bytes, byte[]> range(final Bytes from,
                                                     final Bytes to,
                                                     final boolean forward) {
            return new RocksDBRangeIterator(
                    name,
                    db.newIterator(columnFamily),
                    from,
                    to,
                    forward,
                    true
            );
        }

        @Override
        public void deleteRange(final byte[] from, final byte[] to) {
            try {
                db.deleteRange(columnFamily, wOptions, from, to);
            } catch (final RocksDBException e) {
                // String format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                throw new ProcessorStateException("Error while removing key from store " + name, e);
            }
        }

        @Override
        public ManagedKeyValueIterator<Bytes, byte[]> all(final boolean forward) {
            final RocksIterator innerIterWithTimestamp = db.newIterator(columnFamily);
            if (forward) {
                innerIterWithTimestamp.seekToFirst();
            } else {
                innerIterWithTimestamp.seekToLast();
            }
            return new RocksDbIterator(name, innerIterWithTimestamp, forward);
        }

        @Override
        public ManagedKeyValueIterator<Bytes, byte[]> prefixScan(final Bytes prefix) {
            final Bytes to = incrementWithoutOverflow(prefix);
            return new RocksDBRangeIterator(
                    name,
                    db.newIterator(columnFamily),
                    prefix,
                    to,
                    true,
                    false
            );
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
    }

    void restoreBatch(final Collection<ConsumerRecord<byte[], byte[]>> records) {
        try (final WriteBatch batch = new WriteBatch()) {
            for (final ConsumerRecord<byte[], byte[]> record : records) {
                ChangelogRecordDeserializationHelper.applyChecksAndUpdatePosition(
                    record,
                    consistencyEnabled,
                    position
                );
                // If version headers are not present or version is V0
                dbAccessor.addToBatch(record.key(), record.value(), batch);
            }
            write(batch);
        } catch (final RocksDBException e) {
            throw new ProcessorStateException("Error restoring batch to store " + name, e);
        }

    }

    // for testing
    public Options getOptions() {
        return userSpecifiedOptions;
    }

    @Override
    public Position getPosition() {
        return position;
    }

    /**
     * Same as {@link Bytes#increment(Bytes)} but {@code null} is returned instead of throwing
     * {@code IndexOutOfBoundsException} in the event of overflow.
     *
     * @param input bytes to increment
     * @return A new copy of the incremented byte array, or {@code null} if incrementing would
     *         result in overflow.
     */
    static Bytes incrementWithoutOverflow(final Bytes input) {
        try {
            return Bytes.increment(input);
        } catch (final IndexOutOfBoundsException e) {
            return null;
        }
    }
}