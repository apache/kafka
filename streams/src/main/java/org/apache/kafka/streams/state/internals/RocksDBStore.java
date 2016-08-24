/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.CacheFlushListener;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.internals.MemoryLRUCacheBytes.MemoryLRUCacheBytesIterator;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * A persistent key-value store based on RocksDB.
 *
 * Note that the use of array-typed keys is discouraged because they result in incorrect caching behavior.
 * If you intend to work on byte arrays as key, for example, you may want to wrap them with the {@code Bytes} class,
 * i.e. use {@code RocksDBStore<Bytes, ...>} rather than {@code RocksDBStore<byte[], ...>}.
 *
 * @param <K> The key type
 * @param <V> The value type
 *
 * @see org.apache.kafka.streams.state.Stores#create(String)
 */
public class RocksDBStore<K, V> implements KeyValueStore<K, V> {

    private static final int TTL_NOT_USED = -1;

    // TODO: these values should be configurable
    private static final CompressionType COMPRESSION_TYPE = CompressionType.NO_COMPRESSION;
    private static final CompactionStyle COMPACTION_STYLE = CompactionStyle.UNIVERSAL;
    private static final long WRITE_BUFFER_SIZE = 32 * 1024 * 1024L;
    private static final long BLOCK_CACHE_SIZE = 100 * 1024 * 1024L;
    private static final long BLOCK_SIZE = 4096L;
    private static final int TTL_SECONDS = TTL_NOT_USED;
    private static final int MAX_WRITE_BUFFERS = 3;
    private static final String DB_FILE_DIR = "rocksdb";

    private final String name;
    private final String parentDir;

    protected File dbDir;
    private StateSerdes<K, V> serdes;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final CacheFlushListener<K, V> cacheFlushListener;

    private RocksDB db;

    // the following option objects will be created in the constructor and closed in the close() method
    private Options options;
    private WriteOptions wOptions;
    private FlushOptions fOptions;

    private boolean loggingEnabled = false;

    private LinkedHashMap<K, K> cacheDirtyKeys = new LinkedHashMap<>();
    private boolean cachingEnabled = false;
    private MemoryLRUCacheBytes cache = null;
    private StoreChangeLogger<Bytes, byte[]> changeLogger;
    private StoreChangeLogger.ValueGetter<Bytes, byte[]> getter;

    private volatile boolean open = false;
    private boolean sendOldValues = false;
    private InternalProcessorContext context;

    public KeyValueStore<K, V> enableLogging() {
        loggingEnabled = true;

        return this;
    }

    public RocksDBStore<K, V> enableCaching() {
        cachingEnabled = true;

        return this;
    }

    public RocksDBStore(String name, Serde<K> keySerde, Serde<V> valueSerde, CacheFlushListener<K, V> cacheFlushListeners) {
        this(name, DB_FILE_DIR, keySerde, valueSerde, cacheFlushListeners);
    }


    public RocksDBStore(String name, String parentDir, Serde<K> keySerde, Serde<V> valueSerde, CacheFlushListener<K, V> cacheFlushListeners) {
        this.name = name;
        this.parentDir = parentDir;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.cacheFlushListener = cacheFlushListeners;

        // initialize the default rocksdb options
        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
        tableConfig.setBlockCacheSize(BLOCK_CACHE_SIZE);
        tableConfig.setBlockSize(BLOCK_SIZE);

        options = new Options();
        options.setTableFormatConfig(tableConfig);
        options.setWriteBufferSize(WRITE_BUFFER_SIZE);
        options.setCompressionType(COMPRESSION_TYPE);
        options.setCompactionStyle(COMPACTION_STYLE);
        options.setMaxWriteBufferNumber(MAX_WRITE_BUFFERS);
        options.setCreateIfMissing(true);
        options.setErrorIfExists(false);

        wOptions = new WriteOptions();
        wOptions.setDisableWAL(true);

        fOptions = new FlushOptions();
        fOptions.setWaitForFlush(true);
    }

    @SuppressWarnings("unchecked")
    public void openDB(ProcessorContext context) {
        final Map<String, Object> configs = context.appConfigs();
        final Class<RocksDBConfigSetter> configSetterClass = (Class<RocksDBConfigSetter>) configs.get(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG);
        if (configSetterClass != null) {
            final RocksDBConfigSetter configSetter = Utils.newInstance(configSetterClass);
            configSetter.setConfig(name, options, configs);
        }
        // we need to construct the serde while opening DB since
        // it is also triggered by windowed DB segments without initialization
        this.serdes = new StateSerdes<>(name,
                keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
                valueSerde == null ? (Serde<V>) context.valueSerde() : valueSerde);

        // we need to check if cache is created (although there is already a check in init())
        // since windowed DB segments do not actually call init()
        initCaching(context);

        this.dbDir = new File(new File(context.stateDir(), parentDir), this.name);
        this.db = openDB(this.dbDir, this.options, TTL_SECONDS);
        open = true;
        this.context = (InternalProcessorContext) context;
    }

    public void init(ProcessorContext context, StateStore root) {
        // open the DB dir
        openDB(context);

        this.changeLogger = this.loggingEnabled ? new StoreChangeLogger<>(name, context, WindowStoreUtils.INNER_SERDES) : null;
        initCaching(context);
        // value getter should always read directly from rocksDB
        // since it is only for values that are already flushed
        this.getter = new StoreChangeLogger.ValueGetter<Bytes, byte[]>() {
            @Override
            public byte[] get(Bytes key) {
                return getInternal(key.get());
            }
        };

        context.register(root, loggingEnabled, new StateRestoreCallback() {

            @Override
            public void restore(byte[] key, byte[] value) {
                putInternal(key, value);
            }
        });
    }

    /**
     * Initialize caching. This method may be called multiple times per RocksDb instance, e.g.,
     * from the init() and openDB() methods
     * @param context
     */
    private void initCaching(ProcessorContext context) {
        if (this.cachingEnabled) {
            if (this.cache == null) {
                this.cache = context.getCache();
                if (this.cache == null) {
                    throw new ProcessorStateException("Error getting cache in store " + this.name);
                }
                this.cache.addEldestRemovedListener(new MemoryLRUCacheBytes.EldestEntryRemovalListener<byte[], MemoryLRUCacheBytesEntry>() {
                    @Override
                    public void apply(byte[] key, MemoryLRUCacheBytesEntry entry) {
                        // flush all the dirty entries to RocksDB if this evicted entry is dirty
                        if (entry.isDirty()) {
                            flushCache();
                        }
                    }
                });
            }
        }  else {
            this.cache = null;
            this.cacheDirtyKeys = null;
        }
    }

    private RocksDB openDB(File dir, Options options, int ttl) {
        try {
            if (ttl == TTL_NOT_USED) {
                dir.getParentFile().mkdirs();
                return RocksDB.open(options, dir.getAbsolutePath());
            } else {
                throw new UnsupportedOperationException("Change log is not supported for store " + this.name + " since it is TTL based.");
                // TODO: support TTL with change log?
                // return TtlDB.open(options, dir.toString(), ttl, false);
            }
        } catch (RocksDBException e) {
            throw new ProcessorStateException("Error opening store " + this.name + " at location " + dir.toString(), e);
        }
    }

    @Override
    public String name() {
        return this.name;
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
    public void enableSendingOldValues() {
        this.sendOldValues = true;
    }

    @Override
    public synchronized V get(K key) {
        validateStoreOpen();
        if (cache != null) {
            byte[] cacheKey = serdes.rawMergeStoreNameKey(key, name);
            MemoryLRUCacheBytesEntry<byte[], byte[]> entry = cache.get(cacheKey);
            if (entry == null) {
                byte[] byteValue = getInternal(serdes.rawKey(key));
                //Check value for null, to avoid  deserialization error
                if (byteValue == null) {
                    return null;
                } else {
                    V value = serdes.valueFrom(byteValue);
                    long length = byteValue != null ? byteValue.length : 0;
                    cache.put(cacheKey, new MemoryLRUCacheBytesEntry(cacheKey, byteValue, length));
                    return value;
                }
            } else {
                if (entry.value == null) {
                    return null;
                } else {
                    return serdes.valueFrom(entry.value);
                }
            }
        } else {
            byte[] byteValue = getInternal(serdes.rawKey(key));
            if (byteValue == null) {
                return null;
            } else {
                return serdes.valueFrom(byteValue);
            }
        }

    }

    private void validateStoreOpen() {
        if (!open) {
            throw new InvalidStateStoreException("Store " + this.name + " is currently closed");
        }
    }

    private byte[] getInternal(byte[] rawKey) {
        try {
            return this.db.get(rawKey);
        } catch (RocksDBException e) {
            throw new ProcessorStateException("Error while getting value for key " + serdes.keyFrom(rawKey) +
                    " from store " + this.name, e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized void put(K key, V value) {
        validateStoreOpen();
        byte[] rawKey = serdes.rawKey(key);
        byte[] rawValue = serdes.rawValue(value);
        long length = rawValue != null ? rawValue.length : 0;
        if (cache != null) {
            byte[] cacheKey = serdes.rawMergeStoreNameKey(key, name);

            cacheDirtyKeys.put(key, key);
            cache.put(cacheKey, new MemoryLRUCacheBytesEntry(cacheKey, rawValue, length, true, context.offset(),
                context.timestamp(), context.partition(), context.topic()));
        } else {
            putInternal(rawKey, rawValue);

            if (loggingEnabled) {
                changeLogger.add(Bytes.wrap(rawKey));
                changeLogger.maybeLogChange(this.getter);
            }
        }

    }

    @SuppressWarnings("unchecked")
    synchronized void writeToStore(K key, V value) {
//        cache.put(Bytes.wrap(serdes.rawMergeStoreNameKey(key, name)), new MemoryLRUCacheBytesEntry(value));
        putInternal(serdes.rawKey(key), serdes.rawValue(value));
    }

    @Override
    public synchronized V putIfAbsent(K key, V value) {
        V originalValue = get(key);
        if (originalValue == null) {
            put(key, value);
        }
        return originalValue;
    }

    private void putInternal(byte[] rawKey, byte[] rawValue) {
        if (rawValue == null) {
            try {
                db.remove(wOptions, rawKey);
            } catch (RocksDBException e) {
                throw new ProcessorStateException("Error while removing key " + serdes.keyFrom(rawKey) +
                        " from store " + this.name, e);
            }
        } else {
            try {
                db.put(wOptions, rawKey, rawValue);
            } catch (RocksDBException e) {
                throw new ProcessorStateException("Error while executing put key " + serdes.keyFrom(rawKey) +
                        " and value " + serdes.keyFrom(rawValue) + " from store " + this.name, e);
            }
        }
    }

    @Override
    public void putAll(List<KeyValue<K, V>> entries) {
        for (KeyValue<K, V> entry : entries)
            put(entry.key, entry.value);
    }

    // this function is only called in flushCache()
    private void putAllInternal(List<KeyValue<byte[], byte[]>> entries) {
        try (WriteBatch batch = new WriteBatch()) {
            for (KeyValue<byte[], byte[]> entry : entries) {
                batch.put(entry.key, entry.value);
            }

            db.write(wOptions, batch);
        } catch (RocksDBException e) {
            throw new ProcessorStateException("Error while batch writing to store " + this.name, e);
        }
    }

    @Override
    public synchronized V delete(K key) {
        V value = get(key);
        put(key, null);
        return value;
    }

    @Override
    public synchronized KeyValueIterator<K, V> range(K from, K to) {
        validateStoreOpen();

        // query rocksdb
        RocksDbIterator rocksDbIter = new RocksDBRangeIterator<>(db.newIterator(), serdes, from, to);

        // query cache
        if (cache == null) {
            return rocksDbIter;
        }

        MemoryLRUCacheBytesIterator cacheIter = cache.range(serdes.rawMergeStoreNameKey(from, name),
            serdes.rawMergeStoreNameKey(to, name));


        // merge results
        return new MergedSortedCacheRocksDBIterator<>(this, cacheIter, rocksDbIter);
    }

    /**
     * Merges two iterators. Assumes each of them is sorted by key
     * @param <K>
     * @param <V>
     */
    private static class MergedSortedCacheRocksDBIterator<K, V> implements KeyValueIterator<K, V> {
        private final MemoryLRUCacheBytesIterator cacheIter;
        private KeyValue<K, V> lastEntryCache = null;
        private KeyValue<K, V> cacheNext = null;

        private final RocksDbIterator<K, V> rocksDbIter;
        private KeyValue<K, V> lastEntryRocksDb = null;
        private final RocksDBStore store;
        private final Comparator<byte[]> comparator = Bytes.BYTES_LEXICO_COMPARATOR;
        private KeyValue<K, V> returnValue = null;

        public MergedSortedCacheRocksDBIterator(RocksDBStore store, MemoryLRUCacheBytesIterator cacheIter, RocksDbIterator<K, V> rocksDbIter) {
            this.cacheIter = cacheIter;
            this.rocksDbIter = rocksDbIter;
            this.store = store;
        }

        /**
         * like hasNext(), but for elements of this store only
         * @return
         */
        private boolean cacheHasNextThisStore() {
            cacheGetNextThisStore(false);
            return cacheNext != null;
        }

        @Override
        public boolean hasNext() {
            return rocksDbIter.hasNext() || cacheHasNextThisStore();
        }

        /**
         * Get in advance the next element that belongs to this store
         */
        private void updateCacheNext() {
            boolean found = false;
            KeyValue<K, V> entry = null;
            // skip cache entries that do not belong to our store
            try {
                entry = (KeyValue<K, V>) cacheIter.next();
                while (entry != null) {
                    K originalKey = (K) store.serdes.keyFromRawMergedStoreNameKey((byte[]) entry.key, store.name());
                    String embeddedStoreName = store.serdes.storeNameFromRawMergedStoreNameKey((byte[]) entry.key, originalKey);
                    if (store.name().equals(embeddedStoreName)) {
                        found = true;
                        break;
                    } else {
                        entry = (KeyValue<K, V>) cacheIter.next();
                    }
                }
            } catch (java.util.NoSuchElementException e) {
                found = false;
            }
            if (!found) {
                entry = null;
            }

            cacheNext = entry;
        }

        /**
         * Like next(), but for elements of this store only
         * @param advance
         * @return
         */
        private KeyValue<K, V> cacheGetNextThisStore(boolean advance) {
            KeyValue<K, V> entry = null;

            if (cacheNext == null) {
                updateCacheNext();
            }
            entry = cacheNext;

            if (advance) {
                updateCacheNext();
            }

            return entry;
        }


        @Override
        public KeyValue<K, V> next() {
            if (!cacheHasNextThisStore()) {
                return rocksDbIter.next();
            }

            if (!rocksDbIter.hasNext()) {

                lastEntryCache = cacheGetNextThisStore(true);
                if (lastEntryCache == null) {
                    throw new ProcessorStateException("Cache indicated that it had entry, but it does not");
                }
                K originalKey = (K) store.serdes.keyFromRawMergedStoreNameKey((byte[]) lastEntryCache.key, store.name());
                return new KeyValue<>(originalKey, (V) store.serdes.valueFrom((byte[]) lastEntryCache.value));
            }

            // convert the RocksDb key to a key recognized by the cache
            // TODO: serdes back and forth is inneficient
            byte[] storeKeyToCacheKey = store.serdes.rawMergeStoreNameKey(store.serdes.keyFrom(rocksDbIter.peekRawKey()), store.name());

            if (lastEntryCache == null) {
                lastEntryCache = cacheGetNextThisStore(true);

                if (lastEntryCache == null) {
                    return this.next();
                }
            }
            if (lastEntryRocksDb == null)
                lastEntryRocksDb = rocksDbIter.next();

            // element is in the cache but not in RocksDB. This can be if an item is not flushed yet
            if (comparator.compare((byte[]) lastEntryCache.key, storeKeyToCacheKey) <= 0) {
                K originalKey = (K) store.serdes.keyFromRawMergedStoreNameKey((byte[]) lastEntryCache.key, store.name());
                returnValue = new KeyValue<>(originalKey, (V) store.serdes.valueFrom((byte[]) lastEntryCache.value));
            } else {
                // element is in rocksDb, return it but do not advance the cache element
                returnValue = lastEntryRocksDb;
                lastEntryRocksDb = null;
            }

            // iterator bookeepeing
            if (comparator.compare((byte[]) lastEntryCache.key, storeKeyToCacheKey) < 0) {
                // advance cache iterator, but don't advance RocksDb iterator
                lastEntryCache = null;
            } else if (comparator.compare((byte[]) lastEntryCache.key, storeKeyToCacheKey) == 0) {
                // advance both iterators since the RocksDb balue is superceded by the cache value
                lastEntryCache = null;
                lastEntryRocksDb = null;
            }

            return returnValue;
        }

        @Override
        public void remove() {
            // do nothing
        }

        @Override
        public void close() {
            cacheIter.close();
            rocksDbIter.close();
        }
    }

    @Override
    public synchronized KeyValueIterator<K, V> all() {
        validateStoreOpen();

        // query rocksdb
        RocksIterator innerIter = db.newIterator();
        innerIter.seekToFirst();
        RocksDbIterator rocksDbIter = new RocksDbIterator<>(innerIter, serdes);

        if (cache == null) {
            return rocksDbIter;
        }

        // query cache
        MemoryLRUCacheBytesIterator cacheIter = cache.all();


        // merge results
        return new MergedSortedCacheRocksDBIterator<>(this, cacheIter, rocksDbIter);
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
        long value;
        try {
            value = this.db.getLongProperty("rocksdb.estimate-num-keys");
        } catch (RocksDBException e) {
            throw new ProcessorStateException("Error fetching property from store " + this.name, e);
        }
        if (isOverflowing(value)) {
            return Long.MAX_VALUE;
        }
        if (this.cacheDirtyKeys != null) {
            value += this.cacheDirtyKeys.size();
        }
        if (isOverflowing(value)) {
            return Long.MAX_VALUE;
        }
        return value;
    }

    private boolean isOverflowing(long value) {
        // RocksDB returns an unsigned 8-byte integer, which could overflow long
        // and manifest as a negative value.
        return value < 0;
    }

    public class FlushedEntry {
        MemoryLRUCacheBytesEntry<byte[], byte[]> entry;
        V oldValue;

        public FlushedEntry(final MemoryLRUCacheBytesEntry<byte[], byte[]> entry, final V oldValue) {
            this.entry = entry;
            this.oldValue = oldValue;
        }
    }

    private void flushCache() {
        if (cache != null) {
            List<KeyValue<byte[], byte[]>> putBatch = new ArrayList<>(cache.size());
            List<byte[]> deleteBatch = new ArrayList<>(cache.size());
            final Iterator<Map.Entry<K, K>> dirtyKeyIterator = cacheDirtyKeys.entrySet().iterator();
            final LinkedHashMap<K, FlushedEntry> toForward = new LinkedHashMap<>();

            while (dirtyKeyIterator.hasNext()) {
                final K key = dirtyKeyIterator.next().getKey();
                dirtyKeyIterator.remove();
                byte[] cacheKey = serdes.rawMergeStoreNameKey(key, name);
                MemoryLRUCacheBytesEntry<byte[], byte[]> entry = cache.get(cacheKey);

                if (entry != null) {
                    byte[] rawKey = serdes.rawKey(key);

                    final V oldValue = getValue(rawKey);
                    toForward.put(key, new FlushedEntry(entry, oldValue));

                    if (entry.isDirty()) {
                        if (entry.value != null) {
                            putBatch.add(new KeyValue<>(rawKey, entry.value));
                            entry.markClean();
                        } else {
                            deleteBatch.add(rawKey);
                            cache.delete(cacheKey);
                        }
                    }
                }
            }
            putAllInternal(putBatch);


            if (loggingEnabled) {
                for (KeyValue<byte[], byte[]> kv : putBatch)
                    changeLogger.add(Bytes.wrap(kv.key));
            }

            // check all removed entries and remove them in rocksDB
            // TODO: can this be done in batch as well?
            for (byte[] removedKey : deleteBatch) {
                try {
                    db.remove(wOptions, removedKey);
                } catch (RocksDBException e) {
                    throw new ProcessorStateException("Error while deleting with key " + serdes.keyFrom(removedKey) + " from store " + this.name, e);
                }

                if (loggingEnabled) {
                    changeLogger.delete(Bytes.wrap(removedKey));
                }
            }

            if (loggingEnabled)
                changeLogger.logChange(getter);

            for (Map.Entry<K, FlushedEntry> flush : toForward.entrySet()) {
                final FlushedEntry value = flush.getValue();
                if (sendOldValues) {
                    cacheFlushListener.flushed(flush.getKey(), new Change<>(serdes.valueFrom(value.entry.value), value.oldValue), value.entry, context);
                } else {
                    cacheFlushListener.flushed(flush.getKey(), new Change<>(serdes.valueFrom(value.entry.value), null), value.entry, context);
                }
            }
        }
    }



    private V getValue(final byte [] key) {
        final byte[] rawValue = getInternal(key);
        if (rawValue == null) {
            return null;
        }
        return serdes.valueFrom(rawValue);
    }


    @Override
    public synchronized void flush() {
        if (db == null) {
            return;
        }

        // flush of the cache entries if necessary
        flushCache();

        // flush RocksDB
        flushInternal();
    }
    /**
     * @throws ProcessorStateException if flushing failed because of any internal store exceptions
     */
    private void flushInternal() {
        try {
            db.flush(fOptions);
        } catch (RocksDBException e) {
            throw new ProcessorStateException("Error while executing flush from store " + this.name, e);
        }
    }

    @Override
    public synchronized void close() {
        if (!open) {
            return;
        }
        open = false;
        flush();
        options.close();
        wOptions.close();
        fOptions.close();
        db.close();

        options = null;
        wOptions = null;
        fOptions = null;
        db = null;
    }



    private static class RocksDbIterator<K, V> implements KeyValueIterator<K, V> {
        private final RocksIterator iter;
        private final StateSerdes<K, V> serdes;

        public RocksDbIterator(RocksIterator iter, StateSerdes<K, V> serdes) {
            this.iter = iter;
            this.serdes = serdes;
        }

        public byte[] peekRawKey() {
            return iter.key();
        }

        protected KeyValue<K, V> getKeyValue() {
            return new KeyValue<>(serdes.keyFrom(iter.key()), serdes.valueFrom(iter.value()));
        }

        @Override
        public boolean hasNext() {
            return iter.isValid();
        }

        /**
         * @throws NoSuchElementException if no next element exist
         */
        @Override
        public KeyValue<K, V> next() {
            if (!hasNext())
                throw new NoSuchElementException();

            KeyValue<K, V> entry = this.getKeyValue();
            iter.next();
            return entry;
        }

        /**
         * @throws UnsupportedOperationException
         */
        @Override
        public void remove() {
            throw new UnsupportedOperationException("RocksDB iterator does not support remove");
        }

        @Override
        public void close() {
            iter.close();
        }

    }

    private static class RocksDBRangeIterator<K, V> extends RocksDbIterator<K, V> {
        // RocksDB's JNI interface does not expose getters/setters that allow the
        // comparator to be pluggable, and the default is lexicographic, so it's
        // safe to just force lexicographic comparator here for now.
        private final Comparator<byte[]> comparator = Bytes.BYTES_LEXICO_COMPARATOR;
        private byte[] rawToKey;

        public RocksDBRangeIterator(RocksIterator iter, StateSerdes<K, V> serdes, K from, K to) {
            super(iter, serdes);
            iter.seek(serdes.rawKey(from));
            this.rawToKey = serdes.rawKey(to);
        }

        @Override
        public boolean hasNext() {
            return super.hasNext() && comparator.compare(super.peekRawKey(), this.rawToKey) <= 0;
        }
    }

}
