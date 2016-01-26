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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Serdes;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;

public class RocksDBStore<K, V> implements KeyValueStore<K, V> {

    private static final int TTL_NOT_USED = -1;

    private static final int DEFAULT_CACHE_SIZE = 1000;

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

    private final Options options;
    private final WriteOptions wOptions;
    private final FlushOptions fOptions;

    private ProcessorContext context;
    private Serdes<K, V> serdes;
    protected File dbDir;
    private RocksDB db;

    private int cacheSize = DEFAULT_CACHE_SIZE;
    private InMemoryLRUCacheStoreSupplier.MemoryLRUCache<K, V> cache;

    private boolean loggingEnabled = true;
    private StoreChangeLogger<byte[], byte[]> changeLogger = null;
    protected final StoreChangeLogger.ValueGetter<byte[], byte[]> getter;

    public RocksDBStore<K, V> disableLogging() {
        loggingEnabled = false;

        return this;
    }

    public RocksDBStore<K, V> withCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;

        return this;
    }

    public RocksDBStore(String name, Serdes<K, V> serdes) {
        this.name = name;
        this.serdes = serdes;

        // initialize the rocksdb options
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

        this.getter = new StoreChangeLogger.ValueGetter<byte[], byte[]>() {
            public byte[] get(byte[] key) {
                return inner.get(key);
            }
        };

    }

    private abstract class RocksDBStateRestoreCallback implements StateRestoreCallback {
        private String storeName;

        public RocksDBStateRestoreCallback(String storeName) {
            this.storeName = storeName;
        }
    }

    private abstract class RocksDBCacheRemovalListener implements InMemoryLRUCacheStoreSupplier.EldestEntryRemovalListener<K, V> {
        private String storeName;

        public RocksDBCacheRemovalListener(String storeName) {
            this.storeName = storeName;
        }
    }

    public void init(ProcessorContext context) {
        serdes.init(context);

        this.context = context;
        this.dbDir = new File(new File(this.context.stateDir(), DB_FILE_DIR), this.name);
        this.db = openDB(this.dbDir, this.options, TTL_SECONDS);

        this.changeLogger = this.loggingEnabled ? new StoreChangeLogger<>(name, context, Serdes.withBuiltinTypes("", byte[].class, byte[].class)) : null;

        this.cache = this.cacheSize > 0 ? new InMemoryLRUCacheStoreSupplier.MemoryLRUCache<K, V>(name, this.cacheSize)
                .whenEldestRemoved(new RocksDBCacheRemovalListener(name) {
                    @Override
                    public void apply(K key, V value) {
                        byte[] rawKey = serdes.rawKey(key);

                        // write the entry to the rocksDB file
                        // flush all the dirty entries if this evicted entry is dirty
                        if (changeLogger.isDirty(rawKey)) {
                            for (byte[] dirtyKey : changeLogger.dirtyKeys()) {

                            }
                        }

                        try {
                            putInternal(rawKey, serdes.rawValue(value));
                        } catch (RocksDBException e) {
                            // TODO: this needs to be handled more accurately
                            throw new KafkaException("Error while executing put key " + key.toString() +
                                    " and value " + value.toString() + " from store " + this.storeName, e);
                        }
                    }
                })
                : null;

        context.register(this, loggingEnabled, new RocksDBStateRestoreCallback(name) {

            @Override
            public void restore(byte[] key, byte[] value) {
                try {
                    putInternal(key, value);
                } catch (RocksDBException e) {
                    // TODO: this needs to be handled more accurately
                    throw new KafkaException("Error restoring store " + this.storeName, e);
                }
            }
        });
    }

    private RocksDB openDB(File dir, Options options, int ttl) {
        try {
            if (ttl == TTL_NOT_USED) {
                dir.getParentFile().mkdirs();
                return RocksDB.open(options, dir.toString());
            } else {
                throw new KafkaException("Change log is not supported for store " + this.name + " since it is TTL based.");
                // TODO: support TTL with change log?
                // return TtlDB.open(options, dir.toString(), ttl, false);
            }
        } catch (RocksDBException e) {
            // TODO: this needs to be handled more accurately
            throw new KafkaException("Error opening store " + this.name + " at location " + dir.toString(), e);
        }
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public V get(K key) {
        byte[] rawKey = serdes.rawKey(key);

        try {
            if (cache != null) {
                V value = cache.get(key);

                if (value == null) {
                    value = serdes.valueFrom(getInternal(rawKey));
                    cache.put(key, value);
                }

                return value;
            } else {
                return serdes.valueFrom(getInternal(rawKey));
            }
        } catch (RocksDBException e) {
            // TODO: this needs to be handled more accurately
            throw new KafkaException("Error while executing get for key " + key.toString() + " from store " + this.name, e);
        }
    }

    private byte[] getInternal(byte[] rawKey) throws RocksDBException {
        return this.db.get(rawKey);
    }

    @Override
    public void put(K key, V value) {
        if (cache != null) {
            cache.put(key, value);
        } else {
            byte[] rawKey = serdes.rawKey(key);
            byte[] rawValue = serdes.rawValue(value);

            try {
                putInternal(rawKey, rawValue);
            } catch (RocksDBException e) {
                // TODO: this needs to be handled more accurately
                throw new KafkaException("Error while executing put key " + key.toString() +
                        " and value " + value.toString() + " from store " + this.name, e);
            }

            if (loggingEnabled) {
                changeLogger.add(rawKey);
                changeLogger.maybeLogChange(this.getter);
            }
        }
    }

    private void putInternal(byte[] rawKey, byte[] rawValue) throws RocksDBException {
        if (rawValue == null) {
            db.remove(wOptions, rawKey);
        } else {
            db.put(wOptions, rawKey, rawValue);
        }

    }

    @Override
    public void putAll(List<KeyValue<K, V>> entries) {
        for (KeyValue<K, V> entry : entries)
            put(entry.key, entry.value);
    }

    @Override
    public V delete(K key) {
        V value = get(key);
        put(key, null);
        return value;
    }

    @Override
    public KeyValueIterator<K, V> range(K from, K to) {
        return new RocksDBRangeIterator<K, V>(db.newIterator(), serdes, from, to);
    }

    @Override
    public KeyValueIterator<K, V> all() {
        RocksIterator innerIter = db.newIterator();
        innerIter.seekToFirst();
        return new RocksDbIterator<K, V>(innerIter, serdes);
    }

    @Override
    public void flush() {
        try {
            db.flush(fOptions);
        } catch (RocksDBException e) {
            // TODO: this needs to be handled more accurately
            throw new KafkaException("Error while executing flush from store " + this.name, e);
        }

        if (loggingEnabled)
            changeLogger.logChange(this.getter);
    }

    @Override
    public void close() {
        flush();
        db.close();
    }

    private static class RocksDbIterator<K, V> implements KeyValueIterator<K, V> {
        private final RocksIterator iter;
        private final Serdes<K, V> serdes;

        public RocksDbIterator(RocksIterator iter, Serdes<K, V> serdes) {
            this.iter = iter;
            this.serdes = serdes;
        }

        protected byte[] peekRawKey() {
            return iter.key();
        }

        protected KeyValue<K, V> getKeyValue() {
            return new KeyValue<>(serdes.keyFrom(iter.key()), serdes.valueFrom(iter.value()));
        }

        @Override
        public boolean hasNext() {
            return iter.isValid();
        }

        @Override
        public KeyValue<K, V> next() {
            if (!hasNext())
                throw new NoSuchElementException();

            KeyValue<K, V> entry = this.getKeyValue();
            iter.next();
            return entry;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("RocksDB iterator does not support remove");
        }

        @Override
        public void close() {
            iter.dispose();
        }

    }

    private static class LexicographicComparator implements Comparator<byte[]> {

        @Override
        public int compare(byte[] left, byte[] right) {
            for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
                int leftByte = left[i] & 0xff;
                int rightByte = right[j] & 0xff;
                if (leftByte != rightByte) {
                    return leftByte - rightByte;
                }
            }
            return left.length - right.length;
        }
    }

    private static class RocksDBRangeIterator<K, V> extends RocksDbIterator<K, V> {
        // RocksDB's JNI interface does not expose getters/setters that allow the
        // comparator to be pluggable, and the default is lexicographic, so it's
        // safe to just force lexicographic comparator here for now.
        private final Comparator<byte[]> comparator = new LexicographicComparator();
        byte[] rawToKey;

        public RocksDBRangeIterator(RocksIterator iter, Serdes<K, V> serdes,
                                    K from, K to) {
            super(iter, serdes);
            iter.seek(serdes.rawKey(from));
            this.rawToKey = serdes.rawKey(to);
        }

        @Override
        public boolean hasNext() {
            return super.hasNext() && comparator.compare(super.peekRawKey(), this.rawToKey) < 0;
        }
    }

}
