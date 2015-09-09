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

package org.apache.kafka.streams.state;

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.SystemTime;

import org.apache.kafka.common.utils.Time;
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

public class RocksDBKeyValueStore extends MeteredKeyValueStore<byte[], byte[]> {

    public RocksDBKeyValueStore(String name, ProcessorContext context) {
        this(name, context, new SystemTime());
    }

    public RocksDBKeyValueStore(String name, ProcessorContext context, Time time) {
        super(name, new RocksDBStore(name, context), context, "kafka-streams", time);
    }

    private static class RocksDBStore implements KeyValueStore<byte[], byte[]> {

        private static final int TTL_NOT_USED = -1;

        // TODO: these values should be configurable
        private static final long WRITE_BUFFER_SIZE = 32 * 1024 * 1024L;
        private static final long BLOCK_CACHE_SIZE = 100 * 1024 * 1024L;
        private static final long BLOCK_SIZE = 4096L;
        private static final int TTL_SECONDS = TTL_NOT_USED;
        private static final int MAX_WRITE_BUFFERS = 3;
        private static final CompressionType COMPRESSION_TYPE = CompressionType.NO_COMPRESSION;
        private static final CompactionStyle COMPACTION_STYLE = CompactionStyle.UNIVERSAL;
        private static final String DB_FILE_DIR = "rocksdb";

        private final String topic;
        private final int partition;
        private final ProcessorContext context;

        private final Options options;
        private final WriteOptions wOptions;
        private final FlushOptions fOptions;

        private final String dbName;
        private final String dirName;

        private RocksDB db;

        @SuppressWarnings("unchecked")
        public RocksDBStore(String name, ProcessorContext context) {
            this.topic = name;
            this.partition = context.id();
            this.context = context;

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

            dbName = this.topic + "." + this.partition;
            dirName = this.context.stateDir() + File.separator + DB_FILE_DIR;

            db = openDB(new File(dirName, dbName), this.options, TTL_SECONDS);
        }

        private RocksDB openDB(File dir, Options options, int ttl) {
            try {
                if (ttl == TTL_NOT_USED) {
                    return RocksDB.open(options, dir.toString());
                } else {
                    throw new KafkaException("Change log is not supported for store " + this.topic + " since it is TTL based.");
                    // TODO: support TTL with change log?
                    // return TtlDB.open(options, dir.toString(), ttl, false);
                }
            } catch (RocksDBException e) {
                // TODO: this needs to be handled more accurately
                throw new KafkaException("Error opening store " + this.topic + " at location " + dir.toString(), e);
            }
        }

        @Override
        public String name() {
            return this.topic;
        }

        @Override
        public boolean persistent() {
            return false;
        }

        @Override
        public byte[] get(byte[] key) {
            try {
                return this.db.get(key);
            } catch (RocksDBException e) {
                // TODO: this needs to be handled more accurately
                throw new KafkaException("Error while executing get " + key.toString() + " from store " + this.topic, e);
            }
        }

        @Override
        public void put(byte[] key, byte[] value) {
            try {
                if (value == null) {
                    db.remove(wOptions, key);
                } else {
                    db.put(wOptions, key, value);
                }
            } catch (RocksDBException e) {
                // TODO: this needs to be handled more accurately
                throw new KafkaException("Error while executing put " + key.toString() + " from store " + this.topic, e);
            }
        }

        @Override
        public void putAll(List<Entry<byte[], byte[]>> entries) {
            for (Entry<byte[], byte[]> entry : entries)
                put(entry.key(), entry.value());
        }

        @Override
        public void delete(byte[] key) {
            put(key, null);
        }

        @Override
        public KeyValueIterator<byte[], byte[]> range(byte[] from, byte[] to) {
            return new RocksDBRangeIterator(db.newIterator(), from, to);
        }

        @Override
        public KeyValueIterator<byte[], byte[]> all() {
            RocksIterator innerIter = db.newIterator();
            innerIter.seekToFirst();
            return new RocksDbIterator(innerIter);
        }

        @Override
        public void flush() {
            try {
                db.flush(fOptions);
            } catch (RocksDBException e) {
                // TODO: this needs to be handled more accurately
                throw new KafkaException("Error while executing flush from store " + this.topic, e);
            }
        }

        @Override
        public void close() {
            flush();
            db.close();
        }

        private static class RocksDbIterator implements KeyValueIterator<byte[], byte[]> {
            private final RocksIterator iter;

            public RocksDbIterator(RocksIterator iter) {
                this.iter = iter;
            }

            protected byte[] peekKey() {
                return this.getEntry().key();
            }

            protected Entry<byte[], byte[]> getEntry() {
                return new Entry<>(iter.key(), iter.value());
            }

            @Override
            public boolean hasNext() {
                return iter.isValid();
            }

            @Override
            public Entry<byte[], byte[]> next() {
                if (!hasNext())
                    throw new NoSuchElementException();

                Entry<byte[], byte[]> entry = this.getEntry();
                iter.next();

                return entry;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("RocksDB iterator does not support remove");
            }

            @Override
            public void close() {
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

        private static class RocksDBRangeIterator extends RocksDbIterator {
            // RocksDB's JNI interface does not expose getters/setters that allow the
            // comparator to be pluggable, and the default is lexicographic, so it's
            // safe to just force lexicographic comparator here for now.
            private final Comparator<byte[]> comparator = new LexicographicComparator();
            byte[] to;

            public RocksDBRangeIterator(RocksIterator iter, byte[] from, byte[] to) {
                super(iter);
                iter.seek(from);
                this.to = to;
            }

            @Override
            public boolean hasNext() {
                return super.hasNext() && comparator.compare(super.peekKey(), this.to) < 0;
            }
        }

    }
}
