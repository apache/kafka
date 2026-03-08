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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;

import org.rocksdb.WriteBatchInterface;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Simple in-memory segment implementation for testing AbstractSegments.
 */
class TestSegment implements Segment {
    final long id;
    private final String name;
    private boolean open = false;
    private File dbDir;

    TestSegment(final String name, final long id) {
        this.name = name;
        this.id = id;
    }

    public long id() {
        return id;
    }

    public void destroy() throws IOException {
        if (dbDir != null && dbDir.exists()) {
            deleteDirectory(dbDir);
        }
    }

    public void deleteRange(final Bytes keyFrom, final Bytes keyTo) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writePosition() {
        // no-op
    }

    public void openDB(final Map<String, Object> configs, final File stateDir) {
        if (stateDir != null) {
            final String storeName = name.substring(0, name.indexOf('.'));
            final File storeDir = new File(stateDir, storeName);
            dbDir = new File(storeDir, name);
            if (!dbDir.exists()) {
                dbDir.mkdirs();
            }
        }
        open = true;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void init(final StateStoreContext context, final StateStore root) {
        if (context.stateDir() != null) {
            // Extract store name from segment name (e.g., "test.0" -> "test")
            final String storeName = name.substring(0, name.indexOf('.'));
            final File storeDir = new File(context.stateDir(), storeName);
            dbDir = new File(storeDir, name);
            if (!dbDir.exists()) {
                dbDir.mkdirs();
            }
        }
        open = true;
    }

    // no need to implement KeyValueStore methods

    @SuppressWarnings("deprecation")
    @Override
    public void flush() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        open = false;
    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public Position getPosition() {
        return Position.emptyPosition();
    }

    @Override
    public void put(final Bytes key, final byte[] value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] putIfAbsent(final Bytes key, final byte[] value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] delete(final Bytes key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] get(final Bytes key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseRange(final Bytes from, final Bytes to) {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseAll() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R> QueryResult<R> query(final Query<R> query,
                                    final PositionBound positionBound,
                                    final QueryConfig config) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long approximateNumEntries() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addToBatch(final KeyValue<byte[], byte[]> record,
                          final WriteBatchInterface batch) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(final WriteBatchInterface batch) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commit(final Map<TopicPartition, Long> changelogOffsets) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "TestSegment(id=" + id + ", name=" + name + ")";
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final TestSegment segment = (TestSegment) obj;
        return id == segment.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    private void deleteDirectory(final File directory) throws IOException {
        if (directory.exists()) {
            final File[] files = directory.listFiles();
            if (files != null) {
                for (final File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        if (!file.delete()) {
                            throw new IOException("Failed to delete file: " + file);
                        }
                    }
                }
            }
            if (!directory.delete()) {
                throw new IOException("Failed to delete directory: " + directory);
            }
        }
    }
}
