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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.utils.Utils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A wrapper over an `AbstractIndex` instance that provides a mechanism to defer loading
 * (i.e. memory mapping) the underlying index until it is accessed for the first time via the
 * `get` method.
 *
 * In addition, this class exposes a number of methods (e.g. updateParentDir, renameTo, close,
 * etc.) that provide the desired behavior without causing the index to be loaded. If the index
 * had previously been loaded, the methods in this class simply delegate to the relevant method in
 * the index.
 *
 * This is an important optimization with regards to broker start-up and shutdown time if it has a
 * large number of segments.
 *
 * Methods of this class are thread safe. Make sure to check `AbstractIndex` subclasses
 * documentation to establish their thread safety.
 */
public class LazyIndex<T extends AbstractIndex> implements Closeable {

    @FunctionalInterface
    private interface IndexLoader<T extends AbstractIndex> {
        T load(File file) throws IOException;
    }

    private interface IndexWrapper extends Closeable {
        File file();
        void updateParentDir(File file);
        void renameTo(File file);
        boolean deleteIfExists();
        @Override
        void close();
        void closeHandler();
    }

    private static class IndexFile implements IndexWrapper {

        private volatile File file;

        IndexFile(File file) {
            this.file = file;
        }

        @Override
        public File file() {
            return file;
        }

        @Override
        public void updateParentDir(File parentDir) {
            file = new File(parentDir, file.getName());
        }

        @Override
        public void renameTo(File f) {
            try {
                Utils.atomicMoveWithFallback(file.toPath(), f.toPath(), false);
            } catch (NoSuchFileException e) {
                if (file.exists()) {
                    throw new UncheckedIOException(
                        String.format("Error renaming index file %s to %s", file, f), e);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(
                    String.format("Error renaming index file %s to %s", file, f), e);
            } finally {
                file = f;
            }
        }

        @Override
        public boolean deleteIfExists() {
            try {
                return Files.deleteIfExists(file.toPath());
            } catch (IOException e) {
                throw new UncheckedIOException(
                    String.format("Error deleting index file %s", file), e);
            }
        }

        @Override
        public void close() { }

        @Override
        public void closeHandler() { }

    }

    private static class IndexValue<T extends AbstractIndex> implements IndexWrapper {

        private final T index;

        IndexValue(T index) {
            this.index = index;
        }

        @Override
        public File file() {
            return index.file();
        }

        @Override
        public void updateParentDir(File parentDir) {
            index.updateParentDir(parentDir);
        }

        @Override
        public void renameTo(File f) {
            try {
                index.renameTo(f);
            } catch (IOException e) {
                throw new UncheckedIOException(
                    String.format("Error renaming index file %s to %s", index.file(), f), e);
            }
        }

        @Override
        public boolean deleteIfExists() {
            try {
                return index.deleteIfExists();
            } catch (IOException e) {
                throw new UncheckedIOException(
                    String.format("Error deleting index file %s", index.file()), e);
            }
        }

        @Override
        public void close() {
            try {
                index.close();
            } catch (IOException e) {
                throw new UncheckedIOException(
                    String.format("Error closing index file %s", index.file()), e);
            }
        }

        @Override
        public void closeHandler() {
            index.closeHandler();
        }
    }

    private final Lock lock = new ReentrantLock();
    private final IndexLoader<T> indexLoader;

    private volatile IndexWrapper indexWrapper;

    private LazyIndex(IndexWrapper indexWrapper, IndexLoader<T> indexLoader) {
        this.indexWrapper = indexWrapper;
        this.indexLoader = indexLoader;
    }

    public static LazyIndex<OffsetIndex> forOffset(File file, long baseOffset, int maxIndexSize) {
        return new LazyIndex<>(new IndexFile(file),
            f -> new OffsetIndex(f, baseOffset, maxIndexSize, true));
    }

    public static LazyIndex<TimeIndex> forTime(File file, long baseOffset, int maxIndexSize) {
        return new LazyIndex<>(new IndexFile(file),
            f -> new TimeIndex(f, baseOffset, maxIndexSize, true));
    }

    public File file() {
        return indexWrapper.file();
    }

    @SuppressWarnings("unchecked")
    public T get() {
        IndexWrapper wrapper = indexWrapper;
        if (wrapper instanceof IndexValue<?>) {
            return ((IndexValue<T>) wrapper).index;
        }
        lock.lock();
        try {
            if (indexWrapper instanceof IndexValue<?>) {
                return ((IndexValue<T>) indexWrapper).index;
            } else if (indexWrapper instanceof IndexFile indexFile) {
                T loaded;
                try {
                    loaded = indexLoader.load(indexFile.file);
                } catch (IOException e) {
                    throw new UncheckedIOException(
                        String.format("Error loading index file %s", indexFile.file), e);
                }
                IndexValue<T> indexValue = new IndexValue<>(loaded);
                indexWrapper = indexValue;
                return indexValue.index;
            } else {
                throw new IllegalStateException("Unexpected type for indexWrapper " + indexWrapper.getClass());
            }
        } finally {
            lock.unlock();
        }
    }

    public void updateParentDir(File parentDir) {
        lock.lock();
        try {
            indexWrapper.updateParentDir(parentDir);
        } finally {
            lock.unlock();
        }
    }

    public void renameTo(File f) {
        lock.lock();
        try {
            indexWrapper.renameTo(f);
        } finally {
            lock.unlock();
        }
    }

    public boolean deleteIfExists() {
        lock.lock();
        try {
            return indexWrapper.deleteIfExists();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            indexWrapper.close();
        } finally {
            lock.unlock();
        }
    }

    public void closeHandler() {
        lock.lock();
        try {
            indexWrapper.closeHandler();
        } finally {
            lock.unlock();
        }
    }

}
